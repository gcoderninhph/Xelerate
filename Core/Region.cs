using System.Buffers;
using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;
using Google.Protobuf;
using ProcessServer;

namespace Xelerate;

public class Region : IAsyncDisposable
{
    private readonly long _id;

    private IProducer<string, byte[]> _producer;

    private Task? _loopTask;
    private CancellationTokenSource? _cts;

    Dictionary<string, TimedSortedSet<long, XelerateItem>> _processRunning = new();
    Dictionary<string, Dictionary<long, XelerateItem>> _currentItems = new();

    private ObjectPool<ProcessPayload> _payloadPool;
    private ObjectPool<XelerateItem> _itemPool;

    // 50 KB
    private const int PublishBatchSize = 50 * 1024;
    private Queue<ProcessPayload> _pendingPayload;
    private Dictionary<string, ProcessPayload> _currentPublishingPayload = new();

    private readonly Channel<RegionMessage> _channel;
    private readonly ChannelWriter<DbOperation> _dbWriter;

    public Region(long id, IProducer<string, byte[]> producer, ChannelWriter<DbOperation> dbWriter)
    {
        _id = id;
        _producer = producer;
        _dbWriter = dbWriter;
        // _nats = nats;

        var option = new UnboundedChannelOptions { SingleReader = true };
        _channel = Channel.CreateUnbounded<RegionMessage>(option);

        DefaultObjectPoolProvider provider = new();
        _payloadPool = provider.Create(new XeleratePayloadPooledObjectPolicy());
        _itemPool = provider.Create(new XelerateItemPooledObjectPolicy());

        _pendingPayload = new();

        Start();
    }

    private void Start()
    {
        _cts = new CancellationTokenSource();
        _loopTask = Task.Factory.StartNew(
            () => RegionLoopAsync(_cts.Token),
            _cts.Token,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
        ).Unwrap();
    }

    public void EnqueueEvent(Span<byte> data)
    {
        var payload = _payloadPool.Get();
        payload.MergeFrom(data);
        _channel.Writer.TryWrite(new RegionMessage(payload));
    }

    // THÊM HÀM NÀY: Dùng để nạp dữ liệu từ MySQL lúc khởi động Server
    public void RestoreItem(string unitType, long unitId, long timeTargetMs, byte[]? originalData)
    {
        var currentItem = _itemPool.Get();
        currentItem.UnitId = unitId;
        currentItem.UnitType = unitType;
        currentItem.TimeTargetMs = timeTargetMs;

        if (originalData != null && originalData.Length > 0)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(originalData.Length);
            originalData.CopyTo(buffer, 0);
            currentItem.OriginalData = buffer;
            currentItem.DataLength = originalData.Length;
        }

        var currentItems = GetCurrentItemsDict(unitType);
        currentItems[unitId] = currentItem;

        var sortedSet = GetProcessSorted(unitType);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        if (timeTargetMs < now)
        {
            Done(currentItem); // Hết hạn rồi thì bắn Done luôn
        }
        else
        {
            sortedSet.AddOrUpdate(unitId, currentItem, timeTargetMs);
        }
    }

    private Dictionary<long, XelerateItem> GetCurrentItemsDict(string unitType)
    {
        if (!_currentItems.TryGetValue(unitType, out var currentItems))
        {
            currentItems = new Dictionary<long, XelerateItem>();
            _currentItems[unitType] = currentItems;
        }

        return currentItems;
    }

    private async Task RegionLoopAsync(CancellationToken ct)
    {
        var reader = _channel.Reader;


        while (!ct.IsCancellationRequested)
        {
            try
            {
                await reader.WaitToReadAsync(ct);
            }
            catch (OperationCanceledException)
            {
            }

            int readCount = 0;
            const int maxReadsPerLoop = 1000;
            while (readCount < maxReadsPerLoop && reader.TryRead(out var msg))
            {
                try
                {
                    // Chia luồng xử lý dựa vào Type
                    switch (msg.Type)
                    {
                        case RegionMessageType.ProcessPayload:
                            ProcessEvent(msg.Payload!);
                            break;
                        case RegionMessageType.DoneItem:
                            Done(msg.Item!);
                            break;
                    }
                }
                catch
                {
                    // Nên có Console.WriteLine hoặc ILogger ở đây
                    // để tránh nuốt exception ẩn
                }
                finally
                {
                    readCount++;
                }
            }

            FlushPendingPublishesAsync();
        }
    }

    private HashSet<(string UnitType, long UnitId, ProcessType Type)> _processed = new();

    private void ProcessEvent(ProcessPayload item)
    {
        try
        {
            if (ValidData(item))
            {
                // 👇 THÊM BỘ LỌC KHỬ TRÙNG LẶP (FIX CASE 10)
                _processed.Clear();

                // Chạy vòng lặp NGƯỢC (từ cuối lên) để ưu tiên lấy lệnh mới nhất của Batch
                for (var i = item.UnitIds.Count - 1; i >= 0; i--)
                {
                    var type = item.Type[i];
                    var unitId = item.UnitIds[i];
                    var unitType = item.UnitType;

                    // Nếu UnitId và Type này đã được xử lý ở lệnh sau đó rồi thì bỏ qua
                    if (!_processed.Add((unitType, unitId, type))) continue;

                    switch (type)
                    {
                        case ProcessType.Update:
                            Update(item, i);
                            break;
                        case ProcessType.Delete:
                            Delete(item, i);
                            break;
                    }
                }
            }
        }
        finally
        {
            _payloadPool.Return(item);
        }
    }

    private void FlushPendingPublishesAsync()
    {
        foreach (var payload in _currentPublishingPayload.Values)
        {
            _pendingPayload.Enqueue(payload);
        }

        _currentPublishingPayload.Clear();

        while (_pendingPayload.TryDequeue(out var payload))
        {
            try
            {
                payload.RegionId = _id;


                var message = new Message<string, byte[]>
                {
                    Key = $"Region.{_id}",
                    Value = payload.ToByteArray()
                };

                _producer.Produce("XelerateClientTopic", message, deliveryReport =>
                {
                    if (deliveryReport.Error.IsError) Console.WriteLine(deliveryReport.Error.Reason);
                });
            }
            finally
            {
                _payloadPool.Return(payload);
            }
        }
    }

    private void Update(ProcessPayload item, int i)
    {
        string unitType = item.UnitType;

        // tạo sortset nếu chưa có
        if (!_processRunning.TryGetValue(unitType, out var sortedSet))
        {
            sortedSet = new TimedSortedSet<long, XelerateItem>();
            sortedSet.OnExpired += list =>
            {
                // luồng ngoài -> đẩy chanel
                foreach (var valueTuple in list)
                {
                    _channel.Writer.TryWrite(new RegionMessage(RegionMessageType.DoneItem, valueTuple.Value));
                }
            };
            _processRunning[unitType] = sortedSet;
        }

        // tạo dictionary nếu chưa có
        if (!_currentItems.TryGetValue(unitType, out var currentItems))
        {
            currentItems = new Dictionary<long, XelerateItem>();
            _currentItems[unitType] = currentItems;
        }

        // update data
        if (ValidData(item))
        {
            var unitId = item.UnitIds[i];

            if (!currentItems.TryGetValue(unitId, out var currentItem))
            {
                currentItem = GetByIndex(item, i);
                currentItems[unitId] = currentItem;
            }
            else
            {
                SetValueByIndex(item, i, currentItem);
            }

            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();


            // 👇 THÊM CODE DB VÀO ĐÂY (Sự kiện 1 và 2: Upsert)
            byte[]? clonedData = null;
            if (currentItem.DataLength > 0 && currentItem.OriginalData != null)
            {
                // Clone array để luồng DB xử lý không bị đụng chạm với ArrayPool
                clonedData = new byte[currentItem.DataLength];
                Array.Copy(currentItem.OriginalData, clonedData, currentItem.DataLength);
            }

            _dbWriter.TryWrite(new DbOperation
            {
                OpType = DbOpType.Upsert,
                RegionId = _id,
                UnitType = unitType,
                UnitId = unitId,
                TimeTargetMs = currentItem.TimeTargetMs,
                Data = clonedData
            });
            // 👆 KẾT THÚC THÊM CODE DB


            if (currentItem.TimeTargetMs < now)
            {
                Done(currentItem);
            }
            else
            {
                sortedSet.AddOrUpdate(unitId, currentItem, currentItem.TimeTargetMs);
            }
        }
    }


// luông an toàn
// Client yêu cầu xóa, đây là force delete
    private void Delete(ProcessPayload item, int i)
    {
        var processSorted = GetProcessSorted(item.UnitType); // Thêm

        if (_currentItems.TryGetValue(item.UnitType, out var currentItems))
        {
            var unitId = item.UnitIds[i];

            // 👇 THÊM CODE DB VÀO ĐÂY (Sự kiện 3: Xóa theo yêu cầu)
            _dbWriter.TryWrite(new DbOperation
            {
                OpType = DbOpType.Delete,
                RegionId = _id,
                UnitType = item.UnitType,
                UnitId = unitId
            });
            // 👆 KẾT THÚC THÊM CODE DB


            if (currentItems.TryGetValue(unitId, out var currentItem))
            {
                processSorted.Remove(unitId);

                currentItems.Remove(unitId);
                _itemPool.Return(currentItem);
            }
        }
    }

    // khi là Done, chuyển tất cả status = true, gửi về client OnDone
    // tăng version lên 1
    // lên lịch xóa khỏi _currentItems sau 10 giây
    // luồng an toàn
    private void Done(XelerateItem item)
    {
        item.Type = ProcessType.Done;
        var unitType = item.UnitType;
        var processSorted = GetProcessSorted(unitType);
        processSorted.Remove(item.UnitId);
        SendToClient(item);
        
        // 👇 THÊM CODE DB VÀO ĐÂY (Sự kiện 3: Xóa khi Timeout Done)
        _dbWriter.TryWrite(new DbOperation
        {
            OpType = DbOpType.Delete,
            RegionId = _id,
            UnitType = unitType,
            UnitId = item.UnitId
        });
        // 👆 KẾT THÚC THÊM CODE DB

        if (_currentItems.TryGetValue(item.UnitType, out var currentItems))
        {
            currentItems.Remove(item.UnitId);
        }

        _itemPool.Return(item);
    }

    private static bool ValidData(ProcessPayload item)
    {
        return item.UnitIds.Count == item.TimeTargetMs.Count &&
               item.UnitIds.Count == item.DataList.Count;
    }

    private XelerateItem GetByIndex(ProcessPayload item, int i)
    {
        var unitId = item.UnitIds[i];
        var timeTargetM = item.TimeTargetMs[i];
        var data = item.DataList[i];

        var result = _itemPool.Get();
        result.UnitId = unitId;
        result.TimeTargetMs = timeTargetM;
        result.UnitType = item.UnitType;

        if (data != null && data.Length > 0)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(data.Length);
            data.CopyTo(buffer, 0);
            result.OriginalData = buffer;
            result.DataLength = data.Length;
        }

        return result;
    }

    private void SetValueByIndex(ProcessPayload item, int i, XelerateItem value)
    {
        value.UnitId = item.UnitIds[i];
        value.TimeTargetMs = item.TimeTargetMs[i];
        value.UnitType = item.UnitType;

        var data = item.DataList[i];
        if (data != null && data.Length > 0)
        {
            if (value.OriginalData != null)
                ArrayPool<byte>.Shared.Return(value.OriginalData);

            var buffer = ArrayPool<byte>.Shared.Rent(data.Length);
            data.CopyTo(buffer, 0);
            value.OriginalData = buffer;
            value.DataLength = data.Length;
        }
        else
        {
            if (value.OriginalData != null)
            {
                ArrayPool<byte>.Shared.Return(value.OriginalData);
                value.OriginalData = null;
            }

            value.DataLength = 0;
        }
    }

    private TimedSortedSet<long, XelerateItem> GetProcessSorted(string unitType)
    {
        if (!_processRunning.TryGetValue(unitType, out var sorted))
        {
            sorted = new TimedSortedSet<long, XelerateItem>();
            _processRunning[unitType] = sorted;
            sorted.OnExpired += list =>
            {
                // luồng ngoai -> đẩy chanel
                foreach (var valueTuple in list)
                {
                    _channel.Writer.TryWrite(new RegionMessage(RegionMessageType.DoneItem, valueTuple.Value));
                }
            };
        }

        return sorted;
    }


    private void SendToClient(XelerateItem item)
    {
        if (!_currentPublishingPayload.TryGetValue(item.UnitType, out var payload))
        {
            payload = _payloadPool.Get();
            payload.UnitType = item.UnitType;
            _currentPublishingPayload[item.UnitType] = payload;
        }

        payload.Add(item);

        if (payload.CalculateSize() >= PublishBatchSize)
        {
            _pendingPayload.Enqueue(payload);
            _currentPublishingPayload.Remove(item.UnitType);
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Bước 1: Gửi tín hiệu hủy (Cancel) để yêu cầu _loopTask dừng lại
        if (_cts != null && !_cts.IsCancellationRequested)
        {
            await _cts.CancelAsync();
        }

        // Bước 2: CHỜ cho Task thực sự kết thúc vòng lặp
        if (_loopTask != null)
        {
            try
            {
                await _loopTask; // BẮT BUỘC PHẢI AWAIT TRƯỚC
            }
            catch (OperationCanceledException)
            {
                // Bỏ qua lỗi này vì chúng ta chủ động Cancel
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Region {_id}] Lỗi khi tắt Task: {ex.Message}");
            }
            finally
            {
                // Bước 3: Bây giờ Task đã kết thúc, bạn có thể Dispose an toàn (hoặc bỏ qua luôn)
                _loopTask.Dispose();
            }
        }

        // Dọn dẹp CancellationTokenSource
        if (_cts != null)
        {
            _cts.Dispose();
        }

        // Dọn dẹp Channel và các tài nguyên khác
        _channel.Writer.TryComplete();

        // ... (Giữ nguyên phần dọn dẹp Dictionary / Pool cũ của bạn nếu có)
    }
}