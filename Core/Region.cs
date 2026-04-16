using System.Buffers;
using System.Threading.Channels;
using Microsoft.Extensions.ObjectPool;
using Google.Protobuf;
using NATS.Client.Core;
using ProcessServer;

namespace Xelerate;

public class Region : IAsyncDisposable
{
    private readonly long _id;
    private NatsConnection _nats;

    private Task? _loopTask;
    private CancellationTokenSource? _cts;

    Dictionary<string, TimedSortedSet<long, XelerateItem>> _processRunning = new();
    Dictionary<string, TimedSortedSet<long, XelerateItem>> _deleteDelay = new();
    Dictionary<string, TimedSortedSet<long, XelerateItem>> _pingExpired = new();
    Dictionary<string, Dictionary<long, XelerateItem>> _currentItems = new();

    private ObjectPool<ProcessPayload> _payloadPool;
    private ObjectPool<XelerateItem> _itemPool;

    // 50 KB
    private const int PublishBatchSize = 50 * 1024;
    private Queue<ProcessPayload> _pendingPayload;
    private Dictionary<string, ProcessPayload> _currentPublishingPayload = new();

    private readonly Channel<RegionMessage> _channel;

    public Region(long id, NatsConnection nats)
    {
        _id = id;
        _nats = nats;

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
                        case RegionMessageType.DeleteItem:
                            Delete(msg.Item!);
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

            await FlushPendingPublishesAsync(ct);
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
                        case ProcessType.Ping:
                            Ping(item, i);
                            break;
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

    private async Task FlushPendingPublishesAsync(CancellationToken ct)
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
                var size = payload.CalculateSize();
                var byteData = ArrayPool<byte>.Shared.Rent(size);
                payload.WriteTo(byteData.AsSpan(0, size));
                await _nats.PublishAsync("ProcessClient", byteData.AsMemory(0, size),
                    cancellationToken: ct);
            }
            finally
            {
                _payloadPool.Return(payload);
            }
        }
    }

    // đảm bảo phải ping để unit sống
    // nếu không ping trong vòng 10 giây đối tượng sẽ tự xóa mà không có thông báo gì hết
    // trường hợp đối tượng ping không có báo Require
    private void Ping(ProcessPayload item, int i)
    {
        var unitId = item.UnitIds[i];
        var version = item.Versions[i];
        var unitType = item.UnitType;
        if (_currentItems.TryGetValue(unitType, out var currentItems) &&
            currentItems.TryGetValue(unitId, out var currentItem))
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (!_pingExpired.TryGetValue(unitType, out var pingSorted))
            {
                pingSorted = new TimedSortedSet<long, XelerateItem>();
                _pingExpired[unitType] = pingSorted;
                pingSorted.OnExpired += list =>
                {
                    // luồng ngoài -> đẩy chanel
                    foreach (var valueTuple in list)
                    {
                        _channel.Writer.TryWrite(new RegionMessage(RegionMessageType.DeleteItem, valueTuple.Value));
                    }
                };
            }

            // thêm vào hàng chờ xóa nếu ping không được ping lại trong 10 giây
            pingSorted.AddOrUpdate(unitId, currentItem, now + 10_000);

            // case 1 dữ liệu hiện tại tại server đã hoàn thành nhưng client ping lên version cũ hơn,
            // có thể do client bị lag hoặc gửi nhầm, không cần gửi lại Require,
            // Gửi lại OnDone để client cập nhật lại
            if (version < currentItem.Version && currentItem.Status)
            {
                currentItem.Type = ProcessType.Done;
                SendToClient(currentItem);
            }
            // case 2 dữ liệu version bị khác nhau nhưng không phải case 1, yêu cầu client gửi lại update
            else if (version != currentItem.Version)
            {
                var newItem = GetByIndex(item, i);
                newItem.Type = ProcessType.NeedUpdate;
                SendToClient(newItem);
                _itemPool.Return(newItem);
            }
        }
        else
        {
            var newItem = GetByIndex(item, i);
            newItem.Type = ProcessType.Require;
            SendToClient(newItem);
            _itemPool.Return(newItem);
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
            var pingSorted = GetPingSorted(unitType);
            pingSorted.AddOrUpdate(unitId, currentItem, now + 10_000);
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
        var deleteSorted = GetDeleteSorted(item.UnitType);
        var processSorted = GetProcessSorted(item.UnitType); // Thêm
        var pingSorted = GetPingSorted(item.UnitType);

        if (_currentItems.TryGetValue(item.UnitType, out var currentItems))
        {
            var unitId = item.UnitIds[i];

            if (currentItems.TryGetValue(unitId, out var currentItem))
            {
                deleteSorted.Remove(unitId);
                processSorted.Remove(unitId);
                pingSorted.Remove(unitId);

                currentItems.Remove(unitId);
                _itemPool.Return(currentItem);
            }
        }
    }

// luông an toàn
    private void Delete(XelerateItem item)
    {
        var deleteSorted = GetDeleteSorted(item.UnitType);
        var processSorted = GetProcessSorted(item.UnitType);
        var pingSorted = GetPingSorted(item.UnitType);

        deleteSorted.Remove(item.UnitId);
        processSorted.Remove(item.UnitId);
        pingSorted.Remove(item.UnitId);

        if (_currentItems.TryGetValue(item.UnitType, out var currentItems))
        {
            currentItems.Remove(item.UnitId);
        }

        _itemPool.Return(item);
    }


    // khi là Done, chuyển tất cả status = true, gửi về client OnDone
    // tăng version lên 1
    // lên lịch xóa khỏi _currentItems sau 10 giây
    // luồng an toàn
    private void Done(XelerateItem item)
    {
        item.Status = true;
        item.Type = ProcessType.Done;
        item.Version++;
        var unitType = item.UnitType;
        var deleteSorted = GetDeleteSorted(unitType);
        var processSorted = GetProcessSorted(unitType);
        deleteSorted.AddOrUpdate(item.UnitId, item, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 10_000);
        processSorted.Remove(item.UnitId);
        SendToClient(item);
    }

    private static bool ValidData(ProcessPayload item)
    {
        return item.UnitIds.Count == item.Statuses.Count &&
               item.UnitIds.Count == item.TimeTargetMs.Count &&
               item.UnitIds.Count == item.DataList.Count &&
               item.UnitIds.Count == item.Versions.Count;
    }

    private XelerateItem GetByIndex(ProcessPayload item, int i)
    {
        var unitId = item.UnitIds[i];
        var status = item.Statuses[i];
        var timeTargetM = item.TimeTargetMs[i];
        var data = item.DataList[i];
        var version = item.Versions[i];

        var result = _itemPool.Get();
        result.UnitId = unitId;
        result.Status = status;
        result.TimeTargetMs = timeTargetM;
        result.UnitType = item.UnitType;
        result.Version = version;

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
        value.Status = item.Statuses[i];
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

    private TimedSortedSet<long, XelerateItem> GetDeleteSorted(string unitType)
    {
        if (!_deleteDelay.TryGetValue(unitType, out var sorted))
        {
            sorted = new TimedSortedSet<long, XelerateItem>();
            _deleteDelay[unitType] = sorted;
            sorted.OnExpired += list =>
            {
                // luồng ngoai -> đẩy chanel
                foreach (var valueTuple in list)
                {
                    _channel.Writer.TryWrite(new RegionMessage(RegionMessageType.DeleteItem, valueTuple.Value));
                }
            };
        }

        return sorted;
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

    private TimedSortedSet<long, XelerateItem> GetPingSorted(string unitType)
    {
        if (!_pingExpired.TryGetValue(unitType, out var sorted))
        {
            sorted = new TimedSortedSet<long, XelerateItem>();
            _pingExpired[unitType] = sorted;
            sorted.OnExpired += list =>
            {
                // luồng ngoai -> đẩy chanel
                foreach (var valueTuple in list)
                {
                    _channel.Writer.TryWrite(new RegionMessage(RegionMessageType.DeleteItem, valueTuple.Value));
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