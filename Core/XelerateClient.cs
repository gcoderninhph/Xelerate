using System.Buffers;
using Google.Protobuf;
using NATS.Client.Core;
using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.ObjectPool;
using ProcessServer;

namespace Xelerate;

public interface IXelerateClient
{
    Task StartAsync();
    void OnRequire(string unitType, Func<XelerateResponse, Task<XelerateData>> onRequire);

    // SỬA ĐỔI: Đổi Action thành Func để có thể trả về ProcessData cho Server
    void OnRequire(string unitType, Func<XelerateResponse, XelerateData> onRequire);

    void OnDone(string unitType, Func<XelerateResponse, Task> onDone);
    void OnDone(string unitType, Action<XelerateResponse> onDone);

    IXelerateRequest Create(string unitType);
}

public interface IXelerateRequest
{
    public Task Send(long regionId, long unitId, int version, long timeTargetMs, ReadOnlyMemory<byte> data);
    Task Ping(long regionId, long unitId, int version);
    Task Cancel(long regionId, long unitId, int version);
}

public class XelerateClient : IXelerateClient, IAsyncDisposable
{
    private readonly NatsConnection _nats;
    private readonly CancellationTokenSource _cts = new();

    // Lưu trữ các callback handlers dựa theo UnitType
    private readonly ConcurrentDictionary<string, Func<XelerateResponse, Task<XelerateData?>>> _requireHandlersAsync =
        new();

    private readonly ConcurrentDictionary<string, Func<XelerateResponse, Task>> _doneHandlersAsync = new();
    private readonly ObjectPool<ProcessPayload> _payloadPool;

    private readonly Channel<ProcessPayload> _receiveChannel = Channel.CreateUnbounded<ProcessPayload>();
    private readonly Channel<SendQueueItem> _sendChannel = Channel.CreateUnbounded<SendQueueItem>();
    const int MaxBatchSize = 50 * 1024; // 50KB


    public XelerateClient(string natsUrl)
    {
        _nats = new NatsConnection(new NatsOpts { Url = natsUrl });
        DefaultObjectPoolProvider provider = new();
        _payloadPool = provider.Create(new XeleratePayloadPooledObjectPolicy());
    }

    public async Task StartAsync()
    {
        await _nats.ConnectAsync();

        _ = Task.Run(ReceiveLoopAsync, _cts.Token);
        _ = Task.Run(SendLoopAsync, _cts.Token);
        _ = Task.Run(ProcessLoopAsync, _cts.Token);
    }

    private async Task ProcessLoopAsync()
    {
        var reader = _receiveChannel.Reader;
        var ct = _cts.Token;
        while (!_cts.IsCancellationRequested)
        {
            try
            {
                await reader.WaitToReadAsync(ct);
            }
            catch (OperationCanceledException)
            {
            }

            while (reader.TryRead(out var payload))
            {
                await HandlePayloadAsync(payload);
            }
        }
    }

    private async Task ReceiveLoopAsync()
    {
        var ct = _cts.Token;
        try
        {
            await foreach (var msg in _nats.SubscribeAsync<NatsMemoryOwner<byte>>("XelerateClientSubject",
                               cancellationToken: ct))
            {
                using var memoryOwner = msg.Data;
                var payload = _payloadPool.Get();
                payload.MergeFrom(memoryOwner.Memory.Span);
                _receiveChannel.Writer.TryWrite(payload);
            }
        }
        catch (OperationCanceledException)
        {
            /* Bỏ qua khi tắt Client */
        }
    }

    private async Task SendLoopAsync()
    {
        var reader = _sendChannel.Reader;
        var batches = new Dictionary<(long RegionId, string UnitType), ProcessPayload>();
        var ct = _cts.Token;
        
        try
        {
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
                // Xử lý item đầu tiên mở đầu cho cửa sổ Batch
                while (readCount < maxReadsPerLoop && reader.TryRead(out var firstItem))
                {
                    await ProcessItemAsync(firstItem, batches, MaxBatchSize);
                    readCount++;
                }

                foreach (var payload in batches.Values)
                {
                    await PublishBatchAsync(payload);
                }

                batches.Clear();
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task ProcessItemAsync(SendQueueItem item, Dictionary<(long, string), ProcessPayload> batches,
        int maxSize)
    {
        var key = (item.RegionId, item.UnitType);
        if (!batches.TryGetValue(key, out var payload))
        {
            payload = _payloadPool.Get();
            payload.RegionId = item.RegionId;
            payload.UnitType = item.UnitType;
            batches[key] = payload;
        }

        payload.UnitIds.Add(item.UnitId);
        payload.Versions.Add(item.Version);
        payload.Type.Add(item.Type);
        payload.Statuses.Add(false);
        payload.TimeTargetMs.Add(item.TimeTargetMs);
        payload.DataList.Add(item.Data);

        // KHI VƯỢT QUÁ 50KB: Lập tức Flush batch này ngay
        if (payload.CalculateSize() >= maxSize)
        {
            await PublishBatchAsync(payload);
            batches.Remove(key);
        }
    }

    private async Task PublishBatchAsync(ProcessPayload payload)
    {
        if (payload.UnitIds.Count == 0)
        {
            _payloadPool.Return(payload);
            return;
        }

        try
        {
            int size = payload.CalculateSize();
            var buffer = ArrayPool<byte>.Shared.Rent(size);
            try
            {
                payload.WriteTo(buffer.AsSpan(0, size));
                await _nats.PublishAsync($"XelerateServerSubject.Region.{payload.RegionId}", buffer.AsMemory(0, size));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        catch (Exception)
        {
            /* Log publish error */
        }
        finally
        {
            _payloadPool.Return(payload);
        }
    }

    private async Task HandlePayloadAsync(ProcessPayload payload)
    {
        try
        {
            var unitType = payload.UnitType;
            var regionId = payload.RegionId;

            // Xử lý từng Item giống như cách Region xử lý
            for (int i = 0; i < payload.UnitIds.Count; i++)
            {
                try
                {
                    var type = payload.Type[i];
                    var unitId = payload.UnitIds[i];
                    var version = payload.Versions[i];
                    var data = payload.DataList[i]?.Memory ?? ReadOnlyMemory<byte>.Empty;

                    var response = new XelerateResponse(regionId, unitId, version, data);

                    // Xử lý khi Server Require (mất context) hoặc NeedUpdate (sai version)
                    if (type == ProcessType.Require || type == ProcessType.NeedUpdate)
                    {
                        if (_requireHandlersAsync.TryGetValue(unitType, out var handler))
                        {
                            var resultData = await handler(response);
                            if (resultData.HasValue)
                            {
                                // Lấy dữ liệu từ Callback và tự động gửi Update lại cho Server
                                var req = Create(unitType);
                                await req.Send(regionId, resultData.Value.UnitId, resultData.Value.Version,
                                    resultData.Value.TimeTargetMs, resultData.Value.Data);
                            }
                        }
                    }
                    // Xử lý khi quá trình được Done tại Server
                    else if (type == ProcessType.Done)
                    {
                        if (_doneHandlersAsync.TryGetValue(unitType, out var handler))
                        {
                            await handler(response);
                        }
                    }
                }
                catch
                {
                    //
                }
            }
        }
        finally
        {
            _payloadPool.Return(payload);
        }
    }

    public void OnRequire(string unitType, Func<XelerateResponse, Task<XelerateData>> onRequire)
    {
        _requireHandlersAsync[unitType] = async res => await onRequire(res);
    }

    public void OnRequire(string unitType, Func<XelerateResponse, XelerateData> onRequire)
    {
        _requireHandlersAsync[unitType] = res => Task.FromResult<XelerateData?>(onRequire(res));
    }

    public void OnDone(string unitType, Func<XelerateResponse, Task> onDone)
    {
        _doneHandlersAsync[unitType] = onDone;
    }

    public void OnDone(string unitType, Action<XelerateResponse> onDone)
    {
        _doneHandlersAsync[unitType] = res =>
        {
            onDone(res);
            return Task.CompletedTask;
        };
    }

    public IXelerateRequest Create(string unitType)
    {
        return new XelerateRequest(unitType, item => _sendChannel.Writer.TryWrite(item));
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _cts.Dispose();
        await _nats.DisposeAsync();
    }
}

public class XelerateRequest(string unitType, Action<SendQueueItem> enqueueAction)
    : IXelerateRequest
{
    public Task Send(long regionId, long unitId, int version, long timeTargetMs, ReadOnlyMemory<byte> data)
    {
        var byteData = data.Length > 0 ? ByteString.CopyFrom(data.Span) : ByteString.Empty;
        enqueueAction(
            new SendQueueItem(regionId, unitType, unitId, version, timeTargetMs, byteData, ProcessType.Update));
        return Task.CompletedTask;
    }

    public Task Ping(long regionId, long unitId, int version)
    {
        enqueueAction(new SendQueueItem(regionId, unitType, unitId, version, 0, ByteString.Empty, ProcessType.Ping));
        return Task.CompletedTask;
    }

    public Task Cancel(long regionId, long unitId, int version)
    {
        enqueueAction(new SendQueueItem(regionId, unitType, unitId, version, 0, ByteString.Empty, ProcessType.Delete));
        return Task.CompletedTask;
    }
}

public readonly struct XelerateResponse(long regionId, long unitId, int version, ReadOnlyMemory<byte> data)
{
    public long RegionId { get; } = regionId;
    public long UnitId { get; } = unitId;
    public int Version { get; } = version;
    public ReadOnlyMemory<byte> Data { get; } = data;
}

public readonly struct XelerateData(long unitId, int version, long timeTargetMs, ReadOnlyMemory<byte> data)
{
    public long UnitId { get; } = unitId;
    public int Version { get; } = version;
    public long TimeTargetMs { get; } = timeTargetMs;
    public ReadOnlyMemory<byte> Data { get; } = data;
}

public readonly struct SendQueueItem(
    long regionId,
    string unitType,
    long unitId,
    int version,
    long timeTargetMs,
    ByteString data,
    ProcessType type)
{
    public long RegionId { get; } = regionId;
    public string UnitType { get; } = unitType;
    public long UnitId { get; } = unitId;
    public int Version { get; } = version;
    public long TimeTargetMs { get; } = timeTargetMs;
    public ByteString Data { get; } = data;
    public ProcessType Type { get; } = type;
}