using Google.Protobuf;
using System.Collections.Concurrent;
using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;
using ProcessServer;

namespace Xelerate;

public interface IXelerateClient
{
    void StartAsync();
    Task EnsureTopicsExistAsync();

    void OnDone(string unitType, Func<XelerateResponse, Task> onDone);
    void OnDone(string unitType, Action<XelerateResponse> onDone);

    IXelerateRequest Create(string unitType);
}

public interface IXelerateRequest
{
    void Send(long regionId, long unitId, long timeTargetMs, ReadOnlyMemory<byte> data);
    void Cancel(long regionId, long unitId);
}

public class XelerateClient : IXelerateClient, IAsyncDisposable
{
    // private readonly NatsConnection _nats;
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly IProducer<string, byte[]> _producer;

    private readonly CancellationTokenSource _cts = new();
    
    private readonly ConcurrentDictionary<string, Func<XelerateResponse, Task>> _doneHandlersAsync = new();
    private readonly ObjectPool<ProcessPayload> _payloadPool;

    private readonly Channel<ProcessPayload> _receiveChannel = Channel.CreateUnbounded<ProcessPayload>();
    private readonly Channel<SendQueueItem> _sendChannel = Channel.CreateUnbounded<SendQueueItem>();
    const int MaxBatchSize = 50 * 1024; // 50KB
    private Task[]? _backgroundTasks;
    private readonly string _kafkaBootstrapServers;


    public XelerateClient(string kafkaBootstrapServers, string? group = null)
    {
        _kafkaBootstrapServers = kafkaBootstrapServers;
        // _nats = new NatsConnection(new NatsOpts { Url = natsUrl });
        DefaultObjectPoolProvider provider = new();
        _payloadPool = provider.Create(new XeleratePayloadPooledObjectPolicy());
        // _group = group;

        // 1. Cấu hình Kafka Consumer
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = kafkaBootstrapServers,
            // Nếu không có group, tạo random để hoạt động như Broadcast (giống NATS khi queueGroup = null)
            GroupId = string.IsNullOrEmpty(group) ? $"xelerate-client-{Guid.NewGuid()}" : group,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        _consumer = new ConsumerBuilder<string, byte[]>(consumerConfig)
            .Build();

        _consumer.Subscribe("XelerateClientTopic");

        // 2. Cấu hình Kafka Producer (Zero-Allocation Native)
        var producerConfig = new ProducerConfig { BootstrapServers = kafkaBootstrapServers, Acks = Acks.All };
        _producer = new ProducerBuilder<string, byte[]>(producerConfig).Build();
    }

    public void StartAsync()
    {
        _backgroundTasks = new[]
        {
            Task.Run(ReceiveLoopAsync, _cts.Token),
            Task.Run(SendLoopAsync, _cts.Token),
            Task.Run(ProcessLoopAsync, _cts.Token)
        };
    }

    public Task EnsureTopicsExistAsync()
    {
        return KafkaExtension.EnsureTopicsExistAsync(_kafkaBootstrapServers);
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

    private void ReceiveLoopAsync()
    {
        var ct = _cts.Token;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(ct);
                if (consumeResult == null) continue;
                var payload = _payloadPool.Get();
                payload.MergeFrom(consumeResult.Message.Value);
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
                    ProcessItemAsync(firstItem, batches, MaxBatchSize);
                    readCount++;
                }

                foreach (var payload in batches.Values)
                {
                    PublishBatchAsync(payload);
                }

                batches.Clear();
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private void ProcessItemAsync(SendQueueItem item, Dictionary<(long, string), ProcessPayload> batches,
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
        payload.Type.Add(item.Type);
        payload.TimeTargetMs.Add(item.TimeTargetMs);
        payload.DataList.Add(item.Data);

        // KHI VƯỢT QUÁ 50KB: Lập tức Flush batch này ngay
        if (payload.CalculateSize() >= maxSize)
        {
            PublishBatchAsync(payload);
            batches.Remove(key);
        }
    }

    private void PublishBatchAsync(ProcessPayload payload)
    {
        if (payload.UnitIds.Count == 0)
        {
            _payloadPool.Return(payload);
            return;
        }

        try
        {
            try
            {
                var message = new Message<string, byte[]>
                {
                    Key = $"Region.{payload.RegionId}",
                    Value = payload.ToByteArray()
                };

                _producer.Produce("XelerateServerTopic", message, deliveryReport =>
                {
                    if (deliveryReport.Error.IsError) Console.WriteLine(deliveryReport.Error.Reason);
                });
            }
            catch
            {
                Console.WriteLine("Failed to publish batch for RegionId: " + payload.RegionId);
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
                    var data = payload.DataList[i]?.Memory ?? ReadOnlyMemory<byte>.Empty;

                    var response = new XelerateResponse(regionId, unitId,  data);
                    
                    if (type == ProcessType.Done)
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
        // 1. Kích hoạt hủy token
        await _cts.CancelAsync();

        // 2. Chờ cả 3 luồng ngầm dừng hẳn
        if (_backgroundTasks != null)
        {
            try
            {
                await Task.WhenAll(_backgroundTasks);
            }
            catch (OperationCanceledException)
            {
            }
        }

        // 3. Dọn dẹp Kafka (BẮT BUỘC)
        _consumer.Close();
        _consumer.Dispose();

        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();

        // 4. Đóng các Channel
        _receiveChannel.Writer.TryComplete();
        _sendChannel.Writer.TryComplete();

        _cts.Dispose();
    }
}

public class XelerateRequest(string unitType, Action<SendQueueItem> enqueueAction)
    : IXelerateRequest
{
    public void Send(long regionId, long unitId, long timeTargetMs, ReadOnlyMemory<byte> data)
    {
        var byteData = data.Length > 0 ? ByteString.CopyFrom(data.Span) : ByteString.Empty;
        enqueueAction(
            new SendQueueItem(regionId, unitType, unitId, timeTargetMs, byteData, ProcessType.Update));
    }

    public void Cancel(long regionId, long unitId)
    {
        enqueueAction(new SendQueueItem(regionId, unitType, unitId, 0, ByteString.Empty, ProcessType.Delete));
    }
}

public readonly struct XelerateResponse(long regionId, long unitId,  ReadOnlyMemory<byte> data)
{
    public long RegionId { get; } = regionId;
    public long UnitId { get; } = unitId;
    public ReadOnlyMemory<byte> Data { get; } = data;
}

public readonly struct SendQueueItem(
    long regionId,
    string unitType,
    long unitId,
    long timeTargetMs,
    ByteString data,
    ProcessType type)
{
    public long RegionId { get; } = regionId;
    public string UnitType { get; } = unitType;
    public long UnitId { get; } = unitId;
    public long TimeTargetMs { get; } = timeTargetMs;
    public ByteString Data { get; } = data;
    public ProcessType Type { get; } = type;
}