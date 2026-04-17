using Confluent.Kafka;

namespace Xelerate;

public class XelerateServer : IAsyncDisposable
{
    private readonly IConsumer<string, PooledMessage> _consumer;
    private readonly IProducer<string, byte[]> _producer;

    // private NatsConnection _nats = new(new NatsOpts { Url = natsUrl });
    private CancellationTokenSource _cts = new();
    private Dictionary<long, Region> _regions = new();
    private Task? _consumeTask;
    private readonly string _kafkaBootstrapServers;

    public XelerateServer(string kafkaBootstrapServers)
    {
        _kafkaBootstrapServers = kafkaBootstrapServers;
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = kafkaBootstrapServers,
            GroupId = "xelerate-server-group",
            AutoOffsetReset = AutoOffsetReset.Latest,
        };

        // Gắn PooledBytesDeserializer vào Consumer
        _consumer = new ConsumerBuilder<string, PooledMessage>(consumerConfig)
            .SetValueDeserializer(new PooledBytesDeserializer())
            .Build();

        _consumer.Subscribe("XelerateServerTopic");

        var producerConfig = new ProducerConfig { BootstrapServers = kafkaBootstrapServers };
        _producer = new ProducerBuilder<string, byte[]>(producerConfig)
            .Build();
    }

    public Task EnsureTopicsExistAsync()
    {
        return KafkaExtension.EnsureTopicsExistAsync(_kafkaBootstrapServers);
    }

    public void StartAsync()
    {
        // await _nats.ConnectAsync();


        var ct = _cts.Token;

        _consumeTask = Task.Run(() =>
        {
            while (!ct.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(ct);
                if (consumeResult == null) continue;

                using var pooledMessage = consumeResult.Message.Value;
                var keyParts = consumeResult.Message.Key.Split('.');
                if (keyParts.Length == 2 && long.TryParse(keyParts[1], out var regionId))
                {
                    if (!_regions.TryGetValue(regionId, out var region))
                    {
                        region = new Region(regionId, _producer);
                        _regions[regionId] = region;
                    }

                    region.EnqueueEvent(pooledMessage.Span);
                }
            }
        }, ct);
    }

    // THÊM HÀM NÀY ĐỂ DỌN DẸP
    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();

        // 2. CHỜ cho luồng Consume thực sự thoát khỏi vòng lặp
        if (_consumeTask != null)
        {
            try
            {
                await _consumeTask;
            }
            catch (OperationCanceledException)
            {
            }
        }

        _consumer.Close();
        _consumer.Dispose();

        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();

        _cts.Dispose();

        foreach (var region in _regions.Values)
        {
            await region.DisposeAsync();
        }
    }
}