using System.Threading.Channels;
using Confluent.Kafka;
using MySqlConnector;

namespace Xelerate;

public class XelerateServer : IAsyncDisposable
{
    private readonly IConsumer<string, PooledMessage> _consumer;
    private readonly IProducer<string, byte[]> _producer;

    // private NatsConnection _nats = new(new NatsOpts { Url = natsUrl });
    private CancellationTokenSource _cts = new();
    private Dictionary<long, Region> _regions = new();

    private Task? _consumeTask;
    private Task? _dbBatchTask;

    private readonly string _kafkaBootstrapServers;
    private readonly string _mysqlConnectionString;

    private readonly Channel<DbOperation> _dbChannel;

    public XelerateServer(string kafkaBootstrapServers, string mysqlConnectionString)
    {
        _kafkaBootstrapServers = kafkaBootstrapServers;
        _mysqlConnectionString = mysqlConnectionString;

        _dbChannel = Channel.CreateUnbounded<DbOperation>();

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = kafkaBootstrapServers,
            GroupId = "xelerate-server-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            Acks = Acks.All
        };

        // Gắn PooledBytesDeserializer vào Consumer
        _consumer = new ConsumerBuilder<string, PooledMessage>(consumerConfig)
            .SetValueDeserializer(new PooledBytesDeserializer())
            .Build();

        _consumer.Subscribe("XelerateServerTopic");

        var producerConfig = new ProducerConfig { BootstrapServers = kafkaBootstrapServers, Acks = Acks.All };
        _producer = new ProducerBuilder<string, byte[]>(producerConfig)
            .Build();
    }

    public Task EnsureTopicsExistAsync()
    {
        return KafkaExtension.EnsureTopicsExistAsync(_kafkaBootstrapServers);
    }

    public async Task StartAsync()
    {
        // 1. Tự động khởi tạo bảng DB
        await InitializeDatabaseAsync();

        // 2. Load dữ liệu cũ từ DB vào RAM trước khi cho phép Kafka chạy
        await RestoreFromDatabaseAsync();

        var ct = _cts.Token;

        // 3. Khởi chạy luồng gom mẻ Database (1 luồng duy nhất)
        _dbBatchTask = Task.Run(() => DbBatchLoopAsync(ct), ct);

        // 4. Khởi chạy luồng xử lý Kafka
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
                    var region = GetOrAddRegion(regionId);
                    region.EnqueueEvent(pooledMessage.Span);
                }
            }
        }, ct);
    }

    private Region GetOrAddRegion(long regionId)
    {
        if (!_regions.TryGetValue(regionId, out var region))
        {
            // Truyền cờ _dbChannel.Writer vào để Region có thể gửi data xuống DB
            region = new Region(regionId, _producer, _dbChannel.Writer);
            _regions[regionId] = region;
        }

        return region;
    }

    private async Task InitializeDatabaseAsync()
    {
        await using var conn = new MySqlConnection(_mysqlConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE IF NOT EXISTS `XelerateItems` (
                `Id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `region_id` BIGINT NOT NULL,
                `unit_type` VARCHAR(100) NOT NULL,
                `unit_id` BIGINT NOT NULL,
                `time_target_ms` BIGINT NOT NULL,
                `data_list` LONGBLOB,
                UNIQUE KEY `uix_region_type_unit` (`region_id`, `unit_type`, `unit_id`)
            );";
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task RestoreFromDatabaseAsync()
    {
        await using var conn = new MySqlConnection(_mysqlConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT region_id, unit_type, unit_id, time_target_ms, data_list FROM XelerateItems";

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            long regionId = reader.GetInt64("region_id");
            string unitType = reader.GetString("unit_type");
            long unitId = reader.GetInt64("unit_id");
            long timeTargetMs = reader.GetInt64("time_target_ms");

            byte[]? data = null;
            if (!reader.IsDBNull(reader.GetOrdinal("data_list")))
            {
                data = (byte[])reader["data_list"];
            }

            // Gọi hàm Restore của Region
            var region = GetOrAddRegion(regionId);
            region.RestoreItem(unitType, unitId, timeTargetMs, data);
        }
    }

    private async Task DbBatchLoopAsync(CancellationToken ct)
    {
        var batch = new List<DbOperation>();
        var reader = _dbChannel.Reader;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                // Bước 1: Đợi item đầu tiên mở màn cho một mẻ (không có timeout)
                if (await reader.WaitToReadAsync(ct))
                {
                    if (reader.TryRead(out var firstOp))
                    {
                        batch.Add(firstOp);

                        // Bước 2: Bật đồng hồ đếm ngược 1 phút
                        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                        timeoutCts.CancelAfter(TimeSpan.FromMinutes(1));

                        try
                        {
                            // Thu thập liên tục cho đến khi đủ 200 items hoặc hết 1 phút
                            while (batch.Count < 200)
                            {
                                if (await reader.WaitToReadAsync(timeoutCts.Token))
                                {
                                    while (batch.Count < 200 && reader.TryRead(out var op))
                                    {
                                        batch.Add(op);
                                    }
                                }
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            /* Bị ngắt do hết 1 phút hoặc server tắt */
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                /* Server tắt */
            }

            // Bước 3: Đã đủ điều kiện (200 items hoặc 1 phút) -> Tiến hành ghi DB
            if (batch.Count > 0)
            {
                await SaveBatchToMySqlAsync(batch);
                batch.Clear();
            }
        }
    }

    private async Task SaveBatchToMySqlAsync(List<DbOperation> batch)
    {
        // Gom nhóm các thao tác theo Key để tìm ra hành động cuối cùng của 1 Item trong mẻ này
        // VD: Insert -> Update -> Delete cùng 1 ID trong 1 mẻ sẽ chỉ thực hiện Delete
        var consolidatedOps = new Dictionary<(long RegionId, string UnitType, long UnitId), DbOperation>();
        foreach (var op in batch)
        {
            consolidatedOps[(op.RegionId, op.UnitType, op.UnitId)] = op;
        }

        await using var conn = new MySqlConnection(_mysqlConnectionString);
        await conn.OpenAsync();
        await using var tx = await conn.BeginTransactionAsync();

        try
        {
            foreach (var op in consolidatedOps.Values)
            {
                await using var cmd = conn.CreateCommand();
                cmd.Transaction = tx;

                if (op.OpType == DbOpType.Delete)
                {
                    cmd.CommandText =
                        "DELETE FROM XelerateItems WHERE region_id = @rid AND unit_type = @utype AND unit_id = @uid;";
                    cmd.Parameters.AddWithValue("@rid", op.RegionId);
                    cmd.Parameters.AddWithValue("@utype", op.UnitType);
                    cmd.Parameters.AddWithValue("@uid", op.UnitId);
                }
                else // Upsert: Nếu có thì Update, chưa có thì Insert
                {
                    cmd.CommandText = @"
                        INSERT INTO XelerateItems (region_id, unit_type, unit_id, time_target_ms, data_list)
                        VALUES (@rid, @utype, @uid, @time, @data)
                        ON DUPLICATE KEY UPDATE 
                            time_target_ms = VALUES(time_target_ms),
                            data_list = VALUES(data_list);";

                    cmd.Parameters.AddWithValue("@rid", op.RegionId);
                    cmd.Parameters.AddWithValue("@utype", op.UnitType);
                    cmd.Parameters.AddWithValue("@uid", op.UnitId);
                    cmd.Parameters.AddWithValue("@time", op.TimeTargetMs);
                    cmd.Parameters.AddWithValue("@data", op.Data ?? (object)DBNull.Value);
                }

                await cmd.ExecuteNonQueryAsync();
            }

            await tx.CommitAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Lỗi khi lưu DB Batch: {ex.Message}");
            await tx.RollbackAsync();
        }
    }

    // THÊM HÀM NÀY ĐỂ DỌN DẸP
    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();

        if (_consumeTask != null)
            try
            {
                await _consumeTask;
            }
            catch
            {
                //
            }

        // Ép Channel đóng để DB loop thoát ra khỏi WaitToReadAsync
        _dbChannel.Writer.TryComplete();
        if (_dbBatchTask != null)
            try
            {
                await _dbBatchTask;
            }
            catch
            {
                //
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