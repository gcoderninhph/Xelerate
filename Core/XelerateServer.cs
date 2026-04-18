using System.Threading.Channels;
using Confluent.Kafka;
using MySqlConnector;

namespace Xelerate;

public class XelerateServer : IAsyncDisposable
{
    private readonly IConsumer<string, byte[]> _consumer;
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
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        // Gắn PooledBytesDeserializer vào Consumer
        _consumer = new ConsumerBuilder<string, byte[]>(consumerConfig)
            .Build();

        _consumer.Subscribe("XelerateServerTopic");

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = kafkaBootstrapServers,
            Acks = Acks.All,
            // retry
            EnableIdempotence = true,
            MessageSendMaxRetries = int.MaxValue,

            // timeout
            RequestTimeoutMs = 30000,

            // tránh reorder khi retry
            MaxInFlight = 5, // hoặc 1 nếu muốn cực safe
            
            // 
            LingerMs = 5,
            BatchSize = 200 * 1024
        };
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
        _dbBatchTask = Task.Run(DbBatchLoopAsync, ct);

        // 4. Khởi chạy luồng xử lý Kafka
        _consumeTask = Task.Run(() =>
        {
            while (!ct.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(ct);
                if (consumeResult == null) continue;

                var keyParts = consumeResult.Message.Key.Split('.');
                if (keyParts.Length == 2 && long.TryParse(keyParts[1], out var regionId))
                {
                    var region = GetOrAddRegion(regionId);
                    region.EnqueueEvent(consumeResult.Message.Value);
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

    private async Task DbBatchLoopAsync()
    {
        var batch = new List<DbOperation>();
        var reader = _dbChannel.Reader;

        // WaitToReadAsync không có Token. Nó chỉ trả về 'false' khi ống nước bị 
        // khóa nắp (Writer.TryComplete) VÀ bên trong đã bị hút cạn khô nước.
        while (await reader.WaitToReadAsync())
        {
            if (reader.TryRead(out var firstOp))
            {
                batch.Add(firstOp);

                // Rút ngắn thời gian ngâm Batch xuống 2 giây (chuyên nghiệp hơn 1 phút)
                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

                try
                {
                    // Tăng sức chứa mẻ lên 500 để giải phóng nhanh khi có traffic cao
                    while (batch.Count < 500)
                    {
                        if (await reader.WaitToReadAsync(timeoutCts.Token))
                        {
                            while (batch.Count < 500 && reader.TryRead(out var op))
                            {
                                batch.Add(op);
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Hết 2 giây thì đi tiếp để ghi DB, không lỗi lầm gì cả
                }

                if (batch.Count > 0)
                {
                    await SaveBatchToMySqlAsync(batch);
                    batch.Clear();
                }
            }
        }
        // Khi thoát khỏi vòng lặp này, 100% dữ liệu đã nằm an toàn dưới MySQL.
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

    public async ValueTask DisposeAsync()
    {
        // 1. Tắt luồng Consume Kafka để chặn không nhận thêm event mới
        await _cts.CancelAsync();
        if (_consumeTask != null)
            try
            {
                await _consumeTask;
            }
            catch
            {
            }

        // 2. Tắt các Region. Các luồng ngầm của Region sẽ tranh thủ 
        // đẩy NHỮNG LỆNH XÓA CUỐI CÙNG vào ống nước DB Channel.
        foreach (var region in _regions.Values)
        {
            await region.DisposeAsync();
        }

        // 3. KHÓA NẮP ỐNG NƯỚC DB
        // Báo hiệu cho DbBatchLoopAsync biết: "Không còn ai gửi data nữa, vét nốt đi!"
        _dbChannel.Writer.TryComplete();

        // 4. CHỜ LUỒNG DB VÉT CẠN VÀ GHI MẺ CUỐI XUỐNG MYSQL
        if (_dbBatchTask != null)
            try
            {
                await _dbBatchTask;
            }
            catch
            {
            }

        // 5. Lúc này 100% an toàn. Dọn dẹp nốt Kafka
        _consumer.Close();
        _consumer.Dispose();

        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
        _cts.Dispose();
    }
}