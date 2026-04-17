using Confluent.Kafka;
using Confluent.Kafka.Admin;
using MySqlConnector;
using Xelerate;
using Xunit;
using Assert = Xunit.Assert;

namespace ProcessSystem.Tests;

// Lưu ý: Đảm bảo bạn đang chạy NATS Server ở localhost:4222
public class ProcessSystemTests : IAsyncLifetime
{
    private const string KafkaUrl = "localhost:29092";
    private const string MysqlUrl = "Server=localhost;Port=3306;Database=xelerate;User Id=root;Password=12345678;";

    private XelerateServer _server = null!;
    private XelerateClient _client = null!;

    public async Task InitializeAsync()
    {
        // 1. Dọn dẹp Database trước khi chạy test để tránh xung đột dữ liệu cũ
        await CleanupDatabaseAsync();

        // 2. Khởi tạo Server (Bây giờ dùng await an toàn vì StartAsync đã dùng Task.Run bên trong)
        _server = new XelerateServer(KafkaUrl, MysqlUrl);
        await _server.EnsureTopicsExistAsync();
        await _server.StartAsync();

        // 3. Khởi tạo Client
        // Mỗi test run tạo một GroupId ngẫu nhiên để ép Kafka đọc từ đầu, không dính state cũ
        string uniqueGroupId = $"test-client-group-{Guid.NewGuid()}";
        _client = new XelerateClient(KafkaUrl, uniqueGroupId);
        await _client.EnsureTopicsExistAsync();
        _client.StartAsync();

        // 4. CHỜ KAFKA REBALANCE: Cực kỳ quan trọng (Tối thiểu 5-7 giây)
        // Kafka cần vài giây để gán Partition cho Consumer mới. 
        // Nếu bắn lệnh Send ngay lúc này, dữ liệu sẽ vào Topic nhưng Client chưa kịp nghe.
        Console.WriteLine("Waiting 6 seconds for Kafka Consumer Rebalance...");
        await Task.Delay(6000);
    }

    public async Task DisposeAsync()
    {
        if (_client != null) await _client.DisposeAsync();
        if (_server != null) await _server.DisposeAsync();
    }

    private async Task CleanupDatabaseAsync()
    {
        try
        {
            await using var conn = new MySqlConnection(MysqlUrl);
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            // Xóa toàn bộ bảng để khởi tạo lại sạch sẽ khi Server.StartAsync chạy
            cmd.CommandText = "DROP TABLE IF EXISTS `XelerateItems`;";
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Không thể dọn dẹp DB (Có thể DB chưa bật): {ex.Message}");
        }
    }

    [Fact]
    public async Task TC_E2E_01_HappyPath_ShouldTriggerOnDone()
    {
        // Arrange
        var unitType = "TestBuff_HappyPath";
        long regionId = 1;
        long unitId = 99;
        int version = 1;
        byte[] payloadData = { 1, 2, 3 };

        // Target: 2 giây trong tương lai
        long targetMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2000;
        var tcs = new TaskCompletionSource<XelerateResponse>();

        _client.OnDone(unitType, response =>
        {
            if (response.UnitId == unitId)
            {
                tcs.TrySetResult(response);
            }
        });

        // Act
        var request = _client.Create(unitType);
        request.Send(regionId, unitId, version, targetMs, payloadData);

        // Assert
        // Đặt timeout 15 giây để phòng ngừa độ trễ mạng Kafka
        var result = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(15));

        Assert.Equal(regionId, result.RegionId);
        Assert.Equal(unitId, result.UnitId);
        Assert.True(result.Data.Span.SequenceEqual(payloadData));
    }

    [Fact]
    public async Task TC_E2E_02_CancelFlow_ShouldNeverTriggerOnDone()
    {
        // Arrange
        var unitType = "TestBuff_Cancel";
        long regionId = 2;
        long unitId = 100;

        // Target: 4 giây trong tương lai
        long targetMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 4000;
        bool onDoneCalled = false;

        _client.OnDone(unitType, response =>
        {
            if (response.UnitId == unitId) onDoneCalled = true;
        });

        // Act
        var request = _client.Create(unitType);
        request.Send(regionId, unitId, 1, targetMs, Array.Empty<byte>());

        // Đợi 1 giây rồi gửi lệnh Cancel
        await Task.Delay(1000);
        request.Cancel(regionId, unitId, 1);

        // Assert
        // Đợi thêm 4.5 giây (để chắc chắn đã vượt qua mốc TimeTargetMs)
        await Task.Delay(4500);

        Assert.False(onDoneCalled, "Lệnh Cancel bị lỗi: OnDone vẫn bị gọi!");
    }

    [Fact]
    public async Task TC_E2E_03_UpdateOverride_ShouldTriggerAtNewTime()
    {
        // Arrange
        var unitType = "TestBuff_Override";
        long regionId = 3;
        long unitId = 101;
        var tcs = new TaskCompletionSource<long>();

        _client.OnDone(unitType, response =>
        {
            if (response.UnitId == unitId)
            {
                tcs.TrySetResult(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            }
        });

        // Act
        var request = _client.Create(unitType);

        // Lần 1: Lên lịch 6 giây sau
        long time1 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 6000;
        request.Send(regionId, unitId, 1, time1, Array.Empty<byte>());

        // Lần 2 (Ngay lập tức): Rút ngắn thời gian chỉ còn 2 giây sau
        await Task.Delay(100);
        long time2 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2000;
        request.Send(regionId, unitId, 2, time2, Array.Empty<byte>());

        // Assert
        // Phải nhận được kết quả trong tối đa 5 giây (vì đã update xuống 2 giây)
        var actualTriggerTime = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Kiểm tra logic thời gian
        Assert.True(actualTriggerTime >= time2, "Nổ quá sớm so với Target 2!");
        Assert.True(actualTriggerTime < time1, "Nổ quá trễ, có vẻ lệnh Update thứ 2 không có tác dụng!");
    }

    [Fact]
    public async Task TC_E2E_04_HighThroughput_BatchingCheck()
    {
        // Arrange
        var unitType = "TestBuff_Batching";
        long regionId = 4;
        int totalItems = 1000; // Gửi 1000 items

        int completedCount = 0;
        var tcs = new TaskCompletionSource<bool>();

        // Target: 2 giây trong tương lai cho toàn bộ 1000 items
        long targetMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2000;

        _client.OnDone(unitType, response =>
        {
            var count = Interlocked.Increment(ref completedCount);
            if (count == totalItems)
            {
                tcs.TrySetResult(true);
            }
        });

        // Act
        var request = _client.Create(unitType);
        for (int i = 1; i <= totalItems; i++)
        {
            request.Send(regionId, i, 1, targetMs, new byte[] { 255 });
        }

        // Assert
        // Mở rộng thời gian chờ lên 15 giây vì 1000 items đi qua DB + Kafka sẽ mất chút thời gian
        var success = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(15));

        Assert.True(success);
        Assert.Equal(totalItems, completedCount);
    }
}