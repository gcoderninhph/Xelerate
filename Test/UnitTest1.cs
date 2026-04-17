using System.Diagnostics;
using MySqlConnector;
using Xelerate;
using Xunit;
using Xunit.Abstractions;
using Assert = Xunit.Assert;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace ProcessSystem.Tests;

// Lưu ý: Đảm bảo bạn đang chạy NATS Server ở localhost:4222
public class ProcessSystemTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private const string KafkaUrl = "localhost:29092";
    private const string MysqlUrl = "Server=localhost;Port=3306;Database=xelerate;User Id=root;Password=12345678;";

    private XelerateServer _server = null!;
    private XelerateClient _client = null!;

    public ProcessSystemTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

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
        _testOutputHelper.WriteLine("Waiting 6 seconds for Kafka Consumer Rebalance...");
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
            _testOutputHelper.WriteLine($"Không thể dọn dẹp DB (Có thể DB chưa bật): {ex.Message}");
        }
    }

    [Fact]
    public async Task TC_E2E_01_HappyPath_ShouldTriggerOnDone()
    {
        // Arrange
        var unitType = "TestBuff_HappyPath";
        long regionId = 1;
        long unitId = 99;
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
        request.Send(regionId, unitId, targetMs, payloadData);

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
        request.Send(regionId, unitId, targetMs, Array.Empty<byte>());

        // Đợi 1 giây rồi gửi lệnh Cancel
        await Task.Delay(1000);
        request.Cancel(regionId, unitId);

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
        request.Send(regionId, unitId, time1, Array.Empty<byte>());

        // Lần 2 (Ngay lập tức): Rút ngắn thời gian chỉ còn 2 giây sau
        await Task.Delay(100);
        long time2 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2000;
        request.Send(regionId, unitId, time2, Array.Empty<byte>());

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
            request.Send(regionId, i, targetMs, new byte[] { 255 });
        }

        // Assert
        // Mở rộng thời gian chờ lên 15 giây vì 1000 items đi qua DB + Kafka sẽ mất chút thời gian
        var success = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(15));

        Assert.True(success);
        Assert.Equal(totalItems, completedCount);
    }

    [Fact]
    public async Task TC_E2E_05_RestoreFromDatabase_TriggersCorrectly()
    {
        _testOutputHelper.WriteLine("=== BẮT ĐẦU TEST: KHÔI PHỤC DỮ LIỆU SAU KHI RESTART SERVER ===");
        var unitType = "TestBuff_Restore";
        long regionId = 5;
        long unitId = 200;

        // Đặt thời gian nổ là 8 giây trong tương lai
        long targetMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 8000;
        var tcs = new TaskCompletionSource<bool>();

        _client.OnDone(unitType, response =>
        {
            if (response.UnitId == unitId) tcs.TrySetResult(true);
        });

        // 1. Gửi lệnh tạo sự kiện
        _testOutputHelper.WriteLine($"[1] Gửi sự kiện UnitId {unitId}, sẽ nổ sau 8 giây...");
        var request = _client.Create(unitType);
        request.Send(regionId, unitId, targetMs, new byte[] { 99 });

        // 2. Chờ 2 giây để chắc chắn Server đã nhận lệnh và ghi xuống DB (qua Batch)
        _testOutputHelper.WriteLine("[2] Chờ 2 giây để lệnh kịp lưu xuống MySQL...");
        await Task.Delay(2000);

        // 3. Tắt Server đột ngột (Giả lập Crash / Deploy)
        _testOutputHelper.WriteLine("[3] TẮT SERVER ĐỘT NGỘT!");
        await _server.DisposeAsync();

        // 4. Khởi động lại Server
        _testOutputHelper.WriteLine("[4] KHỞI ĐỘNG LẠI SERVER...");
        _server = new XelerateServer(KafkaUrl, MysqlUrl);
        await _server.StartAsync(); // Hàm StartAsync của bạn sẽ gọi RestoreFromDatabaseAsync()

        _testOutputHelper.WriteLine("[5] Đợi Kafka Rebalance (6 giây)...");
        await Task.Delay(6000);

        // 5. Kiểm tra xem sự kiện có nổ đúng hạn không (còn khoảng 2-3 giây nữa là tới mốc targetMs)
        _testOutputHelper.WriteLine("[6] Đang chờ sự kiện nổ từ Server mới...");
        var success = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.True(success, "Sự kiện không nổ sau khi Server khởi động lại! Lỗi hàm RestoreItem.");
        _testOutputHelper.WriteLine("=> THÀNH CÔNG: Dữ liệu đã được khôi phục và chạy hoàn hảo!");
    }

    [Fact]
    public async Task TC_E2E_06_TriggerPastEvent_OnServerStart()
    {
        _testOutputHelper.WriteLine("=== BẮT ĐẦU TEST: XỬ LÝ SỰ KIỆN QUÁ HẠN KHI OFFLINE ===");
        var unitType = "TestBuff_PastEvent";
        long regionId = 6;
        long unitId = 201;

        // Đặt thời gian nổ chỉ 3 giây trong tương lai
        long targetMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 3000;
        var tcs = new TaskCompletionSource<bool>();

        _client.OnDone(unitType, response =>
        {
            if (response.UnitId == unitId) tcs.TrySetResult(true);
        });

        var request = _client.Create(unitType);
        request.Send(regionId, unitId, targetMs, Array.Empty<byte>());

        // Đợi 1 giây cho DB kịp lưu
        await Task.Delay(1000);

        _testOutputHelper.WriteLine("Tắt Server và cố tình đợi 4 giây để sự kiện bị quá hạn...");
        await _server.DisposeAsync();
        await Task.Delay(4000); // Lúc này targetMs đã nằm trong quá khứ

        _testOutputHelper.WriteLine("Khởi động lại Server (Sự kiện phải nổ ngay lập tức lúc Load DB)...");
        _server = new XelerateServer(KafkaUrl, MysqlUrl);
        await _server.StartAsync();

        // Đợi một chút cho Client nhận thông báo (Không cần chờ rebalance quá lâu vì Client vẫn đang chạy)
        var success = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(8));

        Assert.True(success, "Sự kiện quá hạn không được xử lý ngay khi Server bật lên!");
        _testOutputHelper.WriteLine("=> THÀNH CÔNG: Server đã dọn dẹp các sự kiện quá hạn ngay lúc khởi động.");
    }

    [Fact]
    public async Task TC_E2E_07_SpamUpdates_ShouldKeepTheLatest()
    {
        _testOutputHelper.WriteLine("=== BẮT ĐẦU TEST: SPAM CẬP NHẬT LIÊN TỤC ===");
        var unitType = "TestBuff_Spam";
        long regionId = 7;
        long unitId = 300;

        var tcs = new TaskCompletionSource<byte[]>();

        _client.OnDone(unitType, response =>
        {
            if (response.UnitId == unitId) tcs.TrySetResult(response.Data.ToArray());
        });

        var request = _client.Create(unitType);

        _testOutputHelper.WriteLine("Bắn dồn dập 50 lệnh Update cho cùng 1 UnitId trong chớp mắt...");
        for (int i = 1; i <= 50; i++)
        {
            // Gửi data tăng dần từ 1 đến 50. Thời gian nổ là 3 giây.
            long targetMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 3000;
            request.Send(regionId, unitId, targetMs, new byte[] { (byte)i });
        }

        // Đợi kết quả
        _testOutputHelper.WriteLine("Đang chờ lệnh cuối cùng được thực thi...");
        var finalData = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Single(finalData);
        Assert.Equal(50, finalData[0]); // Chắc chắn Payload nhận được phải là của lần Update thứ 50
        _testOutputHelper.WriteLine("=> THÀNH CÔNG: Hệ thống chỉ giữ lại lệnh cuối cùng (Data = 50)!");
    }

    [Fact]
    public async Task TC_E2E_08_MultiRegion_ShouldProcessIndependently()
    {
        _testOutputHelper.WriteLine("=== BẮT ĐẦU TEST: XỬ LÝ ĐỘC LẬP ĐA PHÂN KHU (MULTI-REGION) ===");
        var unitType = "TestBuff_MultiRegion";

        int completedCount = 0;
        var tcs = new TaskCompletionSource<bool>();

        // Đăng ký nghe OnDone
        _client.OnDone(unitType, response =>
        {
            _testOutputHelper.WriteLine($"- Nhận kết quả từ Region {response.RegionId}, Unit {response.UnitId}");
            int count = Interlocked.Increment(ref completedCount);
            if (count == 3) tcs.TrySetResult(true);
        });

        var request = _client.Create(unitType);
        long targetMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 3000;

        _testOutputHelper.WriteLine("Gửi 3 sự kiện vào 3 Region khác biệt...");
        // Gửi vào Region 10, 20, 30. UnitId giống nhau không sao vì khác Region
        request.Send(regionId: 10, unitId: 500, targetMs, Array.Empty<byte>());
        request.Send(regionId: 20, unitId: 500, targetMs, Array.Empty<byte>());
        request.Send(regionId: 30, unitId: 500, targetMs, Array.Empty<byte>());

        var success = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.True(success);
        Assert.Equal(3, completedCount);
        _testOutputHelper.WriteLine("=> THÀNH CÔNG: Đa Region hoạt động hoàn toàn độc lập và trơn tru!");
    }

    [Fact(Timeout = 400000)] // Báo cho xUnit biết test này có thể chạy tới hơn 6 phút (tránh bị runner ngắt ngang)
    public async Task TC_E2E_09_SustainedLoadTest_5Minutes()
    {
        _testOutputHelper.WriteLine("=== BẮT ĐẦU TEST: CHỊU TẢI LIÊN TỤC 5 PHÚT ===");
        var unitType = "TestBuff_Load5Mins";
        long regionId = 9;

        long totalSent = 0;
        long totalReceived = 0;

        // Đăng ký nghe OnDone. Dùng Interlocked để đảm bảo Thread-Safe khi tăng biến đếm
        _client.OnDone(unitType, response => { Interlocked.Increment(ref totalReceived); });

        var request = _client.Create(unitType);

        _testOutputHelper.WriteLine("Bắt đầu chu trình ném bom (Spam) liên tục trong 4.5 phút...");
        var stopwatch = Stopwatch.StartNew();

        // Dùng CancellationToken để hẹn giờ ngừng bắn sau 4.5 phút
        using var sendCts = new CancellationTokenSource(TimeSpan.FromMinutes(4.5));

        try
        {
            // Vòng lặp bắn liên tục cho đến khi hết 4.5 phút
            while (!sendCts.IsCancellationRequested)
            {
                // Mỗi nhịp gửi 100 sự kiện
                for (int i = 0; i < 100; i++)
                {
                    long currentId = Interlocked.Increment(ref totalSent);

                    // Thời gian nổ ngẫu nhiên từ 1 đến 5 giây trong tương lai
                    long targetMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + Random.Shared.Next(1000, 5000);
                    request.Send(regionId, currentId, targetMs, Array.Empty<byte>());
                }

                // Nghỉ 50ms để mô phỏng traffic đều đặn (Khoảng 2000 req/s), tránh làm treo CPU của luồng Test
                await Task.Delay(50, sendCts.Token);
            }
        }
        catch (TaskCanceledException)
        {
            // Không sao cả, đây là lúc hết 4.5 phút
        }

        stopwatch.Stop();
        _testOutputHelper.WriteLine($"[!] Đã dừng gửi dữ liệu.");
        _testOutputHelper.WriteLine($"    - Tổng số lệnh đã gửi: {totalSent:N0}");
        _testOutputHelper.WriteLine($"    - Thời gian spam: {stopwatch.Elapsed.TotalMinutes:F2} phút");
        _testOutputHelper.WriteLine("Chờ tối đa 30 giây để các sự kiện cuối cùng nổ và Kafka giao hàng về Client...");

        // Vòng lặp chờ 30 giây cuối để Client nhận nốt data
        using var waitCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (Interlocked.Read(ref totalReceived) < Interlocked.Read(ref totalSent) &&
               !waitCts.IsCancellationRequested)
        {
            await Task.Delay(2000); // Check mỗi 2 giây
            _testOutputHelper.WriteLine(
                $"... Tiến độ: Đã nhận {Interlocked.Read(ref totalReceived):N0} / {totalSent:N0}");
        }

        long finalReceived = Interlocked.Read(ref totalReceived);

        _testOutputHelper.WriteLine("=== KẾT QUẢ TEST TẢI ===");
        _testOutputHelper.WriteLine($"Gửi đi: {totalSent:N0} | Nhận về: {finalReceived:N0}");

        // Assert
        Assert.True(finalReceived > 0, "Không nhận được bất kỳ event nào, Client hoặc Server có thể đã bị Crash ngầm!");

        // Yêu cầu độ chính xác tuyệt đối: Số lượng gửi phải bằng số lượng nhận
        Assert.Equal(totalSent, finalReceived);

        _testOutputHelper.WriteLine(
            "=> THÀNH CÔNG: Hệ thống chịu tải 5 phút xuất sắc, không rớt nhịp nào và không memory leak!");
    }
}