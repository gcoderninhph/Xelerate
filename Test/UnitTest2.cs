using System.Collections.Concurrent;
using System.Text;
using Xelerate;
using Xunit;
using Xunit.Abstractions;
using Assert = Xunit.Assert;

namespace ProcessSystem.Tests;

// YÊU CẦU: Đảm bảo NATS Server đang chạy ở localhost:4222 trước khi run test.
public class UnitTest2 : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private const string NatsUrl = "nats://127.0.0.1:4222";

    private Xelerate.XelerateServer _server = null!;
    private XelerateClient _client = null!;
    private IXelerateRequest _request = null!;

    // Tạo UnitType và RegionId ngẫu nhiên cho mỗi bài test để không bị đụng độ dữ liệu cũ
    private string _testUnitType = null!;
    private long _testRegionId;

    public UnitTest2(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    // Chạy TRƯỚC mỗi bài Test
    public async Task InitializeAsync()
    {
        _testUnitType = $"Hero_{Guid.NewGuid():N}";
        _testRegionId = Random.Shared.Next(1000, 9999);

        // 1. Khởi động Server
        _server = new Xelerate.XelerateServer(NatsUrl);
        _ = _server.StartAsync();

        // 2. Khởi động Client
        _client = new XelerateClient(NatsUrl);
        await _client.StartAsync();

        _request = _client.Create(_testUnitType);
    }

    // Chạy SAU mỗi bài Test (Dọn dẹp)
    public async Task DisposeAsync()
    {
        if (_client != null) await _client.DisposeAsync();
        if (_server != null) await _server.DisposeAsync();
    }

    // ==========================================
    // NHÓM 1: NORMAL FLOW & LOGIC CƠ BẢN
    // ==========================================

    [Fact(DisplayName = "1. Normal Flow: Gửi Update và chờ Done thành công")]
    public async Task Case01_NormalFlow_ShouldTriggerDone()
    {
        long unitId = 1;
        var tcs = new TaskCompletionSource<bool>();

        _client.OnDone(_testUnitType, res =>
        {
            if (res.UnitId == unitId) tcs.TrySetResult(true);
        });

        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1000;
        await _request.Send(_testRegionId, unitId, 1, targetTimeMs, ReadOnlyMemory<byte>.Empty);

        var completedTask = await Task.WhenAny(tcs.Task, Task.Delay(3000));
        Assert.True(completedTask == tcs.Task, "Timeout: Không nhận được sự kiện OnDone.");
    }

    [Fact(DisplayName = "2. Require Recovery: Ping Unit lạ phải đòi Require và tự phục hồi")]
    public async Task Case02_RequireRecovery_ShouldRecoverAndDone()
    {
        long unitId = 2;
        var requireTcs = new TaskCompletionSource<bool>();
        var doneTcs = new TaskCompletionSource<bool>();

        _client.OnRequire(_testUnitType, res =>
        {
            if (res.UnitId == unitId) requireTcs.TrySetResult(true);
            long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 500;
            return new XelerateData(res.UnitId, res.Version, targetTimeMs, ReadOnlyMemory<byte>.Empty);
        });

        _client.OnDone(_testUnitType, res =>
        {
            if (res.UnitId == unitId) doneTcs.TrySetResult(true);
        });

        await _request.Ping(_testRegionId, unitId, 1);

        Assert.True(await Task.WhenAny(requireTcs.Task, Task.Delay(2000)) == requireTcs.Task,
            "Không trigger OnRequire");
        Assert.True(await Task.WhenAny(doneTcs.Task, Task.Delay(2000)) == doneTcs.Task,
            "Không trigger OnDone sau khi Require");
    }

    [Fact(DisplayName = "3. Version Conflict: Ping sai Version phải báo NeedUpdate")]
    public async Task Case03_VersionConflict_ShouldTriggerNeedUpdate()
    {
        long unitId = 3;
        var needUpdateTcs = new TaskCompletionSource<bool>();

        _client.OnRequire(_testUnitType, res =>
        {
            if (res.UnitId == unitId && res.Version == 2) needUpdateTcs.TrySetResult(true);
            return new XelerateData(res.UnitId, res.Version, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 500,
                ReadOnlyMemory<byte>.Empty);
        });

        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 5000;
        await _request.Send(_testRegionId, unitId, 1, targetTimeMs, ReadOnlyMemory<byte>.Empty);
        await Task.Delay(200);

        // Cố tình Ping lệch version
        await _request.Ping(_testRegionId, unitId, 2);

        var completedTask = await Task.WhenAny(needUpdateTcs.Task, Task.Delay(3000));
        Assert.True(completedTask == needUpdateTcs.Task, "Server không phát hiện lệch version.");
    }

    [Fact(DisplayName = "4. Cancellation: Hủy bỏ Update không được phép nổ Done")]
    public async Task Case04_Cancellation_ShouldNeverTriggerDone()
    {
        long unitId = 4;
        bool doneTriggered = false;

        _client.OnDone(_testUnitType, res =>
        {
            if (res.UnitId == unitId) doneTriggered = true;
        });

        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1500;
        await _request.Send(_testRegionId, unitId, 1, targetTimeMs, ReadOnlyMemory<byte>.Empty);

        await Task.Delay(200);
        await _request.Cancel(_testRegionId, unitId, 1);

        // Chờ quá target time
        await Task.Delay(2000);
        Assert.False(doneTriggered, "Lỗi: Sự kiện Done vẫn nổ dù đã Cancel.");
    }

    // ==========================================
    // NHÓM 2: TIME-BASED EDGE CASES
    // ==========================================

    [Fact(DisplayName = "5. Instant Done: Gửi time trong quá khứ phải nổ Done ngay lập tức")]
    public async Task Case05_InstantDone_PastTimeTarget()
    {
        long unitId = 5;
        var tcs = new TaskCompletionSource<bool>();

        _client.OnDone(_testUnitType, res =>
        {
            if (res.UnitId == unitId) tcs.TrySetResult(true);
        });

        // Time target cách đây 10 giây
        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 10000;

        var watch = System.Diagnostics.Stopwatch.StartNew();
        await _request.Send(_testRegionId, unitId, 1, targetTimeMs, ReadOnlyMemory<byte>.Empty);

        var completedTask = await Task.WhenAny(tcs.Task, Task.Delay(2000));
        watch.Stop();

        Assert.True(completedTask == tcs.Task, "Không nhận được Done.");
        Assert.True(watch.ElapsedMilliseconds < 500,
            $"Xử lý quá chậm ({watch.ElapsedMilliseconds}ms) cho task hết hạn.");
    }

    [Fact(DisplayName = "6. Ping Expiration: Quên Ping sau 10s Server phải xóa ngầm")]
    public async Task Case06_PingExpiration_ShouldSilentlyDelete()
    {
        long unitId = 6;
        var requireTcs = new TaskCompletionSource<bool>();

        _client.OnRequire(_testUnitType, res =>
        {
            if (res.UnitId == unitId) requireTcs.TrySetResult(true);
            return new XelerateData(res.UnitId, res.Version, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1000,
                ReadOnlyMemory<byte>.Empty);
        });

        // Đặt target là 20s (để không bị nổ Done)
        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 20000;
        await _request.Send(_testRegionId, unitId, 1, targetTimeMs, ReadOnlyMemory<byte>.Empty);

        // Đợi 11 giây (Vượt quá 10s timeout hardcode của Server)
        await Task.Delay(11000);

        // Gửi Ping, lúc này Server đã xóa -> phải báo Require
        await _request.Ping(_testRegionId, unitId, 1);

        var completedTask = await Task.WhenAny(requireTcs.Task, Task.Delay(3000));
        Assert.True(completedTask == requireTcs.Task, "Server không xóa ngầm Unit sau 10s.");
    }

    // ==========================================
    // NHÓM 3: PAYLOAD & CONCURRENCY
    // ==========================================

    [Fact(DisplayName = "7. Large Payload: Gói dữ liệu vượt quá 50KB")]
    public async Task Case07_LargePayload_ExceedsMaxBatchSize()
    {
        long unitId = 7;
        var tcs = new TaskCompletionSource<bool>();
        int payloadSize = 60 * 1024; // 60 KB
        var largeData = new byte[payloadSize];
        Random.Shared.NextBytes(largeData); // Điền data rác

        _client.OnDone(_testUnitType, res =>
        {
            if (res.UnitId == unitId && res.Data.Length == payloadSize)
                tcs.TrySetResult(true);
        });

        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 500;
        await _request.Send(_testRegionId, unitId, 1, targetTimeMs, largeData);

        var completedTask = await Task.WhenAny(tcs.Task, Task.Delay(3000));
        Assert.True(completedTask == tcs.Task, "Gửi file >50KB thất bại hoặc mất data.");
    }

    [Fact(DisplayName = "8. Empty Data: Gửi dữ liệu rỗng không được crash")]
    public async Task Case08_EmptyData_ShouldCompleteNormally()
    {
        long unitId = 8;
        var tcs = new TaskCompletionSource<bool>();

        _client.OnDone(_testUnitType, res =>
        {
            if (res.UnitId == unitId && res.Data.Length == 0) tcs.TrySetResult(true);
        });

        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 500;
        await _request.Send(_testRegionId, unitId, 1, targetTimeMs, ReadOnlyMemory<byte>.Empty);

        Assert.True(await Task.WhenAny(tcs.Task, Task.Delay(2000)) == tcs.Task, "Lỗi khi xử lý Data rỗng.");
    }

    [Fact(DisplayName = "9. Region Isolation: Độc lập UnitId giữa các Region")]
    public async Task Case09_RegionIsolation_IndependentUnitIds()
    {
        long unitId = 9;
        long regionA = 100;
        long regionB = 200;

        var tcsA = new TaskCompletionSource<bool>();
        var tcsB = new TaskCompletionSource<bool>();

        _client.OnDone(_testUnitType, res =>
        {
            if (res.UnitId == unitId)
            {
                var dataStr = Encoding.UTF8.GetString(res.Data.Span);
                if (res.RegionId == regionA && dataStr == "DATA_A") tcsA.TrySetResult(true);
                if (res.RegionId == regionB && dataStr == "DATA_B") tcsB.TrySetResult(true);
            }
        });

        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1000;
        await _request.Send(regionA, unitId, 1, targetTimeMs, "DATA_A"u8.ToArray());
        await _request.Send(regionB, unitId, 1, targetTimeMs, "DATA_B"u8.ToArray());

        await Task.WhenAll(
            Task.WhenAny(tcsA.Task, Task.Delay(3000)),
            Task.WhenAny(tcsB.Task, Task.Delay(3000))
        );

        Assert.True(tcsA.Task.IsCompleted && tcsB.Task.IsCompleted, "Dữ liệu giữa các Region bị ghi đè lẫn nhau.");
    }

    [Fact(DisplayName = "10. Race Condition: Spam Update liên tục chỉ nổ Done 1 lần")]
    public async Task Case10_RaceCondition_SpamUpdate()
    {
        long unitId = 10;
        int doneCount = 0;

        _client.OnDone(_testUnitType, res =>
        {
            if (res.UnitId == unitId) Interlocked.Increment(ref doneCount);
        });

        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2000;
        var spamTasks = new List<Task>();

        // Bắn song song 20 request Update vào cùng 1 UnitId
        for (int i = 0; i < 20; i++)
        {
            spamTasks.Add(_request.Send(_testRegionId, unitId, 1, targetTimeMs, ReadOnlyMemory<byte>.Empty));
        }

        await Task.WhenAll(spamTasks);

        // Chờ qua target time một chút
        await Task.Delay(3500);

        Assert.Equal(1, doneCount); // Chỉ được phép nổ Done ĐÚNG 1 LẦN
    }

    [Fact(DisplayName = "11. Stress Test: Bơm 5000 items để test Batching limits")]
    public async Task Case11_StressTest_Batching5000Items()
    {
        int totalRequests = 5000;
        int completedCount = 0;

        _client.OnDone(_testUnitType, _ => { Interlocked.Increment(ref completedCount); });

        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2000;
        var dummyData = "Data"u8.ToArray();

        // Bắn nhanh 5000 request (Task.Run để không block thread chính)
        _ = Task.Run(async () =>
        {
            for (long i = 1; i <= totalRequests; i++)
            {
                await _request.Send(_testRegionId, i, 1, targetTimeMs, dummyData);
            }
        });

        // Timeout tối đa 10 giây cho toàn bộ 5000 requests
        int waitLoop = 0;
        while (completedCount < totalRequests && waitLoop < 20)
        {
            await Task.Delay(500);
            waitLoop++;
        }

        Assert.Equal(totalRequests, completedCount); // Phải gom đủ 5000 items, không sót
    }
    
    [Fact(DisplayName = "12. Performance: Thông lượng cao (Throughput/TPS) - Xử lý 100,000 requests")]
    public async Task Case12_HighThroughput_100kRequests()
    {
        int totalRequests = 100_000;
        int completedCount = 0;
        var tcs = new TaskCompletionSource<bool>();

        _client.OnDone(_testUnitType, _ =>
        {
            var current = Interlocked.Increment(ref completedCount);
            if (current == totalRequests) tcs.TrySetResult(true);
        });

        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2000;
        
        var watch = System.Diagnostics.Stopwatch.StartNew();

        // Bắn 100,000 requests nhanh nhất có thể
        _ = Task.Run(() =>
        {
            for (long i = 1; i <= totalRequests; i++)
            {
                // Fire and forget để ép Channel hoạt động hết công suất
                _ = _request.Send(_testRegionId, i, 1, targetTimeMs, ReadOnlyMemory<byte>.Empty);
            }
        });

        // Cho phép tối đa 30 giây để xử lý 100k request
        var completedTask = await Task.WhenAny(tcs.Task, Task.Delay(30000));
        watch.Stop();

        Assert.True(completedTask == tcs.Task, $"Chỉ hoàn thành {completedCount}/{totalRequests} trong 30s.");
        
        // In ra TPS (Transactions Per Second)
        double tps = totalRequests / watch.Elapsed.TotalSeconds;
        _testOutputHelper.WriteLine($"\n[Performance] Đã xử lý 100,000 items trong {watch.ElapsedMilliseconds}ms. TPS: {tps:N0} msg/sec");
    }

    [Fact(DisplayName = "13. Performance: Đa luồng (Concurrency) - 100 Region khác nhau")]
    public async Task Case13_HighConcurrency_100Regions()
    {
        int numRegions = 100;
        int itemsPerRegion = 500;
        int totalRequests = numRegions * itemsPerRegion;
        int completedCount = 0;
        var tcs = new TaskCompletionSource<bool>();

        _client.OnDone(_testUnitType, _ =>
        {
            var current = Interlocked.Increment(ref completedCount);
            if (current == totalRequests) tcs.TrySetResult(true);
        });

        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1000;
        
        // Chạy song song bắn dữ liệu vào 100 Region (Kiểm tra xem Dictionary Server có bị lock hay crash không)
        Parallel.For(0, numRegions, regionIndex =>
        {
            long regionId = 10000 + regionIndex;
            for (long i = 1; i <= itemsPerRegion; i++)
            {
                _ = _request.Send(regionId, i, 1, targetTimeMs, ReadOnlyMemory<byte>.Empty);
            }
        });

        var completedTask = await Task.WhenAny(tcs.Task, Task.Delay(20000));

        Assert.True(completedTask == tcs.Task, $"Lỗi xử lý đa Region. Chỉ hoàn thành {completedCount}/{totalRequests}");
    }

    [Fact(DisplayName = "14. Performance: Độ trễ vòng lặp (Round-trip Latency)")]
    public async Task Case14_RoundTripLatency_AverageMeasurement()
    {
        int totalRequests = 1000;
        var latencies = new ConcurrentBag<long>();
        var tcs = new TaskCompletionSource<bool>();
        int completedCount = 0;

        _client.OnDone(_testUnitType, res =>
        {
            // Lấy thời gian gửi từ Data (do ta nhét vào lúc Send)
            var sendTime = BitConverter.ToInt64(res.Data.Span);
            var latency = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - sendTime;
            latencies.Add(latency);

            if (Interlocked.Increment(ref completedCount) == totalRequests)
                tcs.TrySetResult(true);
        });

        // Gửi các gói tin hết hạn ngay lập tức (past time) để đo tốc độ mạng và xử lý thuần túy
        for (long i = 1; i <= totalRequests; i++)
        {
            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var timeBytes = BitConverter.GetBytes(now);
            
            // Gửi đi ngay lập tức (TimeTargetMs = now - 1000)
            await _request.Send(_testRegionId, i, 1, now - 1000, timeBytes);
            
            // Đợi 2ms giữa mỗi request để mô phỏng real-world traffic
            await Task.Delay(2); 
        }

        await Task.WhenAny(tcs.Task, Task.Delay(10000));
        
        Assert.Equal(totalRequests, completedCount);

        double avgLatency = latencies.Average();
        long maxLatency = latencies.Max();
        long minLatency = latencies.Min();

        _testOutputHelper.WriteLine($"\n[Latency] Min: {minLatency}ms | Max: {maxLatency}ms | Avg: {avgLatency:N2}ms");
        Assert.True(avgLatency < 50, $"Cảnh báo hiệu suất: Độ trễ trung bình quá cao ({avgLatency}ms) cho xử lý tức thời.");
    }

    [Fact(DisplayName = "15. Performance: Memory Pool Stability (Test dồn ép Object Pool/Array Pool)")]
    public async Task Case15_MemoryPoolStability_SpamPings()
    {
        // Ping là lệnh nhỏ nhất nhưng lại gọi ObjectPool (Get/Return) liên tục.
        // Test này bắn 200,000 lệnh Ping để xem Pool có tái sử dụng tốt không hay gây tràn RAM.
        int totalPings = 200_000;
        
        var watch = System.Diagnostics.Stopwatch.StartNew();
        
        // Ép dọn dẹp bộ nhớ trước khi test để đo cho chính xác
        GC.Collect();
        long memoryBefore = GC.GetTotalMemory(true);

        _ = Task.Run(() =>
        {
            for (long i = 1; i <= totalPings; i++)
            {
                _ = _request.Ping(_testRegionId, i, 1);
            }
        });

        // Test này không đợi OnDone vì Ping không nổ OnDone. 
        // Chúng ta chỉ đợi 5 giây cho Server hấp thụ hết 200k tin nhắn qua Channel.
        await Task.Delay(5000); 
        
        GC.Collect();
        long memoryAfter = GC.GetTotalMemory(true);
        watch.Stop();

        double memoryDiffMb = (memoryAfter - memoryBefore) / (1024.0 * 1024.0);
        
        _testOutputHelper.WriteLine($"\n[Memory] Sau khi xử lý {totalPings} pings: Tăng/Giảm {memoryDiffMb:N2} MB");

        // Nếu ObjectPool và ArrayPool hoạt động đúng, RAM thay đổi sau khi GC dọn dẹp sẽ rất thấp (thường < 15MB)
        // Nếu bạn quên gọi Pool.Return(), mức tiêu thụ RAM có thể vọt lên hàng trăm MB.
        Assert.True(memoryDiffMb < 50, $"Rò rỉ bộ nhớ tiềm ẩn! Hệ thống tiêu thụ thêm {memoryDiffMb} MB không giải phóng được.");
    }
}