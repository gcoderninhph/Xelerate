using Xelerate;
using Xunit;
using Assert = Xunit.Assert;

namespace ProcessSystem.Tests;

// Lưu ý: Đảm bảo bạn đang chạy NATS Server ở localhost:4222
public class ProcessSystemTests : IAsyncLifetime
{
    private const string KafkaUrl = "localhost:29092";
    private const string TestUnitType = "TestHero";
    private const long TestRegionId = 9999;

    private XelerateServer _server = null!;
    private XelerateClient _client = null!;
    private IXelerateRequest _request = null!;

    // Chạy TRƯỚC mỗi bài Test
    public async Task InitializeAsync()
    {
        // 1. Khởi động Server
        _server = new XelerateServer(KafkaUrl);
        _server.StartAsync();

        // 2. Khởi động Client
        _client = new XelerateClient(KafkaUrl);
        _client.StartAsync();

        _request = _client.Create(TestUnitType);
    }

    // Chạy SAU mỗi bài Test (Dọn dẹp)
    public async Task DisposeAsync()
    {
        if (_client != null) await _client.DisposeAsync();
        if (_server != null) await _server.DisposeAsync();
    }

    [Fact]
    public async Task SendUpdate_WaitUntilTargetTime_ShouldTriggerOnDone()
    {
        // Arrange
        long unitId = 1;
        var tcs = new TaskCompletionSource<bool>();

        _client.OnDone(TestUnitType, res =>
        {
            if (res.UnitId == unitId)
            {
                tcs.TrySetResult(true);
            }
        });

        // Act
        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1000; // 1 giây
        _request.Send(TestRegionId, unitId, 1, targetTimeMs, ReadOnlyMemory<byte>.Empty);

        // Assert
        var delayTask = Task.Delay(3000); // Timeout 3 giây
        var completedTask = await Task.WhenAny(tcs.Task, delayTask);

        Assert.True(completedTask == tcs.Task, "Sự kiện OnDone không được gọi trong thời gian mong đợi (Timeout).");
    }

    [Fact]
    public async Task PingMissingUnit_ShouldTriggerOnRequire_AndThenComplete()
    {
        // Arrange
        long unitId = 2;
        var requireTcs = new TaskCompletionSource<bool>();
        var doneTcs = new TaskCompletionSource<bool>();

        // Lắng nghe Require
        _client.OnRequire(TestUnitType, res =>
        {
            if (res.UnitId == unitId) requireTcs.TrySetResult(true);

            // Tự động trả về dữ liệu phục hồi
            long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 500;
            return new XelerateData(res.UnitId, res.Version, targetTimeMs, ReadOnlyMemory<byte>.Empty);
        });

        // Lắng nghe Done
        _client.OnDone(TestUnitType, res =>
        {
            if (res.UnitId == unitId) doneTcs.TrySetResult(true);
        });

        // Act: Chỉ gửi Ping, không có data tồn tại trên Server -> Server phải đòi (Require)
        _request.Ping(TestRegionId, unitId, 1);

        // Assert
        var delayTask = Task.Delay(3000);

        var requireCompleted = await Task.WhenAny(requireTcs.Task, delayTask);
        Assert.True(requireCompleted == requireTcs.Task, "Không nhận được sự kiện OnRequire từ Server.");

        var doneCompleted = await Task.WhenAny(doneTcs.Task, delayTask);
        Assert.True(doneCompleted == doneTcs.Task, "Không nhận được sự kiện OnDone sau khi đã phục hồi dữ liệu.");
    }

    [Fact]
    public async Task CancelProcessing_ShouldNeverTriggerOnDone()
    {
        // Arrange
        long unitId = 3;
        bool doneTriggered = false;

        _client.OnDone(TestUnitType, res =>
        {
            if (res.UnitId == unitId) doneTriggered = true;
        });

        // Act
        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2000; // Đích đến là 2 giây
        _request.Send(TestRegionId, unitId, 1, targetTimeMs, ReadOnlyMemory<byte>.Empty);

        // Chờ nửa giây rồi hủy
        await Task.Delay(500);
        _request.Cancel(TestRegionId, unitId, 1);

        // Chờ vượt qua targetTimeMs xem Done có nổ không
        await Task.Delay(2500);

        // Assert
        Assert.False(doneTriggered, "Lỗi: Sự kiện OnDone vẫn bị gọi mặc dù đã gửi lệnh Cancel.");
    }

    [Fact]
    public async Task VersionConflict_ShouldTriggerNeedUpdate()
    {
        // Arrange
        long unitId = 4;
        var needUpdateTcs = new TaskCompletionSource<bool>();

        // NeedUpdate chạy chung luồng OnRequire
        _client.OnRequire(TestUnitType, res =>
        {
            if (res.UnitId == unitId && res.Version == 2)
            {
                needUpdateTcs.TrySetResult(true);
            }

            return new XelerateData(res.UnitId, res.Version, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1000,
                ReadOnlyMemory<byte>.Empty);
        });

        // Act: Gửi Update Version 1
        long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 5000;
        _request.Send(TestRegionId, unitId, 1, targetTimeMs, ReadOnlyMemory<byte>.Empty);

        await Task.Delay(200);

        // Gửi Ping lệch Version 2
        _request.Ping(TestRegionId, unitId, 2);

        // Assert
        var delayTask = Task.Delay(3000);
        var completedTask = await Task.WhenAny(needUpdateTcs.Task, delayTask);

        Assert.True(completedTask == needUpdateTcs.Task,
            "Server không phát hiện ra lệch version để báo NeedUpdate (Require).");
    }
}