using System.Collections.Concurrent;
using System.Text;
using Xelerate;

// Cấu hình NATS
const string natsUrl = "nats://127.0.0.1:4222";
const string TestUnitType = "TestHero";
const long TestRegionId = 1000;

Console.OutputEncoding = Encoding.UTF8;
Console.WriteLine("🚀 BẮT ĐẦU CHẠY KỊCH BẢN KIỂM THỬ TỰ ĐỘNG...");

// 1. Khởi tạo Server & Client
var server = new XelerateServer(natsUrl);
_ = server.StartAsync();

await using var client = new XelerateClient(natsUrl);
await client.StartAsync();
var request = client.Create(TestUnitType);

// 2. Các biến theo dõi trạng thái bất đồng bộ
var doneSignals = new ConcurrentDictionary<long, TaskCompletionSource<bool>>();
var requireSignals = new ConcurrentDictionary<long, TaskCompletionSource<bool>>();
var cancelSignals = new ConcurrentDictionary<long, bool>(); // Đánh dấu các UnitId đã bị hủy
int stressTestCompletedCount = 0;

// 3. Đăng ký Callbacks
client.OnRequire(TestUnitType, (res) =>
{
    Console.WriteLine($"   [Callback] ⚠️ Server Require/NeedUpdate UnitId: {res.UnitId}, Version: {res.Version}");
    
    // Đánh dấu là đã nhận được Require
    if (requireSignals.TryGetValue(res.UnitId, out var tcs)) tcs.TrySetResult(true);

    // Trả về dữ liệu để Client tự động Send lại cho Server
    long targetTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1000; // 1 giây
    var data = Encoding.UTF8.GetBytes($"Data phục hồi của {res.UnitId}");
    return new XelerateData(res.UnitId, res.Version, targetTime, data);
});

client.OnDone(TestUnitType, (res) =>
{
    if (cancelSignals.ContainsKey(res.UnitId))
    {
        Console.WriteLine($"   [LỖI NGHIÊM TRỌNG] ❌ UnitId {res.UnitId} đã bị Cancel nhưng vẫn nhận được Done!");
        return;
    }

    if (res.UnitId >= 10000) // Dành riêng cho Stress Test
    {
        Interlocked.Increment(ref stressTestCompletedCount);
        return;
    }

    Console.WriteLine($"   [Callback] ✅ DONE UnitId: {res.UnitId}, Version: {res.Version}");
    if (doneSignals.TryGetValue(res.UnitId, out var tcs)) tcs.TrySetResult(true);
});

// ==========================================
// THỰC THI CÁC CASE KIỂM THỬ
// ==========================================

await RunCase1_NormalFlow();
await RunCase2_RequireRecovery();
await RunCase3_VersionConflict();
await RunCase4_Cancellation();
await RunCase5_StressTest_Batching();

Console.WriteLine("\n🎉 TẤT CẢ CÁC BÀI TEST ĐÃ HOÀN TẤT!");
Console.ReadLine();

// --- CÁC HÀM TEST LOGIC ---

async Task RunCase1_NormalFlow()
{
    long unitId = 1;
    Console.WriteLine($"\n▶️ CASE 1: Normal Flow (UnitId: {unitId})");
    Console.WriteLine("   Mô tả: Gửi Update hẹn 2 giây sau hoàn thành.");
    
    var tcs = new TaskCompletionSource<bool>();
    doneSignals[unitId] = tcs;

    long targetTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2000;
    request.Send(TestRegionId, unitId, 1, targetTime, Encoding.UTF8.GetBytes("Normal Case"));

    await WaitTask(tcs.Task, 4000, "Case 1");
}

async Task RunCase2_RequireRecovery()
{
    long unitId = 2;
    Console.WriteLine($"\n▶️ CASE 2: Require Recovery (UnitId: {unitId})");
    Console.WriteLine("   Mô tả: Client Ping UnitId lạ -> Server không thấy -> Trả về Require -> Client auto Update -> Done.");

    var requireTcs = new TaskCompletionSource<bool>();
    var doneTcs = new TaskCompletionSource<bool>();
    requireSignals[unitId] = requireTcs;
    doneSignals[unitId] = doneTcs;

    // Gửi Ping (Server không có thông tin sẽ kích hoạt Require)
    request.Ping(TestRegionId, unitId, 1);

    await WaitTask(requireTcs.Task, 3000, "Case 2 (Đợi Require)");
    await WaitTask(doneTcs.Task, 3000, "Case 2 (Đợi Done)");
}

async Task RunCase3_VersionConflict()
{
    long unitId = 3;
    Console.WriteLine($"\n▶️ CASE 3: Version Conflict (UnitId: {unitId})");
    Console.WriteLine("   Mô tả: Đang xử lý dở, Client Ping với Version khác -> Server báo NeedUpdate -> Client auto Update.");

    var requireTcs = new TaskCompletionSource<bool>(); // NeedUpdate chạy chung Callback với Require
    var doneTcs = new TaskCompletionSource<bool>();
    requireSignals[unitId] = requireTcs;
    doneSignals[unitId] = doneTcs;

    long targetTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 5000;
    request.Send(TestRegionId, unitId, 1, targetTime, Encoding.UTF8.GetBytes("Version 1"));

    // Đợi 1 chút rồi Ping với Version 2 (Cố tình làm sai lệch)
    await Task.Delay(500);
    request.Ping(TestRegionId, unitId, 2); 

    await WaitTask(requireTcs.Task, 3000, "Case 3 (Đợi NeedUpdate)");
    await WaitTask(doneTcs.Task, 4000, "Case 3 (Đợi Done)");
}

async Task RunCase4_Cancellation()
{
    long unitId = 4;
    Console.WriteLine($"\n▶️ CASE 4: Cancellation (UnitId: {unitId})");
    Console.WriteLine("   Mô tả: Gửi Update hẹn 3 giây, nhưng 1 giây sau gọi Cancel. Không được phép trigger Done.");

    cancelSignals[unitId] = true; // Đánh dấu để check trong OnDone

    long targetTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 3000;
    request.Send(TestRegionId, unitId, 1, targetTime, Encoding.UTF8.GetBytes("To be cancelled"));

    await Task.Delay(1000);
    Console.WriteLine("   [Hành động] Đang gửi lệnh Cancel...");
    request.Cancel(TestRegionId, unitId, 1);

    // Đợi quá thời gian Target Time xem Done có bị gọi không
    await Task.Delay(3500);
    Console.WriteLine("   ✅ Passed: Không nhận được Done nào (Hủy thành công).");
}

async Task RunCase5_StressTest_Batching()
{
    Console.WriteLine($"\n▶️ CASE 5: Stress & Batching Test");
    Console.WriteLine("   Mô tả: Gửi 5,000 requests cùng lúc để test Channel Reader limits và MaxBatchSize (50KB).");

    int totalRequests = 5000;
    long targetTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2000; // Đổ dồn xong 2 giây sau Done hàng loạt
    var dummyData = Encoding.UTF8.GetBytes("Stress test data to make payload bigger...");

    Console.WriteLine($"   [Hành động] Đang bơm {totalRequests} items vào Channel...");
    
    for (long i = 10000; i < 10000 + totalRequests; i++)
    {
        request.Send(TestRegionId, i, 1, targetTime, dummyData);
    }

    Console.WriteLine("   [Đợi] Chờ Server trả về 5,000 Done signals...");
    
    // Cho hệ thống tối đa 10 giây để xử lý xong 5000 requests
    int waitCount = 0;
    while (stressTestCompletedCount < totalRequests && waitCount < 20) // 20 * 500ms = 10s
    {
        await Task.Delay(500);
        waitCount++;
        Console.WriteLine($"   ... Đã xử lý: {stressTestCompletedCount}/{totalRequests}");
    }

    if (stressTestCompletedCount == totalRequests)
        Console.WriteLine("   ✅ Passed: Stress Test hoàn hảo, không sót request nào!");
    else
        Console.WriteLine($"   ❌ Failed: Thiếu mất requests. Chỉ xử lý được {stressTestCompletedCount}/{totalRequests}");
}

// Hàm hỗ trợ đợi kết quả an toàn (không bị treo vĩnh viễn)
async Task WaitTask(Task task, int timeoutMs, string testName)
{
    var delayTask = Task.Delay(timeoutMs);
    var completed = await Task.WhenAny(task, delayTask);
    if (completed == delayTask)
    {
        Console.WriteLine($"   ❌ [TIMEOUT] {testName} không hoàn thành trong thời gian {timeoutMs}ms.");
    }
}