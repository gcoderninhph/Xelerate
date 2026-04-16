# Mục đích dự án
Dự án này được tạo ra nhằm mục đích cung cấp một hệ thống xử lý sự kiện phân tán (Event-Driven) hiệu quả và dễ dàng tích hợp cho các ứng dụng game hoặc ứng dụng thời gian thực khác. Hệ thống sử dụng NATS làm nền tảng giao tiếp, cho phép các client gửi dữ liệu và nhận phản hồi một cách bất đồng bộ, giúp tối ưu hóa hiệu suất và khả năng mở rộng.

# 🚀 NATS Event Processing - Client Integration Guide

Tài liệu này hướng dẫn chi tiết cách tích hợp và sử dụng `XelerateClient` để giao tiếp với hệ thống xử lý sự kiện phân tán (Event-Driven) qua NATS.

Hệ thống hoạt động dựa trên cơ chế **bất đồng bộ (Async)** và **thực thi hẹn giờ (Time-based)**. Client sẽ không nhận kết quả ngay lập tức mà sẽ đăng ký các Callbacks (Sự kiện) để hệ thống tự động gọi lại khi hoàn thành hoặc khi có sự cố dữ liệu.

---

## 📦 1. Khởi tạo & Kết nối

Để bắt đầu, bạn cần khởi tạo Client và kết nối đến NATS Server. Mỗi loại đối tượng (ví dụ: `Monster`, `Hero`, `Building`) sẽ cần tạo ra một `IXelerateRequest` riêng biệt để quản lý.

```csharp
using Xelerate;


// 1. Khởi tạo Client
var natsUrl = "nats://127.0.0.1:4222";

await using server = new XelerateServer(natsUrl);
await server.StartAsync();

await using var client = new XelerateClient(natsUrl);
await client.StartAsync();

// 2. Tạo Request Handler cho một loại đối tượng cụ thể
string unitType = "Monster";
var monsterRequest = client.Create(unitType);
```

---

## 🎧 2. Lắng nghe Sự kiện (Client Callbacks)

Đây là phần quan trọng nhất. Vì hệ thống là bất đồng bộ, bạn **bắt buộc phải đăng ký các sự kiện này** trước khi gửi bất kỳ lệnh nào lên Server.

### ✅ Sự kiện `OnDone` (Hoàn thành)
Được Server gọi lại khi thời gian thực tế vượt qua mốc `TimeTargetMs` mà bạn đã hẹn.

```csharp
client.OnDone("Monster", (response) => 
{
    Console.WriteLine($"[DONE] Quái vật {response.UnitId} đã xử lý xong!");
    Console.WriteLine($"[DONE] Dữ liệu trả về: {response.Data.Length} bytes");
    Console.WriteLine($"[DONE] Thuộc Region: {response.RegionId} | Version: {response.Version}");
});
```

### ⚠️ Sự kiện `OnRequire` (Yêu cầu phục hồi / Cập nhật)
Sự kiện này là cơ chế "Tự chữa lành" (Self-healing) của hệ thống. Khác với `OnDone` chỉ nhận dữ liệu, `OnRequire` **bắt buộc Client phải trả về dữ liệu (Return)** để Server xử lý tiếp.

**Sự kiện này nổ ra trong 2 trường hợp:**
1. **Mất dữ liệu (Require):** Client gửi lệnh `Ping` nhưng Server không tìm thấy UnitId này trên RAM (có thể do Server vừa restart, hoặc UnitId đã bị xóa do quá 10 giây không được Ping).
2. **Sai lệch phiên bản (NeedUpdate):** Client gửi lệnh `Ping` mang Version 2, nhưng Server lại đang lưu Version 1.

```csharp
client.OnRequire("Monster", (response) => 
{
    Console.WriteLine($"[REQUIRE] Server yêu cầu dữ liệu cho Unit {response.UnitId} (Version {response.Version})");

    // BƯỚC 1: Lấy dữ liệu mới nhất từ DB hoặc Cache của Client
    var recoveryData = GetMonsterDataFromDB(response.UnitId); 
    
    // BƯỚC 2: Tính toán lại thời gian hoàn thành (nếu cần)
    long newTargetTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 3000; // hẹn 3 giây nữa
    
    // BƯỚC 3: Trả về XelerateData để Client tự động đóng gói và gửi lại cho Server
    return new XelerateData(response.UnitId, response.Version, newTargetTime, recoveryData);
});
```

---

## 🕹 3. Các Hành động của Client (Client Actions)

Sau khi đã đăng ký lắng nghe sự kiện, bạn sử dụng `monsterRequest` để tương tác với Server.

### 📤 `Send` (Cập nhật / Hẹn giờ)
Lệnh cốt lõi dùng để đẩy dữ liệu lên Server và thiết lập mốc thời gian hoàn thành.
*Lưu ý: Lệnh này cũng tự động reset bộ đếm Ping (10 giây).*

```csharp
long regionId = 1001;
long unitId = 99;
int version = 1;
byte[] data = Encoding.UTF8.GetBytes("Data của Monster 99");

// Đặt mốc thời gian: 5 giây kể từ hiện tại
long targetTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 5000; 

// Gửi lên Server (Sẽ trigger OnDone sau 5 giây)
await monsterRequest.Send(regionId, unitId, version, targetTimeMs, data);
```

### 💓 `Ping` (Duy trì kết nối & Kiểm tra đồng bộ)
Server có một bộ đếm ngầm: **Nếu một UnitId không được nhận lệnh Send hoặc Ping trong vòng 10 giây, nó sẽ bị XÓA THẦM LẶNG để giải phóng RAM.**

Bạn cần gọi `Ping` định kỳ để:
1. Giữ UnitId không bị xóa.
2. Kiểm tra xem Version giữa Client và Server có bị lệch không.

```csharp
// Gửi Ping. Nếu Server không biết Unit này hoặc lệch Version, nó sẽ kích hoạt sự kiện OnRequire.
await monsterRequest.Ping(regionId, unitId, version);
```

### ❌ `Cancel` (Hủy bỏ tức thời)
Nếu bạn không muốn đợi đến khi `TimeTargetMs` kết thúc và muốn hủy bỏ ngay tác vụ này, hãy gọi Cancel.
Sự kiện `OnDone` sẽ KHÔNG BAO GIỜ được gọi cho UnitId này nữa.

```csharp
await monsterRequest.Cancel(regionId, unitId, version);
```

---

## 🔄 4. Tóm tắt Vòng đời chuẩn (Happy Path)

Để dễ hình dung, một kịch bản giao tiếp hoàn hảo sẽ diễn ra như sau:

1. **Client:** Gọi `Send` mang theo dữ liệu, hẹn 15 giây sau thực thi.
2. **Server:** Nhận lệnh, lưu dữ liệu vào RAM, bắt đầu đếm ngược 15 giây và đếm ngược Ping (10 giây).
3. **Client:** 8 giây sau, gọi `Ping`.
4. **Server:** Nhận `Ping`, gia hạn tuổi thọ của đối tượng thêm 10 giây nữa (Tránh bị chết ngầm).
5. **Server:** Đạt mốc 15 giây. Lấy dữ liệu ra, đóng gói, gọi hàm `Done()` đẩy về qua NATS.
6. **Client:** Nhận tin nhắn từ NATS, kích hoạt hàm callback `OnDone` và in ra màn hình.

---

## 🛠 5. Cơ chế gom gói (Batching) dưới nền

Bạn có thể tự do gọi `Send`, `Ping`, `Cancel` hàng ngàn lần cùng lúc (ví dụ trong vòng lặp `for`) mà không lo nghẽn mạng.

`XelerateClient` đã được tích hợp bộ đệm `Channel` kết hợp với thuật toán gộp gói tin (Batching) của Protobuf. Nó sẽ gom nhiều request lại thành một gói tin tối đa **50KB** trước khi thực sự bắn qua NATS, giúp tối ưu hóa thông lượng (Throughput) lên mức cực hạn.