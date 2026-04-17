# Xelerate - Distributed Event Scheduler

**Xelerate** là một hệ thống lập lịch sự kiện phân tán hiệu năng cao, được thiết kế để xử lý hàng triệu bộ đếm thời gian (timers) theo thời gian thực. Hệ thống sử dụng **Kafka** làm luồng giao tiếp bất đồng bộ và **MySQL** để đảm bảo tính toàn vẹn dữ liệu (Persistence) chống mất mát khi crash.

Hệ thống cực kỳ phù hợp cho các bài toán:
- Hệ thống Buff / Debuff trong game (ví dụ: Hết 5 giây thì mất trạng thái đóng băng).
- Lập lịch các tác vụ trễ (Delay Actions).
- Timeout management.

---

## 🚀 1. Khởi tạo Hệ thống (Initialization)

Để hệ thống hoạt động, bạn cần khởi chạy cả **Server** (chịu trách nhiệm đếm giờ và lưu DB) và **Client** (chịu trách nhiệm gửi lệnh và nhận kết quả).

### Khởi chạy Server
Server sẽ tự động tạo bảng MySQL và tự động nạp lại (Restore) các sự kiện đang chạy dở từ database nếu trước đó bị tắt đột ngột.

```csharp
string kafkaUrl = "localhost:29092";
string mysqlUrl = "Server=localhost;Port=3306;Database=xelerate;User Id=root;Password=12345678;";

var server = new XelerateServer(kafkaUrl, mysqlUrl);
await server.EnsureTopicsExistAsync(); // Đảm bảo Kafka topics đã sẵn sàng
await server.StartAsync();             // Bắt đầu lắng nghe và đếm giờ
```

### Khởi chạy Client
Client được dùng để tương tác với Server. Mỗi Client có thể có một `GroupId` riêng hoặc để trống (hệ thống sẽ tự tạo ID ngẫu nhiên để hoạt động như chế độ Broadcast).

```csharp
// Khởi tạo Client
var client = new XelerateClient(kafkaUrl, "my-client-group");
await client.EnsureTopicsExistAsync();
client.StartAsync();
```

---

## 🎮 2. Hướng dẫn Sử dụng (Usage)

Mọi thao tác gửi sự kiện đều thông qua interface `IXelerateRequest`. Các yêu cầu được gom mẻ (Batching) dưới nền để tối ưu hóa băng thông Kafka.

### 2.1 Tạo Request Channel
Trước khi gửi sự kiện, bạn cần tạo một kênh Request dựa trên `unitType` (Nhóm loại sự kiện, VD: `"PlayerBuff"`, `"MonsterRespawn"`).

```csharp
var request = client.Create("PlayerBuff");
```

### 2.2 Đặt lịch sự kiện (Send / Upsert)
Sử dụng hàm `Send` để lên lịch nổ cho một sự kiện.
**Lưu ý:** Hàm `Send` hoạt động như một lệnh **Upsert**. Nếu `UnitId` đã tồn tại, nó sẽ **cập nhật đè** (Override) thời gian và dữ liệu mới nhất.

```csharp
long regionId = 1;              // ID của phân khu (VD: MapID, RoomID)
long unitId = 99;               // ID của đối tượng (VD: PlayerID)
byte[] payloadData = { 1, 2, 3 }; // Dữ liệu đính kèm (có thể rỗng)

// Tính toán thời gian nổ trong tương lai (Ví dụ: 5 giây sau kể từ hiện tại)
long targetMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 5000;

// Gửi lệnh
request.Send(regionId, unitId, targetMs, payloadData);
```

### 2.3 Hủy bỏ sự kiện (Cancel)
Nếu bạn muốn xóa bỏ một sự kiện trước khi nó kịp nổ (Ví dụ: Người chơi dùng vật phẩm giải buff sớm), hãy dùng lệnh `Cancel`.

```csharp
request.Cancel(regionId, unitId);
```
*Hệ thống có cơ chế khử trùng lặp (De-duplication). Nếu bạn gửi `Send` và `Cancel` liên tục trong 1 tích tắc, chỉ có hành động cuối cùng được thực thi.*

---

## ⚡ 3. Các Sự kiện (Events)

Sự kiện duy nhất và quan trọng nhất mà Client cần quan tâm là `OnDone`. Sự kiện này sẽ được Server bắn ngược lại Client khi **thời gian của hệ thống vượt qua mốc `TimeTargetMs`** mà bạn đã thiết lập ở lệnh `Send`.

### Lắng nghe sự kiện (OnDone)
**Quy tắc:** Bạn LUÔN LUÔN phải đăng ký `OnDone` *trước* khi bắt đầu nhận kết quả để tránh miss message.

```csharp
// Đăng ký Callback đồng bộ (Action)
client.OnDone("PlayerBuff", response =>
{
    Console.WriteLine($"[Buff Hết Hạn] Region: {response.RegionId}, Unit: {response.UnitId}");
    
    // Đọc dữ liệu đính kèm nếu có
    if (response.Data.Length > 0)
    {
        var data = response.Data.ToArray();
        // Xử lý data...
    }
});

// Hoặc đăng ký Callback bất đồng bộ (Func<Task>)
client.OnDone("PlayerBuff", async response =>
{
    await SomeAsyncLogic(response.UnitId);
});
```

---

## 🛑 4. Tắt hệ thống an toàn (Graceful Shutdown)

Hệ thống Xelerate lưu trữ dữ liệu trên RAM để đảm bảo độ trễ siêu thấp và dùng luồng nền để đồng bộ xuống MySQL. Do đó, khi tắt ứng dụng, bạn **BẮT BUỘC** phải gọi `DisposeAsync` để hệ thống vét cạn (Drain) dữ liệu còn sót lại trên RAM xuống DB an toàn.

```csharp
// Tắt Client trước để ngừng nhận/gửi request mới
await client.DisposeAsync();

// Tắt Server sau. Server sẽ tự động hoàn tất các lệnh ghi Batch xuống MySQL trước khi tắt hẳn.
await server.DisposeAsync();
```

---

## 🛠️ Luồng hoạt động Tóm tắt (Flow)
1. `Client` gọi `request.Send()`.
2. Lệnh được đưa vào Batch (tối đa 50KB) và gửi qua Kafka (`XelerateServerTopic`).
3. `Server` nhận lệnh, cập nhật `TimedSortedSet` trên RAM và đẩy vào Channel để luồng ngầm lưu xuống MySQL.
4. Khi đồng hồ thời gian thực (`now`) vượt qua `TimeTargetMs`, `TimedSortedSet` nổ event.
5. `Server` bắn gói tin chứa trạng thái `Done` qua Kafka (`XelerateClientTopic`), đồng thời xóa Record dưới MySQL.
6. `Client` nhận được tin nhắn và kích hoạt hàm `OnDone()` mà bạn đã định nghĩa.