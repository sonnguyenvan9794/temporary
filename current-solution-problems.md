# Vấn Đề Của Giải Pháp Hiện Tại (Local Semaphore)

## Bối Cảnh Hệ Thống

- **Hệ thống**: payment
- **Peak load**: 15.000 req/s
- **Target_server capacity**: 1.500 - 2.000 req/s
- **Hiện trạng**: 6 services, mỗi service dùng `Semaphore` local để giới hạn concurrent calls đến Target_server

---

## Vấn Đề 1: Semaphore Không Chia Sẻ Được Giữa Các Service

### Cơ chế hiện tại

```
Service A  [Semaphore: 300]  ──┐
Service B  [Semaphore: 300]  ──┤
Service C  [Semaphore: 300]  ──┼──> Target_server (capacity: 2000/s)
Service D  [Semaphore: 300]  ──┤
Service E  [Semaphore: 300]  ──┤
Service F  [Semaphore: 300]  ──┘
```

Mỗi Semaphore là một object Java riêng biệt trong JVM của từng service. **Chúng không biết nhau tồn tại**.

### Hệ quả

- **Worst case**: 6 × 300 = 1.800 requests đồng thời có thể xuyên qua đến Target_server
- **Best case (nếu tăng limit)**: 6 × 400 = 2.400 → vượt ngưỡng Target_server
- Không có cách nào enforce global limit chính xác là 2.000/s

---

## Vấn Đề 2: Unfair Distribution

### Kịch bản thực tế

Giả sử Target_server capacity = 2.000/s, cấu hình mỗi service = 400/s.

| Thời điểm | Service A | Service B | ... | Target_server nhận |
|-----------|-----------|-----------|-----|----------|
| t=0       | 400/s     | 400/s     | ... | 2.400/s ← CRASH |
| t=1 (light load) | 50/s | 100/s | ... | 350/s (lãng phí 1.650/s capacity) |

- Khi **tất cả services đều bận**: Target_server bị hammer vượt limit → timeout, error cascade
- Khi **ít services bận**: capacity Target_server bị lãng phí vì mỗi service bị cap thấp

---

## Vấn Đề 3: Semaphore Đo Concurrency, Không Đo Throughput

`Semaphore` kiểm soát **số lượng concurrent requests** (đang xử lý đồng thời), không phải **requests/second**.

```java
// Semaphore chỉ biết: "hiện có bao nhiêu thread đang giữ permit"
// Không biết: "trong 1 giây qua có bao nhiêu request đã xử lý xong"
semaphore.acquire();
callTarget_server();        // nếu Target_server trả lời trong 50ms → 1 permit giải phóng nhanh
semaphore.release();
```

Ví dụ với Semaphore(300) và Target_server response time 50ms:
- Throughput thực = 300 permits / 0.05s = **6.000 req/s** → vẫn có thể vượt Target_server

Semaphore không phải rate limiter.

---

## Vấn Đề 4: Crash Tại Peak Load

### Sequence of failure

```
1. Peak: 15.000 req/s đổ vào
2. 6 services đều nhận load đều nhau: ~2.500/s mỗi service
3. Mỗi Semaphore(300) bị chiếm hết → requests queue lên hoặc bị reject
4. Một số requests vẫn xuyên qua Target_server: 6 × burst = 2.400-3.000 req/s
5. Target_server quá tải → response time tăng (500ms → 2s)
6. Target_server response chậm → Semaphore permit bị giữ lâu hơn → throughput giảm
7. Queue build up → timeout cascade → circuit không có (chỉ có Semaphore)
8. 503/timeout lan rộng
```

---

## Vấn Đề 5: Không Có Fallback Strategy

`Semaphore` khi đầy chỉ có 2 hành động:
- `acquire()`: **block thread** chờ permit (nguy hiểm: thread starvation)
- `tryAcquire()`: **return false** → cần code thêm xử lý fallback

Không có:
- Queuing với timeout thông minh
- Warm-up period (tránh cold start spike)
- Priority: urgent payment vs batch payment
- Circuit breaker tự động

---

## Vấn Đề 6: Không Monitor Được Globally

Với 6 Semaphore riêng biệt:
- Không có dashboard thống nhất
- Không biết tổng concurrent requests đến Target_server là bao nhiêu
- Alert phải set trên từng service
- Khó debug khi xảy ra incident: "Target_server đang nhận bao nhiêu req từ toàn hệ thống?"

---

## Tóm Tắt Vấn Đề

| Vấn đề | Mức độ | Impact |
|--------|--------|--------|
| Không có global counter | Nghiêm trọng | Target_server bị over-limit tại peak |
| Unfair distribution | Nghiêm trọng | Service mạnh lấn át service yếu |
| Semaphore != rate limiter | Trung bình | Tính toán limit sai hoàn toàn |
| Không có circuit breaker | Nghiêm trọng | Cascade failure khi Target_server chậm |
| Không monitor được | Trung bình | Mù trong incident |
| Không có fallback | Trung bình | UX xấu khi bị reject |

---

## Root Cause

> Semaphore được thiết kế để giải quyết **resource contention trong 1 JVM**.
> Bài toán ở đây là **rate limiting phân tán qua nhiều JVM** — đây là usecase hoàn toàn khác.
