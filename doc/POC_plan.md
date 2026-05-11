# POC Context — Backpressure Demo

## Mục tiêu POC
Chứng minh hệ thống tự động phát hiện Downstream yếu và giảm tải — không mất message.

---

## Kiến trúc POC

```
Load Generator (thread trong service-order)
      │ sinh N req/s
      ▼
BlockingQueue (in-memory, thay thế Kafka)
      │
Consumer threads (20 threads)
      │
[Sentinel Gate] ← check quota
      │ block → đưa lại queue (KHÔNG mất message)
      │ pass  → gọi tiếp
      ▼
MuleSoft Fake (Spring Boot, port 8082)
      │ forward request + track error rate
      ▼
Downstream Fake (Spring Boot, port 8081)
      │ giới hạn concurrent
      │ trả 429 khi vượt ngưỡng

[AIMD Controller] chạy mỗi 5s trong service-order
      │ đọc error rate từ response codes
      │ error > 10% → limit × 0.5 (giảm nhanh)
      │ error < 1%  → limit + 50  (tăng chậm)
      ▼
Sentinel rule update động
```

---

## 4 File chính cần biết

| File | Vai trò |
|---|---|
| `service-order/ServiceOrderApplication.java` | Toàn bộ logic: queue, consumer, Sentinel, AIMD, metrics API |
| `downstream-fake/DownstreamApplication.java` | Giả lập downstream có giới hạn concurrent |
| `mulesoft-fake/MuleSoftApplication.java` | Giả lập MuleSoft forward request |
| `service-order/static/dashboard.html` | Dashboard Chart.js, poll /metrics mỗi 2s |

---

## Cách chạy

```bash
cd poc
docker-compose up --build
# Mở http://localhost:8080/dashboard.html
# Nhấn "Auto Scenario" để chạy kịch bản tự động
```

---

## Kịch bản test (Auto — 2 phút)

| Phase | Thời gian | Load | Downstream | Kỳ vọng |
|---|---|---|---|---|
| 1 | 0-30s | 200 req/s | limit=100 | Bình thường, Sentinel limit cao |
| 2 | 30-60s | 500 req/s | limit=100 | Downstream stress → AIMD giảm limit |
| 3 | 60-90s | 500 req/s | limit=20 | Downstream yếu → Queue buffer tăng |
| 4 | 90-120s | 200 req/s | limit=100 | Recovery → limit tăng dần |

---

## Metrics endpoint

```
GET http://localhost:8080/metrics
→ sentinelLimit: limit hiện tại
→ queueSize:     message đang chờ trong queue
→ produced:      tổng message đã sinh
→ processed:     tổng message đã xử lý thành công
→ throttled:     tổng message bị giữ lại
→ targetRps:     load hiện tại
→ history:       mảng các điểm dữ liệu để vẽ biểu đồ
```

---

## Admin API

```bash
# Thay đổi load
POST http://localhost:8080/admin/rps?rps=500

# Làm yếu Downstream
POST http://localhost:8080/admin/downstream-limit?limit=10

# Chạy kịch bản tự động
POST http://localhost:8080/admin/run-scenario

# Xem stats downstream
GET  http://localhost:8081/admin/stats
```

---

## Docker services

| Service | Port | Vai trò |
|---|---|---|
| service-order | 8080 | Upstream + AIMD + Dashboard |
| mulesoft-fake | 8082 | Integration layer |
| downstream-fake | 8081 | Downstream có giới hạn |
| sentinel-dashboard | 8858 | UI xem Sentinel rule realtime |

---

## Điểm cần chứng minh qua biểu đồ

```
1. Sentinel Limit tự giảm khi error rate tăng   → AIMD hoạt động
2. Queue tăng khi throttle                       → message không mất
3. Limit tăng từ từ khi recover                  → không thundering herd
4. Processed + Throttled ≈ Produced              → không drop message
```

---

## Điểm khác biệt so với production thực tế

| POC | Production |
|---|---|
| BlockingQueue in-memory | Kafka (nhiều partition, nhiều consumer) |
| 1 service-order instance | Nhiều pod, cần Sentinel Token Server |
| AIMD trong cùng JVM | AIMD Controller riêng (K8s CronJob) |
| Sentinel standalone | Sentinel Cluster Mode |
| MuleSoft Fake | MuleSoft thật với Flex Gateway |

---

## Thông điệp trình bày

> "POC chứng minh 3 điều:
> 1. Sentinel tự động phát hiện Downstream yếu qua error rate
> 2. Message không bị mất — giữ lại trong queue, xử lý khi hệ thống ổn
> 3. Khi hồi phục, tải tăng dần — không gây spike đột ngột cho Downstream"