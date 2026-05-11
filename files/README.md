# POC: Backpressure — Bảo vệ Downstream

## Chạy nhanh

```bash
cd poc
docker-compose up --build
```

Mở browser: **http://localhost:8080/dashboard.html**

---

## Kiến trúc

```
Load Generator (thread)
      │ sinh N req/s
      ▼
BlockingQueue (in-memory, thay Kafka)
      │
Consumer threads (20 threads)
      │
[Sentinel Gate] ← check quota trước khi gọi
      │ block → đưa lại queue (không mất message)
      │ pass  → gọi tiếp
      ▼
MuleSoft Fake (port 8082)
      │ forward + track error
      ▼
Downstream Fake (port 8081)
      │ giới hạn concurrent, trả 429 khi vượt

[AIMD Controller] chạy mỗi 5s
      │ đọc error rate
      │ tính limit mới
      ▼
Sentinel rule update (tự động)
```

---

## Kịch bản test (Auto Scenario — 2 phút)

| Phase | Thời gian | Load | Downstream | Kỳ vọng |
|---|---|---|---|---|
| 1 | 0-30s | 200 req/s | limit=100 | Ổn định, Sentinel limit cao |
| 2 | 30-60s | 500 req/s | limit=100 | Downstream stress → AIMD giảm limit → Queue tăng |
| 3 | 60-90s | 500 req/s | limit=20 | Downstream yếu → limit giảm mạnh → Queue buffer |
| 4 | 90-120s | 200 req/s | limit=100 | Recovery → limit tăng dần → Queue giảm |

---

## Điều cần quan sát trên dashboard

```
✅ Sentinel Limit tự giảm khi Downstream yếu
✅ Queue tăng khi throttle (message giữ lại, không mất)
✅ Error rate kích hoạt AIMD
✅ Limit tăng từ từ khi recover (không thundering herd)
✅ Processed + Throttled = Produced (không mất message)
```

---

## API thủ công

```bash
# Thay đổi load
curl -X POST "http://localhost:8080/admin/rps?rps=500"

# Làm yếu Downstream
curl -X POST "http://localhost:8080/admin/downstream-limit?limit=10"

# Cho Downstream hồi phục
curl -X POST "http://localhost:8080/admin/downstream-limit?limit=100"

# Chạy kịch bản tự động
curl -X POST "http://localhost:8080/admin/run-scenario"

# Xem metrics raw
curl http://localhost:8080/metrics

# Xem Downstream stats
curl http://localhost:8081/admin/stats
```

---

## Sentinel Dashboard

Mở: **http://localhost:8858**
- User/Pass: sentinel / sentinel
- Chọn service: service-order
- Xem real-time QPS, block rate, rule

---

## Thông điệp cho buổi trình bày

> "Khi Downstream bị quá tải, AIMD Controller tự phát hiện qua
> error rate và giảm Sentinel limit xuống. Consumer giữ message
> lại trong Queue — không mất 1 message nào. Khi Downstream
> hồi phục, limit tăng dần từng bước — không có spike đột ngột."
