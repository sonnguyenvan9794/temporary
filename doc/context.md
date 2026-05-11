# Context & Yêu cầu — Bảo vệ Downstream từ phía Upstream

## Hệ thống hiện tại

- **Mô hình:** BIAN (Banking Industry Architecture Network)
- **Infrastructure:** K8s trên AWS, có Grafana + Prometheus, ElastiCache Redis
- **Integration layer:** MuleSoft (đứng giữa upstream và downstream, do team riêng quản lý)
- **CI/CD:** ArgoCD

---

## Flow hệ thống

```
External Kafka
      │
      ▼
Gateway
      │
      ▼ (mỗi Service Order có Kafka topic riêng từ Gateway)
Service Order-1 → Order-7  (5-7 service, mỗi service nhiều pod)
      │
      │ REST API qua MuleSoft
      ▼
Downstream (hệ thống lõi ngân hàng)
```

---

## Thông số kỹ thuật

| Thông số | Giá trị |
|---|---|
| Traffic upstream | ~2000 transaction/s |
| Downstream capacity bình thường | ~5000 req/s |
| Downstream khi đơ | Không rõ ngưỡng |
| Retry hiện tại | 1 lần, 1 phút sau |
| Kafka consumer | Kéo đơn lẻ + batch (10 message), nhiều partition |
| Kafka consumer poll rate | Không thể thay đổi động (cần restart pod) |
| HPA | Đang dùng, không muốn bị ảnh hưởng |

---

## Constraints — Bắt buộc phải tuân thủ

1. **Không yêu cầu Downstream làm gì** — không thể đòi hỏi downstream expose health endpoint, thay đổi response header, hay bất kỳ thay đổi nào
2. **Không phá vỡ hệ thống hiện có** — không tách service mới chỉ để gọi downstream, không thêm internal Kafka nếu không cần thiết
3. **Không conflict với HPA hiện tại** — không để External Kafka lag tăng do giảm pod
4. **Ưu tiên giải pháp bên thứ 3** — không tự code từ đầu nếu có thư viện/tool sẵn có
5. **Upstream team tự làm được Phase 1** — không phụ thuộc team khác cho bước đầu tiên

---

## Yêu cầu chức năng

1. **Adaptive throttling** — khi downstream yếu, tự động giảm tốc độ gọi xuống, không dừng hẳn
2. **Không mất message** — message phải được giữ lại và xử lý sau, không drop
3. **Tín hiệu tự đo** — tự detect downstream yếu từ 429/503/latency, không cần downstream báo
4. **Dynamic rule** — thay đổi quota không cần restart pod
5. **Shared quota** — tổng tất cả Service Order (Order-1 đến Order-7) không vượt quá N req/s
6. **Exception job thông minh** — không bắn lại ồ ạt khi downstream đang đơ
7. **Circuit breaker** — phân biệt lỗi business (không ảnh hưởng circuit) vs lỗi infrastructure (429/503/timeout)

---

## Điểm mấu chốt đã phân tích

### Tại sao chặn ở Consumer tốt hơn chặn ở Service cuối

```
Chặn ở service cuối:
→ Request đã đi qua toàn bộ service chain
→ Tốn tài nguyên vô ích
→ Thread bị giữ

Chặn ở consumer (không commit offset):
→ Message nằm yên trong Kafka
→ Không tốn tài nguyên gì
→ Pod nhàn, không bị pressure
→ Kafka tự buffer tự nhiên
```

### Tại sao không dùng KEDA

```
KEDA scale down pod:
→ External Kafka lag tăng
→ Conflict với HPA hiện tại
→ Phức tạp

Sentinel không commit offset:
→ Lag ở internal topic của từng Service Order
→ HPA không bị ảnh hưởng
→ Đơn giản hơn
```

### Tại sao Sentinel tốt hơn Resilience4j

```
Resilience4j:
→ Rate limit đơn giản, không có cluster mode
→ Mỗi pod limit riêng → không chính xác khi nhiều pod

Sentinel Cluster Mode:
→ Tất cả pod xin token từ 1 Token Server
→ Tổng quota luôn chính xác
→ Dynamic rule không cần restart
→ Dashboard quản lý tập trung
```

### Exception Job — vấn đề hiện tại

```
Job hiện tại retry fixed interval (1 phút)
→ Downstream đang đơ, job vẫn bắn
→ Góp phần làm downstream nặng thêm
→ Cần: check circuit state trước khi bắn + exponential backoff
```

### Token Server tèo thì sao

```
fallbackToLocalWhenFail = true (default):
→ Mỗi pod tự limit local
→ Hệ thống vẫn chạy
→ Chỉ mất tính chính xác shared quota
→ Downstream có thể bị dồn hơn bình thường
```

---

## Giải pháp đã chọn

**Alibaba Sentinel Cluster Mode** tại Consumer của mỗi Service Order

- Sentinel Token Server + Dashboard deploy trên K8s (2 pod nhỏ)
- Sentinel Client library thêm vào mỗi Service Order
- Wrap consumer: không commit offset khi Sentinel block
- Grafana alert → webhook → update Sentinel rule động
- Exception job: check Sentinel circuit state + exponential backoff

---

## Những gì chưa quyết định / cần làm rõ tiếp

1. Quota cụ thể cho từng trạng thái (HEALTHY/DEGRADED/CRITICAL)
2. Config source cho Sentinel rule (ConfigMap hay Nacos)
3. Có cần shared quota với upstream B, C không (Phase 3)
4. Ngưỡng alert Grafana cụ thể (error rate %, latency threshold)
5. MuleSoft có cần apply Rate Limiting SLA Policy không (nếu cần phối hợp với upstream khác)