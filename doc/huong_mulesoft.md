# 2 Hướng Giải Pháp tại MuleSoft

## Bối cảnh

MuleSoft đứng giữa các upstream và Downstream. Khi Downstream bị quá tải, MuleSoft cần tự động giảm lượng request gửi xuống và thông báo cho upstream biết để giữ lại message trong Kafka.

```
Upstream → MuleSoft → Downstream
               │
               └── Khi Downstream yếu:
                   Cần giảm traffic xuống
                   Thông báo upstream giữ lại Kafka
```

---

## Hướng 1: Grafana Alert (Đơn giản)

### Cơ chế

```
MuleSoft nhận lỗi từ Downstream (429/503/timeout)
    │
    ▼
Prometheus thu thập metrics
    │
    ▼
Grafana phát hiện vấn đề → bắn alert
    │
    ▼
Tự động gọi API cập nhật giới hạn:
├── Downstream yếu  → giảm xuống 2500 req/s
├── Downstream rất yếu → giảm xuống 500 req/s
└── Downstream ổn   → tăng về 5000 req/s
    │
    ▼
Upstream nhận ít quota hơn → giữ message trong Kafka
```

### Ưu điểm
- Không cần code thêm gì nhiều
- Grafana và Prometheus đã có sẵn
- Dễ hiểu, dễ audit, dễ giải thích
- Vận hành đơn giản

### Nhược điểm
- Phản ứng chậm (~30-60s sau khi có vấn đề)
- Chỉ có 3 mức: bình thường / yếu / rất yếu
- Khi Downstream hồi phục, tăng quota đột ngột có thể gây spike
- Ngưỡng cảnh báo (10%, 30%) cần tuning thủ công

### Phù hợp khi
```
✅ Ưu tiên đơn giản, ít rủi ro khi vận hành
✅ Traffic pattern tương đối ổn định
✅ Team không có nhiều thời gian implement
✅ Giai đoạn đầu triển khai
```

---

## Hướng 2: Netflix Concurrency-Limits Library (Thích ứng)

### Cơ chế

```
MuleSoft gọi Downstream
    │
    Đo thời gian phản hồi (RTT) liên tục
    │
    So sánh với thời gian tốt nhất từng thấy (baseline)
    │
    ├── RTT ≈ baseline → Downstream đang ổn → tăng dần limit
    └── RTT >> baseline → Downstream đang chậm → giảm limit ngay
    │
    Tự động điều chỉnh liên tục, không cần ngưỡng cứng
    │
    ▼
Upstream nhận ít quota hơn → giữ message trong Kafka
```

### Ví dụ trực quan

```
Bình thường:
Downstream trả lời trong 50ms (baseline)
→ Cho phép 100 request đồng thời

Downstream bắt đầu chậm:
Downstream trả lời trong 200ms
→ Tự động giảm xuống 20 request đồng thời
→ Không cần chờ Grafana alert

Downstream hồi phục:
Trả lời nhanh lại
→ Tăng dần từng bước nhỏ
→ Không có spike đột ngột
```

### Ưu điểm
- Phản ứng gần như tức thì (milliseconds)
- Tự động thích ứng, không cần config ngưỡng
- Tăng quota từ từ khi hồi phục → không thundering herd
- Mỗi API endpoint có giới hạn riêng phù hợp
- Đã được Netflix, Lyft dùng ở production

### Nhược điểm
- Cần MuleSoft team tích hợp thư viện Java
- Mỗi MuleSoft instance tự tính riêng → tổng quota không chính xác tuyệt đối
- Cần kết hợp thêm Flex Gateway hard cap để đảm bảo tổng
- Khó audit hơn (thuật toán tự động)

### Phù hợp khi
```
✅ Traffic phức tạp, không ổn định
✅ Cần phản ứng nhanh, không chờ alert
✅ Nhiều loại API endpoint khác nhau
✅ Team có capacity implement và maintain
```

---

## So sánh 2 Hướng

| Tiêu chí | Hướng 1 (Grafana) | Hướng 2 (Netflix lib) |
|---|---|---|
| Độ phức tạp | Thấp | Trung bình |
| Tốc độ phản ứng | 30-60 giây | Gần tức thì |
| Tự thích ứng | ❌ Ngưỡng cứng | ✅ Liên tục |
| Thundering herd | ⚠️ Có risk | ✅ Không có |
| Dễ audit | ✅ Rõ ràng | ⚠️ Cần log tốt |
| Cần code thêm | Ít | Vừa phải |
| Đã proven | ✅ | ✅ Netflix, Lyft |

---

## Đề xuất

**Ngắn hạn:** Hướng 1 — triển khai nhanh, ổn định, đủ bảo vệ Downstream trong hầu hết trường hợp.

**Dài hạn:** Hướng 2 — nếu thực tế vận hành cho thấy Hướng 1 chưa đủ nhanh hoặc gây spike khi hồi phục.

> Cả 2 hướng đều kết hợp với **Sentinel tại upstream** — upstream nhận tín hiệu hạn chế từ MuleSoft và tự động giữ message lại trong Kafka, không bị mất.