# Giải pháp: Adaptive Throttling bảo vệ Downstream

## Bối cảnh

Hệ thống upstream (BIAN model, K8s trên AWS) cần bảo vệ Downstream khỏi bị dồn tải, kiểm soát hoàn toàn ở phía upstream, không yêu cầu Downstream thay đổi gì.

---

## Kiến trúc tổng thể

```
External Kafka
      │
      ▼
Gateway
      │
      ├──► Topic Order-1 → Consumer (Order-1 pods)
      ├──► Topic Order-2 → Consumer (Order-2 pods)
      ...
      └──► Topic Order-7 → Consumer (Order-7 pods)
                │
         [Sentinel Client - thêm vào mỗi Service Order]
         Check token TRƯỚC khi xử lý message
                │
         Có token  → xử lý → gọi Downstream (REST API)
         Hết token → KHÔNG commit offset → Kafka buffer tự nhiên
                │
                ▼
      [Sentinel Token Server - deploy trên K8s]
      Quota chung tất cả Service Order ≤ N req/s
                │
                ▼
           Downstream
```

---

## Các thành phần

### 1. Sentinel Token Server + Dashboard (deploy trên K8s)

- 2 pod nhỏ (~512MB RAM, 0.5 CPU mỗi pod)
- Token Server: quản lý quota chung toàn bộ Service Order
- Dashboard: quản lý rule động, không cần restart pod
- Giao thức: TCP persistent connection (Netty), keep-alive
- Lưu trữ: in-memory, cần config source bên ngoài (ConfigMap/Nacos) để không mất rule khi restart
- Fallback: `fallbackToLocalWhenFail = true` — Token Server tèo thì mỗi pod tự limit local, hệ thống vẫn chạy

### 2. Sentinel Client (thêm vào mỗi Service Order)

```java
// Wrap consumer poll loop
@SentinelResource(value = "downstream-call",
    blockHandler = "handleBlock",
    fallback = "handleFallback")
public void processMessage(Message msg) {
    // xử lý bình thường
    // gọi Downstream REST API
}

public void handleBlock(Message msg, BlockException e) {
    // KHÔNG commit offset
    // Kafka tự giữ message lại
    // Không throw exception, không vào exception handler
}
```

### 3. Sentinel Cluster Flow Rule

```java
FlowRule rule = new FlowRule();
rule.setResource("downstream-call");
rule.setGrade(RuleConstant.FLOW_GRADE_QPS);
rule.setCount(5000);          // tổng quota tất cả upstream
rule.setClusterMode(true);
rule.setClusterConfig(
    new ClusterFlowConfig()
        .setThresholdType(FLOW_THRESHOLD_GLOBAL) // tổng, không phải per-pod
        .setFallbackToLocalWhenFail(true)
);
```

### 4. Circuit Breaker tại service cuối (Sentinel Degrade Rule)

```java
DegradeRule degradeRule = new DegradeRule();
degradeRule.setResource("downstream-call");

// Theo error rate
degradeRule.setGrade(DEGRADE_GRADE_EXCEPTION_RATIO);
degradeRule.setCount(0.1);        // 10% lỗi → OPEN
degradeRule.setTimeWindow(30);    // giữ OPEN 30s

// Theo response time
degradeRule.setGrade(DEGRADE_GRADE_RT);
degradeRule.setCount(3000);       // >3s → OPEN
```

### 5. Dynamic Rule — Grafana Alert → update Sentinel tự động

```
Grafana Alert Rules:
├── error_rate > 10% trong 30s  → DEGRADED  → quota giảm 50%
├── error_rate > 30% trong 30s  → CRITICAL  → quota giảm 90%
└── error_rate < 5%  trong 60s  → RECOVERED → quota về bình thường

Alert → Webhook → Job gọi Sentinel Dashboard API:
PUT /sentinel/rules/flow
{ "resource": "downstream-call", "count": 2500 }  // DEGRADED
{ "resource": "downstream-call", "count": 500  }  // CRITICAL
{ "resource": "downstream-call", "count": 5000 }  // RECOVERED
```

### 6. Exception Job — retry thông minh

```
Trước khi retry:
1. Check Sentinel circuit state → OPEN? → skip
2. Check quota còn không → hết? → skip
3. Exponential backoff theo loại lỗi:
   ├── rate_limited     → 1m, 2m, 4m, 8m (tối đa 30m)
   ├── downstream_down  → chỉ retry khi RECOVERED
   └── business_error   → retry bình thường
```

---

## Tại sao không dùng KEDA

```
KEDA giảm pod:
→ External Kafka lag tăng
→ Conflict với HPA hiện tại
→ Phức tạp, khó predict

Sentinel không commit offset:
→ Kafka lag ở internal topic từng Service Order
→ HPA hiện tại không bị ảnh hưởng
→ Pod vẫn chạy, chỉ xử lý chậm lại
→ Không cần tách service mới
→ Không phá vỡ hệ thống hiện có
```

---

## Lộ trình thực hiện

```
Phase 1 — Upstream team tự làm:
├── Deploy Sentinel Token Server + Dashboard trên K8s
├── Thêm Sentinel library vào mỗi Service Order
├── Wrap consumer: không commit offset khi bị block
└── Exception job: thêm circuit check + exponential backoff

Phase 2 — Phối hợp DevOps (Grafana đã có):
└── Thêm alert rule → webhook → update Sentinel rule động

Phase 3 — Nếu cần shared quota với upstream khác (B, C):
└── Các upstream khác cùng xin token từ Sentinel Token Server chung
    (cần phối hợp, không bắt buộc ngay)
```

---

## Điểm cần lưu ý

| Vấn đề | Xử lý |
|---|---|
| Token Server restart mất rule | Lưu rule vào ConfigMap/Nacos |
| Token Server tèo | fallbackToLocalWhenFail=true, pod tự limit local |
| Exception job bắn ồ ạt khi downstream đơ | Check circuit state trước khi retry |
| Phân biệt lỗi business vs infrastructure | Chỉ tính circuit breaker cho 429/503/timeout, không tính lỗi business |