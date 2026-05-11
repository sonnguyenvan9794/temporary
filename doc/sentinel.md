# Alibaba Sentinel & Spring Cloud Alibaba

## Sentinel là gì?

Alibaba Sentinel là một framework open source (Apache 2.0) chuyên về **flow control, circuit breaking và adaptive system protection** cho microservices. Được Alibaba phát triển từ 2012 và battle-tested qua hàng chục mùa Double 11 với hàng chục nghìn service.

Điểm khác biệt cốt lõi so với các giải pháp khác (Resilience4j, Hystrix):

```
Resilience4j / Hystrix:     Sentinel:
"Bảo vệ client"             "Bảo vệ server"
Tôi gọi ra bị lỗi           Traffic vào tôi quá nhiều
→ tôi tự xử lý fallback     → tôi chặn bớt lại
```

Github:
https://github.com/alibaba/sentinel 23k star

https://github.com/alibaba/spring-cloud-alibaba 29k star


---

## Tại sao chọn Sentinel cho bài toán Backpressure?

### Bài toán cụ thể

```
External Kafka
      │
      ▼
Gateway
      │
      ▼ (Kafka topic riêng cho từng Service Order)
Service Order-1 → Order-7  (5-7 service, nhiều pod)
      │
      │ REST API
      ▼
MuleSoft → Downstream (hệ thống lõi, không được dồn tải)
```

### Tại sao các giải pháp khác không đủ

| Giải pháp | Vấn đề |
|---|---|
| Resilience4j | Chỉ OPEN/CLOSED, không có adaptive giảm dần |
| Bucket4j + Redis | Phải tự build adaptive logic = tự build lại Sentinel |
| KEDA scale pod | External Kafka lag tăng, conflict HPA hiện tại |
| Envoy Rate Limit | HTTP layer only, không fit Kafka consumer |

### Sentinel giải quyết được vì

```
1. Cluster Mode: tất cả pod xin token từ 1 Token Server
   → Tổng quota chính xác dù 10 hay 40 pod

2. Adaptive: tự giảm tốc khi detect 429/503/latency tăng
   → Không chỉ OPEN/CLOSED mà giảm dần theo thực tế

3. Dynamic Rule: thay đổi quota không cần restart pod
   → Grafana alert → update rule realtime qua API

4. Kafka consumer không commit offset khi bị block
   → Kafka tự buffer message, không mất data
   → Không ảnh hưởng HPA hiện tại
```

---

## Kiến trúc triển khai

```
Sentinel Token Server (K8s pod)
Sentinel Dashboard   (K8s pod)
        │
        │ TCP persistent connection (Netty)
        │ keep-alive, auto-reconnect
        │
Service Order-1 pods ──► check token ──► gọi Downstream
Service Order-2 pods ──► check token ──► gọi Downstream
...
Service Order-7 pods ──► check token ──► gọi Downstream

Tổng quota = N req/s (shared, chính xác)
```

### Khi Token Server tèo

```
fallbackToLocalWhenFail = true (default):
→ Mỗi pod tự limit local (VD: 250 req/s/pod)
→ Hệ thống vẫn chạy
→ Tốt hơn hiện tại vì hiện tại không có gì kiểm soát cả
```

---

## Spring Cloud Alibaba — Con đường chính thức

`spring-cloud-starter-alibaba-sentinel` là cách chính thức, được Spring.io listing, tích hợp Sentinel vào Spring Boot.

### Version Compatibility

| Spring Boot | Spring Cloud Alibaba | JDK |
|---|---|---|
| 2.1.x | greenwich branch | JDK 8+ |
| 2.2.x | 2.2.x | JDK 8+ |
| 2.4.x | 2020.0.x | JDK 8+ |
| 2.6.x | **2021.0.x.x** | JDK 8+ |
| 3.0.x | **2022.x** | JDK 17+ |
| 3.2.x | **2023.x** | JDK 17+ |
| 3.5.x | **2025.0.x** | JDK 17+ |
| 4.0.x | **2025.1.x** | JDK 17+ |

**Hệ thống có cả Spring Boot 2 và 3:** mỗi service chọn đúng SCA version tương ứng — không conflict, cùng kết nối được Token Server.

### Thêm dependency

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.alibaba.cloud</groupId>
      <artifactId>spring-cloud-alibaba-dependencies</artifactId>
      <version>2023.0.3.2</version> <!-- chọn theo Boot version -->
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependency>
  <groupId>com.alibaba.cloud</groupId>
  <artifactId>spring-cloud-starter-alibaba-sentinel</artifactId>
</dependency>
```

### Wrap Kafka consumer

```java
// Không commit offset khi Sentinel block
@SentinelResource(
    value = "downstream-call",
    blockHandler = "handleBlock"
)
public void processMessage(ConsumerRecord<?, ?> record) {
    // xử lý bình thường → gọi downstream
}

public void handleBlock(ConsumerRecord<?, ?> record, BlockException e) {
    // KHÔNG commit offset → Kafka giữ message lại
    // KHÔNG throw exception
    // KHÔNG vào exception handler
    log.warn("Rate limited, holding message in Kafka");
}
```

### Cluster Flow Rule

```java
FlowRule rule = new FlowRule();
rule.setResource("downstream-call");
rule.setGrade(RuleConstant.FLOW_GRADE_QPS);
rule.setCount(5000);           // tổng tất cả pod
rule.setClusterMode(true);
rule.setClusterConfig(
    new ClusterFlowConfig()
        .setThresholdType(FLOW_THRESHOLD_GLOBAL) // global, không phải per-pod
        .setFallbackToLocalWhenFail(true)         // vẫn chạy khi Token Server tèo
);

// Local fallback rule (khi Token Server tèo)
FlowRule localRule = new FlowRule();
localRule.setResource("downstream-call");
localRule.setCount(250);       // 5000 / số pod dự kiến
localRule.setClusterMode(false);
```

### Dynamic Rule qua Grafana

```
Grafana alert (error_rate > 10% trong 30s)
    → Webhook → Lambda / Job
    → Gọi Sentinel Dashboard API:

PUT /sentinel/rules/flow
{ "resource": "downstream-call", "count": 2500 }  // DEGRADED: giảm 50%
{ "resource": "downstream-call", "count": 500  }  // CRITICAL: giảm 90%
{ "resource": "downstream-call", "count": 5000 }  // RECOVERED: về bình thường
```

---

## Vấn đề nếu không dùng Java

### Sentinel SDK cho ngôn ngữ khác

| SDK | Trạng thái | Cluster Mode | Dùng được? |
|---|---|---|---|
| **Java** | ✅ Active, full feature | ✅ | ✅ Recommend |
| **Go** | ⚠️ Ít maintained | ⚠️ Hạn chế | ⚠️ Cân nhắc |
| **C++** | ❌ Gần như abandoned | ❌ | ❌ Không nên |

### Giải pháp cho service không phải Java

Phần quan trọng nhất là **Kafka consumer không commit offset** — logic này đơn giản, mọi ngôn ngữ đều làm được mà không cần Sentinel SDK:

```
Non-Java service chỉ cần:

1. Đọc circuit state từ Redis (do Java service/Sentinel ghi vào)
   key: "circuit:downstream" → CLOSED / DEGRADED / CRITICAL

2. Kafka consumer check trước khi process:
   CLOSED   → process bình thường, commit offset
   DEGRADED → sleep 500ms, thử lại
   CRITICAL → không commit offset, Kafka giữ lại

3. Không cần Sentinel SDK
```

**Python example:**
```python
def consume_messages():
    while True:
        records = consumer.poll(timeout_ms=1000)
        state = redis.get("circuit:downstream")

        if state == "CRITICAL":
            time.sleep(1)
            continue  # không commit, Kafka giữ lại

        for record in records:
            process(record)

        if state != "CRITICAL":
            consumer.commit()
```

**Node.js / Go / bất kỳ ngôn ngữ nào:** logic tương tự — chỉ cần đọc Redis và kiểm soát `commit()`.

### Ai ghi circuit state vào Redis?

```
Java service (có Sentinel) phát hiện downstream yếu
    → Sentinel callback khi circuit thay đổi state
    → Ghi vào Redis: SET "circuit:downstream" "DEGRADED"

Non-Java service đọc Redis key này
    → Tự điều tiết tốc độ commit
    → Không cần biết gì về Sentinel
```

---

## License & Chi phí

- **Apache License 2.0** — hoàn toàn free cho commercial use
- Không giới hạn số instance, không phí bản quyền
- Chỉ cần 2 pod nhỏ (~512MB RAM, 0.5 CPU mỗi pod) trên K8s sẵn có

---

## Tóm lại

```
✅ Apache 2.0 — free commercial
✅ Spring Boot 2.x và 3.x đều được — chọn đúng SCA version
✅ Giải quyết đúng bài toán adaptive throttling
✅ Cluster mode — quota chính xác giữa nhiều pod
✅ Kafka consumer không commit offset — HPA không bị ảnh hưởng
✅ Dynamic rule — không cần restart
✅ Non-Java service — dùng Redis shared state, không cần SDK
⚠️ Cần deploy thêm 2 pod: Token Server + Dashboard
⚠️ Rule cần lưu vào ConfigMap/Nacos để không mất khi restart
```