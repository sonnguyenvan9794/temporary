# Giải Pháp: Sentinel Cluster Mode - Standalone Token Server

## Kiến Trúc Tổng Quan

Một **dedicated `sentinel-token-server`** chạy riêng biệt. Tất cả 6 microservices là pure client — không cần biết nhau, không cần role detection, chỉ cần biết địa chỉ token server.

```
┌──────────────────────────────────────────────────────────────┐
│                   sentinel-token-server                       │
│               (Spring Boot app riêng, port 18080)            │
│                                                               │
│   SentinelDefaultTokenServer (embedded=false)                 │
│   NettyTransportServer: lắng nghe Netty TCP :18080           │
│                                                               │
│   ClusterFlowRuleManager                                      │
│     └─ FlowRule: "Target_server_payment", count=2000, GLOBAL           │
│                                                               │
│   ClusterMetricLeapArray (sliding window, in-memory)          │
│     └─ counter: số token đã cấp trong 1 giây qua             │
│                                                               │
│   GlobalRequestLimiter: maxAllowedQps = 2000                  │
└──────────────────┬───────────────────────────────────────────┘
                   │ Netty TCP :18080
    ┌──────────────┼──────────────────────────┐
    │              │              │            │
┌───▼────┐   ┌────▼───┐   ┌─────▼──┐   ┌────▼───┐
│Svc A   │   │Svc B   │   │Svc C   │   │Svc D   │  ... Svc E, F
│client  │   │client  │   │client  │   │client  │
└───┬────┘   └────┬───┘   └─────┬──┘   └────┬───┘
    └──────────────┴──────────────┴────────────┘
                              │
                     ┌────────▼────────┐
                     │      Target_server        │
                     │  <= 2000 req/s  │
                     └─────────────────┘
```

---

## Cơ Chế Hoạt Động — Từ Source Code

### 1. Token Server Khởi Động

```java
// ClusterServerDemo.java (standalone mode)
ClusterTokenServer tokenServer = new SentinelDefaultTokenServer(); // embedded=false

ClusterServerConfigManager.loadGlobalTransportConfig(
    new ServerTransportConfig().setPort(18080).setIdleSeconds(600)
);
ClusterServerConfigManager.loadServerNamespaceSet(
    Collections.singleton("payment-cluster")
);

tokenServer.start(); // Khởi động NettyTransportServer lắng nghe :18080
```

Bên trong `SentinelDefaultTokenServer.start()` → `NettyTransportServer` bind port, sẵn sàng nhận kết nối từ clients.

### 2. Client Gửi Token Request

Khi bất kỳ service nào (A, B, C...) cần gọi Target_server, `DefaultClusterTokenClient.requestToken()` chạy:

```java
// DefaultClusterTokenClient.java:150-165
public TokenResult requestToken(Long flowId, int acquireCount, boolean prioritized) {
    FlowRequestData data = new FlowRequestData()
        .setCount(acquireCount)
        .setFlowId(flowId)
        .setPriority(prioritized);
    ClusterRequest<FlowRequestData> request =
        new ClusterRequest<>(ClusterConstants.MSG_TYPE_FLOW, data);
    try {
        return sendTokenRequest(request); // Netty async, có timeout
    } catch (Exception ex) {
        return new TokenResult(TokenResultStatus.FAIL); // fallback to local
    }
}
```

Đây là **non-blocking Netty call** — không block thread của service, có timeout configurable (default 20ms).

### 3. Token Server Kiểm Tra (Atomic, In-Memory)

Server nhận request → `ClusterFlowChecker.acquireClusterToken()`:

```java
// ClusterFlowChecker.java:55-112
static TokenResult acquireClusterToken(FlowRule rule, int acquireCount, boolean prioritized) {
    // Bước 1: Global namespace QPS cap
    if (!GlobalRequestLimiter.tryPass(namespace)) {
        return new TokenResult(TokenResultStatus.TOO_MANY_REQUEST);
    }

    // Bước 2: Lấy sliding window metric
    ClusterMetric metric = ClusterMetricStatistics.getMetric(id);
    double latestQps = metric.getAvg(ClusterFlowEvent.PASS); // req/s hiện tại
    double globalThreshold = calcGlobalThreshold(rule);      // = 2000

    // Bước 3: Còn slot không?
    double nextRemaining = globalThreshold - latestQps - acquireCount;
    if (nextRemaining >= 0) {
        metric.add(ClusterFlowEvent.PASS, acquireCount); // cộng vào counter
        return new TokenResult(TokenResultStatus.OK).setRemaining((int) nextRemaining);
    } else {
        metric.add(ClusterFlowEvent.BLOCK, acquireCount);
        return blockedResult(); // TokenResultStatus.BLOCKED
    }
}
```

`ClusterMetric` dùng `ClusterMetricLeapArray` — sliding window 10 buckets × 100ms = 1 giây. Counter nằm hoàn toàn trong **memory của token server**, không đi qua Redis hay bất kỳ external store nào.

### 4. Client Nhận Kết Quả

| Status | Hành động |
|--------|-----------|
| `OK` | Proceed — gọi Target_server |
| `BLOCKED` | Gọi `blockHandler` — trả lỗi cho client |
| `FAIL` | Server unreachable → **fallback to local FlowRule** |
| `TOO_MANY_REQUEST` | Namespace QPS cap bị vượt → block |

---

## Cấu Trúc Deployment

```
sentinel-token-server/          ← Service mới, lightweight
├── pom.xml
└── src/main/java/
    └── TokenServerApplication.java
    └── init/
        └── TokenServerInitFunc.java   ← implements InitFunc

service-a/  service-b/  ...     ← 6 services hiện tại
└── init/
    └── ClusterClientInitFunc.java     ← implements InitFunc (mỗi service)
```

### Token Server — Code Khởi Tạo

```java
// TokenServerInitFunc.java (implements com.alibaba.csp.sentinel.init.InitFunc)
public void init() throws Exception {
    // 1. Đăng ký supplier để load flow rules theo namespace
    ClusterFlowRuleManager.setPropertySupplier(namespace -> {
        // Dùng Nacos datasource để push rules động
        ReadableDataSource<String, List<FlowRule>> ds = new NacosDataSource<>(
            NACOS_HOST, GROUP_ID,
            namespace + "-flow-rules",
            source -> JSON.parseObject(source, new TypeReference<List<FlowRule>>() {})
        );
        return ds.getProperty();
    });

    // 2. Load namespace set server phải serve
    ReadableDataSource<String, Set<String>> namespaceDs = new NacosDataSource<>(
        NACOS_HOST, GROUP_ID, "token-server-namespace-set",
        source -> JSON.parseObject(source, new TypeReference<Set<String>>() {})
    );
    ClusterServerConfigManager.registerNamespaceSetProperty(namespaceDs.getProperty());

    // 3. Load transport config (port)
    ReadableDataSource<String, ServerTransportConfig> transportDs = new NacosDataSource<>(
        NACOS_HOST, GROUP_ID, "token-server-transport-config",
        source -> JSON.parseObject(source, new TypeReference<ServerTransportConfig>() {})
    );
    ClusterServerConfigManager.registerServerTransportProperty(transportDs.getProperty());
}
```

Nếu không dùng Nacos, có thể load trực tiếp bằng code (hardcode hoặc từ application.yml).

### Mỗi Microservice — Chỉ Cần Biết Địa Chỉ Token Server

```java
// ClusterClientInitFunc.java (mỗi service implement 1 cái)
public void init() throws Exception {
    // Trỏ đến token server
    ClusterClientAssignConfig assignConfig =
        new ClusterClientAssignConfig("sentinel-token-server", 18080);
    ClusterClientConfigManager.applyNewAssignConfig(assignConfig);

    // Timeout cho Netty request (ms)
    ClusterClientConfig clientConfig = new ClusterClientConfig().setRequestTimeout(20);
    ClusterClientConfigManager.applyNewConfig(clientConfig);

    // Đăng ký local fallback rules (dùng khi token server down)
    List<FlowRule> localRules = Collections.singletonList(
        buildLocalFallbackRule("Target_server_payment", 333) // 2000 / 6 services
    );
    FlowRuleManager.loadRules(localRules);
}
```

Không cần logic phức tạp. Không cần biết machineId. Không cần Nacos cho client nếu địa chỉ server cố định.

---

## Flow Rule Cần Cấu Hình Trên Token Server

```json
[{
  "resource": "Target_server_payment",
  "grade": 1,
  "count": 2000,
  "clusterMode": true,
  "clusterConfig": {
    "flowId": 101,
    "thresholdType": 0,
    "fallbackToLocalWhenFail": true
  }
}]
```

- `grade: 1` = QPS mode (không phải concurrent)
- `count: 2000` = global threshold cho toàn cluster
- `thresholdType: 0` = `FLOW_THRESHOLD_GLOBAL` — 2000 là tổng, dù có 6 hay 60 services
- `fallbackToLocalWhenFail: true` — khi token server down, từng service tự giới hạn ở 333/s

Namespace trên Nacos: `payment-cluster-flow-rules`

---

## Annotation Trong Mỗi Service

```java
@SentinelResource(
    value = "Target_server_payment",
    blockHandler = "handleBlocked",
    fallback = "handleFallback"
)
public PaymentResult callTarget_server(PaymentRequest request) {
    return Target_serverClient.execute(request);
}

// Bị rate limit (BLOCKED token)
public PaymentResult handleBlocked(PaymentRequest request, BlockException ex) {
    metrics.increment("Target_server.rate_limited");
    return PaymentResult.rateLimited("Hệ thống bận, vui lòng thử lại");
}

// Target_server exception
public PaymentResult handleFallback(PaymentRequest request, Throwable t) {
    return PaymentResult.serviceUnavailable();
}
```

Sentinel tự động xin token trước khi gọi method, không cần code thêm gì trong business logic.

---

## Dependencies

### Token Server (pom.xml riêng)

```xml
<dependency>
    <groupId>com.alibaba.csp</groupId>
    <artifactId>sentinel-cluster-server-default</artifactId>
</dependency>
<dependency>
    <groupId>com.alibaba.csp</groupId>
    <artifactId>sentinel-datasource-nacos</artifactId>
</dependency>
```

### Mỗi Microservice

```xml
<!-- Cluster client -->
<dependency>
    <groupId>com.alibaba.csp</groupId>
    <artifactId>sentinel-cluster-client-default</artifactId>
</dependency>
<!-- Annotation support -->
<dependency>
    <groupId>com.alibaba.csp</groupId>
    <artifactId>sentinel-annotation-aspectj</artifactId>
</dependency>
<!-- Spring Boot starter (nếu dùng Spring) -->
<dependency>
    <groupId>com.alibaba.cloud</groupId>
    <artifactId>spring-cloud-starter-alibaba-sentinel</artifactId>
</dependency>
```

`sentinel-datasource-redis` **không cần** — Redis datasource chỉ push rule config, không làm distributed counting.

---

## Fail-Safe Khi Token Server Down

```
Token server crash
        │
        ▼
Netty connection timeout (20ms)
        │
        ▼
Client nhận TokenResultStatus.FAIL
        │
        ▼ (fallbackToLocalWhenFail = true)
Áp dụng local FlowRule: 333/s/service
        │
        ▼
Target_server nhận tổng: 6 × 333 = ~2000/s  ← vẫn được bảo vệ
```

Hệ thống chạy **degraded mode** tự động, không cần operator can thiệp. Khi token server recover, clients tự reconnect và chuyển lại cluster mode.

---

## Rủi Ro Và Cách Xử Lý

### SPOF — Token Server Là Single Point

| Biện pháp | Chi tiết |
|-----------|----------|
| Fallback to local | `fallbackToLocalWhenFail=true` + local rule 333/s/service |
| K8s deployment | `replicas: 1`, `restartPolicy: Always`, liveness probe |
| Fast restart | Token server là pure Netty, không có state cần persist, restart < 5s |
| Monitoring | Alert ngay khi token server down (Prometheus + Alertmanager) |

Token server **stateless về persistence** — crash và restart không mất gì vì counter là in-memory sliding window, reset sau 1s là acceptable.

### Latency Overhead

| Scenario | Latency thêm |
|----------|-------------|
| Same K8s namespace | ~0.5 - 1ms |
| Same datacenter | ~1 - 3ms |
| Timeout khi server down | 20ms (sau đó fallback ngay) |

Với payment SLA thường 200-500ms, overhead này không đáng kể.

---

## So Sánh Với Semaphore Cũ

| Tiêu chí | Semaphore Local | Sentinel Cluster Standalone |
|----------|----------------|----------------------------|
| Global counter | Không | Có — tập trung tại token server |
| Fair distribution | Không | Có — first-come-first-served toàn cluster |
| Đơn vị đo | Concurrent (sai) | QPS req/s (đúng) |
| Circuit breaker | Không | Có thể thêm CircuitBreakerRule |
| Fallback khi quá tải | Phải tự code | `blockHandler` trong annotation |
| Monitor | Không | Sentinel Dashboard real-time |
| Thêm service mới | Phải config thêm Semaphore | Chỉ cần trỏ client đến token server |

---

## Checklist Triển Khai

- [ ] Tạo project `sentinel-token-server` (Spring Boot, không cần business logic)
- [ ] Implement `TokenServerInitFunc` với Nacos datasource (hoặc hardcode nếu đơn giản hơn)
- [ ] Đẩy FlowRule JSON vào Nacos namespace `payment-cluster-flow-rules`
- [ ] Mỗi service: thêm dependency `sentinel-cluster-client-default`
- [ ] Mỗi service: implement `ClusterClientInitFunc` trỏ đến `sentinel-token-server:18080`
- [ ] Mỗi service: thêm `@SentinelResource` trên method gọi Target_server
- [ ] Mỗi service: đặt local fallback rule 333/s (cho trường hợp server down)
- [ ] Deploy token server lên K8s/VM trước, đảm bảo healthy
- [ ] Load test: kiểm tra Target_server chỉ nhận <= 2000/s khi 6 services đều bắn full tải
- [ ] Chaos test: kill token server → verify fallback hoạt động, Target_server không bị over-limit
- [ ] Setup Sentinel Dashboard kết nối vào token server để monitor
