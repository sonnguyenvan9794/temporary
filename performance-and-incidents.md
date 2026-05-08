# Hiệu Năng, Sự Cố Và Cách Xử Lý — Sentinel Cluster Standalone

Tài liệu này mô tả các vấn đề hiệu năng và sự cố có thể xảy ra khi vận hành Sentinel Cluster Standalone trong môi trường payment (15.000 req/s peak, 6 microservices, Target_server capacity 2.000 req/s). Mỗi vấn đề được truy xuất từ source code thực tế của Sentinel.

---

## 1. Token Server Crash (SPOF)

### Mô tả

Token server là single point of failure. Khi process chết, tất cả 6 services mất khả năng xin token.

### Diễn biến từ source code

Khi connection bị đứt, `TokenClientHandler.channelUnregistered()` được gọi:

```java
// TokenClientHandler.java:97-101
public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    currentState.set(ClientConstants.CLIENT_STATUS_OFF);
    disconnectCallback.run();  // kích hoạt reconnect loop
}
```

`disconnectCallback` trong `NettyTransportClient` lên lịch reconnect với **exponential backoff**:

```java
// NettyTransportClient.java:142-156
SCHEDULER.schedule(() -> {
    startInternal(); // thử reconnect
}, RECONNECT_DELAY_MS * (failConnectedTime.get() + 1), TimeUnit.MILLISECONDS);
// Lần 1: 2s, Lần 2: 4s, Lần 3: 6s, Lần 4: 8s...
```

Trong khoảng thời gian chưa reconnect xong, `isReady()` trả về `false`:

```java
// NettyTransportClient.java:203-205
public boolean isReady() {
    return channel != null && clientHandler != null && clientHandler.hasStarted();
}
```

`sendRequest()` ném `SentinelClusterException("Client not ready")` → client nhận `TokenResultStatus.FAIL`.

### Hành vi khi FAIL

Với `fallbackToLocalWhenFail = true`, mỗi service tự áp dụng local FlowRule:

```
Bình thường:    6 services × N token/s = 2000/s tổng (kiểm soát bởi server)
Khi server down: 6 services × 333/s local = ~2000/s tổng (kiểm soát bởi từng service)
```

Target_server vẫn được bảo vệ, hệ thống chạy degraded mode tự động.

### Cách xử lý

| Biện pháp | Chi tiết |
|-----------|----------|
| Local fallback rule | Set `count = 333` cho local FlowRule (2000 / 6) |
| K8s liveness probe | Restart pod ngay khi health check fail |
| Alert | Cảnh báo khi `sentinel.cluster.client.status = OFF` kéo dài > 10s |
| Monitoring | Track số request đang dùng local fallback vs cluster mode |

**Thời gian recovery thực tế**: Token server restart < 5s (pure Netty, không có state cần restore), client reconnect sau 2s → tổng downtime cluster mode ~7-10s.

---

## 2. Network Partition — Client Không Reach Được Server

### Mô tả

Server vẫn chạy nhưng network giữa client và server bị gián đoạn (network policy, firewall, pod eviction).

### Diễn biến từ source code

Mỗi token request có timeout cứng:

```java
// NettyTransportClient.java:224-226
if (!promise.await(ClusterClientConfigManager.getRequestTimeout())) {
    throw new SentinelClusterException(ClusterErrorMessages.REQUEST_TIME_OUT);
}
// DEFAULT_REQUEST_TIMEOUT = 20ms (ClusterConstants.java:44)
```

Trong 20ms timeout, mỗi request đang inflight bị block tại `promise.await()`. Với 15.000 req/s và 20ms timeout:

```
Concurrent inflight tokens = 15.000 × 0.020 = 300 promises trong PROMISE_MAP cùng lúc
```

Sau 20ms tất cả throw exception → `TokenResultStatus.FAIL` → fallback to local.

### Rủi ro: PROMISE_MAP Tích Lũy

`TokenClientPromiseHolder` dùng `ConcurrentHashMap<Integer, SimpleEntry>`:

```java
// TokenClientPromiseHolder.java:32
private static final Map<Integer, SimpleEntry<ChannelPromise, ClusterResponse>> PROMISE_MAP
    = new ConcurrentHashMap<>();
```

Nếu timeout xảy ra nhưng response đến muộn (sau khi entry đã bị `remove()`), entry cũ bị discard an toàn. Tuy nhiên nếu nhiều requests timeout liên tục, `PROMISE_MAP` có thể tích lũy entries chưa được clean trong khoảng thời gian ngắn — theo dõi heap nếu thấy memory tăng bất thường.

### Cách xử lý

| Biện pháp | Chi tiết                                                                          |
|-----------|-----------------------------------------------------------------------------------|
| Request timeout phù hợp | Set 20ms cho payment system (đủ nhanh để fallback, không quá ngắn gây false fail) |
| Network policy | Đảm bảo services và token server cùng namespace K8s, không có network policy block |
| Readiness probe | Token server expose `/health`, K8s không route traffic nếu chưa sẵn sàng          |
| Alert trên FAIL rate | Cảnh báo khi `TokenResultStatus.FAIL` > 1% trong 30s                              |

---

## 3. Sliding Window Boundary Burst

### Mô tả

`ClusterMetricLeapArray` dùng sliding window 10 buckets × 100ms. Tại ranh giới giữa các bucket, có thể có burst ngắn vượt limit.

### Cơ chế từ source code

```java
// ClusterFlowChecker.java:67-69
double latestQps = metric.getAvg(ClusterFlowEvent.PASS);  // trung bình 1 giây qua
double globalThreshold = calcGlobalThreshold(rule);        // 2000
double nextRemaining = globalThreshold - latestQps - acquireCount;
```

`getAvg()` tính tổng của tất cả buckets hiện hành chia cho interval (1 giây). Khi bucket cũ nhất bị reset (mỗi 100ms), latestQps giảm đột ngột, tạo "khoảng trống" cho burst:

```
t=0ms:   bucket[0..9] đều đầy → avg = 2000/s → chặt chẽ
t=100ms: bucket[0] bị reset → avg có thể giảm xuống ~1800/s
         → trong 100ms tiếp theo có thể pass thêm ~200 requests
         → Target_server nhận tới 2200/s trong burst 100ms
```

### Mức độ ảnh hưởng

Burst tối đa = 1 bucket / 10 buckets = **10% overshoot** trong 100ms. Với limit 2000/s, Target_server có thể nhận ~2200/s trong 100ms mỗi giây. Target_server với buffer thông thường sẽ xử lý được.

### Cách xử lý

```java
// Giảm threshold xuống để bù cho burst
// ClusterServerConfigManager có exceedCount, default = 1.0
// Set effective threshold = 2000 * 0.9 = 1800 để margin 10%
clusterConfig.setFallbackToLocalWhenFail(true);

// Hoặc set count thấp hơn capacity Target_server
rule.setCount(1800); // thay vì 2000, để burst không vượt 2000
```

Ngoài ra tăng `sampleCount` lên 20 (20 buckets × 50ms) để bucket nhỏ hơn, burst nhỏ hơn — nhưng cần cân nhắc CPU.

---

## 4. Latency Overhead Của Token Request

### Mô tả

Mỗi request đến Target_server phải thêm 1 Netty round-trip đến token server. Nếu token server đặt xa hoặc đang tải cao, latency tăng.

### Số liệu từ source code

```java
// NettyTransportServer.java:98-101
.childOption(ChannelOption.TCP_NODELAY, true)       // tắt Nagle → giảm latency
.childOption(ChannelOption.SO_SNDBUF, 32 * 1024)
.childOption(ChannelOption.SO_RCVBUF, 32 * 1024)
```

```java
// NettyTransportClient.java:94-95
.option(ChannelOption.TCP_NODELAY, true)             // tắt Nagle trên client
.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT) // pooled buffer
```

Cả hai phía đều dùng `TCP_NODELAY` và `PooledByteBufAllocator` → latency tối thiểu.

### Thực tế theo môi trường

| Môi trường | RTT token request |
|------------|-------------------|
| Same pod (localhost) | < 0.1ms |
| Same K8s node | 0.2 - 0.5ms |
| Same datacenter, khác node | 0.5 - 2ms |
| Cross-AZ | 3 - 8ms |

Với Payment system SLA 200-500ms và Target_server thường mất 50-200ms, overhead 1-2ms là chấp nhận được.

### Cách xử lý

- Deploy token server **cùng K8s cluster**, **cùng availability zone** với các services
- Nếu cross-AZ là bắt buộc: tăng `requestTimeout` từ 20ms lên 30-50ms
- Monitor P99 latency của token request, không chỉ mean

---

## 5. Token Server Quá Tải (High Connection Count)

### Mô tả

Với 6 services, mỗi service có thể chạy nhiều instances (pods). 6 services × 10 pods = 60 connections. Token server cần xử lý ~15.000 token requests/giây từ 60 connections.

### Giới hạn từ source code

```java
// NettyTransportServer.java:53-55
private static final int DEFAULT_EVENT_LOOP_THREADS =
    Math.max(1, SystemPropertyUtil.getInt(
        "io.netty.eventLoopThreads",
        Runtime.getRuntime().availableProcessors() * 2
    ));
```

Với server 4 CPU: 8 event loop threads. Netty NIO có thể xử lý hàng ngàn connections/thread.

```java
// NettyTransportServer.java:82-83
.option(ChannelOption.SO_BACKLOG, 128) // max pending connections trong OS queue
```

`SO_BACKLOG = 128`: nếu có > 128 services đang đồng loạt kết nối lần đầu (cold start), OS sẽ drop connection request. Không phải vấn đề khi đã connected ổn định.

### Throughput thực tế

Token server chỉ làm 1 việc: nhận request → check counter trong memory → trả response. Mỗi request < 1μs CPU time. Với 8 event loop threads, token server có thể xử lý **hàng trăm nghìn** token requests/giây — 15.000/s là không đáng kể.

### Cách xử lý

- Đặt resource request/limit cho token server pod: 0.5 CPU / 512Mi RAM là đủ
- Nếu cần scale (> 100 services kết nối), tăng event loop threads qua JVM property `-Dio.netty.eventLoopThreads=16`
- Monitor: `sentinel_cluster_server_connection_count`, alert nếu > 200

---

## 6. Cold Start — Kết Nối Lần Đầu Chưa Ready

### Mô tả

Khi service khởi động, `DefaultClusterTokenClient` chưa kết nối xong với token server nhưng traffic đã đến.

### Diễn biến từ source code

`connectTimeout` mặc định = **10 giây**:

```java
// ClusterConstants.java:45
public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 10 * 1000;
```

Trong 10s đầu sau khi service start, nếu connection chưa established:

```java
// NettyTransportClient.java:208-212
public ClusterResponse sendRequest(ClusterRequest request) throws Exception {
    if (!isReady()) {
        throw new SentinelClusterException(ClusterErrorMessages.CLIENT_NOT_READY);
    }
    ...
}
```

→ Mọi token request throw exception → `TokenResultStatus.FAIL` → fallback to local rule (333/s).

### Hệ quả

Trong khoảng 1-3s sau startup (trước khi Netty connect xong), service dùng local rule. Đây là behavior **đúng và an toàn** — Target_server vẫn được bảo vệ.

### Cách xử lý

- Giảm `connectTimeout` xuống 3s nếu token server cùng cluster (không cần 10s)
- Trong K8s: dùng **readiness probe** delay để service không nhận traffic trước khi cluster mode ready
- Nếu muốn biết trạng thái: check `DefaultClusterTokenClient.getState() == CLIENT_STATUS_STARTED`

---

## 7. Rule Không Đồng Bộ Giữa Server Và Client

### Mô tả

Rule trên token server (`ClusterFlowRule`) và local fallback rule trên mỗi service phải được quản lý nhất quán. Nếu update rule trên server mà quên update local fallback, khi server down thì limit sai.

### Ví dụ nguy hiểm

```
Scenario: Tăng Target_server capacity lên 3000/s
- Update cluster rule: count = 3000  ✓
- Quên update local fallback: count = 333 (vẫn là 2000/6)  ✗

Khi server down:
  6 × 333 = 2000/s → Target_server nhận 2000/s, lãng phí 1000/s capacity
  (Không crash nhưng performance kém)

Scenario: Giảm Target_server capacity xuống 1200/s
- Update cluster rule: count = 1200  ✓
- Quên update local fallback: count = 333  ✗

Khi server down:
  6 × 333 = 2000/s → Target_server bị vượt limit → CRASH
```

### Cách xử lý

- **Quản lý cả 2 loại rule từ cùng 1 chỗ** (Nacos hoặc config service)
- Local fallback rule = `cluster_threshold / service_count`, tính tự động khi update
- Alert khi `local_rule_count * service_count > Target_server_capacity`
- Viết runbook: "Khi thay đổi Target_server capacity, phải update cả cluster rule VÀ local fallback"

---

## 8. Request ID Overflow (Lý Thuyết)

### Mô tả

`NettyTransportClient` dùng `AtomicInteger` làm request ID (xid), wrap around khi đạt max:

```java
// NettyTransportClient.java:239-244
private int getCurrentId() {
    int pre, next;
    do {
        pre = idGenerator.get();
        next = pre >= MAX_ID ? MIN_ID : pre + 1;  // MAX_ID = 999_999_999
    } while (!idGenerator.compareAndSet(pre, next));
    return next;
}
```

Tại 15.000 req/s, thời gian để ID wrap around = 999.999.999 / 15.000 ≈ **66.666 giây ≈ 18 giờ**.

### Rủi ro

Nếu 2 requests có cùng xid đang tồn tại trong `PROMISE_MAP` cùng lúc, response của request này có thể complete promise của request kia → sai kết quả.

Tuy nhiên rủi ro này cực kỳ thấp: xid wrap sau 18 giờ, mỗi request chỉ tồn tại 20ms trong PROMISE_MAP. Để collision xảy ra, phải có đúng 1 request cũ với xid X chưa hoàn thành sau 18 giờ — thực tế không xảy ra.

### Cách xử lý

Không cần xử lý. Chỉ cần biết để không lo lắng khi thấy xid reset về 1 trong log.

---

## 9. Sentinel Dashboard Không Kết Nối Được

### Mô tả

Sentinel Dashboard kết nối đến các service qua transport port (mặc định 8719). Nếu không mở port này, Dashboard không hiển thị metrics — nhưng **rate limiting vẫn hoạt động bình thường** vì Dashboard là monitoring tool, không nằm trong critical path.

### Cách xử lý

- Trong K8s: mở port 8719 trong service definition hoặc dùng `hostPort`
- Token server expose thêm port 8719 để Dashboard có thể xem cluster-level metrics
- Dashboard không bắt buộc cho production, chỉ cần trong development/troubleshooting

---

## Tổng Hợp

| Sự Cố | Xác Suất | Impact | Recovery | Tự Động? |
|-------|----------|--------|----------|----------|
| Token server crash | Thấp | Xuống local mode | ~10s | Có (reconnect + fallback) |
| Network partition | Trung bình | Xuống local mode | Ngay khi timeout 20ms | Có |
| Sliding window burst | Luôn xảy ra | Target_server nhận +10% trong 100ms | N/A (by design) | N/A |
| Token latency cao | Thấp | +2-8ms per request | Không cần | N/A |
| Cold start | Luôn xảy ra | Dùng local mode 1-3s | Tự kết nối | Có |
| Rule không đồng bộ | Tùy ops | Limit sai khi degraded | Manual update | Không |
| Token server overload | Rất thấp | N/A tại 15k req/s | Tăng CPU | Không |
