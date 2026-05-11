package com.example.serviceorder;

import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.SphU;
import com.alibaba.csp.sentinel.cluster.ClusterStateManager;
import com.alibaba.csp.sentinel.cluster.client.config.ClusterClientAssignConfig;
import com.alibaba.csp.sentinel.cluster.client.config.ClusterClientConfig;
import com.alibaba.csp.sentinel.cluster.client.config.ClusterClientConfigManager;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.alibaba.csp.sentinel.slots.block.ClusterRuleConstant;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.ClusterFlowConfig;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.client.SimpleClientHttpRequestFactory;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service Order — Kafka consumer + Sentinel gate.
 *
 * Nhiệm vụ:
 *  - Sinh load (giả lập Kafka consumer)
 *  - Kiểm tra quota với Sentinel Token Server TRƯỚC khi gọi MuleSoft
 *  - Nếu bị block → requeue (không mất message, giống không commit Kafka offset)
 *
 * AIMD đã chuyển sang mulesoft-fake (theo huong_mulesoft.md).
 * service-order chỉ enforce limit, không tự tính.
 */
@SpringBootApplication
@RestController
public class ServiceOrderApplication {

    private static final String RESOURCE = "downstream-call";
    private static final int QUEUE_CAPACITY = 50000;
    private static final int CONSUMER_THREADS = 200;

    private final BlockingQueue<String> queue = new LinkedBlockingQueue<String>(QUEUE_CAPACITY);

    private final AtomicLong produced  = new AtomicLong(0);
    private final AtomicLong processed = new AtomicLong(0);
    private final AtomicLong throttled = new AtomicLong(0);

    private volatile int targetRps = 200;

    private final List<Map<String, Object>> history = new CopyOnWriteArrayList<Map<String, Object>>();

    private RestTemplate restTemplate;

    @Value("${mulesoft.url:http://localhost:8082}")
    private String muleSoftUrl;

    @Value("${downstream.url:http://localhost:8081}")
    private String downstreamUrl;

    @Value("${sentinel.server.url:http://localhost:8721}")
    private String sentinelServerUrl;

    @Value("${sentinel.server.host:localhost}")
    private String sentinelServerHost;

    public static void main(String[] args) {
        SpringApplication.run(ServiceOrderApplication.class, args);
    }

    @PostConstruct
    public void init() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(2000);
        factory.setReadTimeout(3000);
        this.restTemplate = new RestTemplate(factory);

        // Kết nối tới Sentinel Token Server — nhận quota từ đây
        ClusterClientConfigManager.applyNewAssignConfig(
                new ClusterClientAssignConfig(sentinelServerHost, 18730)
        );
        ClusterClientConfigManager.applyNewConfig(
                new ClusterClientConfig().setRequestTimeout(20)
        );
        ClusterStateManager.applyState(ClusterStateManager.CLUSTER_CLIENT);

        // Đặt fallback rule local (khi Token Server tèo)
        initLocalFallbackRule(250);

        // Consumer threads
        ExecutorService executor = Executors.newFixedThreadPool(CONSUMER_THREADS);
        for (int i = 0; i < CONSUMER_THREADS; i++) {
            executor.submit(new Runnable() {
                @Override public void run() { consumerLoop(); }
            });
        }

        // Load generator (giả lập Kafka consumer poll)
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(new Runnable() {
                    @Override public void run() { generateLoad(); }
                }, 0, 1, TimeUnit.SECONDS);

        // History recorder cho dashboard
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(new Runnable() {
                    @Override public void run() { recordHistory(); }
                }, 2, 2, TimeUnit.SECONDS);
    }

    /**
     * Fallback rule local — chỉ dùng khi Token Server tèo (fallbackToLocalWhenFail=true).
     * Limit thực sự được quản lý bởi mulesoft-fake AIMD → Sentinel Token Server.
     */
    private void initLocalFallbackRule(double qps) {
        FlowRule rule = new FlowRule();
        rule.setResource(RESOURCE);
        rule.setGrade(RuleConstant.FLOW_GRADE_QPS);
        rule.setCount(qps);
        rule.setClusterMode(true);
        ClusterFlowConfig clusterConfig = new ClusterFlowConfig();
        clusterConfig.setFlowId(1L);
        clusterConfig.setThresholdType(ClusterRuleConstant.FLOW_THRESHOLD_GLOBAL);
        clusterConfig.setFallbackToLocalWhenFail(true);
        rule.setClusterConfig(clusterConfig);
        FlowRuleManager.loadRules(Collections.singletonList(rule));
    }

    private void generateLoad() {
        int rps = targetRps;
        for (int i = 0; i < rps; i++) {
            String msg = "order-" + produced.incrementAndGet();
            if (!queue.offer(msg)) {
                produced.decrementAndGet();
            }
        }
    }

    private void consumerLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                String msg = queue.take();
                Entry entry = null;
                try {
                    entry = SphU.entry(RESOURCE);
                    // Quota ok → gọi MuleSoft
                    callMuleSoft(msg);
                } catch (BlockException e) {
                    // Quota hết → requeue, giống "không commit Kafka offset"
                    throttled.incrementAndGet();
                    try {
                        queue.put(msg);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                } finally {
                    if (entry != null) entry.exit();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void callMuleSoft(String msg) {
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(
                    muleSoftUrl + "/process", msg, String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                processed.incrementAndGet();
            } else {
                // Lỗi từ MuleSoft → message được tính là đã xử lý (với lỗi)
                // MuleSoft sẽ tự tính vào error rate để điều chỉnh Sentinel
                processed.incrementAndGet();
            }
        } catch (Exception e) {
            // Connection error → không tính processed, message bị mất (không xảy ra trong POC)
        }
    }

    /** Đọc limit hiện tại từ Token Server (MuleSoft AIMD đang quản lý limit này). */
    private int getCurrentLimitFromServer() {
        try {
            Map<?, ?> status = restTemplate.getForObject(sentinelServerUrl + "/admin/status", Map.class);
            if (status != null) {
                Map<?, ?> limits = (Map<?, ?>) status.get("limits");
                if (limits != null && limits.containsKey("service-order")) {
                    return ((Number) limits.get("service-order")).intValue();
                }
            }
        } catch (Exception ignored) {
        }
        return -1;
    }

    private void recordHistory() {
        int limit = getCurrentLimitFromServer();
        Map<String, Object> point = new LinkedHashMap<String, Object>();
        point.put("time", System.currentTimeMillis());
        point.put("sentinelLimit", limit);
        point.put("queueSize", queue.size());
        point.put("produced", produced.get());
        point.put("processed", processed.get());
        point.put("throttled", throttled.get());
        point.put("targetRps", targetRps);
        history.add(point);
        while (history.size() > 300) {
            history.remove(0);
        }
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("sentinelLimit", getCurrentLimitFromServer());
        m.put("queueSize", queue.size());
        m.put("produced", produced.get());
        m.put("processed", processed.get());
        m.put("throttled", throttled.get());
        m.put("targetRps", targetRps);
        m.put("history", history);
        return m;
    }

    @PostMapping("/admin/rps")
    public ResponseEntity<String> setRps(@RequestParam int rps) {
        targetRps = rps;
        return ResponseEntity.ok("targetRps set to " + rps);
    }

    @PostMapping("/admin/downstream-limit")
    public ResponseEntity<String> setDownstreamLimit(@RequestParam int limit) {
        setDownstreamConcurrent(limit);
        return ResponseEntity.ok("downstream limit set to " + limit);
    }

    @PostMapping("/admin/run-scenario")
    public ResponseEntity<String> runScenario() {
        new Thread(new Runnable() {
            @Override public void run() { runAutoScenario(); }
        }, "scenario-thread").start();
        return ResponseEntity.ok("Auto scenario started");
    }

    private void runAutoScenario() {
        try {
            targetRps = 200; setDownstreamConcurrent(100); Thread.sleep(30_000);
            targetRps = 500;                                Thread.sleep(30_000);
            setDownstreamConcurrent(20);                    Thread.sleep(30_000);
            targetRps = 200; setDownstreamConcurrent(100); Thread.sleep(30_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void setDownstreamConcurrent(int limit) {
        try {
            restTemplate.postForEntity(downstreamUrl + "/admin/limit?limit=" + limit, null, String.class);
        } catch (Exception ignored) {
        }
    }
}
