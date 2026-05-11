package com.poc;

import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.SphU;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
@EnableScheduling
@RestController
public class ServiceOrderApplication {

    @Value("${mulesoft.url:http://localhost:8082}")
    private String muleSoftUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    // ── In-memory queue thay thế Kafka ──────────────────────────────
    private final BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(10000);

    // ── Metrics ─────────────────────────────────────────────────────
    private final AtomicLong produced    = new AtomicLong(0);
    private final AtomicLong processed   = new AtomicLong(0);
    private final AtomicLong throttled   = new AtomicLong(0);
    private final AtomicLong successCall = new AtomicLong(0);
    private final AtomicLong errorCall   = new AtomicLong(0);

    // ── Sentinel limit hiện tại ──────────────────────────────────────
    private volatile int currentLimit = 200;
    private static final int MAX_LIMIT = 500;
    private static final int MIN_LIMIT = 10;
    private static final String RESOURCE = "downstream-call";

    // ── Load Generator: RPS hiện tại ────────────────────────────────
    private volatile int targetRps = 100;
    private volatile boolean running = true;

    // ── History để vẽ biểu đồ ───────────────────────────────────────
    private final List<Map<String, Object>> history = Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {
        // Kết nối Sentinel Dashboard
        System.setProperty("project.name", "service-order");
        System.setProperty("csp.sentinel.dashboard.server", "sentinel-dashboard:8858");
        System.setProperty("server.port", "8080");
        SpringApplication.run(ServiceOrderApplication.class, args);
    }

    @PostConstruct
    public void init() {
        initSentinelRule(currentLimit);
        startLoadGenerator();
        startConsumers();
    }

    // ── Khởi tạo Sentinel rule ───────────────────────────────────────
    private void initSentinelRule(int limit) {
        FlowRule rule = new FlowRule(RESOURCE);
        rule.setCount(limit);
        rule.setGrade(RuleConstant.FLOW_GRADE_QPS);
        FlowRuleManager.loadRules(Collections.singletonList(rule));
    }

    // ── Load Generator: sinh message vào queue ───────────────────────
    private void startLoadGenerator() {
        Thread generator = new Thread(() -> {
            while (running) {
                try {
                    long startMs = System.currentTimeMillis();
                    int rps = targetRps;
                    for (int i = 0; i < rps; i++) {
                        messageQueue.offer("msg-" + produced.incrementAndGet());
                    }
                    // Ngủ phần còn lại của 1 giây
                    long elapsed = System.currentTimeMillis() - startMs;
                    long sleep = Math.max(0, 1000 - elapsed);
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        generator.setName("load-generator");
        generator.setDaemon(true);
        generator.start();
    }

    // ── Consumer threads: kéo từ queue, check Sentinel, gọi MuleSoft ─
    private void startConsumers() {
        int threads = 20;
        for (int i = 0; i < threads; i++) {
            Thread consumer = new Thread(() -> {
                while (running) {
                    String message = null;
                    try {
                        message = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (message == null) continue;

                        Entry entry = null;
                        try {
                            // Check Sentinel quota
                            entry = SphU.entry(RESOURCE);

                            // Gọi MuleSoft
                            ResponseEntity<String> response = restTemplate
                                .postForEntity(muleSoftUrl + "/mulesoft/process", null, String.class);

                            processed.incrementAndGet();
                            successCall.incrementAndGet();

                        } catch (BlockException e) {
                            // Sentinel block → đưa message lại queue
                            // (giống không commit Kafka offset)
                            throttled.incrementAndGet();
                            messageQueue.offer(message);
                            Thread.sleep(200); // tránh tight loop

                        } catch (HttpClientErrorException e) {
                            // 429 từ MuleSoft → đưa lại queue
                            errorCall.incrementAndGet();
                            messageQueue.offer(message);
                            Thread.sleep(100);

                        } catch (Exception e) {
                            // Lỗi khác → đưa lại queue
                            errorCall.incrementAndGet();
                            messageQueue.offer(message);
                            Thread.sleep(100);
                        } finally {
                            if (entry != null) entry.exit();
                        }

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            consumer.setName("consumer-" + i);
            consumer.setDaemon(true);
            consumer.start();
        }
    }

    // ── AIMD Controller: chạy mỗi 5s, tính error rate, update limit ──
    @Scheduled(fixedDelay = 5000)
    public void aimdControl() {
        long success = successCall.getAndSet(0);
        long error   = errorCall.getAndSet(0);
        long total   = success + error;

        double errorRate = total > 0 ? (double) error / total : 0.0;
        int oldLimit = currentLimit;

        if (errorRate > 0.1) {
            // Giảm 50% khi error rate > 10%
            currentLimit = Math.max(MIN_LIMIT, (int)(currentLimit * 0.5));
        } else if (errorRate > 0.05) {
            // Giảm 20% khi error rate > 5%
            currentLimit = Math.max(MIN_LIMIT, (int)(currentLimit * 0.8));
        } else if (errorRate < 0.01) {
            // Tăng dần +50 khi error rate < 1%
            currentLimit = Math.min(MAX_LIMIT, currentLimit + 50);
        }

        // Update Sentinel rule
        initSentinelRule(currentLimit);

        // Ghi vào history để vẽ biểu đồ
        Map<String, Object> point = new LinkedHashMap<>();
        point.put("time", System.currentTimeMillis());
        point.put("sentinelLimit", currentLimit);
        point.put("errorRate", Math.round(errorRate * 1000.0) / 10.0);
        point.put("queueSize", messageQueue.size());
        point.put("produced", produced.get());
        point.put("processed", processed.get());
        point.put("throttled", throttled.get());
        point.put("rps", targetRps);
        history.add(point);

        // Giữ tối đa 200 điểm (~ 16 phút)
        if (history.size() > 200) history.remove(0);

        System.out.printf("[AIMD] errorRate=%.1f%% limit: %d→%d queue=%d%n",
            errorRate * 100, oldLimit, currentLimit, messageQueue.size());
    }

    // ── REST API ─────────────────────────────────────────────────────

    // Metrics realtime cho dashboard
    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("time", System.currentTimeMillis());
        m.put("sentinelLimit", currentLimit);
        m.put("queueSize", messageQueue.size());
        m.put("produced", produced.get());
        m.put("processed", processed.get());
        m.put("throttled", throttled.get());
        m.put("targetRps", targetRps);
        m.put("history", history);
        return m;
    }

    // Thay đổi RPS của load generator
    @PostMapping("/admin/rps")
    public ResponseEntity<String> setRps(@RequestParam int rps) {
        this.targetRps = Math.max(0, rps);
        return ResponseEntity.ok("RPS set to " + this.targetRps);
    }

    // Thay đổi downstream limit (qua MuleSoft fake → Downstream fake)
    @PostMapping("/admin/downstream-limit")
    public ResponseEntity<String> setDownstreamLimit(@RequestParam int limit) {
        try {
            restTemplate.postForEntity(
                muleSoftUrl.replace("8082", "8081") + "/admin/limit?limit=" + limit,
                null, String.class);
            return ResponseEntity.ok("Downstream limit set to " + limit);
        } catch (Exception e) {
            // Gọi thẳng downstream-fake
            return ResponseEntity.ok("Set limit " + limit + " (check downstream-fake logs)");
        }
    }

    // Chạy kịch bản test tự động
    @PostMapping("/admin/run-scenario")
    public ResponseEntity<String> runScenario() {
        new Thread(() -> {
            try {
                System.out.println("=== SCENARIO START ===");

                // Phase 1: Bình thường
                System.out.println("[Phase 1] Normal load: 200 req/s, downstream limit=100");
                targetRps = 200;
                setDownstreamLimit(100);
                Thread.sleep(30000); // 30s

                // Phase 2: Tăng tải đột ngột
                System.out.println("[Phase 2] Spike: 500 req/s");
                targetRps = 500;
                Thread.sleep(30000); // 30s

                // Phase 3: Downstream yếu hơn
                System.out.println("[Phase 3] Downstream weakened: limit=20");
                setDownstreamLimit(20);
                Thread.sleep(30000); // 30s

                // Phase 4: Downstream hồi phục
                System.out.println("[Phase 4] Recovery: downstream limit=100, load=200");
                setDownstreamLimit(100);
                targetRps = 200;
                Thread.sleep(30000); // 30s

                System.out.println("=== SCENARIO END ===");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();

        return ResponseEntity.ok("Scenario started - watch dashboard at http://localhost:8080/dashboard");
    }
}
