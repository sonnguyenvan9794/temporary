package com.example.mulesoft;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Giả lập MuleSoft + Grafana Alert (theo huong_mulesoft.md - Hướng 1).
 *
 * MuleSoft đứng giữa service-order và Downstream:
 *  - Forward request xuống Downstream
 *  - Track error rate từ 429/503/timeout (thay thế Prometheus + Grafana)
 *  - AIMD mỗi 5s → push limit lên Sentinel Token Server
 *  - Token Server tự đẩy quota mới xuống service-order qua Netty
 */
@SpringBootApplication
@EnableScheduling
@RestController
public class MuleSoftApplication {

    @Value("${downstream.url:http://localhost:8081}")
    private String downstreamUrl;

    @Value("${sentinel.server.url:http://localhost:8721}")
    private String sentinelServerUrl;

    private RestTemplate restTemplate;

    // Metrics tổng
    private final AtomicLong totalForwarded = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    // Cửa sổ 5s cho AIMD (giả lập Grafana alert window)
    private final AtomicLong windowErrors = new AtomicLong(0);
    private final AtomicLong windowSuccess = new AtomicLong(0);

    // Limit hiện tại do AIMD tính — đồng bộ với Token Server
    private volatile double currentLimit = 250.0;

    public static void main(String[] args) {
        SpringApplication.run(MuleSoftApplication.class, args);
    }

    @PostConstruct
    public void init() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(2000);
        factory.setReadTimeout(3000);
        this.restTemplate = new RestTemplate(factory);
    }

    // -------------------------------------------------------------------------
    // Forward request — đây là nơi MuleSoft thấy lỗi Downstream trực tiếp
    // -------------------------------------------------------------------------

    @PostMapping("/process")
    public ResponseEntity<String> process(@RequestBody String body) {
        totalForwarded.incrementAndGet();
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(
                    downstreamUrl + "/process", body, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                windowSuccess.incrementAndGet();
            } else {
                // 429, 503, ... → đây chính là tín hiệu Downstream yếu
                windowErrors.incrementAndGet();
                totalErrors.incrementAndGet();
            }
            return ResponseEntity.status(response.getStatusCode()).body(response.getBody());

        } catch (HttpStatusCodeException e) {
            windowErrors.incrementAndGet();
            totalErrors.incrementAndGet();
            return ResponseEntity.status(e.getStatusCode()).body(e.getResponseBodyAsString());
        } catch (Exception e) {
            // Timeout / connection error → Downstream đang yếu
            windowErrors.incrementAndGet();
            totalErrors.incrementAndGet();
            return ResponseEntity.status(503).body("downstream error: " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // AIMD Controller — giả lập Grafana Alert (Hướng 1, huong_mulesoft.md)
    //
    // Mỗi 5s:
    //   error > 10% → DEGRADED  → limit × 0.5  (giảm nhanh)
    //   error > 30% → CRITICAL  → limit × 0.25 (giảm mạnh)
    //   error < 1%  → RECOVERED → limit + 50   (tăng chậm, tránh thundering herd)
    //
    // → push limit mới lên Sentinel Token Server
    // → Token Server tự đẩy quota xuống service-order qua Netty
    // -------------------------------------------------------------------------

    @Scheduled(initialDelay = 15000, fixedDelay = 5000)
    public void aimdController() {
        long errors = windowErrors.getAndSet(0);
        long successes = windowSuccess.getAndSet(0);
        long total = errors + successes;
        if (total == 0) return;

        double errorRate = (double) errors / total;

        double newLimit = currentLimit;
        if (errorRate > 0.30) {
            // CRITICAL — giảm mạnh
            newLimit = Math.max(10, currentLimit * 0.25);
        } else if (errorRate > 0.10) {
            // DEGRADED — giảm nhanh
            newLimit = Math.max(10, currentLimit * 0.5);
        } else if (errorRate < 0.01) {
            // RECOVERED — tăng chậm, tránh thundering herd
            newLimit = currentLimit + 50;
        }

        if (newLimit != currentLimit) {
            currentLimit = newLimit;
            pushToSentinelTokenServer(currentLimit);
        }
    }

    private void pushToSentinelTokenServer(double limit) {
        try {
            restTemplate.postForEntity(
                    sentinelServerUrl + "/admin/limit?limit=" + limit + "&ns=service-order",
                    null, String.class);
        } catch (Exception e) {
            // Token Server chưa sẵn sàng, bỏ qua — service-order vẫn dùng fallback local
        }
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        long fwd = totalForwarded.get();
        long err = totalErrors.get();
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("forwarded", fwd);
        m.put("errors", err);
        m.put("errorRate", fwd == 0 ? 0 : (double) err / fwd);
        m.put("currentLimit", (int) currentLimit);
        return m;
    }
}

