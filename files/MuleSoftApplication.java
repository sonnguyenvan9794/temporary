package com.poc;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
@RestController
public class MuleSoftApplication {

    @Value("${downstream.url:http://localhost:8081}")
    private String downstreamUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    // Metrics — service-order sẽ đọc qua /metrics
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong errorCount   = new AtomicLong(0);
    private final AtomicLong timeoutCount = new AtomicLong(0);

    public static void main(String[] args) {
        System.setProperty("server.port", "8082");
        SpringApplication.run(MuleSoftApplication.class, args);
    }

    // Forward request xuống Downstream
    @PostMapping("/mulesoft/process")
    public ResponseEntity<String> forward() {
        try {
            ResponseEntity<String> response = restTemplate
                .postForEntity(downstreamUrl + "/api/process", null, String.class);
            successCount.incrementAndGet();
            return response;
        } catch (HttpClientErrorException e) {
            // 429 từ Downstream → trả về upstream
            errorCount.incrementAndGet();
            return ResponseEntity.status(e.getStatusCode()).body(e.getMessage());
        } catch (Exception e) {
            // Timeout hoặc lỗi khác
            timeoutCount.incrementAndGet();
            return ResponseEntity.status(503).body("Downstream unavailable");
        }
    }

    // Metrics endpoint — service-order đọc để tính error rate
    @GetMapping("/metrics")
    public Map<String, Long> metrics() {
        return Map.of(
            "success", successCount.get(),
            "error",   errorCount.get(),
            "timeout", timeoutCount.get()
        );
    }

    // Reset metrics — AIMD controller gọi sau mỗi window
    @PostMapping("/metrics/reset")
    public void resetMetrics() {
        successCount.set(0);
        errorCount.set(0);
        timeoutCount.set(0);
    }
}
