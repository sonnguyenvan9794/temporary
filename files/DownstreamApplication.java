package com.poc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@RestController
public class DownstreamApplication {

    // Số request đang xử lý đồng thời
    private final AtomicInteger concurrent = new AtomicInteger(0);
    // Tổng request đã nhận
    private final AtomicInteger totalReceived = new AtomicInteger(0);
    // Tổng request bị reject
    private final AtomicInteger totalRejected = new AtomicInteger(0);

    // Giới hạn concurrent — có thể thay đổi động qua API
    private volatile int maxConcurrent = 50;

    public static void main(String[] args) {
        System.setProperty("server.port", "8081");
        SpringApplication.run(DownstreamApplication.class, args);
    }

    // Endpoint chính — giả lập xử lý
    @PostMapping("/api/process")
    public ResponseEntity<String> process() throws InterruptedException {
        totalReceived.incrementAndGet();
        int current = concurrent.incrementAndGet();
        try {
            if (current > maxConcurrent) {
                totalRejected.incrementAndGet();
                return ResponseEntity.status(429).body("Too Many Requests - concurrent: " + current + " max: " + maxConcurrent);
            }
            // Giả lập thời gian xử lý
            Thread.sleep(50);
            return ResponseEntity.ok("OK");
        } finally {
            concurrent.decrementAndGet();
        }
    }

    // Thay đổi giới hạn concurrent động — dùng trong test scenario
    @PostMapping("/admin/limit")
    public ResponseEntity<String> setLimit(@RequestParam int limit) {
        this.maxConcurrent = limit;
        return ResponseEntity.ok("Limit set to " + limit);
    }

    // Xem trạng thái hiện tại
    @GetMapping("/admin/stats")
    public Map<String, Object> stats() {
        return Map.of(
            "maxConcurrent", maxConcurrent,
            "currentConcurrent", concurrent.get(),
            "totalReceived", totalReceived.get(),
            "totalRejected", totalRejected.get()
        );
    }
}
