package com.example.downstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
@RestController
public class DownstreamApplication {

    private volatile int concurrentLimit = 100;
    private volatile Semaphore semaphore = new Semaphore(100);

    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong throttledRequests = new AtomicLong(0);
    private final AtomicLong processedRequests = new AtomicLong(0);

    public static void main(String[] args) {
        SpringApplication.run(DownstreamApplication.class, args);
    }

    @PostMapping("/process")
    public ResponseEntity<String> process(@RequestBody String body) {
        totalRequests.incrementAndGet();
        boolean acquired = semaphore.tryAcquire();
        if (!acquired) {
            throttledRequests.incrementAndGet();
            return ResponseEntity.status(429).body("Too Many Requests");
        }
        try {
            // Simulate 50ms processing
            Thread.sleep(50);
            processedRequests.incrementAndGet();
            return ResponseEntity.ok("OK:" + body);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(500).body("Interrupted");
        } finally {
            semaphore.release();
        }
    }

    @PostMapping("/admin/limit")
    public ResponseEntity<String> setLimit(@RequestParam int limit) {
        concurrentLimit = limit;
        semaphore = new Semaphore(limit);
        return ResponseEntity.ok("concurrent limit set to " + limit);
    }

    @GetMapping("/admin/stats")
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("concurrentLimit", concurrentLimit);
        m.put("totalRequests", totalRequests.get());
        m.put("processedRequests", processedRequests.get());
        m.put("throttledRequests", throttledRequests.get());
        m.put("availablePermits", semaphore.availablePermits());
        return m;
    }
}
