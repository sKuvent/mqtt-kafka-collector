package dev.mqttkafka;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Provides a dedicated readiness endpoint for container orchestrators. */
@RestController
public class ReadinessController {
    private final CollectorReadiness collector;

    ReadinessController(CollectorReadiness collector) {
        this.collector = collector;
    }

    @GetMapping("/ready")
    ResponseEntity<String> ready() {
        return collector.isReady()
            ? ResponseEntity.ok("ready\n")
            : ResponseEntity.status(503).body("not ready\n");
    }
}
