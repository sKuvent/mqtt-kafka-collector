package dev.mqttkafka;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Short aliases used by common Kubernetes probes and Prometheus annotations. */
@RestController
public class CompatibilityController {
    private final PrometheusMeterRegistry prometheus;

    CompatibilityController(PrometheusMeterRegistry prometheus) {
        this.prometheus = prometheus;
    }

    @GetMapping(value = "/health", produces = MediaType.TEXT_PLAIN_VALUE)
    String health() {
        return "alive\n";
    }

    @GetMapping(value = "/metrics", produces = "text/plain; version=0.0.4")
    String metrics() {
        return prometheus.scrape();
    }
}
