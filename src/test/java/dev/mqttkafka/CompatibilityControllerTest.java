package dev.mqttkafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;

class CompatibilityControllerTest {
    @Test
    void exposesLivenessAndPrometheusAliases() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.counter("collector.received_total").increment();
        CompatibilityController controller = new CompatibilityController(registry);

        assertEquals("alive\n", controller.health());
        assertTrue(controller.metrics().contains("collector_received_total"));
    }
}
