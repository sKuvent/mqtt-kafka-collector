package dev.mqttkafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

class ReadinessControllerTest {
    @Test
    void reportsReadyOnlyWhenCollectorIsReady() {
        AtomicInteger calls = new AtomicInteger();
        ReadinessController controller = new ReadinessController(() -> calls.getAndIncrement() > 0);

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, controller.ready().getStatusCode());
        assertEquals(HttpStatus.OK, controller.ready().getStatusCode());
    }
}
