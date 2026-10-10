package dev.mqttkafka;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

/** Keep JVM termination out of message processing so the failure path is testable. */
@Component
public class FatalShutdown {
    private final ConfigurableApplicationContext context;

    FatalShutdown(ConfigurableApplicationContext context) { this.context = context; }

    public void request() {
        Thread thread = new Thread(() -> {
            try { context.close(); }
            finally {
                // A fatal delivery error must be visible to the supervisor.  In
                // particular, do not let Spring's context shutdown turn this
                // path into a successful (exit code 0) termination.
                Runtime.getRuntime().halt(1);
            }
        }, "collector-fatal-exit");
        thread.start();
    }
}
