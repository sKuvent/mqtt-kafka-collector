# Operations, metrics and delivery limits

## Delivery prerequisites

At-least-once delivery for QoS 1 requires a stable MQTT client ID, a persistent MQTT session (`cleanStart=false`), sufficient broker queue capacity, existing Kafka topics and a supervisor that restarts the collector after a failure.

On a graceful shutdown, MQTT disconnects before Kafka drains. A record stored before a lost PUBACK can be delivered again. There is no disk spool or exactly-once transaction across MQTT and Kafka; the Paho state directory does not replace durable broker queues.

QoS-1 overload, Kafka failure, MQTT connection loss, a Kafka send error or a failed PUBACK causes a nonzero exit. Configure restart backoff. A permanently oversized QoS-1 message can repeatedly block recovery, so align collector and Kafka size limits.

## Metrics

Counters reset on process restart. Metrics have no topic or device labels, preventing unbounded Prometheus time-series cardinality.

| Metric | Meaning |
| --- | --- |
| `collector_received_total` | MQTT callbacks, including rejected records |
| `collector_delivered_total` | Successful Kafka delivery callbacks |
| `collector_mqtt_acked_total` | Completed local QoS-1 PUBACK calls |
| `collector_rejected_total` | Records rejected before Kafka submission |
| `collector_qos0_unarchived_total` | Observed QoS-0 rejection or failed archive attempt |
| `collector_delivery_errors_total`, `collector_send_errors_total` | Kafka delivery and immediate send failures |
| `collector_ack_errors_total`, `collector_stale_acks_skipped_total` | Failed or skipped QoS-1 acknowledgements |
| `collector_overflow_total` | Admission rejection due to byte or record limit |
| `collector_pending_records`, `collector_pending_bytes` | Current admitted in-flight records and estimated bytes |
| `collector_buffer_limit_bytes`, `collector_buffer_limit_records` | Configured admission limits |
| `collector_ready`, `collector_connected`, `collector_subscribed`, `collector_kafka_ready` | Current state gauges |
| `collector_fatal` | Fatal failure state, separate from normal shutdown |

`received_total - delivered_total` is not a loss count. It includes in-flight messages, restarts, retries and possible duplicates. Persist source sequence IDs for an independent archive audit.

## Resources and Kubernetes

The default 16 MiB pending budget is admission control, not a JVM heap cap. Kafka has its own producer buffer, and the JVM, HTTP server and native compression use additional memory. The example 384 MiB container limit is a tested starting point, not a workload benchmark.

Use a `Recreate` deployment strategy, a writable persistent state volume, non-root execution and writable `/tmp` even with a read-only root filesystem. The image runs as UID/GID 10001. Allow at least 35 seconds for normal shutdown with the default 25-second Kafka drain.
