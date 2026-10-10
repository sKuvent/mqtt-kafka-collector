# Architecture and ordering

The collector receives MQTT messages and writes their original payload bytes to Kafka.
For QoS 1, it sends PUBACK only after Kafka confirms storage. On Kafka or MQTT failure,
the process exits so an orchestrator can restart the persistent MQTT session.

The delivery model is at least once. If Kafka stores a record but the PUBACK is lost, the
broker can redeliver the MQTT message after restart. Consumers need a source event ID or
sequence number to deduplicate.

## Partitioning

Kafka preserves order inside a partition only. Use a stable key such as the full MQTT topic
or device ID so related messages map to one partition.

`murmur2` is the Apache Kafka Java producer default. `crc32` supports migration from
librdkafka's default behavior for non-empty keys. It gives the same partition only while
topic, key bytes, partition count and partitioner stay unchanged.

## Deployment boundaries

Run one active collector per MQTT client ID. A second instance with the same ID can
disconnect the first; different IDs independently archive the same subscription. The
collector has no leader election and no shared spool.
