# Configuration and TLS/SASL

Environment variables cover common connection settings. For limits, routing and Kafka
security properties, mount a read-only configuration file and set:

```text
SPRING_CONFIG_ADDITIONAL_LOCATION=file:/config/
```

```yaml
collector:
  max-pending-bytes: 16777216
  max-pending-messages: 10000
  max-payload-bytes: 1048576
  shutdown-drain-seconds: 25
  mqtt:
    receive-maximum: 100
    session-expiry-seconds: 86400
  kafka:
    client-id: archive-collector
    properties:
      '[security.protocol]': SASL_SSL
      '[sasl.mechanism]': SCRAM-SHA-512
      '[sasl.jaas.config]': '${KAFKA_SASL_JAAS_CONFIG}'
      '[ssl.truststore.location]': /certs/kafka-truststore.p12
      '[ssl.truststore.password]': '${KAFKA_TRUSTSTORE_PASSWORD}'
```

The property map is applied to the Kafka producer. Relevant client and security settings
are forwarded to the AdminClient. The collector enforces bootstrap servers, client ID,
serializers, keyed partitioning, `acks=all`, idempotence, retries and at most five
in-flight requests.

Store credentials and certificates in a secret manager or orchestrator secret and mount
them read-only. For MQTT TLS, set `MQTT_TLS=true` with a `tcp://` URI or use `ssl://`.
Paho uses the JVM truststore and keystore settings.

Configuration binding and forwarding are tested. An authenticated TLS/SASL broker
handshake is not part of the integration suite.
