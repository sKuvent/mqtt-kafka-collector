package dev.mqttkafka;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MqttDefaultFilePersistence;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("collector")
public class CollectorService implements HealthIndicator, CollectorReadiness {
    private static final Logger log = LoggerFactory.getLogger(CollectorService.class);
    private final CollectorProperties config;
    private final FatalShutdown fatalShutdown;
    private final Map<String, Counter> counters = new LinkedHashMap<>();
    private final AtomicBoolean fatal = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean sessionActive = new AtomicBoolean(false);
    private final TopicMapper mapper;
    private final ByteBudget budget;
    private final MeterRegistry meters;
    private final String metricsPrefix;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicBoolean healthy = new AtomicBoolean(false);
    private final AtomicBoolean kafkaReady = new AtomicBoolean(false);
    private final AtomicLong startedAtMillis = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong lastDeliveryAtMillis = new AtomicLong(0);
    private org.apache.kafka.clients.producer.Producer<byte[], byte[]> producer;
    private AdminClient admin;
    private ScheduledExecutorService kafkaProbe;
    private MqttAsyncClient mqtt;

    CollectorService(CollectorProperties config, TopicMapper mapper, MeterRegistry meters, FatalShutdown fatalShutdown) {
        this.config = config; this.mapper = mapper; this.meters = meters; this.fatalShutdown = fatalShutdown;
        this.metricsPrefix = config.metricsPrefix == null || config.metricsPrefix.isBlank()
            ? "collector" : config.metricsPrefix.replaceAll("[^a-zA-Z0-9_:]", "_");
        for (String name : new String[] {"received_total", "received_bytes_total", "received_qos0_total",
                "received_qos1_total", "received_qos2_total", "delivered_total", "delivered_bytes_total",
                "mqtt_acked_total", "rejected_total", "qos0_unarchived_total", "disconnects_total",
                "ack_errors_total", "stale_acks_skipped_total", "delivery_errors_total",
                "send_errors_total", "overflow_total", "kafka_probe_errors_total"}) {
            counters.put(name, Counter.builder(metric(name)).register(meters));
        }
        this.budget = new ByteBudget(config.maxPendingBytes, config.maxPendingMessages);
        Gauge.builder(metric("pending.bytes"), budget, ByteBudget::bytes).register(meters);
        Gauge.builder(metric("buffer.bytes"), budget, ByteBudget::bytes).register(meters);
        Gauge.builder(metric("pending.records"), budget, ByteBudget::records).register(meters);
        Gauge.builder(metric("buffer.limit.bytes"), budget, ByteBudget::maxBytes).register(meters);
        Gauge.builder(metric("buffer.limit.records"), budget, ByteBudget::maxMessages).register(meters);
        Gauge.builder(metric("ready"), this, service -> service.isReady() ? 1 : 0).register(meters);
        Gauge.builder(metric("connected"), this, service -> service.mqtt != null && service.mqtt.isConnected() ? 1 : 0).register(meters);
        Gauge.builder(metric("subscribed"), this, service -> service.healthy.get() ? 1 : 0).register(meters);
        Gauge.builder(metric("kafka.ready"), kafkaReady, v -> v.get() ? 1 : 0).register(meters);
        Gauge.builder(metric("fatal"), fatal, v -> v.get() ? 1 : 0).register(meters);
        Gauge.builder(metric("uptime.seconds"), this, service ->
            Math.max(0, (System.currentTimeMillis() - service.startedAtMillis.get()) / 1000.0)).register(meters);
        Gauge.builder(metric("last.delivery.timestamp.seconds"), lastDeliveryAtMillis,
            value -> value.get() / 1000.0).register(meters);
    }

    private String metric(String name) { return metricsPrefix + "." + name; }
    private void count(String name) { counters.get(name).increment(); }

    @PostConstruct
    public void start() throws Exception {
        try { startClients(); }
        catch (Exception | LinkageError error) {
            shutdown(); // @PreDestroy is not guaranteed after failed bean initialization.
            throw error;
        }
    }

    private void startClients() throws Exception {
        if (config.mqtt.qos != 1) throw new IllegalArgumentException("Only MQTT QoS 1 is supported for archival guarantees");
        if (config.mqtt.receiveMaximum < 1 || config.mqtt.receiveMaximum > 65535) throw new IllegalArgumentException("Invalid receiveMaximum");
        if (config.maxPayloadBytes < 1 || config.maxPendingBytes < config.maxPayloadBytes + 1024L) throw new IllegalArgumentException("Buffer too small");
        Properties props = KafkaClientSettings.producer(config);
        producer = new KafkaProducer<>(props);
        // Validate configured targets without requesting topic creation. Disable broker auto-creation as well.
        admin = AdminClient.create(KafkaClientSettings.admin(props));
        admin.describeTopics(mapper.topics()).allTopicNames().get(15, TimeUnit.SECONDS);
        kafkaReady.set(true);
        kafkaProbe = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "kafka-readiness-probe");
            thread.setDaemon(true);
            return thread;
        });
        kafkaProbe.scheduleWithFixedDelay(this::probeKafka, 30, 30, TimeUnit.SECONDS);
        // Fixed client ID is essential for broker-side persistent session recovery.
        String mqttUri = mqttUri();
        mqtt = new MqttAsyncClient(mqttUri, config.mqtt.clientId,
            new MqttDefaultFilePersistence(config.mqtt.persistenceDirectory));
        mqtt.setManualAcks(true);
        mqtt.setCallback(new MqttCallback() {
            @Override public void messageArrived(String topic, MqttMessage message) { ingest(topic, message); }
            @Override public void disconnected(MqttDisconnectResponse response) {
                sessionActive.set(false);
                count("disconnects_total");
                if (!stopping.get()) fail("MQTT disconnected: " + response, null);
            }
            @Override public void mqttErrorOccurred(MqttException exception) { fail("MQTT protocol error", exception); }
            @Override public void deliveryComplete(org.eclipse.paho.mqttv5.client.IMqttToken token) { }
            @Override public void connectComplete(boolean reconnect, String serverURI) {
                // Existing sessions can deliver queued messages before SUBACK.
                sessionActive.set(true);
                log.info("MQTT connected: {}", serverURI);
            }
            @Override public void authPacketArrived(int reasonCode, MqttProperties properties) { }
        });
        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setCleanStart(false);
        options.setAutomaticReconnect(false); // Restart process on disconnect: never reuse stale ACK packet IDs.
        options.setSessionExpiryInterval(config.mqtt.sessionExpirySeconds);
        options.setReceiveMaximum(config.mqtt.receiveMaximum);
        if (!config.mqtt.username.isBlank()) {
            options.setUserName(config.mqtt.username);
            options.setPassword(config.mqtt.password.getBytes(StandardCharsets.UTF_8));
        }
        mqtt.connect(options).waitForCompletion(15000);
        // CONNACK has completed; ACK eligibility must not depend on SUBACK/readiness.
        sessionActive.set(true);
        var subscription = mqtt.subscribe(config.mqtt.topic, 1);
        subscription.waitForCompletion(15000);
        for (int reason : subscription.getReasonCodes()) {
            if (reason != 1) throw new IllegalStateException("MQTT subscription did not grant QoS 1: " + reason);
        }
        healthy.set(true);
        log.info("Archiving MQTT {} => Kafka {}", config.mqtt.topic, mapper.topics());
    }

    void ingest(String topic, MqttMessage message) {
        if (stopping.get()) return;
        int qos = message.getQos();
        byte[] payload = message.getPayload();
        count("received_total");
        count("received_bytes_total", payload.length);
        count("received_qos" + qos + "_total");
        if (qos == 0 && config.mqtt.qos0Policy == CollectorProperties.Qos0Policy.REJECT) {
            reject(qos);
            return;
        }
        if (qos > 1 || payload.length > config.maxPayloadBytes) {
            reject(qos);
            if (qos > 0) fail("Unsupported QoS / oversized MQTT payload; operator action required", null);
            return;
        }
        long charge = payload.length + topic.getBytes(StandardCharsets.UTF_8).length + 1024L;
        if (!budget.reserve(charge)) {
            reject(qos);
            count("overflow_total");
            if (qos > 0) fail("Buffer full: restart to recover unACKed MQTT messages", null);
            return;
        }
        long receivedAt = System.currentTimeMillis();
        int packetId = message.getId();
        final TopicMapper.Route route;
        try { route = mapper.route(topic); }
        catch (RuntimeException ex) {
            budget.release(charge);
            reject(qos);
            fail("Invalid routing", ex); return;
        }
        var record = KafkaRecordFactory.create(
            route, topic, payload, qos, message.isRetained(), message.isDuplicate(),
            receivedAt, config.collectorRole, mqttUri());
        AtomicBoolean released = new AtomicBoolean();
        Runnable release = () -> { if (released.compareAndSet(false, true)) budget.release(charge); };
        try {
            producer.send(record, (metadata, error) -> {
                try {
                    if (error != null) {
                        kafkaReady.set(false);
                        count("delivery_errors_total");
                        fail("Kafka delivery failed", error);
                        if (qos == 0) count("qos0_unarchived_total");
                    } else {
                        // Kafka success is independent of whether a PUBACK can still be sent.
                        count("delivered_total");
                        count("delivered_bytes_total", payload.length);
                        lastDeliveryAtMillis.set(System.currentTimeMillis());
                        if (qos == 1) {
                            if (!stopping.get() && sessionActive.get() && mqtt.isConnected()) {
                                try {
                                    mqtt.messageArrivedComplete(packetId, qos);
                                    count("mqtt_acked_total");
                                } catch (MqttException ex) {
                                    count("ack_errors_total");
                                    fail("MQTT PUBACK failed", ex);
                                }
                            } else count("stale_acks_skipped_total");
                        }
                    }
                } finally { release.run(); }
            });
        } catch (Exception ex) {
            release.run();
            kafkaReady.set(false);
            count("send_errors_total");
            if (qos == 0) count("qos0_unarchived_total");
            fail("Kafka send rejected", ex);
        }
    }

    private void reject(int qos) {
        count("rejected_total");
        if (qos == 0) count("qos0_unarchived_total");
    }

    private String mqttUri() {
        if (config.mqtt.tls && config.mqtt.uri.startsWith("tcp://")) {
            return "ssl://" + config.mqtt.uri.substring("tcp://".length());
        }
        return config.mqtt.uri;
    }

    private void count(String name, long amount) {
        if (amount > 0) {
            counters.get(name).increment(amount);
        }
    }

    private void probeKafka() {
        if (stopping.get() || admin == null) return;
        try {
            admin.describeCluster().clusterId().get(5, TimeUnit.SECONDS);
            kafkaReady.set(true);
        } catch (Exception error) {
            kafkaReady.set(false);
            count("kafka_probe_errors_total");
            log.warn("Kafka readiness probe failed: {}", error.toString());
        }
    }

    private void fail(String reason, Throwable error) {
        if (!stopping.compareAndSet(false, true)) return;
        fatal.set(true);
        healthy.set(false);
        if (error == null) log.error("Collector stopping: {}", reason);
        else log.error("Collector stopping: " + reason, error);
        fatalShutdown.request();
    }

    @Override public Health health() {
        return isReady() ? Health.up().build() : Health.down()
            .withDetail("mqttConnected", mqtt != null && mqtt.isConnected())
            .withDetail("kafkaReady", kafkaReady.get()).build();
    }

    @Override public boolean isReady() {
        return healthy.get() && kafkaReady.get() && !stopping.get() && mqtt != null && mqtt.isConnected();
    }

    @PreDestroy
    public void shutdown() {
        if (!closed.compareAndSet(false, true)) return;
        stopping.set(true);
        sessionActive.set(false);
        healthy.set(false);
        kafkaReady.set(false);
        if (kafkaProbe != null) kafkaProbe.shutdownNow();
        // Prevent any new MQTT deliveries before draining Kafka; unACKed messages are redelivered by the broker.
        if (mqtt != null) {
            try { if (mqtt.isConnected()) mqtt.disconnect().waitForCompletion(5000); }
            catch (Exception e) { log.warn("MQTT disconnect: {}", e.toString()); }
        }
        if (producer != null) producer.close(Duration.ofSeconds(config.shutdownDrainSeconds));
        if (admin != null) admin.close(Duration.ofSeconds(5));
        if (mqtt != null) try { mqtt.close(); } catch (Exception ignored) { }
    }
}
