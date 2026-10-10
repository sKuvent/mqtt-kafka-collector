package dev.mqttkafka;

import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClientConfig;

final class KafkaClientSettings {
    private KafkaClientSettings() { }

    static Properties producer(CollectorProperties config) {
        Properties props = new Properties();
        props.put("compression.type", "zstd");
        props.put("linger.ms", "10");
        props.put("batch.size", "65536");
        props.put("max.block.ms", "10000");
        props.put("max.request.size", Integer.toString(config.kafka.maxRequestBytes));
        props.put("delivery.timeout.ms", Integer.toString(config.kafka.deliveryTimeoutMs));
        props.put("buffer.memory", Long.toString(Math.multiplyExact(config.maxPendingBytes, 2)));
        props.putAll(config.kafka.properties);
        // These invariants cannot be disabled by the generic property map.
        props.put("bootstrap.servers", config.kafka.bootstrapServers);
        props.put("client.id", config.kafka.clientId);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("enable.idempotence", "true");
        props.put("acks", "all");
        props.put("max.in.flight.requests.per.connection", "5");
        props.put("retries", Integer.toString(Integer.MAX_VALUE));
        props.remove("partitioner.class");
        props.put("partitioner.ignore.keys", "false");
        switch (config.kafka.partitioning) {
            case "murmur2" -> { }
            case "crc32" -> props.put("partitioner.class", Crc32Partitioner.class.getName());
            default -> throw new IllegalArgumentException("partitioning must be murmur2 or crc32");
        }
        return props;
    }

    static Properties admin(Properties producer) {
        Properties props = new Properties();
        for (String key : AdminClientConfig.configNames()) {
            if (producer.containsKey(key)) props.put(key, producer.get(key));
        }
        return props;
    }
}
