package dev.mqttkafka;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

/** Creates the byte-preserving Kafka envelope consumed by the history audit. */
final class KafkaRecordFactory {
    private KafkaRecordFactory() { }

    static ProducerRecord<byte[], byte[]> create(
        TopicMapper.Route route,
        String mqttTopic,
        byte[] payload,
        int qos,
        boolean retained,
        boolean duplicate,
        long receivedAt,
        String collectorRole,
        String sourceBroker
    ) {
        byte[] key = route.key().getBytes(StandardCharsets.UTF_8);
        List<Header> headers = List.of(
            header("mqttTopic", mqttTopic),
            header("mqttQos", Integer.toString(qos)),
            header("mqttRetain", Boolean.toString(retained)),
            header("mqttDup", Boolean.toString(duplicate)),
            header("collectorRole", collectorRole),
            header("receivedAtUnixMs", Long.toString(receivedAt)),
            header("eventId", UUID.randomUUID().toString()),
            header("sourceBroker", sourceBroker)
        );
        return new ProducerRecord<>(route.topic(), null, receivedAt, key, payload, headers);
    }

    private static RecordHeader header(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }
}
