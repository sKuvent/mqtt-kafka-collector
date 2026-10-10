package dev.mqttkafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class KafkaRecordFactoryTest {
    @Test
    void preservesPayloadAndWritesAuditHeaders() {
        byte[] payload = "{\"sequence\":83227}".getBytes(StandardCharsets.UTF_8);
        var record = KafkaRecordFactory.create(
            new TopicMapper.Route("mqtt.raw.v1", "sensors/device-1/telemetry"),
            "sensors/device-1/telemetry", payload, 1, false, true, 1791469181000L,
            "raw-ingress", "tcp://mosquitto:1883");

        assertEquals("mqtt.raw.v1", record.topic());
        assertArrayEquals("sensors/device-1/telemetry".getBytes(StandardCharsets.UTF_8), record.key());
        assertArrayEquals(payload, record.value());
        assertEquals("sensors/device-1/telemetry", header(record, "mqttTopic"));
        assertEquals("1791469181000", header(record, "receivedAtUnixMs"));
        assertEquals("raw-ingress", header(record, "collectorRole"));
        assertEquals("true", header(record, "mqttDup"));
        assertNotNull(header(record, "eventId"));
    }

    private static String header(org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]> record, String name) {
        var header = record.headers().lastHeader(name);
        assertNotNull(header, "missing header " + name);
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
