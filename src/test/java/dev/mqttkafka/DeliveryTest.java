package dev.mqttkafka;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class DeliveryTest {
    private final CollectorProperties properties = new CollectorProperties();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final FatalShutdown exit = mock(FatalShutdown.class);
    private final MqttAsyncClient mqtt = mock(MqttAsyncClient.class);
    private final MockProducer<byte[], byte[]> producer = new MockProducer<>(false,
        new ByteArraySerializer(), new ByteArraySerializer());

    private CollectorService service() {
        var service = new CollectorService(properties, new TopicMapper(properties), registry, exit);
        ReflectionTestUtils.setField(service, "mqtt", mqtt);
        ReflectionTestUtils.setField(service, "producer", producer);
        ReflectionTestUtils.setField(service, "sessionActive", new AtomicBoolean(true));
        when(mqtt.isConnected()).thenReturn(true);
        return service;
    }

    private MqttMessage message(int qos) {
        var msg = new MqttMessage(new byte[] {0, (byte) 255, 1});
        msg.setQos(qos);
        msg.setId(42);
        return msg;
    }

    private double count(String metric) { return registry.get("collector." + metric).counter().count(); }

    @Test void qos1AckWaitsForKafkaAndDoesNotDependOnSubscriptionReadiness() throws Exception {
        var service = service();
        service.ingest("sensor/a", message(1));
        verify(mqtt, never()).messageArrivedComplete(anyInt(), anyInt());
        assertEquals(0, count("delivered_total"));
        producer.completeNext();
        verify(mqtt).messageArrivedComplete(42, 1);
        assertEquals(1, count("delivered_total"));
        assertEquals(1, count("mqtt_acked_total"));
    }

    @Test void failedKafkaDeliveryIsNeverAcknowledged() throws Exception {
        var service = service();
        service.ingest("sensor/a", message(1));
        producer.errorNext(new RuntimeException("broker unavailable"));
        verify(mqtt, never()).messageArrivedComplete(anyInt(), anyInt());
        verify(exit).request();
        assertEquals(0, count("delivered_total"));
        assertEquals(1, count("delivery_errors_total"));
        assertEquals(0, registry.get("collector.pending.records").gauge().value());
    }

    @Test void disconnectedAckDoesNotHideKafkaSuccess() throws Exception {
        var service = service();
        service.ingest("sensor/a", message(1));
        when(mqtt.isConnected()).thenReturn(false);
        producer.completeNext();
        assertEquals(1, count("delivered_total"));
        assertEquals(1, count("stale_acks_skipped_total"));
        assertEquals(0, count("mqtt_acked_total"));
        verify(mqtt, never()).messageArrivedComplete(anyInt(), anyInt());
    }

    @Test void qos0IsArchivedWithoutMqttAck() throws Exception {
        var service = service();
        service.ingest("sensor/a", message(0));
        producer.completeNext();
        assertEquals(1, count("received_qos0_total"));
        assertEquals(1, count("delivered_total"));
        verify(mqtt, never()).messageArrivedComplete(anyInt(), anyInt());
        verifyNoInteractions(exit);
    }

    @Test void rejectedQos0DoesNotRestartCollector() {
        properties.mqtt.qos0Policy = CollectorProperties.Qos0Policy.REJECT;
        service().ingest("sensor/a", message(0));
        assertEquals(1, count("received_total"));
        assertEquals(1, count("rejected_total"));
        assertEquals(1, count("qos0_unarchived_total"));
        assertTrue(producer.history().isEmpty());
        verifyNoInteractions(exit);
    }

    @Test void qos0BufferOverflowDropsOnlyThatMessage() {
        properties.maxPendingMessages = 1;
        var service = service();
        service.ingest("sensor/a", message(1));
        service.ingest("sensor/a", message(0));
        assertEquals(1, count("overflow_total"));
        assertEquals(1, count("qos0_unarchived_total"));
        assertEquals(1, producer.history().size());
        verifyNoInteractions(exit);
    }

    @Test void qos1BufferOverflowStopsWithoutAcknowledging() throws Exception {
        properties.maxPendingMessages = 1;
        var service = service();
        service.ingest("sensor/a", message(1));
        service.ingest("sensor/a", message(1));
        verify(exit).request();
        verify(mqtt, never()).messageArrivedComplete(anyInt(), anyInt());
        producer.completeNext();
        assertEquals(1, count("delivered_total"));
        assertEquals(0, count("mqtt_acked_total"));
    }
}
