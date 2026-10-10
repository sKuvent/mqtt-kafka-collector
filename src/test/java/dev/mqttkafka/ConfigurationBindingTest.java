package dev.mqttkafka;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

class ConfigurationBindingTest {
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(CollectorProperties.class)
    static class Config { }

    @Test void bindsDocumentedAliasesAndNestedRoutingAndSecurity() {
        new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withUserConfiguration(Config.class)
            .withPropertyValues("KAFKA_BOOTSTRAP_SERVERS=other-broker:19092", "MQTT_HOST=other-mqtt",
                "MQTT_PORT=1884", "MQTT_TOPIC=sensors/#", "MQTT_CLIENT_ID=unique", "MQTT_PASSWORD=example",
                "MQTT_QOS0_POLICY=reject", "METRICS_PREFIX=custom", "COLLECTOR_ROLE=test",
                "KAFKA_TOPIC=other.raw", "KAFKA_PARTITIONING=crc32", "collector.max-pending-bytes=4096",
                "collector.mapping.rules[0].pattern=^sensors/(?<device>[^/]+)$",
                "collector.mapping.rules[0].kafka-topic=other.mapped",
                "collector.mapping.rules[0].kafka-key={device}",
                "collector.kafka.properties[security.protocol]=SASL_SSL")
            .run(context -> {
                assertThat(context).hasNotFailed();
                var p = context.getBean(CollectorProperties.class);
                assertThat(p.kafka.bootstrapServers).isEqualTo("other-broker:19092");
                assertThat(p.mqtt.uri).isEqualTo("tcp://other-mqtt:1884");
                assertThat(p.mqtt.clientId).isEqualTo("unique");
                assertThat(p.mqtt.password).isEqualTo("example");
                assertThat(p.mqtt.topic).isEqualTo("sensors/#");
                assertThat(p.mqtt.qos0Policy).isEqualTo(CollectorProperties.Qos0Policy.REJECT);
                assertThat(p.metricsPrefix).isEqualTo("custom");
                assertThat(p.collectorRole).isEqualTo("test");
                assertThat(p.maxPendingBytes).isEqualTo(4096);
                assertThat(p.kafka.partitioning).isEqualTo("crc32");
                assertThat(p.kafka.properties).containsEntry("security.protocol", "SASL_SSL");
                var mapper = new TopicMapper(p);
                assertThat(mapper.route("sensors/a")).isEqualTo(new TopicMapper.Route("other.mapped", "a"));
                assertThat(mapper.route("unmatched").topic()).isEqualTo("other.raw");
            });
    }

    @Test void forwardsSecurityToBothClientsAndPreservesDeliveryInvariants() {
        var p = new CollectorProperties();
        p.kafka.properties.put("security.protocol", "SASL_SSL");
        p.kafka.properties.put("sasl.mechanism", "PLAIN");
        p.kafka.properties.put("acks", "0");
        p.kafka.properties.put("enable.idempotence", "false");
        var producer = KafkaClientSettings.producer(p);
        var admin = KafkaClientSettings.admin(producer);
        assertThat(producer.getProperty("acks")).isEqualTo("all");
        assertThat(producer.getProperty("enable.idempotence")).isEqualTo("true");
        assertThat(admin.getProperty("security.protocol")).isEqualTo("SASL_SSL");
        assertThat(admin.getProperty("sasl.mechanism")).isEqualTo("PLAIN");
        assertThat(admin).doesNotContainKey("acks");
        assertThat(producer).doesNotContainKey("allow.auto.create.topics");
    }
}
