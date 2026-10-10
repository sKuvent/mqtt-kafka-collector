package dev.mqttkafka;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "collector")
public class CollectorProperties {
    public Mqtt mqtt = new Mqtt();
    public Kafka kafka = new Kafka();
    public Mapping mapping = new Mapping();
    public long maxPendingBytes = 16 * 1024 * 1024;
    public int maxPendingMessages = 10000;
    public int maxPayloadBytes = 1024 * 1024;
    public int shutdownDrainSeconds = 25;
    public String collectorRole = "mqtt-kafka-collector";
    public String metricsPrefix = "collector";

    public Mqtt getMqtt() { return mqtt; }
    public void setMqtt(Mqtt value) { mqtt = value; }
    public Kafka getKafka() { return kafka; }
    public void setKafka(Kafka value) { kafka = value; }
    public Mapping getMapping() { return mapping; }
    public void setMapping(Mapping value) { mapping = value; }
    public long getMaxPendingBytes() { return maxPendingBytes; }
    public void setMaxPendingBytes(long value) { maxPendingBytes = value; }
    public int getMaxPendingMessages() { return maxPendingMessages; }
    public void setMaxPendingMessages(int value) { maxPendingMessages = value; }
    public int getMaxPayloadBytes() { return maxPayloadBytes; }
    public void setMaxPayloadBytes(int value) { maxPayloadBytes = value; }
    public int getShutdownDrainSeconds() { return shutdownDrainSeconds; }
    public void setShutdownDrainSeconds(int value) { shutdownDrainSeconds = value; }
    public String getCollectorRole() { return collectorRole; }
    public void setCollectorRole(String value) { collectorRole = value; }
    public String getMetricsPrefix() { return metricsPrefix; }
    public void setMetricsPrefix(String value) { metricsPrefix = value; }

    public enum Qos0Policy { BEST_EFFORT, REJECT }

    public static class Mqtt {
        public String uri = "tcp://localhost:1883";
        public boolean tls = false;
        public String clientId = "mqtt-kafka-collector";
        public String topic = "#";
        public String username = "";
        public String password = "";
        public int qos = 1;
        public Qos0Policy qos0Policy = Qos0Policy.BEST_EFFORT;
        public String persistenceDirectory = "/app/state";
        public long sessionExpirySeconds = 86400;
        public int receiveMaximum = 100;
        public String getUri() { return uri; }
        public void setUri(String value) { uri = value; }
        public boolean getTls() { return tls; }
        public void setTls(boolean value) { tls = value; }
        public String getClientId() { return clientId; }
        public void setClientId(String value) { clientId = value; }
        public String getTopic() { return topic; }
        public void setTopic(String value) { topic = value; }
        public String getUsername() { return username; }
        public void setUsername(String value) { username = value; }
        public String getPassword() { return password; }
        public void setPassword(String value) { password = value; }
        public int getQos() { return qos; }
        public void setQos(int value) { qos = value; }
        public Qos0Policy getQos0Policy() { return qos0Policy; }
        public void setQos0Policy(Qos0Policy value) { qos0Policy = value; }
        public String getPersistenceDirectory() { return persistenceDirectory; }
        public void setPersistenceDirectory(String value) { persistenceDirectory = value; }
        public long getSessionExpirySeconds() { return sessionExpirySeconds; }
        public void setSessionExpirySeconds(long value) { sessionExpirySeconds = value; }
        public int getReceiveMaximum() { return receiveMaximum; }
        public void setReceiveMaximum(int value) { receiveMaximum = value; }
    }
    public static class Kafka {
        public String bootstrapServers = "localhost:9092";
        public String clientId = "mqtt-kafka-collector";
        public int maxRequestBytes = 2097152;
        public int deliveryTimeoutMs = 120000;
        public String partitioning = "murmur2";
        public Map<String, String> properties = new LinkedHashMap<>();
        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String value) { bootstrapServers = value; }
        public String getClientId() { return clientId; }
        public void setClientId(String value) { clientId = value; }
        public int getMaxRequestBytes() { return maxRequestBytes; }
        public void setMaxRequestBytes(int value) { maxRequestBytes = value; }
        public int getDeliveryTimeoutMs() { return deliveryTimeoutMs; }
        public void setDeliveryTimeoutMs(int value) { deliveryTimeoutMs = value; }
        public String getPartitioning() { return partitioning; }
        public void setPartitioning(String value) { partitioning = value; }
        public Map<String, String> getProperties() { return properties; }
        public void setProperties(Map<String, String> value) { properties = value; }
    }
    public static class Mapping {
        public String defaultTopic = "mqtt.raw.v1";
        public String defaultKey = "{topic}";
        public List<Rule> rules = new ArrayList<>();
        public String getDefaultTopic() { return defaultTopic; }
        public void setDefaultTopic(String value) { defaultTopic = value; }
        public String getDefaultKey() { return defaultKey; }
        public void setDefaultKey(String value) { defaultKey = value; }
        public List<Rule> getRules() { return rules; }
        public void setRules(List<Rule> value) { rules = value; }
    }
    public static class Rule {
        public String pattern;
        public String kafkaTopic;
        public String kafkaKey = "{topic}";
        public String getPattern() { return pattern; }
        public void setPattern(String value) { pattern = value; }
        public String getKafkaTopic() { return kafkaTopic; }
        public void setKafkaTopic(String value) { kafkaTopic = value; }
        public String getKafkaKey() { return kafkaKey; }
        public void setKafkaKey(String value) { kafkaKey = value; }
    }
}
