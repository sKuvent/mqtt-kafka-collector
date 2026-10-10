package dev.mqttkafka;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class TopicMapperTest {
    @Test void fallbackPreservesTopicAsKey() {
        CollectorProperties p = new CollectorProperties();
        TopicMapper.Route route = new TopicMapper(p).route("sensors/device-1/status");
        assertEquals("mqtt.raw.v1", route.topic());
        assertEquals("sensors/device-1/status", route.key());
    }
    @Test void firstMatchingRuleUsesNamedGroup() {
        CollectorProperties p = new CollectorProperties();
        CollectorProperties.Rule r = new CollectorProperties.Rule();
        r.pattern = "^sensors/(?<device>[^/]+)/telemetry$";
        r.kafkaTopic = "mqtt.telemetry.v1";
        r.kafkaKey = "{device}";
        p.mapping.rules.add(r);
        TopicMapper mapper = new TopicMapper(p);
        assertEquals(new TopicMapper.Route("mqtt.telemetry.v1", "abc"), mapper.route("sensors/abc/telemetry"));
        assertEquals("mqtt.raw.v1", mapper.route("sensors/abc/config").topic());
    }
    @Test void rejectsInvalidTopic() {
        CollectorProperties p = new CollectorProperties(); p.mapping.defaultTopic = "not allowed";
        assertThrows(IllegalArgumentException.class, () -> new TopicMapper(p));
    }
    @Test void rejectsMissingGroupOnRoute() {
        CollectorProperties p = new CollectorProperties();
        CollectorProperties.Rule r = new CollectorProperties.Rule();
        r.pattern = "^a$"; r.kafkaTopic = "abc"; r.kafkaKey = "{missing}";
        p.mapping.rules.add(r);
        assertThrows(IllegalArgumentException.class, () -> new TopicMapper(p).route("a"));
    }
}
