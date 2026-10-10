package dev.mqttkafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

@Component
public class TopicMapper {
    public record Route(String topic, String key) {}
    private record Compiled(Pattern pattern, String topic, String key) {}
    private final List<Compiled> rules;
    private final String fallbackTopic;
    private final String fallbackKey;

    public TopicMapper(CollectorProperties properties) {
        fallbackTopic = validateTopic(properties.mapping.defaultTopic);
        fallbackKey = properties.mapping.defaultKey;
        rules = new ArrayList<>();
        for (CollectorProperties.Rule rule : properties.mapping.rules) {
            Pattern regex = Pattern.compile(rule.pattern);
            validateTopic(rule.kafkaTopic);
            rules.add(new Compiled(regex, rule.kafkaTopic, rule.kafkaKey));
        }
    }

    public Route route(String mqttTopic) {
        for (Compiled rule : rules) {
            Matcher match = rule.pattern.matcher(mqttTopic);
            if (match.matches()) return new Route(rule.topic, render(rule.key, mqttTopic, match));
        }
        return new Route(fallbackTopic, render(fallbackKey, mqttTopic, null));
    }

    private String render(String template, String topic, Matcher match) {
        if (template == null || template.isBlank()) return topic;
        StringBuilder result = new StringBuilder();
        Matcher tags = Pattern.compile("\\{(topic|[a-zA-Z][a-zA-Z0-9_]*)\\}").matcher(template);
        while (tags.find()) {
            String name = tags.group(1);
            String value;
            if (name.equals("topic")) value = topic;
            else {
                if (match == null) throw new IllegalArgumentException("Unknown key variable: " + name);
                try { value = match.group(name); }
                catch (IllegalArgumentException e) { throw new IllegalArgumentException("Undefined regex group: " + name, e); }
            }
            tags.appendReplacement(result, Matcher.quoteReplacement(value == null ? "" : value));
        }
        tags.appendTail(result);
        return result.toString();
    }

    private static String validateTopic(String topic) {
        if (topic == null || !topic.matches("[a-zA-Z0-9._-]+") || topic.equals(".") || topic.equals(".."))
            throw new IllegalArgumentException("Invalid Kafka topic: " + topic);
        return topic;
    }
    public List<String> topics() {
        return java.util.stream.Stream.concat(java.util.stream.Stream.of(fallbackTopic), rules.stream().map(Compiled::topic)).distinct().toList();
    }
}
