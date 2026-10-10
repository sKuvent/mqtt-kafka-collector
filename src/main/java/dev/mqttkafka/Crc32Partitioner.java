package dev.mqttkafka;

import java.util.Map;
import java.util.zip.CRC32;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/** librdkafka's consistent/consistent_random hash for non-empty keys. */
public final class Crc32Partitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value,
                         byte[] valueBytes, Cluster cluster) {
        if (keyBytes == null || keyBytes.length == 0) {
            throw new IllegalArgumentException("CRC32 partitioning requires a non-empty key");
        }
        CRC32 hash = new CRC32();
        hash.update(keyBytes);
        return (int) (hash.getValue() % cluster.partitionCountForTopic(topic));
    }

    @Override public void configure(Map<String, ?> configs) { }
    @Override public void close() { }
}
