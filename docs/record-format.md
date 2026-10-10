# Kafka record format

The Kafka value is the exact MQTT payload bytes. It is not wrapped in JSON and can be
binary, invalid UTF-8 or empty. The key is the UTF-8 result of the routing key template.
The timestamp is collector reception time in Unix milliseconds; Kafka broker append time
can replace it.

All headers are UTF-8 text headers. They describe the MQTT delivery seen by this
collector; they are not part of the payload and are not parsed or rewritten by the
collector.

| Header | Meaning |
| --- | --- |
| `mqttTopic` | Original MQTT topic |
| `mqttQos` | Delivered QoS (`0` or `1`) |
| `mqttRetain`, `mqttDup` | MQTT flags as `true` or `false` |
| `collectorRole` | Configured collector role |
| `receivedAtUnixMs` | Collector reception time |
| `eventId` | UUID for this capture attempt |
| `sourceBroker` | Configured MQTT endpoint URI |

`eventId` is generated per capture attempt. It is not a stable source event identity;
preserve a device sequence number or application event ID for downstream deduplication.

The record does not contain a schema version or a transformed FIWARE representation. A
consumer must know the source payload format. A repeated record with the same payload can
be a legitimate MQTT redelivery after a lost PUBACK. Kafka ordering is guaranteed only
within one partition and is determined by the record key and configured partitioner.

## Example

An MQTT publisher sends the following message:

**MQTT topic:**
```text
sensors/device-1/temperature
```

**MQTT payload:**
```json
{"messageSeqId":42,"value":21.5}
```

With the default routing configuration, the resulting Kafka record looks like this:

| Field | Value |
| --- | --- |
| Kafka topic | `mqtt.raw.v1` |
| Kafka key | `sensors/device-1/temperature` |
| Kafka value | `{"messageSeqId":42,"value":21.5}` |
| Kafka timestamp | Collector reception time |

Selected Kafka headers:

```text
mqttTopic:        sensors/device-1/temperature
mqttQos:          1
mqttRetain:       false
mqttDup:          false
collectorRole:    mqtt-kafka-collector
receivedAtUnixMs: 1790000000000
eventId:          <generated UUID>
sourceBroker:     tcp://mqtt:1883
```

The payload is stored as raw bytes without JSON parsing or modification. MQTT metadata is preserved separately in Kafka headers.

The Kafka key determines partition assignment. Messages with the same key are normally assigned to the same partition, preserving their Kafka append order within that partition.
