"""Test-only client; no application imports, independent byte/header verification."""
import base64
import json
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
import paho.mqtt.client as mqtt


def run(request):
    operation = request['op']
    if operation == 'bootstrap':
        admin = AdminClient({'bootstrap.servers': 'kafka:9092', 'socket.timeout.ms': 3000})
        existing = admin.list_topics(timeout=5).topics
        for name in ('test.raw', 'test.events'):
            if name not in existing:
                admin.create_topics([NewTopic(name, 3, 1)])[name].result(10)
        return list(admin.list_topics(timeout=5).topics)
    if operation == 'http':
        try:
            with urllib.request.urlopen('http://collector:8000' + request['path'], timeout=3) as response:
                return {'status': response.status, 'body': response.read().decode()}
        except urllib.error.HTTPError as error:
            return {'status': error.code, 'body': error.read().decode()}
    if operation == 'publish':
        connected = threading.Event()
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                             client_id='probe-' + uuid.uuid4().hex, protocol=mqtt.MQTTv5)
        def on_connect(client, userdata, flags, reason, props):
            if not reason.is_failure:
                connected.set()
        client.on_connect = on_connect
        client.connect('mqtt', 1883, 20)
        client.loop_start()
        try:
            if not connected.wait(5):
                raise RuntimeError('publisher failed to connect')
            for record in request['records']:
                info = client.publish(record['topic'], base64.b64decode(record['payload']),
                                      qos=record.get('qos', 1))
                info.wait_for_publish(5)
                if not info.is_published():
                    raise RuntimeError('publisher did not finish')
        finally:
            client.disconnect()
            client.loop_stop()
        return {'published': len(request['records'])}
    if operation == 'read':
        consumer = Consumer({'bootstrap.servers': 'kafka:9092', 'group.id': 'probe-' + uuid.uuid4().hex,
                             'enable.auto.commit': False, 'allow.auto.create.topics': False,
                             'auto.offset.reset': 'earliest'})
        try:
            topic = request.get('topic', 'test.raw')
            metadata = consumer.list_topics(topic, timeout=5).topics[topic]
            consumer.assign([TopicPartition(topic, p, 0) for p in metadata.partitions])
            expected = set(request['payloads'])
            found = []
            seen = set()
            deadline = time.monotonic() + request.get('timeout', 12)
            while time.monotonic() < deadline:
                message = consumer.poll(0.25)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(str(message.error()))
                headers = {key: value.decode() for key, value in message.headers() or []}
                if headers.get('mqttTopic') != request['mqttTopic']:
                    continue
                payload = base64.b64encode(message.value()).decode()
                found.append({'payload': payload, 'headers': headers,
                              'key': message.key().decode(), 'partition': message.partition(),
                              'offset': message.offset(), 'timestamp': message.timestamp()[1]})
                seen.add(payload)
                if expected and expected <= seen:
                    break
            return found
        finally:
            consumer.close()
    raise ValueError(operation)


if __name__ == '__main__':
    print(json.dumps(run(json.load(sys.stdin))))
