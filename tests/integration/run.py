#!/usr/bin/env python3
"""Run real broker tests on an isolated Compose network; no host ports or production addresses.
Requires Python 3.10+, Docker Engine and Compose. Uses only Python's standard library.
"""
import base64
import json
import os
from pathlib import Path
import subprocess
import time
import unittest
import uuid
import zlib

ROOT = Path(__file__).resolve().parents[2]
PROJECT = 'collector-it-' + uuid.uuid4().hex[:10]
ARTIFACTS = ROOT / 'target' / 'integration'
BASE = ['docker', 'compose', '-p', PROJECT, '-f', str(Path(__file__).with_name('compose.yaml'))]


def compose(*args, env=None, data=None, check=True, timeout=120):
    return subprocess.run(BASE + list(args), input=data, text=True, capture_output=True,
                          env=os.environ | (env or {}), check=check, timeout=timeout)


def probe(request):
    result = compose('exec', '-T', 'probe', 'python', '/tests/probe.py',
                     data=json.dumps(request), timeout=40)
    return json.loads(result.stdout)


def until(function, timeout=60):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = function()
            if last:
                return last
        except (subprocess.SubprocessError, ValueError, KeyError) as error:
            last = error
        time.sleep(0.5)
    raise AssertionError(f'Condition not satisfied after {timeout}s: {last}')


def http(path):
    return probe({'op': 'http', 'path': path})


def metric(name):
    text = http('/metrics')['body']
    for line in text.splitlines():
        if line.startswith('test_collector_' + name + ' '):
            return float(line.split()[-1])
    raise AssertionError('Missing metric: ' + name)


class CollectorIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        cls.addClassCleanup(cls.cleanup)
        compose('up', '-d', '--build', 'mqtt', 'kafka', 'probe', timeout=300)
        until(lambda: probe({'op': 'bootstrap'}), timeout=120)

    @classmethod
    def cleanup(cls):
        compose('unpause', 'kafka', check=False)
        logs = compose('logs', '--no-color', check=False).stdout
        (ARTIFACTS / 'brokers.log').write_text(logs)
        compose('down', '--volumes', '--remove-orphans', check=False)

    def setUp(self):
        self.stream = uuid.uuid4().hex[:12]
        self.topic = 'tests/' + self.stream + '/data'
        self.client_id = 'collector-' + self.stream
        self.env = {'TEST_CLIENT_ID': self.client_id, 'TEST_MQTT_TOPIC': 'tests/' + self.stream + '/#'}
        self.addCleanup(self.collect_logs)
        self.addCleanup(lambda: compose('unpause', 'kafka', check=False))

    def collect_logs(self):
        logs = compose('logs', '--no-color', 'collector', check=False).stdout
        (ARTIFACTS / (self._testMethodName + '.log')).write_text(logs)
        compose('stop', 'collector', check=False)

    def start(self, **extra):
        self.env.update(extra)
        compose('up', '-d', '--force-recreate', 'collector', env=self.env)
        until(lambda: http('/ready')['status'] == 200)

    def payloads(self, count=12):
        return [json.dumps({'deviceId': self.stream, 'messageSeqId': i, 'value': i * 0.5}).encode()
                for i in range(count)]

    def publish(self, payloads, qos=1, topic=None):
        return probe({'op': 'publish', 'records': [
            {'topic': topic or self.topic, 'payload': base64.b64encode(p).decode(), 'qos': qos}
            for p in payloads]})

    def read(self, payloads, topic=None, kafka_topic='test.raw', timeout=12):
        return probe({'op': 'read', 'topic': kafka_topic, 'mqttTopic': topic or self.topic,
                      'payloads': [base64.b64encode(p).decode() for p in payloads], 'timeout': timeout})

    def assert_payloads(self, payloads, records):
        expected = {base64.b64encode(p).decode() for p in payloads}
        actual = {r['payload'] for r in records}
        self.assertEqual(expected, actual, 'Missing or altered Kafka values')
        print(f'  checked={len(expected)} records={len(records)} duplicates={len(records)-len(actual)}', flush=True)

    def exited(self):
        cid = compose('ps', '-a', '-q', 'collector').stdout.strip()
        state = json.loads(subprocess.check_output(['docker', 'inspect', cid], text=True))[0]['State']
        return state if state['Status'] == 'exited' else False

    def test_01_bytes_headers_order_routing_http(self):
        self.start()
        for path in ('/health', '/ready', '/metrics', '/actuator/health', '/actuator/info', '/actuator/prometheus'):
            self.assertEqual(200, http(path)['status'], path)
        self.assertEqual(0, metric('delivery_errors_total'))
        payloads = self.payloads() + [b'\x00\xff\x80\x01binary\x00', b'']
        self.publish(payloads)
        rows = self.read(payloads)
        self.assert_payloads(payloads, rows)
        self.assertEqual([base64.b64encode(p).decode() for p in payloads], [r['payload'] for r in rows])
        self.assertEqual(1, len({r['partition'] for r in rows}))
        for row in rows:
            self.assertEqual(self.topic, row['key'])
            self.assertEqual('1', row['headers']['mqttQos'])
            self.assertEqual('integration', row['headers']['collectorRole'])
            self.assertEqual('tcp://mqtt:1883', row['headers']['sourceBroker'])
            self.assertEqual(row['timestamp'], int(row['headers']['receivedAtUnixMs']))
            uuid.UUID(row['headers']['eventId'])
        until(lambda: metric('mqtt_acked_total') == len(payloads))
        self.assertEqual(len(payloads), metric('delivered_total'))
        event_topic = 'tests/' + self.stream + '/events'
        self.publish([b'event'], topic=event_topic)
        routed = self.read([b'event'], topic=event_topic, kafka_topic='test.events')
        self.assertEqual(self.stream, routed[0]['key'])

    def test_02_qos0_best_effort(self):
        self.start()
        payloads = self.payloads(3)
        self.publish(payloads, qos=0)
        self.assert_payloads(payloads, self.read(payloads))
        self.assertEqual(3, metric('received_qos0_total'))
        self.assertEqual(3, metric('delivered_total'))
        self.assertEqual(0, metric('mqtt_acked_total'))
        self.assertEqual(200, http('/ready')['status'])

    def test_03_qos0_reject_and_oversize(self):
        self.start(TEST_QOS0_POLICY='reject')
        self.publish([b'rejected'], qos=0)
        until(lambda: metric('qos0_unarchived_total') == 1)
        self.assertEqual([], self.read([b'rejected'], timeout=2))
        self.assertEqual(200, http('/ready')['status'])
        self.start(TEST_QOS0_POLICY='best-effort', TEST_MAX_PAYLOAD_BYTES='64')
        self.publish([b'x' * 65], qos=0)
        until(lambda: metric('rejected_total') == 1)
        self.assertEqual(1, metric('qos0_unarchived_total'))
        self.publish([b'valid'], qos=1)
        self.assert_payloads([b'valid'], self.read([b'valid']))

    def test_04_kafka_failure_no_early_puback_and_recovery(self):
        self.start()
        self.publish([b'warmup'])
        until(lambda: metric('mqtt_acked_total') == 1)
        previous_acks = compose('logs', '--no-color', 'mqtt').stdout.count('Received PUBACK from ' + self.client_id)
        compose('pause', 'kafka')
        payloads = self.payloads(6)
        self.publish(payloads)
        until(lambda: metric('received_total') > 1, timeout=10)
        self.assertEqual(1, metric('mqtt_acked_total'))
        state = until(self.exited, timeout=40)
        self.assertEqual(1, state['ExitCode'])
        broker_logs = compose('logs', '--no-color', 'mqtt').stdout
        self.assertEqual(previous_acks, broker_logs.count('Received PUBACK from ' + self.client_id))
        compose('unpause', 'kafka')
        until(lambda: probe({'op': 'bootstrap'}))
        self.start()
        self.assert_payloads([b'warmup'] + payloads, self.read([b'warmup'] + payloads))
        until(lambda: metric('mqtt_acked_total') >= len(payloads))

    def test_05_sigkill_with_unacknowledged_records(self):
        self.start()
        compose('pause', 'kafka')
        payloads = self.payloads(4)
        self.publish(payloads)
        until(lambda: metric('received_total') > 0, timeout=10)
        compose('kill', '-s', 'SIGKILL', 'collector')
        compose('unpause', 'kafka')
        until(lambda: probe({'op': 'bootstrap'}))
        self.start()
        self.assert_payloads(payloads, self.read(payloads))

    def test_06_mqtt_restart_and_offline_session(self):
        self.start()
        self.publish([b'before'])
        self.read([b'before'])
        until(lambda: metric('mqtt_acked_total') == 1)
        compose('stop', 'mqtt')
        self.assertEqual(1, until(self.exited, timeout=40)['ExitCode'])
        compose('start', 'mqtt')
        until(lambda: self.publish([b'offline']))
        self.start()
        self.assert_payloads([b'before', b'offline'], self.read([b'before', b'offline']))

    def test_07_graceful_stop_and_offline_session(self):
        self.start()
        payloads = self.payloads()
        self.publish(payloads)
        compose('stop', 'collector')
        self.assertIn(until(self.exited)['ExitCode'], (0, 143))
        self.publish([b'offline-graceful'])
        self.start()
        self.assert_payloads(payloads + [b'offline-graceful'], self.read(payloads + [b'offline-graceful']))

    def test_08_buffer_overflow_recovers_qos1(self):
        self.start(TEST_MAX_PENDING_MESSAGES='1')
        self.assertEqual(1, metric('buffer_limit_records'))
        self.publish([b'warmup'])
        until(lambda: metric('mqtt_acked_total') == 1)
        compose('pause', 'kafka')
        payloads = self.payloads(4)
        self.publish(payloads)
        self.assertEqual(1, until(self.exited, timeout=40)['ExitCode'])
        logs = compose('logs', 'collector').stdout
        self.assertIn('Buffer full', logs)
        compose('unpause', 'kafka')
        until(lambda: probe({'op': 'bootstrap'}))
        self.start(TEST_MAX_PENDING_MESSAGES='10000')
        self.assert_payloads([b'warmup'] + payloads, self.read([b'warmup'] + payloads))

    def test_09_crc32_partition_compatibility(self):
        self.start(TEST_PARTITIONING='crc32')
        payloads = self.payloads(2)
        self.publish(payloads)
        rows = self.read(payloads)
        self.assert_payloads(payloads, rows)
        self.assertEqual({zlib.crc32(self.topic.encode()) % 3}, {r['partition'] for r in rows})

    def test_10_missing_topic_fails_without_creation(self):
        missing = 'missing-' + self.stream
        self.env['TEST_KAFKA_TOPIC'] = missing
        compose('up', '-d', '--force-recreate', 'collector', env=self.env)
        self.assertNotEqual(0, until(self.exited, timeout=50)['ExitCode'])
        self.assertNotIn(missing, probe({'op': 'bootstrap'}))


if __name__ == '__main__':
    unittest.main(verbosity=2)
