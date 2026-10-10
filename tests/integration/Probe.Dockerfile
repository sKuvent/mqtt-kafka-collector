FROM python:3.12-slim
RUN pip install --no-cache-dir paho-mqtt==2.1.0 confluent-kafka==2.6.1
COPY probe.py /tests/probe.py
