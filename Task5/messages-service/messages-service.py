from flask import Flask, jsonify
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import threading
import os
import time
import consul
import atexit
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
node_id = os.environ.get("INSTANCE_ID", "default")
svc_port = int(os.environ.get("PORT", "8890"))
cfg_server_host = os.environ.get("CONSUL_HOST", "consul")
cfg_server_port = int(os.environ.get("CONSUL_PORT", "8500"))

msg_store = []

consul_svc = consul.Consul(host=cfg_server_host, port=cfg_server_port)

def init_service():
    consul_svc.agent.service.register(
        service_id=node_id,
        name="messages-service",
        address=f"messages-service{node_id[-1]}",
        port=svc_port,
        check=consul.Check.http(f"http://messages-service{node_id[-1]}:{svc_port}/health", interval="10s", timeout="5s")
    )
    logger.info(f"[{node_id}] Registered in Consul as {node_id}")

def cleanup_service():
    consul_svc.agent.service.deregister(node_id)
    logger.info(f"[{node_id}] Deregistered from Consul")

@app.route("/health", methods=["GET"])
def check_health():
    return jsonify({"status": "healthy"}), 200

def fetch_kafka_settings():
    for attempt in range(10):
        try:
            _, brokers_data = consul_svc.kv.get("config/kafka/brokers")
            if brokers_data is None:
                raise ValueError("Key config/kafka/brokers not found in Consul")
            brokers = json.loads(brokers_data["Value"].decode("utf-8"))
            _, topic_data = consul_svc.kv.get("config/kafka/topic")
            if topic_data is None:
                raise ValueError("Key config/kafka/topic not found in Consul")
            topic = json.loads(topic_data["Value"].decode("utf-8"))
            logger.info(f"[{node_id}] Fetched Kafka config: brokers={brokers}, topic={topic}")
            return brokers, topic
        except Exception as e:
            logger.error(f"[{node_id}] Attempt {attempt + 1} failed: {e}")
            time.sleep(5)
    raise Exception(f"[{node_id}] Unable to fetch Kafka config after retries")

def init_kafka_consumer():
    kafka_servers, kafka_topic = fetch_kafka_settings()
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=kafka_servers,
                auto_offset_reset="earliest",
                group_id="msg-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                fetch_max_bytes=52428800
            )
            logger.info(f"[{node_id}] Kafka consumer initialized")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"[{node_id}] Kafka unavailable (attempt {attempt+1}/10), retrying in 5s...")
            time.sleep(5)
    raise Exception(f"[{node_id}] Kafka unavailable after retries")

def poll_messages():
    consumer = init_kafka_consumer()
    logger.info(f"[{node_id}] Starting message consumption")
    for msg in consumer:
        data = msg.value.get("content")
        if data:
            msg_store.append(data)
            logger.info(f"[{node_id}] Consumed message: {data}")

@app.route("/msg", methods=["GET"])
def fetch_messages():
    logger.info(f"[{node_id}] Returning {len(msg_store)} messages")
    return jsonify({"messages": msg_store})

if __name__ == "__main__":
    time.sleep(20)
    init_service()
    atexit.register(cleanup_service)
    logger.info(f"[{node_id}] Starting message service on port {svc_port}")
    threading.Thread(target=poll_messages, daemon=True).start()
    app.run(host="0.0.0.0", port=svc_port)
