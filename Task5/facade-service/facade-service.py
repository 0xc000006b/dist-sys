import requests
import random
import os
from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import consul
import atexit
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
cfg_server_host = os.environ.get("CONSUL_HOST", "consul")
cfg_server_port = int(os.environ.get("CONSUL_PORT", "8500"))
node_id = os.environ.get("INSTANCE_ID", "facade1")
svc_port = 8880

consul_svc = consul.Consul(host=cfg_server_host, port=cfg_server_port)

def init_service():
    consul_svc.agent.service.register(
        service_id=node_id,
        name="facade-service",
        address="facade-service",
        port=svc_port,
        check=consul.Check.http(f"http://facade-service:{svc_port}/health", interval="10s", timeout="5s")
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
            logger.error(f"[{node_id}] Failed to fetch Kafka config: {e}")
            time.sleep(5)
    raise Exception(f"[{node_id}] Unable to fetch Kafka config after retries")

kafka_servers, kafka_topic = fetch_kafka_settings()

def init_kafka_producer():
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                batch_size=16384,
                linger_ms=10
            )
            logger.info(f"[{node_id}] Kafka producer initialized")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"[{node_id}] Kafka unavailable (attempt {attempt+1}/10), retrying in 5s...")
            time.sleep(5)
    raise Exception(f"[{node_id}] Kafka unavailable after retries")

producer = init_kafka_producer()

def fetch_svc_urls(svc_name):
    try:
        _, instances = consul_svc.catalog.service(svc_name)
        urls = [f"http://{s['ServiceAddress']}:{s['ServicePort']}" for s in instances if s["ServiceID"]]
        logger.info(f"[{node_id}] Fetched URLs for {svc_name}: {urls}")
        return urls
    except Exception as e:
        logger.error(f"[{node_id}] Failed to fetch URLs for {svc_name}: {e}")
        return []

def send_to_service(svc_name, endpoint, method="GET", payload=None):
    urls = fetch_svc_urls(svc_name)
    random.shuffle(urls)
    for url in urls:
        try:
            full_url = f"{url}{endpoint}"
            if method == "POST":
                resp = requests.post(full_url, json=payload, timeout=3)
            else:
                resp = requests.get(full_url, timeout=3)
            if resp.status_code in (200, 201):
                logger.info(f"[{node_id}] Successfully sent to {url}")
                return resp.json()
        except Exception as e:
            logger.warning(f"[{node_id}] Failed to reach {url}: {e}")
    logger.error(f"[{node_id}] All instances of {svc_name} failed")
    return {"error": f"All {svc_name} instances unavailable"}, 500

@app.route("/msg", methods=["POST", "GET"])
def process_messages():
    if request.method == "POST":
        content = request.json.get("content")
        if not content:
            logger.warning(f"[{node_id}] Empty message received")
            return jsonify({"error": "Message content missing"}), 400

        msg = {"content": content}
        try:
            producer.send(kafka_topic, msg)
            producer.flush()
            logger.info(f"[{node_id}] Message sent to Kafka: {content}")
        except Exception as e:
            logger.error(f"[{node_id}] Kafka send error: {e}")
            return jsonify({"error": str(e)}), 500

        response = send_to_service("logging-service", "/log", method="POST", payload=msg)
        if "error" in response:
            logger.error(f"[{node_id}] Logging service error: {response['error']}")
            return jsonify({"error": "Failed to save to logging-service"}), 500

        logger.info(f"[{node_id}] Successfully sent to logging-service: {response}")
        return jsonify({"status": "Sent to Kafka and logging-service"}), 201

    elif request.method == "GET":
        results = {"log_services": [], "msg_services": [], "combined_messages": []}

        log_svcs = fetch_svc_urls("logging-service")
        if log_svcs:
            log_response = send_to_service("logging-service", "/log", method="GET")
            if "error" not in log_response:
                results["log_services"].append({
                    "service": "logging-service",
                    "messages": log_response.get("messages", [])
                })

        msg_svcs = fetch_svc_urls("messages-service")
        if msg_svcs:
            msg_response = send_to_service("messages-service", "/msg", method="GET")
            if "error" not in msg_response:
                results["msg_services"] = msg_response.get("messages", [])

        combined = list(set(
            [m for m in results["log_services"][0]["messages"]] +
            [m for m in results["msg_services"]]
        ))
        results["combined_messages"] = [{"content": m} for m in combined]

        return jsonify(results), 200

if __name__ == "__main__":
    init_service()
    atexit.register(cleanup_service)
    logger.info(f"[{node_id}] Starting facade service on port {svc_port}")
    app.run(host="0.0.0.0", port=svc_port)
