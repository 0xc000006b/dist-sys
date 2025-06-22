import json

from flask import Flask, request, jsonify
import hazelcast
import os
import socket
import logging
import consul
import time
import atexit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
node_id = os.environ.get("INSTANCE_ID", "default")
svc_port = int(os.environ.get("PORT", "8881"))
cfg_server_host = os.environ.get("CONSUL_HOST", "consul")
cfg_server_port = int(os.environ.get("CONSUL_PORT", "8500"))

consul_svc = consul.Consul(host=cfg_server_host, port=cfg_server_port)

def init_service():
    consul_svc.agent.service.register(
        service_id=node_id,
        name="logging-service",
        address=f"logging-service{node_id[-1]}",
        port=svc_port,
        check=consul.Check.http(f"http://logging-service{node_id[-1]}:{svc_port}/health", interval="10s", timeout="5s")
    )
    logger.info(f"[{node_id}] Registered in Consul as {node_id}")

def cleanup_service():
    consul_svc.agent.service.deregister(node_id)
    logger.info(f"[{node_id}] Deregistered from Consul")

@app.route("/health", methods=["GET"])
def check_health():
    return jsonify({"status": "healthy"}), 200

def fetch_hz_settings():
    for attempt in range(10):
        try:
            _, members_data = consul_svc.kv.get("config/hazelcast/cluster_members")
            if members_data is None:
                raise ValueError("Key config/hazelcast/cluster_members not found in Consul")
            members = json.loads(members_data["Value"].decode("utf-8"))
            _, cluster_data = consul_svc.kv.get("config/hazelcast/cluster_name")
            if cluster_data is None:
                raise ValueError("Key config/hazelcast/cluster_name not found in Consul")
            cluster_name = json.loads(cluster_data["Value"].decode("utf-8"))
            logger.info(f"[{node_id}] Fetched Hazelcast config: members={members}, cluster_name={cluster_name}")
            return members, cluster_name
        except Exception as e:
            logger.error(f"[{node_id}] Attempt {attempt + 1} failed: {e}")
            time.sleep(5)
    raise Exception(f"[{node_id}] Unable to fetch Hazelcast config after retries")

hz_nodes, hz_cluster = fetch_hz_settings()
hz_client = hazelcast.HazelcastClient(cluster_name=hz_cluster, cluster_members=hz_nodes)
msg_store = hz_client.get_map("msg-store").blocking()

@app.route("/log", methods=["POST", "GET"])
def handle_log():
    if request.method == "POST":
        if request.is_json:
            data = request.get_json()
            msg_content = data.get("content")
        else:
            msg_content = request.form.get("content")

        if not msg_content:
            logger.warning(f"[{node_id}] Invalid message received")
            return jsonify({"error": "Message content missing"}), 400

        if msg_store.contains_key(msg_content):
            logger.info(f"[{node_id}] Skipped duplicate: {msg_content}")
            return jsonify({"status": "Duplicate"}), 201

        msg_store.put(msg_content, f"{msg_content} (handled by {node_id})")
        logger.info(f"[{node_id}] Stored message: {msg_content}")
        return jsonify({"status": "Stored"}), 201

    elif request.method == "GET":
        messages = list(msg_store.values())
        logger.info(f"[{node_id}] Fetched {len(messages)} messages")
        return jsonify({"node": node_id, "messages": messages})

if __name__ == "__main__":
    time.sleep(20)
    init_service()
    atexit.register(cleanup_service)
    logger.info(f"[{node_id}] Starting logging service on port {svc_port}")
    app.run(host="0.0.0.0", port=svc_port)
