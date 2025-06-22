from flask import Flask, request, jsonify
import hazelcast
import os
import socket
import logging
import time

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [LOG] %(message)s")
logger = logging.getLogger(__name__)

instance = socket.gethostname()

time.sleep(5)

hz_clusters = os.environ.get("HZ_CLUSTERS", "hazelcast1:5701,hazelcast2:5701,hazelcast3:5701").split(',')
try:
    client = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=hz_clusters,
        reconnect_mode="ASYNC"
    )
    message_map = client.get_map("messages").blocking()
    logger.info(f"[{instance}] Connected to Hazelcast cluster")
except Exception as e:
    logger.error(f"[{instance}] Failed to connect to Hazelcast: {e}")
    raise

@app.route('/logging', methods=['POST', 'GET'])
def process_logging():
    """Handle logging requests."""
    logger.info(f"[{instance}] Received {request.method} request")
    if request.method == 'POST':
        msg_id = request.form.get('id')
        text = request.form.get('txt')
        if not (msg_id and text):
            logger.warning(f"[{instance}] Missing id or txt")
            return jsonify({"error": "Id and txt required"}), 400

        if message_map.contains_key(msg_id):
            logger.info(f"[{instance}] Duplicate id={msg_id} detected")
            return jsonify({"message": "Duplicate entry"}), 200

        try:
            message_map.put(msg_id, f"{text} [via {instance}]")
            logger.info(f"[{instance}] Stored id={msg_id}, txt={text}")
            return jsonify({"message": "Stored", "id": msg_id}), 201
        except Exception as e:
            logger.error(f"[{instance}] Failed to store id={msg_id}: {e}")
            return jsonify({"error": str(e)}), 500

    elif request.method == 'GET':
        try:
            all_messages = list(message_map.values())
            logger.info(f"[{instance}] Retrieved {len(all_messages)} messages")
            return jsonify({"instance": instance, "messages": all_messages}), 200
        except Exception as e:
            logger.error(f"[{instance}] Failed to retrieve messages: {e}")
            return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('LOG_PORT', 50051))
    logger.info(f"[{instance}] Starting logging-service on port {port}")
    app.run(host='0.0.0.0', port=port)