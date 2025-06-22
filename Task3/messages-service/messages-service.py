from flask import Flask, request, jsonify
import hazelcast
import os
import logging
import time

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [MSG] %(message)s")
logger = logging.getLogger(__name__)

time.sleep(5)

hz_clusters = os.environ.get("HZ_CLUSTERS", "hazelcast1:5701,hazelcast2:5701,hazelcast3:5701").split(',')
try:
    client = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=hz_clusters,
        reconnect_mode="ASYNC"
    )
    message_map = client.get_map("messages").blocking()
    logger.info("Connected to Hazelcast cluster")
except Exception as e:
    logger.error(f"Hazelcast connection failed: {e}")
    raise

@app.route('/messages', methods=['GET'])
def get_messages():
    """Retrieve messages from Hazelcast."""
    try:
        msg_id = request.args.get('id')
        if msg_id:
            if message_map.contains_key(msg_id):
                message = message_map.get(msg_id)
                logger.info(f"Retrieved message id={msg_id}")
                return jsonify({"id": msg_id, "text": message}), 200
            logger.warning(f"Message id={msg_id} not found")
            return jsonify({"error": "Message not found"}), 404

        messages = [{"id": key, "text": value} for key, value in message_map.entry_set()]
        logger.info(f"Retrieved {len(messages)} messages")
        return jsonify(messages), 200
    except Exception as e:
        logger.error(f"Failed to fetch messages: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting messages-service on port 8882")
    app.run(host='0.0.0.0', port=8882)