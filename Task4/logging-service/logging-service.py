from flask import Flask, request, jsonify
import hazelcast
import os
import socket
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
node_id = socket.gethostname()

hz_nodes = os.environ.get("HZ_NODES", "hazelcast1:5701,hazelcast2:5701,hazelcast3:5701").split(',')
hz_client = hazelcast.HazelcastClient(cluster_name="dev", cluster_members=hz_nodes)
msg_store = hz_client.get_map("msg-store").blocking()

@app.route('/log', methods=['POST'])
def store_message():
    try:
        data = request.get_json()
        if not data or 'id' not in data or 'content' not in data:
            logger.warning(f"[{node_id}] Invalid message received")
            return jsonify({'error': 'Invalid message format'}), 400
        msg_id = data['id']
        msg_content = data['content']
        msg_store.put(msg_id, f"{msg_content} (handled by {node_id})")
        logger.info(f"[{node_id}] Stored message ID {msg_id}")
        return jsonify({'status': 'stored'}), 201
    except Exception as e:
        logger.error(f"[{node_id}] Error storing message: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/log', methods=['GET'])
def retrieve_messages():
    try:
        messages = list(msg_store.values())
        logger.info(f"[{node_id}] Fetched {len(messages)} messages")
        return jsonify({'node': node_id, 'messages': messages})
    except Exception as e:
        logger.error(f"[{node_id}] Error fetching messages: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 50051))
    logger.info(f"[{node_id}] Starting logging service on port {port}")
    app.run(host='0.0.0.0', port=port)