from flask import Flask, request, jsonify
from kafka import KafkaConsumer
import os
import socket
import logging
import json
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
node_id = socket.gethostname()
msg_store = []


kafka_servers = os.environ.get("KAFKA_SERVERS", "kafka1:9092,kafka2:9093,kafka3:9094").split(',')

def init_kafka_consumer():
    logger.info(f"[{node_id}] Initializing Kafka consumer")
    try:
        consumer = KafkaConsumer(
            'msg-queue',
            bootstrap_servers=kafka_servers,
            auto_offset_reset='earliest',
            group_id='msg-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            fetch_max_bytes=52428800
        )
        logger.info(f"[{node_id}] Kafka consumer ready")
        return consumer
    except Exception as e:
        logger.error(f"[{node_id}] Kafka consumer failed: {e}")
        raise

consumer = init_kafka_consumer()

def poll_messages():
    logger.info(f"[{node_id}] Starting message consumption")
    for msg in consumer:
        data = msg.value
        msg_store.append(data)
        logger.info(f"[{node_id}] Consumed message ID {data['id']}: {data['content']}")

threading.Thread(target=poll_messages, daemon=True).start()

@app.route('/msg', methods=['GET'])
def fetch_messages():
    try:
        msg_id = request.args.get('id')
        if msg_id:
            for msg in msg_store:
                if msg['id'] == msg_id:
                    return jsonify({'id': msg['id'], 'content': msg['content']})
            logger.warning(f"[{node_id}] Message ID {msg_id} not found")
            return jsonify({'error': 'Message not found'}), 404

        logger.info(f"[{node_id}] Returning {len(msg_store)} messages")
        return jsonify([{'id': m['id'], 'content': m['content']} for m in msg_store])
    except Exception as e:
        logger.error(f"[{node_id}] Error fetching messages: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8882))
    logger.info(f"[{node_id}] Starting message service on port {port}")
    app.run(host='0.0.0.0', port=port)