import requests
import uuid
import random
import os
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import logging
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Get environment variables with defaults
cfg_server = os.environ.get("CFG_SERVER_URL", "http://config-server:8881")
kafka_servers = os.environ.get("KAFKA_SERVERS", "kafka1:9092,kafka2:9093,kafka3:9094").split(',')

def init_kafka_producer():
    """Initialize and return a Kafka producer."""
    logger.info("Connecting to Kafka cluster")
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            batch_size=16384,
            linger_ms=10
        )
        logger.info("Kafka producer initialized")
        return producer
    except Exception as e:
        logger.error(f"Kafka connection failed: {e}")
        raise

def fetch_svc_urls(svc_name):
    """Fetch service URLs from the config server."""
    logger.info(f"Querying config server for {svc_name} URLs")
    try:
        resp = requests.get(f"{cfg_server}/svc/{svc_name}", timeout=5)
        if resp.status_code == 200:
            urls = resp.json().get("urls", [])
            logger.info(f"Got URLs for {svc_name}: {urls}")
            return urls
        logger.error(f"Config server error for {svc_name}: {resp.status_code}")
        return []
    except Exception as e:
        logger.error(f"Failed to fetch URLs for {svc_name}: {e}")
        return []

def send_to_service(services, data, max_retries=3):
    """Send data to a randomly selected service with retries."""
    attempted = set()
    for _ in range(max_retries):
        if len(attempted) == len(services):
            break
        svc = random.choice([s for s in services if s not in attempted])
        attempted.add(svc)
        try:
            logger.info(f"Sending data to {svc}")
            resp = requests.post(svc, json=data, timeout=5)
            if resp.status_code == 201:
                logger.info(f"Data sent to {svc}")
                return resp
        except Exception as e:
            logger.warning(f"Failed to send to {svc}: {e}")
    logger.error(f"All services failed for data: {data}")
    raise Exception("All services unavailable")

@app.route('/msg', methods=['POST', 'GET'])
def process_messages():
    if request.method == 'POST':
        content = request.form.get('content')
        if not content:
            logger.warning("Empty message received")
            return jsonify({'error': 'Message content missing'}), 400
        
        msg_id = str(uuid.uuid4())
        msg = {'id': msg_id, 'content': content}

        # Fetch logging service URLs
        log_svcs = fetch_svc_urls("log")
        if not log_svcs:
            logger.error("No logging services found")
            return jsonify({'error': 'Logging services unavailable'}), 503
        
        try:
            send_to_service(log_svcs, msg)
        except Exception as e:
            logger.error(f"Logging service error: {e}")
            return jsonify({'error': str(e)}), 500

        # Initialize producer per request
        producer = init_kafka_producer()
        try:
            partition = int(hashlib.md5(msg_id.encode()).hexdigest(), 16) % 3  # Розподіл по 3 партиціях
            logger.info(f"Sending message {msg_id} to partition {partition}")
            producer.send('msg-queue', msg, partition=partition)
            producer.flush()
            logger.info(f"Message sent to Kafka: {msg_id}")
        except Exception as e:
            logger.error(f"Kafka send error: {e}")
            return jsonify({'error': str(e)}), 500
        finally:
            producer.close()  # Clean up producer

        return jsonify(msg), 200

    elif request.method == 'GET':
        log_svcs = fetch_svc_urls("log")
        msg_svcs = fetch_svc_urls("msg")
        results = {'log_services': [], 'msg_services': [], 'combined_messages': []}

        if log_svcs:
            log_svc = random.choice(log_svcs)
            try:
                logger.info(f"Querying logging service: {log_svc}")
                resp = requests.get(log_svc, timeout=5)
                data = resp.json()
                results['log_services'].append({
                    'service': log_svc,
                    'instance': data.get('node'),
                    'messages': data.get('messages', [])
                })
            except Exception as e:
                logger.error(f"Error querying {log_svc}: {e}")
                results['log_services'].append({'service': log_svc, 'error': str(e)})

        if msg_svcs:
            for svc in msg_svcs:
                try:
                    logger.info(f"Querying message service: {svc}")
                    resp = requests.get(svc, timeout=5)
                    messages = resp.json()
                    results['msg_services'].extend(messages)
                    for msg in messages:
                        results['combined_messages'].append({'id': msg['id'], 'content': msg['content']})
                except Exception as e:
                    logger.error(f"Error querying {svc}: {e}")

        if results['log_services']:
            for log_result in results['log_services']:
                if 'messages' in log_result:
                    for msg_text in log_result['messages']:
                        try:
                            msg_content = msg_text.split(' (handled by')[0]
                            matching_msg = next((m for m in results['msg_services'] if m['content'] == msg_content), None)
                            if matching_msg:
                                results['combined_messages'].append({
                                    'id': matching_msg['id'],
                                    'content': msg_content
                                })
                        except Exception as e:
                            logger.error(f"Failed to parse log message {msg_text}: {e}")

        seen_ids = set()
        results['combined_messages'] = [m for m in results['combined_messages'] if not (m['id'] in seen_ids or seen_ids.add(m['id']))]

        return jsonify({
            'log_services': results['log_services'],
            'msg_services': results['msg_services'],
            'combined_messages': results['combined_messages']
        }), 200

if __name__ == '__main__':
    logger.info("Launching facade service on port 8880")
    app.run(host='0.0.0.0', port=8880)