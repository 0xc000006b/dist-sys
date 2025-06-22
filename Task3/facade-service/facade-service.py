from flask import Flask, request, jsonify
import requests
import uuid
import random
import time
import os
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FCD] %(message)s")
logger = logging.getLogger(__name__)

CONFIG_URL = os.environ.get("CONFIG_URL", "http://config-server:8883/services")
MAX_ATTEMPTS = 3

def retrieve_service_urls(service):
    """Fetch service URLs from config-server."""
    full_url = f"{CONFIG_URL}/{service}" 
    logger.info(f"Requesting URLs from {full_url}")
    try:
        response = requests.get(full_url, timeout=2.5)
        response.raise_for_status()
        urls = response.json().get("urls", [])
        if not urls:
            logger.error(f"No URLs returned for {service}")
        logger.info(f"Retrieved URLs for {service}: {urls}")
        return urls
    except requests.RequestException as e:
        logger.error(f"Failed to fetch URLs for {service} from {full_url}: {e}")
        return []

def post_with_retries(services, payload, msg_id):
    """Send payload to a service with retries."""
    logger.info(f"Attempting to send message id={msg_id}")
    random.shuffle(services)
    tried = set()
    attempt = 0
    while attempt < MAX_ATTEMPTS and len(tried) < len(services):
        service = services[attempt % len(services)]
        if service in tried:
            attempt += 1
            continue
        tried.add(service)
        delay = 0.5 * (2 ** attempt)
        try:
            logger.info(f"Attempt {attempt + 1} to {service}")
            response = requests.post(service, data=payload, timeout=2.5)
            if response.status_code == 201:
                logger.info(f"Successfully sent to {service}")
                return True, ""
            logger.warning(f"Failed at {service}: status {response.status_code}")
        except requests.RequestException as e:
            logger.error(f"Error sending to {service}: {e}")
        if attempt < MAX_ATTEMPTS - 1:
            logger.info(f"Waiting {delay}s before retry")
            time.sleep(delay)
        attempt += 1
    logger.error(f"Failed to send id={msg_id} after {MAX_ATTEMPTS} attempts")
    return False, "All attempts failed"

@app.route('/messages', methods=['POST'])
def handle_post():
    """Process POST requests to store messages."""
    logger.info(f"Received POST: {request.form}")
    text = request.form.get('txt')
    if not text:
        logger.warning("Missing txt field")
        return jsonify({"error": "Text required"}), 400

    msg_id = str(uuid.uuid4())
    payload = {"id": msg_id, "txt": text}
    logging_services = retrieve_service_urls("logging")
    if not logging_services:
        logger.error("No logging services available")
        return jsonify({"error": "Logging services unavailable"}), 503

    success, error = post_with_retries(logging_services, payload, msg_id)
    if not success:
        logger.error(f"Failed to process: {error}")
        return jsonify({"error": error}), 500

    logger.info(f"Stored message id={msg_id}")
    return jsonify({"result": "ok", "id": msg_id}), 200

@app.route('/messages', methods=['GET'])
def handle_get():
    """Process GET requests to retrieve messages."""
    logging_services = retrieve_service_urls("logging")
    messages_services = retrieve_service_urls("messages")
    if not logging_services:
        logger.error("No logging services available")
        return jsonify({"error": "Logging services unavailable"}), 503

    results = []
    random.shuffle(logging_services)
    idx = 0
    while idx < len(logging_services):
        service = logging_services[idx]
        logger.info(f"Fetching from {service}")
        try:
            response = requests.get(service, timeout=2.5)
            data = response.json()
            results.append({
                "service": service,
                "instance": data.get("instance"),
                "messages": data.get("messages")
            })
            logger.info(f"Retrieved from {service}: {data.get('messages')}")
            break
        except requests.RequestException as e:
            logger.error(f"Failed to fetch from {service}: {e}")
            results.append({"service": service, "error": str(e)})
            idx += 1
    if idx >= len(logging_services):
        logger.error("No logs retrieved")

    all_msgs = []
    for service in messages_services:
        logger.info(f"Fetching messages from {service}")
        try:
            response = requests.get(service, timeout=2.5)
            all_msgs = response.json()
            logger.info(f"Retrieved from messages: {all_msgs}")
            break
        except requests.RequestException as e:
            logger.error(f"Failed to fetch from {service}: {e}")
            all_msgs = {"error": str(e)}

    logger.info("Returning combined response")
    return jsonify({"logging_services": results, "all_messages": all_msgs}), 200

if __name__ == '__main__':
    debug = os.getenv('FCD_DEBUG', 'false').lower() == 'true'
    logger.info("Starting facade-service on port 8880")
    app.run(host='0.0.0.0', port=8880, debug=debug)