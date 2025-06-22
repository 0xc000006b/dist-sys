from flask import Flask, jsonify
import os
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CFG] %(message)s")
logger = logging.getLogger(__name__)

logging_urls = os.environ.get("LOGGING_URLS", "logging1:50051,logging2:50052,logging3:50053").split(',')
messages_urls = os.environ.get("MESSAGES_URLS", "messages:8882").split(',')

logging_urls = [f"http://{url}/logging" for url in logging_urls]
messages_urls = [f"http://{url}/messages" for url in messages_urls]

@app.route('/services/<service_name>', methods=['GET'])
def retrieve_service_urls(service_name):
    """Return URLs for the requested service."""
    logger.info(f"Query for {service_name} URLs")
    if service_name == "logging":
        return jsonify({"urls": logging_urls}), 200
    elif service_name == "messages":
        return jsonify({"urls": messages_urls}), 200
    logger.warning(f"Service {service_name} not found")
    return jsonify({"error": f"Service {service_name} not found"}), 404

if __name__ == '__main__':
    port = int(os.environ.get("CFG_PORT", 8883))
    logger.info(f"Starting config-server on port {port}")
    app.run(host='0.0.0.0', port=port)