from flask import Flask, jsonify
import os

app = Flask(__name__)

# Fetch service addresses from environment variables
log_svc_addrs = os.environ.get("LOG_SVCS", "logging1:50051,logging2:50052,logging3:50053").split(',')
msg_svc_addrs = os.environ.get("MSG_SVCS", "messages1:8882,messages2:8883").split(',')

# Construct full URLs for services
log_svc_urls = [f"http://{addr}/log" for addr in log_svc_addrs]
msg_svc_urls = [f"http://{addr}/msg" for addr in msg_svc_addrs]

@app.route('/svc/<svc_name>', methods=['GET'])
def fetch_service_urls(svc_name):
    if svc_name == "log":
        return jsonify({"urls": log_svc_urls})
    elif svc_name == "msg":
        return jsonify({"urls": msg_svc_urls})
    return jsonify({"error": f"Service {svc_name} not found"}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8881)