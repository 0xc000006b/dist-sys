from flask import Flask, request, jsonify
import uuid
import grpc
import time

import logging_pb2
import logging_pb2_grpc

import requests

app = Flask(__name__)

LOGGING_SERVICE_GRPC_HOST = 'localhost'
LOGGING_SERVICE_GRPC_PORT = 50051
MESSAGES_SERVICE_URL = 'http://localhost:5002/messages'

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 1

def get_logging_stub():
    target = f"{LOGGING_SERVICE_GRPC_HOST}:{LOGGING_SERVICE_GRPC_PORT}"
    channel = grpc.insecure_channel(target)
    stub = logging_pb2_grpc.LoggingServiceStub(channel)
    return stub


def grpc_log_message_with_retry(msg_id: str, msg_text: str):
    stub = get_logging_stub()
    request = logging_pb2.LogRequest(id=msg_id, msg=msg_text)

    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[facade] Attempt #{attempt} to send LogMessage(id={msg_id})")
            response = stub.LogMessage(request, timeout=3.0)
            if response.success:
                if response.error:
                    return False, f"LoggingService returned error: {response.error}"
                print(f"[facade] LogMessage success for id={msg_id}")
                return True, ""
            else:
                return False, f"LoggingService returned success=False, error={response.error}"
        except grpc.RpcError as e:
            last_exception = e
            print(f"[facade] gRPC attempt #{attempt} failed: {e}. Retrying in {RETRY_DELAY_SECONDS}s...")
            time.sleep(RETRY_DELAY_SECONDS)

    return False, f"Failed to send to LoggingService after {MAX_RETRIES} attempts: {last_exception}"


@app.route('/messages', methods=['POST'])
def post_message():
    data = request.get_json(force=True)
    msg_text = data.get('msg')
    if not msg_text:
        return jsonify({"error": "Missing 'msg' in request body"}), 400

    new_id = str(uuid.uuid4())

    success, error_msg = grpc_log_message_with_retry(new_id, msg_text)
    if not success:
        return jsonify({"error": error_msg}), 502

    return jsonify({"status": "ok", "id": new_id}), 200


@app.route('/messages', methods=['GET'])
def get_messages():
    try:
        stub = get_logging_stub()
        empty_req = logging_pb2.Empty()
        resp = stub.GetMessages(empty_req, timeout=3.0)
        logs_list = resp.messages
        print(f"[facade] Received from logging-service: {logs_list}")
    except grpc.RpcError as e:
        err = f"Failed to get messages from logging-service: {e}"
        print(f"[facade] {err}")
        return jsonify({"error": err}), 502

    try:
        http_resp = requests.get(MESSAGES_SERVICE_URL, timeout=3.0)
        http_resp.raise_for_status()
        msg_text = http_resp.text.strip()
        print(f"[facade] Received from messages-service: '{msg_text}'")
    except requests.RequestException as e:
        err = f"Failed to get from messages-service: {e}"
        print(f"[facade] {err}")
        return jsonify({"error": err}), 502

    if logs_list:
        combined_logs = ', '.join(logs_list)
        combined = f"{combined_logs} | {msg_text}"
    else:
        combined = msg_text

    return combined, 200, {'Content-Type': 'text/plain; charset=utf-8'}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
