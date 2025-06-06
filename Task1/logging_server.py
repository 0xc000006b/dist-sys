import threading
from concurrent import futures
import grpc
import time

import logging_pb2
import logging_pb2_grpc

storage = {}
storage_lock = threading.Lock()

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class LoggingServiceServicer(logging_pb2_grpc.LoggingServiceServicer):
    def LogMessage(self, request, context):
        msg_id = request.id
        msg_text = request.msg

        if not msg_id or not msg_text:
            return logging_pb2.LogResponse(
                success=False,
                error="Missing 'id' or 'msg' in request"
            )

        with storage_lock:
            if msg_id in storage:
                print(f"[logging-service] Duplicate detected for id={msg_id}. Skipping storage.")
                return logging_pb2.LogResponse(success=True, error="")

            storage[msg_id] = msg_text
            print(f"[logging-service] Stored new message: id={msg_id}, msg='{msg_text}'")

        return logging_pb2.LogResponse(success=True, error="")

    def GetMessages(self, request, context):
        with storage_lock:
            all_msgs = list(storage.values())
        response = logging_pb2.MessagesResponse(messages=all_msgs)
        print(f"[logging-service] Returning all messages: {all_msgs}")
        return response


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_pb2_grpc.add_LoggingServiceServicer_to_server(
        LoggingServiceServicer(), server
    )
    server.add_insecure_port('[::]:50051')
    server.start()
    print("[logging-service] gRPC server started on port 50051")
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        print("[logging-service] Shutting down server...")
        server.stop(0)


if __name__ == '__main__':
    serve()
