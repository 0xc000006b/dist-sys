FROM python:3.10-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir flask hazelcast-python-client requests tenacity kafka-python python-consul

CMD ["python", "facade-service.py"]
