FROM python:3.10-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir flask hazelcast-python-client

CMD ["python", "logging-service.py"]