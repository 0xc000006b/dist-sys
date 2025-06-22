FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir flask hazelcast-python-client

CMD ["python", "messages-service.py"]