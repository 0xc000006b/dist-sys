FROM python:3.10-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir flask
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

CMD ["python", "config-server.py"]
