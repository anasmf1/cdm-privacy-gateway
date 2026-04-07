FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    confluent-kafka==2.3.0 \
    faker==24.0.0

COPY synthetic_data/ ./

ENV PYTHONUNBUFFERED=1

CMD ["python", "kafka_producer.py"]
