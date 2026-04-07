FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    confluent-kafka==2.3.0 \
    redis==5.0.3 \
    requests==2.31.0 \
    pyyaml==6.0.1

COPY streamguard/ ./streamguard/
COPY cdm_extensions/ ./cdm_extensions/
COPY templates/ ./templates/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["python", "streamguard/pipeline.py"]
