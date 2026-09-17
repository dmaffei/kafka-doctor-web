FROM python:3.12-slim

WORKDIR /app

# confluent-kafka ships a manylinux wheel with librdkafka bundled — no apt needed.
RUN pip install --no-cache-dir fastapi==0.115.0 "uvicorn[standard]==0.30.6" confluent-kafka==2.13.0

# Baked-in doctor + web wrapper (portable: does not depend on host copy)
COPY kafka_doctor.py /app/kafka_doctor.py
COPY app.py          /app/app.py
COPY index.html      /app/index.html

ENV KD_PORT=8899 \
    KD_DEFAULT_BOOTSTRAP=127.0.0.1:9092 \
    KD_DOCTOR=/app/kafka_doctor.py \
    KD_LOG=/data/kafka-doctor-web.log \
    KD_TIMEOUT=8

EXPOSE 8899
VOLUME ["/data"]

CMD ["python", "app.py"]
