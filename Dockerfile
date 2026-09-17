FROM python:3.12-slim

WORKDIR /app

# System deps for python-snappy (C extension needs libsnappy + a compiler).
# lz4 and zstandard ship manylinux wheels and need no system packages.
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc libsnappy-dev \
 && rm -rf /var/lib/apt/lists/*

# Python deps:
#  - fastapi/uvicorn: the web+REST wrapper
#  - confluent-kafka: producer/consumer for produce-test/simulate/codec (librdkafka bundled)
#  - lz4 / python-snappy / zstandard: compression codecs for the 'codec' round-trip test
RUN pip install --no-cache-dir \
      fastapi==0.115.0 "uvicorn[standard]==0.30.6" confluent-kafka==2.13.0 \
      lz4==4.3.3 python-snappy==0.7.3 zstandard==0.23.0

# Baked-in doctor + web wrapper (portable: does not depend on host copy)
COPY kafka_doctor.py /app/kafka_doctor.py
COPY app.py          /app/app.py
COPY kdx.py          /app/kdx.py
COPY kdproxy.py      /app/kdproxy.py
COPY index.html      /app/index.html

ENV KD_PORT=8899 \
    KD_DEFAULT_BOOTSTRAP=127.0.0.1:9092 \
    KD_DOCTOR=/app/kafka_doctor.py \
    KD_LOG=/data/kafka-doctor-web.log \
    KD_TIMEOUT=8

EXPOSE 8899
VOLUME ["/data"]

CMD ["python", "app.py"]
