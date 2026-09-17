# kafka-doctor-web

Portable, self-contained web + REST wrapper around **kafka_doctor.py**. Drop it on
any NETSCOUT AI Streamer host to validate the Kafka path **from that host** —
the same network position the streamer produces from — plus inspect topics,
consumer groups, and run audited produce/consume tests.

## Why run it on the streamer host
Kafka connectivity problems are usually path/firewall or advertised-listener
issues that only reproduce from the streamer's own source IP and egress route.
This container runs with `--network host`, so every test originates from the
host it runs on — making "connect: all stages OK" a valid proof that the
streamer's path is open.

## Contents
- `kafka-doctor-web-image.tar.gz` — prebuilt image (kafka_doctor + FastAPI + confluent-kafka baked in). Offline-ready.
- `kafka_doctor.py`, `app.py`, `index.html`, `Dockerfile` — source, for rebuilds.
- `deploy.sh` — one-command deploy (loads image, runs container, host network).
- `data/` — created at runtime; holds the persistent write-audit log.

## Requirements
Docker on the target host. No internet, no Python, no pip on the host — it's all in the image.

## Deploy
```bash
unzip kafka-doctor-web-bundle.zip -d kafka-doctor-web && cd kafka-doctor-web
./deploy.sh
```
Then open `http://<host>:8899/`.

Options (env vars):
- `KD_PORT=8899`            — UI/API port (default 8899)
- `KD_DEFAULT_BOOTSTRAP=127.0.0.1:9092` — prefilled broker in the UI
- `KD_DATA_DIR=$(pwd)/data` — where the write-audit log persists

Example, prefilling the local broker on port 9092:
```bash
KD_DEFAULT_BOOTSTRAP=127.0.0.1:9092 ./deploy.sh
```

## Using it
- **UI**: enter the broker `host:port`, click **connect** first (stage-by-stage:
  DNS → TCP → handshake → metadata → advertised-broker — the firewall/path
  validator). Then health, discover, trace, freshness, groups, lag, etc.
- **REST** (examples):
  ```
  curl "http://host:8899/api/connect?bootstrap=192.168.30.32:9092"
  curl "http://host:8899/api/health?bootstrap=192.168.30.32:9092"
  curl "http://host:8899/api/trace?topic=omnis.dns&bootstrap=192.168.30.32:9092"
  curl "http://host:8899/api/lag?group=clickhouse_dns_consumer&bootstrap=..."
  curl -XPOST http://host:8899/api/produce-test -H 'Content-Type: application/json' \
       -d '{"bootstrap":"192.168.30.32:9092","topic":"kafkadoctor_test"}'
  ```

## Topic name case matters
OMNIS topics are **lowercase with dots** (`omnis.dns`, `omnis.http-flow`).
Uppercase `OMNIS.dns` is often a different, empty topic. When a topic looks
empty, check the lowercase name too — `trace` on both is the fastest tell.

## Writes are audited
`produce-test` and `simulate` write real records (to `*-test` topics) and every
invocation is appended to `data/kafka-doctor-web.log`. View via the UI
("view write log") or `GET /api/log`.

## Endpoints
Read: `/api/connect /api/health /api/discover /api/auth /api/topic /api/trace
/api/freshness /api/config /api/size /api/consume /api/poison /api/groups
/api/lag /api/meta /api/log`
Write (POST): `/api/produce-test /api/simulate`

## Teardown
```bash
docker rm -f kafka-doctor-web
```

## Rebuild from source
```bash
docker build -t kafka-doctor-web:latest .
./deploy.sh
```

## Notes
- Baked `kafka_doctor.py` is a snapshot; to pick up a newer doctor, replace the
  file and rebuild.
- No auth by design (lab use). Do not expose the port to untrusted networks —
  the write endpoints produce to Kafka.
