# kafka-doctor-web

Portable, self-contained web + REST wrapper around **kafka_doctor.py**. Drop it on
any NETSCOUT AI Streamer host to validate the Kafka path **from that host** —
the same network position the streamer produces from — plus inspect topics,
consumer groups, run compression-codec round-trips, and audited produce/consume tests.

## Why run it on the streamer host
Kafka connectivity problems are usually path/firewall or advertised-listener
issues that only reproduce from the streamer's own source IP and egress route.
This container runs with `--network host`, so every test originates from the
host it runs on — making "connect: all stages OK" a valid proof that the
streamer's path is open.

## Requirements
Docker on the target host. No internet, Python, or pip needed on the host — it's
all in the image (built once, then portable).

---

## Deploy from GitHub (recommended)

On the target host (e.g. an AI Streamer). Needs internet the first time to build
the image; after that it runs offline.

```bash
cd ~
git clone https://github.com/dmaffei/kafka-doctor-web.git
cd kafka-doctor-web
docker build -t kafka-doctor-web:latest .

docker run -d --name kafka-doctor-web --restart unless-stopped \
  --network host \
  -e KD_PORT=8899 \
  -e KD_DEFAULT_BOOTSTRAP=192.168.30.32:9092 \
  -v "$(pwd)/data:/data" \
  kafka-doctor-web:latest
```

Set `KD_DEFAULT_BOOTSTRAP` to the broker you test most (it only prefills the UI
field; you can point any test at any broker at runtime).

### Open the firewall (required for off-host / browser access)
The container listens on `0.0.0.0:8899`, but a host firewall (firewalld) will
block access from your laptop until the port is opened. On the host:

```bash
sudo firewall-cmd --zone=public --add-port=8899/tcp --permanent
sudo firewall-cmd --reload
sudo firewall-cmd --zone=public --list-ports   # confirm 8899/tcp
```
(Runtime-only test without persisting: `sudo firewall-cmd --zone=public --add-port=8899/tcp`
— do NOT run `--reload` before a `--permanent` add, or the runtime rule is wiped.)

Then open `http://<host>:8899/`.

---

## Deploy offline (air-gapped host)

Build on a connected host, carry the image over:

```bash
# on a host WITH internet:
git clone https://github.com/dmaffei/kafka-doctor-web.git && cd kafka-doctor-web
docker build -t kafka-doctor-web:latest .
docker save kafka-doctor-web:latest | gzip > kafka-doctor-web-image.tar.gz
#   copy kafka-doctor-web-image.tar.gz to the target host, then:

# on the target host (offline):
gunzip -c kafka-doctor-web-image.tar.gz | docker load
docker run -d --name kafka-doctor-web --restart unless-stopped --network host \
  -e KD_PORT=8899 -e KD_DEFAULT_BOOTSTRAP=192.168.30.32:9092 \
  -v "$(pwd)/data:/data" kafka-doctor-web:latest
```
`deploy.sh` automates the offline path: if `kafka-doctor-web-image.tar.gz` is
present it loads it, otherwise it builds from source, then runs the container.

---

## Update to the latest version

```bash
cd ~/kafka-doctor-web
git pull
docker build -t kafka-doctor-web:latest .
docker rm -f kafka-doctor-web
docker run -d --name kafka-doctor-web --restart unless-stopped --network host \
  -e KD_PORT=8899 -e KD_DEFAULT_BOOTSTRAP=192.168.30.32:9092 \
  -v "$(pwd)/data:/data" kafka-doctor-web:latest
```
`index.html` and `kafka_doctor.py` are baked into the image (not mounted), so
changes require a rebuild — a plain restart won't pick them up.

---

## Using it
- **UI** (`http://<host>:8899/`): enter the broker `host:port`, click **connect**
  first — the stage-by-stage path validator (DNS → TCP → handshake → metadata →
  advertised-broker). Then health, discover, trace, freshness, groups, lag, etc.
- **Topic names are case-sensitive**: OMNIS topics are lowercase with dots
  (`omnis.dns`, not `OMNIS.dns`). When a topic looks empty, check the lowercase
  name — `trace` on both is the fastest tell.
- **Write tests** (audited to `data/kafka-doctor-web.log`):
  - **produce-test** — one record to a test topic.
  - **codec** — compression round-trip for a single selected codec
    (`none`, `gzip`, `lz4`, `snappy`, `zstd`), produced to a throwaway topic.
  - **simulate** — N AI-Streamer-format records to a `*-test` topic.

### REST examples
```
curl "http://host:8899/api/connect?bootstrap=192.168.30.32:9092"
curl "http://host:8899/api/health?bootstrap=192.168.30.32:9092"
curl "http://host:8899/api/trace?topic=omnis.dns&bootstrap=192.168.30.32:9092"
curl "http://host:8899/api/lag?group=clickhouse_dns_consumer&bootstrap=..."
curl -XPOST http://host:8899/api/produce-test -H 'Content-Type: application/json' \
     -d '{"bootstrap":"192.168.30.32:9092","topic":"kafkadoctor_test"}'
curl -XPOST http://host:8899/api/codec -H 'Content-Type: application/json' \
     -d '{"bootstrap":"192.168.30.32:9092","codecs":"zstd"}'
```

### Endpoints
Read (GET): `/api/connect /api/health /api/discover /api/auth /api/topic
/api/trace /api/freshness /api/config /api/size /api/consume /api/poison
/api/groups /api/lag /api/meta /api/log`
Write (POST, audited): `/api/produce-test /api/simulate /api/codec`

---

## Options (env vars)
- `KD_PORT=8899`                        — UI/API port
- `KD_DEFAULT_BOOTSTRAP=127.0.0.1:9092` — prefilled broker in the UI
- `KD_DATA_DIR=$(pwd)/data`             — where the write-audit log persists (deploy.sh only)

## Teardown
```bash
docker rm -f kafka-doctor-web
```

## Notes
- `--network host` is required so tests originate from this host's real network
  position. Do not switch to bridged.
- Baked `kafka_doctor.py` is a snapshot; to pick up a newer doctor, replace the
  file and rebuild.
- Compression codecs (`lz4`, `python-snappy`, `zstandard`) are installed in the
  image so the codec round-trip covers gzip/lz4/snappy/zstd/none.
- No auth by design (lab use). The write endpoints produce to Kafka, so don't
  expose the port to untrusted networks.
