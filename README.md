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

To enable the relay's optional **pcap** capture-on-error (real packets for
Wireshark), add `--cap-add=NET_RAW` to the `docker run` command. Frames-mode
capture works without it.

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
- **Diagnostics for "no data in topic":**
  - **check case** (`/api/topiccheck?topic=...`) — finds same-name/different-case
    topics and shows which case actually holds the data (the omnis.dns vs
    OMNIS.dns trap), flagging a case-mismatch with record counts.
  - **tcp probe** (`/api/connectprobe`) — classifies a raw TCP failure as
    *refused* (nothing listening) vs *timeout* (firewall DROP / broken return
    path), pointing straight at the fix.
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

### Kafka relay / proxy — watch the end-to-end exchange
Point a producer's `bootstrap.servers` at the proxy to see the live Kafka
conversation decoded (connection *and* message transfer). Handles the flexible
v9+/v12 protocol modern clients (e.g. kafka-clients 4.2) use; rewrites the
broker's advertised address to route produce/fetch through the proxy.

Start it in the UI (relay section) or via REST:
```
curl -XPOST http://host:8899/api/proxy/start -H 'Content-Type: application/json' \
     -d '{"upstream":"192.168.30.32:9092","listen_port":9099}'      # advertise_host optional (auto)
# point the streamer at  host:9099 ...
curl "http://host:8899/api/proxy/events?format=text"    # decoded feed (text)
curl "http://host:8899/api/proxy/stats"                 # per-topic produce req/ok/err/bytes/codec
curl "http://host:8899/api/proxy/logfile?tail=500"      # persisted feed (survives restarts)
curl -XPOST http://host:8899/api/proxy/stop
```

The feed decodes ApiVersions, Metadata (advertised brokers + topic list with
partition counts and topic-level errors), Produce/Fetch (topic, partition, acks,
**compression codec**, and response error code), plus per-message latency/size.
The "open log" button opens the feed in a new window with **all / handshake /
transfer** filters — so you can isolate whether an issue is in the handshake or
the message transfer. This distinguishes: never-connected, connected-but-stalls-
at-metadata, bad-advertised-address, produce-attempted-but-rejected (error code),
and never-produced (no Produce frames).

**Capture on error:** enable "capture on error" when starting the relay (UI
selector or `capture_on_error: true` / `pcap: true` in the start payload). When
the proxy detects a handshake/produce error — upstream connect failure, a
RAW/undecoded frame (protocol or TLS mismatch), or a non-NONE Produce response
(MESSAGE_TOO_LARGE, LEADER_NOT_AVAILABLE, UNKNOWN_TOPIC_OR_PARTITION, ...) — it
saves a capture automatically to `/data/captures/`:
- **frames** mode (no privilege): the recent decoded + hex Kafka frames around
  the error (the full ApiVersions/Metadata/Produce sequence). View in the
  browser or via REST.
- **frames + pcap** mode: also snapshots a rolling `tcpdump` of the relay's
  traffic to a `.pcap` for Wireshark. Requires running the container with
  `--cap-add=NET_RAW` (tcpdump is already in the image); without it, frames are
  still captured and the pcap step reports it can't get raw access.

List/download captures with the **captures** button, or:
```
curl "http://host:8899/api/proxy/captures"                       # list
curl "http://host:8899/api/proxy/capture?file=<name>.frames.txt" # view frames
curl -O "http://host:8899/api/proxy/capture?file=<name>.pcap"    # download pcap -> Wireshark
```

Scope: single upstream broker, plaintext. Record payloads are Avro (Schema
Registry) — the proxy decodes the envelope, not the Avro business fields.

### Endpoints
Read (GET): `/api/connect /api/health /api/discover /api/auth /api/topic
/api/trace /api/freshness /api/config /api/size /api/consume /api/poison
/api/groups /api/lag /api/topiccheck /api/connectprobe /api/meta /api/log`
Write (POST, audited): `/api/produce-test /api/simulate /api/codec`
Relay/proxy: `POST /api/proxy/start` `POST /api/proxy/stop` `GET /api/proxy/status`
`GET /api/proxy/events[?format=text&since=...]` `GET /api/proxy/stats`
`GET /api/proxy/logfile[?tail=N]` `GET /api/proxy/captures` `GET /api/proxy/capture?file=...`

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
