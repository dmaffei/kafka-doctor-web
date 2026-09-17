#!/usr/bin/env bash
# kafka-doctor-web — portable deploy for any AI Streamer host.
# Loads the bundled image (offline) if present, else builds, then runs with
# host networking so tests originate from THIS host's network position.
set -euo pipefail
cd "$(dirname "$0")"

IMG="kafka-doctor-web:latest"
PORT="${KD_PORT:-8899}"
BOOTSTRAP="${KD_DEFAULT_BOOTSTRAP:-127.0.0.1:9092}"
DATA_DIR="${KD_DATA_DIR:-$(pwd)/data}"

command -v docker >/dev/null 2>&1 || { echo "[!] docker not found"; exit 1; }
mkdir -p "$DATA_DIR"

# 1) obtain image
if [ -f kafka-doctor-web-image.tar.gz ]; then
  echo "[*] loading bundled image ..."
  gunzip -c kafka-doctor-web-image.tar.gz | docker load
elif ! docker image inspect "$IMG" >/dev/null 2>&1; then
  echo "[*] building image from source ..."
  docker build -t "$IMG" .
fi

# 2) (re)create container — HOST NETWORK is required for valid path testing
echo "[*] (re)starting kafka-doctor-web on port ${PORT} (host network) ..."
docker rm -f kafka-doctor-web >/dev/null 2>&1 || true
docker run -d --name kafka-doctor-web --restart unless-stopped \
  --network host \
  -e KD_PORT="${PORT}" \
  -e KD_DEFAULT_BOOTSTRAP="${BOOTSTRAP}" \
  -v "${DATA_DIR}:/data" \
  "$IMG" >/dev/null

sleep 2
docker ps --filter name=kafka-doctor-web --format '  {{.Names}}  {{.Status}}'
echo "[✓] UI:   http://<this-host>:${PORT}/"
echo "[✓] API:  http://<this-host>:${PORT}/api/connect?bootstrap=<host:port>"
echo "[✓] write log persists at: ${DATA_DIR}/kafka-doctor-web.log"
