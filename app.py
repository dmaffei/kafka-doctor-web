#!/usr/bin/env python3
"""
kafka-doctor-web — a thin FastAPI wrapper around kafka_doctor.py.

Design goals:
  * Portable: everything (doctor + deps) baked into one container image.
  * Runs on the AI Streamer host with --network host, so tests originate from
    the streamer's real network position (valid firewall/path validation).
  * Bootstrap (broker host:port) is a RUNTIME input, never baked.
  * Read endpoints are safe. Write endpoints (produce-test/simulate) are
    explicit, and every write is appended to a persistent audit log.

It shells out to:  python kafka_doctor.py <subcmd> --json --bootstrap <hp> [...]
and relays the output. This never modifies the doctor script.

Note on kafka_doctor --json: output is inconsistent per subcommand — some emit
human text followed by a trailing JSON object, others emit only human text.
So we key success on the process exit code (0 = success) and additionally try
to extract a trailing {...} JSON object if one is present, while always
returning the full human-readable text for the UI.
"""
import json, os, re, shlex, subprocess, time, datetime, pathlib
from typing import Optional
from fastapi import FastAPI, Query, Body
from fastapi.responses import HTMLResponse, PlainTextResponse

DOCTOR = os.environ.get("KD_DOCTOR", "/app/kafka_doctor.py")
PORT = int(os.environ.get("KD_PORT", "8899"))
DEFAULT_BOOTSTRAP = os.environ.get("KD_DEFAULT_BOOTSTRAP", "127.0.0.1:9092")
TIMEOUT = os.environ.get("KD_TIMEOUT", "8")
LOG_PATH = os.environ.get("KD_LOG", "/data/kafka-doctor-web.log")

READ_CMDS = {"discover", "health", "connect", "config", "lag", "freshness",
             "groups", "poison", "trace", "topic", "consume", "size", "codec", "auth"}
WRITE_CMDS = {"produce-test", "simulate"}

app = FastAPI(title="kafka-doctor-web", version="1.0")


def _audit(line: str):
    try:
        pathlib.Path(LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_PATH, "a") as f:
            f.write(f"{datetime.datetime.utcnow().isoformat()}Z  {line}\n")
    except Exception:
        pass  # logging must never break the request


def _extract_trailing_json(text: str):
    """kafka_doctor may print human text then a JSON object. Grab the last
    top-level {...} block if it parses; else None."""
    if not text:
        return None
    # try whole-string first
    try:
        return json.loads(text)
    except Exception:
        pass
    # find last balanced {...} by scanning from the last '{'
    idx = text.rfind("{")
    while idx != -1:
        chunk = text[idx:]
        try:
            return json.loads(chunk)
        except Exception:
            idx = text.rfind("{", 0, idx)
    return None


def run_doctor(subcmd: str, bootstrap: str, extra: Optional[list] = None,
               tls: bool = False, timeout: str = None):
    cmd = ["python3", DOCTOR, subcmd, "--bootstrap", bootstrap, "--json",
           "--timeout", timeout or TIMEOUT]
    if tls:
        cmd.append("--tls")
    if extra:
        cmd += extra
    started = time.time()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "doctor subprocess timed out (90s)",
                "subcmd": subcmd, "bootstrap": bootstrap,
                "cmd": " ".join(shlex.quote(c) for c in cmd)}
    dur = round(time.time() - started, 3)
    out = (proc.stdout or "").strip()
    parsed = _extract_trailing_json(out)
    result = {
        # success is the process exit code; kafka_doctor exits 0 on success
        "ok": proc.returncode == 0,
        "subcmd": subcmd, "bootstrap": bootstrap, "exit_code": proc.returncode,
        "duration_s": dur,
        "result": parsed,          # structured verdict if the subcommand emitted one
        "text": out or None,       # full human-readable report (always present)
        "stderr": (proc.stderr or "").strip() or None,
        "cmd": " ".join(shlex.quote(c) for c in cmd),
    }
    if subcmd in WRITE_CMDS:
        _audit(f"WRITE {subcmd} bootstrap={bootstrap} extra={extra} "
               f"exit={proc.returncode} dur={dur}s")
    return result


# ---------- REST: read endpoints ----------
def _bootstrap(q): return q or DEFAULT_BOOTSTRAP

@app.get("/api/health")
def api_health(bootstrap: str = Query(None), tls: bool = False):
    return run_doctor("health", _bootstrap(bootstrap), tls=tls)

@app.get("/api/connect")
def api_connect(bootstrap: str = Query(None), tls: bool = False):
    return run_doctor("connect", _bootstrap(bootstrap), tls=tls)

@app.get("/api/discover")
def api_discover(bootstrap: str = Query(None), tls: bool = False):
    return run_doctor("discover", _bootstrap(bootstrap), tls=tls)

@app.get("/api/groups")
def api_groups(bootstrap: str = Query(None), tls: bool = False):
    return run_doctor("groups", _bootstrap(bootstrap), tls=tls)

@app.get("/api/auth")
def api_auth(bootstrap: str = Query(None), tls: bool = False):
    return run_doctor("auth", _bootstrap(bootstrap), tls=tls)

@app.get("/api/topic")
def api_topic(topic: str, bootstrap: str = Query(None), tls: bool = False):
    return run_doctor("topic", _bootstrap(bootstrap), ["--topic", topic], tls=tls)

@app.get("/api/trace")
def api_trace(topic: str, bootstrap: str = Query(None), tls: bool = False):
    return run_doctor("trace", _bootstrap(bootstrap), ["--topic", topic], tls=tls)

@app.get("/api/freshness")
def api_freshness(topic: str, bootstrap: str = Query(None), tls: bool = False):
    return run_doctor("freshness", _bootstrap(bootstrap), ["--topic", topic], tls=tls)

@app.get("/api/config")
def api_config(topic: str = Query(None), bootstrap: str = Query(None), tls: bool = False):
    extra = ["--topic", topic] if topic else None
    return run_doctor("config", _bootstrap(bootstrap), extra, tls=tls)

@app.get("/api/lag")
def api_lag(group: str, bootstrap: str = Query(None), tls: bool = False):
    return run_doctor("lag", _bootstrap(bootstrap), ["--group", group], tls=tls)

@app.get("/api/consume")
def api_consume(topic: str, max: int = 5, bootstrap: str = Query(None), tls: bool = False):
    return run_doctor("consume", _bootstrap(bootstrap),
                      ["--topic", topic, "--max", str(max)], tls=tls)

@app.get("/api/size")
def api_size(topic: str, bootstrap: str = Query(None), tls: bool = False):
    return run_doctor("size", _bootstrap(bootstrap), ["--topic", topic], tls=tls)

@app.get("/api/poison")
def api_poison(topic: str, max: int = 200, bootstrap: str = Query(None), tls: bool = False):
    return run_doctor("poison", _bootstrap(bootstrap),
                      ["--topic", topic, "--max", str(max)], tls=tls)


# ---------- REST: write endpoints (audited) ----------
@app.post("/api/produce-test")
def api_produce_test(payload: dict = Body(default={})):
    bootstrap = _bootstrap(payload.get("bootstrap"))
    topic = payload.get("topic", "kafkadoctor_test")
    tls = bool(payload.get("tls", False))
    return run_doctor("produce-test", bootstrap, ["--topic", topic], tls=tls)

@app.post("/api/simulate")
def api_simulate(payload: dict = Body(default={})):
    bootstrap = _bootstrap(payload.get("bootstrap"))
    topic = payload.get("topic", "OMNIS.assets-test")
    count = str(int(payload.get("count", 50)))
    rate = str(int(payload.get("rate", 0)))
    tls = bool(payload.get("tls", False))
    return run_doctor("simulate", bootstrap,
                      ["--topic", topic, "--count", count, "--rate", rate], tls=tls)


@app.get("/api/log", response_class=PlainTextResponse)
def api_log(tail: int = 200):
    try:
        with open(LOG_PATH) as f:
            lines = f.readlines()
        return "".join(lines[-tail:])
    except FileNotFoundError:
        return "(no writes logged yet)"

@app.get("/api/meta")
def api_meta():
    return {"default_bootstrap": DEFAULT_BOOTSTRAP, "doctor": DOCTOR,
            "read_cmds": sorted(READ_CMDS), "write_cmds": sorted(WRITE_CMDS),
            "port": PORT, "log": LOG_PATH}


@app.get("/", response_class=HTMLResponse)
def index():
    return INDEX_HTML.replace("__DEFAULT_BOOTSTRAP__", DEFAULT_BOOTSTRAP)


INDEX_HTML = open(os.path.join(os.path.dirname(__file__), "index.html")).read() \
    if os.path.exists(os.path.join(os.path.dirname(__file__), "index.html")) else "<h1>kafka-doctor-web</h1><p>index.html missing</p>"


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
