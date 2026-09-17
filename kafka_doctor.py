#!/usr/bin/env python3
"""
kafka_doctor.py  —  Kafka broker discovery & connectivity troubleshooter
========================================================================

A self-contained, dependency-free (stdlib-only) tool that speaks the Kafka
wire protocol directly to answer, for any bootstrap endpoint:

  * Is it reachable? (TCP + Kafka handshake)
  * Single host or cluster? How many brokers? Their ids/hosts/ports/racks.
  * Cluster id, controller id.
  * Security: PLAINTEXT vs TLS vs SASL (best-effort probe).
  * Data format hint (schema-registry presence is out of band; we report the
    produce/consume path and record-batch magic when a test message is used).
  * Fault tolerance: replication factor & ISR per partition for a topic;
    under-replicated / offline partition detection.
  * Bootstrap list vs discovered server list (and mismatches / advertised-
    listener traps — e.g. broker advertises an unreachable internal IP).
  * Connection behaviour knobs (timeouts, client id) are configurable.

OPTIONAL (only if confluent-kafka or kafka-python is installed):
  * Produce a test message to a throwaway topic and verify it landed
    (offset check) — never writes to your real topics unless you force it.

SAFETY
------
Discovery/health commands are strictly READ-ONLY (Metadata/ApiVersions only).
The 'produce-test' command writes ONLY to the topic you name (default
'__kafka_doctor_test'); it refuses to write to topics matching --protect
(default: OMNIS.*) unless --force is given.

USAGE
-----
  python3 kafka_doctor.py discover  --bootstrap host:9092[,host2:9092]
  python3 kafka_doctor.py health    --bootstrap host:9092
  python3 kafka_doctor.py topic     --bootstrap host:9092 --topic OMNIS.dns
  python3 kafka_doctor.py connect   --bootstrap host:9092   # deep connectivity doctor
  python3 kafka_doctor.py produce-test --bootstrap host:9092 [--topic __kafka_doctor_test]

Common options:
  --timeout 5           socket timeout seconds (connection behaviour)
  --client-id kdoctor   Kafka client id sent in requests
  --json                machine-readable output
  --tls                 wrap socket in TLS (for SSL/SASL_SSL listeners)
"""

import argparse, socket, struct, sys, json, time, ssl as _ssl
import logging, os, re as _re

# ---------------------------------------------------------------------------
# Minimal Kafka wire protocol (request/response) — enough for ApiVersions and
# Metadata, which is all discovery needs. Uses the older, non-compact
# (pre-flexible) encodings with fixed api_versions for maximum broker compat.
# ---------------------------------------------------------------------------

API_VERSIONS = 18
METADATA = 3

# ---------------------------------------------------------------------------
# Phase 0: logging, credential redaction, severity model, exit status.
# Default behaviour is UNCHANGED (findings to stdout). Logging is opt-in via
# -v/-vv and --log-file. Redaction is ALWAYS on.
# ---------------------------------------------------------------------------
log = logging.getLogger("kafka_doctor")
_SECRET_KEYS = ("sasl_pass","password","passwd","token","secret",
                "keystore_password","truststore_password","key_password")
_REDACT_PATTERNS = [
    _re.compile(r"(--sasl-pass(?:word)?[= ])(\S+)"),
    _re.compile(r"(password[=:] ?)(\S+)", _re.IGNORECASE),
    _re.compile(r"(token[=:] ?)(\S+)", _re.IGNORECASE),
]
def _redact_text(msg):
    if not isinstance(msg, str): return msg
    for pat in _REDACT_PATTERNS:
        msg = pat.sub(lambda m: m.group(1) + "***REDACTED***", msg)
    return msg
class _RedactionFilter(logging.Filter):
    def filter(self, record):
        try:
            record.msg = _redact_text(record.getMessage()); record.args = ()
        except Exception: pass
        return True
_WORST = {"level": 0}
_SEV_RANK = {"OK":0,"INFO":0,"WARN":1,"FAIL":2}
def note_severity(sev): _WORST["level"] = max(_WORST["level"], _SEV_RANK.get(sev,0))
def exit_code(): return _WORST["level"]
def sev_tag(sev): return {"OK":"[ OK ]","WARN":"[WARN]","FAIL":"[FAIL]","INFO":"[INFO]"}.get(sev,"[    ]")
def emit(sev, key, text):
    note_severity(sev); log.info("%s %s: %s", sev_tag(sev), key, text); return (sev,key,text)
def redact_args_dict(ns):
    out = {}
    for k,v in vars(ns).items():
        out[k] = "***REDACTED***" if (any(s in k.lower() for s in _SECRET_KEYS) and v) else v
    return out
def _resolve_log_file(a):
    """Decide the log-file path. Default (log_file unset) => ./kafka_doctor-<UTCts>.log.
       log_file == "none" (any case) disables the file. An explicit path is used as-is."""
    lf = getattr(a, "log_file", None)
    if isinstance(lf, str) and lf.strip().lower() == "none":
        return None
    if lf:
        return lf
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return os.path.join(os.getcwd(), f"kafka_doctor-{ts}.log")

def init_logging(a):
    verbosity = getattr(a,"verbose",0) or 0
    logfile = _resolve_log_file(a)
    log.setLevel(logging.DEBUG); log.handlers.clear()
    redactor = _RedactionFilter()
    if verbosity >= 1:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(logging.DEBUG if verbosity>=2 else logging.INFO)
        ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s","%H:%M:%S"))
        ch.addFilter(redactor); log.addHandler(ch)
    if logfile:
        try:
            fh = logging.FileHandler(logfile); fh.setLevel(logging.DEBUG)
            fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s","%Y-%m-%dT%H:%M:%S"))
            fh.addFilter(redactor); log.addHandler(fh)
        except Exception as e:
            sys.stderr.write(f"  (could not open log file {logfile}: {e})\n")
    if not log.handlers: log.addHandler(logging.NullHandler())
    log.info("=== kafka_doctor run start %s ===", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    if logfile:
        log.info("advanced run log: %s", logfile)
    log.info("args: %s", json.dumps(redact_args_dict(a)))


# ---------------------------------------------------------------------------
# Phase 0b: optional JSON config file. Precedence: CLI flag > config file >
# built-in default. Auto-loads ~/.kafka_doctor.json if present and no --config
# given. Secrets in the file are honoured but never logged (redaction applies).
# ---------------------------------------------------------------------------
_CONFIG_DEFAULTS = ("bootstrap", "tls", "timeout", "client_id", "verbose",
                    "log_file", "protect")

def _script_dir_config():
    """Path to kafka_doctor.json next to this script, or None."""
    try:
        d = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        d = os.getcwd()
    p = os.path.join(d, "kafka_doctor.json")
    return p if os.path.isfile(p) else None

def _auto_config_path():
    """Search order when no --config given: script-dir file, then ~/.kafka_doctor.json."""
    return _script_dir_config() or (
        os.path.expanduser("~/.kafka_doctor.json")
        if os.path.isfile(os.path.expanduser("~/.kafka_doctor.json")) else None)

def load_config_file(path):
    """Return a dict of settings from a JSON config file, or {} if none/invalid."""
    if not path:
        path = _auto_config_path()
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path) as f:
            cfg = json.load(f)
        log.info("loaded config file: %s (keys: %s)", path, ",".join(sorted(cfg)))
        return cfg if isinstance(cfg, dict) else {}
    except Exception as e:
        log.info("config file %s ignored: %s", path, e)
        return {}

def apply_config_file(a):
    """Merge config-file values into namespace `a` WITHOUT overriding values the
       user set explicitly on the CLI. argparse defaults are treated as 'unset'
       for the keys we manage."""
    cfg = load_config_file(getattr(a, "config", None))
    if not cfg:
        return a
    builtin = {"bootstrap": None, "tls": False, "timeout": 5.0,
               "client_id": "kafka-doctor", "verbose": 0, "log_file": None,
               "protect": r"OMNIS\."}
    for k in _CONFIG_DEFAULTS:
        if k not in cfg:
            continue
        cur = getattr(a, k, None)
        # only take file value if the current value is still the built-in default
        if cur == builtin.get(k):
            setattr(a, k, cfg[k])
    return a

def _enc_str(s):
    if s is None:
        return struct.pack('>h', -1)
    b = s.encode('utf-8')
    return struct.pack('>h', len(b)) + b

def _req_header(api_key, api_version, corr_id, client_id):
    # Request header v1: api_key(2) api_version(2) corr_id(4) client_id(STRING)
    return struct.pack('>hhi', api_key, api_version, corr_id) + _enc_str(client_id)

def _send_request(sock, payload):
    sock.sendall(struct.pack('>i', len(payload)) + payload)

def _recv_response(sock):
    raw_len = _recv_n(sock, 4)
    (n,) = struct.unpack('>i', raw_len)
    return _recv_n(sock, n)

def _recv_n(sock, n):
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("connection closed by peer mid-response")
        buf += chunk
    return buf

class Reader:
    def __init__(self, b): self.b, self.i = b, 0
    def i8(self):  v = self.b[self.i]; self.i += 1; return v
    def i16(self): v, = struct.unpack_from('>h', self.b, self.i); self.i += 2; return v
    def i32(self): v, = struct.unpack_from('>i', self.b, self.i); self.i += 4; return v
    def i64(self): v, = struct.unpack_from('>q', self.b, self.i); self.i += 8; return v
    def string(self):
        ln = self.i16()
        if ln < 0: return None
        s = self.b[self.i:self.i+ln].decode('utf-8', 'replace'); self.i += ln; return s
    def nullable_string(self): return self.string()
    def remaining(self): return len(self.b) - self.i
    def uvarint(self):
        r=0; s=0
        while True:
            x=self.b[self.i]; self.i+=1; r|=(x&0x7f)<<s
            if not (x&0x80): break
            s+=7
        return r
    def compact_string(self):
        ln=self.uvarint()
        if ln==0: return None
        ln-=1; sv=self.b[self.i:self.i+ln].decode('utf-8','replace'); self.i+=ln; return sv
    def compact_array_len(self):
        n=self.uvarint(); return 0 if n==0 else n-1

def _enc_uvarint(n):
    out=b''
    while True:
        x=n&0x7f; n>>=7
        out += bytes([x|0x80]) if n else bytes([x])
        if not n: break
    return out

def _enc_cstr(s):
    if s is None: return _enc_uvarint(0)
    bs=s.encode('utf-8'); return _enc_uvarint(len(bs)+1)+bs


def open_socket(host, port, timeout, use_tls):
    s = socket.create_connection((host, port), timeout=timeout)
    s.settimeout(timeout)
    if use_tls:
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        s = ctx.wrap_socket(s, server_hostname=host)
    return s


def api_versions(host, port, timeout, client_id, use_tls):
    """ApiVersions v0 — the first thing any client sends. Also our TLS/plaintext probe."""
    s = open_socket(host, port, timeout, use_tls)
    try:
        payload = _req_header(API_VERSIONS, 0, 1, client_id)
        _send_request(s, payload)
        resp = _recv_response(s)
        r = Reader(resp)
        r.i32()  # correlation id
        err = r.i16()
        count = r.i32()
        apis = []
        for _ in range(count):
            k = r.i16(); mn = r.i16(); mx = r.i16()
            apis.append((k, mn, mx))
        return {"error_code": err, "api_count": count,
                "supports_metadata_v12": any(k == METADATA and mx >= 12 for k, _, mx in apis),
                "max_metadata_version": max((mx for k,_,mx in apis if k==METADATA), default=0)}
    finally:
        s.close()


def metadata(host, port, timeout, client_id, use_tls, topic=None, mver=1):
    """Metadata request. v1 gives brokers(+rack), controller_id, cluster is v2+.
       We use v1 (broadly supported) and, if the broker supports it, v2 for cluster_id."""
    use_v2 = mver >= 2
    s = open_socket(host, port, timeout, use_tls)
    try:
        hdr = _req_header(METADATA, 2 if use_v2 else 1, 7, client_id)
        # body: topics array. -1 (null) => all topics (v1); [] => none.
        if topic:
            body = struct.pack('>i', 1) + _enc_str(topic)
        else:
            body = struct.pack('>i', -1)  # all topics
        _send_request(s, hdr + body)
        resp = _recv_response(s)
        r = Reader(resp)
        r.i32()  # corr id
        brokers = []
        bn = r.i32()
        for _ in range(bn):
            nid = r.i32(); h = r.string(); p = r.i32()
            rack = r.string() if use_v2 else None
            brokers.append({"node_id": nid, "host": h, "port": p, "rack": rack})
        cluster_id = r.nullable_string() if use_v2 else None
        controller_id = r.i32()
        topics = []
        tn = r.i32()
        for _ in range(tn):
            terr = r.i16(); tname = r.string()
            if use_v2:
                _internal = r.i8()
            parts = []
            pn = r.i32()
            for _ in range(pn):
                perr = r.i16(); pid = r.i32(); leader = r.i32()
                repn = r.i32(); replicas = [r.i32() for _ in range(repn)]
                isrn = r.i32(); isr = [r.i32() for _ in range(isrn)]
                parts.append({"partition": pid, "leader": leader,
                              "replicas": replicas, "isr": isr, "error": perr})
            topics.append({"topic": tname, "error": terr, "partitions": parts})
        return {"brokers": brokers, "cluster_id": cluster_id,
                "controller_id": controller_id, "topics": topics}
    finally:
        s.close()


LIST_OFFSETS = 2
FETCH = 1

def list_offsets(host, port, timeout, client_id, use_tls, topic, partition, which):
    """ListOffsets v1. which=-2 => earliest, -1 => latest. Returns offset (int)."""
    s = open_socket(host, port, timeout, use_tls)
    try:
        hdr = _req_header(LIST_OFFSETS, 1, 11, client_id)
        # replica_id(-1), topics array
        body = struct.pack('>i', -1)
        body += struct.pack('>i', 1) + _enc_str(topic)          # 1 topic
        body += struct.pack('>i', 1) + struct.pack('>i', partition) + struct.pack('>q', which)
        _send_request(s, hdr + body)
        r = Reader(_recv_response(s))
        r.i32()  # corr
        r.i32()  # topics count
        r.string()  # topic name
        r.i32()  # partitions count
        r.i32()  # partition id
        err = r.i16()
        r.i64()  # timestamp
        off = r.i64()
        return off if err == 0 else None
    finally:
        s.close()


def _parse_records(rd, n, base_offset, out):
    """Parse n uncompressed v2 records from Reader rd into out."""
    for _ in range(n):
        _rlen = _read_varint(rd)
        rd.i8()  # record attributes
        _td = _read_varint(rd)     # timestamp delta
        od = _read_varint(rd)      # offset delta
        klen = _read_varint(rd)
        key = None if klen < 0 else _read_bytes(rd, klen)
        vlen = _read_varint(rd)
        val = None if vlen < 0 else _read_bytes(rd, vlen)
        hn = _read_varint(rd)      # header count
        for _h in range(hn):
            hk = _read_varint(rd); _read_bytes(rd, hk)
            hv = _read_varint(rd)
            if hv >= 0: _read_bytes(rd, hv)
        out.append((base_offset + od, key, val))

def _decode_record_batches(buf):
    """Decode one or more v2 record batches from raw bytes. Returns list of
       (offset, key, value). Transparently decompresses gzip/snappy/lz4/zstd
       batches. Propagates _MissingCodecLib when a codec's Python lib is absent."""
    out = []
    r = Reader(buf)
    while r.remaining() >= 61:  # min batch header
        base_offset = r.i64()
        batch_len = r.i32()
        if batch_len <= 0 or batch_len > r.remaining() + 4:
            break
        end = r.i + batch_len  # first byte past this batch (batch_len counts from here)
        try:
            r.i32()  # partition leader epoch
            magic = r.i8()
            if magic != 2:
                break  # only v2 supported
            r.i32()  # crc
            attributes = r.i16()
            r.i32()  # last offset delta
            r.i64()  # first timestamp
            r.i64()  # max timestamp
            r.i64()  # producer id
            r.i16()  # producer epoch
            r.i32()  # base sequence
            n = r.i32()  # record count
            codec = _CODEC_BY_ATTR.get(attributes & 0x07, "none")
            records_blob = r.b[r.i:end]   # everything after the header, to end of batch
            if codec == "none":
                _parse_records(Reader(records_blob), n, base_offset, out)
            else:
                # records section is compressed as a single unit; decompress then parse.
                # _MissingCodecLib is intentionally NOT swallowed here.
                plain = _codec_decompress(codec, records_blob)
                _parse_records(Reader(plain), n, base_offset, out)
        except _MissingCodecLib:
            raise
        except Exception:
            # malformed / partial batch: stop cleanly with whatever we have
            r.i = end
            break
        r.i = end  # advance to next batch regardless of inner parsing
    return out


def _read_varint(r):
    """Zig-zag varint (Kafka record encoding)."""
    raw = _read_uvarint(r)
    return (raw >> 1) ^ -(raw & 1)

def _read_uvarint(r):
    result = 0; shift = 0
    while True:
        b = r.i8()
        result |= (b & 0x7f) << shift
        if not (b & 0x80): break
        shift += 7
    return result

def _read_bytes(r, n):
    b = r.b[r.i:r.i+n]; r.i += n; return b


def fetch(host, port, timeout, client_id, use_tls, topic, partition, start_offset, max_bytes=1048576):
    """Fetch v4 from start_offset. Returns list of (offset, key, value)."""
    s = open_socket(host, port, timeout, use_tls)
    try:
        hdr = _req_header(FETCH, 4, 22, client_id)
        body = struct.pack('>i', -1)          # replica_id
        body += struct.pack('>i', int(timeout*1000))  # max_wait_ms
        body += struct.pack('>i', 1)          # min_bytes
        body += struct.pack('>i', max_bytes)  # max_bytes (v3+)
        body += struct.pack('>b', 0)          # isolation_level (v4+): read_uncommitted
        body += struct.pack('>i', 1)          # topics count
        body += _enc_str(topic)
        body += struct.pack('>i', 1)          # partitions count
        body += struct.pack('>i', partition)
        body += struct.pack('>q', start_offset)
        body += struct.pack('>i', max_bytes)  # partition max_bytes
        _send_request(s, hdr + body)
        r = Reader(_recv_response(s))
        r.i32()  # corr
        r.i32()  # throttle_time_ms (v1+)
        r.i32()  # topics count
        r.string()  # topic
        r.i32()  # partitions count
        r.i32()  # partition id
        err = r.i16()
        r.i64()  # high_watermark
        r.i64()  # last_stable_offset (v4+)
        abort_cnt = r.i32()  # aborted_transactions array (v4+); -1 = null/none
        if abort_cnt > 0:
            for _ in range(abort_cnt):
                r.i64()  # producer_id
                r.i64()  # first_offset
        rs_len = r.i32()  # record_set bytes length
        if err != 0 or rs_len <= 0:
            return []
        raw = r.b[r.i:r.i+rs_len]
        return _decode_record_batches(raw)
    finally:
        s.close()


DESCRIBE_CONFIGS = 32
RESOURCE_BROKER = 4
RESOURCE_TOPIC = 2

def describe_configs(host, port, timeout, client_id, use_tls, resource_type, resource_name,
                     keys=None, api_ver=4):
    """DescribeConfigs v4 (flexible). resource_type: 4=broker, 2=topic.
       keys=None -> all configs. Returns (error_code, error_msg, {name:value})."""
    s = open_socket(host, port, timeout, use_tls)
    try:
        cid = (client_id or "").encode('utf-8')
        hdr = struct.pack('>hhi', DESCRIBE_CONFIGS, api_ver, 101)
        hdr += struct.pack('>h', len(cid)) + cid + _enc_uvarint(0)
        body = _enc_uvarint(1 + 1)
        body += struct.pack('>b', resource_type)
        body += _enc_cstr(resource_name)
        if keys is None:
            body += _enc_uvarint(0)
        else:
            body += _enc_uvarint(len(keys) + 1)
            for k in keys:
                body += _enc_cstr(k)
        body += _enc_uvarint(0)
        body += struct.pack('>b', 0)
        body += struct.pack('>b', 0)
        body += _enc_uvarint(0)
        _send_request(s, hdr + body)
        r = Reader(_recv_response(s))
        r.i32()
        r.uvarint()
        r.i32()
        nres = r.compact_array_len()
        out = {}
        for _ in range(nres):
            errc = r.i16()
            errm = r.compact_string()
            rtype = r.i8()
            rname = r.compact_string()
            ncfg = r.compact_array_len()
            cfgs = {}
            for _ in range(ncfg):
                name = r.compact_string()
                val = r.compact_string()
                r.i8()
                r.i8()
                r.i8()
                nsyn = r.compact_array_len()
                for _ in range(nsyn):
                    r.compact_string(); r.compact_string(); r.i8(); r.uvarint()
                r.i8()
                r.compact_string()
                r.uvarint()
                cfgs[name] = val
            r.uvarint()
            out = (errc, errm, cfgs)
        return out if out else (None, None, {})
    finally:
        s.close()


def analyze_configs(broker_cfg, topic_cfg, topic_name):
    """Produce-relevant findings from broker+topic configs. Returns list of (sev,key,text)."""
    findings = []
    def g(d, k): return (d or {}).get(k)

    ct_b = g(broker_cfg, "compression.type")
    ct_t = g(topic_cfg, "compression.type")
    eff_ct = ct_t if ct_t not in (None, "producer") else ct_b
    if eff_ct == "producer" or eff_ct is None:
        findings.append(emit("OK", "compression.type",
            f"broker={ct_b} topic={ct_t} -> pass-through (broker stores producer codec as-is)"))
    else:
        findings.append(emit("WARN", "compression.type",
            f"effective={eff_ct} -> broker RECOMPRESSES on write; native codec lib MANDATORY. "
            f"If that lib is missing/unloadable, produce fails and data does not land."))

    tt = g(broker_cfg, "log.message.timestamp.type")
    if tt == "LogAppendTime":
        findings.append(emit("WARN", "timestamp.type",
            "LogAppendTime -> broker rewrites timestamps -> DECOMPRESSES even in pass-through; "
            "native codec lib required for compressed batches."))
    elif tt:
        findings.append(emit("OK", "timestamp.type", f"{tt} (no forced decompress in pass-through)"))

    mmb_b = g(broker_cfg, "message.max.bytes")
    mmb_t = g(topic_cfg, "max.message.bytes")
    rfmb  = g(broker_cfg, "replica.fetch.max.bytes")
    try:
        mmb_b_i = int(mmb_b) if mmb_b is not None else None
        mmb_t_i = int(mmb_t) if mmb_t is not None else None
        rfmb_i  = int(rfmb)  if rfmb  is not None else None
    except ValueError:
        mmb_b_i = mmb_t_i = rfmb_i = None

    if mmb_t_i is not None:
        findings.append(emit("OK", "max.message.bytes",
            f"topic '{topic_name}' max.message.bytes={mmb_t_i} "
            f"(broker message.max.bytes={mmb_b_i}) -> batches above this rejected (MESSAGE_TOO_LARGE)"))
    if mmb_b_i and mmb_t_i and mmb_t_i < mmb_b_i:
        findings.append(emit("WARN", "size_mismatch",
            f"topic limit ({mmb_t_i}) < broker limit ({mmb_b_i}); topic value wins -> large batches fail on this topic only"))
    if mmb_b_i and rfmb_i and rfmb_i < mmb_b_i:
        # The default pairing is message.max=1048588 / replica.fetch=1048576 (a ~12B header
        # allowance) and is HARMLESS. Only flag a materially smaller replica.fetch, which
        # would actually block replication of large messages.
        gap = mmb_b_i - rfmb_i
        if gap > 1024:
            findings.append(emit("FAIL", "replica_fetch",
                f"replica.fetch.max.bytes ({rfmb_i}) is {gap} bytes < message.max.bytes ({mmb_b_i}) -> "
                f"messages larger than replica.fetch can be produced but NOT replicated (stuck/under-replicated)."))
        else:
            findings.append(emit("OK", "replica_fetch",
                f"replica.fetch.max.bytes ({rfmb_i}) vs message.max.bytes ({mmb_b_i}) -> default {gap}B header allowance, harmless"))
    return findings


# ---------------------------------------------------------------------------
# Diagnostics / guidance layer
# ---------------------------------------------------------------------------

def probe_security(host, port, timeout, client_id):
    """Best-effort: does PLAINTEXT ApiVersions work? does TLS handshake work?"""
    result = {"plaintext": None, "tls": None, "verdict": None}
    try:
        api_versions(host, port, timeout, client_id, use_tls=False)
        result["plaintext"] = "ok"
    except Exception as e:
        result["plaintext"] = f"failed: {type(e).__name__}"
    try:
        api_versions(host, port, timeout, client_id, use_tls=True)
        result["tls"] = "ok"
    except Exception as e:
        result["tls"] = f"failed: {type(e).__name__}"
    if result["plaintext"] == "ok":
        result["verdict"] = "PLAINTEXT listener (no TLS) — data on wire is unencrypted"
    elif result["tls"] == "ok":
        result["verdict"] = "TLS listener (SSL/SASL_SSL) — plaintext rejected"
    else:
        result["verdict"] = "neither plaintext nor TLS handshake succeeded — check port/SASL"
    return result


def analyze(md, bootstrap_list):
    """Turn raw metadata into findings + guidance."""
    findings = []
    brokers = md["brokers"]
    n = len(brokers)
    single = n == 1
    findings.append(("cluster_size", f"{n} broker(s) — {'SINGLE HOST' if single else 'CLUSTER'}"))
    findings.append(("cluster_id", md.get("cluster_id") or "(not reported; broker metadata < v2)"))
    findings.append(("controller", f"node {md['controller_id']}"))

    # advertised-listener trap: does any broker advertise an addr not in bootstrap
    # and possibly unreachable (e.g. an internal docker IP)?
    boot_hosts = {b.split(':')[0] for b in bootstrap_list}
    for b in brokers:
        adv = b["host"]
        if adv not in boot_hosts:
            findings.append(("advertised_listener",
                f"broker {b['node_id']} advertises {adv}:{b['port']} "
                f"(not in bootstrap {sorted(boot_hosts)}). "
                f"Clients will try to reach {adv} directly — if that address is not "
                f"routable from the client (e.g. a docker-internal IP), produce/consume "
                f"will hang after Metadata. This is the #1 'connects then stalls' cause."))

    # fault tolerance from any topic present
    rf_seen = set()
    urp = []  # under-replicated
    offline = []
    for t in md["topics"]:
        for p in t["partitions"]:
            rf_seen.add(len(p["replicas"]))
            if len(p["isr"]) < len(p["replicas"]):
                urp.append(f"{t['topic']}[{p['partition']}] isr={p['isr']} < replicas={p['replicas']}")
            if p["leader"] < 0:
                offline.append(f"{t['topic']}[{p['partition']}] leader=-1 (OFFLINE)")
    if rf_seen:
        maxrf = max(rf_seen)
        ft = ("NOT fault tolerant (RF=1: any broker/disk loss = data loss)"
              if maxrf == 1 else
              f"fault tolerant up to {maxrf-1} broker loss per partition (RF={maxrf})")
        findings.append(("replication", f"replication factor seen: {sorted(rf_seen)} — {ft}"))
    if urp:
        findings.append(("UNDER_REPLICATED", f"{len(urp)} partition(s): " + "; ".join(urp[:5])))
    if offline:
        findings.append(("OFFLINE_PARTITIONS", "; ".join(offline[:5])))
    if single:
        findings.append(("guidance",
            "Single-broker RF=1: fine for lab/demo, but no failover and no "
            "acks=all durability guarantee beyond the one node. A produce with acks=1 "
            "that appears to succeed can still be lost if the single broker's disk fails "
            "before flush."))
    return findings


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_discover(a):
    boot = a.bootstrap.split(',')
    host, port = boot[0].split(':'); port = int(port)
    out = {}
    # negotiate metadata version
    try:
        av = api_versions(host, port, a.timeout, a.client_id, a.tls)
        mver = 2 if av["max_metadata_version"] >= 2 else 1
        out["api_versions"] = av
    except Exception as e:
        _fail(a, f"ApiVersions failed against {host}:{port}: {e}",
              guidance="TCP reached but no Kafka handshake — is this really a Kafka "
                       "broker port? Is it a TLS-only listener? Try --tls.")
        return
    md = metadata(host, port, a.timeout, a.client_id, a.tls, topic=None, mver=mver)
    findings = analyze(md, boot)
    if a.json:
        print(json.dumps({"metadata": md, "findings": findings}, indent=2))
        return
    _banner(f"KAFKA DISCOVERY — bootstrap {a.bootstrap}")
    for k, v in findings:
        tag = "  " if k.islower() else ">>"
        print(f"{tag} {k:20} {v}")
    print("\n  Discovered brokers (the real server list):")
    for b in md["brokers"]:
        rack = f"  rack={b['rack']}" if b['rack'] else ""
        print(f"     node {b['node_id']:>3}  {b['host']}:{b['port']}{rack}")
    print(f"\n  Topics visible: {len(md['topics'])}")


def cmd_health(a):
    boot = a.bootstrap.split(',')
    host, port = boot[0].split(':'); port = int(port)
    _banner(f"KAFKA HEALTH — {a.bootstrap}")
    sec = probe_security(host, port, a.timeout, a.client_id)
    print(f"  security     plaintext={sec['plaintext']}  tls={sec['tls']}")
    print(f"               => {sec['verdict']}")
    try:
        av = api_versions(host, port, a.timeout, a.client_id, a.tls)
        print(f"  handshake    OK  ({av['api_count']} APIs, metadata<=v{av['max_metadata_version']})")
        mver = 2 if av["max_metadata_version"] >= 2 else 1
        md = metadata(host, port, a.timeout, a.client_id, a.tls, mver=mver)
        for k, v in analyze(md, boot):
            print(f"  {k:20} {v}")
        sev, key, text = check_produce_version(host, port, a.timeout, a.client_id, a.tls)
        print(f"  {sev_tag(sev)} {key}: {text}")
    except Exception as e:
        print(f"  handshake    FAILED: {e}")


def cmd_topic(a):
    boot = a.bootstrap.split(',')
    host, port = boot[0].split(':'); port = int(port)
    av = api_versions(host, port, a.timeout, a.client_id, a.tls)
    mver = 2 if av["max_metadata_version"] >= 2 else 1
    md = metadata(host, port, a.timeout, a.client_id, a.tls, topic=a.topic, mver=mver)
    if a.json:
        print(json.dumps(md, indent=2)); return
    _banner(f"TOPIC — {a.topic}")
    ts = [t for t in md["topics"] if t["topic"] == a.topic]
    if not ts or ts[0]["error"] != 0:
        err = ts[0]["error"] if ts else "not found"
        print(f"  topic '{a.topic}' error={err} "
              f"({'UNKNOWN_TOPIC (does not exist / not created)' if err==3 else err})")
        return
    t = ts[0]
    print(f"  {a.topic}: {len(t['partitions'])} partition(s)")
    for p in t["partitions"]:
        flag = ""
        if p["leader"] < 0: flag = "  <== OFFLINE (no leader)"
        elif len(p["isr"]) < len(p["replicas"]): flag = "  <== UNDER-REPLICATED"
        print(f"     p{p['partition']:<3} leader=node{p['leader']:<3} "
              f"replicas={p['replicas']} isr={p['isr']}{flag}")
    for k, v in analyze(md, boot):
        if k in ("replication", "UNDER_REPLICATED", "OFFLINE_PARTITIONS"):
            print(f"  {k}: {v}")


def _resolve(host):
    """Return list of resolved IPs for a host (or [host] if already an IP)."""
    try:
        infos = socket.getaddrinfo(host, None)
        ips = sorted({i[4][0] for i in infos})
        return ips
    except Exception as e:
        return []

def _is_private_or_loopback(ip):
    return (ip.startswith("127.") or ip == "::1" or ip.startswith("10.") or
            ip.startswith("192.168.") or
            any(ip.startswith(f"172.{n}.") for n in range(16, 32)))

def cmd_connect(a):
    """Deep connectivity doctor: DNS -> TCP -> handshake -> metadata -> advertised
       reachability, pinpointing where it breaks. Findings feed exit code + logging."""
    _banner(f"CONNECTIVITY DOCTOR - {a.bootstrap}")
    boot = a.bootstrap.split(',')
    ok = True
    for ep in boot:
        host, port = ep.split(':'); port = int(port)
        print(f"\n  bootstrap {ep}:")

        # stage 0: DNS resolution of the bootstrap host
        ips = _resolve(host)
        if not ips:
            emit("FAIL", "dns", f"bootstrap host '{host}' does NOT resolve")
            print(f"     [0] DNS resolve .......... FAIL ('{host}' has no A/AAAA record)")
            print(f"         guidance: name resolution failed from THIS host — fix DNS/hosts entry.")
            ok = False; continue
        else:
            emit("OK", "dns", f"bootstrap '{host}' -> {', '.join(ips)}")
            print(f"     [0] DNS resolve .......... OK ({host} -> {', '.join(ips)})")

        # stage 1: TCP
        try:
            s = socket.create_connection((host, port), timeout=a.timeout); s.close()
            print(f"     [1] TCP connect .......... OK")
        except Exception as e:
            emit("FAIL", "tcp", f"{ep} TCP connect failed: {type(e).__name__}")
            print(f"     [1] TCP connect .......... FAIL ({type(e).__name__}: {e})")
            print(f"         guidance: host/port unreachable - firewall, wrong port, or broker down.")
            ok = False; continue

        # stage 2: Kafka handshake
        try:
            av = api_versions(host, port, a.timeout, a.client_id, a.tls)
            print(f"     [2] Kafka handshake ...... OK (metadata<=v{av['max_metadata_version']})")
        except Exception as e:
            emit("FAIL", "handshake", f"{ep} Kafka handshake failed: {type(e).__name__}")
            print(f"     [2] Kafka handshake ...... FAIL ({type(e).__name__})")
            print(f"         guidance: TCP ok but no Kafka response - TLS-only listener? try --tls; "
                  f"or the port is not Kafka.")
            ok = False; continue

        # stage 3: metadata
        mver = 2 if av["max_metadata_version"] >= 2 else 1
        md = metadata(host, port, a.timeout, a.client_id, a.tls, mver=mver)
        print(f"     [3] Metadata ............. OK (cluster_id={md.get('cluster_id')}, "
              f"{len(md['brokers'])} broker(s))")

        # stage 4: reach each advertised broker + resolve its advertised name (the stall trap)
        boot_ips = set()
        for bhost in {b.split(':')[0] for b in boot}:
            boot_ips.update(_resolve(bhost))
        for b in md["brokers"]:
            adv = b["host"]
            adv_ips = _resolve(adv)
            # resolution check for advertised name
            if not adv_ips:
                ok = False
                emit("FAIL", "advertised_dns",
                     f"node{b['node_id']} advertises '{adv}' which does NOT resolve from this host")
                print(f"     [4] node{b['node_id']} {adv}:{b['port']} .. FAIL (advertised name does not resolve)")
                print(f"         guidance: the broker advertises a hostname the client can't resolve. "
                      f"Produce/consume hangs after Metadata. Fix advertised.listeners or add a hosts entry.")
                continue
            # reachability
            try:
                s = socket.create_connection((adv, b["port"]), timeout=a.timeout); s.close()
                # extra flag: advertised resolves to a private/docker-internal IP not in bootstrap set
                priv = [ip for ip in adv_ips if _is_private_or_loopback(ip)]
                if priv and not (set(adv_ips) & boot_ips) and adv not in {bh.split(':')[0] for bh in boot}:
                    emit("WARN", "advertised_private",
                         f"node{b['node_id']} advertises {adv} ({','.join(adv_ips)}) — private/loopback, "
                         f"differs from bootstrap; reachable from HERE but may not be from other clients")
                    print(f"     [4] node{b['node_id']} {adv}:{b['port']} .. OK  (WARN: advertises private {','.join(adv_ips)})")
                else:
                    emit("OK", "advertised_reach", f"node{b['node_id']} {adv}:{b['port']} reachable ({','.join(adv_ips)})")
                    print(f"     [4] node{b['node_id']} {adv}:{b['port']} .. OK ({','.join(adv_ips)})")
            except Exception as e:
                ok = False
                emit("FAIL", "advertised_reach",
                     f"node{b['node_id']} advertises {adv}:{b['port']} but it is UNREACHABLE ({type(e).__name__})")
                print(f"     [4] node{b['node_id']} {adv}:{b['port']} .. FAIL ({type(e).__name__})")
                print(f"         guidance: broker ADVERTISES {adv} but the client can't reach it. "
                      f"This is the #1 'connects then stalls' cause. Fix advertised.listeners to an "
                      f"address routable from clients, or add a route/hosts entry.")

    result = "all stages OK" if ok else "problems found (see FAIL lines + guidance)"
    print("\n  RESULT:", result)
    if a.json:
        print(json.dumps({"result": result, "ok": ok}, indent=2))


def cmd_produce_test(a):
    """Produce a single test record to a THROWAWAY topic and verify offset advanced.
       Requires confluent-kafka or kafka-python. Refuses protected topics."""
    import re
    if re.match(a.protect, a.topic) and not a.force:
        _banner("PRODUCE-TEST BLOCKED")
        print(f"  refusing to produce to protected topic '{a.topic}' (matches --protect '{a.protect}').")
        print(f"  use a throwaway topic (default __kafka_doctor_test) or pass --force (NOT advised on prod).")
        return
    prod = None
    try:
        from confluent_kafka import Producer, Consumer, TopicPartition, KafkaException  # type: ignore
        prod = "confluent"
    except Exception:
        try:
            from kafka import KafkaProducer, KafkaConsumer, TopicPartition  # type: ignore
            prod = "kafka-python"
        except Exception:
            _banner("PRODUCE-TEST UNAVAILABLE")
            print("  neither confluent-kafka nor kafka-python is installed.")
            print("  discovery/health/connect commands work without them (stdlib only).")
            print("  to enable produce-test:  pip install confluent-kafka --break-system-packages")
            return
    _banner(f"PRODUCE-TEST — topic '{a.topic}' via {prod}")
    payload = f"kafka_doctor test {time.time()}".encode()
    if prod == "confluent":
        from confluent_kafka import Producer, Consumer, TopicPartition
        p = Producer({"bootstrap.servers": a.bootstrap, "socket.timeout.ms": int(a.timeout*1000),
                      "client.id": a.client_id})
        # baseline offset
        c = Consumer({"bootstrap.servers": a.bootstrap, "group.id": "kdoctor-"+str(int(time.time())),
                      "enable.auto.commit": False, "auto.offset.reset": "latest"})
        before = _cf_end_offset(c, a.topic)
        delivered = {}
        def cb(err, msg):
            delivered["err"] = err
            delivered["offset"] = None if err else msg.offset()
            delivered["partition"] = None if err else msg.partition()
        p.produce(a.topic, payload, callback=cb); p.flush(a.timeout)
        after = _cf_end_offset(c, a.topic); c.close()
        if delivered.get("err"):
            print(f"  produce FAILED: {delivered['err']}")
            print(f"  guidance: {_produce_err_guidance(str(delivered['err']))}")
        else:
            print(f"  produce OK -> partition {delivered['partition']} offset {delivered['offset']}")
            print(f"  topic end offset: before={before} after={after} "
                  f"({'advanced ✓' if after>before else 'NOT advanced ✗'})")
    else:
        from kafka import KafkaProducer, KafkaConsumer, TopicPartition
        kp = KafkaProducer(bootstrap_servers=a.bootstrap.split(','),
                           request_timeout_ms=int(a.timeout*1000), client_id=a.client_id)
        fut = kp.send(a.topic, payload)
        try:
            rm = fut.get(timeout=a.timeout)
            print(f"  produce OK -> partition {rm.partition} offset {rm.offset}")
        except Exception as e:
            print(f"  produce FAILED: {e}")
            print(f"  guidance: {_produce_err_guidance(str(e))}")
        kp.close()


def _cf_end_offset(consumer, topic):
    try:
        from confluent_kafka import TopicPartition
        md = consumer.list_topics(topic, timeout=5)
        t = md.topics.get(topic)
        if not t: return 0
        total = 0
        for pid in t.partitions:
            _lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=5)
            total += hi
        return total
    except Exception:
        return -1


def _produce_err_guidance(err):
    e = err.lower()
    if "timed out" in e or "timeout" in e:
        return ("produce timed out — broker acked the connection but not the write. Classic "
                "advertised-listener trap (client can't reach the real broker addr) OR the "
                "partition leader is unavailable. Run 'connect' to check advertised reachability.")
    if "unknown_topic" in e or "unknown topic" in e:
        return "topic does not exist and auto-create is off. Create it or enable auto.create.topics."
    if "not_leader" in e or "leader" in e:
        return "partition leader moved/unavailable — transient during rebalance, or a dead broker in a cluster."
    if "authentication" in e or "sasl" in e or "ssl" in e:
        return "auth/TLS handshake issue — listener needs SASL/SSL creds this tool wasn't given."
    return "see broker logs; run 'health' and 'connect' to localize the stage."


# ---------------------------------------------------------------------------
def cmd_consume(a):
    """Read records from a topic (native wire Fetch, stdlib-only). Reads from EARLIEST
       by default so you SEE what is already there, not just new messages."""
    boot = a.bootstrap.split(',')
    host, port = boot[0].split(':'); port = int(port)
    av = api_versions(host, port, a.timeout, a.client_id, a.tls)
    mver = 2 if av["max_metadata_version"] >= 2 else 1
    md = metadata(host, port, a.timeout, a.client_id, a.tls, topic=a.topic, mver=mver)
    ts = [t for t in md["topics"] if t["topic"] == a.topic]
    if not ts or ts[0]["error"] != 0:
        err = ts[0]["error"] if ts else "not found"
        _banner(f"CONSUME — {a.topic}")
        print(f"  topic '{a.topic}' error={err} "
              f"({'UNKNOWN_TOPIC (does not exist)' if err==3 else err})")
        return
    parts = [p["partition"] for p in ts[0]["partitions"]]
    _banner(f"CONSUME — {a.topic}  ({len(parts)} partition(s), from {'latest' if a.latest else 'earliest'})")
    grand_total = 0
    shown = 0
    for pid in sorted(parts):
        earliest = list_offsets(host, port, a.timeout, a.client_id, a.tls, a.topic, pid, -2)
        latest   = list_offsets(host, port, a.timeout, a.client_id, a.tls, a.topic, pid, -1)
        avail = (latest or 0) - (earliest or 0)
        grand_total += avail
        if avail == 0:
            print(f"  p{pid}: empty (earliest={earliest} latest={latest})")
            continue
        # where to start
        if a.latest:
            start = max(earliest or 0, (latest or 0) - a.max)
        else:
            start = earliest or 0
        print(f"  p{pid}: {avail} record(s) available (offsets {earliest}..{latest-1}); reading from {start}")
        try:
            recs = fetch(host, port, a.timeout, a.client_id, a.tls, a.topic, pid, start)
        except _MissingCodecLib as e:
            if e.codec not in _CODEC_MISSING_WARNED:
                _CODEC_MISSING_WARNED.add(e.codec)
                print(f"     records on p{pid} are {e.codec.upper()}-compressed and the "
                      f"'{e.codec}' library is not installed.")
                print(f"     install it to view them:  pip install {e.pip_name}")
                print(f"     (this topic's producer, e.g. the AI Streamer, compresses with {e.codec.upper()})")
            continue
        for off, key, val in recs:
            if shown >= a.max:
                break
            v = "(null)" if val is None else val[:a.width].decode('utf-8', 'replace')
            k = "" if key is None else f" key={key[:40].decode('utf-8','replace')}"
            print(f"     off{off}{k}: {v}")
            shown += 1
        if shown >= a.max:
            print(f"     ... (stopped at --max {a.max}; more records exist)")
            break
    print(f"\n  total records in topic: {grand_total}   shown: {shown}")
    if grand_total > 0 and shown == 0:
        print("  NOTE: records exist but none shown — try without --latest, or raise --max.")
    if grand_total == 0:
        print("  topic is genuinely EMPTY (earliest == latest on every partition).")


def cmd_simulate(a):
    """Simulate AI Streamer producing OMNIS-format JSON flow records to a test topic.
       Emits records matching the netops/'assets' (aggregate_telemetry) schema, as JSON,
       the way the real Streamer exports (producer_destination_format=json).
       Uses confluent-kafka (install: pip install confluent-kafka)."""
    import json as _json, random, time as _time
    if not a.force and not a.topic.endswith("-test") and re_match_protected(a.topic):
        _banner("SIMULATE BLOCKED")
        print(f"  '{a.topic}' looks like a real OMNIS topic. Use a *-test topic "
              f"(e.g. OMNIS.assets-test) or pass --force.")
        return
    try:
        from confluent_kafka import Producer
    except Exception:
        _banner("SIMULATE UNAVAILABLE")
        print("  confluent-kafka not installed:  pip install confluent-kafka")
        return

    _banner(f"SIMULATE AI STREAMER -> {a.topic}  ({a.count} record(s), JSON)")

    # netops / aggregate_telemetry ('assets') field set, exact order from collection config
    SENSORS = [("192.168.30.99", "vstream99", 3), ("192.168.30.2", "win2019", 3)]
    APPS = [("HTTPS", "Web"), ("DNS", "Network Services"), ("LDAP", "Directory"),
            ("SSL", "Web"), ("Kerberos", "Directory")]
    SUBNETS = ["192.168.30.", "192.168.12.", "192.168.50.", "10.8.8."]

    def rec(now_ms):
        sip = random.choice(SUBNETS) + str(random.randint(2, 254))
        cip = random.choice(SUBNETS) + str(random.randint(2, 254))
        app, grp = random.choice(APPS)
        sensor_ip, sensor_name, ifn = random.choice(SENSORS)
        status = random.choice(["success", "success", "success", "failure"])
        tsp = random.randint(1, 500); fsp = random.randint(1, 500)
        return {
            "timestamp": now_ms,
            "server_host_ip_address": sip,
            "server_port": random.choice([443, 53, 389, 88, 80]),
            "client_host_ip_address": cip,
            "vlan_id": random.choice([0, 10, 12, 30, 50]),
            "application_name": app,
            "ai_sensor_ip_address": sensor_ip,
            "ai_sensor_name": sensor_name,
            "ai_sensor_interface_number": ifn,
            "message_name": app + "_transaction",
            "application_group": grp,
            "transaction_status": status,
            "response_code": 0 if status == "success" else random.choice([1, 2, 3]),
            "response_description": "OK" if status == "success" else "ERROR",
            "to_server_packets": tsp,
            "from_server_packets": fsp,
            "to_server_octets": tsp * random.randint(40, 1500),
            "from_server_octets": fsp * random.randint(40, 1500),
            "client_min_window_size": random.randint(1024, 65535),
            "server_min_window_size": random.randint(1024, 65535),
            "sum_of_client_latency": random.randint(0, 50000),
            "sum_of_server_latency": random.randint(0, 50000),
            "client_latency_count": random.randint(0, tsp),
            "server_latency_count": random.randint(0, fsp),
            "client_ack_retransmissions_count": random.randint(0, 5),
            "server_ack_retransmissions_count": random.randint(0, 5),
        }

    p = Producer({"bootstrap.servers": a.bootstrap,
                  "client.id": f"ods.davonet.com-export-1-{a.topic}.kafka_default_cluster",
                  "socket.timeout.ms": int(a.timeout * 1000)})
    sent = {"ok": 0, "err": 0}
    def cb(err, msg):
        if err: sent["err"] += 1
        else:   sent["ok"] += 1

    now_ms = int(_time.time() * 1000)
    for i in range(a.count):
        r = rec(now_ms)
        p.produce(a.topic, _json.dumps(r).encode("utf-8"), callback=cb)
        if i < 2:
            print(f"  sample record {i}: {_json.dumps(r)[:180]}...")
        if a.rate > 0:
            p.poll(0); _time.sleep(1.0 / a.rate)
    p.flush(a.timeout + 5)
    print(f"\n  produced: {sent['ok']} ok, {sent['err']} failed  ->  {a.topic}")
    print(f"  verify with:  python kafka_read.py {a.bootstrap} {a.topic} --max 5")


def cmd_config(a):
    """Show produce-relevant broker + topic configs and flag misconfigs (read-only)."""
    boot = a.bootstrap.split(',')
    host, port = boot[0].split(':'); port = int(port)
    av = api_versions(host, port, a.timeout, a.client_id, a.tls)
    mver = 2 if av["max_metadata_version"] >= 2 else 1
    md = metadata(host, port, a.timeout, a.client_id, a.tls, mver=mver)
    broker_id = str(md["controller_id"] if md["controller_id"] >= 0 else md["brokers"][0]["node_id"])
    bkeys = ["compression.type","message.max.bytes","log.message.timestamp.type",
             "replica.fetch.max.bytes"]
    tkeys = ["compression.type","max.message.bytes","retention.ms","cleanup.policy"]
    try:
        _, _, bcfg = describe_configs(host, port, a.timeout, a.client_id, a.tls,
                                      RESOURCE_BROKER, broker_id, keys=bkeys)
    except Exception as e:
        bcfg = {}; log.info("broker DescribeConfigs failed: %s", e)
    tcfg = {}
    if getattr(a, "topic", None):
        try:
            _, _, tcfg = describe_configs(host, port, a.timeout, a.client_id, a.tls,
                                          RESOURCE_TOPIC, a.topic, keys=tkeys)
        except Exception as e:
            log.info("topic DescribeConfigs failed: %s", e)
    findings = analyze_configs(bcfg, tcfg, getattr(a, "topic", None) or "(none)")
    if a.json:
        print(json.dumps({"broker_config": bcfg, "topic_config": tcfg,
                          "findings": [(s,k,v) for (s,k,v) in findings]}, indent=2)); return
    _banner(f"CONFIG - broker {broker_id}" + (f" + topic {a.topic}" if getattr(a,'topic',None) else ""))
    print("  broker configs:")
    for k in bkeys: print(f"     {k:32} {bcfg.get(k)}")
    if getattr(a, "topic", None):
        print(f"  topic '{a.topic}' configs:")
        for k in tkeys: print(f"     {k:32} {tcfg.get(k)}")
    print("  findings:")
    for sev, key, text in findings:
        print(f"     {sev_tag(sev)} {key}: {text}")



# ---------------------------------------------------------------------------
# Phase 2: codec round-trip. Produce one record per compression codec and
# verify the broker accepted+stored it (Produce error code == 0 AND the topic's
# committed offset advanced). Stdlib-only for none+gzip (always tested);
# lz4/snappy/zstd are tested opportunistically IF their libs are importable,
# else reported as SKIPPED-no-lib. Even the gzip test exercises the broker's
# decompress path (relevant when compression.type!=producer or LogAppendTime).
# ---------------------------------------------------------------------------
import gzip as _gzip, io as _io

PRODUCE = 0
CREATE_TOPICS = 19
_CODEC_ATTR = {"none": 0, "gzip": 1, "snappy": 2, "lz4": 3, "zstd": 4}

_CRC32C_POLY = 0x82F63B78
_CRC32C_TABLE = []
for _n in range(256):
    _c = _n
    for _ in range(8):
        _c = (_c >> 1) ^ _CRC32C_POLY if (_c & 1) else (_c >> 1)
    _CRC32C_TABLE.append(_c)

def _crc32c(data, crc=0):
    crc ^= 0xffffffff
    for b in data:
        crc = (crc >> 8) ^ _CRC32C_TABLE[(crc ^ b) & 0xff]
    return crc ^ 0xffffffff

def _zigzag_varint(n):
    n = (n << 1) ^ (n >> 63)
    out = b''
    while True:
        b = n & 0x7f; n >>= 7
        out += bytes([b | 0x80]) if n else bytes([b])
        if not n: break
    return out

def _codec_compress(codec, raw):
    if codec == "none":
        return raw
    if codec == "gzip":
        buf = _io.BytesIO()
        with _gzip.GzipFile(fileobj=buf, mode='wb') as g:
            g.write(raw)
        return buf.getvalue()
    if codec == "lz4":
        import lz4.frame as _L; return _L.compress(raw)
    if codec == "snappy":
        import snappy as _S; return _S.compress(raw)
    if codec == "zstd":
        try:
            import zstandard as _Z; return _Z.ZstdCompressor(write_content_size=True).compress(raw)
        except ImportError:
            import zstd as _Z2; return _Z2.compress(raw)
    raise ValueError(codec)


# Inverse of _CODEC_ATTR: batch-attributes low 3 bits -> codec name
_CODEC_BY_ATTR = {0: "none", 1: "gzip", 2: "snappy", 3: "lz4", 4: "zstd"}

# Records the codecs we have already warned about (missing lib) so we prompt once.
_CODEC_MISSING_WARNED = set()

class _MissingCodecLib(Exception):
    """Raised when a batch is compressed with a codec whose Python lib is absent."""
    def __init__(self, codec, pip_name):
        self.codec = codec; self.pip_name = pip_name
        super().__init__(f"{codec} library not installed (pip install {pip_name})")

def _codec_decompress(codec, comp):
    """Inverse of _codec_compress. Raises _MissingCodecLib if the codec's lib is absent."""
    if codec == "none":
        return comp
    if codec == "gzip":
        with _gzip.GzipFile(fileobj=_io.BytesIO(comp), mode='rb') as g:
            return g.read()
    if codec == "lz4":
        try:
            import lz4.frame as _L
        except ImportError:
            raise _MissingCodecLib("lz4", "lz4")
        return _L.decompress(comp)
    if codec == "snappy":
        try:
            import snappy as _S
        except ImportError:
            raise _MissingCodecLib("snappy", "python-snappy")
        return _S.decompress(comp)
    if codec == "zstd":
        try:
            import zstandard as _Z
            return _Z.ZstdDecompressor().decompress(comp)
        except ImportError:
            try:
                import zstd as _Z2
                return _Z2.decompress(comp)
            except ImportError:
                raise _MissingCodecLib("zstd", "zstandard")
    raise ValueError(codec)

def _build_record(key, value):
    body = b'\x00'                       # attributes
    body += _zigzag_varint(0)            # timestamp delta
    body += _zigzag_varint(0)            # offset delta
    body += _zigzag_varint(-1) if key is None else _zigzag_varint(len(key)) + key
    body += _zigzag_varint(len(value)) + value
    body += _zigzag_varint(0)            # header count
    return _zigzag_varint(len(body)) + body

def _build_batch(codec, key, value):
    recs = _build_record(key, value)
    comp = _codec_compress(codec, recs)
    attributes = _CODEC_ATTR[codec]      # low 3 bits carry the codec
    now = int(time.time() * 1000)
    after_crc  = struct.pack('>h', attributes)
    after_crc += struct.pack('>i', 0)    # lastOffsetDelta (single record)
    after_crc += struct.pack('>q', now)  # first timestamp
    after_crc += struct.pack('>q', now)  # max timestamp
    after_crc += struct.pack('>q', -1)   # producerId
    after_crc += struct.pack('>h', -1)   # producerEpoch
    after_crc += struct.pack('>i', -1)   # baseSequence
    after_crc += struct.pack('>i', 1)    # record count
    after_crc += comp
    crc = _crc32c(after_crc) & 0xffffffff
    magic = 2
    batch_body = struct.pack('>i', 0) + struct.pack('>b', magic) + struct.pack('>I', crc) + after_crc
    return struct.pack('>q', 0) + struct.pack('>i', len(batch_body)) + batch_body

def produce_one(host, port, timeout, client_id, use_tls, topic, codec, partition=0):
    """Produce a single record with the given codec via Produce v3.
       Returns (error_code, base_offset, value_bytes)."""
    s = open_socket(host, port, timeout, use_tls)
    try:
        cid = (client_id or "").encode('utf-8')
        hdr = struct.pack('>hhi', PRODUCE, 7, 55) + struct.pack('>h', len(cid)) + cid
        value = f"kdoctor-{codec}-{time.time()}".encode()
        batch = _build_batch(codec, None, value)
        body  = struct.pack('>h', -1)                 # transactional_id (null)
        body += struct.pack('>h', 1)                  # acks=1
        body += struct.pack('>i', int(timeout * 1000))
        body += struct.pack('>i', 1)                  # topics
        body += _enc_str(topic)
        body += struct.pack('>i', 1)                  # partitions
        body += struct.pack('>i', partition)
        body += struct.pack('>i', len(batch)) + batch
        _send_request(s, hdr + body)
        r = Reader(_recv_response(s))
        r.i32()                                       # corr
        r.i32(); r.string()                           # topics count, topic name
        r.i32(); r.i32()                              # partitions count, partition index
        err = r.i16()
        base = r.i64()
        return err, base, value
    finally:
        s.close()

_ERRNAME = {0: "NONE", 3: "UNKNOWN_TOPIC_OR_PARTITION", 6: "NOT_LEADER_OR_FOLLOWER",
            7: "REQUEST_TIMED_OUT", 10: "MESSAGE_TOO_LARGE", 35: "UNSUPPORTED_VERSION",
            42: "INVALID_RECORD", 87: "INVALID_RECORD"}

def create_topic(host, port, timeout, client_id, use_tls, topic, partitions=1, rf=1):
    """CreateTopics v0. Returns (error_code, error_msg). err 36 = ALREADY_EXISTS (treated OK)."""
    s = open_socket(host, port, timeout, use_tls)
    try:
        hdr = _req_header(CREATE_TOPICS, 0, 99, client_id)
        body = struct.pack('>i', 1)                      # topics array: 1
        body += _enc_str(topic)
        body += struct.pack('>i', partitions)            # num_partitions
        body += struct.pack('>h', rf)                    # replication_factor
        body += struct.pack('>i', 0)                     # replica_assignments: 0
        body += struct.pack('>i', 0)                     # config_entries: 0
        body += struct.pack('>i', int(timeout * 1000))   # timeout_ms
        _send_request(s, hdr + body)
        r = Reader(_recv_response(s))
        r.i32()                                          # corr id
        r.i32()                                          # topics count
        r.string()                                       # topic name
        err = r.i16()
        return err, None
    finally:
        s.close()


def cmd_codec(a):
    """Produce one record per compression codec to a THROWAWAY topic and verify the
       broker accepted+stored it (error==0 and committed offset advanced).
       Read-only for real topics: refuses --protect topics unless --force."""
    import re
    topic = getattr(a, "topic", None) or "__kdoctor_codec_test"
    if re.match(a.protect, topic) and not a.force:
        _banner("CODEC TEST BLOCKED")
        print(f"  refusing to produce to protected topic '{topic}' (matches --protect '{a.protect}').")
        print(f"  use a throwaway topic (default __kdoctor_codec_test) or pass --force.")
        note_severity("WARN")
        return
    boot = a.bootstrap.split(',')
    host, port = boot[0].split(':'); port = int(port)
    codecs = [c.strip() for c in (getattr(a, "codecs", None) or "none,gzip,lz4,snappy,zstd").split(',')]
    _banner(f"CODEC TEST -> topic '{topic}'  ({', '.join(codecs)})")
    print("  (produces 1 record per codec; verifies broker stored it via offset delta)")

    def latest(pid=0):
        return list_offsets(host, port, a.timeout, a.client_id, a.tls, topic, pid, -1)

    # ensure topic exists; if missing, auto-create it (default throwaway __kdoctor_codec_test)
    before = latest()
    if before is None:
        try:
            cerr, _ = create_topic(host, port, a.timeout, a.client_id, a.tls, topic, partitions=1, rf=1)
            if cerr in (0, 36):
                print(f"  topic '{topic}' {'already existed' if cerr == 36 else 'auto-created (1 partition, RF=1)'}.")
                time.sleep(0.3)
            else:
                cname = _ERRNAME.get(cerr, f"error_{cerr}")
                print(f"  NOTE: could not auto-create topic '{topic}': {cname} (code {cerr}); "
                      f"codec produce may fail with UNKNOWN_TOPIC_OR_PARTITION.")
        except Exception as e:
            print(f"  NOTE: auto-create of '{topic}' errored ({type(e).__name__}: {e}); "
                  f"codec produce may fail if the topic is missing.")

    results = []
    for codec in codecs:
        try:
            pre = latest() or 0
            err, base, val = produce_one(host, port, a.timeout, a.client_id, a.tls, topic, codec)
            time.sleep(0.2)
            post = latest() or 0
            errname = _ERRNAME.get(err, f"error_{err}")
            if err == 0 and post > pre:
                results.append(emit("OK", f"codec:{codec}",
                    f"produced & stored (err=NONE, offset {pre}->{post})"))
            elif err == 0:
                results.append(emit("WARN", f"codec:{codec}",
                    f"produce returned NONE but offset did not advance ({pre}->{post}) — verify manually"))
            else:
                results.append(emit("FAIL", f"codec:{codec}",
                    f"produce REJECTED: {errname} (code {err}). "
                    f"{'topic missing — create it first' if err==3 else 'broker cannot accept this codec on this topic'}"))
        except ImportError as e:
            results.append(emit("INFO", f"codec:{codec}",
                f"SKIPPED — optional lib '{e.name}' not installed (stdlib tests none+gzip; "
                f"install {e.name} to test this codec)"))
        except Exception as e:
            results.append(emit("FAIL", f"codec:{codec}", f"ERROR {type(e).__name__}: {e}"))

    if a.json:
        print(json.dumps({"topic": topic, "results": [(s,k,v) for (s,k,v) in results]}, indent=2))
        return
    print("  results:")
    for sev, key, text in results:
        print(f"     {sev_tag(sev)} {key}: {text}")
    print("\n  interpretation: a codec that FAILS here while others pass points to a "
          "\n  broker-side native-lib or recompression problem for that codec. If none+gzip "
          "\n  pass but lz4 is SKIPPED, install python-lz4 to actively test the Streamer's codec, "
          "\n  or run the broker-side jar/native check (kafka-codec-check.sh).")



# ---------------------------------------------------------------------------
# Phase 3: size boundary probe. Reads the effective max.message.bytes (topic
# override or broker default via DescribeConfigs), then produces a record just
# UNDER it (expect NONE) and just OVER it (expect MESSAGE_TOO_LARGE=10),
# proving the broker actually enforces the number it reports. Also surfaces
# producer-side clipping: if the under-limit record fails to send at all, the
# client-side max.request.size is lower than the broker's limit.
# Throwaway-topic only unless --force. Reuses Phase 2 batch builder.
# ---------------------------------------------------------------------------
def _produce_sized(host, port, timeout, client_id, use_tls, topic, nbytes, codec="none", partition=0):
    s = open_socket(host, port, timeout, use_tls)
    try:
        cid = (client_id or "").encode('utf-8')
        hdr = struct.pack('>hhi', PRODUCE, 7, 55) + struct.pack('>h', len(cid)) + cid
        value = b'X' * nbytes
        batch = _build_batch(codec, None, value)
        body  = struct.pack('>h', -1) + struct.pack('>h', 1) + struct.pack('>i', int(timeout*1000))
        body += struct.pack('>i', 1) + _enc_str(topic)
        body += struct.pack('>i', 1) + struct.pack('>i', partition)
        body += struct.pack('>i', len(batch)) + batch
        _send_request(s, hdr + body)
        r = Reader(_recv_response(s))
        r.i32(); r.i32(); r.string(); r.i32(); r.i32()
        err = r.i16(); base = r.i64()
        return err, base, len(batch)
    finally:
        s.close()

def cmd_size(a):
    """Probe the effective message-size limit: produce just-under (expect OK) and
       just-over (expect MESSAGE_TOO_LARGE). Confirms enforcement and catches
       producer-side max.request.size clipping. Throwaway topic only unless --force."""
    import re
    topic = getattr(a, "topic", None) or "__kdoctor_size_test"
    if re.match(a.protect, topic) and not a.force:
        _banner("SIZE PROBE BLOCKED")
        print(f"  refusing to produce to protected topic '{topic}' (matches --protect '{a.protect}').")
        print(f"  use a throwaway topic (default __kdoctor_size_test) or pass --force.")
        note_severity("WARN")
        return
    boot = a.bootstrap.split(',')
    host, port = boot[0].split(':'); port = int(port)

    # discover effective limit: topic max.message.bytes overrides broker message.max.bytes
    limit = None; src = None
    try:
        _, _, tcfg = describe_configs(host, port, a.timeout, a.client_id, a.tls,
                                      RESOURCE_TOPIC, topic, keys=["max.message.bytes"])
        if tcfg.get("max.message.bytes"):
            limit = int(tcfg["max.message.bytes"]); src = f"topic '{topic}' max.message.bytes"
    except Exception as e:
        log.info("topic size cfg failed: %s", e)
    if limit is None:
        try:
            md = metadata(host, port, a.timeout, a.client_id, a.tls, mver=2)
            bid = str(md["controller_id"] if md["controller_id"] >= 0 else md["brokers"][0]["node_id"])
            _, _, bcfg = describe_configs(host, port, a.timeout, a.client_id, a.tls,
                                          RESOURCE_BROKER, bid, keys=["message.max.bytes"])
            if bcfg.get("message.max.bytes"):
                limit = int(bcfg["message.max.bytes"]); src = "broker message.max.bytes"
        except Exception as e:
            log.info("broker size cfg failed: %s", e)
    if limit is None:
        limit = 1048588; src = "default (could not read config)"

    _banner(f"SIZE PROBE -> topic '{topic}'")
    print(f"  effective limit: {limit} bytes  (from {src})")

    # batch overhead is ~74 bytes; aim the value so the BATCH lands just under / just over.
    OVERHEAD = 80
    under_val = max(1, limit - OVERHEAD - 64)   # comfortably under
    over_val  = limit + 256                       # comfortably over
    results = []

    try:
        err, base, blen = _produce_sized(host, port, a.timeout, a.client_id, a.tls, topic, under_val)
        if err == 0:
            results.append(emit("OK", "under_limit",
                f"{blen}B batch accepted (value {under_val}B, offset {base}) — under-limit produce works"))
        elif err == 10:
            results.append(emit("WARN", "under_limit",
                f"{blen}B batch REJECTED MESSAGE_TOO_LARGE though under the reported limit — "
                f"the real enforced limit is lower than {limit} (check for a lower topic override or broker setting)"))
        else:
            results.append(emit("FAIL", "under_limit",
                f"{blen}B batch failed unexpectedly: {_ERRNAME.get(err, 'error_'+str(err))} (code {err})"))
    except Exception as e:
        results.append(emit("FAIL", "under_limit",
            f"producer could not even SEND a {under_val}B record ({type(e).__name__}) — "
            f"client-side max.request.size is smaller than the broker limit"))

    try:
        err, base, blen = _produce_sized(host, port, a.timeout, a.client_id, a.tls, topic, over_val)
        if err == 10:
            results.append(emit("OK", "over_limit",
                f"{blen}B batch correctly REJECTED (MESSAGE_TOO_LARGE) — broker enforces the limit"))
        elif err == 0:
            results.append(emit("WARN", "over_limit",
                f"{blen}B batch was ACCEPTED though over the reported limit ({limit}) — "
                f"real limit is higher than reported, or config read the wrong scope"))
        else:
            results.append(emit("WARN", "over_limit",
                f"{blen}B batch returned {_ERRNAME.get(err,'error_'+str(err))} (code {err}) instead of MESSAGE_TOO_LARGE"))
    except Exception as e:
        results.append(emit("INFO", "over_limit",
            f"over-limit record could not be sent client-side ({type(e).__name__}) — "
            f"producer max.request.size caps it before the broker; that's a client-side limit, not the broker's"))

    if a.json:
        print(json.dumps({"topic": topic, "limit": limit, "limit_source": src,
                          "results": [(s,k,v) for (s,k,v) in results]}, indent=2))
        return
    print("  results:")
    for sev, key, text in results:
        print(f"     {sev_tag(sev)} {key}: {text}")
    print(f"\n  interpretation: healthy = under_limit OK + over_limit rejected(10). "
          f"\n  If the customer's AI Streamer produces large aggregate batches, compare their "
          f"\n  batch sizes (from a pcap) against this limit; batches over it drop whole with MESSAGE_TOO_LARGE.")


FIND_COORDINATOR = 10
OFFSET_FETCH = 9
LIST_GROUPS = 16
DESCRIBE_GROUPS = 15

def find_coordinator(host, port, timeout, client_id, use_tls, key, key_type=0):
    """FindCoordinator v2. key_type 0=group. Returns (err, node_id, host, port)."""
    s = open_socket(host, port, timeout, use_tls)
    try:
        hdr = _req_header(FIND_COORDINATOR, 2, 71, client_id)
        body = _enc_str(key) + struct.pack('>b', key_type)
        _send_request(s, hdr + body)
        r = Reader(_recv_response(s))
        r.i32(); r.i32()                    # corr, throttle
        err = r.i16(); r.string()           # error, error_msg
        nid = r.i32(); h = r.string(); p = r.i32()
        return err, nid, h, p
    finally:
        s.close()

def offset_fetch(host, port, timeout, client_id, use_tls, group):
    """OffsetFetch v5 (all topics). Returns (top_err, [(topic,partition,committed,err)])."""
    s = open_socket(host, port, timeout, use_tls)
    try:
        hdr = _req_header(OFFSET_FETCH, 5, 72, client_id)
        body = _enc_str(group) + struct.pack('>i', -1)   # null topics => all committed
        _send_request(s, hdr + body)
        r = Reader(_recv_response(s))
        r.i32(); r.i32()                    # corr, throttle
        nt = r.i32()
        out = []
        for _ in range(nt):
            t = r.string(); npart = r.i32()
            for _ in range(npart):
                pid = r.i32(); off = r.i64(); r.i32(); r.string(); perr = r.i16()
                out.append((t, pid, off, perr))
        top_err = r.i16()
        return top_err, out
    finally:
        s.close()

def fetch_newest_timestamp(host, port, timeout, client_id, use_tls, topic, partition, log_end):
    """Return the max timestamp (ms) of the newest batch in a partition, or None.
       Reads only batch headers, so it works regardless of codec (no decompress)."""
    if not log_end or log_end <= 0:
        return None
    start = max(0, log_end - 1)
    s = open_socket(host, port, timeout, use_tls)
    try:
        hdr = _req_header(FETCH, 4, 23, client_id)
        body = struct.pack('>i', -1)
        body += struct.pack('>i', int(timeout*1000))
        body += struct.pack('>i', 1)
        body += struct.pack('>i', 1048576)
        body += struct.pack('>b', 0)
        body += struct.pack('>i', 1) + _enc_str(topic)
        body += struct.pack('>i', 1) + struct.pack('>i', partition)
        body += struct.pack('>q', start) + struct.pack('>i', 1048576)
        _send_request(s, hdr + body)
        r = Reader(_recv_response(s))
        r.i32(); r.i32(); r.i32(); r.string(); r.i32(); r.i32()
        err = r.i16(); r.i64(); r.i64()
        ac = r.i32()
        if ac > 0:
            for _ in range(ac): r.i64(); r.i64()
        rs_len = r.i32()
        if err != 0 or rs_len <= 0:
            return None
        raw = r.b[r.i:r.i+rs_len]
        rr = Reader(raw)
        newest = None
        while rr.remaining() >= 61:
            rr.i64()                 # base offset
            blen = rr.i32()
            end = rr.i + blen
            rr.i32(); mag = rr.i8()
            if mag != 2: break
            rr.i32(); rr.i16(); rr.i32()
            rr.i64()                 # first timestamp
            maxts = rr.i64()         # max timestamp of the batch
            if newest is None or maxts > newest:
                newest = maxts
            rr.i = end
        return newest
    except Exception:
        return None
    finally:
        s.close()


def _fmt_age(ms):
    if ms is None: return "unknown"
    secs = int(ms // 1000)
    if secs < 0: secs = 0
    d, rem = divmod(secs, 86400); h, rem = divmod(rem, 3600); m, sec = divmod(rem, 60)
    if d:   return f"{d}d{h}h{m}m"
    if h:   return f"{h}h{m}m{sec}s"
    if m:   return f"{m}m{sec}s"
    return f"{sec}s"


def cmd_freshness(a):
    """Is data flowing RIGHT NOW? Samples log-end offsets over a short window to
       measure records/sec, and reads the newest message age per topic. A dead
       producer shows 0 rec/s + growing age even though lag looks fine."""
    boot = a.bootstrap.split(',')
    host, port = boot[0].split(':'); port = int(port)
    window = getattr(a, "window", 5.0) or 5.0
    av = api_versions(host, port, a.timeout, a.client_id, a.tls)
    mver = 2 if av["max_metadata_version"] >= 2 else 1
    # topic selection: --topic (may be a glob), else all non-internal topics
    md = metadata(host, port, a.timeout, a.client_id, a.tls,
                  topic=(a.topic if a.topic and "*" not in a.topic else None), mver=mver)
    want = a.topic
    def match(t):
        if not want: return not t.startswith("__")
        if "*" in want:
            import fnmatch; return fnmatch.fnmatch(t, want)
        return t == want
    topics = sorted({t["topic"] for t in md["topics"] if match(t["topic"])})
    if not topics:
        _banner("FRESHNESS"); print(f"  no topics matched '{want}'."); return
    _banner(f"FRESHNESS — {len(topics)} topic(s), {window:.0f}s sample window")

    # snapshot 1: log-end per (topic,partition)
    def snap():
        out = {}
        for t in topics:
            ts = [x for x in md["topics"] if x["topic"] == t]
            if not ts: continue
            for p in ts[0]["partitions"]:
                pid = p["partition"]
                le = list_offsets(host, port, a.timeout, a.client_id, a.tls, t, pid, -1)
                out[(t, pid)] = le or 0
        return out
    s1 = snap()
    time.sleep(window)
    s2 = snap()
    now_ms = int(time.time() * 1000)

    rows = []
    for t in topics:
        parts = [k for k in s1 if k[0] == t]
        produced = sum(max(0, s2[k] - s1[k]) for k in parts)
        rate = produced / window
        # newest message age: check the partition with the highest log-end
        le_by_part = {k[1]: s2[k] for k in parts}
        hot = max(le_by_part, key=le_by_part.get) if le_by_part else 0
        nts = fetch_newest_timestamp(host, port, a.timeout, a.client_id, a.tls, t, hot, le_by_part.get(hot, 0))
        age = (now_ms - nts) if nts else None
        rows.append((t, rate, produced, age))

    stale_ms = int(getattr(a, "stale", 900.0) * 1000)  # default 15m tolerance
    for t, rate, produced, age in rows:
        if rate > 0:
            sev = "OK"; note = "  (actively producing)"
        elif age is None:
            sev = "WARN"; note = "  (empty or unreadable)"
        elif age < stale_ms:
            sev = "OK"; note = "  (idle in window but recent — normal for batchy producers)"
        else:
            sev = "FAIL"; note = "  <== STALE (no data for > threshold; producer likely dead/stalled)"
        emit(sev, "freshness",
             f"{t}: {rate:.2f} rec/s, newest {_fmt_age(age)} old{note}")
    if a.json:
        print(json.dumps({"window_s": window,
                          "topics": [{"topic": t, "rate_per_s": round(r,3),
                                      "produced_in_window": p,
                                      "newest_age_ms": age} for t,r,p,age in rows]}, indent=2))
        return
    print()
    print(f"  {'topic':40} {'rec/s':>10} {'in window':>12} {'newest age':>14}")
    for t, rate, produced, age in rows:
        print(f"  {t:40} {rate:>10.2f} {produced:>12} {_fmt_age(age):>14}")
    dead = [t for t,r,p,age in rows if r == 0 and (age is not None and age >= stale_ms)]
    if dead:
        print(f"\n  {len(dead)} topic(s) with NO fresh data: {', '.join(dead)}")
        print("  -> check the PRODUCER (is the source process/exporter running?), not the consumer.")
    else:
        print("\n  all selected topics are receiving data or were written recently.")


def list_groups(host, port, timeout, client_id, use_tls):
    """ListGroups v0. Returns list of (group_id, protocol_type) or []. Note: v0 asks
       the contacted broker for the groups it coordinates; in a single-broker lab that
       is all of them. For multi-broker clusters this is best-effort per-broker."""
    s = open_socket(host, port, timeout, use_tls)
    try:
        _send_request(s, _req_header(LIST_GROUPS, 0, 41, client_id))
        r = Reader(_recv_response(s))
        r.i32()                 # corr
        err = r.i16()
        n = r.i32()
        out = []
        for _ in range(n):
            gid = r.string(); pt = r.string()
            out.append((gid, pt))
        return out if err == 0 else []
    finally:
        s.close()


def describe_groups(host, port, timeout, client_id, use_tls, group_ids):
    """DescribeGroups v0. Returns {group_id: {state, protocol, members:[member_id,...]}}."""
    s = open_socket(host, port, timeout, use_tls)
    try:
        hdr = _req_header(DESCRIBE_GROUPS, 0, 42, client_id)
        body = struct.pack('>i', len(group_ids))
        for g in group_ids:
            body += _enc_str(g)
        _send_request(s, hdr + body)
        r = Reader(_recv_response(s))
        r.i32()                 # corr
        n = r.i32()
        out = {}
        for _ in range(n):
            err = r.i16(); gid = r.string(); state = r.string()
            proto_type = r.string(); proto = r.string()
            mcount = r.i32(); members = []
            for _m in range(mcount):
                mid = r.string(); cid = r.string(); chost = r.string()
                meta_len = r.i32();  r.i = r.i + max(0, meta_len)   # skip member metadata
                asg_len = r.i32();   r.i = r.i + max(0, asg_len)    # skip assignment
                members.append((mid, cid, chost))
            out[gid] = {"state": state, "protocol": proto, "members": members, "error": err}
        return out
    finally:
        s.close()


def cmd_groups(a):
    """List consumer groups and their state (Stable/Empty/PreparingRebalance/Dead)
       without needing to know the group name. 'Empty' with committed offsets means
       the CONSUMER PROCESS is down (not lagging) — a different fix than lag."""
    boot = a.bootstrap.split(',')
    host, port = boot[0].split(':'); port = int(port)
    groups = list_groups(host, port, a.timeout, a.client_id, a.tls)
    _banner(f"GROUPS — {len(groups)} consumer group(s)")
    if not groups:
        print("  no consumer groups found on this broker.")
        print("  (single-broker lab: this is all groups. multi-broker: run per broker.)")
        return
    gids = [g for g, _pt in groups]
    desc = describe_groups(host, port, a.timeout, a.client_id, a.tls, gids)
    rows = []
    for gid, pt in sorted(groups):
        d = desc.get(gid, {})
        state = d.get("state", "?"); members = d.get("members", [])
        rows.append((gid, state, len(members), pt or "-"))
    for gid, state, nmem, pt in rows:
        if state == "Stable" and nmem > 0:
            sev = "OK";   hint = ""
        elif state == "Empty":
            sev = "WARN"; hint = "  <== no active members (consumer process likely DOWN)"
        elif state == "Dead":
            sev = "FAIL"; hint = "  <== group is DEAD"
        elif state in ("PreparingRebalance", "CompletingRebalance"):
            sev = "WARN"; hint = "  (rebalancing)"
        else:
            sev = "WARN"; hint = ""
        emit(sev, "groups", f"{gid}: state={state} members={nmem} type={pt}{hint}")
    if a.json:
        print(json.dumps({"groups": [{"group": g, "state": s, "members": m, "type": t}
                                     for g,s,m,t in rows]}, indent=2)); return
    print(f"\n  {'group':40} {'state':22} {'members':>8}  type")
    for gid, state, nmem, pt in rows:
        print(f"  {gid:40} {state:22} {nmem:>8}  {pt}")
    empties = [g for g,s,m,t in rows if s == "Empty"]
    if empties:
        print(f"\n  {len(empties)} group(s) EMPTY (committed offsets but no live consumer): {', '.join(empties)}")
        print("  -> the consumer process is down. Use 'lag --group <g>' to see how far behind it will be.")


def cmd_poison(a):
    """Scan a topic for 'poison' records: values that fail to parse the way the
       downstream consumer expects. Default check is JSON validity (the OMNIS/OCI
       pipeline expects JSON) plus UTF-8 decodability. Reports offset + snippet of
       each bad record so you can pinpoint a stall like a stray 'hs-test' string."""
    import json as _json
    boot = a.bootstrap.split(',')
    host, port = boot[0].split(':'); port = int(port)
    expect = getattr(a, "expect", "json")
    av = api_versions(host, port, a.timeout, a.client_id, a.tls)
    mver = 2 if av["max_metadata_version"] >= 2 else 1
    md = metadata(host, port, a.timeout, a.client_id, a.tls, topic=a.topic, mver=mver)
    ts = [t for t in md["topics"] if t["topic"] == a.topic]
    if not ts or ts[0]["error"] != 0:
        _banner(f"POISON — {a.topic}")
        print(f"  topic '{a.topic}' not found or error."); return
    parts = [p["partition"] for p in ts[0]["partitions"]]
    _banner(f"POISON SCAN — {a.topic}  (expect={expect}, scanning up to --max {a.max} records)")
    scanned = 0; bad = 0
    for pid in sorted(parts):
        earliest = list_offsets(host, port, a.timeout, a.client_id, a.tls, a.topic, pid, -2) or 0
        latest   = list_offsets(host, port, a.timeout, a.client_id, a.tls, a.topic, pid, -1) or 0
        if latest <= earliest:
            continue
        start = max(earliest, latest - a.max) if a.latest else earliest
        try:
            recs = fetch(host, port, a.timeout, a.client_id, a.tls, a.topic, pid, start)
        except _MissingCodecLib as e:
            print(f"  p{pid}: {e.codec.upper()}-compressed; install to scan: pip install {e.pip_name}")
            continue
        for off, key, val in recs:
            if scanned >= a.max:
                break
            scanned += 1
            problem = None
            if val is None:
                problem = "null value"
            else:
                try:
                    txt = val.decode("utf-8")
                except UnicodeDecodeError:
                    problem = "not valid UTF-8"
                else:
                    if expect == "json":
                        s = txt.strip()
                        try:
                            _json.loads(s)
                        except Exception as je:
                            snippet = s[:60].replace("\n", " ")
                            problem = f"not valid JSON ({je.__class__.__name__}); starts: {snippet!r}"
            if problem:
                bad += 1
                emit("FAIL", "poison", f"{a.topic}[{pid}] off{off}: {problem}")
                print(f"     off{off}: POISON — {problem}")
        if scanned >= a.max:
            break
    if a.json:
        print(json.dumps({"topic": a.topic, "scanned": scanned, "bad": bad}, indent=2)); return
    print(f"\n  scanned {scanned} record(s); {bad} poison record(s) found.")
    if bad == 0:
        print("  no malformed records in the scanned range — a stall is NOT due to a poison message here.")
    else:
        print("  a downstream consumer that hard-fails on parse errors will STALL at the first bad offset.")
        print("  fix: fix/skip the producer of the bad record, or reset the consumer past that offset.")


def cmd_trace(a):
    """'Data was written but isn't in the topic' — walk the decision tree and name
       the cause. Read-only. Tuned for a known producer (e.g. the AI Streamer on .31
       exporting to OMNIS.*): distinguishes never-accepted / wrong-place / aged-out /
       present-but-unreadable, per topic."""
    boot = a.bootstrap.split(',')
    host, port = boot[0].split(':'); port = int(port)
    av = api_versions(host, port, a.timeout, a.client_id, a.tls)
    mver = 2 if av["max_metadata_version"] >= 2 else 1
    md = metadata(host, port, a.timeout, a.client_id, a.tls, topic=a.topic, mver=mver)
    ts = [t for t in md["topics"] if t["topic"] == a.topic]
    _banner(f"TRACE — where did writes to '{a.topic}' go?")

    # ---- branch A: does the topic even exist? ----
    if not ts or ts[0]["error"] != 0:
        err = ts[0]["error"] if ts else "missing"
        emit("FAIL", "trace", f"topic '{a.topic}' does not exist (error {err})")
        print(f"  [A] topic '{a.topic}' DOES NOT EXIST on this cluster (error {err}).")
        print(f"      -> the producer is writing to a topic name the broker doesn't have,")
        print(f"         OR auto-create is off and nothing created it. Check the exact topic")
        print(f"         string the Streamer exports to, and that bootstrap {host}:{port} is")
        print(f"         the SAME cluster the producer targets (advertised-listener mismatch).")
        print(f"      -> run: kafka_doctor.py connect --bootstrap {host}:{port}")
        return

    parts = ts[0]["partitions"]
    # per-partition earliest/latest/age
    total = 0; per = []
    now_ms = int(time.time() * 1000)
    newest_age = None
    for p in parts:
        pid = p["partition"]
        e = list_offsets(host, port, a.timeout, a.client_id, a.tls, a.topic, pid, -2) or 0
        l = list_offsets(host, port, a.timeout, a.client_id, a.tls, a.topic, pid, -1) or 0
        avail = l - e
        total += avail
        nts = fetch_newest_timestamp(host, port, a.timeout, a.client_id, a.tls, a.topic, pid, l) if avail>0 else None
        age = (now_ms - nts) if nts else None
        if age is not None and (newest_age is None or age < newest_age):
            newest_age = age
        oldest = None
        if avail > 0:
            # age of the EARLIEST retained record (for retention reasoning)
            ots = fetch_newest_timestamp(host, port, a.timeout, a.client_id, a.tls, a.topic, pid, e+1) if False else None
        per.append((pid, e, l, avail, age))
    print(f"  [B] partition offsets ({len(parts)} partition(s)):")
    for pid, e, l, avail, age in per:
        amark = f"newest {_fmt_age(age)} old" if age is not None else "empty"
        print(f"        p{pid}: earliest={e} latest={l} records={avail}  ({amark})")

    # ---- topic config: retention + cleanup policy ----
    keys = ["retention.ms","retention.bytes","cleanup.policy","max.message.bytes","compression.type"]
    try:
        _, _, cfg = describe_configs(host, port, a.timeout, a.client_id, a.tls,
                                     RESOURCE_TOPIC, a.topic, keys=keys)
    except Exception:
        cfg = {}
    ret_ms = cfg.get("retention.ms"); policy = cfg.get("cleanup.policy")

    # ---- verdict ----
    print()
    if total == 0:
        emit("WARN", "trace", f"'{a.topic}' is EMPTY on every partition")
        print(f"  [C] VERDICT: topic exists but is EMPTY (earliest == latest everywhere).")
        print(f"      Either nothing was ever accepted, or everything aged out. Distinguish:")
        if ret_ms and ret_ms.isdigit():
            hrs = int(ret_ms)/3600000.0
            print(f"        - retention.ms={ret_ms} (~{hrs:.1f}h): if the producer writes in")
            print(f"          bursts spaced further apart than this, records expire between writes.")
        print(f"        - if the PRODUCER (Streamer on .31) reports success but this stays empty,")
        print(f"          the write is going elsewhere: wrong cluster/bootstrap, or a rejected")
        print(f"          batch the producer didn't surface (acks=0, or MESSAGE_TOO_LARGE).")
        print(f"      -> confirm the producer target and run 'freshness' while it writes.")
        return

    # topic HAS data — is it fresh or stale?
    stale_ms = int(getattr(a,"stale",900.0)*1000)
    if newest_age is not None and newest_age >= stale_ms:
        emit("FAIL", "trace", f"'{a.topic}' has data but newest is {_fmt_age(newest_age)} old (STALE)")
        print(f"  [C] VERDICT: topic HAS data, but the newest record is {_fmt_age(newest_age)} old.")
        print(f"      New writes are NOT arriving. The records you expect were never accepted here.")
        print(f"      Most likely (AI Streamer case): this specific export STREAM stopped on the")
        print(f"      producer while others kept flowing. Compare against a live topic:")
        print(f"        kafka_doctor.py freshness --bootstrap {host}:{port} --topic 'OMNIS.*'")
        print(f"      If netops/applications are fresh but THIS is stale, it's a per-use-case")
        print(f"      producer stop on .31 (restart/relearn that export), not a broker problem.")
        if policy and "compact" in policy:
            print(f"      NOTE: cleanup.policy={policy} — older keys are compacted away; only the")
            print(f"            latest value per key survives, which can look like 'missing' records.")
        return

    # fresh data present — the write DID land. So 'not appearing' is a READ-side issue.
    emit("OK", "trace", f"'{a.topic}' has fresh data (newest {_fmt_age(newest_age)} old) — writes ARE landing")
    print(f"  [C] VERDICT: writes ARE landing — newest record is only {_fmt_age(newest_age)} old,")
    print(f"      {total} record(s) present. If it 'doesn't appear', the problem is on the READ side:")
    print(f"        - wrong PARTITION: your consumer reads a subset; data may be in another partition")
    print(f"          (see the per-partition counts above — is it lopsided?).")
    print(f"        - reading LATEST: a consumer starting at 'latest' skips existing records.")
    print(f"        - POISON ahead of it: a malformed record stalls the consumer before your data.")
    print(f"          run: kafka_doctor.py poison --topic {a.topic}")
    if policy and "compact" in policy:
        print(f"        - cleanup.policy={policy}: compaction keeps only the latest value per key.")
    print(f"        - CODEC: if your consumer can't decode the batch codec it sees nothing;")
    print(f"          this tool's 'consume' decodes gzip/lz4/snappy/zstd — compare what it sees.")
    print(f"      -> verify directly:  kafka_doctor.py consume --topic {a.topic} --max 5")


def cmd_lag(a):
    """Consumer-group lag: committed offset vs log-end per partition. 'Data produced
       but unread' looks like 'transfer failed' from outside but is a different fix."""
    boot = a.bootstrap.split(',')
    host, port = boot[0].split(':'); port = int(port)
    group = a.group
    err, nid, ch, cp = find_coordinator(host, port, a.timeout, a.client_id, a.tls, group)
    if err != 0:
        _banner(f"LAG - group '{group}'")
        emit("FAIL", "coordinator", f"FindCoordinator error {err} for group '{group}' "
             f"({'group unknown / no coordinator' if err in (15,16) else 'error'})")
        print(f"  could not locate group coordinator (error {err}).")
        return
    ch = ch or host; cp = cp if cp > 0 else port
    top_err, offs = offset_fetch(ch, cp, a.timeout, a.client_id, a.tls, group)
    _banner(f"LAG - group '{group}'  (coordinator node{nid} {ch}:{cp})")
    if top_err != 0:
        emit("WARN", "offset_fetch", f"OffsetFetch top-level error {top_err}")
    if not offs:
        emit("WARN", "lag", f"group '{group}' has NO committed offsets (never consumed, or offsets expired)")
        print("  no committed offsets for this group.")
        if a.json: print(json.dumps({"group": group, "partitions": []}, indent=2))
        return
    rows = []
    total_lag = 0
    for t, pid, committed, perr in sorted(offs):
        le = list_offsets(host, port, a.timeout, a.client_id, a.tls, t, pid, -1)
        lag = (le - committed) if (le is not None and committed >= 0) else None
        if lag is not None: total_lag += max(lag, 0)
        rows.append((t, pid, committed, le, lag, perr))
    for t, pid, committed, le, lag, perr in rows:
        if lag is None:
            sev = "WARN"; txt = f"{t}[{pid}] committed={committed} logEnd=? lag=? (no committed offset)"
        elif lag == 0:
            sev = "OK"; txt = f"{t}[{pid}] committed={committed} logEnd={le} lag=0 (caught up)"
        elif lag > 0:
            sev = "WARN" if lag < 100000 else "FAIL"
            txt = f"{t}[{pid}] committed={committed} logEnd={le} LAG={lag}"
        else:
            sev = "OK"; txt = f"{t}[{pid}] committed={committed} logEnd={le} lag={lag}"
        emit(sev, "lag", txt)
    if a.json:
        print(json.dumps({"group": group, "coordinator": f"{ch}:{cp}", "total_lag": total_lag,
                          "partitions": [{"topic": t, "partition": pid, "committed": c,
                                          "log_end": le, "lag": lg} for t,pid,c,le,lg,_ in rows]}, indent=2))
        return
    print(f"  {'topic[part]':45} {'committed':>12} {'logEnd':>12} {'lag':>10}")
    for t, pid, committed, le, lag, perr in rows:
        mark = "" if lag == 0 else ("  <== LAG" if (lag or 0) > 0 else "")
        print(f"  {t+'['+str(pid)+']':45} {committed:>12} {str(le):>12} {str(lag):>10}{mark}")
    print(f"\n  total lag across group: {total_lag}")
    if total_lag == 0:
        print("  group is fully caught up — if data looks 'missing' downstream, it is NOT a consumer-lag issue.")
    else:
        print("  group is BEHIND — data is produced but not yet consumed. 'Missing' data may just be unread.")


def check_produce_version(host, port, timeout, client_id, use_tls):
    """Compare broker's supported Produce (apiKey 0) version range to a modern client's.
       Flags a skew that would cause UNSUPPORTED_VERSION (error 35)."""
    s = open_socket(host, port, timeout, use_tls)
    try:
        _send_request(s, _req_header(API_VERSIONS, 0, 1, client_id))
        r = Reader(_recv_response(s))
        r.i32(); r.i16()                    # corr, error
        cnt = r.i32()
        prod = None
        for _ in range(cnt):
            k = r.i16(); mn = r.i16(); mx = r.i16()
            if k == 0: prod = (mn, mx)
        if not prod:
            return emit("WARN", "produce_version", "broker did not report Produce API support")
        mn, mx = prod
        MODERN = 9   # typical modern client default range top; produce v9+ is flexible
        if mx < 3:
            return emit("WARN", "produce_version",
                f"broker Produce max=v{mx} is very old; modern clients may send higher -> UNSUPPORTED_VERSION(35)")
        return emit("OK", "produce_version",
            f"broker Produce v{mn}-v{mx}; clients negotiate within this range (no skew expected)")
    finally:
        s.close()



# ---------------------------------------------------------------------------
# Phase 6: authentication diagnostics.
#   * TLS cert surfacing (subject/issuer/expiry/self-signed) — read-only.
#   * SASL mechanism discovery (SaslHandshake enabled list) — no credentials.
#   * SASL/PLAIN and SCRAM-SHA-256/512 auth — credentials via getpass prompt or
#     --sasl-pass-env ENVVAR only. A plaintext --sasl-pass is accepted but warns
#     (visible in process list). Credentials are used in-memory only and NEVER
#     logged (the Phase 0 redaction filter covers args/log/byte dumps).
#   * GSSAPI/OAUTHBEARER are reported (mechanism detected) but not performed
#     (needs native libs / token provider beyond stdlib).
# ---------------------------------------------------------------------------
import ssl as _ssl2, hashlib, hmac, base64, os as _os2, getpass as _getpass, datetime as _dt

SASL_HANDSHAKE = 17
SASL_AUTHENTICATE = 36

def tls_certificate(host, port, timeout):
    """Attempt a TLS handshake; if it succeeds, return cert facts. Read-only."""
    ctx = _ssl2.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl2.CERT_NONE
    try:
        raw = socket.create_connection((host, port), timeout=timeout)
        ss = ctx.wrap_socket(raw, server_hostname=host)
        cert = ss.getpeercert(binary_form=False)
        cipher = ss.cipher()
        proto = ss.version()
        ss.close()
        return {"tls": True, "cert": cert, "cipher": cipher, "protocol": proto}
    except _ssl2.SSLError as e:
        return {"tls": False, "reason": f"ssl_error: {getattr(e,'reason',None) or e}"}
    except Exception as e:
        return {"tls": False, "reason": f"{type(e).__name__}: {e}"}

def _cert_findings(info):
    out = []
    if not info.get("tls"):
        out.append(emit("OK", "tls", f"no TLS on this listener ({info.get('reason','plaintext')})"))
        return out
    out.append(emit("OK", "tls", f"TLS OK ({info.get('protocol')}, cipher {info.get('cipher',[None])[0]})"))
    cert = info.get("cert") or {}
    if not cert:
        out.append(emit("WARN", "tls_cert",
            "TLS handshake succeeded but no cert details (verify_mode was NONE / self-signed) — "
            "cannot read subject/expiry without a CA"))
        return out
    subj = dict(x[0] for x in cert.get("subject", []))
    issuer = dict(x[0] for x in cert.get("issuer", []))
    out.append(emit("OK", "tls_subject", f"subject CN={subj.get('commonName')} issuer CN={issuer.get('commonName')}"))
    na = cert.get("notAfter")
    if na:
        try:
            exp = _dt.datetime.strptime(na, "%b %d %H:%M:%S %Y %Z")
            days = (exp - _dt.datetime.utcnow()).days
            if days < 0:
                out.append(emit("FAIL", "tls_expiry", f"certificate EXPIRED {(-days)} day(s) ago ({na}) — TLS handshakes will fail"))
            elif days < 15:
                out.append(emit("WARN", "tls_expiry", f"certificate expires in {days} day(s) ({na}) — renew soon"))
            else:
                out.append(emit("OK", "tls_expiry", f"certificate valid, expires in {days} day(s) ({na})"))
        except Exception:
            out.append(emit("OK", "tls_expiry", f"notAfter={na}"))
    if subj.get("commonName") and issuer.get("commonName") and subj["commonName"] == issuer["commonName"]:
        out.append(emit("WARN", "tls_selfsigned", "certificate appears SELF-SIGNED (subject==issuer) — clients need this CA in truststore"))
    return out

def sasl_mechanisms(host, port, timeout, client_id, use_tls):
    """SaslHandshake v1 with an invalid mechanism -> broker returns enabled list.
       Returns (error_code, [mechanisms]). error 34 => SASL not enabled."""
    s = open_socket(host, port, timeout, use_tls)
    try:
        _send_request(s, _req_header(SASL_HANDSHAKE, 1, 81, client_id) + _enc_str("__probe__"))
        r = Reader(_recv_response(s))
        r.i32()
        err = r.i16()
        n = r.i32()
        mechs = [r.string() for _ in range(n)]
        return err, mechs
    finally:
        s.close()

def _sasl_authenticate_bytes(sock, blob):
    """Send SaslAuthenticate v1 with an opaque auth blob; return (err, errmsg, server_bytes)."""
    hdr = _req_header(SASL_AUTHENTICATE, 1, 91, "kdoctor")
    body = struct.pack('>i', len(blob)) + blob      # auth_bytes: BYTES (int32 len + data)
    _send_request(sock, hdr + body)
    r = Reader(_recv_response(sock))
    r.i32()                                          # corr
    err = r.i16()
    errmsg = r.string()
    blen = r.i32()
    server = r.b[r.i:r.i+blen] if blen > 0 else b''
    return err, errmsg, server

def _do_handshake(sock, mechanism):
    _send_request(sock, _req_header(SASL_HANDSHAKE, 1, 90, "kdoctor") + _enc_str(mechanism))
    r = Reader(_recv_response(sock))
    r.i32(); err = r.i16(); n = r.i32(); mechs = [r.string() for _ in range(n)]
    return err, mechs

def sasl_authenticate(host, port, timeout, use_tls, mechanism, username, password):
    """Perform SASL auth. Supports PLAIN and SCRAM-SHA-256/512 (stdlib crypto).
       Returns (ok: bool, detail: str). Password used in-memory only."""
    mech = mechanism.upper()
    s = open_socket(host, port, timeout, use_tls)
    try:
        herr, mechs = _do_handshake(s, mech)
        if herr != 0:
            return False, f"SaslHandshake rejected mechanism {mech} (err {herr}); broker offers {mechs}"
        if mech == "PLAIN":
            token = b"\x00" + username.encode() + b"\x00" + password.encode()
            err, msg, _ = _sasl_authenticate_bytes(s, token)
            return (err == 0), ("authenticated" if err == 0 else f"auth failed (err {err}: {msg})")
        if mech in ("SCRAM-SHA-256", "SCRAM-SHA-512"):
            digest = hashlib.sha256 if mech.endswith("256") else hashlib.sha512
            return _scram_auth(s, username, password, digest)
        return False, f"mechanism {mech} not supported by kafka_doctor (stdlib does PLAIN/SCRAM only)"
    finally:
        s.close()

def _scram_auth(sock, username, password, digest):
    import secrets
    gs2 = "n,,"
    cnonce = base64.b64encode(secrets.token_bytes(18)).decode()
    client_first_bare = f"n={username},r={cnonce}"
    err, msg, server = _sasl_authenticate_bytes(sock, (gs2 + client_first_bare).encode())
    if err != 0:
        return False, f"SCRAM client-first rejected (err {err}: {msg})"
    sf = server.decode()
    parts = dict(kv.split("=", 1) for kv in sf.split(","))
    rnonce = parts["r"]; salt = base64.b64decode(parts["s"]); iters = int(parts["i"])
    if not rnonce.startswith(cnonce):
        return False, "SCRAM nonce mismatch (possible MITM or server error)"
    salted = hashlib.pbkdf2_hmac(digest().name, password.encode(), salt, iters)
    client_key = hmac.new(salted, b"Client Key", digest).digest()
    stored_key = digest(client_key).digest()
    channel = base64.b64encode(gs2.encode()).decode()
    client_final_noproof = f"c={channel},r={rnonce}"
    auth_msg = f"{client_first_bare},{sf},{client_final_noproof}"
    client_sig = hmac.new(stored_key, auth_msg.encode(), digest).digest()
    proof = base64.b64encode(bytes(a ^ b for a, b in zip(client_key, client_sig))).decode()
    client_final = f"{client_final_noproof},p={proof}"
    err, msg, server2 = _sasl_authenticate_bytes(sock, client_final.encode())
    if err != 0:
        return False, f"SCRAM auth failed (err {err}: {msg}) — bad username/password or mechanism"
    return True, "authenticated (SCRAM)"

def _get_password(a):
    """Resolve password with the safest available source. NEVER returned in logs."""
    envvar = getattr(a, "sasl_pass_env", None)
    if envvar and _os2.environ.get(envvar):
        return _os2.environ[envvar]
    if getattr(a, "sasl_pass", None):
        log.info("WARNING: --sasl-pass given on CLI (visible in process list); prefer --sasl-pass-env")
        return a.sasl_pass
    try:
        return _getpass.getpass("  SASL password (input hidden): ")
    except Exception:
        return None

def tls_certificate_ex(host, port, timeout, cafile=None, certfile=None, keyfile=None, keypass=None):
    """TLS handshake with optional CA validation and client cert (mTLS).
       Returns dict with tls bool, validation verdict, cert facts, and any error.
       When cafile is given, performs real chain+hostname validation and reports
       the verdict. When cert/key given, presents a client cert (mTLS)."""
    result = {"tls": False, "validated": None, "client_cert_sent": bool(certfile)}
    # Pass 1 (optional): strict validation if a CA file was provided
    if cafile:
        try:
            vctx = _ssl2.create_default_context(cafile=cafile)
            vctx.check_hostname = True
            vctx.verify_mode = _ssl2.CERT_REQUIRED
            if certfile:
                vctx.load_cert_chain(certfile, keyfile=keyfile, password=keypass)
            raw = socket.create_connection((host, port), timeout=timeout)
            ss = vctx.wrap_socket(raw, server_hostname=host)
            result["validated"] = True
            result["protocol"] = ss.version(); result["cipher"] = ss.cipher()
            result["cert"] = ss.getpeercert(binary_form=False)
            ss.close()
            result["tls"] = True
            return result
        except _ssl2.SSLCertVerificationError as e:
            result["validated"] = False
            result["validation_error"] = str(e)
            # fall through to permissive read so we can still show WHY it failed
        except _ssl2.SSLError as e:
            result["validated"] = False
            result["validation_error"] = f"ssl_error: {getattr(e,'reason',None) or e}"
        except Exception as e:
            result["validated"] = False
            result["validation_error"] = f"{type(e).__name__}: {e}"
    # Pass 2: permissive handshake to READ the presented cert (and send client cert if mTLS)
    try:
        pctx = _ssl2.create_default_context()
        pctx.check_hostname = False
        pctx.verify_mode = _ssl2.CERT_NONE
        if certfile:
            try:
                pctx.load_cert_chain(certfile, keyfile=keyfile, password=keypass)
            except Exception as e:
                result["client_cert_error"] = f"could not load client cert/key: {type(e).__name__}: {e}"
        raw = socket.create_connection((host, port), timeout=timeout)
        ss = pctx.wrap_socket(raw, server_hostname=host)
        result["tls"] = True
        result["protocol"] = ss.version(); result["cipher"] = ss.cipher()
        result["cert"] = ss.getpeercert(binary_form=False)
        ss.close()
    except _ssl2.SSLError as e:
        reason = getattr(e, "reason", None) or str(e)
        result["reason"] = f"ssl_error: {reason}"
        # a client-auth-required listener rejects a certless handshake here
        if certfile is None and ("CERTIFICATE_REQUIRED" in str(reason) or "peer did not return a certificate" in str(e).lower()
                                 or "alert handshake failure" in str(e).lower()):
            result["client_auth_required"] = True
    except Exception as e:
        result["reason"] = f"{type(e).__name__}: {e}"
    return result

def _cert_findings_ex(info):
    out = []
    if not info.get("tls"):
        if info.get("client_auth_required"):
            out.append(emit("FAIL", "mtls_required",
                "listener requires a CLIENT certificate (mTLS) but none was provided — "
                "pass --cert and --key to authenticate"))
        else:
            out.append(emit("OK", "tls", f"no TLS on this listener ({info.get('reason','plaintext')})"))
        return out
    # validation verdict (only when --cafile was given)
    if info.get("validated") is True:
        out.append(emit("OK", "tls_validation", "certificate chain + hostname VALIDATED against provided CA"))
    elif info.get("validated") is False:
        out.append(emit("FAIL", "tls_validation",
            f"certificate did NOT validate against provided CA: {info.get('validation_error')}"))
    if info.get("client_cert_error"):
        out.append(emit("FAIL", "client_cert", info["client_cert_error"]))
    elif info.get("client_cert_sent"):
        out.append(emit("OK", "client_cert", "client certificate presented (mTLS handshake completed)"))
    out.append(emit("OK", "tls", f"TLS OK ({info.get('protocol')}, cipher {(info.get('cipher') or [None])[0]})"))
    # reuse the standard cert-fact findings
    out.extend(_cert_findings({"tls": True, "cert": info.get("cert"),
                               "protocol": info.get("protocol"), "cipher": info.get("cipher")})[1:])
    return out


def cmd_auth(a):
    """Diagnose the security/auth path: TLS cert facts + SASL mechanism discovery,
       and optionally attempt SASL auth (PLAIN/SCRAM) if --sasl-user is given."""
    boot = a.bootstrap.split(',')
    host, port = boot[0].split(':'); port = int(port)
    _banner(f"AUTH / SECURITY - {host}:{port}")

    # 1) TLS cert facts (+ optional CA validation / client cert for mTLS)
    keypass = None
    kpe = getattr(a, "key_pass_env", None)
    if kpe and _os2.environ.get(kpe):
        keypass = _os2.environ[kpe]
    tinfo = tls_certificate_ex(host, port, a.timeout,
                               cafile=getattr(a, "cafile", None),
                               certfile=getattr(a, "cert", None),
                               keyfile=getattr(a, "key", None),
                               keypass=keypass)
    tfind = _cert_findings_ex(tinfo)
    for sev, key, text in tfind:
        print(f"  {sev_tag(sev)} {key}: {text}")

    # 2) SASL mechanism discovery (no creds). Probe both plaintext and TLS paths.
    err, mechs = None, []
    for use_tls in ((True,) if tinfo.get("tls") else (False, True)):
        try:
            err, mechs = sasl_mechanisms(host, port, a.timeout, a.client_id, use_tls)
            if mechs or err == 0:
                break
        except Exception:
            continue
    if mechs:
        emit("OK", "sasl_mechanisms", f"broker offers SASL: {', '.join(mechs)}")
        print(f"  {sev_tag('OK')} sasl_mechanisms: {', '.join(mechs)}")
        for m in mechs:
            if m in ("GSSAPI", "OAUTHBEARER"):
                emit("INFO", "sasl_unsupported",
                     f"{m} detected — kafka_doctor cannot authenticate {m} (needs "
                     f"{'Kerberos native libs' if m=='GSSAPI' else 'a token provider'}); use a native client")
                print(f"  {sev_tag('INFO')} sasl_unsupported: {m} (not performable by this tool)")
    elif err == 34:
        emit("OK", "sasl", "SASL not enabled on this listener (ILLEGAL_SASL_STATE) — plaintext/no-auth")
        print(f"  {sev_tag('OK')} sasl: not enabled (plaintext/no-auth listener)")
    else:
        emit("INFO", "sasl", f"no SASL mechanisms reported (err={err})")
        print(f"  {sev_tag('INFO')} sasl: none reported (err={err})")

    # 3) optional SASL auth attempt
    user = getattr(a, "sasl_user", None)
    mech = getattr(a, "sasl_mechanism", None)
    if user and mech:
        pw = _get_password(a)
        if not pw:
            emit("WARN", "sasl_auth", "no password supplied; skipping auth attempt")
            print(f"  {sev_tag('WARN')} sasl_auth: no password supplied")
        else:
            use_tls = bool(tinfo.get("tls"))
            try:
                ok, detail = sasl_authenticate(host, port, a.timeout, use_tls, mech, user, pw)
                sev = "OK" if ok else "FAIL"
                # detail never contains the password
                emit(sev, "sasl_auth", f"{mech} as '{user}': {detail}")
                print(f"  {sev_tag(sev)} sasl_auth: {mech} as '{user}': {detail}")
            except Exception as e:
                emit("FAIL", "sasl_auth", f"{mech} attempt errored: {type(e).__name__}: {e}")
                print(f"  {sev_tag('FAIL')} sasl_auth: {type(e).__name__}: {e}")
            finally:
                del pw
    if a.json:
        # cert dict may contain no secrets; auth result already emitted (redacted)
        print(json.dumps({"tls": tinfo.get("tls"), "sasl_mechanisms": mechs}, indent=2))


def re_match_protected(topic):
    import re
    return bool(re.match(r"OMNIS\.", topic))


def _banner(t):
    print("\n" + "="*70 + f"\n {t}\n" + "="*70)

def _fail(a, msg, guidance=None):
    if a.json:
        print(json.dumps({"error": msg, "guidance": guidance}, indent=2))
    else:
        print(f"ERROR: {msg}")
        if guidance: print(f"guidance: {guidance}")

def write_default_config():
    """Write a default kafka_doctor.json next to the script. Returns the path."""
    try:
        d = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        d = os.getcwd()
    path = os.path.join(d, "kafka_doctor.json")
    default = {
        "bootstrap": "192.168.30.32:9092",
        "tls": False,
        "timeout": 5.0,
        "client_id": "kafka-doctor",
        "verbose": 0,
        "log_file": None,
        "protect": "OMNIS\\."
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(default, f, indent=2)
        f.write("\n")
    return path


def main():
    # No arguments  ->  friendly interactive mode.
    if len(sys.argv) == 1:
        return interactive()
    ap = argparse.ArgumentParser(description="Kafka broker discovery & connectivity doctor")
    sub = ap.add_subparsers(dest="cmd", required=True)
    def common(p):
        p.add_argument("--bootstrap", default=None, help="host:port[,host:port,...] (or via --config)")
        p.add_argument("--timeout", type=float, default=5.0, help="socket timeout seconds")
        p.add_argument("--client-id", default="kafka-doctor", help="Kafka client id")
        p.add_argument("--tls", action="store_true", help="wrap socket in TLS")
        p.add_argument("--json", action="store_true", help="machine-readable output")
        p.add_argument("-v", "--verbose", action="count", default=0,
                       help="-v wire summary, -vv full decode (to stderr)")
        p.add_argument("--log-file", default=None,
                       help="write a full redacted run log to this file")
        p.add_argument("--config", default=None,
                       help="JSON config file for defaults (bootstrap, tls, timeout, "
                            "client-id, verbose, log-file, protect); auto-loads "
                            "kafka_doctor.json next to the script, then ~/.kafka_doctor.json")
        p.add_argument("--write-config", action="store_true",
                       help="write a default kafka_doctor.json next to the script and exit")
        p.add_argument("--watch", type=float, default=0, metavar="SECONDS",
                       help="repeat this command every SECONDS until Ctrl-C (great for freshness/lag/groups)")
    for name in ("discover", "health", "connect"):
        common(sub.add_parser(name))
    pcfg = sub.add_parser("config"); common(pcfg)
    pcfg.add_argument("--topic", default=None, help="also show this topic's configs")
    pcod = sub.add_parser("codec"); common(pcod)
    pcod.add_argument("--topic", default="__kdoctor_codec_test", help="throwaway topic to test in")
    pcod.add_argument("--codecs", default="none,gzip,lz4,snappy,zstd", help="comma list of codecs")
    pcod.add_argument("--protect", default=r"OMNIS\.", help="regex of protected topics")
    pcod.add_argument("--force", action="store_true", help="allow producing to a protected topic")
    psz = sub.add_parser("size"); common(psz)
    psz.add_argument("--topic", default="__kdoctor_size_test", help="throwaway topic to probe in")
    psz.add_argument("--protect", default=r"OMNIS\.", help="regex of protected topics")
    psz.add_argument("--force", action="store_true", help="allow producing to a protected topic")
    plag = sub.add_parser("lag"); common(plag)
    plag.add_argument("--group", required=True, help="consumer group id")
    pfr = sub.add_parser("freshness"); common(pfr)
    pfr.add_argument("--topic", default=None, help="topic or glob (e.g. 'OMNIS.*'); default: all non-internal")
    pfr.add_argument("--window", type=float, default=5.0, help="sample window seconds (default 5)")
    pfr.add_argument("--stale", type=float, default=900.0,
                     help="seconds since newest message before a topic is flagged STALE (default 900 = 15m)")
    common(sub.add_parser("groups"))
    ppo = sub.add_parser("poison"); common(ppo); ppo.add_argument("--topic", required=True)
    ppo.add_argument("--max", type=int, default=500, help="max records to scan (default 500)")
    ppo.add_argument("--expect", default="json", choices=["json", "utf8"],
                     help="what a valid record should be (default json)")
    ppo.add_argument("--latest", action="store_true", help="scan the tail instead of from earliest")
    ptr = sub.add_parser("trace"); common(ptr); ptr.add_argument("--topic", required=True)
    ptr.add_argument("--stale", type=float, default=900.0,
                     help="seconds since newest record before 'stale' (default 900=15m)")
    pau = sub.add_parser("auth"); common(pau)
    pau.add_argument("--sasl-mechanism", default=None,
                     help="PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 (to attempt auth)")
    pau.add_argument("--sasl-user", default=None, help="SASL username (to attempt auth)")
    pau.add_argument("--sasl-pass-env", default=None,
                     help="ENV VAR holding the SASL password (preferred over --sasl-pass)")
    pau.add_argument("--sasl-pass", default=None,
                     help="SASL password (DISCOURAGED: visible in process list; use --sasl-pass-env or prompt)")
    pau.add_argument("--cafile", default=None,
                     help="CA bundle to VALIDATE the broker cert chain + hostname")
    pau.add_argument("--cert", default=None, help="client certificate (PEM) for mTLS listeners")
    pau.add_argument("--key", default=None, help="client private key (PEM) for mTLS")
    pau.add_argument("--key-pass-env", default=None,
                     help="ENV VAR holding the client key password (never plaintext on CLI)")
    pt = sub.add_parser("topic"); common(pt); pt.add_argument("--topic", required=True)
    pc = sub.add_parser("consume"); common(pc); pc.add_argument("--topic", required=True)
    pc.add_argument("--max", type=int, default=10, help="max records to print")
    pc.add_argument("--width", type=int, default=200, help="max bytes of each value to show")
    pc.add_argument("--latest", action="store_true", help="read the tail instead of from earliest")
    pp = sub.add_parser("produce-test"); common(pp)
    pp.add_argument("--topic", default="__kafka_doctor_test")
    pp.add_argument("--protect", default=r"OMNIS\.", help="regex of protected topic prefixes")
    pp.add_argument("--force", action="store_true", help="allow producing to a protected topic")
    ps = sub.add_parser("simulate"); common(ps)
    ps.add_argument("--topic", default="OMNIS.assets-test", help="target test topic")
    ps.add_argument("--count", type=int, default=50, help="number of records to send")
    ps.add_argument("--rate", type=float, default=0, help="records/sec (0 = as fast as possible)")
    ps.add_argument("--force", action="store_true", help="allow a non -test / OMNIS.* topic")
    a = ap.parse_args()
    if getattr(a, "write_config", False):
        p = write_default_config()
        print(f"wrote default config: {p}")
        print("edit it (bootstrap/tls/protect); set \"log_file\": \"none\" to disable the run log file.")
        sys.exit(0)
    a = apply_config_file(a)
    init_logging(a)
    _cfgsrc = getattr(a, "config", None) or _auto_config_path()
    if _cfgsrc:
        log.info("config file in effect: %s", _cfgsrc)
    if not getattr(a, "bootstrap", None):
        ap.error("--bootstrap is required (pass it, or set it in a --config file)")
    _dispatch = {"discover": cmd_discover, "health": cmd_health, "topic": cmd_topic,
     "connect": cmd_connect, "consume": cmd_consume, "simulate": cmd_simulate,
     "produce-test": cmd_produce_test, "config": cmd_config,
     "codec": cmd_codec, "size": cmd_size, "lag": cmd_lag,
     "freshness": cmd_freshness, "groups": cmd_groups, "poison": cmd_poison,
     "trace": cmd_trace,
     "auth": cmd_auth}
    fn = _dispatch[a.cmd]
    watch = getattr(a, "watch", 0) or 0
    if watch and watch > 0:
        # write-heavy commands should not be looped automatically
        if a.cmd in ("produce-test", "simulate", "codec", "size"):
            ap.error(f"--watch is not allowed with '{a.cmd}' (it writes to the broker)")
        iteration = 0
        try:
            while True:
                iteration += 1
                stamp = time.strftime("%Y-%m-%d %H:%M:%S")
                print(f"\n===== {a.cmd} @ {stamp}  (watch {watch:g}s, iter {iteration}; Ctrl-C to stop) =====")
                fn(a)
                time.sleep(watch)
        except KeyboardInterrupt:
            print("\n(stopped watching)")
            sys.exit(exit_code())
    else:
        fn(a)
    sys.exit(exit_code())


# ---------------------------------------------------------------------------
# Interactive / guided mode
# ---------------------------------------------------------------------------

class Args:
    """Lightweight stand-in for argparse Namespace so interactive mode can call cmd_*."""
    def __init__(self, **kw):
        self.timeout = 5.0
        self.client_id = "kafka-doctor"
        self.tls = False
        self.json = False
        self.protect = r"OMNIS\."
        self.force = False
        self.max = 10
        self.width = 200
        self.latest = False
        self.count = 50
        self.rate = 0
        self.verbose = 0
        self.log_file = None
        self.topic = None
        self.config = None
        self.codecs = "none,gzip,lz4,snappy,zstd"
        self.group = None
        self.sasl_mechanism = None
        self.sasl_user = None
        self.sasl_pass = None
        self.sasl_pass_env = None
        self.cafile = None
        self.cert = None
        self.key = None
        self.key_pass_env = None
        self.window = 5.0
        self.stale = 900.0
        self.expect = "json"
        self.watch = 0
        for k, v in kw.items():
            setattr(self, k, v)


def _ask(prompt, default=None):
    d = f" [{default}]" if default is not None else ""
    try:
        val = input(f"  {prompt}{d}: ").strip()
    except (EOFError, KeyboardInterrupt):
        print(); sys.exit(0)
    return val if val else (default if default is not None else "")


def _ask_yesno(prompt, default=False):
    d = "Y/n" if default else "y/N"
    v = _ask(f"{prompt} ({d})", "")
    if not v: return default
    return v.lower().startswith("y")


def interactive():
    print("\n" + "="*70)
    print("  KAFKA DOCTOR - interactive mode")
    print("  (press Enter to accept the [default] shown in brackets)")
    print("="*70)

    # optional config file + logging prompts (parity with CLI)
    cfgpath = _ask("config file (blank = none / ~/.kafka_doctor.json)", "")
    filecfg = load_config_file(cfgpath or None)
    verbose = 1 if _ask_yesno("verbose output (show wire log)?", False) else 0
    logf = _ask("write run log to file (blank = none)", filecfg.get("log_file", "") or "")
    init_logging(Args(verbose=verbose, log_file=(logf or None)))
    bootstrap = _ask("Kafka bootstrap (host:port)",
                     filecfg.get("bootstrap", "192.168.30.32:9092"))
    tls = _ask_yesno("Use TLS?", bool(filecfg.get("tls", False)))

    while True:
        print("\n" + "-"*70)
        print("  What do you want to do?")
        print("    1) discover      - what is this broker? (size, cluster id, security, RF)")
        print("    2) health        - security probe + handshake + summary")
        print("    3) connect       - stage-by-stage connectivity troubleshooter")
        print("    4) topic         - inspect one topic (partitions, leader, ISR)")
        print("    5) consume       - READ records from a topic (see what's in it)")
        print("    6) produce-test  - write ONE test record and verify it landed")
        print("    7) simulate      - send AI Streamer-format records to a *-test topic")
        print("    8) config        - broker/topic compression & size limits (+ flags)")
        print("    9) codec         - test each compression codec end-to-end (throwaway topic)")
        print("    10) size         - probe the message-size limit (under/over boundary)")
        print("    11) lag          - consumer-group lag (committed vs log-end)")
        print("    12) trace        - 'I wrote data but it is not in the topic' — find out why")
        print("    13) freshness    - is data flowing NOW? rec/s + newest-message age")
        print("    14) groups       - list consumer groups + state (Empty=consumer down)")
        print("    15) poison       - scan a topic for malformed/non-JSON records")
        print("    16) auth         - TLS cert + SASL mechanism discovery (+ optional auth)")
        print("    17) change broker / TLS")
        print("    18) quit")
        choice = _ask("choose 1-18", "1")

        base = dict(bootstrap=bootstrap, tls=tls, verbose=verbose, log_file=(logf or None))
        try:
            if choice == "1":
                cmd_discover(Args(**base))
            elif choice == "2":
                cmd_health(Args(**base))
            elif choice == "3":
                cmd_connect(Args(**base))
            elif choice == "4":
                t = _ask("topic name", "OMNIS.netops")
                cmd_topic(Args(topic=t, **base))
            elif choice == "5":
                t = _ask("topic to read", "OMNIS.assets-test")
                mx = int(_ask("max records to show", "10"))
                latest = _ask_yesno("read the TAIL (most recent) instead of from start?", False)
                cmd_consume(Args(topic=t, max=mx, latest=latest, width=300, **base))
            elif choice == "6":
                t = _ask("test topic (should NOT be a real OMNIS.* topic)", "kafkadoctor_test")
                force = False
                if re_match_protected(t):
                    force = _ask_yesno(f"'{t}' looks like a real OMNIS topic - really write to it?", False)
                    if not force:
                        print("  cancelled."); continue
                cmd_produce_test(Args(topic=t, force=force, **base))
            elif choice == "7":
                print("  simulate: emits AI Streamer 'assets'/netops-format JSON records.")
                t = _ask("target test topic (must end in -test)", "OMNIS.assets-test")
                n = int(_ask("how many records", "50"))
                rate = float(_ask("records/sec (0 = burst all at once)", "0"))
                force = False
                if not t.endswith("-test") and re_match_protected(t):
                    force = _ask_yesno(f"'{t}' is not a -test topic - force?", False)
                    if not force:
                        print("  cancelled."); continue
                cmd_simulate(Args(topic=t, count=n, rate=rate, force=force, **base))
            elif choice == "8":
                t = _ask("topic (blank = broker only)", "OMNIS.netops")
                cmd_config(Args(topic=(t or None), **base))
            elif choice == "9":
                tt = _ask("throwaway topic for codec test", "__kdoctor_codec_test")
                cmd_codec(Args(topic=tt, codecs="none,gzip,lz4,snappy,zstd",
                               protect=r"OMNIS\.", force=False, **base))
            elif choice == "10":
                tt = _ask("throwaway topic for size probe", "__kdoctor_size_test")
                cmd_size(Args(topic=tt, protect=r"OMNIS\.", force=False, **base))
            elif choice == "11":
                g = _ask("consumer group id", "clickhouse_dns_consumer")
                cmd_lag(Args(group=g, **base))
            elif choice == "12":
                t = _ask("topic that seems to be missing data", "OMNIS.dns")
                cmd_trace(Args(topic=t, stale=900.0, **base))
            elif choice == "13":
                t = _ask("topic or glob (blank = all non-internal)", "OMNIS.*")
                w = float(_ask("sample window seconds", "5"))
                cmd_freshness(Args(topic=(t or None), window=w, stale=900.0, **base))
            elif choice == "14":
                cmd_groups(Args(**base))
            elif choice == "15":
                t = _ask("topic to scan for poison records", "OMNIS.netops")
                mx = int(_ask("max records to scan", "500"))
                latest = _ask_yesno("scan the TAIL instead of from start?", False)
                cmd_poison(Args(topic=t, max=mx, expect="json", latest=latest, **base))
            elif choice == "16":
                do_auth = _ask_yesno("attempt SASL auth (needs username)?", False)
                if do_auth:
                    m = _ask("mechanism (PLAIN/SCRAM-SHA-256/SCRAM-SHA-512)", "PLAIN")
                    u = _ask("SASL username", "")
                    cmd_auth(Args(sasl_mechanism=m, sasl_user=u, sasl_pass=None,
                                  sasl_pass_env=None, **base))
                else:
                    cmd_auth(Args(sasl_mechanism=None, sasl_user=None, sasl_pass=None,
                                  sasl_pass_env=None, **base))
            elif choice == "17":
                bootstrap = _ask("Kafka bootstrap (host:port)", bootstrap)
                tls = _ask_yesno("Use TLS?", tls)
            elif choice in ("18", "q", "quit", "exit"):
                print("  bye.\n"); return
            else:
                print("  (pick a number 1-18)")
        except Exception as e:
            print(f"\n  ERROR: {type(e).__name__}: {e}")
            print("  tip: run 'connect' (option 3) to localize where the connection breaks.")

        if not _ask_yesno("\nrun another command?", True):
            print("  bye.\n"); return

if __name__ == "__main__":
    main()
