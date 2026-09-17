#!/usr/bin/env python3
"""
kdx.py — extra diagnostics for kafka-doctor-web that encode failure modes seen
in the field. Imports the baked-in kafka_doctor.py and reuses its wire-protocol
primitives (metadata, list_offsets, open_socket) so there is no duplicated
protocol code and no modification to the doctor itself.

Two checks:
  1) topic_variants(): CASE-VARIANT DETECTION. Kafka topic names are
     case-sensitive. A producer writing to 'omnis.dns' while a consumer reads
     'OMNIS.dns' (empty) is a real, time-wasting trap. Given a topic, this lists
     every case-insensitive match on the broker and shows record counts, so you
     can see instantly which case actually holds the data.
  2) connect_probe(): TIMEOUT-vs-REFUSED. A bare TCP failure is ambiguous; the
     distinction is diagnostic — refused = nothing listening; timeout = firewall
     DROP or broken return path (the firewalld / subnet-mask class of problem).
"""
import errno, importlib.util, socket, time

_KD_PATH = "/app/kafka_doctor.py"
_kd = None

def _doctor():
    global _kd
    if _kd is None:
        spec = importlib.util.spec_from_file_location("kafka_doctor", _KD_PATH)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        _kd = m
    return _kd


def _split(bootstrap):
    host, _, port = bootstrap.partition(":")
    return host.strip(), int(port or "9092")


def topic_variants(bootstrap, topic, timeout=8.0, tls=False):
    """List case-insensitive matches of `topic` on the broker, with record
    counts, and flag the case-mismatch trap."""
    kd = _doctor()
    host, port = _split(bootstrap)
    try:
        md = kd.metadata(host, port, timeout, "kdx", tls, topic=None, mver=2)
    except Exception as e:
        return {"ok": False, "error": f"metadata failed: {e}", "queried_topic": topic}

    all_topics = [t.get("topic") for t in md.get("topics", []) if t.get("topic")]
    tl = topic.lower()
    variants = sorted([t for t in all_topics if t.lower() == tl])

    rows = []
    for v in variants:
        # sum records across partitions
        parts = next((t.get("partitions", []) for t in md["topics"] if t.get("topic") == v), [])
        total = 0
        pcount = len(parts)
        ok = True
        for p in parts:
            pid = p.get("partition", p) if isinstance(p, dict) else p
            try:
                e = kd.list_offsets(host, port, timeout, "kdx", tls, v, pid, -2)
                l = kd.list_offsets(host, port, timeout, "kdx", tls, v, pid, -1)
                total += max(0, (l or 0) - (e or 0))
            except Exception:
                ok = False
        rows.append({"topic": v, "partitions": pcount, "records": total,
                     "exact_match": v == topic, "offsets_read": ok})

    result = {"ok": True, "queried_topic": topic,
              "exists_exact": topic in variants,
              "variants": rows}

    # verdict: the trap
    with_data = [r for r in rows if r["records"] > 0]
    queried_row = next((r for r in rows if r["topic"] == topic), None)
    if len(variants) > 1:
        others = [r for r in with_data if r["topic"] != topic]
        if queried_row and queried_row["records"] == 0 and others:
            names = ", ".join(f"{r['topic']} ({r['records']} records)" for r in others)
            result["verdict"] = ("CASE-MISMATCH TRAP: queried topic "
                f"'{topic}' is EMPTY, but a case-variant has data: {names}. "
                "A producer/consumer is very likely using the wrong-case name.")
            result["severity"] = "FAIL"
        elif not queried_row and others:
            names = ", ".join(f"{r['topic']} ({r['records']} records)" for r in others)
            result["verdict"] = ("Queried topic "
                f"'{topic}' does NOT exist, but a case-variant does: {names}. "
                "Check topic-name case in the producer/consumer config.")
            result["severity"] = "FAIL"
        else:
            result["verdict"] = (f"Multiple case-variants exist: "
                + ", ".join(r["topic"] for r in variants)
                + ". Confirm all producers/consumers use the same case.")
            result["severity"] = "WARN"
    elif not variants:
        result["verdict"] = (f"No topic matching '{topic}' (any case) exists on this broker.")
        result["severity"] = "INFO"
    else:
        result["verdict"] = f"Only one topic named '{variants[0]}' exists — no case-variant trap."
        result["severity"] = "OK"
    return result


def connect_probe(bootstrap, timeout=6.0):
    """Raw TCP probe that classifies the failure: refused vs timeout vs other.
    This distinction points at the fix (nothing-listening vs firewall/return-path)."""
    host, port = _split(bootstrap)
    started = time.time()
    # resolve first
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        resolved = infos[0][4][0] if infos else None
    except Exception as e:
        return {"ok": False, "stage": "dns", "bootstrap": bootstrap,
                "verdict": f"DNS resolution failed for '{host}': {e}"}

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        dur = round(time.time() - started, 3)
        s.close()
        return {"ok": True, "bootstrap": bootstrap, "resolved": resolved,
                "duration_s": dur,
                "verdict": f"TCP connect to {host}:{port} succeeded ({dur}s). "
                           "Port is open and reachable from this host."}
    except socket.timeout:
        dur = round(time.time() - started, 3)
        return {"ok": False, "bootstrap": bootstrap, "resolved": resolved,
                "failure": "timeout", "duration_s": dur,
                "verdict": (f"TCP connect to {host}:{port} TIMED OUT after {dur}s. "
                    "Timeout (not refused) means the SYN is being silently dropped: "
                    "a firewall DROP rule (host firewalld/DOCKER-USER, or a firewall "
                    "in the path), or a broken return path (e.g. wrong subnet mask / "
                    "asymmetric route on the broker). The broker process being down "
                    "also times out. NOT a 'nothing listening' case — that would refuse.")}
    except ConnectionRefusedError:
        dur = round(time.time() - started, 3)
        return {"ok": False, "bootstrap": bootstrap, "resolved": resolved,
                "failure": "refused", "duration_s": dur,
                "verdict": (f"TCP connect to {host}:{port} was REFUSED ({dur}s). "
                    "Refused (RST) means the host is reachable but NOTHING is "
                    "listening on that port: broker not running, bound to a different "
                    "interface (e.g. 127.0.0.1 only), or the wrong port.")}
    except OSError as e:
        dur = round(time.time() - started, 3)
        name = errno.errorcode.get(e.errno, str(e.errno))
        return {"ok": False, "bootstrap": bootstrap, "resolved": resolved,
                "failure": name, "duration_s": dur,
                "verdict": f"TCP connect to {host}:{port} failed: {e} ({name})."}
    finally:
        try: s.close()
        except Exception: pass
