#!/usr/bin/env python3
"""kdproxy.py — Kafka-aware TCP relay for kafka-doctor-web (flexible v9+ aware)."""
import socket, struct, threading, time, collections, os

API_NAMES = {0:"Produce",1:"Fetch",2:"ListOffsets",3:"Metadata",8:"OffsetCommit",
             9:"OffsetFetch",10:"FindCoordinator",11:"JoinGroup",12:"Heartbeat",
             18:"ApiVersions",19:"CreateTopics"}
ERR = {0:"NONE",-1:"UNKNOWN",3:"UNKNOWN_TOPIC_OR_PARTITION",5:"LEADER_NOT_AVAILABLE",
       6:"NOT_LEADER_OR_FOLLOWER",7:"REQUEST_TIMED_OUT",9:"REPLICA_NOT_AVAILABLE",
       10:"MESSAGE_TOO_LARGE",13:"NETWORK_EXCEPTION",15:"COORDINATOR_NOT_AVAILABLE",
       37:"INVALID_PARTITIONS",56:"KAFKA_STORAGE_ERROR",75:"UNKNOWN_TOPIC_ID"}

def _is_flex_md(ver): return ver >= 9
def _is_flex_prod(ver): return ver >= 9

def _read_string(b, i):
    (ln,) = struct.unpack_from(">h", b, i); i += 2
    if ln < 0: return None, i
    return b[i:i+ln].decode("utf-8","replace"), i+ln

def _read_uvarint(b, i):
    val = 0; shift = 0
    while True:
        byte = b[i]; i += 1
        val |= (byte & 0x7F) << shift
        if not (byte & 0x80): break
        shift += 7
    return val, i

def _write_uvarint(n):
    out = bytearray()
    while True:
        x = n & 0x7F; n >>= 7
        if n: out.append(x | 0x80)
        else: out.append(x); break
    return bytes(out)

def _read_compact_string(b, i):
    ln, i = _read_uvarint(b, i)
    if ln == 0: return None, i
    ln -= 1
    return b[i:i+ln].decode("utf-8","replace"), i+ln

def _skip_tagged_fields(b, i):
    cnt, i = _read_uvarint(b, i)
    for _ in range(cnt):
        _tag, i = _read_uvarint(b, i)
        size, i = _read_uvarint(b, i)
        i += size
    return i


class _Frames:
    def __init__(self): self.buf = b""
    def feed(self, data):
        self.buf += data; out = []
        while len(self.buf) >= 4:
            (n,) = struct.unpack(">i", self.buf[:4])
            if n < 0 or n > 100_000_000:
                out.append(("RAW", self.buf)); self.buf = b""; break
            if len(self.buf) < 4 + n: break
            out.append(("FRAME", self.buf[4:4+n])); self.buf = self.buf[4+n:]
        return out


class Proxy:
    def __init__(self, listen_port, upstream_host, upstream_port, advertise_host, log_size=800):
        self.listen_port = int(listen_port)
        self.up_host = upstream_host; self.up_port = int(upstream_port)
        self.adv_host = advertise_host
        self.events = collections.deque(maxlen=log_size)
        self.lock = threading.Lock()
        self.logfile = os.environ.get("KD_PROXY_LOG", "/data/proxy-feed.log")
        self._srv = None; self._thread = None
        self.running = False; self.started_at = None; self.conns = 0
        self.pending = {}
        self.stats = {}   # topic -> {produce_req, produce_ok, produce_err, bytes, last_err, last_t}

    def _bump(self, topic, field, n=1):
        d = self.stats.setdefault(topic, {"produce_req":0,"produce_ok":0,"produce_err":0,
                                          "bytes":0,"errors":{},"last_t":0})
        d[field] = d.get(field,0) + n
        d["last_t"] = round(time.time(),3)

    def log(self, direction, kind, detail):
        ev = {"t": round(time.time(),3), "dir": direction, "kind": kind, "detail": detail}
        with self.lock:
            self.events.append(ev)
        try:
            os.makedirs(os.path.dirname(self.logfile), exist_ok=True)
            with open(self.logfile, "a") as f:
                f.write(f"{ev['t']}  {ev['dir']:4} {ev['kind']:14} {ev['detail']}\n")
        except Exception:
            pass  # persistence must never break the relay

    def snapshot(self, since=0):
        with self.lock: evs = list(self.events)
        return [e for e in evs if e["t"] > since]

    def _decode_request(self, frame, client):
        try:
            api, ver, corr = struct.unpack_from(">hhi", frame, 0)
            i = 8
            cid, i = _read_string(frame, i)
            name = API_NAMES.get(api, f"api{api}")
            self.pending[corr] = (name, ver, time.time(), client)
            detail = f"{name} v{ver} corr={corr} client={cid or '-'} ({len(frame)}B)"
            if api == 0:
                pinfo = self._peek_produce_req(frame, i, ver)
                detail += pinfo
                import re as _re
                m = _re.search(r"first='([^']*)'", pinfo)
                if m:
                    self._bump(m.group(1), "produce_req"); self._bump(m.group(1), "bytes", len(frame))
            self.log("C\u2192B", name, detail)
        except Exception as e:
            self.log("C\u2192B", "req", f"(undecoded {len(frame)}B: {e})")

    def _peek_produce_req(self, b, i, ver):
        try:
            j = i
            if _is_flex_prod(ver):
                j = _skip_tagged_fields(b, j)
                if ver >= 3:
                    _tx, j = _read_compact_string(b, j)
                acks, = struct.unpack_from(">h", b, j); j += 2
                _tmo, = struct.unpack_from(">i", b, j); j += 4
                ntop, j = _read_uvarint(b, j); ntop -= 1
                if ntop > 0:
                    tname, j = _read_compact_string(b, j)
                    return f"  acks={acks} topics={ntop} first='{tname}'"
            else:
                if ver >= 3:
                    _tx, j = _read_string(b, j)
                acks, = struct.unpack_from(">h", b, j); j += 2
                _tmo, = struct.unpack_from(">i", b, j); j += 4
                ntop, = struct.unpack_from(">i", b, j); j += 4
                if ntop > 0:
                    tname, j = _read_string(b, j)
                    return f"  acks={acks} topics={ntop} first='{tname}'"
        except Exception:
            pass
        return ""

    def _decode_response(self, frame, client):
        try:
            corr, = struct.unpack_from(">i", frame, 0)
            meta = self.pending.pop(corr, None)
            if not meta:
                self.log("B\u2192C","resp",f"corr={corr} ({len(frame)}B) [no matching req]"); return
            name, ver, t0, _c = meta
            lat = round((time.time()-t0)*1000,1)
            detail = f"{name} v{ver} corr={corr} {lat}ms ({len(frame)}B)"
            if name == "Metadata":
                detail += self._peek_metadata(frame, ver)
            elif name == "Produce":
                pr = self._peek_produce_resp(frame, ver)
                detail += pr
                import re as _re
                mt = _re.search(r"topic='([^']*)'", pr); me = _re.search(r"err=(\S+)", pr)
                if mt:
                    t = mt.group(1); e = me.group(1) if me else "?"
                    if e == "NONE": self._bump(t, "produce_ok")
                    else:
                        self._bump(t, "produce_err")
                        d = self.stats.setdefault(t, {}); d.setdefault("errors",{}); d["errors"][e]=d["errors"].get(e,0)+1
            self.log("B\u2192C", name, detail)
        except Exception as e:
            self.log("B\u2192C","resp",f"(undecoded {len(frame)}B: {e})")

    def _peek_metadata(self, b, ver):
        try:
            i = 4
            if _is_flex_md(ver):
                i = _skip_tagged_fields(b, i)
                if ver >= 3: i += 4                      # throttle
                nb, i = _read_uvarint(b, i); nb -= 1     # brokers (compact array)
                adv = []
                for _ in range(nb):
                    _nid, = struct.unpack_from(">i", b, i); i += 4
                    host, i = _read_compact_string(b, i)
                    port, = struct.unpack_from(">i", b, i); i += 4
                    _rack, i = _read_compact_string(b, i)
                    i = _skip_tagged_fields(b, i)
                    adv.append(f"{host}:{port}")
                # cluster_id (compact nullable), controller_id(4)
                _cid, i = _read_compact_string(b, i)
                i += 4
                nt, i = _read_uvarint(b, i); nt -= 1     # topics (compact array)
                topics = []
                for _ in range(nt):
                    terr, = struct.unpack_from(">h", b, i); i += 2
                    tname, i = _read_compact_string(b, i)
                    if ver >= 10: i += 16                 # topic_id (uuid)
                    _internal = b[i]; i += 1
                    npart, i = _read_uvarint(b, i); npart -= 1
                    # skip partitions block (varies); we only need counts here.
                    # Each partition (v9+): err(2) idx(4) leader(4) leader_epoch(4)
                    #   replicas(compact arr of i32) isr(compact) offline(compact) tagged
                    for _ in range(npart):
                        i += 2 + 4 + 4
                        if ver >= 7: i += 4               # leader_epoch
                        for _arr in range(3):
                            an, i = _read_uvarint(b, i); an -= 1
                            i += an * 4
                        i = _skip_tagged_fields(b, i)
                    if ver >= 8: i = _skip_tagged_fields(b, i)  # topic_authorized_operations sometimes; then tagged
                    i = _skip_tagged_fields(b, i)
                    topics.append((tname, npart, terr))
                return self._fmt_md(adv, topics)
            else:
                if ver >= 3: i += 4
                nb, = struct.unpack_from(">i", b, i); i += 4
                adv = []
                for _ in range(nb):
                    _nid, = struct.unpack_from(">i", b, i); i += 4
                    host, i = _read_string(b, i)
                    port, = struct.unpack_from(">i", b, i); i += 4
                    if ver >= 1:
                        _rack, i = _read_string(b, i)
                    adv.append(f"{host}:{port}")
                if ver >= 2:
                    _cid, i = _read_string(b, i)          # cluster_id
                if ver >= 1:
                    i += 4                                # controller_id
                nt, = struct.unpack_from(">i", b, i); i += 4
                topics = []
                for _ in range(nt):
                    terr, = struct.unpack_from(">h", b, i); i += 2
                    tname, i = _read_string(b, i)
                    if ver >= 1: i += 1                   # is_internal
                    npart, = struct.unpack_from(">i", b, i); i += 4
                    for _ in range(npart):
                        i += 2 + 4 + 4                    # err, idx, leader
                        if ver >= 7: i += 4
                        rn, = struct.unpack_from(">i", b, i); i += 4; i += rn*4
                        isn, = struct.unpack_from(">i", b, i); i += 4; i += isn*4
                        if ver >= 5:
                            on, = struct.unpack_from(">i", b, i); i += 4; i += on*4
                    topics.append((tname, npart, terr))
                return self._fmt_md(adv, topics)
        except Exception as e:
            return f"  advertises parsed; topics skipped ({e})"

    def _fmt_md(self, adv, topics):
        out = "  advertises=[" + ", ".join(adv) + "]"
        if topics:
            shown = []
            for (name, np, err) in topics[:12]:
                tag = "" if err == 0 else f"!{ERR.get(err, err)}"
                shown.append(f"{name}({np}p{tag})")
            more = f" +{len(topics)-12} more" if len(topics) > 12 else ""
            out += "  topics=" + str(len(topics)) + " [" + ", ".join(shown) + more + "]"
        return out

    def _peek_produce_resp(self, b, ver):

        try:
            i = 4
            if _is_flex_prod(ver):
                i = _skip_tagged_fields(b, i)
                ntop, i = _read_uvarint(b, i); ntop -= 1
                if ntop > 0:
                    tname, i = _read_compact_string(b, i)
                    nparts, i = _read_uvarint(b, i); nparts -= 1
                    if nparts > 0:
                        pid, = struct.unpack_from(">i", b, i); i += 4
                        ecode, = struct.unpack_from(">h", b, i); i += 2
                        return f"  topic='{tname}' p{pid} err={ERR.get(ecode,ecode)}"
            else:
                ntop, = struct.unpack_from(">i", b, i); i += 4
                if ntop > 0:
                    tname, i = _read_string(b, i)
                    nparts, = struct.unpack_from(">i", b, i); i += 4
                    if nparts > 0:
                        pid, = struct.unpack_from(">i", b, i); i += 4
                        ecode, = struct.unpack_from(">h", b, i); i += 2
                        return f"  topic='{tname}' p{pid} err={ERR.get(ecode,ecode)}"
        except Exception:
            pass
        return ""

    def _rewrite_metadata_adv(self, frame, ver):
        try:
            if _is_flex_md(ver):
                return self._rewrite_flexible(frame, ver)
            return self._rewrite_classic(frame, ver)
        except Exception:
            return frame

    def _rewrite_classic(self, frame, ver):
        i = 4; out = bytearray(frame[:i])
        if ver >= 3: out += frame[i:i+4]; i += 4
        nb, = struct.unpack_from(">i", frame, i); out += frame[i:i+4]; i += 4
        for _ in range(nb):
            out += frame[i:i+4]; i += 4
            ln, = struct.unpack_from(">h", frame, i); i += 2; i += ln
            nh = self.adv_host.encode()
            out += struct.pack(">h", len(nh)) + nh
            i += 4  # skip real port
            out += struct.pack(">i", self.listen_port)  # advertise OUR port
            if ver >= 1:
                rl, = struct.unpack_from(">h", frame, i); i += 2
                if rl < 0: out += struct.pack(">h", -1)
                else: out += struct.pack(">h", rl) + frame[i:i+rl]; i += rl
        out += frame[i:]
        return bytes(out)

    def _rewrite_flexible(self, frame, ver):
        i = 4; out = bytearray(frame[:i])
        start = i; i = _skip_tagged_fields(frame, i); out += frame[start:i]
        if ver >= 3: out += frame[i:i+4]; i += 4
        nb1, ni = _read_uvarint(frame, i); out += frame[i:ni]; i = ni
        nb = nb1 - 1
        for _ in range(nb):
            out += frame[i:i+4]; i += 4
            hlen1, ni = _read_uvarint(frame, i); i = ni
            hlen = hlen1 - 1 if hlen1 > 0 else 0
            i += hlen
            nh = self.adv_host.encode()
            out += _write_uvarint(len(nh) + 1) + nh
            i += 4  # skip real port
            out += struct.pack(">i", self.listen_port)  # advertise OUR port
            rl1, ni = _read_uvarint(frame, i); out += frame[i:ni]; i = ni
            if rl1 > 0:
                rl = rl1 - 1; out += frame[i:i+rl]; i += rl
            start = i; i = _skip_tagged_fields(frame, i); out += frame[start:i]
        out += frame[i:]
        return bytes(out)

    def _pump_c2b(self, csock, usock, client):
        fr = _Frames()
        try:
            while self.running:
                data = csock.recv(65536)
                if not data: break
                for kind, payload in fr.feed(data):
                    if kind == "FRAME": self._decode_request(payload, client)
                usock.sendall(data)
        except Exception: pass
        finally:
            for s in (csock, usock):
                try: s.close()
                except Exception: pass

    def _pump_b2c(self, usock, csock, client):
        fr = _Frames()
        try:
            while self.running:
                data = usock.recv(65536)
                if not data: break
                frames = fr.feed(data)
                if frames and any(k=="FRAME" for k,_ in frames):
                    rebuilt = bytearray()
                    for kind, payload in frames:
                        if kind != "FRAME":
                            rebuilt += payload; continue
                        corr, = struct.unpack_from(">i", payload, 0)
                        meta = self.pending.get(corr)
                        outp = payload
                        if meta and meta[0] == "Metadata":
                            outp = self._rewrite_metadata_adv(payload, meta[1])
                        self._decode_response(payload, client)
                        rebuilt += struct.pack(">i", len(outp)) + outp
                    csock.sendall(bytes(rebuilt))
                else:
                    csock.sendall(data)
        except Exception: pass
        finally:
            for s in (csock, usock):
                try: s.close()
                except Exception: pass

    def _handle(self, csock, caddr):
        client = f"{caddr[0]}:{caddr[1]}"
        self.conns += 1
        self.log("*","connect",f"client {client} connected; dialing upstream {self.up_host}:{self.up_port}")
        try:
            usock = socket.create_connection((self.up_host, self.up_port), timeout=10)
        except Exception as e:
            self.log("*","error",f"upstream connect failed: {e}")
            try: csock.close()
            except Exception: pass
            return
        threading.Thread(target=self._pump_c2b, args=(csock,usock,client), daemon=True).start()
        threading.Thread(target=self._pump_b2c, args=(usock,csock,client), daemon=True).start()

    def _serve(self):
        try:
            self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._srv.bind(("0.0.0.0", self.listen_port)); self._srv.listen(50)
            self._srv.settimeout(1.0)
            self.log("*","listen",f"proxy listening on :{self.listen_port} -> {self.up_host}:{self.up_port} (advertise as {self.adv_host})")
            while self.running:
                try: csock, caddr = self._srv.accept()
                except socket.timeout: continue
                except OSError: break
                threading.Thread(target=self._handle, args=(csock,caddr), daemon=True).start()
        finally:
            if self._srv:
                try: self._srv.close()
                except Exception: pass

    def start(self):
        if self.running: return False
        self.running = True; self.started_at = time.time()
        self._thread = threading.Thread(target=self._serve, daemon=True); self._thread.start()
        return True

    def stop(self):
        self.running = False
        if self._srv:
            try: self._srv.close()
            except Exception: pass
        self.log("*","stop","proxy stopped"); return True

    def stats_report(self):
        rows = []
        for t, d in sorted(self.stats.items()):
            rows.append({"topic": t, "produce_req": d.get("produce_req",0),
                         "produce_ok": d.get("produce_ok",0), "produce_err": d.get("produce_err",0),
                         "bytes": d.get("bytes",0), "errors": d.get("errors",{}),
                         "last_t": d.get("last_t",0)})
        return {"since": self.started_at, "topics": rows}

    def status(self):
        return {"running": self.running, "listen_port": self.listen_port,
                "upstream": f"{self.up_host}:{self.up_port}", "advertise_host": self.adv_host,
                "connections": self.conns,
                "uptime_s": round(time.time()-self.started_at,1) if self.started_at else 0}


_proxy = None
_plock = threading.Lock()

def start_proxy(listen_port, upstream, advertise_host):
    global _proxy
    uh, _, up = upstream.partition(":")
    with _plock:
        if _proxy and _proxy.running:
            return {"ok": False, "error": "proxy already running", "status": _proxy.status()}
        _proxy = Proxy(listen_port, uh, up or "9092", advertise_host)
        ok = _proxy.start(); time.sleep(0.3)
        return {"ok": ok, "status": _proxy.status()}

def stop_proxy():
    global _proxy
    with _plock:
        if not _proxy or not _proxy.running:
            return {"ok": False, "error": "proxy not running"}
        _proxy.stop(); return {"ok": True, "status": _proxy.status()}

def proxy_status():
    return _proxy.status() if _proxy else {"running": False}

def proxy_events(since=0.0):
    return _proxy.snapshot(since) if _proxy else []

def proxy_stats():
    return _proxy.stats_report() if _proxy else {"since": None, "topics": []}

def proxy_logfile_path():
    return _proxy.logfile if _proxy else os.environ.get("KD_PROXY_LOG", "/data/proxy-feed.log")
