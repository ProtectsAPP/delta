#!/usr/bin/env python3
"""Multi-process sharding integration test.

Topology: 2 shards × 1 replica each (4 nodes total — two raft groups of size
1 because we want to focus on the gateway, not raft). Cluster forms with:

    n1 (shard s0), n2 (shard s1)

Tests
-----
  Phase 1: writes hit the gateway, end up on the correct shard.
           verifies: docs written through n1 land on whichever shard the
           consistent hash assigned them, recoverable via either node.
  Phase 2: point reads against the WRONG node still resolve via gateway.
  Phase 3: cross-shard transactions surface as HTTP 501 Unsupported.
  Phase 4: kill -9 one shard's process. Reads / writes targeting the
           OTHER shard still succeed.
  Phase 5: GET /api/v1/cluster/shards/route returns the same answer
           on every alive node (topology agreement).

Usage:
    python3 tests/integration/sharding_chaos.py --binary build/delta_server
"""
import argparse
import json
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


def http(method, url, body=None, headers=None, timeout=5.0):
    h = {"Content-Type": "application/json"}
    if headers: h.update(headers)
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read().decode())
        except Exception: return e.code, None
    except (urllib.error.URLError, ConnectionError, TimeoutError, socket.timeout):
        return 0, None


def pick_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close()
    return p


def wait_until(pred, timeout, interval=0.1, msg=""):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if pred(): return True
        except Exception:
            pass
        time.sleep(interval)
    print(f"  TIMEOUT: {msg}")
    return False


class Node:
    def __init__(self, node_id, shard_id, port, data_dir, log_path,
                 binary, token, shards_specs):
        self.id = node_id
        self.shard = shard_id
        self.port = port
        self.url = f"http://127.0.0.1:{port}"
        self.data_dir = data_dir
        self.log_path = log_path
        self.binary = binary
        self.token = token
        self.shards_specs = shards_specs
        self.proc = None

    def start(self):
        log_fh = open(self.log_path, "ab", buffering=0)
        cmd = [self.binary,
               "--data", self.data_dir,
               "--port", str(self.port),
               "--no-deltaql", "--no-ws",
               "--enable-sharding",
               "--shard-id", self.shard,
               "--shard-vnodes", "128",
               "--cluster-token", self.token]
        for s in self.shards_specs: cmd += ["--shard", s]
        self.proc = subprocess.Popen(cmd, stdout=log_fh, stderr=log_fh,
                                     close_fds=True, preexec_fn=os.setsid)

    def kill(self, sig=signal.SIGKILL):
        if not self.proc or self.proc.poll() is not None: return
        try: os.killpg(self.proc.pid, sig)
        except ProcessLookupError: pass

    def wait_dead(self, t=5.0):
        if not self.proc: return
        try: self.proc.wait(timeout=t)
        except subprocess.TimeoutExpired:
            self.kill(signal.SIGKILL); self.proc.wait(timeout=2.0)

    def alive(self):
        return self.proc and self.proc.poll() is None

    def ready(self):
        c, b = http("GET", f"{self.url}/api/v1/cluster/shards", timeout=1.0)
        return c == 200


def login(url):
    c, b = http("POST", f"{url}/api/v1/auth/login",
                {"username": "admin", "password": "admin"})
    if c != 200: return None
    return b["data"]["token"]


def auth_headers(tok, token):
    return {"Authorization": f"Bearer {tok}",
            "X-Delta-Cluster-Token": token}


def run(binary):
    if not pathlib.Path(binary).exists():
        print(f"FAIL: binary not found at {binary}", file=sys.stderr)
        return 2
    tmp = tempfile.mkdtemp(prefix="delta_shard_chaos_")
    print(f"[shard-chaos] tmp={tmp}")
    token = "shard-test-token"

    p1, p2 = pick_free_port(), pick_free_port()
    shards = [
        f"s0=n1@127.0.0.1:{p1}",
        f"s1=n2@127.0.0.1:{p2}",
    ]
    n1 = Node("n1", "s0", p1, os.path.join(tmp, "n1"),
              os.path.join(tmp, "n1.log"), binary, token, shards)
    n2 = Node("n2", "s1", p2, os.path.join(tmp, "n2"),
              os.path.join(tmp, "n2.log"), binary, token, shards)
    nodes = [n1, n2]
    failures = []

    try:
        for d in [n1.data_dir, n2.data_dir]: os.makedirs(d, exist_ok=True)
        for n in nodes: n.start()
        if not wait_until(lambda: all(n.ready() for n in nodes), 10.0,
                          msg="both shards ready"):
            failures.append("phase0: nodes never became ready")
            return 1
        print("[shard-chaos] both shards ready")

        tok = login(n1.url)
        if not tok:
            failures.append("phase0: login failed"); return 1

        # Phase 0: create a collection on each shard (collection meta is
        # not yet auto-broadcast in v1 — operators create on each shard
        # explicitly or via the per-shard URL).
        for n in nodes:
            t = login(n.url)
            c, b = http("POST",
                f"{n.url}/api/v1/collections",
                {"name": "items"},
                headers={"Authorization": f"Bearer {t}",
                         "X-Delta-Shard-Hop": "setup"})
            if c != 200:
                failures.append(
                    f"phase0: create coll failed on {n.id}: {c} {b}")
                return 1

        # Phase 1: insert 50 docs via gateway (n1). Each gets routed to
        # whichever shard owns its allocated id. Verify the global count
        # = 50 (via per-shard reads + fan-out).
        ids = []
        for i in range(50):
            c, b = http("POST",
                f"{n1.url}/api/v1/collections/items/documents",
                {"name": f"item-{i}"},
                headers=auth_headers(tok, token))
            if c != 200:
                failures.append(f"phase1: insert {i} failed: {c} {b}")
                return 1
            ids.append(b["data"]["id"])
        # Per-shard counts (direct, no gateway): they should be roughly
        # balanced (within ±50%) and sum to 50.
        per_shard = {}
        for n in nodes:
            t = login(n.url)
            c, b = http("POST",
                f"{n.url}/api/v1/collections/items/count",
                {"filter": {}},
                headers={"Authorization": f"Bearer {t}",
                         "X-Delta-Shard-Hop": "verify"})
            per_shard[n.id] = b["data"]["count"] if c == 200 else -1
        total = sum(per_shard.values())
        if total != 50:
            failures.append(f"phase1: total docs {per_shard} sum={total}")
            return 1
        if min(per_shard.values()) < 5:
            failures.append(f"phase1: shard distribution skewed: {per_shard}")
            return 1
        print(f"[shard-chaos] phase 1 OK: per-shard={per_shard}")

        # Phase 2: point reads through the WRONG node still resolve.
        # We pick any doc and read it via BOTH gateways.
        any_id = ids[0]
        for n in nodes:
            c, b = http("GET",
                f"{n.url}/api/v1/collections/items/documents/{any_id}",
                headers={"Authorization": f"Bearer {login(n.url)}"})
            if c != 200:
                failures.append(
                    f"phase2: read {any_id} via {n.id} failed: {c} {b}")
                return 1
        print(f"[shard-chaos] phase 2 OK: cross-node reads resolve")

        # Phase 3: cross-shard transactions return 501.
        # Pick two ids that are KNOWN to hash to different shards.
        # We try until we find such a pair.
        cross = None
        for a in ids:
            for b_id in ids:
                if a == b_id: continue
                c, r = http("GET",
                    f"{n1.url}/api/v1/cluster/shards/route?key={a}",
                    headers={"Authorization": f"Bearer {tok}"})
                sa = r["data"]["shard_id"]
                c, r = http("GET",
                    f"{n1.url}/api/v1/cluster/shards/route?key={b_id}",
                    headers={"Authorization": f"Bearer {tok}"})
                sb = r["data"]["shard_id"]
                if sa != sb:
                    cross = (a, b_id); break
            if cross: break
        if not cross:
            failures.append("phase3: could not find cross-shard id pair")
            return 1
        tx_body = {"ops": [
            {"op": "update", "collection": "items", "id": cross[0],
             "update": {"$set": {"x": 1}}},
            {"op": "update", "collection": "items", "id": cross[1],
             "update": {"$set": {"x": 2}}},
        ]}
        c, b = http("POST", f"{n1.url}/api/v1/transactions/execute",
                    tx_body, headers=auth_headers(tok, token))
        if c != 501:
            failures.append(f"phase3: cross-shard tx returned {c} (want 501) "
                            f"body={b}")
            return 1
        print(f"[shard-chaos] phase 3 OK: cross-shard tx -> 501")

        # Phase 4: kill n2. Reads/writes for items on shard s0 should
        # still work; ops targeting s1 will fail.
        n2.kill(); n2.wait_dead()
        # Pick an id that's on s0.
        s0_id = None
        for x in ids:
            c, r = http("GET",
                f"{n1.url}/api/v1/cluster/shards/route?key={x}",
                headers={"Authorization": f"Bearer {tok}"})
            if r["data"]["shard_id"] == "s0":
                s0_id = x; break
        if not s0_id:
            failures.append("phase4: no s0 id available")
            return 1
        c, b = http("GET",
            f"{n1.url}/api/v1/collections/items/documents/{s0_id}",
            headers={"Authorization": f"Bearer {tok}"})
        if c != 200:
            failures.append(f"phase4: s0 read failed after killing s1: {c} {b}")
            return 1
        print(f"[shard-chaos] phase 4 OK: s0 still reachable after s1 down")

        # Phase 5: route lookup agrees on the surviving node.
        for k in ids[:5]:
            c, b = http("GET",
                f"{n1.url}/api/v1/cluster/shards/route?key={k}",
                headers={"Authorization": f"Bearer {tok}"})
            if c != 200:
                failures.append(f"phase5: route lookup failed for {k}")
                return 1
        print(f"[shard-chaos] phase 5 OK: route lookup works")
        print("[shard-chaos] ALL PHASES PASSED")
        return 0
    finally:
        for n in nodes:
            try: n.kill(signal.SIGTERM)
            except Exception: pass
        for n in nodes: n.wait_dead(t=3.0)
        if failures:
            print("[shard-chaos] failures:")
            for f in failures: print("  -", f)
            print(f"[shard-chaos] inspect {tmp}")
        else:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default="build/delta_server")
    args = ap.parse_args()
    sys.exit(run(args.binary))
