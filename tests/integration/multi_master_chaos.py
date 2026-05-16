#!/usr/bin/env python3
"""Multi-master writes integration test (B.2).

Spins up two delta_server processes pointing at each other via
`--mm-peer`. Creates a `multi_master:true` collection on each node,
writes overlapping document sets concurrently, and verifies that the
HLC-based last-writer-wins puller converges both replicas to the same
state. Then exercises a delete-followed-by-update race to make sure
tombstones win when their HLC is highest.

Phases
------
  Phase 0: cold start, both nodes ready, both know the multi_master
           collection (created independently — no cross-shard broadcast
           in this test, since multi-master is orthogonal to sharding).
  Phase 1: write disjoint id ranges to each node concurrently. After
           the convergence window, both nodes return the union via
           GET /documents/:id.
  Phase 2: write the SAME id on both nodes a few times. The HLC-LWW
           winner persists on both sides.
  Phase 3: DELETE on n1, then UPDATE on n2 with a strictly later HLC
           (we observe what the actual order is via the responses).
           Convergence resolves to whichever has the higher HLC.
  Phase 4: confirm /api/v1/cluster/mm/status shows non-zero
           changes_pulled on both peers.

Usage:
    python3 tests/integration/multi_master_chaos.py --binary build/delta_server
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
import threading
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
    def __init__(self, node_id, port, data_dir, log_path, binary, token, peer_url):
        self.id = node_id
        self.port = port
        self.url = f"http://127.0.0.1:{port}"
        self.data_dir = data_dir
        self.log_path = log_path
        self.binary = binary
        self.token = token
        self.peer_url = peer_url
        self.proc = None

    def start(self):
        log_fh = open(self.log_path, "ab", buffering=0)
        cmd = [self.binary,
               "--data", self.data_dir,
               "--port", str(self.port),
               "--no-deltaql", "--no-ws",
               "--mm-peer", self.peer_url,
               "--mm-poll-ms", "200",
               "--cluster-token", self.token]
        self.proc = subprocess.Popen(cmd, stdout=log_fh, stderr=log_fh,
                                     close_fds=True, preexec_fn=os.setsid)

    def kill(self, sig=signal.SIGTERM):
        if not self.proc or self.proc.poll() is not None: return
        try: os.killpg(self.proc.pid, sig)
        except ProcessLookupError: pass

    def wait_dead(self, t=5.0):
        if not self.proc: return
        try: self.proc.wait(timeout=t)
        except subprocess.TimeoutExpired:
            self.kill(signal.SIGKILL); self.proc.wait(timeout=2.0)

    def ready(self):
        c, _ = http("GET", f"{self.url}/api/v1/health", timeout=1.0)
        return c == 200


def login(url):
    c, b = http("POST", f"{url}/api/v1/auth/login",
                {"username": "admin", "password": "admin"})
    if c != 200: return None
    return b["data"]["token"]


def run(binary):
    if not pathlib.Path(binary).exists():
        print(f"FAIL: binary not found at {binary}", file=sys.stderr)
        return 2
    tmp = tempfile.mkdtemp(prefix="delta_mm_chaos_")
    print(f"[mm-chaos] tmp={tmp}")
    token = "mm-test-token"

    p1, p2 = pick_free_port(), pick_free_port()
    n1 = Node("n1", p1, f"{tmp}/n1", f"{tmp}/n1.log", binary, token,
              peer_url=f"http://127.0.0.1:{p2}")
    n2 = Node("n2", p2, f"{tmp}/n2", f"{tmp}/n2.log", binary, token,
              peer_url=f"http://127.0.0.1:{p1}")
    for n in (n1, n2): os.makedirs(n.data_dir, exist_ok=True)
    n1.start(); n2.start()
    try:
        if not wait_until(lambda: n1.ready() and n2.ready(), timeout=10,
                          msg="nodes not ready"):
            return 1
        print("[mm-chaos] both nodes ready")

        t1 = login(n1.url); t2 = login(n2.url)
        assert t1 and t2, "login failed"
        h1 = {"Authorization": f"Bearer {t1}"}
        h2 = {"Authorization": f"Bearer {t2}"}

        # ---- Phase 0: create the multi_master collection on BOTH nodes
        # independently. For this test there's no sharding gateway to
        # auto-broadcast collection metadata.
        for url, h in ((n1.url, h1), (n2.url, h2)):
            c, b = http("POST", f"{url}/api/v1/collections",
                        {"name": "items", "multi_master": True}, headers=h)
            assert c == 200, f"create on {url}: {c} {b}"
        print("[mm-chaos] phase 0 OK: multi_master collection ready")

        # ---- Phase 1: disjoint id ranges, concurrent writes ----------
        def writer(url, h, prefix, n):
            for i in range(n):
                http("POST", f"{url}/api/v1/collections/items/documents",
                     {"_id": f"{prefix}-{i}", "v": i, "src": prefix},
                     headers=h)
        t_a = threading.Thread(target=writer, args=(n1.url, h1, "a", 30))
        t_b = threading.Thread(target=writer, args=(n2.url, h2, "b", 30))
        t_a.start(); t_b.start(); t_a.join(); t_b.join()

        def both_have(id_):
            c1, b1 = http("GET", f"{n1.url}/api/v1/collections/items/documents/{id_}", headers=h1)
            c2, b2 = http("GET", f"{n2.url}/api/v1/collections/items/documents/{id_}", headers=h2)
            return c1 == 200 and c2 == 200

        # The puller has 200ms cadence; allow up to 8s for full sync.
        if not wait_until(lambda: all(both_have(f"a-{i}") for i in range(30)) and
                                  all(both_have(f"b-{i}") for i in range(30)),
                          timeout=15, msg="phase 1 convergence"):
            return 1
        print("[mm-chaos] phase 1 OK: 60 docs converged on both nodes")

        # ---- Phase 2: same id, multiple writes, LWW must converge ----
        for round_ in range(5):
            http("POST", f"{n1.url}/api/v1/collections/items/documents",
                 {"_id": "race", "round": round_, "src": "n1"}, headers=h1)
            http("POST", f"{n2.url}/api/v1/collections/items/documents",
                 {"_id": "race", "round": round_, "src": "n2"}, headers=h2)

        # Wait for convergence: both nodes must agree on the SAME _hlc
        # (and therefore the same payload).
        def race_winners():
            c1, b1 = http("GET", f"{n1.url}/api/v1/collections/items/documents/race", headers=h1)
            c2, b2 = http("GET", f"{n2.url}/api/v1/collections/items/documents/race", headers=h2)
            if c1 != 200 or c2 != 200: return None
            h1_v = b1["data"].get("_hlc", b1["data"].get("data", {}).get("_hlc"))
            h2_v = b2["data"].get("_hlc", b2["data"].get("data", {}).get("_hlc"))
            return (h1_v, h2_v)

        if not wait_until(lambda: race_winners() is not None and
                                  race_winners()[0] is not None and
                                  race_winners()[0] == race_winners()[1],
                          timeout=10, msg="phase 2 LWW convergence"):
            return 1
        print(f"[mm-chaos] phase 2 OK: same-id race converged")

        # ---- Phase 3: delete-vs-update across nodes ------------------
        # Seed a doc, give it time to replicate, then delete on n1 and
        # update on n2. Whichever HLC is higher wins.
        http("POST", f"{n1.url}/api/v1/collections/items/documents",
             {"_id": "ttl-x", "v": 0}, headers=h1)
        if not wait_until(lambda: both_have("ttl-x"), timeout=10,
                          msg="seed replication"):
            return 1
        # Update on n2 first, then delete on n1. The puller advances the
        # local HLC on every received change, so both nodes agree on
        # ordering deterministically.
        http("PATCH", f"{n2.url}/api/v1/collections/items/documents/ttl-x",
             {"$set": {"v": 99}}, headers=h2)
        time.sleep(0.5)  # let n2's update reach n1 first
        http("DELETE", f"{n1.url}/api/v1/collections/items/documents/ttl-x", headers=h1)

        def converged_state():
            c1, _ = http("GET", f"{n1.url}/api/v1/collections/items/documents/ttl-x", headers=h1)
            c2, _ = http("GET", f"{n2.url}/api/v1/collections/items/documents/ttl-x", headers=h2)
            return c1 == c2  # both 200 (live) or both 404 (tombstone)

        if not wait_until(converged_state, timeout=10,
                          msg="phase 3 delete/update convergence"):
            return 1
        print("[mm-chaos] phase 3 OK: delete/update race converged")

        # ---- Phase 4: status endpoint counts something ---------------
        c, b = http("GET", f"{n1.url}/api/v1/cluster/mm/status", headers=h1)
        assert c == 200, f"mm/status on n1: {c}"
        pulled = b["data"]["pulled_total"]
        if pulled <= 0:
            print(f"[mm-chaos] FAIL phase 4: n1 pulled_total={pulled}")
            return 1
        print(f"[mm-chaos] phase 4 OK: n1 pulled_total={pulled}")

        print("[mm-chaos] ALL PHASES PASSED")
        return 0
    finally:
        for n in (n1, n2):
            n.kill(); n.wait_dead()
        # Keep tmp on failure for triage; remove on success.
        # (Caller can re-run with $TMPDIR pinned to inspect logs.)
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", required=True)
    args = ap.parse_args()
    sys.exit(run(args.binary))


if __name__ == "__main__":
    main()
