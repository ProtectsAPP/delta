#!/usr/bin/env python3
"""Multi-process Raft integration test with chaos.

Spawns three real delta_server processes wired together via --enable-raft.
Exercises:

  1. Cluster forms a single leader.
  2. Writes through the leader's HTTP API replicate to all three nodes.
  3. SIGKILL the leader. A new leader emerges within ~3s. Subsequent
     writes commit on the surviving two-node majority.
  4. Restart the killed node. It catches up via Raft log replication
     (or InstallSnapshot when a forced snapshot has truncated the log).
  5. Live membership change: remove a peer, write again, the smaller
     quorum still commits.

Usage:
    python3 tests/integration/raft_chaos.py
    python3 tests/integration/raft_chaos.py --binary build/delta_server

Exit code 0 = all phases passed.

Environment notes
-----------------
* Requires a built `delta_server` binary. Defaults to ./build/delta_server.
* Picks ports dynamically so it doesn't conflict with other tests.
* Logs each child process to a tmp directory; on failure the path is
  printed so you can grep `[Delta][...]` lines for the actual cause.
* Designed to run inside CI as well as on a developer laptop.
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

# ---------------------------------------------------------------------------
# HTTP helpers — kept dependency-free (no requests/aiohttp) so the script
# runs on a vanilla Python 3.
# ---------------------------------------------------------------------------

def http(method, url, body=None, headers=None, timeout=5.0):
    data = None
    h = {"Content-Type": "application/json"}
    if headers: h.update(headers)
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8"))
        except Exception:
            return e.code, None
    except (urllib.error.URLError, ConnectionError, TimeoutError, socket.timeout):
        return 0, None


def pick_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


# ---------------------------------------------------------------------------
# Server lifecycle helpers.
# ---------------------------------------------------------------------------

class Node:
    """One delta_server child process with raft enabled."""
    def __init__(self, node_id, http_port, data_dir, log_path, peers, token, binary):
        self.node_id    = node_id
        self.http_port  = http_port
        self.data_dir   = data_dir
        self.log_path   = log_path
        self.peers      = peers          # list of "id@host:port"
        self.token      = token
        self.binary     = binary
        self.proc       = None
        self.url        = f"http://127.0.0.1:{http_port}"

    def start(self):
        log_fh = open(self.log_path, "ab", buffering=0)
        cmd = [
            self.binary,
            "--data", self.data_dir,
            "--port", str(self.http_port),
            "--no-deltaql", "--no-ws",
            "--enable-raft",
            "--node-id", self.node_id,
            "--cluster-token", self.token,
            "--raft-election-min-ms", "300",
            "--raft-election-max-ms", "600",
            "--raft-heartbeat-ms",    "80",
            "--raft-tick-ms",         "20",
            # tight propose timeout so tests fail fast when commits stall.
            "--raft-propose-timeout-ms", "3000",
        ]
        for p in self.peers:
            cmd += ["--cluster-peer", p]
        self.proc = subprocess.Popen(
            cmd, stdout=log_fh, stderr=log_fh,
            close_fds=True, preexec_fn=os.setsid)

    def kill(self, sig=signal.SIGKILL):
        if not self.proc or self.proc.poll() is not None: return
        try:
            os.killpg(self.proc.pid, sig)
        except ProcessLookupError:
            pass

    def wait_dead(self, timeout=5.0):
        if not self.proc: return
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.kill(signal.SIGKILL)
            self.proc.wait(timeout=2.0)

    def status(self):
        code, body = http("GET", f"{self.url}/api/v1/cluster/raft/status", timeout=1.0)
        if code != 200 or not body or "data" not in body: return None
        return body["data"]

    def is_leader(self):
        s = self.status()
        return bool(s) and s.get("role") == "leader"

    def is_alive(self):
        return self.proc and self.proc.poll() is None


def wait_until(pred, timeout, interval=0.1, msg=""):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if pred(): return True
        except Exception:
            pass
        time.sleep(interval)
    print(f"  TIMEOUT waiting for: {msg}")
    return False


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def login(node, user="admin", pw="admin"):
    """Returns (token, expires_at) or (None, None) on failure."""
    code, body = http("POST", f"{node.url}/api/v1/auth/login",
                      {"username": user, "password": pw})
    if code != 200 or not body or "data" not in body: return None, None
    return body["data"]["token"], body["data"]["expires_at"]


def propose_kv(node, payload):
    """Pushes an opaque entry through Raft using the cluster_token. Returns the
    committed index, or None on failure (e.g. node is a follower)."""
    code, body = http("POST", f"{node.url}/api/v1/cluster/raft/propose",
                      {"payload": payload},
                      headers={"X-Delta-Cluster-Token": node.token})
    if code != 200 or not body or "data" not in body: return None
    return body["data"].get("index")


def find_leader(nodes, timeout=10.0):
    """Return the leader Node, or None if no leader within timeout."""
    leaders = []
    def picked():
        leaders.clear()
        for n in nodes:
            if not n.is_alive(): continue
            if n.is_leader(): leaders.append(n)
        return len(leaders) == 1
    if not wait_until(picked, timeout, msg="exactly one leader"):
        return None
    return leaders[0]


def commit_indices(nodes):
    out = {}
    for n in nodes:
        s = n.status() if n.is_alive() else None
        out[n.node_id] = s.get("commit_index", -1) if s else -1
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(binary):
    if not pathlib.Path(binary).exists():
        print(f"FAIL: binary not found at {binary}", file=sys.stderr)
        return 2

    tmp = tempfile.mkdtemp(prefix="delta_raft_chaos_")
    print(f"[chaos] tmp={tmp}")
    token = "shared-cluster-token-for-tests"

    # Pick ports up front so peers can reference each other.
    p1, p2, p3 = pick_free_port(), pick_free_port(), pick_free_port()
    peer_specs = [
        f"n1@127.0.0.1:{p1}",
        f"n2@127.0.0.1:{p2}",
        f"n3@127.0.0.1:{p3}",
    ]

    nodes = [
        Node("n1", p1, os.path.join(tmp, "n1"), os.path.join(tmp, "n1.log"),
             peer_specs, token, binary),
        Node("n2", p2, os.path.join(tmp, "n2"), os.path.join(tmp, "n2.log"),
             peer_specs, token, binary),
        Node("n3", p3, os.path.join(tmp, "n3"), os.path.join(tmp, "n3.log"),
             peer_specs, token, binary),
    ]
    for n in nodes: os.makedirs(n.data_dir, exist_ok=True)

    failures = []
    try:
        for n in nodes: n.start()
        print("[chaos] all three started")

        # ---- Phase 1: leader election ----------------------------------
        leader = find_leader(nodes, timeout=15.0)
        if not leader:
            failures.append("phase1: no leader emerged")
            return 1
        print(f"[chaos] phase 1 OK: leader={leader.node_id}")

        # ---- Phase 2: writes through leader replicate ------------------
        for i in range(20):
            idx = propose_kv(leader, f"phase2-{i}")
            if idx is None:
                failures.append(f"phase2: propose i={i} failed")
                return 1
        # Wait for all alive nodes to converge on the same commit_index.
        def converged():
            ci = commit_indices(nodes)
            vals = list(ci.values())
            return all(v >= 20 for v in vals) and len(set(vals)) == 1
        if not wait_until(converged, 10.0, msg="phase2 convergence"):
            failures.append(f"phase2: commit indices diverged {commit_indices(nodes)}")
            return 1
        print(f"[chaos] phase 2 OK: {commit_indices(nodes)}")

        # ---- Phase 3: kill leader, new leader takes over ---------------
        old_leader_id = leader.node_id
        print(f"[chaos] killing leader {old_leader_id}")
        leader.kill()
        leader.wait_dead()
        survivors = [n for n in nodes if n.node_id != old_leader_id]
        new_leader = find_leader(survivors, timeout=15.0)
        if not new_leader:
            failures.append("phase3: no new leader after kill")
            return 1
        if new_leader.node_id == old_leader_id:
            failures.append("phase3: same leader returned")
            return 1
        print(f"[chaos] phase 3 OK: new leader={new_leader.node_id}")

        # Writes still work on the new leader.
        for i in range(10):
            idx = propose_kv(new_leader, f"phase3-{i}")
            if idx is None:
                failures.append(f"phase3: propose i={i} failed")
                return 1
        def survivors_converged():
            ci = commit_indices(survivors)
            return all(v >= 30 for v in ci.values()) and \
                   len(set(ci.values())) == 1
        if not wait_until(survivors_converged, 10.0,
                          msg="phase3 survivor convergence"):
            failures.append(f"phase3: survivors diverged {commit_indices(survivors)}")
            return 1
        print(f"[chaos] phase 3b OK: survivors at {commit_indices(survivors)}")

        # ---- Phase 4: restart the killed node, it catches up -----------
        old = next(n for n in nodes if n.node_id == old_leader_id)
        print(f"[chaos] restarting {old_leader_id}")
        old.start()
        def all_converged():
            ci = commit_indices(nodes)
            return all(v >= 30 for v in ci.values()) and \
                   len(set(ci.values())) == 1
        if not wait_until(all_converged, 20.0, msg="phase4 full convergence"):
            failures.append(f"phase4: restart catch-up failed {commit_indices(nodes)}")
            return 1
        print(f"[chaos] phase 4 OK: all at {commit_indices(nodes)}")

        # ---- Phase 5: membership change (remove one peer) --------------
        # Pick a peer to remove that ISN'T the current leader. Otherwise the
        # add_peer/remove_peer endpoint exercises the self-step-down path,
        # which is interesting but slower to verify.
        cur_leader = find_leader(nodes, timeout=5.0)
        if not cur_leader:
            failures.append("phase5: no leader before membership change")
            return 1
        victim = next(n for n in nodes if n.node_id != cur_leader.node_id)

        # remove_peer is admin-only, so we need a login token. record_login_ip
        # writes via raft → leader-only login.
        tok, _ = login(cur_leader)
        if not tok:
            failures.append("phase5: leader login failed")
            return 1
        code, body = http(
            "POST", f"{cur_leader.url}/api/v1/cluster/raft/remove_peer",
            {"peer_id": victim.node_id},
            headers={"Authorization": f"Bearer {tok}",
                     "X-Delta-Cluster-Token": token})
        if code != 200:
            failures.append(f"phase5: remove_peer failed code={code} body={body}")
            return 1
        print(f"[chaos] phase 5: removed {victim.node_id}, "
              f"new peers={body.get('data', {}).get('peers')}")

        # The smaller cluster (cur_leader + the third node) must still commit.
        remaining = [n for n in nodes
                     if n.node_id != victim.node_id and n.is_alive()]
        for i in range(5):
            idx = propose_kv(cur_leader, f"phase5-{i}")
            if idx is None:
                failures.append(f"phase5: propose i={i} failed")
                return 1
        def remaining_converged():
            ci = commit_indices(remaining)
            # +5 writes on top of phase 4 baseline (>=30) + 1 config entry.
            return all(v >= 36 for v in ci.values()) and \
                   len(set(ci.values())) == 1
        if not wait_until(remaining_converged, 10.0,
                          msg="phase5 smaller-quorum convergence"):
            failures.append(f"phase5: smaller quorum diverged {commit_indices(remaining)}")
            return 1
        print(f"[chaos] phase 5 OK: remaining at {commit_indices(remaining)}")

        print("[chaos] ALL PHASES PASSED")
        return 0
    finally:
        for n in nodes:
            try:
                n.kill(signal.SIGTERM)
            except Exception:
                pass
        for n in nodes: n.wait_dead(timeout=3.0)
        if failures:
            print("[chaos] failures:")
            for f in failures: print("  -", f)
            print(f"[chaos] inspect logs under {tmp}")
        else:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default="build/delta_server",
                    help="path to the delta_server binary")
    args = ap.parse_args()
    sys.exit(run(args.binary))
