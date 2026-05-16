# Delta — Production Roadmap

This document is the canonical "what we ship" and "what we don't yet ship" list
for a Delta deployment. Items above the **Shipped** line are usable today.
Items below it are deliberately scoped for future iterations and described
here so an operator can plan around them.

---

## Shipped (rev. May 2026)

### Storage / consistency
- LSM-tree with WAL, CRC32C torn-write detection, fsynced LSN file
- Bloom-filtered SSTables (v2 with magic-number rejection of legacy files)
- Crash-recovery replay via WAL on startup
- Online compaction (background thread)
- Multi-document ACID transactions: optimistic concurrency control, atomic
  commit-or-rollback across collections, read-set version validation,
  unique-index/version checks at commit time
  (`POST /api/v1/transactions/execute`)

### Query / indexing
- Single-field equality + `$in` index plans
- Composite (multi-field) index plans: equality on a leading prefix of
  fields, with subsequent range/regex/etc. clauses correctly re-applied
  on the candidate set
- MongoDB-style aggregation pipeline:
  `$match / $group / $sort / $limit / $skip / $project / $unwind / $lookup`

### Replication
- Master / replica roles with WAL streaming (`/api/v1/cluster/*`)
- Snapshot bootstrap for new replicas (`/cluster/snapshot`)
- Cluster-token gated control plane
- Master `apply_replicated()` advances LSN atomically; restore reuses it
- **Raft consensus (production wiring complete)** — leader election, log
  replication, §5.4.1 election restriction, §9.6 pre-vote, §7 log
  compaction via InstallSnapshot, Raft thesis §4.3 single-server
  membership change. Pluggable `RaftTransport` / `RaftStateMachine`
  interfaces; file-backed persistent state with snapshot bytes inline;
  HTTP-backed `HttpRaftTransport` over `/api/v1/cluster/raft/*`.
  Enable per-node with `--enable-raft --node-id <id> --cluster-peer <id@host:port>`
  (repeatable). Every `LSMTree::put / del` on the leader synchronously
  routes through `RaftNode::propose`, waits for commit AND local apply,
  then returns to the HTTP caller — so a successful 200 means a
  durably-replicated, locally-visible write. Six unit scenarios
  (`test_raft`), three LSM-integration scenarios (`test_raft_lsm`), an
  HTTP loopback suite (`test_raft_http`), and a five-phase multi-process
  chaos test (`test_raft_chaos`: election → replication → kill -9
  leader → restart catch-up → live membership remove) all green on the
  default and TLS builds.

### Auth / security
- PBKDF2-HMAC-SHA256 (120,000 iterations) password hashing, PHC-prefixed
- Per-IP+username sliding-window login rate limiter
- Per-IP token-bucket connection rate limiter (`--conn-rate`/`--conn-burst`)
- Bearer-token sessions with revocation, role + permission model, RLS
- CORS allow-list with `Vary: Origin` + `Allow-Credentials`
- Audit log (`audit.log`, JSONL): login success/failure/rate-limit, logout,
  backup, restore, restore-decrypt-failure, replica-write-rejection
- Slow-query log (`slow.log`, JSONL): every HTTP request exceeding
  `--slow-query-ms` (default 500 ms)

### Observability
- `/metrics` Prometheus text format with:
  - Process counters (uptime, requests, threads, connections)
  - Storage gauges (sstables, docs, vectors)
  - Cache gauges (keys, bytes, hit/miss, hit_rate)
  - HTTP response status-class counters
  - **Per-path HTTP latency histogram (canonical buckets)**
  - **Per-IP rate-limit reject counter**
  - **WebSocket + DeltaQL traffic counters** (frames, bytes, conns)
- Structured JSON logger with level filter and thread-local trace-id
- Trace-id round-trips via `X-Trace-Id` header

### Operations
- `GET /api/v1/admin/backup` — superuser-only full keyspace dump
- `POST /api/v1/admin/restore` — replays into store + auto-reloads metadata
  caches (DatabaseManager, AuthManager)
- **Backup encryption (PBKDF2 + HMAC-SHA256 CTR + HMAC-SHA256 auth)** —
  envelope format `delta-backup-1-encrypted`, configured by
  `--backup-passphrase` or `backup_passphrase` in the JSON config
- `delta-admin` CLI: `login | backup | restore | metrics | databases |
  users | audit-tail | slow-tail`
- **Native TLS (opt-in)** — build with `-DDELTA_TLS=ON` to link OpenSSL
  and serve HTTPS / `wss://` directly via cpp-httplib's SSLServer.
  `--tls-cert` / `--tls-key` flags activate at startup. Default binary
  stays libssl-free for distros without OpenSSL dev headers.
- Single-binary Docker image, multi-arch GHCR releases
- C++ integration test driving a real `HttpServer` over loopback

---

## Not yet shipped — by tier

### Tier A · MUST (target: next 1–2 releases)

#### A.1 Automatic failover — *shipped (rev. May 2026)*
**Status:** done. Raft is wired through the LSM write path; see the
"Replication" entry above for the feature list. Operators enable it
with `--enable-raft --node-id <id> --cluster-peer <id@host:port>` per
node and the legacy single-master `--role master`/`--role replica`
streaming becomes optional. Membership changes go through
`POST /api/v1/cluster/raft/{add_peer,remove_peer}` (superuser-gated)
and snapshots can be forced via `POST /api/v1/cluster/raft/snapshot`.

#### A.2 TLS termination in-process — *shipped (rev. May 2026)*
**Status:** done. Build with `-DDELTA_TLS=ON`; pass `--tls-cert` /
`--tls-key` at startup. cpp-httplib's `SSLServer` is used in place of the
plain `Server`, and the `HttpServer` route registry is identical. Default
build stays OpenSSL-free; the same flags on a non-TLS build warn and fall
back to plain HTTP (so reverse-proxy deployments are unaffected).

#### A.3 Data sharding — *shipped (rev. May 2026)*
**Status:** done. Every `delta_server` process belongs to exactly one
shard (a Raft group from Round 2). Every node knows the full shard map
and runs a consistent-hash gateway in front of its local engine:

  * `src/cluster/shard_router.hpp` — FNV-1a-64 + SplitMix64 finalizer
    on a ring with 256 virtual nodes per shard by default (>= 128
    floor). `route(key)` is stable across processes / restarts and
    moves only ~1/(N+1) of keys when a shard joins.
  * Per-document HTTP routing on `POST /collections/{c}/documents`,
    `POST /documents/bulk`, `GET|PATCH|DELETE /documents/{id}`. Doc ids
    are allocated by the gateway when absent so routing is
    deterministic from the first write onward.
  * Scatter-gather fan-out on `POST /documents/search`,
    `POST /aggregate`, `POST /count` — gateway merges per-shard
    responses (concat + re-apply skip/limit; documented caveat:
    pipelines containing `$group` / `$sort` keep single-shard
    correctness).
  * Cross-shard transactions: `POST /transactions/execute` returns
    HTTP 501 `Status::UNSUPPORTED` with the touched shard ids when ops
    span multiple shards. Single-shard transactions transparently
    forward to the owning shard's leader.
  * Auth trust path: `X-Delta-Cluster-Token` + `X-Delta-Internal-User`
    headers let one shard delegate an already-validated user identity
    to another without replicating the session DB.
  * Collection metadata is broadcast to every peer shard on create so
    `CREATE COLLECTION` once on the gateway is enough for every shard.
  * Topology persisted in `delta_system:shards` for backup/restore.
  * Re-shard tooling: `tools/delta-reshard` walks every collection on
    every shard, recomputes the owner via the gateway's `route` API,
    and moves docs that landed on the wrong shard (POST to new owner
    via gateway + DELETE on old owner direct).
  * Tests: `test_sharding` (6 unit cases: distribution, stability,
    minimal disruption, CLI parser, JSON round-trip, vnode clamping)
    and `test_sharding_chaos` (5-phase multi-process: write, cross-
    node read, cross-shard tx -> 501, kill-shard survival, route
    agreement).

Enable per-node:

```
delta_server \
  --enable-sharding --shard-id s0 \
  --shard 's0=n1@host1:16888,n2@host2:16888' \
  --shard 's1=n3@host3:16888,n4@host4:16888' \
  --shard-vnodes 256 \
  --cluster-token <secret> \
  --port 16888 --data /var/lib/delta/n1
```

Sharding and raft compose: each shard's peers should also be a Raft
group (Round 2) so a shard tolerates node loss, and the sharding
gateway tolerates whole-shard loss only for ops targeting *other*
shards (an unreachable shard surfaces as 503 to the client).

### Tier B · SHOULD (target: same quarter)

#### B.1 Monitoring dashboard
**Status:** `/metrics` is Prometheus-scrapable but no canonical Grafana JSON
is shipped. Plan to add `dashboards/delta.json` plus alerting rules
(`alerts/delta.yml`) covering: error-rate, p99 latency, cache hit rate,
disk-space, replica lag, audit anomalies (failed-login rate spikes).

#### B.2 Multi-master writes (active-active)
**Status:** master is single-writer.
**Plan:** CRDT-encoded document model (LWW-register + add-wins set) for a
subset of collection types. Existing collections stay single-master; the
operator opts in per-collection at creation time.

#### B.3 Cross-DC replication
**Status:** replication assumes <1ms RTT between master and replica.
**Plan:** async log shipping channel with bounded queue + delta compression.
Replica role gains a `--lag-tolerance` flag; alerting fires when lag exceeds
it.

### Tier C · NICE TO HAVE

#### C.1 Official client SDKs
**Status:** REST is documented and HTTPie/curl/Python-stdlib works fine, but
no idiomatic SDK ships. Plan to publish:
- `python/delta-client` (sync + asyncio)
- `go/delta-client`
- `java/delta-client` (Vert.x + blocking)
- `typescript/delta-client` (browser + node)

#### C.2 Query-planner cost-based optimizer
**Status:** the engine has a query planner with stats-driven index choice
already; a cost-based v2 would help complex aggregations and joins.

#### C.3 Tiered storage (cold to S3)
**Status:** all SSTables live on local disk.
**Plan:** background mover that pushes age-out SSTables to object storage
and lazily re-pulls them; needs a chunk-level cache on the read path.

---

## Backwards-compatibility commitments

- Wire format of `delta-backup-1-encrypted` envelopes is frozen once shipped.
  Future schema bumps will carry a different `format` string.
- The `/api/v1` prefix is stable. Breaking changes go behind `/api/v2`.
- Replication WAL frame layout is stable across patch releases.
- `/metrics` series names follow the `delta_*` convention; new series will
  be added at any time, existing series will not be renamed.
