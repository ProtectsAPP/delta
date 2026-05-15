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

#### A.1 Automatic failover
**Status:** manual `promote` only.
**Plan:** lightweight Raft-style leader election over the existing cluster
token + heartbeat. The replication WAL is already idempotent (LSN-keyed), so
the storage layer is ready; what's missing is consensus on *which* replica
becomes the new master. Plan to integrate a small Raft library (e.g. NuRaft
or a hand-rolled implementation, the metadata footprint is tiny) rather than
inventing a custom protocol.
**Estimated effort:** ~2 weeks engineering + 1 week chaos testing.

#### A.2 TLS termination in-process — *shipped (rev. May 2026)*
**Status:** done. Build with `-DDELTA_TLS=ON`; pass `--tls-cert` /
`--tls-key` at startup. cpp-httplib's `SSLServer` is used in place of the
plain `Server`, and the `HttpServer` route registry is identical. Default
build stays OpenSSL-free; the same flags on a non-TLS build warn and fall
back to plain HTTP (so reverse-proxy deployments are unaffected).

#### A.3 Data sharding
**Status:** single-node store can hold whatever fits on one disk.
**Plan:** consistent-hash router in front of N independent Delta nodes; the
collection-engine already namespaces keys by `db:schema:collection`, so the
shard key can be appended at the prefix level. Cross-shard transactions and
secondary-index queries are out of scope for v1 of this feature; offline
re-shard tooling will land alongside.
**Estimated effort:** ~3 weeks engineering + migration tooling.

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
