# Delta — document DB + cache + vector search + admin console

[![Repo](https://img.shields.io/badge/github-ProtectsAPP%2Fdelta-4ade80?logo=github)](https://github.com/ProtectsAPP/delta)
[![llms.txt](https://img.shields.io/badge/llms.txt-AI--ready-7c3aed)](./llms.txt)

Delta is a high-performance, single-binary, multi-engine database that bundles **document storage**, **Redis-style key/value & data-structure cache**, **HNSW vector search**, **users / roles / permissions**, **multi-database + schema isolation**, **row-level security (RLS)**, and a **web admin console** into one process.

> **For developers and LLM coding agents:** Delta ships with [`llms.txt`](./llms.txt) — a **standalone tutorial with a built-in ~80-line client class you copy into your project**. No SDK to install, no package to manage. The recommended way to use Delta is to open one persistent `deltaql://` TCP connection (the MySQL-equivalent for Delta — stateful, sticky-auth, multiplexed by 32-bit `rid`, with server-push pubsub on the same socket) and reuse it for the whole life of your process. The full client and protocol reference is in [`llms.txt`](./llms.txt) §2; every running server also serves it at `GET /llms.txt`.

## Connecting

Pick your transport:

| Use case                                                       | URL                       | Where to copy the client from |
| -------------------------------------------------------------- | ------------------------- | ----------------------------- |
| Backend service / agent / daemon (recommended)                 | `deltaql://<host>:16889`  | [`llms.txt`](./llms.txt) §2.2 — ~80 lines of stdlib Python |
| Browser / mobile / edge runtime                                | `ws://<host>:16890/`      | [`llms.txt`](./llms.txt) §3 — ~50 lines of plain JS |
| One-off `curl` / migration script                              | `http://<host>:16888`     | Just `curl`, see [`llms.txt`](./llms.txt) §4 |

There are no client SDKs to install. Every transport is just bytes — `llms.txt` shows you the exact wire format and a copy-pasteable ~80-line client class for each language.

## One-line install

```bash
curl -fsSL https://raw.githubusercontent.com/ProtectsAPP/delta/main/install.sh | bash
```

The installer:

1. Checks for `git / cmake / make / cc` build prerequisites.
2. Clones the source into `~/.local/share/delta`.
3. Builds `delta_server` (a single static binary, < 4 MB).
4. Symlinks `delta-server` into `~/.local/bin`.
5. Creates the data directory `~/.deltaql` (persists WAL + SST + indexes).
6. Registers Delta as a system service:
   - macOS: `launchd` agent → `com.delta.server`
   - Linux: `systemd --user` unit → `delta.service`
7. Starts it and verifies via `/api/v1/health`.

Override defaults with environment variables:

```bash
DELTA_DATA=~/data DELTA_PORT=18000 \
  curl -fsSL https://raw.githubusercontent.com/ProtectsAPP/delta/main/install.sh | bash

# Install only — don't register a service
DELTA_NO_SERVICE=1 curl -fsSL .../install.sh | bash
```

## Architecture

```
┌─────────────────────────────────────────────┐
│  NaiveUI Console (Vue 3 + Pinia + Vite)     │
│   Dashboard | DBs | Collections | Documents │
│   Query | Vector | Cache | Users | Roles    │
│   Permissions | Policies | Connections      │
└──────────────────────┬──────────────────────┘
                       │ HTTP REST /api/v1
                       │ WebSocket  /  (port 16890)
                       │ deltaql://    (port 16889, persistent)
┌──────────────────────┴──────────────────────┐
│         Delta Server (C++17)                │
│  ┌───────────┐ ┌─────────┐ ┌─────────┐      │
│  │ Document  │ │  Cache  │ │ Vector  │      │
│  │  Engine   │ │ Engine  │ │  HNSW   │      │
│  └─────┬─────┘ └────┬────┘ └────┬────┘      │
│        │            │            │           │
│  ┌─────┴────────────┴────────────┴────┐     │
│  │        Storage (LSM + WAL)         │     │
│  └────────────────────────────────────┘     │
│  ┌─────────────────────────────────────┐    │
│  │   Auth: Users / Roles / Permissions │    │
│  │   Databases / Schemas / RLS / Pool  │    │
│  └─────────────────────────────────────┘    │
└─────────────────────────────────────────────┘
```

## Feature matrix

| Module | What's there |
| --- | --- |
| **Documents** | Collection CRUD; nested-field queries (`address.city`); `$eq/$ne/$gt/$gte/$lt/$lte/$in/$nin/$exists/$regex/$contains`; `$and/$or/$nor`; sort; pagination; projection; bulk insert |
| **Updates** | `$set / $unset / $inc / $mul / $push / $pull / $addToSet / $rename` |
| **Aggregation** | `$match / $group / $sort / $limit / $skip` with `$sum / $avg / $min / $max / $count` |
| **Indexes** | B+ tree / hash / full-text / vector; unique constraints; auto-maintained |
| **Storage** | LSM-Tree (MemTable + SSTable + WAL); crash recovery |
| **Cache** | String / Hash / List / Set / Sorted Set / Pub-Sub / TTL / LRU eviction / counters |
| **Vectors** | HNSW with cosine / euclidean / dot distance; multiple indexes per collection; bulk insert |
| **Users** | Create, modify, salted+iterated SHA-256 passwords, expiry, connection limits, lock/unlock, auto-lock after 5 failed logins |
| **Roles** | Role inheritance; system roles `superuser / db_admin / user_admin / read_write / read_only / public` |
| **Permissions** | 14 privilege bits (SELECT/INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/INDEX/TRUNCATE/USAGE/CONNECT/EXECUTE/...); GRANT / REVOKE; WITH GRANT OPTION |
| **Databases** | Multi-database, multi-schema; default DB `default`, system DB `delta_system` |
| **Row-level security** | RLS policies with USING / WITH CHECK expressions, per-command scope (ALL/SELECT/INSERT/...), evaluated per role |
| **Sessions / connections** | Bearer-token auth; connection pool stats / limits per user / DB; idle reaping; force-close |
| **Persistent protocol** | `deltaql://` TCP on `:16889` — stateful, sticky-auth, multiplexed by 32-bit `rid`, server-push pubsub on the same socket |
| **WebSocket bridge** | `ws://` on `:16890/` — same persistent protocol, JSON-framed, browser-friendly |
| **REST API** | Full CRUD plus users / roles / permissions / databases / schemas / RLS / cache / vectors / stats |
| **Admin console** | 14 pages: login, dashboard, databases, collections, documents, query, vectors, cache, users, roles, permissions, RLS policies, connections, monitoring, settings |

## Repository layout

```
delta/
├── CMakeLists.txt
├── third_party/        json.hpp, httplib.h
├── src/
│   ├── core/           common.hpp, document.hpp, collection.hpp
│   ├── storage/        lsm_tree.hpp (WAL + MemTable + SSTable)
│   ├── cache/          cache_engine.hpp
│   ├── vector/         hnsw_index.hpp
│   ├── auth/           password.hpp, auth_manager.hpp
│   ├── database/       database_manager.hpp (DB / schema / RLS)
│   ├── network/        http_server.hpp, deltaql_protocol.hpp,
│   │                   deltaql_server.hpp, ws_server.hpp,
│   │                   connection_pool.hpp, replication.hpp
│   └── server/         main.cpp, config.{hpp,cpp}
├── llms.txt            standalone HTTP+JSON tutorial with built-in client
├── console/            Vue 3 + NaiveUI admin console
├── tests/unit/         C++ unit tests
└── data/               runtime data directory (created at first start)
```

## Build & run

### Backend (C++)

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j8
ctest --test-dir build                   # unit tests
./build/delta_server --data ./data \
  --port 16888 --deltaql-port 16889 --ws-port 16890
```

### Admin console (Vue 3)

```bash
cd console
cnpm install
cnpm run dev                             # dev mode (proxies /api → 16888)
cnpm run build                           # production build into dist/
```

Open <http://localhost:5173> (or whatever port Vite picks) and log in with the default account **admin / admin**.

## REST API at a glance

All paths are prefixed with `/api/v1`. After login, requests must carry `Authorization: Bearer <token>`. (Over `deltaql://` and `ws://`, auth is sticky to the connection — set once, never re-sent.)

```
POST /auth/login                                  log in
POST /auth/logout
GET  /auth/me
POST /use                                         switch active db / schema

GET|POST /databases ; GET|PATCH|DELETE /databases/{name}
POST|GET|DELETE /databases/{db}/schemas[/{name}]

GET|POST /collections ; GET|DELETE /collections/{name}
POST|DELETE /collections/{name}/indexes[/{idx}]

POST /collections/{c}/documents              insert
POST /collections/{c}/documents/bulk         bulk insert
GET|PATCH|DELETE /collections/{c}/documents/{id}
POST /collections/{c}/documents/search       MongoDB-style filter
POST /collections/{c}/aggregate              aggregation pipeline
POST /collections/{c}/count

POST|DELETE /collections/{c}/vectors[/{id}]
POST /collections/{c}/vectors/search

POST|GET|DELETE /cache, /cache/{key}
POST /cache/{k}/incr ; /cache/{k}/expire
POST|GET|DELETE /cache/{k}/hash[/{f}]
POST|GET /cache/{k}/list[/push]
POST|GET|DELETE /cache/{k}/set[/{m}]
POST|GET /cache/{k}/zset
POST /pubsub/publish

GET|POST|PATCH|DELETE /users[/{name}][/lock|unlock|roles[/{r}]]
GET|POST|DELETE /roles[/{name}]
POST /permissions/grant ; /permissions/revoke ; /permissions/grant-all
GET  /permissions

POST|GET|DELETE /databases/{db}/schemas/{s}/collections/{c}/policies[/{p}]
POST /databases/{db}/schemas/{s}/collections/{c}/rls

GET  /connections  ;  DELETE /connections/{id}
GET  /status   ;   GET /stats   ;   GET /health
GET  /llms.txt                                    (no auth — for AI agents)
```

For every endpoint above, the request body is identical whether you send it over `deltaql://`, `ws://`, or `http://` — see `llms.txt` §5–§15.

## Default credentials

| User | Password | Role |
|---|---|---|
| `admin` | `admin` | `superuser` |

On first start Delta creates the `default` and `delta_system` databases plus the system roles `superuser / db_admin / user_admin / read_write / read_only / public`.

## Tests

- `./build/test_basic` — built-in C++ unit tests (storage / collection / cache / HNSW / auth / databases).
- The `llms.txt` §2.2 reference client (~80 lines of stdlib Python) doubles as an end-to-end smoke test: copy it into a file, point it at a running `delta_server`, and you have a full multiplexed pubsub round-trip in 5 seconds.

## Performance characteristics

- LSM-Tree write path with WAL durability for crash safety.
- HNSW approximate-nearest-neighbour search in O(log N).
- LRU + TTL cache for hot-path acceleration.
- Single process, multi-threaded, I/O and compute concurrent.
- Persistent `deltaql://` clients see ~6,500 req/s for `/health` with 200 concurrent in-flight requests on a single socket (loopback, MacBook Pro M-series), versus the per-request TCP+HTTP setup tax HTTP clients pay.

## License

See [`LICENSE`](./LICENSE).
