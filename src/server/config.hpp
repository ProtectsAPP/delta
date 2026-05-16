// =============================================================================
// config.hpp — startup configuration for the server binary.
//
// P1-25: every field that exists in the struct is now read by both
//        --flag CLI args AND by the JSON config file (`from_file`). The
//        previous from_file() touched only 5 of the 16 fields and silently
//        ignored the rest, which made operators think they had set
//        cluster_token in the config file when they had not.
//
// P0-9 : new `CorsConfig` block. Empty `allowed_origins` (the default)
//        falls back to wide-open `*` so the dev experience doesn't break,
//        but operators can pin it to one or more origins for production.
//        See http_server.hpp for how the value is consumed.
// =============================================================================
#pragma once
#include "../core/common.hpp"
#include "../core/constants.hpp"

namespace delta::server {

struct CorsConfig {
    // Empty = legacy permissive behavior ("Access-Control-Allow-Origin: *").
    // Non-empty = the browser must have an Origin in this list to be
    //             granted CORS headers; OPTIONS preflight returns 204
    //             without ACAO if the Origin doesn't match.
    std::vector<std::string> allowed_origins;
    std::vector<std::string> allowed_methods = {"GET","POST","PATCH","DELETE","OPTIONS"};
    std::vector<std::string> allowed_headers = {"Authorization","Content-Type","X-Delta-Token"};
    bool allow_credentials = true;
};

struct ServerConfig {
    std::string data_dir = "./data";
    std::string http_host = "0.0.0.0";
    int http_port = 16888;
    size_t cache_max_keys = 1000000;
    int max_connections = 1024;
    int idle_timeout_sec = 300;
    // HTTP worker threads. cpp-httplib pins one thread per persistent
    // connection, so this should be >= expected concurrent clients.
    // 0 = auto: max(256, max_connections).
    int http_threads = 0;
    int keepalive_max_count = 1000;
    int keepalive_timeout_sec = 30;

    // One-shot recovery: on startup, force admin status=active + reset
    // failed_attempts. Use after a lockout from too many bad password tries.
    bool reset_admin = false;

    // Native deltaql:// TCP protocol. 0 disables.
    int deltaql_port = 16889;
    // WebSocket bridge (browsers / JS clients). 0 disables.
    int ws_port = 16890;

    // Replication / clustering. role: "standalone" | "master" | "replica".
    std::string role = "standalone";
    std::string master_url;       // required when role == "replica"
    std::string cluster_token;    // shared secret between master and replicas

    // P0-9: CORS policy applied to the HTTP listener.
    CorsConfig cors;

    // Logger.
    std::string log_level = "info";    // debug | info | warn | error | off
    std::string log_file;              // optional path to mirror stderr into

    // Observability.
    double slow_query_ms = 500.0;      // anything >= this duration -> slow.log

    // Connection rate limit at the HTTP intake (per remote IP, token bucket).
    // 0 disables. Bucket refills `conn_rate_per_sec` tokens per second and
    // is capped at `conn_rate_burst` tokens.
    int conn_rate_per_sec = 0;
    int conn_rate_burst   = 0;

    // TLS (cpp-httplib OpenSSL build). Empty = plain HTTP.
    std::string tls_cert;
    std::string tls_key;

    // Backup encryption: optional AES-256-GCM passphrase applied to the
    // /admin/backup payload. Empty = unencrypted JSON.
    std::string backup_passphrase;

    // -------------------------------------------------------------------------
    // Raft cluster (Round 2 part 3).
    //
    // enable_raft = true switches the leader write path on. Every LSMTree::put
    // / LSMTree::del routes through RaftNode::propose, blocks until commit,
    // and applies on every replica through LsmRaftStateMachine. The legacy
    // master/replica streaming (role/master_url) stays in the binary but is
    // not used when enable_raft is true.
    //
    // node_id MUST be unique within the cluster and stable across restarts.
    //
    // cluster_peers is a vector of "id@host:port" strings, e.g.
    //   "n1@127.0.0.1:16888". The local node MUST be included so the peer
    //   majority math works.
    // -------------------------------------------------------------------------
    bool enable_raft = false;
    std::string node_id;
    std::vector<std::string> cluster_peers;     // "id@host:port"
    // Raft tunables. Defaults are conservative for cross-WAN latency.
    int  raft_election_min_ms = 200;
    int  raft_election_max_ms = 400;
    int  raft_heartbeat_ms    = 50;
    int  raft_tick_ms         = 20;
    bool raft_pre_vote        = true;
    // Snapshot threshold in committed entries. 0 disables auto-snapshot.
    int  raft_snapshot_threshold = 10000;
    // Per-write propose+commit deadline. If exceeded, put() throws and the
    // HTTP layer surfaces it as a 500 to the caller.
    int  raft_propose_timeout_ms = 5000;

    // -------------------------------------------------------------------------
    // Sharding (Round 3).
    //
    // enable_sharding = true turns on the routing prefilter on the HTTP
    // listener. shard_id is THIS process' shard (must appear in `shards`).
    // shards lists every shard in the cluster using the CLI form
    //   "shard_id=node@host:port,node@host:port,..."
    //
    // shard_vnodes is the number of consistent-hash virtual nodes per
    // shard (>= 128). Higher = smoother key distribution, lower = less
    // memory; the default 256 is a good balance.
    //
    // Sharding and raft are orthogonal: a single-shard cluster can still
    // use raft for replication, and a multi-shard cluster typically uses
    // raft INSIDE each shard.
    // -------------------------------------------------------------------------
    bool enable_sharding   = false;
    std::string shard_id;
    std::vector<std::string> shards;          // "shard_id=peer,peer,..."
    int  shard_vnodes      = 256;
    // Per-cross-shard-call deadline. Conservative because the gateway
    // may have to follow a leader hint and retry once.
    int  shard_rpc_timeout_ms = 2500;

    // to_json() never includes cluster_token — it lands in startup logs.
    json to_json() const {
        return {
            {"data_dir", data_dir},
            {"http_host", http_host},
            {"http_port", http_port},
            {"cache_max_keys", cache_max_keys},
            {"max_connections", max_connections},
            {"idle_timeout_sec", idle_timeout_sec},
            {"http_threads", http_threads},
            {"keepalive_max_count", keepalive_max_count},
            {"keepalive_timeout_sec", keepalive_timeout_sec},
            {"reset_admin", reset_admin},
            {"deltaql_port", deltaql_port},
            {"ws_port", ws_port},
            {"role", role},
            {"master_url", master_url},
            {"cluster_token_set", !cluster_token.empty()},
            {"cors_origins", cors.allowed_origins},
            {"enable_raft", enable_raft},
            {"node_id", node_id},
            {"cluster_peers", cluster_peers},
            {"enable_sharding", enable_sharding},
            {"shard_id", shard_id},
            {"shards", shards},
            {"shard_vnodes", shard_vnodes},
        };
    }
    static ServerConfig from_args(int argc, char** argv);
    static ServerConfig from_file(const std::string& path);

    // Parsed cluster peer spec. cluster_peers entries look like
    // "id@host:port". parse_peer_spec returns id="" on malformed input so
    // callers can reject without exceptions.
    struct PeerSpec {
        std::string id;
        std::string host;
        int port = 0;
        std::string base_url() const {
            return std::string("http://") + host + ":" + std::to_string(port);
        }
        bool valid() const { return !id.empty() && !host.empty() && port > 0; }
    };
    static PeerSpec parse_peer_spec(const std::string& s);
};

} // namespace delta::server
