#include "config.hpp"
#include "../core/validation.hpp"
#include <fstream>
#include <cstring>
#include <iostream>
#include <sstream>

namespace delta::server {

namespace {
// Split a comma-separated list (used for --cors-origin a,b,c).
std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        // trim ascii whitespace
        size_t l = tok.find_first_not_of(" \t");
        size_t r = tok.find_last_not_of(" \t");
        if (l == std::string::npos) continue;
        out.emplace_back(tok.substr(l, r - l + 1));
    }
    return out;
}
}

ServerConfig ServerConfig::from_args(int argc, char** argv) {
    using namespace delta::validation;
    ServerConfig c;
    // P1-20 / P1-25: every numeric flag goes through safe_stoi/safe_stoll
    // so a malformed value (e.g. `--port abc`) prints a warning and uses
    // the default rather than throwing std::invalid_argument from main().
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string { return (i + 1 < argc) ? std::string(argv[++i]) : std::string(); };
        if (a == "--data") c.data_dir = next();
        else if (a == "--host") c.http_host = next();
        else if (a == "--port") c.http_port = safe_stoi(next(), c.http_port);
        else if (a == "--threads") c.http_threads = safe_stoi(next(), c.http_threads);
        else if (a == "--max-conn") c.max_connections = safe_stoi(next(), c.max_connections);
        else if (a == "--idle-timeout") c.idle_timeout_sec = safe_stoi(next(), c.idle_timeout_sec);
        else if (a == "--keepalive-max") c.keepalive_max_count = safe_stoi(next(), c.keepalive_max_count);
        else if (a == "--keepalive-timeout") c.keepalive_timeout_sec = safe_stoi(next(), c.keepalive_timeout_sec);
        else if (a == "--cache-max-keys") c.cache_max_keys = (size_t)safe_stoll(next(), (long long)c.cache_max_keys);
        else if (a == "--deltaql-port") c.deltaql_port = safe_stoi(next(), c.deltaql_port);
        else if (a == "--ws-port") c.ws_port = safe_stoi(next(), c.ws_port);
        else if (a == "--no-deltaql") c.deltaql_port = 0;
        else if (a == "--no-ws") c.ws_port = 0;
        else if (a == "--reset-admin") c.reset_admin = true;
        else if (a == "--role") c.role = next();
        else if (a == "--master-url") c.master_url = next();
        else if (a == "--cluster-token") c.cluster_token = next();
        else if (a == "--cors-origin") {
            // Repeatable flag: comma-separated or one origin per occurrence.
            for (auto& o : split_csv(next())) c.cors.allowed_origins.push_back(o);
        }
        else if (a == "--log-level") c.log_level = next();
        else if (a == "--log-file")  c.log_file  = next();
        else if (a == "--slow-query-ms") c.slow_query_ms = std::atof(next().c_str());
        else if (a == "--conn-rate")  c.conn_rate_per_sec = safe_stoi(next(), c.conn_rate_per_sec);
        else if (a == "--conn-burst") c.conn_rate_burst   = safe_stoi(next(), c.conn_rate_burst);
        else if (a == "--tls-cert")   c.tls_cert = next();
        else if (a == "--tls-key")    c.tls_key  = next();
        else if (a == "--backup-passphrase") c.backup_passphrase = next();
        else if (a == "--enable-raft") c.enable_raft = true;
        else if (a == "--node-id") c.node_id = next();
        else if (a == "--cluster-peer") {
            // Repeatable. Comma-separated is also accepted, but the
            // canonical form is one --cluster-peer per peer.
            for (auto& p : split_csv(next())) c.cluster_peers.push_back(p);
        }
        else if (a == "--raft-election-min-ms") c.raft_election_min_ms = safe_stoi(next(), c.raft_election_min_ms);
        else if (a == "--raft-election-max-ms") c.raft_election_max_ms = safe_stoi(next(), c.raft_election_max_ms);
        else if (a == "--raft-heartbeat-ms")    c.raft_heartbeat_ms    = safe_stoi(next(), c.raft_heartbeat_ms);
        else if (a == "--raft-tick-ms")         c.raft_tick_ms         = safe_stoi(next(), c.raft_tick_ms);
        else if (a == "--raft-snapshot-threshold") c.raft_snapshot_threshold = safe_stoi(next(), c.raft_snapshot_threshold);
        else if (a == "--raft-propose-timeout-ms") c.raft_propose_timeout_ms = safe_stoi(next(), c.raft_propose_timeout_ms);
        else if (a == "--raft-no-pre-vote")     c.raft_pre_vote = false;
        else if (a == "--enable-sharding") c.enable_sharding = true;
        else if (a == "--shard-id") c.shard_id = next();
        else if (a == "--shard")    c.shards.push_back(next());
        else if (a == "--shard-vnodes") c.shard_vnodes = safe_stoi(next(), c.shard_vnodes);
        else if (a == "--shard-rpc-timeout-ms") c.shard_rpc_timeout_ms = safe_stoi(next(), c.shard_rpc_timeout_ms);
        else if (a == "--config") { c = from_file(next()); }
        else if (a == "--help" || a == "-h") {
            std::cout << "Delta Server\n"
                "Usage: delta_server [options]\n"
                "  --data DIR              Data directory (./data)\n"
                "  --host HOST             Bind host (0.0.0.0)\n"
                "  --port PORT             Bind port (16888)\n"
                "  --threads N             HTTP worker threads (auto = max(256, max-conn))\n"
                "  --max-conn N            Max concurrent connections (1024)\n"
                "  --keepalive-max N       Max keep-alive requests per connection (1000)\n"
                "  --keepalive-timeout N   Keep-alive timeout seconds (30)\n"
                "  --cache-max-keys N      Total cache key budget (1,000,000)\n"
                "  --deltaql-port PORT     DeltaQL native TCP port (16889, 0=disable)\n"
                "  --ws-port PORT          WebSocket bridge port (16890, 0=disable)\n"
                "  --no-deltaql            Disable native deltaql:// listener\n"
                "  --no-ws                 Disable WebSocket listener\n"
                "  --role ROLE             standalone | master | replica\n"
                "  --master-url URL        Master URL (required for replica)\n"
                "  --cluster-token TOKEN   Shared cluster secret\n"
                "  --idle-timeout N        Idle connection timeout seconds (300)\n"
                "  --cors-origin LIST      CORS allowlist (comma-sep or repeatable)\n"
                "  --reset-admin           Unlock + clear failed_attempts on admin user\n"
                "  --log-level LEVEL       debug|info|warn|error|off (default info)\n"
                "  --log-file PATH         mirror stderr to file (optional)\n"
                "  --slow-query-ms N       slow-query threshold ms (default 500)\n"
                "  --conn-rate N           per-IP token-bucket fill rate (0=off)\n"
                "  --conn-burst N          per-IP bucket capacity\n"
                "  --tls-cert PATH         enable TLS with this PEM cert\n"
                "  --tls-key  PATH         TLS private key (PEM)\n"
                "  --backup-passphrase S   enable AES-256-GCM backup encryption\n"
                "  --enable-raft           enable Raft consensus on the write path\n"
                "  --node-id ID            unique node id within the cluster (e.g. n1)\n"
                "  --cluster-peer SPEC     repeatable; format: id@host:port (include self)\n"
                "  --raft-election-min-ms N\n"
                "  --raft-election-max-ms N\n"
                "  --raft-heartbeat-ms N\n"
                "  --raft-tick-ms N\n"
                "  --raft-snapshot-threshold N   committed entries before auto-snapshot\n"
                "  --raft-propose-timeout-ms N   per-write commit deadline (default 5000)\n"
                "  --raft-no-pre-vote      disable §9.6 pre-vote (debug only)\n"
                "  --enable-sharding       enable consistent-hash sharding gateway\n"
                "  --shard-id ID           this process' shard id (must appear in --shard)\n"
                "  --shard SPEC            repeatable; shard_id=id@host:port,id@host:port\n"
                "  --shard-vnodes N        virtual nodes per shard (default 256, min 128)\n"
                "  --shard-rpc-timeout-ms N  cross-shard call deadline (default 2500)\n"
                "  --config FILE           Load JSON config\n";
            std::exit(0);
        }
    }
    return c;
}
// P1-25: full-coverage JSON loader. Every public field on ServerConfig is
// readable from the file. Unknown keys are ignored — nlohmann/json's
// `value()` swallows them — so old configs keep loading after schema
// extensions.
ServerConfig ServerConfig::from_file(const std::string& path) {
    ServerConfig c;
    std::ifstream f(path);
    if (!f) {
        std::cerr << "[Delta][WARN] config file not found: " << path << std::endl;
        return c;
    }
    json j;
    try { f >> j; }
    catch (const std::exception& e) {
        std::cerr << "[Delta][ERR] config parse failed: " << e.what() << std::endl;
        return c;
    }

    c.data_dir              = j.value("data_dir",               c.data_dir);
    c.http_host             = j.value("http_host",              c.http_host);
    c.http_port             = j.value("http_port",              c.http_port);
    c.cache_max_keys        = j.value("cache_max_keys",         c.cache_max_keys);
    c.max_connections       = j.value("max_connections",        c.max_connections);
    c.idle_timeout_sec      = j.value("idle_timeout_sec",       c.idle_timeout_sec);
    c.http_threads          = j.value("http_threads",           c.http_threads);
    c.keepalive_max_count   = j.value("keepalive_max_count",    c.keepalive_max_count);
    c.keepalive_timeout_sec = j.value("keepalive_timeout_sec",  c.keepalive_timeout_sec);
    c.deltaql_port          = j.value("deltaql_port",           c.deltaql_port);
    c.ws_port               = j.value("ws_port",                c.ws_port);
    c.role                  = j.value("role",                   c.role);
    c.master_url            = j.value("master_url",             c.master_url);
    c.cluster_token         = j.value("cluster_token",          c.cluster_token);
    c.reset_admin           = j.value("reset_admin",            c.reset_admin);
    c.log_level             = j.value("log_level",              c.log_level);
    c.log_file              = j.value("log_file",               c.log_file);
    c.slow_query_ms         = j.value("slow_query_ms",          c.slow_query_ms);
    c.conn_rate_per_sec     = j.value("conn_rate_per_sec",      c.conn_rate_per_sec);
    c.conn_rate_burst       = j.value("conn_rate_burst",        c.conn_rate_burst);
    c.tls_cert              = j.value("tls_cert",               c.tls_cert);
    c.tls_key               = j.value("tls_key",                c.tls_key);
    c.backup_passphrase     = j.value("backup_passphrase",      c.backup_passphrase);

    c.enable_raft              = j.value("enable_raft",              c.enable_raft);
    c.node_id                  = j.value("node_id",                  c.node_id);
    if (j.contains("cluster_peers") && j["cluster_peers"].is_array())
        c.cluster_peers = j["cluster_peers"].get<std::vector<std::string>>();
    c.raft_election_min_ms     = j.value("raft_election_min_ms",     c.raft_election_min_ms);
    c.raft_election_max_ms     = j.value("raft_election_max_ms",     c.raft_election_max_ms);
    c.raft_heartbeat_ms        = j.value("raft_heartbeat_ms",        c.raft_heartbeat_ms);
    c.raft_tick_ms             = j.value("raft_tick_ms",             c.raft_tick_ms);
    c.raft_pre_vote            = j.value("raft_pre_vote",            c.raft_pre_vote);
    c.raft_snapshot_threshold  = j.value("raft_snapshot_threshold",  c.raft_snapshot_threshold);
    c.raft_propose_timeout_ms  = j.value("raft_propose_timeout_ms",  c.raft_propose_timeout_ms);

    c.enable_sharding          = j.value("enable_sharding",          c.enable_sharding);
    c.shard_id                 = j.value("shard_id",                 c.shard_id);
    if (j.contains("shards") && j["shards"].is_array())
        c.shards = j["shards"].get<std::vector<std::string>>();
    c.shard_vnodes             = j.value("shard_vnodes",             c.shard_vnodes);
    c.shard_rpc_timeout_ms     = j.value("shard_rpc_timeout_ms",     c.shard_rpc_timeout_ms);

    if (j.contains("cors") && j["cors"].is_object()) {
        const auto& co = j["cors"];
        if (co.contains("allowed_origins") && co["allowed_origins"].is_array())
            c.cors.allowed_origins = co["allowed_origins"].get<std::vector<std::string>>();
        if (co.contains("allowed_methods") && co["allowed_methods"].is_array())
            c.cors.allowed_methods = co["allowed_methods"].get<std::vector<std::string>>();
        if (co.contains("allowed_headers") && co["allowed_headers"].is_array())
            c.cors.allowed_headers = co["allowed_headers"].get<std::vector<std::string>>();
        c.cors.allow_credentials = co.value("allow_credentials", c.cors.allow_credentials);
    }
    return c;
}

// Parse "id@host:port". Returns an empty-id PeerSpec when the format is
// invalid; the caller is expected to detect and warn. Whitespace is trimmed
// on the boundary characters so "n1 @ 127.0.0.1:16888" still works.
ServerConfig::PeerSpec ServerConfig::parse_peer_spec(const std::string& raw) {
    PeerSpec p;
    std::string s = raw;
    // trim
    auto l = s.find_first_not_of(" \t");
    auto r = s.find_last_not_of(" \t");
    if (l == std::string::npos) return p;
    s = s.substr(l, r - l + 1);

    auto at = s.find('@');
    if (at == std::string::npos) return p;
    auto colon = s.rfind(':');
    if (colon == std::string::npos || colon <= at + 1) return p;

    p.id   = s.substr(0, at);
    p.host = s.substr(at + 1, colon - at - 1);
    try { p.port = std::stoi(s.substr(colon + 1)); }
    catch (...) { p.port = 0; }
    if (p.id.empty() || p.host.empty() || p.port <= 0 || p.port > 65535) {
        p = PeerSpec{};  // mark invalid
    }
    return p;
}

}
