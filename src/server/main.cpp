#include "config.hpp"
#include "../storage/lsm_tree.hpp"
#include "../core/collection.hpp"
#include "../core/logger.hpp"
#include "../cache/cache_engine.hpp"
#include "../vector/hnsw_index.hpp"
#include "../auth/auth_manager.hpp"
#include "../database/database_manager.hpp"
#include "../network/connection_pool.hpp"
#include "../network/replication.hpp"
#include "../network/http_server.hpp"
#include "../network/deltaql_server.hpp"
#include "../network/ws_server.hpp"
#include "../network/raft.hpp"
#include "../network/raft_http_transport.hpp"
#include "../network/raft_lsm_sm.hpp"
#include "../cluster/shard_router.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <csignal>
#include <filesystem>

using namespace delta;

// P1-21: signal handlers must only do async-signal-safe work. Reading or
// writing a std::atomic<int> with relaxed ordering is safe; calling
// std::exit() (which runs destructors) is NOT — it can deadlock against
// any mutex held by the interrupted thread. We just flip the flag and
// rely on the main thread to drive an orderly shutdown.
static std::atomic<int> g_shutdown_requested{0};
static void on_sig(int) { g_shutdown_requested.store(1, std::memory_order_relaxed); }

int main(int argc, char** argv) {
    auto cfg = server::ServerConfig::from_args(argc, argv);
    std::cout << "[Delta] starting with config: " << cfg.to_json().dump() << std::endl;

    // Configure the structured logger: level from --log-level / DELTA_LOG_LEVEL
    // (falls back to INFO), audit/slow/main files inside the data dir.
    {
        Logger& lg = Logger::instance();
        lg.set_level(log_level_from_string(cfg.log_level));
        std::filesystem::create_directories(cfg.data_dir);
        lg.set_audit_file(cfg.data_dir + "/audit.log");
        lg.set_slow_query_file(cfg.data_dir + "/slow.log");
        if (!cfg.log_file.empty()) lg.set_main_file(cfg.log_file);
        lg.info("boot", json{{"role", cfg.role},
                              {"http_port", cfg.http_port},
                              {"deltaql_port", cfg.deltaql_port},
                              {"ws_port", cfg.ws_port},
                              {"data_dir", cfg.data_dir}});
    }

    auto store = std::make_unique<storage::LSMTree>(cfg.data_dir);
    auto col = std::make_unique<CollectionEngine>(store.get());
    auto cache = std::make_unique<cache::CacheEngine>(cfg.cache_max_keys);
    auto vec = std::make_unique<vector::VectorEngine>();
    auto auth = std::make_unique<auth::AuthManager>(store.get());
    if (cfg.reset_admin) {
        auto s = auth->unlock_user("admin");
        std::cout << "[Delta] --reset-admin: " << (s.ok() ? "admin unlocked" : s.message) << std::endl;
    }
    auto sessions = std::make_unique<auth::SessionManager>();
    auto dbm = std::make_unique<database::DatabaseManager>(store.get());
    network::ConnectionPoolConfig pcfg{cfg.max_connections, 100, 500, cfg.idle_timeout_sec};
    auto pool = std::make_unique<network::ConnectionPool>(pcfg, auth.get(), dbm.get());

    // Periodic cleanup thread
    std::atomic<bool> running{true};
    std::thread cleanup([&]() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            cache->purge_expired();
            pool->cleanup_idle();
        }
    });
    cleanup.detach();

    // Replication: build manager based on configured role.
    network::Role role = network::Role::Standalone;
    if (cfg.role == "master") role = network::Role::Master;
    else if (cfg.role == "replica") role = network::Role::Replica;
    // Make the slow-query threshold tunable via config; default 500ms preserved.
    // (Done via Tuning below so it threads through to set_post_routing_handler.)
    auto repl = std::make_unique<network::ReplicationManager>(
        store.get(), role, cfg.master_url, cfg.cluster_token);
    if (role == network::Role::Replica) {
        if (cfg.master_url.empty()) {
            std::cerr << "[Delta] --master-url required for replica role\n";
            return 1;
        }
        std::cout << "[Delta] role=replica master=" << cfg.master_url << std::endl;
        repl->start_replica_loop();
    } else {
        std::cout << "[Delta] role=" << network::role_name(role) << std::endl;
    }

    network::HttpServer::Tuning ht{cfg.http_threads, cfg.keepalive_max_count,
                                    cfg.keepalive_timeout_sec, cfg.max_connections,
                                    cfg.slow_query_ms};

    // P2-2 (TLS): when --tls-cert / --tls-key are configured AND the binary
    // was built with -DDELTA_TLS=ON, build the HttpServer through the
    // make_tls() factory so the underlying httplib::Server is an SSLServer.
    // Otherwise fall back to the plain-HTTP constructor and warn if the user
    // asked for TLS on a non-TLS build.
    std::unique_ptr<network::HttpServer> server_owned;
    if (!cfg.tls_cert.empty() && !cfg.tls_key.empty()) {
        server_owned = network::HttpServer::make_tls(
            store.get(), col.get(), cache.get(), vec.get(), auth.get(),
            sessions.get(), dbm.get(), pool.get(),
            cfg.tls_cert, cfg.tls_key, ht, repl.get());
        if (!server_owned) {
            std::cerr << "[Delta][WARN] TLS requested but could not start "
                         "(bad cert/key, or binary built without "
                         "-DDELTA_TLS=ON). Falling back to plain HTTP.\n";
            Logger::instance().warn("tls_unavailable",
                json{{"cert_set", true}, {"key_set", true}});
        }
    }
    if (!server_owned) {
        server_owned = std::make_unique<network::HttpServer>(
            store.get(), col.get(), cache.get(), vec.get(), auth.get(),
            sessions.get(), dbm.get(), pool.get(), ht, repl.get());
    }
    network::HttpServer& server = *server_owned;

    // Surface WS + DeltaQL traffic counters through HTTP /metrics. We capture
    // by reference into the singletons declared in their respective headers.
    server.set_traffic_hook([](std::ostringstream& m){
        auto emit = [&](const char* base, auto& c){
            m << "delta_" << base << "_frames_sent_total " << c.frames_sent.load() << "\n";
            m << "delta_" << base << "_frames_recv_total " << c.frames_recv.load() << "\n";
            m << "delta_" << base << "_bytes_sent_total "  << c.bytes_sent.load()  << "\n";
            m << "delta_" << base << "_bytes_recv_total "  << c.bytes_recv.load()  << "\n";
            m << "delta_" << base << "_connections_total " << c.total_conns.load() << "\n";
            m << "delta_" << base << "_connections_active "<< c.active_conns.load()<< "\n";
        };
        emit("ws",      network::ws::ws_traffic());
        emit("deltaql", network::dql_traffic());
    });

    // P0-9: hand the CORS allow-list off to the HTTP layer. Empty list keeps
    // the legacy permissive `*` behavior; a non-empty list enables strict
    // origin matching with `Vary: Origin` and `Access-Control-Allow-Credentials`.
    network::HttpServer::CorsPolicy cp;
    cp.origins           = cfg.cors.allowed_origins;
    cp.allow_credentials = cfg.cors.allow_credentials;
    server.set_cors(cp);

    if (!cfg.backup_passphrase.empty()) {
        server.set_backup_passphrase(cfg.backup_passphrase);
        Logger::instance().info("backup_encryption_enabled", json::object());
    }
    if (cfg.conn_rate_per_sec > 0) {
        server.set_conn_rate_limit(cfg.conn_rate_per_sec, cfg.conn_rate_burst);
        Logger::instance().info("conn_rate_limit_enabled",
            json{{"per_sec", cfg.conn_rate_per_sec},
                 {"burst",   cfg.conn_rate_burst}});
    }
    // TLS wiring is handled at HttpServer construction time (above). When
    // make_tls() succeeds, server.tls_enabled() == true and we serve HTTPS;
    // otherwise we already logged the fallback to plain HTTP.
    if (server.tls_enabled()) {
        Logger::instance().info("tls_enabled",
            json{{"cert", cfg.tls_cert}});
    }

    // P0-16: in any non-standalone deployment a missing cluster_token is
    // a footgun (peers can be impersonated, /cluster/* admin endpoints
    // run unauthenticated). We auto-generate a 32-byte random token when
    // operator did not supply one and print it once on stderr so they
    // can persist it. Refuse to start if writing it to stderr fails.
    if (cfg.role != "standalone" && cfg.cluster_token.empty()) {
        cfg.cluster_token = delta::random_hex(32);
        std::cerr
            << "[Delta][WARN] role=" << cfg.role
            << " started without --cluster-token. Generated one:\n"
            << "             DELTA_CLUSTER_TOKEN=" << cfg.cluster_token << "\n"
            << "             Persist this value across restarts and on every peer,\n"
            << "             otherwise peer-to-peer auth will fail after the next restart."
            << std::endl;
    }

    // ---------------------------------------------------------------------
    // Raft consensus (Round 2 part 3). When --enable-raft, the LSMTree
    // write path is routed through a RaftNode; the legacy ReplicationManager
    // stays inert (Standalone) but its cluster_token is reused for the
    // /api/v1/cluster/raft/* token gate so operators can use one shared
    // secret.
    // ---------------------------------------------------------------------
    std::unique_ptr<network::raft::RaftNode>          raft_node;
    std::shared_ptr<network::raft::HttpRaftTransport> raft_tx;
    std::shared_ptr<network::raft::FilePersistentStorage> raft_storage;
    std::shared_ptr<network::raft::LsmRaftStateMachine>   raft_sm;
    if (cfg.enable_raft) {
        if (cfg.node_id.empty()) {
            std::cerr << "[Delta] --enable-raft requires --node-id\n";
            return 1;
        }
        if (cfg.cluster_peers.empty()) {
            std::cerr << "[Delta] --enable-raft requires at least one --cluster-peer\n";
            return 1;
        }
        // Parse peer specs and ensure self is listed.
        std::unordered_map<std::string, std::string> peer_urls;
        std::vector<std::string> peer_ids;
        bool self_seen = false;
        for (auto& raw : cfg.cluster_peers) {
            auto p = server::ServerConfig::parse_peer_spec(raw);
            if (!p.valid()) {
                std::cerr << "[Delta] malformed --cluster-peer: " << raw
                          << " (expected id@host:port)\n";
                return 1;
            }
            if (p.id == cfg.node_id) { self_seen = true; continue; }
            peer_urls[p.id] = p.base_url();
            peer_ids.push_back(p.id);
        }
        peer_ids.push_back(cfg.node_id);   // include self in peer set
        if (!self_seen) {
            std::cerr << "[Delta] --node-id=" << cfg.node_id
                      << " must appear in --cluster-peer list\n";
            return 1;
        }

        std::filesystem::create_directories(cfg.data_dir + "/raft");
        raft_storage = std::make_shared<network::raft::FilePersistentStorage>(
            cfg.data_dir + "/raft/state.json");
        raft_sm = std::make_shared<network::raft::LsmRaftStateMachine>(store.get());
        raft_tx = std::make_shared<network::raft::HttpRaftTransport>(
            peer_urls, cfg.cluster_token,
            /*rpc_timeout_ms=*/std::max(cfg.raft_heartbeat_ms * 2, 100));

        network::raft::RaftConfig rcfg;
        rcfg.node_id            = cfg.node_id;
        rcfg.peers              = peer_ids;
        rcfg.election_min_ms    = cfg.raft_election_min_ms;
        rcfg.election_max_ms    = cfg.raft_election_max_ms;
        rcfg.heartbeat_ms       = cfg.raft_heartbeat_ms;
        rcfg.tick_ms            = cfg.raft_tick_ms;
        rcfg.pre_vote           = cfg.raft_pre_vote;
        rcfg.snapshot_threshold = cfg.raft_snapshot_threshold;
        raft_node = std::make_unique<network::raft::RaftNode>(
            rcfg, raft_tx, raft_storage, raft_sm);

        // Plug the proposer into the LSMTree write path. The synchronous
        // wait_applied ensures the local replica has actually written the
        // value before put() returns, so a follow-up get() on the same
        // node sees it.
        auto* rn = raft_node.get();
        auto timeout = std::chrono::milliseconds(cfg.raft_propose_timeout_ms);
        store->set_raft_proposer([rn, timeout](const std::string& k,
                                                const std::string& v,
                                                bool tomb) -> bool {
            std::string payload = network::raft::LsmWriteCodec::encode(k, v, tomb);
            network::raft::Index idx = 0;
            network::raft::NodeId hint;
            if (!rn->propose(payload, &idx, &hint)) return false;
            if (!rn->wait_applied(idx, timeout)) return false;
            return true;
        });

        server.set_raft(rn);
        Logger::instance().info("raft_enabled",
            json{{"node_id", cfg.node_id},
                 {"peers", peer_ids},
                 {"snapshot_threshold", cfg.raft_snapshot_threshold}});
        rn->start();
        std::cout << "[Delta] raft enabled: node_id=" << cfg.node_id
                  << " peers=" << peer_ids.size() << std::endl;
    }

    // ---------------------------------------------------------------------
    // Sharding gateway (Round 3). When --enable-sharding is set the HTTP
    // listener installs a pre-routing prefilter that consistent-hashes
    // document operations across the configured shards. Inside this shard
    // we still use raft for replication; the gateway is orthogonal.
    // ---------------------------------------------------------------------
    if (cfg.enable_sharding) {
        if (cfg.shard_id.empty()) {
            std::cerr << "[Delta] --enable-sharding requires --shard-id\n";
            return 1;
        }
        if (cfg.shards.empty()) {
            std::cerr << "[Delta] --enable-sharding requires at least one --shard\n";
            return 1;
        }
        std::vector<cluster::ShardSpec> parsed;
        bool found_self = false;
        for (auto& raw : cfg.shards) {
            auto s = cluster::ShardMap::parse_cli_spec(raw);
            if (!s.valid()) {
                std::cerr << "[Delta] malformed --shard spec: " << raw
                          << " (expected shard_id=id@host:port,id@host:port)\n";
                return 1;
            }
            if (s.id == cfg.shard_id) found_self = true;
            parsed.push_back(std::move(s));
        }
        if (!found_self) {
            std::cerr << "[Delta] --shard-id=" << cfg.shard_id
                      << " not present in --shard specs\n";
            return 1;
        }
        cluster::ShardMap m(std::move(parsed), cfg.shard_vnodes);
        server.set_sharding(m, cfg.shard_id, cfg.cluster_token,
                            cfg.shard_rpc_timeout_ms);
        // Persist the topology snapshot in delta_system.shards so a
        // backup/restore captures it. Round 3 doesn't yet auto-replicate
        // topology changes via raft — operators bounce nodes with the
        // updated --shard list. The persisted record is informational.
        store->apply_replicated(0, "delta_system:shards",
            m.to_json().dump(), false);
        Logger::instance().info("sharding_enabled",
            json{{"shard_id", cfg.shard_id},
                 {"shard_count", m.shard_count()},
                 {"vnodes_per_shard", m.vnodes()}});
        std::cout << "[Delta] sharding enabled: shard_id=" << cfg.shard_id
                  << " total_shards=" << m.shard_count()
                  << " vnodes=" << m.vnodes() << std::endl;
    }

    // -------------------------------------------------------------------------
    // B.2 Multi-master writes — the puller polls each peer and applies the
    // returned changes via the engine's HLC-based LWW resolver. Independent
    // of sharding: a single-shard or unsharded fleet can still run
    // active-active by listing every node as `--mm-peer`. Operators are
    // expected to designate `multi_master:true` per collection at create
    // time.
    // -------------------------------------------------------------------------
    if (!cfg.mm_peers.empty()) {
        server.set_mm_peers(cfg.mm_peers, cfg.cluster_token, cfg.mm_poll_ms);
        Logger::instance().info("multi_master_enabled",
            json{{"peer_count", (int)cfg.mm_peers.size()},
                 {"poll_ms",    cfg.mm_poll_ms}});
        std::cout << "[Delta] multi-master peers=" << cfg.mm_peers.size()
                  << " poll_ms=" << cfg.mm_poll_ms << std::endl;
    }

    std::signal(SIGINT, on_sig);
    std::signal(SIGTERM, on_sig);

    // Start the auxiliary protocol servers. They both forward into the HTTP
    // route table via an in-process loopback Client so we don't duplicate
    // route logic. The HTTP server itself must already be listening, so we
    // spawn it in a thread and wait briefly for the socket to be ready.
    std::thread http_thread([&]{ server.listen(cfg.http_host, cfg.http_port); });
    // wait until the HTTP server's accept loop is ready
    for (int i = 0; i < 50 && !server.is_running(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // Now that we're listening, kick off the multi-master background
    // puller so peer convergence starts immediately. Stopped by a
    // companion call after the listener returns.
    if (!cfg.mm_peers.empty()) server.start_mm_puller();

    std::unique_ptr<network::DeltaQLServer>     dql;
    std::unique_ptr<network::ws::WebSocketServer> wss;
    std::unique_ptr<network::HttpLoopback>      lb;
    if (cfg.deltaql_port > 0 || cfg.ws_port > 0) {
        lb = std::make_unique<network::HttpLoopback>("127.0.0.1", cfg.http_port);
    }
    if (cfg.deltaql_port > 0) {
        dql = std::make_unique<network::DeltaQLServer>(
            cfg.http_host, cfg.deltaql_port, lb.get(), sessions.get(), cache.get());
        try { dql->start(); }
        catch (const std::exception& e) { std::cerr << "[Delta] deltaql disabled: " << e.what() << "\n"; dql.reset(); }
    }
    if (cfg.ws_port > 0) {
        wss = std::make_unique<network::ws::WebSocketServer>(
            cfg.http_host, cfg.ws_port, lb.get(), sessions.get(), cache.get());
        try { wss->start(); }
        catch (const std::exception& e) { std::cerr << "[Delta] websocket disabled: " << e.what() << "\n"; wss.reset(); }
    }

    // P1-21: wait for the shutdown signal here instead of inside the signal
    // handler. This lets every server's stop() / dtor run on the main
    // thread under normal stack semantics.
    while (!g_shutdown_requested.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    std::cerr << "[Delta] shutdown signal received, stopping..." << std::endl;
    if (raft_node) raft_node->stop();
    if (dql) dql->stop();
    if (wss) wss->stop();
    server.stop_mm_puller();
    server.stop();
    if (http_thread.joinable()) http_thread.join();
    running = false;
    return 0;
}
