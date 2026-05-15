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

    // P1-24: refuse to silently expose cluster admin endpoints with no token.
    // We don't *fail* startup so single-node dev still works, but we shout.
    if (cfg.role != "standalone" && cfg.cluster_token.empty()) {
        std::cerr
            << "[Delta][WARN] role=" << cfg.role
            << " but --cluster-token is empty. /cluster/* endpoints are unauthenticated.\n"
            << "             Set --cluster-token=<long-random-string> for any non-dev deployment."
            << std::endl;
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

    std::unique_ptr<network::DeltaQLServer>     dql;
    std::unique_ptr<network::ws::WebSocketServer> wss;
    std::unique_ptr<network::HttpLoopback>      lb;
    if (cfg.deltaql_port > 0 || cfg.ws_port > 0) {
        lb = std::make_unique<network::HttpLoopback>("127.0.0.1", cfg.http_port);
    }
    if (cfg.deltaql_port > 0) {
        dql = std::make_unique<network::DeltaQLServer>(
            cfg.http_host, cfg.deltaql_port, lb.get(), cache.get());
        try { dql->start(); }
        catch (const std::exception& e) { std::cerr << "[Delta] deltaql disabled: " << e.what() << "\n"; dql.reset(); }
    }
    if (cfg.ws_port > 0) {
        wss = std::make_unique<network::ws::WebSocketServer>(
            cfg.http_host, cfg.ws_port, lb.get(), cache.get());
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
    if (dql) dql->stop();
    if (wss) wss->stop();
    server.stop();
    if (http_thread.joinable()) http_thread.join();
    running = false;
    return 0;
}
