#include "config.hpp"
#include "../storage/lsm_tree.hpp"
#include "../core/collection.hpp"
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

using namespace delta;
static network::HttpServer* g_srv = nullptr;
static network::DeltaQLServer* g_dql = nullptr;
static network::ws::WebSocketServer* g_ws = nullptr;
static void on_sig(int) {
    if (g_dql) g_dql->stop();
    if (g_ws)  g_ws->stop();
    if (g_srv) g_srv->stop();
    std::exit(0);
}

int main(int argc, char** argv) {
    auto cfg = server::ServerConfig::from_args(argc, argv);
    std::cout << "[Delta] starting with config: " << cfg.to_json().dump() << std::endl;

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
                                    cfg.keepalive_timeout_sec, cfg.max_connections};
    network::HttpServer server(store.get(), col.get(), cache.get(), vec.get(),
                                auth.get(), sessions.get(), dbm.get(), pool.get(), ht, repl.get());
    g_srv = &server;
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
        g_dql = dql.get();
        try { dql->start(); }
        catch (const std::exception& e) { std::cerr << "[Delta] deltaql disabled: " << e.what() << "\n"; dql.reset(); g_dql = nullptr; }
    }
    if (cfg.ws_port > 0) {
        wss = std::make_unique<network::ws::WebSocketServer>(
            cfg.http_host, cfg.ws_port, lb.get(), cache.get());
        g_ws = wss.get();
        try { wss->start(); }
        catch (const std::exception& e) { std::cerr << "[Delta] websocket disabled: " << e.what() << "\n"; wss.reset(); g_ws = nullptr; }
    }

    http_thread.join();
    running = false;
    return 0;
}
