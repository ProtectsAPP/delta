// =============================================================================
// test_http_integration.cpp — end-to-end HTTP integration test driving a real
// `HttpServer` over loopback with the bundled httplib::Client.
//
// What this verifies:
//   * /api/v1/auth/login + bearer token round-trip
//   * X-Trace-Id propagation (request -> response echo)
//   * /metrics text-format presence of:
//       - delta_http_request_duration_ms_bucket{path=...,le=...}
//       - delta_http_responses_total{class="2xx"}
//       - delta_ws_frames_sent_total (traffic hook)
//   * /api/v1/admin/backup -> /api/v1/admin/restore round-trip on a second
//     fresh server pointing at a different data dir
//   * Audit log: audit.log JSONL contains a `login_succeeded` line for admin
//   * Backup encryption envelope ("delta-backup-1-encrypted") when a
//     passphrase is set, plus successful decrypt+restore by a second server
//     configured with the same passphrase.
//
// Runs against ports allocated from a high range, falling back if busy. The
// test deliberately disables the auxiliary protocol servers (DeltaQL/WS) to
// keep the test footprint small and avoid extra port allocations.
// =============================================================================
#include "../../src/storage/lsm_tree.hpp"
#include "../../src/core/collection.hpp"
#include "../../src/core/logger.hpp"
#include "../../src/cache/cache_engine.hpp"
#include "../../src/vector/hnsw_index.hpp"
#include "../../src/auth/auth_manager.hpp"
#include "../../src/database/database_manager.hpp"
#include "../../src/network/connection_pool.hpp"
#include "../../src/network/replication.hpp"
#include "../../src/network/http_server.hpp"

#include <httplib.h>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <thread>

using namespace delta;
namespace fs = std::filesystem;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

static std::string scratch(const std::string& tag) {
    std::string p = "./test_http_" + tag + "_" + std::to_string(now_ms());
    fs::remove_all(p);
    fs::create_directories(p);
    return p;
}

// Pick a port that isn't in use right now. We can't fully eliminate races
// with another process grabbing it, but the integration test is single-host.
static int pick_port() {
    std::random_device rd;
    return 23000 + (int)(rd() % 4000);
}

// A miniature stand-in for main.cpp that wires the server + sets the
// traffic-hook with stable counter sources so /metrics emits the expected
// lines regardless of whether the real WS/DeltaQL listeners are up.
struct ServerRig {
    std::string                          dir;
    std::unique_ptr<storage::LSMTree>    store;
    std::unique_ptr<CollectionEngine>    col;
    std::unique_ptr<cache::CacheEngine>  cache;
    std::unique_ptr<vector::VectorEngine> vec;
    std::unique_ptr<auth::AuthManager>   auth;
    std::unique_ptr<auth::SessionManager> sessions;
    std::unique_ptr<database::DatabaseManager> dbm;
    std::unique_ptr<network::ConnectionPool>   pool;
    std::unique_ptr<network::ReplicationManager> repl;
    std::unique_ptr<network::HttpServer> srv;
    std::thread                           th;
    int                                   port = 0;

    void start(const std::string& tag,
               const std::string& backup_passphrase = "") {
        dir = scratch(tag);
        Logger::instance().set_audit_file(dir + "/audit.log");
        Logger::instance().set_slow_query_file(dir + "/slow.log");
        store    = std::make_unique<storage::LSMTree>(dir);
        col      = std::make_unique<CollectionEngine>(store.get());
        cache    = std::make_unique<cache::CacheEngine>(10000);
        vec      = std::make_unique<vector::VectorEngine>();
        auth     = std::make_unique<auth::AuthManager>(store.get());
        sessions = std::make_unique<auth::SessionManager>();
        dbm      = std::make_unique<database::DatabaseManager>(store.get());
        network::ConnectionPoolConfig pcfg{256, 100, 500, 300};
        pool = std::make_unique<network::ConnectionPool>(pcfg, auth.get(), dbm.get());
        repl = std::make_unique<network::ReplicationManager>(
            store.get(), network::Role::Standalone, "", "");
        network::HttpTuning ht; ht.threads = 4;
        srv = std::make_unique<network::HttpServer>(
            store.get(), col.get(), cache.get(), vec.get(),
            auth.get(), sessions.get(), dbm.get(), pool.get(), ht, repl.get());
        if (!backup_passphrase.empty()) srv->set_backup_passphrase(backup_passphrase);

        // Tiny synthetic traffic hook so /metrics has WS/DeltaQL lines even
        // though we don't run those listeners in this test.
        srv->set_traffic_hook([](std::ostringstream& m){
            m << "delta_ws_frames_sent_total 0\n";
            m << "delta_ws_frames_recv_total 0\n";
            m << "delta_deltaql_frames_sent_total 0\n";
            m << "delta_deltaql_frames_recv_total 0\n";
        });

        // Try a handful of candidate ports before giving up.
        for (int i = 0; i < 8; ++i) {
            int p = pick_port();
            std::atomic<bool> bound{false};
            th = std::thread([&, p]{ try { srv->listen("127.0.0.1", p); }
                                     catch (...) {} bound = true; });
            for (int j = 0; j < 50; ++j) {
                if (srv->is_running()) { port = p; return; }
                std::this_thread::sleep_for(std::chrono::milliseconds(40));
            }
            srv->stop();
            if (th.joinable()) th.join();
        }
        throw std::runtime_error("could not bind any test port");
    }
    void stop() {
        srv->stop();
        if (th.joinable()) th.join();
    }
};

static std::string login(httplib::Client& c, const std::string& user,
                         const std::string& pw) {
    json b = {{"username", user}, {"password", pw}};
    auto r = c.Post("/api/v1/auth/login", b.dump(), "application/json");
    if (!r || r->status != 200) throw std::runtime_error("login failed");
    auto j = json::parse(r->body);
    return j["data"]["token"].get<std::string>();
}

static int run() {
    // ---- source server ------------------------------------------------------
    ServerRig src;
    src.start("src", /*passphrase=*/"correct horse battery staple");

    httplib::Client c1("127.0.0.1", src.port);
    c1.set_connection_timeout(2);
    c1.set_read_timeout(10);

    // 1) login + trace-id propagation
    std::string token = login(c1, "admin", "admin");
    CHECK(!token.empty());
    {
        httplib::Headers h = {{"Authorization", "Bearer " + token},
                              {"X-Trace-Id", "test-trace-1234567890abcdef"}};
        auto r = c1.Get("/api/v1/auth/me", h);
        CHECK(r);
        CHECK(r->status == 200);
        CHECK(r->get_header_value("X-Trace-Id") == "test-trace-1234567890abcdef");
    }
    // The audit log file must contain a login_succeeded line for admin.
    {
        std::ifstream f(src.dir + "/audit.log");
        std::string line, audit_blob;
        while (std::getline(f, line)) audit_blob += line + "\n";
        CHECK(audit_blob.find("login_succeeded") != std::string::npos);
        CHECK(audit_blob.find("\"user\":\"admin\"") != std::string::npos);
    }

    // 2) seed data so the backup roundtrip has something to do
    {
        httplib::Headers h = {{"Authorization", "Bearer " + token}};
        c1.Post("/api/v1/databases", h,
                json({{"name", "shop"}, {"owner", "admin"}}).dump(),
                "application/json");
        c1.Post("/api/v1/use", h,
                json({{"database", "shop"}, {"schema", "public"}}).dump(),
                "application/json");
        c1.Post("/api/v1/collections", h,
                json({{"name", "items"}, {"database", "shop"},
                      {"schema", "public"}}).dump(),
                "application/json");
        for (int i = 0; i < 3; ++i) {
            c1.Post("/api/v1/collections/items/documents?database=shop", h,
                    json({{"i", i}, {"name", "thing-" + std::to_string(i)}}).dump(),
                    "application/json");
        }
    }

    // 3) /metrics smoke + histogram presence
    {
        auto r = c1.Get("/metrics");
        CHECK(r && r->status == 200);
        const auto& b = r->body;
        CHECK(b.find("delta_http_request_duration_ms_bucket") != std::string::npos);
        CHECK(b.find("delta_http_responses_total{class=\"2xx\"}") != std::string::npos);
        CHECK(b.find("delta_ws_frames_sent_total") != std::string::npos);
        CHECK(b.find("delta_deltaql_frames_sent_total") != std::string::npos);
        CHECK(b.find("delta_http_rate_limited_total") != std::string::npos);
    }

    // 4) GET /admin/backup, encrypted because we configured a passphrase.
    json backup_data;
    {
        httplib::Headers h = {{"Authorization", "Bearer " + token}};
        auto r = c1.Get("/api/v1/admin/backup", h);
        CHECK(r && r->status == 200);
        auto j = json::parse(r->body);
        CHECK(j["code"] == 200);
        backup_data = j["data"];
        CHECK(backup_data.is_object());
        CHECK(backup_data.value("format", "") == "delta-backup-1-encrypted");
        CHECK(backup_data.contains("ciphertext"));
        CHECK(backup_data.contains("tag"));
    }

    // 5) start a second server with the SAME passphrase and restore into it.
    ServerRig dst;
    dst.start("dst", /*passphrase=*/"correct horse battery staple");

    httplib::Client c2("127.0.0.1", dst.port);
    c2.set_read_timeout(30);
    std::string token2 = login(c2, "admin", "admin");
    {
        httplib::Headers h = {{"Authorization", "Bearer " + token2}};
        auto r = c2.Post("/api/v1/admin/restore", h,
                         backup_data.dump(), "application/json");
        CHECK(r && r->status == 200);
        auto j = json::parse(r->body);
        CHECK(j["code"] == 200);
        CHECK(j["data"]["applied"].get<int>() > 0);
    }
    {
        httplib::Headers h = {{"Authorization", "Bearer " + token2}};
        c2.Post("/api/v1/use", h,
                json({{"database", "shop"}, {"schema", "public"}}).dump(),
                "application/json");
        auto r = c2.Post("/api/v1/collections/items/documents/search?database=shop",
                         h, "{}", "application/json");
        CHECK(r && r->status == 200);
        auto j = json::parse(r->body);
        CHECK(j["data"]["total"].get<int>() == 3);
    }

    // 6) Wrong passphrase should NOT be able to restore the same envelope.
    ServerRig bad;
    bad.start("bad", /*passphrase=*/"completely different");
    httplib::Client c3("127.0.0.1", bad.port);
    std::string token3 = login(c3, "admin", "admin");
    {
        httplib::Headers h = {{"Authorization", "Bearer " + token3}};
        auto r = c3.Post("/api/v1/admin/restore", h,
                         backup_data.dump(), "application/json");
        CHECK(r);
        // Either the parse_body wraps the response in code != 200 or the
        // server returns 400/500. Either way, the data must be empty.
        auto j = json::parse(r->body);
        CHECK(j["code"] != 200);
    }

    src.stop();
    dst.stop();
    bad.stop();
    fs::remove_all(src.dir);
    fs::remove_all(dst.dir);
    fs::remove_all(bad.dir);
    std::cout << "test_http_integration: OK" << std::endl;
    return 0;
}

int main() {
    try { return run(); }
    catch (const std::exception& e) {
        std::cerr << "EXCEPTION: " << e.what() << std::endl;
        return 1;
    }
}
