// =============================================================================
// test_tls.cpp — end-to-end TLS coverage for HttpServer::make_tls.
//
// Only meaningful when the binary is built with -DDELTA_TLS=ON
// (CPPHTTPLIB_OPENSSL_SUPPORT defined). On a non-TLS build the test exits
// 0 without doing anything so it doesn't break the standard test matrix.
//
// Procedure:
//   1. Generate a self-signed cert + key with the OpenSSL CLI to a tmp dir.
//   2. Stand up a minimal HttpServer via make_tls() bound to a free port.
//   3. Hit GET /api/v1/health with httplib::SSLClient configured to skip
//      cert verification (we're talking to our own self-signed cert).
//   4. Assert the response is HTTP 200 and JSON-parseable.
//   5. Confirm tls_enabled() reports true on the server side.
// =============================================================================
#ifndef CPPHTTPLIB_OPENSSL_SUPPORT
#include <iostream>
int main() {
    std::cout << "test_tls SKIPPED (built without -DDELTA_TLS=ON)\n";
    return 0;
}
#else

#include "../../src/network/http_server.hpp"
#include "../../src/storage/lsm_tree.hpp"
#include "../../src/core/collection.hpp"
#include "../../src/cache/cache_engine.hpp"
#include "../../src/vector/hnsw_index.hpp"
#include "../../src/auth/auth_manager.hpp"
#include "../../src/database/database_manager.hpp"
#include "../../src/network/connection_pool.hpp"

#include <httplib.h>
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <thread>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

namespace {

// Run an arbitrary shell command and return its exit status.
int sh(const std::string& cmd) {
    return std::system(cmd.c_str());
}

// Generate a self-signed RSA cert valid for 24 hours into the given dir.
// Returns 0 on success. Requires the `openssl` CLI to be on PATH; on the
// CI image and any developer mac with brew openssl@3 installed this is
// always present.
int generate_self_signed(const std::string& dir,
                         std::string& cert_out,
                         std::string& key_out) {
    std::filesystem::create_directories(dir);
    cert_out = dir + "/cert.pem";
    key_out  = dir + "/key.pem";
    std::string cmd =
        "openssl req -x509 -newkey rsa:2048 -nodes -days 1 "
        "-keyout '" + key_out + "' -out '" + cert_out + "' "
        "-subj '/CN=localhost' >/dev/null 2>&1";
    return sh(cmd);
}

// Pick a free TCP port by binding to 0 and letting the kernel choose.
// Returns 0 on failure.
int pick_free_port() {
    httplib::Server probe;
    int port = probe.bind_to_any_port("127.0.0.1");
    return port > 0 ? port : 0;
}

} // namespace

int main() {
    // ---- 1. self-signed cert ----------------------------------------------
    std::string tmpdir = "./test_tls_" + std::to_string(now_ms());
    std::string cert, key;
    int rc = generate_self_signed(tmpdir, cert, key);
    if (rc != 0) {
        // openssl not on PATH — degrade to skip rather than failing CI on
        // environments without the CLI tool.
        std::cout << "test_tls SKIPPED (openssl CLI unavailable)\n";
        std::filesystem::remove_all(tmpdir);
        return 0;
    }

    // ---- 2. spin up the engines + an SSL HttpServer -----------------------
    std::string data_dir = tmpdir + "/data";
    storage::LSMTree store(data_dir);
    CollectionEngine col(&store);
    cache::CacheEngine cache;
    vector::VectorEngine vec;
    auth::AuthManager auth(&store);
    auth::SessionManager sessions;
    database::DatabaseManager dbm(&store);
    network::ConnectionPoolConfig pcfg{1000, 100, 500, 300};
    network::ConnectionPool pool(pcfg, &auth, &dbm);

    network::HttpServer::Tuning ht;
    auto server = network::HttpServer::make_tls(
        &store, &col, &cache, &vec, &auth, &sessions, &dbm, &pool,
        cert, key, ht, /*repl=*/nullptr);
    CHECK(server);
    CHECK(server->tls_enabled());

    int port = 0;
    {
        // pick_free_port spins up its own probe and releases — there is a
        // tiny TOCTOU window before the real server binds, but it's
        // negligible for a local test.
        port = pick_free_port();
        CHECK(port > 0);
    }

    std::thread srv_thread([&] { server->listen("127.0.0.1", port); });
    // Give the server a moment to start listening.
    for (int i = 0; i < 50 && !server->is_running(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    CHECK(server->is_running());

    // ---- 3. issue a real HTTPS request to /api/v1/health ------------------
    httplib::SSLClient cli("127.0.0.1", port);
    cli.enable_server_certificate_verification(false);
    cli.set_connection_timeout(2, 0);
    cli.set_read_timeout(5, 0);

    auto res = cli.Get("/api/v1/health");
    CHECK(res != nullptr);
    CHECK(res->status == 200);
    json body;
    try {
        body = json::parse(res->body);
    } catch (const std::exception& e) {
        std::cerr << "FAIL: response not JSON: " << e.what() << std::endl;
        server->stop();
        srv_thread.join();
        std::filesystem::remove_all(tmpdir);
        return 1;
    }
    // The /health endpoint returns {"code":200, "data":{...}, "message":...}
    // Just sanity-check that the wrapper is there.
    CHECK(body.contains("data"));

    // ---- 4. plain HTTP client should fail to talk to TLS server -----------
    {
        httplib::Client plain("127.0.0.1", port);
        plain.set_connection_timeout(1, 0);
        plain.set_read_timeout(2, 0);
        auto bad = plain.Get("/api/v1/health");
        // Either the connect fails outright or the server hangs up because
        // the TLS handshake bytes look like garbage HTTP. Both are fine —
        // any non-200 (including null) is the expected outcome.
        if (bad && bad->status == 200) {
            std::cerr << "FAIL: plain HTTP succeeded against TLS server\n";
            server->stop();
            srv_thread.join();
            std::filesystem::remove_all(tmpdir);
            return 1;
        }
    }

    // ---- 5. teardown ------------------------------------------------------
    server->stop();
    srv_thread.join();
    std::filesystem::remove_all(tmpdir);
    std::cout << "test_tls OK\n";
    return 0;
}

#endif  // CPPHTTPLIB_OPENSSL_SUPPORT
