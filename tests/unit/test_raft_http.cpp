// =============================================================================
// test_raft_http.cpp — end-to-end Raft over HTTP loopback.
//
// Three RaftNode instances each get their own real HttpServer listening on
// a distinct local port. They talk to each other through the HTTP transport
// (raft_http_transport.hpp) using the actual /api/v1/cluster/raft/{vote,append}
// endpoints. We assert:
//
//   1. Election: with all three nodes healthy, exactly one leader emerges
//      (probed via GET /cluster/raft/status).
//   2. Replication: POST /cluster/raft/propose on the leader → followers'
//      commit_index advances and their state machines apply the same payloads.
//   3. Failover: stop the leader's HTTP server → the remaining two elect a
//      new leader and keep accepting proposes.
//
// This test only runs when DELTA_TLS is OFF (it uses plain http://). On the
// TLS build the same logic would need SSLClient + cert plumbing; out of scope
// for this round.
// =============================================================================
#include "../../src/network/http_server.hpp"
#include "../../src/network/raft.hpp"
#include "../../src/network/raft_http_transport.hpp"
#include "../../src/storage/lsm_tree.hpp"
#include "../../src/core/collection.hpp"
#include "../../src/cache/cache_engine.hpp"
#include "../../src/vector/hnsw_index.hpp"
#include "../../src/auth/auth_manager.hpp"
#include "../../src/database/database_manager.hpp"
#include "../../src/network/connection_pool.hpp"

#include <httplib.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

using namespace delta;
using namespace delta::network::raft;
using namespace std::chrono_literals;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

namespace {

class CountingSM : public RaftStateMachine {
public:
    void apply(const LogEntry& entry) override {
        std::lock_guard<std::mutex> lk(mu_);
        applied_.push_back(entry.payload);
    }
    std::vector<std::string> snapshot() {
        std::lock_guard<std::mutex> lk(mu_);
        return applied_;
    }
private:
    std::mutex mu_;
    std::vector<std::string> applied_;
};

// All the per-node baggage that a real Delta server would have, packaged
// up so we can stand up three of them in a smoke test.
struct Node {
    std::string id;
    int port = 0;
    std::string base_url;       // http://127.0.0.1:<port>
    std::string data_dir;
    std::unique_ptr<storage::LSMTree> store;
    std::unique_ptr<CollectionEngine> col;
    std::unique_ptr<cache::CacheEngine> cache;
    std::unique_ptr<vector::VectorEngine> vec;
    std::unique_ptr<auth::AuthManager> auth_mgr;
    std::unique_ptr<auth::SessionManager> sessions;
    std::unique_ptr<database::DatabaseManager> dbm;
    std::unique_ptr<network::ConnectionPool> pool;
    std::unique_ptr<network::ReplicationManager> repl;  // for cluster_token
    std::unique_ptr<network::HttpServer> server;
    std::unique_ptr<RaftNode> raft_node;
    std::shared_ptr<CountingSM> sm;
    std::shared_ptr<MemoryPersistentStorage> raft_store;
    std::shared_ptr<HttpRaftTransport> tx;
    std::thread http_thread;
    std::atomic<bool> stopped{false};

    Node() = default;
    Node(Node&&) = delete;            // contains thread + locks; not movable
    Node& operator=(Node&&) = delete;

    ~Node() { teardown(); }

    void teardown() {
        if (stopped.exchange(true)) return;
        // RaftNode threads must stop before the transport, since they may
        // still be in send_*().
        if (raft_node) raft_node->stop();
        if (server) server->stop();
        if (http_thread.joinable()) http_thread.join();
    }
};

int pick_free_port() {
    httplib::Server probe;
    int p = probe.bind_to_any_port("127.0.0.1");
    return p > 0 ? p : 0;
}

bool wait_until(std::function<bool()> pred, std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(5ms);
    }
    return pred();
}

// Build a node, with shared cluster_token so RPC auth lines up. Does NOT
// start the raft tick loop yet; do that after all peers are constructed
// so initial elections see real peers.
std::unique_ptr<Node> make_node(const std::string& id, int port,
                                const std::string& tmproot,
                                const std::string& cluster_token) {
    auto n = std::make_unique<Node>();
    n->id = id;
    n->port = port;
    n->base_url = "http://127.0.0.1:" + std::to_string(port);
    n->data_dir = tmproot + "/" + id;
    std::filesystem::create_directories(n->data_dir);

    n->store    = std::make_unique<storage::LSMTree>(n->data_dir);
    n->col      = std::make_unique<CollectionEngine>(n->store.get());
    n->cache    = std::make_unique<cache::CacheEngine>();
    n->vec      = std::make_unique<vector::VectorEngine>();
    n->auth_mgr = std::make_unique<auth::AuthManager>(n->store.get());
    n->sessions = std::make_unique<auth::SessionManager>();
    n->dbm      = std::make_unique<database::DatabaseManager>(n->store.get());
    network::ConnectionPoolConfig pcfg{1000, 100, 500, 300};
    n->pool = std::make_unique<network::ConnectionPool>(pcfg, n->auth_mgr.get(), n->dbm.get());
    // ReplicationManager is here only because HttpServer reads
    // cluster_token from it. role=Standalone keeps it inert.
    n->repl = std::make_unique<network::ReplicationManager>(
        n->store.get(), network::Role::Standalone, std::string(), cluster_token);

    network::HttpServer::Tuning tuning;
    n->server = std::make_unique<network::HttpServer>(
        n->store.get(), n->col.get(), n->cache.get(), n->vec.get(),
        n->auth_mgr.get(), n->sessions.get(), n->dbm.get(), n->pool.get(),
        tuning, n->repl.get());

    n->sm         = std::make_shared<CountingSM>();
    n->raft_store = std::make_shared<MemoryPersistentStorage>();
    return n;
}

} // namespace

int main() {
    std::string tmproot = "./test_raft_http_" + std::to_string(now_ms());
    std::filesystem::create_directories(tmproot);
    std::string cluster_token = "test-secret-token";

    // ---- pick three free ports BEFORE constructing any node, so each
    // node knows its peers' base URLs at construction time ----------------
    int p1 = pick_free_port(), p2 = pick_free_port(), p3 = pick_free_port();
    CHECK(p1 > 0 && p2 > 0 && p3 > 0 && p1 != p2 && p1 != p3 && p2 != p3);

    auto n1 = make_node("n1", p1, tmproot, cluster_token);
    auto n2 = make_node("n2", p2, tmproot, cluster_token);
    auto n3 = make_node("n3", p3, tmproot, cluster_token);

    // ---- raft transport for each node knows about all THREE peer URLs ---
    auto build_tx = [&](const std::string& self) {
        std::unordered_map<NodeId, std::string> peers;
        peers["n1"] = n1->base_url;
        peers["n2"] = n2->base_url;
        peers["n3"] = n3->base_url;
        peers.erase(self);  // never RPC ourselves over HTTP
        // Tighter than the default 200ms so a dead peer doesn't block the
        // serial broadcast loop on a faster heartbeat.
        return std::make_shared<HttpRaftTransport>(peers, cluster_token, 80);
    };
    n1->tx = build_tx("n1");
    n2->tx = build_tx("n2");
    n3->tx = build_tx("n3");

    auto build_raft = [&](Node* n) {
        RaftConfig cfg;
        cfg.node_id = n->id;
        cfg.peers = {"n1", "n2", "n3"};
        // Election timers are looser than the in-memory test so the HTTP
        // round-trip latency doesn't trigger spurious re-elections on a
        // loaded developer machine.
        cfg.election_min_ms = 200;
        cfg.election_max_ms = 400;
        cfg.heartbeat_ms    = 50;
        cfg.tick_ms         = 20;
        cfg.pre_vote        = true;
        n->raft_node = std::make_unique<RaftNode>(cfg, n->tx, n->raft_store, n->sm);
        n->server->set_raft(n->raft_node.get());
    };
    build_raft(n1.get());
    build_raft(n2.get());
    build_raft(n3.get());

    // ---- start every HTTP server BEFORE starting raft, so when raft
    // fires its first election, peers are already accepting RPC ----------
    n1->http_thread = std::thread([&] { n1->server->listen("127.0.0.1", p1); });
    n2->http_thread = std::thread([&] { n2->server->listen("127.0.0.1", p2); });
    n3->http_thread = std::thread([&] { n3->server->listen("127.0.0.1", p3); });
    CHECK(wait_until([&] {
        return n1->server->is_running() &&
               n2->server->is_running() &&
               n3->server->is_running();
    }, 2s));

    n1->raft_node->start();
    n2->raft_node->start();
    n3->raft_node->start();

    // ---- 1) exactly one leader within 5s -------------------------------
    auto leaders = [&]() -> std::vector<Node*> {
        std::vector<Node*> out;
        for (auto* n : {n1.get(), n2.get(), n3.get()}) {
            if (n->raft_node->is_leader()) out.push_back(n);
        }
        return out;
    };
    bool elected = wait_until([&] {
        return leaders().size() == 1;
    }, 5s);
    CHECK(elected);
    Node* leader = leaders()[0];
    std::cerr << "[test] leader=" << leader->id << "\n";

    // ---- 2) propose 5 entries through HTTP, all three commit ------------
    httplib::Client client(leader->base_url.c_str());
    client.set_connection_timeout(2, 0);
    client.set_read_timeout(5, 0);
    httplib::Headers h{{"X-Delta-Cluster-Token", cluster_token},
                       {"Content-Type", "application/json"}};

    Index last_idx = 0;
    for (int i = 0; i < 5; ++i) {
        json body = {{"payload", std::string("op-") + std::to_string(i)}};
        auto r = client.Post("/api/v1/cluster/raft/propose", h,
                              body.dump(), "application/json");
        CHECK(r != nullptr);
        CHECK(r->status == 200);
        try {
            json env = json::parse(r->body);
            CHECK(env["data"].contains("index"));
            last_idx = env["data"]["index"].get<Index>();
        } catch (const std::exception& e) {
            std::cerr << "FAIL: bad propose response: " << e.what() << "\n";
            return 1;
        }
    }
    CHECK(last_idx > 0);

    // Wait for all three to apply. Followers may lag the leader by a tick.
    bool applied = wait_until([&] {
        return n1->sm->snapshot().size() == 5 &&
               n2->sm->snapshot().size() == 5 &&
               n3->sm->snapshot().size() == 5;
    }, 5s);
    CHECK(applied);
    for (auto* n : {n1.get(), n2.get(), n3.get()}) {
        auto a = n->sm->snapshot();
        for (int i = 0; i < 5; ++i) {
            CHECK(a[i] == "op-" + std::to_string(i));
        }
    }

    // ---- 3) status endpoint -------------------------------------------
    auto sr = client.Get("/api/v1/cluster/raft/status");
    CHECK(sr != nullptr);
    CHECK(sr->status == 200);
    try {
        json env = json::parse(sr->body);
        CHECK(env["data"]["enabled"].get<bool>());
        CHECK(env["data"]["role"] == "leader");
        CHECK(env["data"]["leader_id"] == leader->id);
        CHECK(env["data"]["commit_index"].get<Index>() >= last_idx);
    } catch (const std::exception& e) {
        std::cerr << "FAIL: bad status response: " << e.what() << "\n";
        return 1;
    }

    // ---- 4) failover: stop the leader's HTTP server, watch new leader --
    leader->teardown();
    bool refound = wait_until([&] {
        // A NEW leader among the surviving two.
        std::vector<Node*> alive;
        for (auto* n : {n1.get(), n2.get(), n3.get()}) {
            if (n != leader && !n->stopped.load() && n->raft_node->is_leader()) alive.push_back(n);
        }
        return alive.size() == 1;
    }, 6s);
    CHECK(refound);
    Node* new_leader = nullptr;
    for (auto* n : {n1.get(), n2.get(), n3.get()}) {
        if (n != leader && !n->stopped.load() && n->raft_node->is_leader()) {
            new_leader = n; break;
        }
    }
    CHECK(new_leader != nullptr);
    CHECK(new_leader != leader);
    std::cerr << "[test] new leader=" << new_leader->id << "\n";

    // The new leader accepts a propose and the surviving follower applies it.
    httplib::Client nclient(new_leader->base_url.c_str());
    nclient.set_connection_timeout(2, 0);
    nclient.set_read_timeout(5, 0);
    json body = {{"payload", "post-failover"}};
    auto r = nclient.Post("/api/v1/cluster/raft/propose", h,
                          body.dump(), "application/json");
    CHECK(r != nullptr);
    CHECK(r->status == 200);
    bool applied2 = wait_until([&] {
        for (auto* n : {n1.get(), n2.get(), n3.get()}) {
            if (n == leader || n->stopped.load()) continue;
            auto a = n->sm->snapshot();
            if (a.empty() || a.back() != "post-failover") return false;
        }
        return true;
    }, 5s);
    CHECK(applied2);

    // ---- teardown ------------------------------------------------------
    n1->teardown();
    n2->teardown();
    n3->teardown();
    std::filesystem::remove_all(tmproot);
    std::cout << "test_raft_http OK\n";
    return 0;
}
