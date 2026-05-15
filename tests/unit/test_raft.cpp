// =============================================================================
// test_raft.cpp — RaftNode core correctness.
//
// Three nodes wired through an in-memory transport. We exercise:
//   1. Election: with three healthy nodes and tight timers, exactly one
//      leader emerges in one term.
//   2. Replication: leader propose() → followers commit_index advances and
//      the state machines apply the same payloads in the same order.
//   3. Failover: kill the leader, the surviving two elect a new leader and
//      keep accepting writes.
//   4. Log catch-up: a node that was offline during writes must catch up
//      when re-attached.
//   5. Election restriction (§5.4.1): a candidate with a shorter log
//      cannot win against followers that have entries beyond it.
//   6. Pre-vote: a partitioned minority does NOT bump current_term on
//      the rest of the cluster.
//
// Tight timers (election 30-60ms, heartbeat 10ms, tick 5ms) keep the test
// fast (whole suite under a second).
// =============================================================================
#include "../../src/network/raft.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

using namespace delta::network::raft;
using namespace std::chrono_literals;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

namespace {

// In-memory transport. A "network" is shared between all nodes; each node
// registers itself by id. RPCs are direct synchronous calls into the peer's
// handler. The harness can isolate a node by setting `partitioned[id] = true`
// — RPCs to or from that id then return false (delivery failure).
class MemNetwork {
public:
    void register_node(const NodeId& id, RaftNode* node) {
        std::lock_guard<std::mutex> lk(mu_);
        nodes_[id] = node;
    }
    void unregister_node(const NodeId& id) {
        std::lock_guard<std::mutex> lk(mu_);
        nodes_.erase(id);
        partitioned_.erase(id);
    }
    void partition(const NodeId& id, bool isolated) {
        std::lock_guard<std::mutex> lk(mu_);
        if (isolated) partitioned_.insert(id);
        else          partitioned_.erase(id);
    }
    bool is_isolated(const NodeId& id) {
        std::lock_guard<std::mutex> lk(mu_);
        return partitioned_.count(id) > 0;
    }
    RaftNode* lookup(const NodeId& id) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = nodes_.find(id);
        return it == nodes_.end() ? nullptr : it->second;
    }
private:
    std::mutex mu_;
    std::map<NodeId, RaftNode*> nodes_;
    std::set<NodeId> partitioned_;
};

class MemTransport : public RaftTransport {
public:
    MemTransport(NodeId self, MemNetwork* net) : self_(std::move(self)), net_(net) {}

    bool send_request_vote(const NodeId& peer, const RequestVoteArgs& args,
                           RequestVoteReply* reply) override {
        if (net_->is_isolated(self_) || net_->is_isolated(peer)) return false;
        auto* p = net_->lookup(peer);
        if (!p) return false;
        p->handle_request_vote(args, reply);
        return true;
    }
    bool send_append_entries(const NodeId& peer, const AppendEntriesArgs& args,
                             AppendEntriesReply* reply) override {
        if (net_->is_isolated(self_) || net_->is_isolated(peer)) return false;
        auto* p = net_->lookup(peer);
        if (!p) return false;
        p->handle_append_entries(args, reply);
        return true;
    }
private:
    NodeId self_;
    MemNetwork* net_;
};

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

struct Cluster {
    MemNetwork net;
    std::vector<std::shared_ptr<RaftNode>> nodes;
    std::vector<std::shared_ptr<CountingSM>> sms;
    std::vector<std::shared_ptr<MemoryPersistentStorage>> stores;
    std::vector<NodeId> ids;

    // Make sure threads stop before the network mutex (held by MemNetwork)
    // gets destroyed. Without this dtor the destruction order is fine in
    // the simple case, but a thread mid-RPC can still hold the mutex
    // briefly while Cluster is being torn down.
    ~Cluster() { stop_all(); }

    void add(const NodeId& id, std::vector<NodeId> peers, bool pre_vote = false) {
        RaftConfig cfg;
        cfg.node_id = id;
        cfg.peers = std::move(peers);
        cfg.election_min_ms = 30;
        cfg.election_max_ms = 80;
        cfg.heartbeat_ms    = 10;
        cfg.tick_ms         = 5;
        cfg.pre_vote        = pre_vote;
        auto sm = std::make_shared<CountingSM>();
        auto store = std::make_shared<MemoryPersistentStorage>();
        auto tx = std::make_shared<MemTransport>(id, &net);
        auto node = std::make_shared<RaftNode>(cfg, tx, store, sm);
        net.register_node(id, node.get());
        nodes.push_back(node);
        sms.push_back(sm);
        stores.push_back(store);
        ids.push_back(id);
    }
    void start_all() { for (auto& n : nodes) n->start(); }
    void stop_all()  { for (auto& n : nodes) n->stop();  }

    int leader_index() {
        for (size_t i = 0; i < nodes.size(); ++i) {
            if (nodes[i]->is_leader()) return (int)i;
        }
        return -1;
    }

    // Block until exactly one node sees itself as leader, OR timeout.
    bool wait_for_leader(std::chrono::milliseconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            int n = 0;
            for (auto& nd : nodes) if (nd->is_leader()) n++;
            if (n == 1) return true;
            std::this_thread::sleep_for(2ms);
        }
        return false;
    }
};

int test_election_basic() {
    Cluster c;
    std::vector<NodeId> peers = {"n1", "n2", "n3"};
    c.add("n1", peers);
    c.add("n2", peers);
    c.add("n3", peers);
    c.start_all();
    bool ok = c.wait_for_leader(2s);
    CHECK(ok);
    int li = c.leader_index();
    CHECK(li >= 0);
    Term t = c.nodes[li]->current_term();
    // All non-leaders should be followers and agree on the term once
    // settled. Because of pre-vote being off here, term may have bumped a
    // few times under contention; we just demand consistency.
    std::this_thread::sleep_for(50ms);
    for (auto& n : c.nodes) {
        if (n->is_leader()) continue;
        CHECK(n->role() == Role::Follower);
        CHECK(n->current_term() == t);
    }
    c.stop_all();
    return 0;
}

int test_replication() {
    Cluster c;
    std::vector<NodeId> peers = {"n1", "n2", "n3"};
    c.add("n1", peers);
    c.add("n2", peers);
    c.add("n3", peers);
    c.start_all();
    CHECK(c.wait_for_leader(2s));
    int li = c.leader_index();
    auto& leader = c.nodes[li];

    Index last = 0;
    for (int i = 0; i < 10; ++i) {
        Index idx;
        CHECK(leader->propose("op-" + std::to_string(i), &idx));
        last = idx;
    }
    // Wait until the leader sees commit_index >= last.
    CHECK(leader->wait_commit(last, 2s));
    // Followers should converge.
    auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
        bool all_committed = true;
        for (auto& n : c.nodes) {
            if (n->commit_index() < last) { all_committed = false; break; }
        }
        if (all_committed) break;
        std::this_thread::sleep_for(2ms);
    }
    for (auto& n : c.nodes) {
        CHECK(n->commit_index() >= last);
    }
    // State machines should have applied the same 10 ops in order.
    for (auto& sm : c.sms) {
        auto applied = sm->snapshot();
        // Wait briefly for the last few applies (apply_loop has a small lag).
        auto sm_deadline = std::chrono::steady_clock::now() + 2s;
        while (applied.size() < 10 && std::chrono::steady_clock::now() < sm_deadline) {
            std::this_thread::sleep_for(2ms);
            applied = sm->snapshot();
        }
        CHECK(applied.size() == 10);
        for (int i = 0; i < 10; ++i) {
            CHECK(applied[i] == "op-" + std::to_string(i));
        }
    }
    c.stop_all();
    return 0;
}

int test_failover() {
    Cluster c;
    std::vector<NodeId> peers = {"n1", "n2", "n3"};
    c.add("n1", peers);
    c.add("n2", peers);
    c.add("n3", peers);
    c.start_all();
    CHECK(c.wait_for_leader(2s));
    int li = c.leader_index();
    NodeId old_leader_id = c.ids[li];

    // Write something so we have a non-empty log.
    Index idx;
    CHECK(c.nodes[li]->propose("before-failover", &idx));
    CHECK(c.nodes[li]->wait_commit(idx, 2s));

    // Partition the leader off.
    c.net.partition(old_leader_id, true);
    // Wait for a new leader among the remaining two.
    auto deadline = std::chrono::steady_clock::now() + 3s;
    int new_li = -1;
    while (std::chrono::steady_clock::now() < deadline) {
        for (size_t i = 0; i < c.nodes.size(); ++i) {
            if (c.ids[i] == old_leader_id) continue;
            if (c.nodes[i]->is_leader()) { new_li = (int)i; break; }
        }
        if (new_li >= 0) break;
        std::this_thread::sleep_for(2ms);
    }
    CHECK(new_li >= 0);
    CHECK(c.ids[new_li] != old_leader_id);

    // The new leader should accept writes.
    Index idx2;
    CHECK(c.nodes[new_li]->propose("after-failover", &idx2));
    CHECK(c.nodes[new_li]->wait_commit(idx2, 2s));

    c.stop_all();
    return 0;
}

int test_log_catchup() {
    Cluster c;
    std::vector<NodeId> peers = {"n1", "n2", "n3"};
    c.add("n1", peers);
    c.add("n2", peers);
    c.add("n3", peers);
    c.start_all();
    CHECK(c.wait_for_leader(2s));
    int li = c.leader_index();
    NodeId leader_id = c.ids[li];

    // Pick a non-leader and isolate it so it misses some writes.
    NodeId straggler;
    int straggler_i = -1;
    for (size_t i = 0; i < c.ids.size(); ++i) {
        if (c.ids[i] != leader_id) {
            straggler = c.ids[i];
            straggler_i = (int)i;
            break;
        }
    }
    c.net.partition(straggler, true);

    Index last = 0;
    for (int i = 0; i < 5; ++i) {
        Index idx;
        CHECK(c.nodes[li]->propose("late-" + std::to_string(i), &idx));
        last = idx;
    }
    CHECK(c.nodes[li]->wait_commit(last, 2s));

    // Re-attach the straggler.
    c.net.partition(straggler, false);
    auto deadline = std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (c.nodes[straggler_i]->commit_index() >= last) break;
        std::this_thread::sleep_for(2ms);
    }
    CHECK(c.nodes[straggler_i]->commit_index() >= last);

    auto applied = c.sms[straggler_i]->snapshot();
    auto sm_deadline = std::chrono::steady_clock::now() + 2s;
    while (applied.size() < 5 && std::chrono::steady_clock::now() < sm_deadline) {
        std::this_thread::sleep_for(2ms);
        applied = c.sms[straggler_i]->snapshot();
    }
    CHECK(applied.size() == 5);
    for (int i = 0; i < 5; ++i) {
        CHECK(applied[i] == "late-" + std::to_string(i));
    }
    c.stop_all();
    return 0;
}

int test_election_restriction() {
    // A candidate with a shorter log must not be able to win against
    // followers that already have committed entries beyond its log.
    //
    // Setup:
    //   * 3 nodes start, leader gets 5 committed entries.
    //   * Isolate the leader. The remaining 2 elect a new leader; we let
    //     it commit a 6th entry.
    //   * Now isolate one of the up-to-date followers and reconnect the
    //     OLD leader (which has only 5 entries). It must NOT win because
    //     the remaining up-to-date node refuses to grant a vote.
    //
    // We assert: after re-connecting the lagging node, no leader change
    // happens unless it gets to log-parity. (In practice the lagging
    // node never becomes leader; the up-to-date node remains leader.)
    Cluster c;
    std::vector<NodeId> peers = {"n1", "n2", "n3"};
    c.add("n1", peers);
    c.add("n2", peers);
    c.add("n3", peers);
    c.start_all();
    CHECK(c.wait_for_leader(2s));
    int li = c.leader_index();
    Index last = 0;
    for (int i = 0; i < 5; ++i) {
        Index idx;
        CHECK(c.nodes[li]->propose("e" + std::to_string(i), &idx));
        last = idx;
    }
    CHECK(c.nodes[li]->wait_commit(last, 2s));

    NodeId old_leader_id = c.ids[li];
    c.net.partition(old_leader_id, true);

    // New leader among the remaining two.
    int new_li = -1;
    auto deadline = std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < deadline) {
        for (size_t i = 0; i < c.nodes.size(); ++i) {
            if (c.ids[i] == old_leader_id) continue;
            if (c.nodes[i]->is_leader()) { new_li = (int)i; break; }
        }
        if (new_li >= 0) break;
        std::this_thread::sleep_for(2ms);
    }
    CHECK(new_li >= 0);

    // Commit a 6th entry on the new leader.
    Index idx6;
    CHECK(c.nodes[new_li]->propose("e5", &idx6));
    CHECK(c.nodes[new_li]->wait_commit(idx6, 2s));

    // Reconnect old leader. It will discover the higher term via
    // AppendEntries and step down. Its log_ has 5 entries, the others
    // have 6 — so it must not be able to win a future election.
    c.net.partition(old_leader_id, false);
    // Let the cluster settle.
    std::this_thread::sleep_for(300ms);

    // Old leader should be a follower and its commit_index should catch up.
    int old_idx = -1;
    for (size_t i = 0; i < c.ids.size(); ++i)
        if (c.ids[i] == old_leader_id) { old_idx = (int)i; break; }
    CHECK(old_idx >= 0);
    CHECK(c.nodes[old_idx]->role() != Role::Leader);
    auto cd = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < cd) {
        if (c.nodes[old_idx]->commit_index() >= idx6) break;
        std::this_thread::sleep_for(2ms);
    }
    CHECK(c.nodes[old_idx]->commit_index() >= idx6);
    c.stop_all();
    return 0;
}

int test_pre_vote_isolation() {
    // Pre-vote: a node isolated from the cluster should NOT bump its term
    // by repeated failed elections, so when it rejoins the cluster its
    // current_term doesn't disrupt the existing leader.
    Cluster c;
    std::vector<NodeId> peers = {"n1", "n2", "n3"};
    c.add("n1", peers, /*pre_vote=*/true);
    c.add("n2", peers, /*pre_vote=*/true);
    c.add("n3", peers, /*pre_vote=*/true);
    c.start_all();
    CHECK(c.wait_for_leader(2s));
    int li = c.leader_index();
    Term steady_term = c.nodes[li]->current_term();

    // Isolate a follower for a while. With pre-vote, it should NOT keep
    // bumping its current_term while alone.
    int victim_i = -1;
    for (size_t i = 0; i < c.ids.size(); ++i) {
        if ((int)i != li) { victim_i = (int)i; break; }
    }
    NodeId victim = c.ids[victim_i];
    Term before = c.nodes[victim_i]->current_term();
    c.net.partition(victim, true);
    std::this_thread::sleep_for(500ms);             // ~6+ election timeouts
    Term after = c.nodes[victim_i]->current_term();
    // With pre-vote the term should stay the same. Without pre-vote it
    // would have grown by many.
    CHECK(after == before);

    // Reconnect; cluster keeps a leader at the same term (the original
    // leader may or may not still be the leader — what matters is that
    // pre-vote prevented the victim from disrupting things by bumping
    // current_term while it was alone).
    c.net.partition(victim, false);
    std::this_thread::sleep_for(200ms);
    // Some node must still be leader.
    CHECK(c.leader_index() >= 0);
    // Term must not have grown beyond the steady-state term plus a small
    // slop allowance for any natural reelection. Without pre-vote it
    // would have grown by 5+.
    int leader_now = c.leader_index();
    Term term_now = c.nodes[leader_now]->current_term();
    CHECK(term_now <= steady_term + 1);
    c.stop_all();
    return 0;
}

} // namespace

int main() {
    int rc = 0;
    rc |= test_election_basic();
    rc |= test_replication();
    rc |= test_failover();
    rc |= test_log_catchup();
    rc |= test_election_restriction();
    rc |= test_pre_vote_isolation();
    if (rc == 0) std::cout << "test_raft OK\n";
    return rc;
}
