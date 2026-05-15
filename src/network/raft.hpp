#pragma once
// =============================================================================
// raft.hpp — leader election + log replication for Delta clusters.
//
// This is the algorithmic core only. It deliberately does NOT depend on
// LSMTree, httplib, or any other Delta module. The two extension points are
// abstract base classes:
//
//   * RaftTransport  — sends RequestVote / AppendEntries RPCs to peers.
//                      Tests use a thread-safe in-memory loopback; production
//                      will plug an HTTP-backed transport on top of the
//                      existing /cluster/* control plane.
//
//   * RaftStateMachine — receives apply(entry) calls when commit_index
//                        advances. The Delta integration will route applies
//                        into LSMTree::apply_replicated() (same path the
//                        current master→replica streaming uses).
//
// Persistent state (current_term, voted_for, log[]) is written through a
// PersistentStorage interface; the default file-backed impl lives at the
// bottom of this header. Tests can supply an in-memory variant.
//
// Threading model
//   * Each RaftNode owns one background thread (`tick_thread_`) that runs
//     a 10 ms tick loop driving the election + heartbeat timers.
//   * All public methods are thread-safe: mutations take a unique lock on
//     `mu_`, reads take a shared lock.
//   * RaftTransport callbacks (handle_request_vote / handle_append_entries)
//     are called by inbound RPC threads and acquire the same lock.
//
// What is implemented in this revision
//   * Leader election (RequestVote + randomised election timeout)
//   * AppendEntries: heartbeat AND log replication, with §5.3 consistency
//     check + leader log-backoff
//   * commit_index advancement on majority match_index
//   * §5.4.1 election restriction (vote only for at-least-as-up-to-date logs)
//   * State machine apply_loop driving commit_index → state_machine.apply()
//   * Pre-vote (§9.6) to prevent isolated nodes from disrupting leaders
//
// Deferred (tracked in Round 2 follow-up)
//   * InstallSnapshot / log compaction
//   * Joint-consensus membership change
//   * HTTP transport binding (the abstract Transport is in place)
//   * Wiring leader writes into LSMTree
// =============================================================================
#include "../core/common.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace delta::network::raft {

// ---------------------------------------------------------------------------
// Wire types — kept as plain structs so they JSON-serialise trivially via
// json{...} and so that any transport (HTTP, in-mem, gRPC, ...) can ferry
// them across without templating on the transport type.
// ---------------------------------------------------------------------------

using NodeId = std::string;
using Term   = uint64_t;
using Index  = uint64_t;

enum class Role { Follower, Candidate, Leader };
inline const char* role_name(Role r) {
    switch (r) {
        case Role::Follower:  return "follower";
        case Role::Candidate: return "candidate";
        case Role::Leader:    return "leader";
    }
    return "?";
}

// One replicated state-machine command. `payload` is opaque bytes; for the
// Delta integration this will be a serialized (key, value, tombstone) write
// matching LSMTree::apply_replicated's input.
struct LogEntry {
    Term  term  = 0;
    Index index = 0;
    std::string payload;        // opaque to raft, applied by RaftStateMachine
};

// RPC: §5.2 leader election.
struct RequestVoteArgs {
    Term     term = 0;
    NodeId   candidate_id;
    Index    last_log_index = 0;
    Term     last_log_term  = 0;
    bool     pre_vote = false;  // §9.6: dry-run to avoid disrupting leaders
};
struct RequestVoteReply {
    Term     term = 0;
    bool     vote_granted = false;
};

// RPC: §5.3 log replication + heartbeat.
struct AppendEntriesArgs {
    Term     term = 0;
    NodeId   leader_id;
    Index    prev_log_index = 0;
    Term     prev_log_term  = 0;
    std::vector<LogEntry> entries;        // empty == heartbeat
    Index    leader_commit = 0;
};
struct AppendEntriesReply {
    Term     term = 0;
    bool     success = false;
    // §5.3 fast-rollback hint: when consistency check fails, we tell the
    // leader the first index of our conflicting term so it can rewind
    // next_index in one RPC instead of decrementing by one each round.
    Index    conflict_index = 0;
    Term     conflict_term  = 0;
};

// ---------------------------------------------------------------------------
// Extension points.
// ---------------------------------------------------------------------------

// Outbound RPC. send_* return false to mean "could not deliver" (timeout,
// peer down, network partition). The caller treats false as a missing
// response — it will retry on the next tick. Implementations MUST be
// thread-safe; raft drives all peers concurrently.
class RaftTransport {
public:
    virtual ~RaftTransport() = default;
    virtual bool send_request_vote(const NodeId& peer,
                                   const RequestVoteArgs& args,
                                   RequestVoteReply* reply) = 0;
    virtual bool send_append_entries(const NodeId& peer,
                                     const AppendEntriesArgs& args,
                                     AppendEntriesReply* reply) = 0;
};

// Apply committed entries onto the application state. RaftNode calls
// apply(entry) in monotonically increasing index order on its
// apply_thread_. apply() MUST be deterministic and side-effect-only on
// the application state — never block on RPC, never throw.
class RaftStateMachine {
public:
    virtual ~RaftStateMachine() = default;
    virtual void apply(const LogEntry& entry) = 0;
};

// Persistent storage for (current_term, voted_for, log[]). Writes MUST be
// durable before returning; the default file-backed impl fsyncs on every
// save. Callers may supply an in-memory variant for tests.
class PersistentStorage {
public:
    virtual ~PersistentStorage() = default;
    struct State {
        Term   current_term = 0;
        NodeId voted_for;             // empty if none
        std::vector<LogEntry> log;    // 1-indexed; log[0] is a sentinel
    };
    virtual State load() = 0;
    virtual void  save(const State& s) = 0;
};

// File-backed PersistentStorage. JSON dump + fsync. Robust enough for
// hundreds of writes per second; not optimised for sustained-load Raft.
class FilePersistentStorage : public PersistentStorage {
public:
    explicit FilePersistentStorage(const std::string& path);
    State load() override;
    void  save(const State& s) override;
private:
    std::string path_;
    std::mutex  mu_;
};

// In-memory PersistentStorage. Used by tests and as a no-op when a node
// runs in volatile mode (e.g., a recovery dry-run). All "saves" stay in
// the process; on restart everything is lost.
class MemoryPersistentStorage : public PersistentStorage {
public:
    State load() override { return state_; }
    void  save(const State& s) override { state_ = s; }
private:
    State state_;
};

// ---------------------------------------------------------------------------
// RaftNode — one Raft replica.
// ---------------------------------------------------------------------------

struct RaftConfig {
    NodeId              node_id;
    std::vector<NodeId> peers;          // includes node_id itself
    // Election timeout drawn uniformly from [election_min_ms, election_max_ms].
    // Standard Raft tuning: 150-300 ms. For tests we lower this to ~30-60 ms.
    int election_min_ms = 150;
    int election_max_ms = 300;
    // Heartbeat interval. Must be << election_min_ms. Standard 50 ms.
    int heartbeat_ms    = 50;
    // Background tick granularity. 10 ms is standard.
    int tick_ms         = 10;
    // §9.6 pre-vote: try a dry-run RequestVote first to avoid bumping
    // current_term in isolated minorities. Recommended on for production;
    // off in tests that want deterministic single-shot elections.
    bool pre_vote = true;
};

class RaftNode {
public:
    RaftNode(RaftConfig cfg,
             std::shared_ptr<RaftTransport> transport,
             std::shared_ptr<PersistentStorage> storage,
             std::shared_ptr<RaftStateMachine> sm);
    ~RaftNode();

    // Lifecycle.
    void start();
    void stop();

    // Public introspection (thread-safe).
    Role  role() const;
    Term  current_term() const;
    NodeId leader_id() const;          // empty if unknown / no current leader
    Index commit_index() const;
    Index last_log_index() const;
    bool  is_leader() const { return role() == Role::Leader; }

    // Append a client write through this node. Only succeeds on the leader;
    // on a follower returns false and sets out_leader_hint to the believed
    // current leader (may be empty). On success, returns true with the
    // log index assigned. Replication is asynchronous; the caller can poll
    // commit_index() (or, in production, register a notify callback) to
    // wait for durability.
    bool propose(const std::string& payload, Index* out_index,
                 NodeId* out_leader_hint = nullptr);

    // Wait until commit_index >= target_index OR timeout elapses. Returns
    // true if the index committed in time. Convenience helper for tests
    // and simple synchronous clients.
    bool wait_commit(Index target_index,
                     std::chrono::milliseconds timeout);

    // RPC inbound — called by the transport when a peer hits us.
    void handle_request_vote(const RequestVoteArgs& args, RequestVoteReply* reply);
    void handle_append_entries(const AppendEntriesArgs& args, AppendEntriesReply* reply);

    // Test hooks: pause / resume the tick loop without tearing the node down.
    // Used by the test harness to script deterministic election scenarios.
    void pause_for_test();
    void resume_for_test();

private:
    // Tick loop: runs every cfg.tick_ms, drives election + heartbeat timers.
    void tick_loop();
    // Apply loop: drains committed-but-not-yet-applied entries into the SM.
    void apply_loop();

    // Internal state transitions (caller must hold mu_).
    void become_follower(Term new_term, const NodeId& new_leader);
    void become_candidate();
    void become_leader();

    // Election orchestration. Runs an asynchronous RequestVote round across
    // peers; returns when this round completes (granted/denied/timeout).
    void start_election(bool dry_run);

    // Leader heartbeat: send AppendEntries with whatever entries each peer
    // is missing. Triggered both by the heartbeat timer and immediately
    // after propose() so the new entry doesn't wait a full heartbeat to
    // start replicating.
    void broadcast_append_entries();

    // Helper: try to advance commit_index based on per-peer match_index.
    // §5.4.2: only commit entries from current_term directly; older terms
    // commit by way of subsequent same-term entries.
    void maybe_advance_commit_index_locked();

    // Reset election timer with a fresh random deadline.
    void reset_election_deadline_locked();

    // Persist the relevant slice of state. Must be called whenever
    // current_term, voted_for, or log changes.
    void persist_locked();

    // ---- members ----------------------------------------------------------
    RaftConfig cfg_;
    std::shared_ptr<RaftTransport>      tx_;
    std::shared_ptr<PersistentStorage>  storage_;
    std::shared_ptr<RaftStateMachine>   sm_;

    mutable std::shared_mutex mu_;

    // Persistent (durable across restarts).
    Term   current_term_ = 0;
    NodeId voted_for_;
    // log_ is 0-indexed in storage but treated as 1-indexed by Raft. log_[0]
    // is a permanent sentinel with term=0, index=0.
    std::vector<LogEntry> log_;

    // Volatile (everywhere).
    Role   role_ = Role::Follower;
    NodeId leader_id_;                    // best guess at current leader
    Index  commit_index_ = 0;
    Index  last_applied_ = 0;

    // Volatile (leader only).
    std::unordered_map<NodeId, Index> next_index_;
    std::unordered_map<NodeId, Index> match_index_;

    // Election timer state.
    std::chrono::steady_clock::time_point election_deadline_;
    std::mt19937 rng_;

    // Background threads.
    std::atomic<bool> running_{false};
    std::atomic<bool> paused_{false};
    std::thread tick_thread_;
    std::thread apply_thread_;
    std::condition_variable_any apply_cv_;
};

} // namespace delta::network::raft
