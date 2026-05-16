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
//   * InstallSnapshot RPC + log compaction (auto-snapshot above threshold)
//   * Single-server membership change (Raft thesis §4.3): add_server /
//     remove_server emit Config log entries; new peer set takes effect on
//     append (not commit). Safe because only one change is in-flight at a
//     time — propose_config_change blocks new changes until the previous
//     Config entry commits.
//   * HTTP transport binding for vote / append / install_snapshot
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
//
// `type` distinguishes regular data writes (Normal) from raft-internal
// membership changes (Config). Config entries are interpreted by raft itself
// and NOT forwarded to the state machine; their payload is a comma-separated
// NodeId list, e.g. "n1,n2,n3".
struct LogEntry {
    enum Type : uint8_t { Normal = 0, Config = 1 };

    Term  term  = 0;
    Index index = 0;
    Type  type  = Normal;
    std::string payload;        // opaque to raft (Normal) or "n1,n2,..." (Config)
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

// RPC: §7 log compaction / catch-up. Sent by the leader when a follower is
// so far behind that the relevant log prefix has already been compacted.
// The payload is the entire state-machine snapshot bytes — opaque to raft
// and re-applied via RaftStateMachine::restore_snapshot.
struct InstallSnapshotArgs {
    Term        term = 0;
    NodeId      leader_id;
    Index       last_included_index = 0;
    Term        last_included_term  = 0;
    // Single-shot transfer: no chunking. Production deployments with
    // multi-GB state machines should chunk, but the Delta core (single
    // LSMTree per node) fits comfortably in one shot for now.
    std::string data;
    // Peer set as of last_included_index. Empty = unchanged (i.e. the
    // membership at snapshot time matched cfg_.peers).
    std::vector<NodeId> peers;
};
struct InstallSnapshotReply {
    Term     term = 0;
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
    // Snapshot delivery. Default returns false so any transport that
    // hasn't been upgraded yet (e.g. existing in-memory tests) keeps
    // working — the cluster simply won't compact past the slowest peer.
    virtual bool send_install_snapshot(const NodeId& /*peer*/,
                                       const InstallSnapshotArgs& /*args*/,
                                       InstallSnapshotReply* /*reply*/) {
        return false;
    }
};

// Apply committed entries onto the application state. RaftNode calls
// apply(entry) in monotonically increasing index order on its
// apply_thread_. apply() MUST be deterministic and side-effect-only on
// the application state — never block on RPC, never throw.
class RaftStateMachine {
public:
    virtual ~RaftStateMachine() = default;
    virtual void apply(const LogEntry& entry) = 0;

    // Snapshot interface (Raft paper §7). Defaults are no-ops so existing
    // single-shot test SMs compile without modification — they just won't
    // benefit from log compaction.
    //
    // take_snapshot is called by raft from a thread that already holds the
    // raft state lock; do NOT call back into raft from this method.
    virtual void take_snapshot(std::string* /*out*/) {}
    // restore_snapshot is called after raft has rolled the log forward past
    // last_included_index (e.g. on InstallSnapshot or fresh restart with a
    // snapshot on disk). The SM must atomically reset its state to exactly
    // what was captured at snapshot time.
    virtual void restore_snapshot(const std::string& /*in*/) {}
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
        // 1-indexed; log[0] is a sentinel whose (term,index) is either
        // (0,0) for a fresh log or (snapshot_term, snapshot_index) after
        // a compaction. last_log_index = log.back().index.
        std::vector<LogEntry> log;
        // Snapshot bookkeeping (§7). snapshot_data is the bytes returned
        // by RaftStateMachine::take_snapshot at snapshot_index.
        Index       snapshot_index = 0;
        Term        snapshot_term  = 0;
        std::string snapshot_data;
        // Latest peer set as of snapshot_index (empty before any config
        // change). Used to bootstrap cfg_.peers on restart so a node that
        // was added or removed mid-cluster doesn't snap back to its
        // launch-time configuration.
        std::vector<NodeId> peers;
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
    // Log compaction (§7). When commit_index - last_snapshot_index_ exceeds
    // this number, the leader (and each follower) snapshots its state
    // machine and truncates the log up to last_applied_. 0 disables.
    int snapshot_threshold = 0;
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

    // Wait until last_applied_ >= target_index OR timeout. Tighter
    // guarantee than wait_commit: when this returns true, the state
    // machine has already observed the entry. The LSM proposer relies
    // on this so that a successful HTTP put() leaves the value visible
    // to the next get() on the same node.
    bool wait_applied(Index target_index,
                      std::chrono::milliseconds timeout);

    // Membership change (Raft thesis §4.3, single-server). Leader-only.
    // Returns false on a follower (out_leader_hint set when known) or if
    // a previous config change has not yet committed. The peer set takes
    // effect immediately on append (not commit), which is safe because
    // we only allow one change at a time.
    bool propose_config_change(const std::vector<NodeId>& new_peers,
                               Index* out_index,
                               NodeId* out_leader_hint = nullptr);

    // Current peer set (after any committed or in-flight membership
    // changes). Thread-safe snapshot.
    std::vector<NodeId> peers() const;

    // Force a snapshot right now. Test hook + admin /raft/snapshot route.
    // Returns the last_included_index of the new snapshot, or 0 if there
    // was nothing to snapshot.
    Index snapshot_now();

    // RPC inbound — called by the transport when a peer hits us.
    void handle_request_vote(const RequestVoteArgs& args, RequestVoteReply* reply);
    void handle_append_entries(const AppendEntriesArgs& args, AppendEntriesReply* reply);
    void handle_install_snapshot(const InstallSnapshotArgs& args,
                                 InstallSnapshotReply* reply);

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
    // current_term, voted_for, log, snapshot_*, or peers_ changes.
    void persist_locked();

    // Snapshot helpers.
    // Take a snapshot at last_applied_ when permitted. Drops the lock
    // around the SM call so a slow SM doesn't stall raft. No-op if
    // last_applied_ is not strictly greater than current snapshot_index_,
    // or if snapshot_threshold == 0 and `force` is false.
    Index maybe_take_snapshot(bool force);
    // Send InstallSnapshot to one peer. Returns true if delivered AND
    // the peer accepted. Updates match_index_/next_index_ on success.
    // Caller must NOT hold mu_ — this takes its own locks.
    bool install_snapshot_to_peer(const NodeId& peer);
    // log index helpers that respect snapshot_index_:
    //   log_first_index_locked() == snapshot_index_     (sentinel)
    //   log_last_index_locked()  == log_.back().index
    //   log_term_at_locked(idx)  == term at idx, or 0 if compacted/out-of-range.
    Index log_first_index_locked() const { return log_.front().index; }
    Index log_last_index_locked()  const { return log_.back().index;  }
    // Returns true if the absolute log index `idx` is currently in log_
    // (between snapshot_index+1 and last_log_index inclusive, plus the
    // sentinel at snapshot_index). Out-of-range -> false.
    bool  has_log_index_locked(Index idx) const {
        return idx >= log_.front().index && idx <= log_.back().index;
    }
    const LogEntry& log_at_locked(Index idx) const {
        return log_[idx - log_.front().index];
    }
    LogEntry& log_at_locked_mut(Index idx) {
        return log_[idx - log_.front().index];
    }

    // Membership: parse "n1,n2,..." payload from a Config entry.
    static std::vector<NodeId> parse_config_payload(const std::string& s);
    static std::string         encode_config_payload(const std::vector<NodeId>& peers);

    // Update current peer set + leader bookkeeping when a Config entry is
    // appended (leader path) or replicated to us (follower path). Caller
    // holds mu_ in unique mode.
    void apply_config_locked(const std::vector<NodeId>& new_peers);

    // ---- members ----------------------------------------------------------
    RaftConfig cfg_;
    std::shared_ptr<RaftTransport>      tx_;
    std::shared_ptr<PersistentStorage>  storage_;
    std::shared_ptr<RaftStateMachine>   sm_;

    mutable std::shared_mutex mu_;

    // Persistent (durable across restarts).
    Term   current_term_ = 0;
    NodeId voted_for_;
    // log_ is 0-indexed in this vector but maps to absolute raft indices via
    // log_.front().index. Pre-snapshot, log_[0] = {term=0,index=0}. After a
    // snapshot at (T,I), log_[0] = {term=T,index=I} so the "previous entry"
    // of every subsequent record stays consistent with §5.3's check.
    std::vector<LogEntry> log_;
    // Snapshot bookkeeping. snapshot_data_ is the bytes returned by
    // RaftStateMachine::take_snapshot at snapshot_index_.
    Index       snapshot_index_ = 0;
    Term        snapshot_term_  = 0;
    std::string snapshot_data_;
    // Effective peer set. Initialised from cfg_.peers, then maintained by
    // membership changes (apply_config_locked). Persisted with the rest of
    // state so a restart sees the latest config.
    std::vector<NodeId> peers_;
    // Index of the most recent unconfirmed Config entry, or 0 if none.
    // While > 0, propose_config_change rejects new requests.
    Index pending_config_index_ = 0;

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
    // Signalled after each successful state-machine apply. wait_applied
    // blocks on this; the LSM proposer relies on it to surface a
    // committed-AND-applied write to the HTTP put() caller.
    std::condition_variable_any applied_cv_;
};

} // namespace delta::network::raft
