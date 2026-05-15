// =============================================================================
// raft.cpp — leader election + log replication (out-of-line bodies).
//
// References to the Raft paper (Ongaro & Ousterhout 2014) are inline as
// "§5.x" / "§9.x" comments at the points they're enforced.
// =============================================================================
#include "raft.hpp"

#include <algorithm>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <unistd.h>

namespace delta::network::raft {

// ---------------------------------------------------------------------------
// FilePersistentStorage — JSON dump + fsync. Plenty for tens of writes per
// second; not the bottleneck for failover correctness.
// ---------------------------------------------------------------------------

FilePersistentStorage::FilePersistentStorage(const std::string& path)
    : path_(path) {}

PersistentStorage::State FilePersistentStorage::load() {
    std::lock_guard<std::mutex> lk(mu_);
    State s;
    std::ifstream f(path_);
    if (!f.good()) return s;
    std::string content((std::istreambuf_iterator<char>(f)),
                         std::istreambuf_iterator<char>());
    try {
        json j = json::parse(content);
        s.current_term = j.value("current_term", (Term)0);
        s.voted_for    = j.value("voted_for",    std::string());
        if (j.contains("log")) {
            for (auto& e : j["log"]) {
                LogEntry le;
                le.term    = e.value("term",    (Term)0);
                le.index   = e.value("index",   (Index)0);
                le.payload = e.value("payload", std::string());
                s.log.push_back(std::move(le));
            }
        }
    } catch (...) {
        // Treat any parse failure as fresh state. The caller (RaftNode)
        // will re-derive everything from peers.
        s = State{};
    }
    return s;
}

void FilePersistentStorage::save(const State& s) {
    std::lock_guard<std::mutex> lk(mu_);
    json j = {
        {"current_term", s.current_term},
        {"voted_for",    s.voted_for},
        {"log",          json::array()}
    };
    for (auto& e : s.log) {
        j["log"].push_back({
            {"term",    e.term},
            {"index",   e.index},
            {"payload", e.payload},
        });
    }
    // Atomic write: temp file + rename + fsync the directory if possible.
    std::string tmp = path_ + ".tmp";
    {
        std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
        f << j.dump();
        f.flush();
    }
    int fd = ::open(tmp.c_str(), O_WRONLY);
    if (fd >= 0) { ::fsync(fd); ::close(fd); }
    std::rename(tmp.c_str(), path_.c_str());
}

// ---------------------------------------------------------------------------
// RaftNode lifecycle.
// ---------------------------------------------------------------------------

RaftNode::RaftNode(RaftConfig cfg,
                   std::shared_ptr<RaftTransport> transport,
                   std::shared_ptr<PersistentStorage> storage,
                   std::shared_ptr<RaftStateMachine> sm)
    : cfg_(std::move(cfg)), tx_(std::move(transport)),
      storage_(std::move(storage)), sm_(std::move(sm)),
      rng_(std::random_device{}()) {
    // Load persistent state.
    auto s = storage_->load();
    current_term_ = s.current_term;
    voted_for_    = s.voted_for;
    log_          = std::move(s.log);
    // Sentinel at index 0 so that log_.back().index == last log index, and
    // so that the "previous log entry" of the very first real entry has
    // term=0 / index=0 (matches what AppendEntries §5.3 expects).
    if (log_.empty() || log_[0].index != 0 || log_[0].term != 0) {
        log_.insert(log_.begin(), LogEntry{});
    }
}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::start() {
    if (running_.exchange(true)) return;
    {
        std::unique_lock<std::shared_mutex> lk(mu_);
        reset_election_deadline_locked();
    }
    tick_thread_  = std::thread([this] { tick_loop(); });
    apply_thread_ = std::thread([this] { apply_loop(); });
}

void RaftNode::stop() {
    if (!running_.exchange(false)) return;
    apply_cv_.notify_all();
    if (tick_thread_.joinable())  tick_thread_.join();
    if (apply_thread_.joinable()) apply_thread_.join();
}

void RaftNode::pause_for_test()  { paused_.store(true);  }
void RaftNode::resume_for_test() { paused_.store(false); }

// ---------------------------------------------------------------------------
// Read-side public introspectors.
// ---------------------------------------------------------------------------

Role RaftNode::role() const {
    std::shared_lock<std::shared_mutex> lk(mu_);
    return role_;
}
Term RaftNode::current_term() const {
    std::shared_lock<std::shared_mutex> lk(mu_);
    return current_term_;
}
NodeId RaftNode::leader_id() const {
    std::shared_lock<std::shared_mutex> lk(mu_);
    return leader_id_;
}
Index RaftNode::commit_index() const {
    std::shared_lock<std::shared_mutex> lk(mu_);
    return commit_index_;
}
Index RaftNode::last_log_index() const {
    std::shared_lock<std::shared_mutex> lk(mu_);
    return log_.back().index;
}

// ---------------------------------------------------------------------------
// State transitions. The caller MUST hold mu_ in unique mode.
// ---------------------------------------------------------------------------

void RaftNode::become_follower(Term new_term, const NodeId& new_leader) {
    if (new_term > current_term_) {
        current_term_ = new_term;
        voted_for_.clear();
    }
    role_ = Role::Follower;
    leader_id_ = new_leader;
    next_index_.clear();
    match_index_.clear();
    persist_locked();
    reset_election_deadline_locked();
}

void RaftNode::become_candidate() {
    role_ = Role::Candidate;
    current_term_++;
    voted_for_ = cfg_.node_id;
    leader_id_.clear();
    persist_locked();
    reset_election_deadline_locked();
}

void RaftNode::become_leader() {
    role_ = Role::Leader;
    leader_id_ = cfg_.node_id;
    Index next = log_.back().index + 1;
    next_index_.clear();
    match_index_.clear();
    for (auto& peer : cfg_.peers) {
        if (peer == cfg_.node_id) continue;
        next_index_[peer]  = next;
        match_index_[peer] = 0;
    }
}

void RaftNode::reset_election_deadline_locked() {
    std::uniform_int_distribution<int> dist(cfg_.election_min_ms,
                                            cfg_.election_max_ms);
    election_deadline_ = std::chrono::steady_clock::now() +
                         std::chrono::milliseconds(dist(rng_));
}

void RaftNode::persist_locked() {
    PersistentStorage::State s;
    s.current_term = current_term_;
    s.voted_for    = voted_for_;
    s.log          = log_;
    storage_->save(s);
}

// ---------------------------------------------------------------------------
// Tick loop.
// ---------------------------------------------------------------------------

void RaftNode::tick_loop() {
    auto last_heartbeat = std::chrono::steady_clock::now();
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg_.tick_ms));
        if (paused_.load()) continue;

        Role r;
        bool need_election = false;
        bool need_heartbeat = false;
        {
            std::shared_lock<std::shared_mutex> lk(mu_);
            r = role_;
            auto now = std::chrono::steady_clock::now();
            if (r != Role::Leader) {
                if (now >= election_deadline_) need_election = true;
            } else {
                if (now - last_heartbeat >=
                    std::chrono::milliseconds(cfg_.heartbeat_ms)) {
                    need_heartbeat = true;
                }
            }
        }
        if (need_election) {
            // §9.6: try pre-vote first if enabled. If pre-vote loses, we
            // don't bump our term; we just reset the deadline and wait.
            if (cfg_.pre_vote) {
                start_election(/*dry_run=*/true);
            } else {
                start_election(/*dry_run=*/false);
            }
        }
        if (need_heartbeat) {
            broadcast_append_entries();
            last_heartbeat = std::chrono::steady_clock::now();
        }
    }
}

// ---------------------------------------------------------------------------
// Election. start_election(dry_run=true) does a pre-vote round (§9.6); on
// success it falls through into the real election. dry_run=false runs the
// real RequestVote round, bumping term and recording the self-vote.
// ---------------------------------------------------------------------------

void RaftNode::start_election(bool dry_run) {
    RequestVoteArgs base;
    NodeId self;
    std::vector<NodeId> peers;
    {
        std::unique_lock<std::shared_mutex> lk(mu_);
        if (role_ == Role::Leader) return;       // someone else won meanwhile
        if (!dry_run) {
            become_candidate();                  // bumps term, votes for self
        } else {
            // Pre-vote: do NOT bump term, do NOT record voted_for. Just
            // probe whether a real election would have a chance.
            reset_election_deadline_locked();
        }
        base.term = dry_run ? (current_term_ + 1) : current_term_;
        base.candidate_id   = cfg_.node_id;
        base.last_log_index = log_.back().index;
        base.last_log_term  = log_.back().term;
        base.pre_vote = dry_run;
        self  = cfg_.node_id;
        peers = cfg_.peers;
    }

    // Self counts as one yes vote.
    int yes = 1;
    int total_peers = (int)peers.size();
    int needed = total_peers / 2 + 1;             // strict majority

    for (auto& peer : peers) {
        if (peer == self) continue;
        RequestVoteReply rep;
        // Note: serial dispatch keeps the implementation simple; for
        // production-grade latency we'd want concurrent send_request_vote.
        // For 3-5 node clusters with a 50 ms RPC timeout this is well
        // within election_min_ms.
        bool delivered = tx_->send_request_vote(peer, base, &rep);
        if (!delivered) continue;

        std::unique_lock<std::shared_mutex> lk(mu_);
        // §5.1 stale-term: stepping down trumps any reply contents.
        if (rep.term > current_term_) {
            become_follower(rep.term, /*leader=*/"");
            return;
        }
        // Ignore replies from stale rounds (we may have moved on).
        if (!dry_run && role_ != Role::Candidate) return;
        if (rep.vote_granted) yes++;
    }

    if (yes < needed) {
        // Lost (or tied). Reset deadline and wait for the next election
        // cycle. The persisted current_term bump (in the non-dry case)
        // stays — that's how Raft prevents perpetual ties from a single
        // partitioned node.
        std::unique_lock<std::shared_mutex> lk(mu_);
        reset_election_deadline_locked();
        return;
    }

    if (dry_run) {
        // Pre-vote won. Now fire the real election.
        start_election(/*dry_run=*/false);
        return;
    }

    // Real election won. Become leader and start replicating.
    {
        std::unique_lock<std::shared_mutex> lk(mu_);
        if (role_ != Role::Candidate) return;     // raced with stepdown
        become_leader();
    }
    broadcast_append_entries();
}

// ---------------------------------------------------------------------------
// Heartbeat / replication: fire one AppendEntries to each peer, with as
// many entries as it's missing.
// ---------------------------------------------------------------------------

void RaftNode::broadcast_append_entries() {
    NodeId self;
    std::vector<NodeId> peers;
    Term current_term;
    Index leader_commit;
    std::vector<LogEntry> log_snapshot;
    std::unordered_map<NodeId, Index> next_idx_snapshot;
    {
        std::shared_lock<std::shared_mutex> lk(mu_);
        if (role_ != Role::Leader) return;
        self          = cfg_.node_id;
        peers         = cfg_.peers;
        current_term  = current_term_;
        leader_commit = commit_index_;
        log_snapshot  = log_;
        next_idx_snapshot = next_index_;
    }

    for (auto& peer : peers) {
        if (peer == self) continue;
        Index ni = 1;
        auto it = next_idx_snapshot.find(peer);
        if (it != next_idx_snapshot.end()) ni = it->second;
        if (ni < 1) ni = 1;

        AppendEntriesArgs args;
        args.term      = current_term;
        args.leader_id = self;
        // prev = entry just before next_index. log_snapshot[ni - 1] is the
        // previous because log is 1-indexed (sentinel at slot 0).
        Index prev_idx = ni - 1;
        if (prev_idx >= log_snapshot.size()) prev_idx = log_snapshot.size() - 1;
        args.prev_log_index = log_snapshot[prev_idx].index;
        args.prev_log_term  = log_snapshot[prev_idx].term;
        // Entries from ni..end.
        for (Index i = ni; i < log_snapshot.size(); ++i) {
            args.entries.push_back(log_snapshot[i]);
        }
        args.leader_commit = leader_commit;

        AppendEntriesReply rep;
        bool delivered = tx_->send_append_entries(peer, args, &rep);
        if (!delivered) continue;

        std::unique_lock<std::shared_mutex> lk(mu_);
        // §5.1: a higher term means we've been deposed.
        if (rep.term > current_term_) {
            become_follower(rep.term, /*leader=*/"");
            return;
        }
        if (role_ != Role::Leader || current_term_ != current_term) return;

        if (rep.success) {
            Index new_match = args.prev_log_index + (Index)args.entries.size();
            if (new_match > match_index_[peer]) match_index_[peer] = new_match;
            next_index_[peer] = match_index_[peer] + 1;
            maybe_advance_commit_index_locked();
        } else {
            // §5.3 fast-rollback: jump next_index to the conflict hint
            // rather than decrementing by one. Bound at 1.
            Index back = next_index_[peer];
            if (rep.conflict_index > 0 && rep.conflict_index < back) {
                back = rep.conflict_index;
            } else if (back > 1) {
                back--;
            }
            if (back < 1) back = 1;
            next_index_[peer] = back;
        }
    }
}

void RaftNode::maybe_advance_commit_index_locked() {
    // §5.4.2: only commit entries from the current term directly.
    // Walk down from last log index; pick the largest N such that:
    //   * N > commit_index_
    //   * a majority of match_index_ >= N (counting self trivially)
    //   * log_[N].term == current_term_
    Index last = log_.back().index;
    int total_peers = (int)cfg_.peers.size();
    int needed = total_peers / 2 + 1;
    for (Index N = last; N > commit_index_; --N) {
        if (log_[N].term != current_term_) continue;
        int count = 1;                          // self
        for (auto& [_, mi] : match_index_) {
            if (mi >= N) count++;
        }
        if (count >= needed) {
            commit_index_ = N;
            apply_cv_.notify_all();
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Apply loop. Drains [last_applied_+1 .. commit_index_] in order.
// ---------------------------------------------------------------------------

void RaftNode::apply_loop() {
    while (running_.load()) {
        std::unique_lock<std::shared_mutex> lk(mu_);
        apply_cv_.wait_for(lk, std::chrono::milliseconds(50), [&] {
            return !running_.load() || commit_index_ > last_applied_;
        });
        if (!running_.load()) return;
        while (commit_index_ > last_applied_) {
            last_applied_++;
            // Copy the entry while holding the lock, drop the lock for the
            // SM call (apply must be allowed to be slow without blocking
            // RPC handling).
            LogEntry e = log_[last_applied_];
            lk.unlock();
            try { sm_->apply(e); } catch (...) { /* SM bugs are SM's */ }
            lk.lock();
        }
    }
}

// ---------------------------------------------------------------------------
// Inbound RPC handlers.
// ---------------------------------------------------------------------------

void RaftNode::handle_request_vote(const RequestVoteArgs& args,
                                   RequestVoteReply* reply) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    reply->term = current_term_;
    reply->vote_granted = false;

    // §9.6 pre-vote: same checks as a real RequestVote, but never persists
    // a vote and never bumps current_term. The point is to fail fast in a
    // partitioned minority without disrupting a healthy leader.
    if (args.pre_vote) {
        if (args.term < current_term_) return;
        // up-to-date check (§5.4.1).
        Term  my_last_term  = log_.back().term;
        Index my_last_index = log_.back().index;
        bool log_ok = (args.last_log_term > my_last_term) ||
                      (args.last_log_term == my_last_term &&
                       args.last_log_index >= my_last_index);
        if (log_ok) reply->vote_granted = true;
        return;
    }

    // Real RequestVote.
    if (args.term < current_term_) return;
    if (args.term > current_term_) {
        become_follower(args.term, /*leader=*/"");
        reply->term = current_term_;
    }
    bool can_vote = voted_for_.empty() || voted_for_ == args.candidate_id;
    Term  my_last_term  = log_.back().term;
    Index my_last_index = log_.back().index;
    bool log_ok = (args.last_log_term > my_last_term) ||
                  (args.last_log_term == my_last_term &&
                   args.last_log_index >= my_last_index);
    if (can_vote && log_ok) {
        voted_for_ = args.candidate_id;
        reply->vote_granted = true;
        persist_locked();
        reset_election_deadline_locked();
    }
}

void RaftNode::handle_append_entries(const AppendEntriesArgs& args,
                                     AppendEntriesReply* reply) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    reply->term = current_term_;
    reply->success = false;
    reply->conflict_index = 0;
    reply->conflict_term  = 0;

    // §5.1 stale-term reject.
    if (args.term < current_term_) return;
    // Higher-or-equal term: step down (idempotent if already follower).
    if (args.term > current_term_ || role_ != Role::Follower) {
        become_follower(args.term, args.leader_id);
    } else {
        leader_id_ = args.leader_id;
    }
    reply->term = current_term_;
    reset_election_deadline_locked();

    // §5.3 consistency check: prev_log_* must match what we have.
    if (args.prev_log_index >= log_.size()) {
        reply->conflict_index = log_.back().index + 1;
        return;
    }
    if (log_[args.prev_log_index].term != args.prev_log_term) {
        // Tell the leader the first index of our conflicting term so it
        // can rewind faster than -1 per round.
        Term bad_term = log_[args.prev_log_index].term;
        Index first = args.prev_log_index;
        while (first > 1 && log_[first - 1].term == bad_term) first--;
        reply->conflict_index = first;
        reply->conflict_term  = bad_term;
        return;
    }

    // Append / overwrite entries. §5.3: existing entries that conflict
    // with new ones are deleted along with everything after them.
    Index write_at = args.prev_log_index + 1;
    for (size_t i = 0; i < args.entries.size(); ++i, ++write_at) {
        if (write_at < log_.size()) {
            if (log_[write_at].term != args.entries[i].term) {
                log_.resize(write_at);
                log_.push_back(args.entries[i]);
            } // else: same entry already present, skip
        } else {
            log_.push_back(args.entries[i]);
        }
    }
    persist_locked();

    // §5.3 commit advance. min(leader_commit, last new index in this RPC).
    Index last_new = args.prev_log_index + (Index)args.entries.size();
    Index new_ci = std::min<Index>(args.leader_commit, last_new);
    if (new_ci > commit_index_) {
        commit_index_ = new_ci;
        apply_cv_.notify_all();
    }
    reply->success = true;
}

// ---------------------------------------------------------------------------
// Client write path.
// ---------------------------------------------------------------------------

bool RaftNode::propose(const std::string& payload, Index* out_index,
                       NodeId* out_leader_hint) {
    {
        std::unique_lock<std::shared_mutex> lk(mu_);
        if (role_ != Role::Leader) {
            if (out_leader_hint) *out_leader_hint = leader_id_;
            return false;
        }
        LogEntry e;
        e.term    = current_term_;
        e.index   = log_.back().index + 1;
        e.payload = payload;
        log_.push_back(e);
        match_index_[cfg_.node_id] = e.index;
        persist_locked();
        if (out_index) *out_index = e.index;
    }
    // Kick off replication immediately.
    broadcast_append_entries();
    return true;
}

bool RaftNode::wait_commit(Index target_index,
                           std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    std::unique_lock<std::shared_mutex> lk(mu_);
    while (commit_index_ < target_index) {
        if (apply_cv_.wait_until(lk, deadline) == std::cv_status::timeout) {
            return commit_index_ >= target_index;
        }
    }
    return true;
}

} // namespace delta::network::raft
