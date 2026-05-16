// =============================================================================
// raft.cpp — leader election + log replication + snapshots + membership.
//
// References to the Raft paper (Ongaro & Ousterhout 2014) are inline as
// "§5.x" / "§7" / "§9.x" comments at the points they're enforced.
// Round 2 part 3 adds:
//   * Log compaction via InstallSnapshot (§7)
//   * Single-server membership change (Raft thesis §4.3): Config log entries.
// =============================================================================
#include "raft.hpp"

#include <algorithm>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sstream>
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
                le.type    = (LogEntry::Type)e.value("type", (uint8_t)0);
                le.payload = e.value("payload", std::string());
                s.log.push_back(std::move(le));
            }
        }
        s.snapshot_index = j.value("snapshot_index", (Index)0);
        s.snapshot_term  = j.value("snapshot_term",  (Term)0);
        s.snapshot_data  = j.value("snapshot_data",  std::string());
        if (j.contains("peers") && j["peers"].is_array()) {
            for (auto& p : j["peers"]) s.peers.push_back(p.get<std::string>());
        }
    } catch (...) {
        // Treat any parse failure as fresh state.
        s = State{};
    }
    return s;
}

void FilePersistentStorage::save(const State& s) {
    std::lock_guard<std::mutex> lk(mu_);
    json j = {
        {"current_term", s.current_term},
        {"voted_for",    s.voted_for},
        {"log",          json::array()},
        {"snapshot_index", s.snapshot_index},
        {"snapshot_term",  s.snapshot_term},
        {"snapshot_data",  s.snapshot_data},
        {"peers",          s.peers},
    };
    for (auto& e : s.log) {
        j["log"].push_back({
            {"term",    e.term},
            {"index",   e.index},
            {"type",    (uint8_t)e.type},
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
// Config payload codec. Trivial CSV of NodeIds; NodeIds must not contain
// ',' (we reject empty / comma-bearing ids at the membership boundary).
// ---------------------------------------------------------------------------

std::vector<NodeId> RaftNode::parse_config_payload(const std::string& s) {
    std::vector<NodeId> out;
    std::string tok;
    std::stringstream ss(s);
    while (std::getline(ss, tok, ',')) {
        if (!tok.empty()) out.push_back(tok);
    }
    return out;
}

std::string RaftNode::encode_config_payload(const std::vector<NodeId>& peers) {
    std::string out;
    for (size_t i = 0; i < peers.size(); ++i) {
        if (i) out += ",";
        out += peers[i];
    }
    return out;
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
    auto s = storage_->load();
    current_term_   = s.current_term;
    voted_for_      = s.voted_for;
    log_            = std::move(s.log);
    snapshot_index_ = s.snapshot_index;
    snapshot_term_  = s.snapshot_term;
    snapshot_data_  = s.snapshot_data;

    // Peer set bootstrap: persisted peers win if present, otherwise we fall
    // back to whatever the operator passed in cfg_.peers. This is what lets
    // a node that was added at runtime survive a restart with its post-
    // change configuration rather than reverting to its launch-time list.
    if (!s.peers.empty()) peers_ = s.peers;
    else                  peers_ = cfg_.peers;

    // Ensure the sentinel matches (snapshot_term_, snapshot_index_). When
    // we crashed before persisting a log after a snapshot, log_ may already
    // start at the right index; otherwise we prepend a fresh sentinel.
    if (log_.empty() || log_[0].index != snapshot_index_ ||
        log_[0].term != snapshot_term_) {
        // Drop any stale prefix (entries at or before snapshot_index_) and
        // splice in the sentinel.
        std::vector<LogEntry> rebuilt;
        LogEntry sentinel;
        sentinel.term  = snapshot_term_;
        sentinel.index = snapshot_index_;
        rebuilt.push_back(sentinel);
        for (auto& e : log_) {
            if (e.index > snapshot_index_) rebuilt.push_back(e);
        }
        log_ = std::move(rebuilt);
    }

    // commit/apply pointers start AT the snapshot — those entries are
    // baked into the SM state we'll restore below.
    commit_index_ = snapshot_index_;
    last_applied_ = snapshot_index_;

    // Restore SM from snapshot bytes (if any). This must happen BEFORE the
    // apply loop starts running, so the SM observes snapshot state before
    // any post-snapshot entries replay through apply().
    if (!snapshot_data_.empty()) {
        try { sm_->restore_snapshot(snapshot_data_); }
        catch (...) { /* SM bugs are SM's */ }
    }
}

RaftNode::~RaftNode() { stop(); }

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
    applied_cv_.notify_all();
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
std::vector<NodeId> RaftNode::peers() const {
    std::shared_lock<std::shared_mutex> lk(mu_);
    return peers_;
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
    for (auto& peer : peers_) {
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
    s.current_term   = current_term_;
    s.voted_for      = voted_for_;
    s.log            = log_;
    s.snapshot_index = snapshot_index_;
    s.snapshot_term  = snapshot_term_;
    s.snapshot_data  = snapshot_data_;
    s.peers          = peers_;
    storage_->save(s);
}

void RaftNode::apply_config_locked(const std::vector<NodeId>& new_peers) {
    peers_ = new_peers;
    // Drop next_/match_index entries for peers no longer in the set and
    // initialise any newly added peers so the next heartbeat starts
    // catching them up.
    if (role_ == Role::Leader) {
        std::unordered_map<NodeId, Index> next_keep, match_keep;
        Index next = log_.back().index + 1;
        for (auto& p : peers_) {
            if (p == cfg_.node_id) continue;
            auto it_n = next_index_.find(p);
            next_keep[p]  = it_n != next_index_.end() ? it_n->second : next;
            auto it_m = match_index_.find(p);
            match_keep[p] = it_m != match_index_.end() ? it_m->second : 0;
        }
        next_index_  = std::move(next_keep);
        match_index_ = std::move(match_keep);
    }
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
            if (cfg_.pre_vote) start_election(/*dry_run=*/true);
            else               start_election(/*dry_run=*/false);
        }
        if (need_heartbeat) {
            broadcast_append_entries();
            last_heartbeat = std::chrono::steady_clock::now();
        }
        // Opportunistic snapshot: cheap to check, only acts when the
        // threshold has been crossed.
        if (cfg_.snapshot_threshold > 0) {
            maybe_take_snapshot(/*force=*/false);
        }
    }
}

// ---------------------------------------------------------------------------
// Election. dry_run=true is §9.6 pre-vote: probe whether a real election
// would have a chance without bumping current_term.
// ---------------------------------------------------------------------------

void RaftNode::start_election(bool dry_run) {
    RequestVoteArgs base;
    NodeId self;
    std::vector<NodeId> peers;
    {
        std::unique_lock<std::shared_mutex> lk(mu_);
        if (role_ == Role::Leader) return;
        // A node that is no longer in the cluster MUST NOT campaign. This
        // becomes relevant immediately after a remove-server config commits.
        bool in_cluster = false;
        for (auto& p : peers_) if (p == cfg_.node_id) { in_cluster = true; break; }
        if (!in_cluster) { reset_election_deadline_locked(); return; }

        if (!dry_run) become_candidate();
        else          reset_election_deadline_locked();

        base.term = dry_run ? (current_term_ + 1) : current_term_;
        base.candidate_id   = cfg_.node_id;
        base.last_log_index = log_.back().index;
        base.last_log_term  = log_.back().term;
        base.pre_vote = dry_run;
        self  = cfg_.node_id;
        peers = peers_;
    }

    std::atomic<int> yes{1};                  // self
    int total_peers = (int)peers.size();
    int needed = total_peers / 2 + 1;

    std::vector<std::thread> workers;
    workers.reserve(peers.size());
    std::atomic<bool> stepped_down{false};
    for (auto& peer : peers) {
        if (peer == self) continue;
        workers.emplace_back([this, peer, base, dry_run,
                              &yes, &stepped_down]() {
            RequestVoteReply rep;
            bool delivered = tx_->send_request_vote(peer, base, &rep);
            if (!delivered) return;
            std::unique_lock<std::shared_mutex> lk(mu_);
            if (rep.term > current_term_) {
                become_follower(rep.term, /*leader=*/"");
                stepped_down.store(true);
                return;
            }
            if (!dry_run && role_ != Role::Candidate) return;
            if (rep.vote_granted) yes.fetch_add(1);
        });
    }
    for (auto& w : workers) if (w.joinable()) w.join();

    if (stepped_down.load()) return;
    if (yes.load() < needed) {
        std::unique_lock<std::shared_mutex> lk(mu_);
        reset_election_deadline_locked();
        return;
    }
    if (dry_run) { start_election(/*dry_run=*/false); return; }

    {
        std::unique_lock<std::shared_mutex> lk(mu_);
        if (role_ != Role::Candidate) return;
        become_leader();
    }
    broadcast_append_entries();
}

// ---------------------------------------------------------------------------
// Heartbeat / replication: fire one AppendEntries to each peer, with as
// many entries as it's missing. Falls back to InstallSnapshot when a
// follower's next_index_ has slid below our snapshot floor.
// ---------------------------------------------------------------------------

void RaftNode::broadcast_append_entries() {
    NodeId self;
    std::vector<NodeId> peers;
    Term current_term;
    Index leader_commit;
    std::vector<LogEntry> log_snapshot;
    std::unordered_map<NodeId, Index> next_idx_snapshot;
    Index snapshot_index_local;
    {
        std::shared_lock<std::shared_mutex> lk(mu_);
        if (role_ != Role::Leader) return;
        self          = cfg_.node_id;
        peers         = peers_;
        current_term  = current_term_;
        leader_commit = commit_index_;
        log_snapshot  = log_;
        next_idx_snapshot = next_index_;
        snapshot_index_local = snapshot_index_;
    }

    std::vector<std::thread> workers;
    workers.reserve(peers.size());
    for (auto& peer : peers) {
        if (peer == self) continue;
        Index ni = log_snapshot.front().index + 1;
        auto it = next_idx_snapshot.find(peer);
        if (it != next_idx_snapshot.end()) ni = it->second;
        if (ni < 1) ni = 1;

        // If the follower is so far behind that prev_log_index would point
        // into the compacted prefix, fall back to InstallSnapshot.
        if (ni <= snapshot_index_local) {
            workers.emplace_back([this, peer]() {
                install_snapshot_to_peer(peer);
            });
            continue;
        }

        workers.emplace_back([this, peer, ni, current_term, self,
                              leader_commit, log_snapshot]() mutable {
            AppendEntriesArgs args;
            args.term      = current_term;
            args.leader_id = self;
            Index first = log_snapshot.front().index;
            Index prev_idx = ni - 1;
            // prev_idx must be in [first, last] inclusive.
            if (prev_idx < first) prev_idx = first;
            if (prev_idx > log_snapshot.back().index) prev_idx = log_snapshot.back().index;
            const LogEntry& prev = log_snapshot[prev_idx - first];
            args.prev_log_index = prev.index;
            args.prev_log_term  = prev.term;
            for (Index i = ni; i <= log_snapshot.back().index; ++i) {
                args.entries.push_back(log_snapshot[i - first]);
            }
            args.leader_commit = leader_commit;

            AppendEntriesReply rep;
            bool delivered = tx_->send_append_entries(peer, args, &rep);
            if (!delivered) return;

            std::unique_lock<std::shared_mutex> lk(mu_);
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
                // §5.3 fast-rollback.
                Index back = next_index_[peer];
                if (rep.conflict_index > 0 && rep.conflict_index < back) {
                    back = rep.conflict_index;
                } else if (back > 1) {
                    back--;
                }
                if (back < 1) back = 1;
                next_index_[peer] = back;
            }
        });
    }
    for (auto& w : workers) if (w.joinable()) w.join();
}

void RaftNode::maybe_advance_commit_index_locked() {
    // §5.4.2: only commit entries from the current term directly.
    Index last = log_.back().index;
    int total_peers = (int)peers_.size();
    if (total_peers == 0) return;
    int needed = total_peers / 2 + 1;
    bool self_in_peers = false;
    for (auto& pp : peers_) if (pp == cfg_.node_id) { self_in_peers = true; break; }
    for (Index N = last; N > commit_index_; --N) {
        if (N <= snapshot_index_) break;
        const LogEntry& e = log_at_locked(N);
        if (e.term != current_term_) continue;
        int count = 0;
        // Self automatically has match_index >= log_.back().index >= N.
        if (self_in_peers) count++;
        for (auto& [p, mi] : match_index_) {
            if (p == cfg_.node_id) continue;
            bool in_set = false;
            for (auto& pp : peers_) if (pp == p) { in_set = true; break; }
            if (in_set && mi >= N) count++;
        }
        if (count >= needed) {
            commit_index_ = N;
            apply_cv_.notify_all();
            // If a Config entry just committed and we removed ourselves
            // from the peer set, step down now — we have no business
            // continuing to lead.
            if (e.type == LogEntry::Config && !self_in_peers) {
                role_ = Role::Follower;
                leader_id_.clear();
                next_index_.clear();
                match_index_.clear();
                reset_election_deadline_locked();
            }
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Apply loop.
// ---------------------------------------------------------------------------

void RaftNode::apply_loop() {
    while (running_.load()) {
        std::unique_lock<std::shared_mutex> lk(mu_);
        apply_cv_.wait_for(lk, std::chrono::milliseconds(50), [&] {
            return !running_.load() || commit_index_ > last_applied_;
        });
        if (!running_.load()) return;
        while (commit_index_ > last_applied_) {
            // If an InstallSnapshot jumped commit_index_ past our log
            // window, advance last_applied_ to snapshot_index_ in one step.
            if (last_applied_ < snapshot_index_) {
                last_applied_ = snapshot_index_;
                applied_cv_.notify_all();
                continue;
            }
            Index next = last_applied_ + 1;
            if (!has_log_index_locked(next)) {
                // Race-narrow: snapshot installed mid-loop. Skip ahead.
                last_applied_ = std::max(next, snapshot_index_);
                applied_cv_.notify_all();
                continue;
            }
            LogEntry e = log_at_locked(next);
            lk.unlock();
            if (e.type == LogEntry::Normal) {
                try { sm_->apply(e); } catch (...) { /* SM bugs are SM's */ }
            }
            // Config entries: state machine is not involved.
            lk.lock();
            // IMPORTANT: bump last_applied_ AFTER the SM has finished
            // applying. wait_applied is meant to be a tighter contract
            // than wait_commit: when it returns, the SM has observed the
            // entry. Bumping before would race the LSM proposer's
            // post-put visibility check.
            last_applied_ = next;
            applied_cv_.notify_all();
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

    if (args.pre_vote) {
        if (args.term < current_term_) return;
        Term  my_last_term  = log_.back().term;
        Index my_last_index = log_.back().index;
        bool log_ok = (args.last_log_term > my_last_term) ||
                      (args.last_log_term == my_last_term &&
                       args.last_log_index >= my_last_index);
        if (log_ok) reply->vote_granted = true;
        return;
    }

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

    if (args.term < current_term_) return;
    if (args.term > current_term_ || role_ != Role::Follower) {
        become_follower(args.term, args.leader_id);
    } else {
        leader_id_ = args.leader_id;
    }
    reply->term = current_term_;
    reset_election_deadline_locked();

    // §5.3 consistency check, with snapshot-aware adjustment. If the
    // leader's prev_log_index lies in our compacted prefix, we cannot
    // verify the match — accept whatever entries strictly follow our
    // snapshot and treat the rest as a no-op.
    if (args.prev_log_index < log_.front().index) {
        // The leader is replaying entries we've already snapshotted.
        // Find the first entry at or after our snapshot frontier and
        // splice the rest in.
        Index first_keep = log_.front().index + 1;
        size_t skip = 0;
        for (; skip < args.entries.size(); ++skip) {
            if (args.entries[skip].index >= first_keep) break;
        }
        // After the skip, we proceed exactly as if prev_log_index was the
        // sentinel and entries[skip..] are what we ought to append.
        // Splice in:
        Index write_at = first_keep;
        for (size_t i = skip; i < args.entries.size(); ++i, ++write_at) {
            if (has_log_index_locked(write_at)) {
                if (log_at_locked(write_at).term != args.entries[i].term) {
                    // Truncate to write_at-1.
                    log_.resize(write_at - log_.front().index);
                    log_.push_back(args.entries[i]);
                    if (args.entries[i].type == LogEntry::Config) {
                        apply_config_locked(parse_config_payload(args.entries[i].payload));
                    }
                }
            } else {
                log_.push_back(args.entries[i]);
                if (args.entries[i].type == LogEntry::Config) {
                    apply_config_locked(parse_config_payload(args.entries[i].payload));
                }
            }
        }
        persist_locked();
        Index last_new = args.prev_log_index + (Index)args.entries.size();
        Index new_ci = std::min<Index>(args.leader_commit, last_new);
        if (new_ci > commit_index_) {
            commit_index_ = new_ci;
            apply_cv_.notify_all();
        }
        reply->success = true;
        return;
    }
    if (!has_log_index_locked(args.prev_log_index)) {
        reply->conflict_index = log_.back().index + 1;
        return;
    }
    if (log_at_locked(args.prev_log_index).term != args.prev_log_term) {
        Term bad_term = log_at_locked(args.prev_log_index).term;
        Index first = args.prev_log_index;
        while (first > log_.front().index + 1 &&
               log_at_locked(first - 1).term == bad_term) {
            first--;
        }
        reply->conflict_index = first;
        reply->conflict_term  = bad_term;
        return;
    }

    // Append / overwrite.
    Index write_at = args.prev_log_index + 1;
    for (size_t i = 0; i < args.entries.size(); ++i, ++write_at) {
        if (has_log_index_locked(write_at)) {
            if (log_at_locked(write_at).term != args.entries[i].term) {
                log_.resize(write_at - log_.front().index);
                log_.push_back(args.entries[i]);
                if (args.entries[i].type == LogEntry::Config) {
                    apply_config_locked(parse_config_payload(args.entries[i].payload));
                }
            } // else: identical, leave in place.
        } else {
            log_.push_back(args.entries[i]);
            if (args.entries[i].type == LogEntry::Config) {
                apply_config_locked(parse_config_payload(args.entries[i].payload));
            }
        }
    }
    persist_locked();

    Index last_new = args.prev_log_index + (Index)args.entries.size();
    Index new_ci = std::min<Index>(args.leader_commit, last_new);
    if (new_ci > commit_index_) {
        commit_index_ = new_ci;
        apply_cv_.notify_all();
    }
    reply->success = true;
}

void RaftNode::handle_install_snapshot(const InstallSnapshotArgs& args,
                                       InstallSnapshotReply* reply) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    reply->term = current_term_;
    if (args.term < current_term_) return;
    if (args.term > current_term_ || role_ != Role::Follower) {
        become_follower(args.term, args.leader_id);
    } else {
        leader_id_ = args.leader_id;
    }
    reply->term = current_term_;
    reset_election_deadline_locked();

    // Stale or already-incorporated snapshot? No-op.
    if (args.last_included_index <= snapshot_index_) return;

    // Restore SM. We drop the lock during the SM call to avoid stalling
    // any in-flight handle_request_vote — the SM can be slow for big
    // snapshots.
    std::string data = args.data;
    lk.unlock();
    try { sm_->restore_snapshot(data); } catch (...) {}
    lk.lock();

    // Truncate log. Keep entries strictly after last_included_index.
    std::vector<LogEntry> kept;
    LogEntry sentinel;
    sentinel.term  = args.last_included_term;
    sentinel.index = args.last_included_index;
    kept.push_back(sentinel);
    for (auto& e : log_) {
        if (e.index > args.last_included_index) kept.push_back(e);
    }
    log_ = std::move(kept);

    snapshot_index_ = args.last_included_index;
    snapshot_term_  = args.last_included_term;
    snapshot_data_  = std::move(data);
    if (commit_index_ < snapshot_index_) commit_index_ = snapshot_index_;
    if (last_applied_ < snapshot_index_) last_applied_ = snapshot_index_;
    if (!args.peers.empty()) apply_config_locked(args.peers);
    persist_locked();
    apply_cv_.notify_all();
    applied_cv_.notify_all();
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
        e.type    = LogEntry::Normal;
        e.payload = payload;
        log_.push_back(e);
        match_index_[cfg_.node_id] = e.index;
        persist_locked();
        if (out_index) *out_index = e.index;
    }
    broadcast_append_entries();
    return true;
}

bool RaftNode::propose_config_change(const std::vector<NodeId>& new_peers,
                                     Index* out_index,
                                     NodeId* out_leader_hint) {
    {
        std::unique_lock<std::shared_mutex> lk(mu_);
        if (role_ != Role::Leader) {
            if (out_leader_hint) *out_leader_hint = leader_id_;
            return false;
        }
        // Reject if a previous config change has not yet committed.
        if (pending_config_index_ > commit_index_) return false;
        if (new_peers.empty()) return false;
        // Reject malformed ids (would break our CSV encoding).
        for (auto& p : new_peers) {
            if (p.empty() || p.find(',') != std::string::npos) return false;
        }
        LogEntry e;
        e.term    = current_term_;
        e.index   = log_.back().index + 1;
        e.type    = LogEntry::Config;
        e.payload = encode_config_payload(new_peers);
        log_.push_back(e);
        match_index_[cfg_.node_id] = e.index;
        pending_config_index_ = e.index;
        apply_config_locked(new_peers);   // takes effect on APPEND, §4.3.
        persist_locked();
        if (out_index) *out_index = e.index;
    }
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

bool RaftNode::wait_applied(Index target_index,
                            std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    std::unique_lock<std::shared_mutex> lk(mu_);
    while (last_applied_ < target_index) {
        if (applied_cv_.wait_until(lk, deadline) == std::cv_status::timeout) {
            return last_applied_ >= target_index;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// Snapshotting.
// ---------------------------------------------------------------------------

Index RaftNode::maybe_take_snapshot(bool force) {
    Index target_idx;
    Term  target_term;
    std::vector<NodeId> peers_snapshot;
    {
        std::shared_lock<std::shared_mutex> lk(mu_);
        if (!force && cfg_.snapshot_threshold <= 0) return 0;
        if (last_applied_ <= snapshot_index_) return 0;
        Index lag = last_applied_ - snapshot_index_;
        if (!force && (int)lag < cfg_.snapshot_threshold) return 0;
        target_idx  = last_applied_;
        // last_applied_ must be in log_ (by apply_loop invariant).
        if (!has_log_index_locked(target_idx)) return 0;
        target_term = log_at_locked(target_idx).term;
        peers_snapshot = peers_;
    }
    // Drop the lock for take_snapshot — the SM call can be slow.
    std::string data;
    try { sm_->take_snapshot(&data); } catch (...) { return 0; }
    {
        std::unique_lock<std::shared_mutex> lk(mu_);
        // Re-check: another path could have snapshotted while we were out.
        if (target_idx <= snapshot_index_) return snapshot_index_;
        // Truncate log: keep entries strictly after target_idx, with new
        // sentinel = (target_term, target_idx).
        std::vector<LogEntry> kept;
        LogEntry sentinel;
        sentinel.term  = target_term;
        sentinel.index = target_idx;
        kept.push_back(sentinel);
        for (auto& e : log_) {
            if (e.index > target_idx) kept.push_back(e);
        }
        log_ = std::move(kept);
        snapshot_index_ = target_idx;
        snapshot_term_  = target_term;
        snapshot_data_  = std::move(data);
        // peers_ stays as-is; the snapshot carries the post-change set.
        persist_locked();
    }
    return target_idx;
}

Index RaftNode::snapshot_now() { return maybe_take_snapshot(/*force=*/true); }

bool RaftNode::install_snapshot_to_peer(const NodeId& peer) {
    InstallSnapshotArgs args;
    Term current_term;
    {
        std::shared_lock<std::shared_mutex> lk(mu_);
        if (role_ != Role::Leader) return false;
        args.term                = current_term_;
        args.leader_id           = cfg_.node_id;
        args.last_included_index = snapshot_index_;
        args.last_included_term  = snapshot_term_;
        args.data                = snapshot_data_;
        args.peers               = peers_;
        current_term             = current_term_;
    }
    InstallSnapshotReply rep;
    bool delivered = tx_->send_install_snapshot(peer, args, &rep);
    if (!delivered) return false;

    std::unique_lock<std::shared_mutex> lk(mu_);
    if (rep.term > current_term_) {
        become_follower(rep.term, /*leader=*/"");
        return false;
    }
    if (role_ != Role::Leader || current_term_ != current_term) return false;
    Index ack = args.last_included_index;
    if (ack > match_index_[peer]) match_index_[peer] = ack;
    next_index_[peer] = ack + 1;
    maybe_advance_commit_index_locked();
    return true;
}

} // namespace delta::network::raft
