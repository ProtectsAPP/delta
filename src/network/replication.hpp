#pragma once
// Master/replica replication for Delta.
//
// Architecture
//   * Master writes go through LSMTree::put / del which assign a monotonic
//     LSN. A WriteHook routes each (lsn, key, value, tombstone) into a ring
//     buffer (`Master::buffer_`) keyed by LSN.
//   * Replicas run a polling thread that calls `GET /cluster/changes?from=N`
//     on the master. On success, each record is written via
//     LSMTree::apply_replicated, which mirrors the master's WAL bit-for-bit.
//   * If a replica's `from_lsn` falls below `min_buffered_lsn` (e.g. master
//     restarted, or the replica was offline for a long time), the master
//     responds with `gap=true` and the replica falls back to
//     `GET /cluster/snapshot` to bootstrap, then resumes streaming.
//
// The HTTP layer also enforces read-only mode on replicas: any mutating route
// returns 503 unless the request is internally tagged as a replication apply.
//
// Implementation note (P2-1, May 2026):
//   The bodies of all non-trivial methods live in replication.cpp so that
//   <httplib.h> stays out of every translation unit that only needs to
//   reference ReplicationManager (e.g. http_server.cpp, main.cpp). Only
//   the small inline helpers are kept in this header.
#include "../core/common.hpp"
#include "../storage/lsm_tree.hpp"
#include <atomic>
#include <thread>
#include <deque>
#include <shared_mutex>

namespace delta::network {

enum class Role { Standalone, Master, Replica };
inline const char* role_name(Role r) {
    switch (r) {
        case Role::Master:  return "master";
        case Role::Replica: return "replica";
        default:            return "standalone";
    }
}

struct ReplicaInfo {
    std::string id;
    std::string remote_addr;
    uint64_t last_seen_ms = 0;
    uint64_t last_acked_lsn = 0;
};

class ReplicationManager {
public:
    ReplicationManager(storage::LSMTree* store, Role role,
                       const std::string& master_url,
                       const std::string& cluster_token,
                       size_t buffer_capacity = 100000);
    ~ReplicationManager();

    void start_replica_loop();
    void stop();

    Role role() const { return role_.load(); }
    bool read_only() const { return role_.load() == Role::Replica; }
    uint64_t replica_lsn() const { return store_->current_lsn(); }
    uint64_t master_last_lsn() const { return last_lsn_.load(); }
    const std::string& master_url() const { return master_url_; }
    const std::string& token() const { return token_; }

    struct ChangesResp {
        bool gap = false;
        uint64_t from_lsn = 0;
        uint64_t to_lsn = 0;
        std::vector<json> records;
    };

    // Master endpoint backing: collect records with lsn > from_lsn.
    ChangesResp get_changes(uint64_t from_lsn, size_t limit = 1000);

    // Replica registration, master-side.
    void register_replica(const std::string& id, const std::string& addr, uint64_t acked);
    std::vector<ReplicaInfo> list_replicas();

    // Promote a replica to master at runtime (failover). After promotion the
    // node accepts writes and starts emitting on the hook.
    void promote_to_master();

    // Demote to replica pointing at a new master.
    void demote_to_replica(const std::string& new_master_url);

private:
    struct Entry { uint64_t lsn; std::string key, value; bool tomb; };

    void replica_loop();
    bool sync_once();
    bool bootstrap_snapshot();

    // Install the master-side WriteHook on the underlying LSMTree. Pulled
    // into its own helper so promote_to_master() and the constructor share
    // the same closure body.
    void install_master_hook();

    storage::LSMTree* store_;
    std::atomic<Role> role_;
    std::string master_url_;
    std::string token_;
    size_t capacity_;

    mutable std::shared_mutex buf_mu_;
    std::deque<Entry> buffer_;
    std::atomic<uint64_t> last_lsn_{0};

    mutable std::shared_mutex replica_mu_;
    std::unordered_map<std::string, ReplicaInfo> replicas_;

    std::atomic<bool> running_{false};
    std::thread thread_;
};

} // namespace delta::network
