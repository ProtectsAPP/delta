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
#include "../core/common.hpp"
#include "../storage/lsm_tree.hpp"
#include <httplib.h>
#include <atomic>
#include <thread>
#include <deque>
#include <shared_mutex>

namespace delta::network {

enum class Role { Standalone, Master, Replica };
inline const char* role_name(Role r) {
    switch (r) { case Role::Master: return "master"; case Role::Replica: return "replica"; default: return "standalone"; }
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
                       size_t buffer_capacity = 100000)
        : store_(store), role_(role), master_url_(master_url),
          token_(cluster_token), capacity_(buffer_capacity) {
        if (role_ == Role::Master) {
            store_->set_write_hook([this](uint64_t lsn, const std::string& k, const std::string& v, bool tomb){
                std::unique_lock lk(buf_mu_);
                buffer_.push_back({lsn, k, v, tomb});
                if (buffer_.size() > capacity_) buffer_.pop_front();
                last_lsn_ = lsn;
            });
            last_lsn_ = store_->current_lsn();
        }
    }
    ~ReplicationManager() { stop(); }

    void start_replica_loop() {
        if (role_ != Role::Replica) return;
        running_ = true;
        thread_ = std::thread([this]{ replica_loop(); });
    }
    void stop() {
        running_ = false;
        if (thread_.joinable()) thread_.join();
    }

    Role role() const { return role_; }
    bool read_only() const { return role_ == Role::Replica; }
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
    ChangesResp get_changes(uint64_t from_lsn, size_t limit = 1000) {
        ChangesResp r; r.from_lsn = from_lsn;
        std::shared_lock lk(buf_mu_);
        if (buffer_.empty()) { r.to_lsn = last_lsn_.load(); return r; }
        if (from_lsn < buffer_.front().lsn - 1) { r.gap = true; return r; }
        for (auto& e : buffer_) {
            if (e.lsn <= from_lsn) continue;
            r.records.push_back({{"lsn", e.lsn}, {"key", e.key}, {"value", e.value}, {"tombstone", e.tomb}});
            r.to_lsn = e.lsn;
            if (r.records.size() >= limit) break;
        }
        if (r.records.empty()) r.to_lsn = last_lsn_.load();
        return r;
    }

    // Replica registration, master-side.
    void register_replica(const std::string& id, const std::string& addr, uint64_t acked) {
        std::unique_lock lk(replica_mu_);
        auto& r = replicas_[id];
        r.id = id; r.remote_addr = addr; r.last_seen_ms = now_ms(); r.last_acked_lsn = acked;
    }
    std::vector<ReplicaInfo> list_replicas() {
        std::shared_lock lk(replica_mu_);
        std::vector<ReplicaInfo> out;
        for (auto& [_, r] : replicas_) out.push_back(r);
        return out;
    }

    // Promote a replica to master at runtime (failover). After promotion the
    // node accepts writes and starts emitting on the hook.
    void promote_to_master() {
        if (role_ == Role::Master) return;
        stop();
        store_->set_write_hook([this](uint64_t lsn, const std::string& k, const std::string& v, bool tomb){
            std::unique_lock lk(buf_mu_);
            buffer_.push_back({lsn, k, v, tomb});
            if (buffer_.size() > capacity_) buffer_.pop_front();
            last_lsn_ = lsn;
        });
        last_lsn_ = store_->current_lsn();
        role_ = Role::Master;
    }

    // Demote to replica pointing at a new master.
    void demote_to_replica(const std::string& new_master_url) {
        store_->set_write_hook(nullptr);
        master_url_ = new_master_url;
        role_ = Role::Replica;
        start_replica_loop();
    }

private:
    struct Entry { uint64_t lsn; std::string key, value; bool tomb; };

    void replica_loop() {
        // Sleep briefly so the surrounding HTTP server has time to bind.
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        while (running_) {
            try {
                if (!sync_once()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                }
            } catch (const std::exception& e) {
                std::cerr << "[replica] sync error: " << e.what() << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            }
        }
    }

    httplib::Client make_client() {
        httplib::Client c(master_url_.c_str());
        c.set_connection_timeout(5);
        c.set_read_timeout(15);
        c.set_keep_alive(true);
        return c;
    }

    bool sync_once() {
        uint64_t from = store_->current_lsn();
        auto cli = make_client();
        httplib::Headers h{{"X-Delta-Cluster-Token", token_}};
        std::string path = "/api/v1/cluster/changes?from_lsn=" + std::to_string(from) + "&limit=2000";
        auto res = cli.Get(path.c_str(), h);
        if (!res) return false;
        if (res->status != 200) return false;
        json body;
        try { body = json::parse(res->body); } catch (...) { return false; }
        auto data = body.value("data", json::object());
        if (data.value("gap", false)) {
            std::cout << "[replica] LSN gap detected, fetching full snapshot..." << std::endl;
            return bootstrap_snapshot();
        }
        auto recs = data.value("records", json::array());
        for (auto& r : recs) {
            store_->apply_replicated(r["lsn"].get<uint64_t>(),
                                      r["key"].get<std::string>(),
                                      r["value"].get<std::string>(),
                                      r["tombstone"].get<bool>());
        }
        if (recs.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return true;
    }

    bool bootstrap_snapshot() {
        auto cli = make_client();
        httplib::Headers h{{"X-Delta-Cluster-Token", token_}};
        auto res = cli.Get("/api/v1/cluster/snapshot", h);
        if (!res || res->status != 200) return false;
        json body; try { body = json::parse(res->body); } catch(...) { return false; }
        auto data = body.value("data", json::object());
        uint64_t lsn = data.value("lsn", (uint64_t)0);
        auto recs = data.value("records", json::array());
        for (auto& r : recs) {
            store_->apply_replicated(lsn, r["key"].get<std::string>(),
                                      r["value"].get<std::string>(),
                                      r.value("tombstone", false));
        }
        std::cout << "[replica] snapshot applied: " << recs.size() << " keys, lsn=" << lsn << std::endl;
        return true;
    }

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
