// =============================================================================
// replication.cpp — out-of-line bodies for ReplicationManager.
//
// Confining <httplib.h> to this translation unit keeps the heavy template-
// instantiation cost (and the header's transitive includes) out of every
// caller that only needs the public ReplicationManager API.
// =============================================================================
#include "replication.hpp"

#include <httplib.h>
#include <chrono>
#include <iostream>

namespace delta::network {

namespace {

// Build a fresh httplib::Client with the timeouts and keep-alive policy we
// want for replica → master polling. Fresh-per-call is cheap and avoids the
// thread-safety quirks of long-lived httplib::Client instances.
httplib::Client make_client(const std::string& master_url) {
    httplib::Client c(master_url.c_str());
    c.set_connection_timeout(5);
    c.set_read_timeout(15);
    c.set_keep_alive(true);
    return c;
}

} // namespace

ReplicationManager::ReplicationManager(storage::LSMTree* store, Role role,
                                       const std::string& master_url,
                                       const std::string& cluster_token,
                                       size_t buffer_capacity)
    : store_(store), role_(role), master_url_(master_url),
      token_(cluster_token), capacity_(buffer_capacity) {
    if (role_.load() == Role::Master) {
        install_master_hook();
        last_lsn_ = store_->current_lsn();
    }
}

ReplicationManager::~ReplicationManager() {
    stop();
}

void ReplicationManager::install_master_hook() {
    store_->set_write_hook([this](uint64_t lsn,
                                   const std::string& k,
                                   const std::string& v,
                                   bool tomb) {
        std::unique_lock lk(buf_mu_);
        buffer_.push_back({lsn, k, v, tomb});
        if (buffer_.size() > capacity_) buffer_.pop_front();
        last_lsn_ = lsn;
    });
}

void ReplicationManager::start_replica_loop() {
    if (role_.load() != Role::Replica) return;
    running_ = true;
    thread_ = std::thread([this] { replica_loop(); });
}

void ReplicationManager::stop() {
    running_ = false;
    if (thread_.joinable()) thread_.join();
}

ReplicationManager::ChangesResp
ReplicationManager::get_changes(uint64_t from_lsn, size_t limit) {
    ChangesResp r;
    r.from_lsn = from_lsn;
    std::shared_lock lk(buf_mu_);
    if (buffer_.empty()) {
        r.to_lsn = last_lsn_.load();
        return r;
    }
    if (from_lsn < buffer_.front().lsn - 1) {
        r.gap = true;
        return r;
    }
    for (auto& e : buffer_) {
        if (e.lsn <= from_lsn) continue;
        r.records.push_back({{"lsn",       e.lsn},
                             {"key",       e.key},
                             {"value",     e.value},
                             {"tombstone", e.tomb}});
        r.to_lsn = e.lsn;
        if (r.records.size() >= limit) break;
    }
    if (r.records.empty()) r.to_lsn = last_lsn_.load();
    return r;
}

void ReplicationManager::register_replica(const std::string& id,
                                          const std::string& addr,
                                          uint64_t acked) {
    std::unique_lock lk(replica_mu_);
    auto& r = replicas_[id];
    r.id = id;
    r.remote_addr = addr;
    r.last_seen_ms = now_ms();
    r.last_acked_lsn = acked;
}

std::vector<ReplicaInfo> ReplicationManager::list_replicas() {
    std::shared_lock lk(replica_mu_);
    std::vector<ReplicaInfo> out;
    out.reserve(replicas_.size());
    for (auto& [_, r] : replicas_) out.push_back(r);
    return out;
}

void ReplicationManager::promote_to_master() {
    if (role_.load() == Role::Master) return;
    stop();
    install_master_hook();
    last_lsn_ = store_->current_lsn();
    role_ = Role::Master;
}

void ReplicationManager::demote_to_replica(const std::string& new_master_url) {
    store_->set_write_hook(nullptr);
    master_url_ = new_master_url;
    role_ = Role::Replica;
    start_replica_loop();
}

void ReplicationManager::replica_loop() {
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

bool ReplicationManager::sync_once() {
    uint64_t from = store_->current_lsn();
    auto cli = make_client(master_url_);
    httplib::Headers h{{"X-Delta-Cluster-Token", token_}};
    std::string path = "/api/v1/cluster/changes?from_lsn="
                       + std::to_string(from) + "&limit=2000";
    auto res = cli.Get(path.c_str(), h);
    if (!res) return false;
    if (res->status != 200) return false;
    json body;
    try { body = json::parse(res->body); } catch (...) { return false; }
    auto data = body.value("data", json::object());
    if (data.value("gap", false)) {
        std::cout << "[replica] LSN gap detected, fetching full snapshot..."
                  << std::endl;
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

bool ReplicationManager::bootstrap_snapshot() {
    auto cli = make_client(master_url_);
    httplib::Headers h{{"X-Delta-Cluster-Token", token_}};
    auto res = cli.Get("/api/v1/cluster/snapshot", h);
    if (!res || res->status != 200) return false;
    json body;
    try { body = json::parse(res->body); } catch (...) { return false; }
    auto data = body.value("data", json::object());
    uint64_t lsn = data.value("lsn", static_cast<uint64_t>(0));
    auto recs = data.value("records", json::array());
    for (auto& r : recs) {
        store_->apply_replicated(lsn,
                                 r["key"].get<std::string>(),
                                 r["value"].get<std::string>(),
                                 r.value("tombstone", false));
    }
    std::cout << "[replica] snapshot applied: " << recs.size()
              << " keys, lsn=" << lsn << std::endl;
    return true;
}

} // namespace delta::network
