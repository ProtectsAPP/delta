#pragma once
#include "../core/common.hpp"
#include "../auth/auth_manager.hpp"
#include "../database/database_manager.hpp"
#include <atomic>
#include <shared_mutex>

namespace delta::network {

struct Connection {
    uint64_t id = 0;
    std::string client_ip;
    uint16_t client_port = 0;
    std::string state = "idle"; // idle | active | waiting | closing
    uint64_t connected_at = 0;
    uint64_t last_active = 0;
    std::string username;
    std::string database;
    std::string schema = "public";
    std::string session_token;
    uint64_t queries_executed = 0;
    uint64_t bytes_in = 0;
    uint64_t bytes_out = 0;
    json to_json() const {
        return {{"id",id},{"client_ip",client_ip},{"client_port",client_port},{"state",state},
                {"connected_at",connected_at},{"last_active",last_active},{"username",username},
                {"database",database},{"schema",schema},{"queries_executed",queries_executed},
                {"bytes_in",bytes_in},{"bytes_out",bytes_out}};
    }
};

struct ConnectionPoolConfig {
    int max_connections = 1000;
    int max_per_user = 100;
    int max_per_database = 500;
    int idle_timeout_sec = 300;
};

class ConnectionPool {
public:
    explicit ConnectionPool(ConnectionPoolConfig cfg, auth::AuthManager* auth, database::DatabaseManager* dbm)
        : cfg_(cfg), auth_(auth), dbm_(dbm) {}

    uint64_t accept(const std::string& ip, uint16_t port) {
        std::unique_lock<std::shared_mutex> lk(mu_);
        if ((int)connections_.size() >= cfg_.max_connections) return 0;
        Connection c; c.id = next_id_++; c.client_ip = ip; c.client_port = port;
        c.connected_at = c.last_active = now_ms();
        connections_[c.id] = c;
        return c.id;
    }
    void close(uint64_t id) {
        std::unique_lock<std::shared_mutex> lk(mu_);
        auto it = connections_.find(id); if (it == connections_.end()) return;
        if (!it->second.username.empty()) user_counts_[it->second.username]--;
        if (!it->second.database.empty()) db_counts_[it->second.database]--;
        connections_.erase(it);
    }
    Status bind_user(uint64_t id, const std::string& username, const std::string& db, const std::string& sch, const std::string& token) {
        std::unique_lock<std::shared_mutex> lk(mu_);
        auto it = connections_.find(id); if (it == connections_.end()) return Status::NotFound("connection");
        auto u = auth_->get_user(username); if (!u) return Status::NotFound("user");
        if (u->connection_limit > 0 && user_counts_[username] >= u->connection_limit) return Status::Forbidden("user connection limit");
        if (user_counts_[username] >= cfg_.max_per_user) return Status::Forbidden("user connection limit");
        if (!db.empty()) {
            auto d = dbm_->get_database(db); if (!d) return Status::NotFound("database");
            if (db_counts_[db] >= d->max_connections) return Status::Forbidden("database connection limit");
        }
        if (!it->second.username.empty()) user_counts_[it->second.username]--;
        if (!it->second.database.empty()) db_counts_[it->second.database]--;
        it->second.username = username; it->second.database = db; it->second.schema = sch;
        it->second.session_token = token; it->second.state = "active";
        user_counts_[username]++; if (!db.empty()) db_counts_[db]++;
        it->second.last_active = now_ms();
        return Status::Ok();
    }
    void touch(uint64_t id) {
        // touch is read-mostly: only mutates a single existing entry's counters.
        // Use unique lock since we mutate fields, but keep it as cheap as possible.
        std::unique_lock<std::shared_mutex> lk(mu_);
        auto it = connections_.find(id); if (it == connections_.end()) return;
        it->second.last_active = now_ms();
        it->second.queries_executed++;
    }
    std::vector<Connection> list(const std::string& user = "", const std::string& db = "") {
        std::shared_lock<std::shared_mutex> lk(mu_);
        std::vector<Connection> out;
        for (auto& [_, c] : connections_) {
            if (!user.empty() && c.username != user) continue;
            if (!db.empty() && c.database != db) continue;
            out.push_back(c);
        }
        return out;
    }
    int total() { std::shared_lock<std::shared_mutex> lk(mu_); return connections_.size(); }
    int active() {
        std::shared_lock<std::shared_mutex> lk(mu_); int c = 0; uint64_t cutoff = now_ms() - 60000;
        for (auto& [_, x] : connections_) if (x.last_active > cutoff) c++; return c;
    }
    void cleanup_idle() {
        std::unique_lock<std::shared_mutex> lk(mu_);
        uint64_t cutoff = now_ms() - (uint64_t)cfg_.idle_timeout_sec * 1000;
        std::vector<uint64_t> to_close;
        for (auto& [id, c] : connections_) if (c.last_active < cutoff) to_close.push_back(id);
        for (auto id : to_close) {
            auto it = connections_.find(id); if (it == connections_.end()) continue;
            if (!it->second.username.empty()) user_counts_[it->second.username]--;
            if (!it->second.database.empty()) db_counts_[it->second.database]--;
            connections_.erase(it);
        }
    }
private:
    ConnectionPoolConfig cfg_;
    auth::AuthManager* auth_;
    database::DatabaseManager* dbm_;
    std::shared_mutex mu_;
    std::unordered_map<uint64_t, Connection> connections_;
    std::unordered_map<std::string, int> user_counts_;
    std::unordered_map<std::string, int> db_counts_;
    std::atomic<uint64_t> next_id_{1};
};

} // namespace delta::network
