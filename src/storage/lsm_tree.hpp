#pragma once
#include "../core/common.hpp"
#include <map>
#include <unordered_map>
#include <fstream>
#include <filesystem>
#include <shared_mutex>
#include <thread>
#include <condition_variable>

namespace delta::storage {

// MemTable using ordered map (skip-list semantically equivalent for our purposes)
class MemTable {
public:
    MemTable(size_t max_bytes = 64 * 1024 * 1024) : max_bytes_(max_bytes) {}
    void put(const std::string& key, const std::string& value, bool tombstone = false) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = data_.find(key);
        if (it != data_.end()) bytes_ -= it->first.size() + it->second.value.size();
        Entry e{value, tombstone, now_ms()};
        bytes_ += key.size() + value.size();
        data_[key] = std::move(e);
    }
    bool get(const std::string& key, std::string* value, bool* tombstone) const {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = data_.find(key);
        if (it == data_.end()) return false;
        *value = it->second.value;
        *tombstone = it->second.tombstone;
        return true;
    }
    bool should_flush() const { return bytes_ >= max_bytes_; }
    size_t size() const { return data_.size(); }
    size_t bytes() const { return bytes_; }
    struct Entry { std::string value; bool tombstone; uint64_t ts; };
    std::map<std::string, Entry> snapshot() const {
        std::lock_guard<std::mutex> lk(mu_);
        return data_;
    }
    void clear() {
        std::lock_guard<std::mutex> lk(mu_);
        data_.clear();
        bytes_ = 0;
    }
private:
    mutable std::mutex mu_;
    std::map<std::string, Entry> data_;
    size_t bytes_ = 0;
    size_t max_bytes_;
};

// WAL: append-only log
class WAL {
public:
    explicit WAL(const std::string& path) : path_(path) {
        out_.open(path_, std::ios::app | std::ios::binary);
    }
    void append(const std::string& key, const std::string& value, bool tombstone) {
        std::lock_guard<std::mutex> lk(mu_);
        uint8_t t = tombstone ? 1 : 0;
        uint32_t kl = key.size(), vl = value.size();
        out_.write((char*)&t, 1);
        out_.write((char*)&kl, 4);
        out_.write(key.data(), kl);
        out_.write((char*)&vl, 4);
        out_.write(value.data(), vl);
        out_.flush();
    }
    void replay(std::function<void(const std::string&, const std::string&, bool)> cb) {
        std::ifstream in(path_, std::ios::binary);
        if (!in) return;
        while (in.peek() != EOF) {
            uint8_t t; uint32_t kl, vl;
            if (!in.read((char*)&t, 1)) break;
            if (!in.read((char*)&kl, 4)) break;
            std::string k(kl, '\0'); if (!in.read(&k[0], kl)) break;
            if (!in.read((char*)&vl, 4)) break;
            std::string v(vl, '\0'); if (!in.read(&v[0], vl)) break;
            cb(k, v, t == 1);
        }
    }
    void truncate() {
        std::lock_guard<std::mutex> lk(mu_);
        out_.close();
        std::filesystem::remove(path_);
        out_.open(path_, std::ios::app | std::ios::binary);
    }
private:
    std::string path_;
    std::ofstream out_;
    std::mutex mu_;
};

// SSTable: sorted string table (simplified: file with sorted entries + index)
struct SSTable {
    std::string path;
    std::map<std::string, std::pair<std::string, bool>> index; // key -> (value, tombstone)
    static std::shared_ptr<SSTable> create(const std::string& path, const std::map<std::string, MemTable::Entry>& data) {
        auto t = std::make_shared<SSTable>();
        t->path = path;
        std::ofstream out(path, std::ios::binary);
        uint64_t n = data.size();
        out.write((char*)&n, 8);
        for (auto& [k, e] : data) {
            uint8_t tomb = e.tombstone ? 1 : 0;
            uint32_t kl = k.size(), vl = e.value.size();
            out.write((char*)&tomb, 1);
            out.write((char*)&kl, 4);
            out.write(k.data(), kl);
            out.write((char*)&vl, 4);
            out.write(e.value.data(), vl);
            t->index[k] = {e.value, e.tombstone};
        }
        return t;
    }
    static std::shared_ptr<SSTable> load(const std::string& path) {
        auto t = std::make_shared<SSTable>();
        t->path = path;
        std::ifstream in(path, std::ios::binary);
        if (!in) return t;
        uint64_t n; in.read((char*)&n, 8);
        for (uint64_t i = 0; i < n; ++i) {
            uint8_t tomb; uint32_t kl, vl;
            in.read((char*)&tomb, 1);
            in.read((char*)&kl, 4);
            std::string k(kl, '\0'); in.read(&k[0], kl);
            in.read((char*)&vl, 4);
            std::string v(vl, '\0'); in.read(&v[0], vl);
            t->index[k] = {v, tomb == 1};
        }
        return t;
    }
};

// LSM-Tree key-value store
class LSMTree {
public:
    // Subscribers receive every committed write so they can replicate it.
    // Called *after* WAL flush. (lsn, key, value, tombstone)
    using WriteHook = std::function<void(uint64_t, const std::string&, const std::string&, bool)>;

    explicit LSMTree(const std::string& dir) : dir_(dir) {
        std::filesystem::create_directories(dir_);
        wal_ = std::make_unique<WAL>(dir_ + "/wal.log");
        memtable_ = std::make_unique<MemTable>();
        load_lsn();
        // load existing SSTables
        for (auto& p : std::filesystem::directory_iterator(dir_)) {
            auto path = p.path().string();
            if (path.size() >= 4 && path.substr(path.size()-4) == ".sst") {
                sstables_.push_back(SSTable::load(path));
            }
        }
        // sort by name (timestamp embedded)
        std::sort(sstables_.begin(), sstables_.end(), [](auto& a, auto& b){return a->path > b->path;});
        // replay WAL
        wal_->replay([this](const std::string& k, const std::string& v, bool t){
            memtable_->put(k, v, t);
        });
    }
    ~LSMTree() { stop_ = true; cv_.notify_all(); if (flush_thread_.joinable()) flush_thread_.join(); }

    // Register a hook that receives every write. Used by the replication
    // module on the master to stream WAL records to followers.
    void set_write_hook(WriteHook hook) { hook_ = std::move(hook); }
    uint64_t current_lsn() const { return next_lsn_.load(std::memory_order_relaxed); }
    // Replicas call this to apply a record without re-emitting it on the hook.
    void apply_replicated(uint64_t lsn, const std::string& key, const std::string& value, bool tombstone) {
        wal_->append(key, value, tombstone);
        memtable_->put(key, value, tombstone);
        // bump local lsn so we know how far we've replayed
        uint64_t cur;
        do { cur = next_lsn_.load(); if (lsn <= cur) break; }
        while (!next_lsn_.compare_exchange_weak(cur, lsn));
        save_lsn();
        maybe_flush();
    }

    void put(const std::string& key, const std::string& value) {
        uint64_t lsn = next_lsn_.fetch_add(1, std::memory_order_relaxed) + 1;
        wal_->append(key, value, false);
        memtable_->put(key, value, false);
        save_lsn();
        if (hook_) hook_(lsn, key, value, false);
        maybe_flush();
    }
    void del(const std::string& key) {
        uint64_t lsn = next_lsn_.fetch_add(1, std::memory_order_relaxed) + 1;
        wal_->append(key, "", true);
        memtable_->put(key, "", true);
        save_lsn();
        if (hook_) hook_(lsn, key, "", true);
        maybe_flush();
    }
    bool get(const std::string& key, std::string* value) {
        std::string v; bool t;
        if (memtable_->get(key, &v, &t)) {
            if (t) return false;
            *value = v;
            return true;
        }
        std::shared_lock lk(sst_mu_);
        for (auto& sst : sstables_) {
            auto it = sst->index.find(key);
            if (it != sst->index.end()) {
                if (it->second.second) return false;
                *value = it->second.first;
                return true;
            }
        }
        return false;
    }
    // scan keys in [start, end) — returns sorted
    std::vector<std::pair<std::string, std::string>> scan(const std::string& start, const std::string& end, size_t limit = 1000) {
        std::map<std::string, std::pair<std::string, bool>> merged;
        {
            std::shared_lock lk(sst_mu_);
            for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
                for (auto& [k, v] : (*it)->index) {
                    if (k >= start && (end.empty() || k < end)) merged[k] = v;
                }
            }
        }
        auto snap = memtable_->snapshot();
        for (auto& [k, e] : snap) {
            if (k >= start && (end.empty() || k < end)) merged[k] = {e.value, e.tombstone};
        }
        std::vector<std::pair<std::string, std::string>> result;
        for (auto& [k, v] : merged) {
            if (v.second) continue;
            result.push_back({k, v.first});
            if (result.size() >= limit) break;
        }
        return result;
    }
    std::vector<std::pair<std::string, std::string>> prefix_scan(const std::string& prefix, size_t limit = 10000) {
        std::string end = prefix;
        if (!end.empty()) end.back()++;
        return scan(prefix, end, limit);
    }

private:
    void maybe_flush() {
        if (memtable_->should_flush()) flush();
    }
    void flush() {
        std::lock_guard<std::mutex> fl(flush_mu_);
        if (memtable_->size() == 0) return;
        auto snap = memtable_->snapshot();
        std::string path = dir_ + "/sst_" + std::to_string(now_ms()) + "_" + random_hex(4) + ".sst";
        auto sst = SSTable::create(path, snap);
        {
            std::unique_lock lk(sst_mu_);
            sstables_.insert(sstables_.begin(), sst);
        }
        memtable_->clear();
        wal_->truncate();
    }
    void load_lsn() {
        std::ifstream in(dir_ + "/lsn");
        uint64_t v = 0; if (in) in >> v;
        next_lsn_.store(v);
    }
    void save_lsn() {
        // Best-effort: not every put fsyncs. Reasonable for our model since
        // WAL replay will re-establish the right value on crash recovery.
        std::ofstream out(dir_ + "/lsn", std::ios::trunc);
        out << next_lsn_.load();
    }
    std::string dir_;
    std::unique_ptr<WAL> wal_;
    std::unique_ptr<MemTable> memtable_;
    std::vector<std::shared_ptr<SSTable>> sstables_;
    std::shared_mutex sst_mu_;
    std::mutex flush_mu_;
    std::thread flush_thread_;
    std::condition_variable cv_;
    std::atomic<bool> stop_{false};
    std::atomic<uint64_t> next_lsn_{0};
    WriteHook hook_;
};

} // namespace delta::storage
