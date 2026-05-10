#pragma once
#include "../core/common.hpp"
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <deque>
#include <functional>
#include <atomic>
#include <array>
#include <shared_mutex>

namespace delta::cache {

// Sharded in-memory cache. Every key is routed to exactly one shard via
// std::hash(key) % SHARDS. Each shard owns an independent mutex so concurrent
// access on different keys can proceed in parallel. This dramatically reduces
// lock contention under high QPS workloads (DDoS / stress).
class CacheEngine {
public:
    static constexpr size_t SHARDS = 32; // power of 2 keeps modulo cheap

    explicit CacheEngine(size_t max_keys = 1000000) {
        size_t per_shard = std::max<size_t>(1024, max_keys / SHARDS);
        for (auto& s : shards_) s.max_keys = per_shard;
    }

    // ---- string / general ----
    bool set(const std::string& k, const std::string& v, uint32_t ttl = 0) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        s.kv[k] = v;
        if (ttl > 0) s.expires[k] = now_ms() + (uint64_t)ttl * 1000;
        else s.expires.erase(k);
        touch(s, k);
        evict(s);
        return true;
    }
    std::optional<std::string> get(const std::string& k) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        if (is_expired(s, k)) { erase_locked(s, k); misses_.fetch_add(1, std::memory_order_relaxed); return std::nullopt; }
        auto it = s.kv.find(k);
        if (it == s.kv.end()) { misses_.fetch_add(1, std::memory_order_relaxed); return std::nullopt; }
        hits_.fetch_add(1, std::memory_order_relaxed);
        touch(s, k);
        return it->second;
    }
    bool del(const std::string& k) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        return erase_locked(s, k);
    }
    bool exists(const std::string& k) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        if (is_expired(s, k)) { erase_locked(s, k); return false; }
        return has_any(s, k);
    }
    int64_t incr(const std::string& k, int64_t delta = 1) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        if (is_expired(s, k)) erase_locked(s, k);
        int64_t cur = 0;
        auto it = s.kv.find(k);
        if (it != s.kv.end()) { try { cur = std::stoll(it->second); } catch(...) { cur = 0; } }
        cur += delta;
        s.kv[k] = std::to_string(cur);
        touch(s, k); evict(s);
        return cur;
    }
    int64_t decr(const std::string& k, int64_t delta = 1) { return incr(k, -delta); }

    bool expire(const std::string& k, uint32_t seconds) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        if (!has_any(s, k)) return false;
        s.expires[k] = now_ms() + (uint64_t)seconds * 1000;
        return true;
    }
    int64_t ttl(const std::string& k) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        if (!has_any(s, k)) return -2;
        auto it = s.expires.find(k);
        if (it == s.expires.end()) return -1;
        int64_t r = (int64_t)((it->second - now_ms()) / 1000);
        return r < 0 ? -2 : r;
    }
    bool persist(const std::string& k) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        return s.expires.erase(k) > 0;
    }

    std::vector<std::string> keys(const std::string& pattern = "*") {
        std::vector<std::string> out;
        for (auto& s : shards_) {
            std::lock_guard<std::mutex> lk(s.mu);
            std::unordered_set<std::string> seen;
            auto add = [&](const std::string& k){ if (!seen.count(k) && match_glob(k, pattern)) { out.push_back(k); seen.insert(k); } };
            for (auto& [k,_] : s.kv) add(k);
            for (auto& [k,_] : s.hashes) add(k);
            for (auto& [k,_] : s.lists) add(k);
            for (auto& [k,_] : s.sets) add(k);
            for (auto& [k,_] : s.zsets) add(k);
        }
        return out;
    }

    // ---- hash ----
    bool hset(const std::string& k, const std::string& f, const std::string& v) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu); s.hashes[k][f] = v; touch(s, k); return true;
    }
    std::optional<std::string> hget(const std::string& k, const std::string& f) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        auto it = s.hashes.find(k); if (it == s.hashes.end()) return std::nullopt;
        auto fit = it->second.find(f); if (fit == it->second.end()) return std::nullopt;
        return fit->second;
    }
    std::unordered_map<std::string,std::string> hgetall(const std::string& k) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        auto it = s.hashes.find(k); return it == s.hashes.end() ? std::unordered_map<std::string,std::string>{} : it->second;
    }
    bool hdel(const std::string& k, const std::string& f) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        auto it = s.hashes.find(k); if (it == s.hashes.end()) return false;
        return it->second.erase(f) > 0;
    }

    // ---- list ----
    int64_t lpush(const std::string& k, const std::string& v) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu); s.lists[k].push_front(v); touch(s, k); return s.lists[k].size();
    }
    int64_t rpush(const std::string& k, const std::string& v) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu); s.lists[k].push_back(v); touch(s, k); return s.lists[k].size();
    }
    std::optional<std::string> lpop(const std::string& k) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        auto it = s.lists.find(k); if (it == s.lists.end() || it->second.empty()) return std::nullopt;
        std::string v = it->second.front(); it->second.pop_front(); return v;
    }
    std::optional<std::string> rpop(const std::string& k) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        auto it = s.lists.find(k); if (it == s.lists.end() || it->second.empty()) return std::nullopt;
        std::string v = it->second.back(); it->second.pop_back(); return v;
    }
    std::vector<std::string> lrange(const std::string& k, int64_t start, int64_t stop) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        auto it = s.lists.find(k); if (it == s.lists.end()) return {};
        int64_t n = it->second.size();
        if (start < 0) start += n; if (stop < 0) stop += n;
        if (start < 0) start = 0; if (stop >= n) stop = n - 1;
        std::vector<std::string> out;
        if (start > stop) return out;
        auto bi = it->second.begin();
        std::advance(bi, start);
        for (int64_t i = start; i <= stop; i++, ++bi) out.push_back(*bi);
        return out;
    }

    // ---- set ----
    int64_t sadd(const std::string& k, const std::string& m) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu); auto r = s.sets[k].insert(m); touch(s, k); return r.second ? 1 : 0;
    }
    int64_t srem(const std::string& k, const std::string& m) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu); auto it = s.sets.find(k); if (it == s.sets.end()) return 0;
        return it->second.erase(m);
    }
    bool sismember(const std::string& k, const std::string& m) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu); auto it = s.sets.find(k); if (it == s.sets.end()) return false;
        return it->second.count(m) > 0;
    }
    std::vector<std::string> smembers(const std::string& k) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu); auto it = s.sets.find(k); if (it == s.sets.end()) return {};
        return std::vector<std::string>(it->second.begin(), it->second.end());
    }

    // ---- zset ----
    int64_t zadd(const std::string& k, double score, const std::string& m) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        auto& z = s.zsets[k]; touch(s, k);
        auto it = z.member_to_score.find(m);
        if (it != z.member_to_score.end()) {
            z.entries.erase({it->second, m});
            z.member_to_score[m] = score;
            z.entries.insert({score, m});
            return 0;
        }
        z.member_to_score[m] = score; z.entries.insert({score, m});
        return 1;
    }
    int64_t zrem(const std::string& k, const std::string& m) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        auto it = s.zsets.find(k); if (it == s.zsets.end()) return 0;
        auto mit = it->second.member_to_score.find(m); if (mit == it->second.member_to_score.end()) return 0;
        it->second.entries.erase({mit->second, m});
        it->second.member_to_score.erase(mit);
        return 1;
    }
    std::vector<std::pair<std::string, double>> zrange(const std::string& k, int64_t start, int64_t stop, bool /*with_scores*/=false) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        auto it = s.zsets.find(k); if (it == s.zsets.end()) return {};
        auto& es = it->second.entries;
        int64_t n = es.size();
        if (start < 0) start += n; if (stop < 0) stop += n;
        if (start < 0) start = 0; if (stop >= n) stop = n - 1;
        std::vector<std::pair<std::string,double>> out;
        if (start > stop) return out;
        auto i = es.begin();
        std::advance(i, start);
        for (int64_t j = start; j <= stop; j++, ++i) out.push_back({i->second, i->first});
        return out;
    }

    // ---- pubsub (kept centralized; rare hot path) ----
    int64_t publish(const std::string& ch, const std::string& msg) {
        std::vector<std::function<void(const std::string&, const std::string&)>> cbs;
        std::vector<std::function<void(const std::string&, const std::string&)>> firehose;
        {
            std::lock_guard<std::mutex> lk(pubsub_mu_);
            auto it = subs_.find(ch);
            if (it != subs_.end()) cbs = it->second;
            firehose = firehose_;
        }
        for (auto& cb : cbs) cb(ch, msg);
        for (auto& cb : firehose) cb(ch, msg);
        return cbs.size();
    }
    // Global firehose used by transports (DeltaQL TCP, WS) so they can fan out
    // PUBLISH frames to whichever connection subscribed to a given channel.
    void on_publish(std::function<void(const std::string&, const std::string&)> cb) {
        std::lock_guard<std::mutex> lk(pubsub_mu_);
        firehose_.push_back(std::move(cb));
    }
    uint64_t subscribe(const std::string& ch, std::function<void(const std::string&, const std::string&)> cb) {
        std::lock_guard<std::mutex> lk(pubsub_mu_);
        uint64_t id = next_sub_id_++;
        subs_[ch].push_back(cb);
        sub_ids_[id] = ch;
        return id;
    }
    void unsubscribe(uint64_t id) {
        std::lock_guard<std::mutex> lk(pubsub_mu_);
        auto it = sub_ids_.find(id); if (it == sub_ids_.end()) return;
        sub_ids_.erase(it);
    }

    // ---- stats ----
    struct Stats { size_t total_keys; size_t hits; size_t misses; double hit_rate; };
    Stats stats() {
        size_t total = 0;
        for (auto& s : shards_) {
            std::lock_guard<std::mutex> lk(s.mu);
            total += s.kv.size() + s.hashes.size() + s.lists.size() + s.sets.size() + s.zsets.size();
        }
        size_t h = hits_.load(std::memory_order_relaxed);
        size_t m = misses_.load(std::memory_order_relaxed);
        size_t total_lookups = h + m;
        return {total, h, m, total_lookups ? (double)h/total_lookups : 0.0};
    }

    // periodic expire (called from a single janitor thread)
    void purge_expired() {
        uint64_t cur = now_ms();
        for (auto& s : shards_) {
            std::lock_guard<std::mutex> lk(s.mu);
            std::vector<std::string> exp;
            for (auto& [k, t] : s.expires) if (cur >= t) exp.push_back(k);
            for (auto& k : exp) erase_locked(s, k);
        }
    }

private:
    struct Shard {
        std::mutex mu;
        size_t max_keys = 0;
        std::unordered_map<std::string, std::string> kv;
        std::unordered_map<std::string, std::unordered_map<std::string,std::string>> hashes;
        std::unordered_map<std::string, std::deque<std::string>> lists;
        std::unordered_map<std::string, std::unordered_set<std::string>> sets;
        struct ZSet {
            std::set<std::pair<double, std::string>> entries;
            std::unordered_map<std::string, double> member_to_score;
        };
        std::unordered_map<std::string, ZSet> zsets;
        std::unordered_map<std::string, uint64_t> expires;
        // LRU just for kv eviction (other types rarely mass-dropped)
        std::list<std::string> lru;
        std::unordered_map<std::string, std::list<std::string>::iterator> lru_pos;
    };
    std::array<Shard, SHARDS> shards_;
    std::atomic<size_t> hits_{0}, misses_{0};

    // pubsub state (centralized, low contention)
    std::mutex pubsub_mu_;
    std::unordered_map<std::string, std::vector<std::function<void(const std::string&,const std::string&)>>> subs_;
    std::unordered_map<uint64_t, std::string> sub_ids_;
    std::vector<std::function<void(const std::string&,const std::string&)>> firehose_;
    uint64_t next_sub_id_ = 1;

    static inline size_t hash_key(const std::string& k) {
        return std::hash<std::string>{}(k);
    }
    Shard& shard(const std::string& k) { return shards_[hash_key(k) & (SHARDS - 1)]; }

    static bool has_any(const Shard& s, const std::string& k) {
        return s.kv.count(k) || s.hashes.count(k) || s.lists.count(k) || s.sets.count(k) || s.zsets.count(k);
    }
    static bool is_expired(const Shard& s, const std::string& k) {
        auto it = s.expires.find(k); return it != s.expires.end() && now_ms() >= it->second;
    }
    static bool erase_locked(Shard& s, const std::string& k) {
        bool removed = false;
        if (s.kv.erase(k)) removed = true;
        if (s.hashes.erase(k)) removed = true;
        if (s.lists.erase(k)) removed = true;
        if (s.sets.erase(k)) removed = true;
        if (s.zsets.erase(k)) removed = true;
        s.expires.erase(k);
        auto it = s.lru_pos.find(k);
        if (it != s.lru_pos.end()) { s.lru.erase(it->second); s.lru_pos.erase(it); }
        return removed;
    }
    static void touch(Shard& s, const std::string& k) {
        auto it = s.lru_pos.find(k);
        if (it != s.lru_pos.end()) s.lru.erase(it->second);
        s.lru.push_front(k);
        s.lru_pos[k] = s.lru.begin();
    }
    static void evict(Shard& s) {
        while (s.kv.size() > s.max_keys && !s.lru.empty()) {
            std::string k = s.lru.back(); s.lru.pop_back();
            s.lru_pos.erase(k);
            s.kv.erase(k); s.expires.erase(k);
        }
    }
    static bool match_glob(const std::string& s, const std::string& p) {
        size_t si=0, pi=0, star=std::string::npos, ss=0;
        while (si < s.size()) {
            if (pi < p.size() && (p[pi] == '?' || p[pi] == s[si])) { si++; pi++; }
            else if (pi < p.size() && p[pi] == '*') { star = pi++; ss = si; }
            else if (star != std::string::npos) { pi = star + 1; si = ++ss; }
            else return false;
        }
        while (pi < p.size() && p[pi] == '*') pi++;
        return pi == p.size();
    }
};

} // namespace delta::cache
