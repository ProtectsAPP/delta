// =============================================================================
// cache_engine.hpp — sharded in-memory cache (Redis-shaped surface).
//
// Audit fixes wired in here:
//
//   P1-10 (unsubscribe memory leak): the previous implementation only
//          erased the (id → channel) reverse map; the actual callback
//          stayed in subs_[channel] forever and kept being invoked on
//          every publish, leaking captured state and — worse — firing
//          back into a destroyed connection. We now keep callbacks as
//          (id, cb) pairs and remove the matching id from subs_[channel].
//
//   P1-11 (LRU coverage): touch()/evict() used to look at the kv map
//          only. Hash/List/Set/ZSet keys grew unbounded. Now every
//          mutating operation calls touch(), evict() considers the total
//          key count across all five containers, and erase_locked()
//          drops the key from whichever container holds it.
//
//   P1-12 (gradual expiration): purge_expired_batch() samples a small
//          number of expired keys per shard per tick instead of taking
//          the lock for the full scan. The janitor thread should call
//          this every 100ms; purge_expired() is kept for tests/admin.
//
//   P2-2 (byte budget): every mutation path now increments/decrements a
//          per-shard `mem_used` counter using an O(1) delta computed from
//          the sizes of the keys/values being written or removed. `evict()`
//          trims the LRU tail while either the key count *or* the byte
//          count exceeds the configured caps. One 1 GB string no longer
//          blows the process past the memory budget — it triggers
//          immediate eviction of older keys to make room.
//
//   Constants centralised: SHARDS, default max_keys etc. read from
//          src/core/constants.hpp.
// =============================================================================
#pragma once
#include "../core/common.hpp"
#include "../core/constants.hpp"
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
    static constexpr size_t SHARDS = constants::CACHE_SHARD_COUNT; // power of 2

    explicit CacheEngine(size_t max_keys = constants::CACHE_DEFAULT_MAX_KEYS,
                         size_t max_bytes = constants::CACHE_DEFAULT_MAX_BYTES) {
        size_t per_shard_keys  = std::max<size_t>(1024, max_keys  / SHARDS);
        size_t per_shard_bytes = std::max<size_t>(1ull << 20, max_bytes / SHARDS);
        for (auto& s : shards_) {
            s.max_keys  = per_shard_keys;
            s.max_bytes = per_shard_bytes;
        }
    }

    // Live, cheap read. Sums Shard::mem_used without taking shard locks
    // (each shard's counter is a size_t — word-sized reads are safe on
    // every supported arch; worst case a caller sees a slightly stale
    // total which is fine for monitoring).
    size_t memory_used_bytes() const {
        size_t total = 0;
        for (auto& s : shards_) total += s.mem_used;
        return total;
    }

    // ---- string / general ----
    bool set(const std::string& k, const std::string& v, uint32_t ttl = 0) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        auto it = s.kv.find(k);
        if (it == s.kv.end()) {
            add_bytes(s, k.size() + v.size());
            s.kv.emplace(k, v);
        } else {
            add_delta_signed(s, (ssize_t)v.size() - (ssize_t)it->second.size());
            it->second = v;
        }
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
        std::string nv = std::to_string(cur);
        if (it == s.kv.end()) {
            add_bytes(s, k.size() + nv.size());
            s.kv.emplace(k, std::move(nv));
        } else {
            add_delta_signed(s, (ssize_t)nv.size() - (ssize_t)it->second.size());
            it->second = std::move(nv);
        }
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
        std::lock_guard<std::mutex> lk(s.mu);
        auto hit = s.hashes.find(k);
        if (hit == s.hashes.end()) {
            add_bytes(s, k.size() + f.size() + v.size());
            s.hashes[k][f] = v;
        } else {
            auto fit = hit->second.find(f);
            if (fit == hit->second.end()) {
                add_bytes(s, f.size() + v.size());
                hit->second.emplace(f, v);
            } else {
                add_delta_signed(s, (ssize_t)v.size() - (ssize_t)fit->second.size());
                fit->second = v;
            }
        }
        touch(s, k); evict(s); return true;
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
        auto fit = it->second.find(f);
        if (fit == it->second.end()) return false;
        sub_bytes(s, f.size() + fit->second.size());
        it->second.erase(fit);
        if (it->second.empty()) erase_locked(s, k);
        return true;
    }

    // ---- list ----
    int64_t lpush(const std::string& k, const std::string& v) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        bool fresh = !s.lists.count(k);
        add_bytes(s, (fresh ? k.size() : 0) + v.size());
        s.lists[k].push_front(v); touch(s, k); evict(s); return s.lists[k].size();
    }
    int64_t rpush(const std::string& k, const std::string& v) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        bool fresh = !s.lists.count(k);
        add_bytes(s, (fresh ? k.size() : 0) + v.size());
        s.lists[k].push_back(v); touch(s, k); evict(s); return s.lists[k].size();
    }
    std::optional<std::string> lpop(const std::string& k) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        auto it = s.lists.find(k); if (it == s.lists.end() || it->second.empty()) return std::nullopt;
        std::string v = it->second.front(); it->second.pop_front();
        sub_bytes(s, v.size());
        if (it->second.empty()) erase_locked(s, k);
        return v;
    }
    std::optional<std::string> rpop(const std::string& k) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        auto it = s.lists.find(k); if (it == s.lists.end() || it->second.empty()) return std::nullopt;
        std::string v = it->second.back(); it->second.pop_back();
        sub_bytes(s, v.size());
        if (it->second.empty()) erase_locked(s, k);
        return v;
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
        std::lock_guard<std::mutex> lk(s.mu);
        bool fresh = !s.sets.count(k);
        auto r = s.sets[k].insert(m);
        if (fresh)     add_bytes(s, k.size());
        if (r.second)  add_bytes(s, m.size());
        touch(s, k); evict(s); return r.second ? 1 : 0;
    }
    int64_t srem(const std::string& k, const std::string& m) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu); auto it = s.sets.find(k); if (it == s.sets.end()) return 0;
        int64_t n = it->second.erase(m);
        if (n) sub_bytes(s, m.size());
        if (it->second.empty()) erase_locked(s, k);
        return n;
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
        bool fresh = !s.zsets.count(k);
        auto& z = s.zsets[k];
        if (fresh) add_bytes(s, k.size());
        touch(s, k);
        auto it = z.member_to_score.find(m);
        if (it != z.member_to_score.end()) {
            z.entries.erase({it->second, m});
            z.member_to_score[m] = score;
            z.entries.insert({score, m});
            evict(s);
            return 0;
        }
        z.member_to_score[m] = score; z.entries.insert({score, m});
        add_bytes(s, m.size() + sizeof(double));
        evict(s);
        return 1;
    }
    int64_t zrem(const std::string& k, const std::string& m) {
        auto& s = shard(k);
        std::lock_guard<std::mutex> lk(s.mu);
        auto it = s.zsets.find(k); if (it == s.zsets.end()) return 0;
        auto mit = it->second.member_to_score.find(m); if (mit == it->second.member_to_score.end()) return 0;
        it->second.entries.erase({mit->second, m});
        it->second.member_to_score.erase(mit);
        sub_bytes(s, m.size() + sizeof(double));
        if (it->second.member_to_score.empty()) erase_locked(s, k);
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
    using PubCb = std::function<void(const std::string&, const std::string&)>;

    int64_t publish(const std::string& ch, const std::string& msg) {
        std::vector<PubCb> cbs;
        std::vector<PubCb> firehose;
        {
            std::lock_guard<std::mutex> lk(pubsub_mu_);
            auto it = subs_.find(ch);
            if (it != subs_.end()) {
                cbs.reserve(it->second.size());
                for (auto& s : it->second) cbs.push_back(s.cb);
            }
            firehose = firehose_;
        }
        for (auto& cb : cbs) cb(ch, msg);
        for (auto& cb : firehose) cb(ch, msg);
        return cbs.size();
    }
    // Global firehose used by transports (DeltaQL TCP, WS) so they can fan out
    // PUBLISH frames to whichever connection subscribed to a given channel.
    void on_publish(PubCb cb) {
        std::lock_guard<std::mutex> lk(pubsub_mu_);
        firehose_.push_back(std::move(cb));
    }
    uint64_t subscribe(const std::string& ch, PubCb cb) {
        std::lock_guard<std::mutex> lk(pubsub_mu_);
        uint64_t id = next_sub_id_++;
        subs_[ch].push_back({id, std::move(cb)});
        sub_ids_[id] = ch;
        return id;
    }
    // P1-10: drop the callback from subs_[channel] *and* the reverse map.
    void unsubscribe(uint64_t id) {
        std::lock_guard<std::mutex> lk(pubsub_mu_);
        auto it = sub_ids_.find(id);
        if (it == sub_ids_.end()) return;
        const std::string& ch = it->second;
        auto cit = subs_.find(ch);
        if (cit != subs_.end()) {
            auto& v = cit->second;
            v.erase(std::remove_if(v.begin(), v.end(),
                                   [id](const Sub& s){ return s.id == id; }),
                    v.end());
            if (v.empty()) subs_.erase(cit);
        }
        sub_ids_.erase(it);
    }
    size_t subscriber_count(const std::string& ch) {
        std::lock_guard<std::mutex> lk(pubsub_mu_);
        auto it = subs_.find(ch);
        return it == subs_.end() ? 0 : it->second.size();
    }

    // ---- stats ----
    struct Stats {
        size_t total_keys;
        size_t hits;
        size_t misses;
        double hit_rate;
        size_t mem_bytes;      // P2-2
    };
    Stats stats() {
        size_t total = 0, bytes = 0;
        for (auto& s : shards_) {
            std::lock_guard<std::mutex> lk(s.mu);
            total += s.kv.size() + s.hashes.size() + s.lists.size() + s.sets.size() + s.zsets.size();
            bytes += s.mem_used;
        }
        size_t h = hits_.load(std::memory_order_relaxed);
        size_t m = misses_.load(std::memory_order_relaxed);
        size_t total_lookups = h + m;
        return {total, h, m, total_lookups ? (double)h/total_lookups : 0.0, bytes};
    }

    // Full sweep (used by tests / admin endpoints).
    void purge_expired() {
        uint64_t cur = now_ms();
        for (auto& s : shards_) {
            std::lock_guard<std::mutex> lk(s.mu);
            std::vector<std::string> exp;
            for (auto& [k, t] : s.expires) if (cur >= t) exp.push_back(k);
            for (auto& k : exp) erase_locked(s, k);
        }
    }

    // P1-12: incremental expiration. Looks at at most `batch` expires per
    // shard so the janitor never holds a shard mutex for an entire million-
    // entry sweep. Returns the number of keys actually removed.
    size_t purge_expired_batch(size_t batch = constants::CACHE_PURGE_BATCH_SIZE) {
        uint64_t cur = now_ms();
        size_t removed = 0;
        for (auto& s : shards_) {
            std::lock_guard<std::mutex> lk(s.mu);
            size_t inspected = 0;
            std::vector<std::string> exp;
            for (auto& [k, t] : s.expires) {
                if (inspected++ >= batch) break;
                if (cur >= t) exp.push_back(k);
            }
            for (auto& k : exp) { erase_locked(s, k); ++removed; }
        }
        return removed;
    }

private:
    struct Shard {
        std::mutex mu;
        size_t max_keys  = 0;
        size_t max_bytes = 0;           // P2-2
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
        // P1-11: shared LRU across all five containers.
        std::list<std::string> lru;
        std::unordered_map<std::string, std::list<std::string>::iterator> lru_pos;
        // P2-2: live byte total of all key/value payloads in this shard.
        // Updated incrementally at every mutation site so the evictor can
        // make O(1) decisions. NOT an exact heap-footprint — it ignores
        // per-entry STL overhead — but it's a tight enough approximation
        // to keep the process inside a user-supplied memory budget.
        size_t mem_used  = 0;

        size_t total_keys() const {
            return kv.size() + hashes.size() + lists.size() + sets.size() + zsets.size();
        }
    };
    std::array<Shard, SHARDS> shards_;
    std::atomic<size_t> hits_{0}, misses_{0};

    // pubsub state (centralized, low contention)
    struct Sub { uint64_t id; PubCb cb; };
    std::mutex pubsub_mu_;
    std::unordered_map<std::string, std::vector<Sub>> subs_;
    std::unordered_map<uint64_t, std::string> sub_ids_;
    std::vector<PubCb> firehose_;
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
    // Subtract the full byte-footprint of `k` across whichever container
    // holds it, then erase the key from every container + LRU bookkeeping.
    static bool erase_locked(Shard& s, const std::string& k) {
        bool removed = false;
        size_t freed = 0;
        if (auto it = s.kv.find(k); it != s.kv.end()) {
            freed += k.size() + it->second.size();
            s.kv.erase(it); removed = true;
        }
        if (auto it = s.hashes.find(k); it != s.hashes.end()) {
            freed += k.size();
            for (auto& [f, v] : it->second) freed += f.size() + v.size();
            s.hashes.erase(it); removed = true;
        }
        if (auto it = s.lists.find(k); it != s.lists.end()) {
            freed += k.size();
            for (auto& v : it->second) freed += v.size();
            s.lists.erase(it); removed = true;
        }
        if (auto it = s.sets.find(k); it != s.sets.end()) {
            freed += k.size();
            for (auto& m : it->second) freed += m.size();
            s.sets.erase(it); removed = true;
        }
        if (auto it = s.zsets.find(k); it != s.zsets.end()) {
            freed += k.size();
            for (auto& [m, _] : it->second.member_to_score) freed += m.size() + sizeof(double);
            s.zsets.erase(it); removed = true;
        }
        sub_bytes(s, freed);
        s.expires.erase(k);
        auto it = s.lru_pos.find(k);
        if (it != s.lru_pos.end()) { s.lru.erase(it->second); s.lru_pos.erase(it); }
        return removed;
    }

    static void add_bytes(Shard& s, size_t n) { s.mem_used += n; }
    static void sub_bytes(Shard& s, size_t n) {
        s.mem_used = (s.mem_used > n) ? s.mem_used - n : 0;
    }
    static void add_delta_signed(Shard& s, ssize_t d) {
        if (d >= 0) add_bytes(s, (size_t)d);
        else        sub_bytes(s, (size_t)(-d));
    }
    static void touch(Shard& s, const std::string& k) {
        auto it = s.lru_pos.find(k);
        if (it != s.lru_pos.end()) s.lru.erase(it->second);
        s.lru.push_front(k);
        s.lru_pos[k] = s.lru.begin();
    }
    // P1-11 + P2-2: evict the LRU tail while *either* the key count or
    // the live byte count exceeds its cap. Going through erase_locked()
    // keeps mem_used in sync (it subtracts the evictee's footprint).
    static void evict(Shard& s) {
        while (!s.lru.empty() &&
               (s.total_keys() > s.max_keys ||
                (s.max_bytes > 0 && s.mem_used > s.max_bytes))) {
            std::string victim = s.lru.back();
            erase_locked(s, victim);
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
