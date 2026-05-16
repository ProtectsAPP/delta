#pragma once
// =============================================================================
// shard_router.hpp — Round 3 sharding gateway for Delta.
//
// Topology model
//   * Each `delta_server` process belongs to exactly one shard (a Raft
//     group with N >= 1 peers). The shard id is set at startup via
//     --shard-id and persists across restarts.
//   * EVERY node knows the full ShardMap: a list of shards, each with an
//     id and the base URLs of its peers (one of which is the current
//     Raft leader at any instant).
//   * Routing key: for document-scoped operations we hash the document
//     id; for collection/database/schema metadata we replicate to ALL
//     shards (each shard owns a full copy so reads stay node-local).
//
// Consistent hashing
//   * `vnodes_per_shard` (>= 128, default 256) virtual nodes per shard
//     on a unit ring of uint64 positions. The owning shard for a key is
//     the first vnode at or after `hash64(key)`, wrapping. This keeps
//     the map of (key -> shard) stable under shard add/remove: only
//     ~1/N of keys move when a shard joins or leaves.
//   * Hash: FNV-1a 64-bit. Not crypto-strong, but branch-free,
//     dependency-free, and uniformly distributed across our key shape
//     (UUID-like 32-char hex doc ids).
// =============================================================================
#include "../core/common.hpp"

#include <algorithm>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace delta::cluster {

using ShardId = std::string;

struct ShardPeer {
    std::string node_id;     // raft NodeId, unique cluster-wide
    std::string host;
    int         port = 0;
    std::string base_url() const {
        return std::string("http://") + host + ":" + std::to_string(port);
    }
    bool valid() const {
        return !node_id.empty() && !host.empty() && port > 0;
    }
};

struct ShardSpec {
    ShardId               id;
    std::vector<ShardPeer> peers;
    bool valid() const { return !id.empty() && !peers.empty(); }
};

// 64-bit FNV-1a with a SplitMix64 finalizer. Stable across platforms and
// compilers. FNV-1a alone has measurable bias when feeding very similar
// short strings ("s0#0", "s0#1", …) onto a ring; the finalizer cheaply
// scrambles the result and tightens the empirical distribution.
inline uint64_t mix64(uint64_t x) {
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}
inline uint64_t fnv1a64(const std::string& s) {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (unsigned char c : s) {
        h ^= c;
        h *= 0x100000001b3ULL;
    }
    return mix64(h);
}

// ---------------------------------------------------------------------------
// ShardMap — immutable-after-construction topology snapshot.
// ---------------------------------------------------------------------------
class ShardMap {
public:
    ShardMap() = default;

    ShardMap(std::vector<ShardSpec> shards, int vnodes_per_shard = 256)
        : shards_(std::move(shards)), vnodes_(vnodes_per_shard) {
        if (vnodes_ < 128) vnodes_ = 128;
        for (size_t i = 0; i < shards_.size(); ++i) {
            auto& sh = shards_[i];
            if (!sh.valid()) continue;
            for (int v = 0; v < vnodes_; ++v) {
                std::string slot = sh.id + "#" + std::to_string(v);
                uint64_t pos = fnv1a64(slot);
                ring_[pos] = sh.id;
            }
            // Store an INDEX (stable under copy/move) instead of a raw
            // pointer into shards_. Copying / moving a ShardMap by value
            // (which set_sharding does once at startup) would otherwise
            // leave index_ pointing at freed memory and segfault the
            // server the first time someone hit /cluster/shards/route.
            index_[sh.id] = i;
        }
    }

    // Pick the shard that owns `key`. Throws if the map is empty.
    const ShardId& route(const std::string& key) const {
        if (ring_.empty()) {
            throw std::runtime_error("shard map empty; cannot route");
        }
        uint64_t pos = fnv1a64(key);
        auto it = ring_.lower_bound(pos);
        if (it == ring_.end()) it = ring_.begin();     // wrap
        return it->second;
    }

    bool empty() const { return shards_.empty(); }
    size_t shard_count() const { return shards_.size(); }
    const std::vector<ShardSpec>& shards() const { return shards_; }
    int vnodes() const { return vnodes_; }

    const ShardSpec* find_shard(const ShardId& id) const {
        auto it = index_.find(id);
        if (it == index_.end()) return nullptr;
        return &shards_[it->second];
    }

    // List every shard id other than `self`.
    std::vector<ShardId> peers_of_self(const ShardId& self) const {
        std::vector<ShardId> out;
        out.reserve(shards_.size());
        for (auto& s : shards_) if (s.id != self) out.push_back(s.id);
        return out;
    }

    // Serialisation: stable JSON for persistence in delta_system.shards.
    json to_json() const {
        json arr = json::array();
        for (auto& s : shards_) {
            json peers = json::array();
            for (auto& p : s.peers) {
                peers.push_back({
                    {"node_id", p.node_id},
                    {"host",    p.host},
                    {"port",    p.port},
                });
            }
            arr.push_back({{"id", s.id}, {"peers", peers}});
        }
        return json{{"version", 1}, {"vnodes_per_shard", vnodes_},
                    {"shards", arr}};
    }

    static ShardMap from_json(const json& j) {
        std::vector<ShardSpec> shards;
        if (j.contains("shards") && j["shards"].is_array()) {
            for (auto& s : j["shards"]) {
                ShardSpec sh;
                sh.id = s.value("id", std::string());
                if (s.contains("peers") && s["peers"].is_array()) {
                    for (auto& p : s["peers"]) {
                        ShardPeer pp;
                        pp.node_id = p.value("node_id", std::string());
                        pp.host    = p.value("host",    std::string());
                        pp.port    = p.value("port",    0);
                        if (pp.valid()) sh.peers.push_back(std::move(pp));
                    }
                }
                if (sh.valid()) shards.push_back(std::move(sh));
            }
        }
        int v = j.value("vnodes_per_shard", 256);
        return ShardMap(std::move(shards), v);
    }

    // Parser for the CLI form "shard_id=id1@host:port,id2@host:port,..."
    // Returns invalid ShardSpec on malformed input.
    static ShardSpec parse_cli_spec(const std::string& s) {
        ShardSpec out;
        auto eq = s.find('=');
        if (eq == std::string::npos) return out;
        out.id = s.substr(0, eq);
        std::string rest = s.substr(eq + 1);
        size_t start = 0;
        while (start <= rest.size()) {
            size_t comma = rest.find(',', start);
            std::string tok = rest.substr(start,
                comma == std::string::npos ? std::string::npos : comma - start);
            auto l = tok.find_first_not_of(" \t");
            auto r = tok.find_last_not_of(" \t");
            if (l != std::string::npos) {
                tok = tok.substr(l, r - l + 1);
                auto at = tok.find('@');
                auto col = tok.rfind(':');
                if (at != std::string::npos && col != std::string::npos &&
                    col > at + 1) {
                    ShardPeer p;
                    p.node_id = tok.substr(0, at);
                    p.host    = tok.substr(at + 1, col - at - 1);
                    try { p.port = std::stoi(tok.substr(col + 1)); }
                    catch (...) { p.port = 0; }
                    if (p.valid()) out.peers.push_back(std::move(p));
                }
            }
            if (comma == std::string::npos) break;
            start = comma + 1;
        }
        if (out.peers.empty()) out.id.clear();
        return out;
    }

private:
    std::vector<ShardSpec> shards_;
    int vnodes_ = 256;
    std::map<uint64_t, ShardId> ring_;
    std::unordered_map<ShardId, size_t> index_;   // shard_id -> shards_ idx
};

// ---------------------------------------------------------------------------
// Document-scoped routing helpers.
//
// For document operations the consistent-hash key is the document id; that
// id is also the suffix of every storage key (`doc:db:sch:col:{id}`), of
// every index entry, and of every WAL frame, so a single hash decides
// where ALL state for one logical document lives.
//
// Collection / database / schema / auth state lives outside the document
// keyspace and is REPLICATED to every shard. The HTTP layer broadcasts
// those writes to every shard's leader at handler time, so a follower
// scan on any node can answer collection-list, login, RLS, etc.
// ---------------------------------------------------------------------------

// Route by document id directly.
inline const ShardId& route_doc(const ShardMap& m, const std::string& doc_id) {
    return m.route(doc_id);
}

// Pick a deterministic peer URL for a shard. Callers that don't yet know
// the current Raft leader for the shard hit ANY peer; that peer responds
// 503 with `data.leader_id` and the caller retries. The first peer in
// the configured order acts as a stable initial guess.
inline std::string first_peer_url(const ShardSpec& s) {
    return s.peers.empty() ? std::string() : s.peers.front().base_url();
}

inline std::vector<std::string> all_peer_urls(const ShardSpec& s) {
    std::vector<std::string> out;
    out.reserve(s.peers.size());
    for (auto& p : s.peers) out.push_back(p.base_url());
    return out;
}

} // namespace delta::cluster
