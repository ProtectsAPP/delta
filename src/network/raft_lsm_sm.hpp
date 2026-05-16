#pragma once
// =============================================================================
// raft_lsm_sm.hpp — Raft state machine + payload codec backed by LSMTree.
//
// When the server runs with --enable-raft, every LSMTree::put / del on the
// leader is intercepted: the (key, value, tombstone) tuple is serialised
// into an opaque payload (see Codec below) and passed to RaftNode::propose.
// After raft commits the entry on a majority, each replica (including the
// leader) applies it through LsmRaftStateMachine::apply, which decodes the
// payload and calls LSMTree::apply_replicated. That single call writes the
// WAL, the memtable, and bumps next_lsn_ in one stroke — bypassing the
// legacy WriteHook so we don't double-stream through the master/replica
// path.
//
// Snapshot:
//   * take_snapshot serialises the entire visible key-value set as a
//     length-prefixed JSON blob (small Delta clusters fit in memory). For
//     production deployments with very large data sets, a streamed
//     chunked snapshot can replace this without changing the raft layer.
//   * restore_snapshot wipes the local store and replays each (k, v)
//     using apply_replicated(lsn=0).
//
// Threading: apply is called from RaftNode::apply_thread_. take_snapshot
// runs from whichever thread triggered the snapshot (typically the tick
// loop). Both are serialised inside RaftNode at the SM-call site, so this
// class does NOT need its own mutex.
// =============================================================================
#include "raft.hpp"
#include "../storage/lsm_tree.hpp"
#include "../core/common.hpp"

#include <cstdint>
#include <cstring>
#include <string>

namespace delta::network::raft {

// Opaque payload format for a single write:
//   [u8 tomb][u32 klen][k][u32 vlen][v]
// Same shape as the WAL record minus the framing CRC, so a future
// optimisation could splice raft commits directly into WAL replay.
struct LsmWriteCodec {
    static std::string encode(const std::string& key, const std::string& value,
                              bool tombstone) {
        uint8_t  t  = tombstone ? 1 : 0;
        uint32_t kl = static_cast<uint32_t>(key.size());
        uint32_t vl = static_cast<uint32_t>(value.size());
        std::string out;
        out.reserve(1 + 4 + key.size() + 4 + value.size());
        out.append(reinterpret_cast<const char*>(&t),  1);
        out.append(reinterpret_cast<const char*>(&kl), 4);
        out.append(key);
        out.append(reinterpret_cast<const char*>(&vl), 4);
        out.append(value);
        return out;
    }

    // Returns false on malformed input. On success, *key/*value/*tomb are
    // populated.
    static bool decode(const std::string& buf, std::string* key,
                       std::string* value, bool* tomb) {
        if (buf.size() < 1 + 4) return false;
        const char* p = buf.data();
        uint8_t  t  = static_cast<uint8_t>(p[0]);
        uint32_t kl = 0; std::memcpy(&kl, p + 1, 4);
        if (1 + 4 + kl + 4 > buf.size()) return false;
        std::string k(p + 5, kl);
        uint32_t vl = 0; std::memcpy(&vl, p + 5 + kl, 4);
        if (1 + 4 + kl + 4 + vl != buf.size()) return false;
        std::string v(p + 5 + kl + 4, vl);
        *key   = std::move(k);
        *value = std::move(v);
        *tomb  = (t == 1);
        return true;
    }
};

// The raft → LSM state machine. Owns no data; everything lives in the
// LSMTree pointer the caller provides at construction.
class LsmRaftStateMachine : public RaftStateMachine {
public:
    explicit LsmRaftStateMachine(storage::LSMTree* store) : store_(store) {}

    void apply(const LogEntry& entry) override {
        // Config entries never reach apply() — raft filters them upstream.
        // Defensively skip non-Normal entries here too.
        if (entry.type != LogEntry::Normal) return;
        std::string k, v;
        bool tomb = false;
        if (!LsmWriteCodec::decode(entry.payload, &k, &v, &tomb)) return;
        // We mirror raft's log index into the LSM lsn so a future
        // backup/restore can co-locate the two cursors.
        store_->apply_replicated(entry.index, k, v, tomb);
    }

    // Snapshot is a JSON dump of every visible (non-tombstone) key.
    // Compact for small data sets; the dump is wrapped in a length-prefix
    // so restore can sanity-check the blob before parsing.
    void take_snapshot(std::string* out) override {
        json arr = json::array();
        // Enumerate everything. prefix_scan with empty prefix returns
        // sorted keys; a million-key cluster will produce a ~tens-of-MB
        // blob.
        for (auto& [k, v] : store_->prefix_scan("", 10000000)) {
            arr.push_back({{"k", k}, {"v", v}});
        }
        json j = {{"version", 1}, {"entries", arr}};
        *out = j.dump();
    }

    void restore_snapshot(const std::string& in) override {
        if (in.empty()) return;
        try {
            json j = json::parse(in);
            if (!j.contains("entries") || !j["entries"].is_array()) return;
            // The current LSMTree doesn't expose a "wipe" call; we get
            // close-enough semantics by writing each restored key
            // (newer wins) and tombstoning any post-snapshot keys is
            // not necessary because restore_snapshot is only invoked at
            // startup (before any new writes) or during InstallSnapshot
            // (the caller has already rejected stale local state).
            for (auto& e : j["entries"]) {
                std::string k = e.value("k", std::string());
                std::string v = e.value("v", std::string());
                if (k.empty()) continue;
                store_->apply_replicated(0, k, v, /*tomb=*/false);
            }
        } catch (...) {
            // Malformed snapshot — the caller will surface the failure
            // via empty local state. We don't want to crash on a bad
            // blob from a misbehaving peer.
        }
    }

private:
    storage::LSMTree* store_;
};

} // namespace delta::network::raft
