// =============================================================================
// collection.hpp — document store layered over the LSM tree.
//
// Audit fixes wired in here:
//
//   P0-5 (concurrency): every public mutating method now takes a unique
//          lock on `mu_` (a std::shared_mutex). Read methods (get / find /
//          count / aggregate / list_collections / get_collection) take a
//          shared lock. Before this, two concurrent inserts could pass the
//          unique-index check at the same time and both succeed, producing
//          duplicates in a "unique" index.
//
//   P0-3 indirectly: writes go through LSMTree which now serializes WAL
//          and memtable updates, so the write hook ordering is sane.
//
//   COLLECTION_MAX_DOC_BYTES (P1-20 family): documents larger than the
//          per-record cap are rejected with INVALID instead of silently
//          breaking WAL framing.
//
//   P0-4 (index-driven query plan): find()/count() now run plan_query()
//          which spots equality (`field: V`, `field: {$eq: V}`) and
//          `$in` clauses against a *single-field* secondary index, and
//          fetches only the matching doc IDs via the idx: keyspace
//          instead of a full doc: prefix scan. Anything not covered by
//          an index keeps the original linear-scan fallback so all
//          existing semantics are preserved bit-for-bit.
//
//   P1-6 (optimistic locking): update() accepts an optional
//          `expected_version` parameter. When non-zero, the write is
//          rejected with Status::Conflict if the stored document's
//          version has advanced past the value the caller read. Clients
//          supply it via the HTTP `If-Match: <version>` header (RFC 7232
//          style). expected_version == 0 keeps the legacy last-write-
//          wins behaviour intact for existing callers.
//
// What is intentionally still TODO (tracked in the audit roadmap):
//   * Multi-document ACID transactions.
// =============================================================================
#pragma once
#include "document.hpp"
#include "constants.hpp"
#include "../storage/lsm_tree.hpp"
#include "../cluster/hlc.hpp"
#include <set>
#include <shared_mutex>

namespace delta {

// Document storage layered over LSM-Tree.
// Key format: doc:{db}:{schema}:{collection}:{id}
// Meta key: meta:collection:{db}:{schema}:{collection}
// Index key: idx:{db}:{schema}:{collection}:{indexname}:{value}:{docid}
// Forward declaration so we can friend it without including transaction.hpp
// (which itself needs to know about CollectionEngine).
class Transaction;

class CollectionEngine {
public:
    explicit CollectionEngine(storage::LSMTree* store) : store_(store) {}

    // ----------- B.2 multi-master ----------------------------------------
    // The engine owns a single HybridLogicalClock instance. Every multi-
    // master write goes through `hlc_.tick()` to stamp the envelope, and
    // every received remote change feeds `hlc_.update()` to keep the
    // local clock ahead of incoming HLCs. Single-master collections are
    // unaffected (the field is simply absent).
    cluster::HybridLogicalClock& hlc() { return hlc_; }

    // Apply a remote change under last-writer-wins semantics. `change`
    // carries either a full envelope (insert/update) or a tombstone
    // marker `{"_tombstone": true, "_id": ..., "_hlc": ...}`. The
    // returned status reports whether we actually accepted it (Ok) or
    // discarded as stale (already-applied / older HLC); both are
    // success outcomes from the caller's perspective. Hard errors
    // (corrupt JSON, missing collection) bubble up as non-Ok.
    Status apply_remote_change(const std::string& db,
                               const std::string& sch,
                               const std::string& col,
                               const json& change,
                               bool* applied_out = nullptr) {
        if (applied_out) *applied_out = false;
        std::unique_lock<std::shared_mutex> lk(mu_);
        CollectionMeta m;
        if (!get_collection_unlocked(db, sch, col, &m))
            return Status::NotFound("collection not found");
        if (!m.multi_master)
            return Status::Invalid("collection is not multi_master");
        if (!change.is_object() || !change.contains("_id"))
            return Status::Invalid("change envelope missing _id");
        std::string id = change["_id"].get<std::string>();
        std::string remote_hlc = change.value("_hlc", std::string());
        if (remote_hlc.empty())
            return Status::Invalid("change envelope missing _hlc");
        // Step the local clock forward so any subsequent local write
        // gets an HLC strictly greater than the remote one.
        try { hlc_.update(cluster::HybridLogicalClock::parse(remote_hlc)); }
        catch (...) { return Status::Invalid("change has malformed _hlc"); }

        std::string k = doc_key(db, sch, col, id);
        std::string existing;
        std::string local_hlc;
        if (store_->get(k, &existing)) {
            try {
                json ex = json::parse(existing);
                local_hlc = ex.value("_hlc", std::string());
            } catch (...) {}
        }
        // Strict-greater LWW. Equal HLCs cannot collide between distinct
        // nodes (the counter portion + node-relative tick guarantees
        // global uniqueness once we add a per-node nonce — for v1 we
        // resolve ties by lex order on the rendered string, which is
        // stable across nodes).
        if (!local_hlc.empty() && local_hlc >= remote_hlc) return Status::Ok();

        bool is_tombstone = change.value("_tombstone", false);
        if (is_tombstone) {
            // Materialise the tombstone so the change shows up in
            // subsequent pull-since responses to other peers. The
            // tombstone envelope keeps `_hlc` for ordering and a
            // `_deleted` marker for read-side filtering.
            json env = {
                {"_id",         id},
                {"_hlc",        remote_hlc},
                {"_tombstone",  true},
                {"_deleted",    true},
                {"updated_at",  now_ms()}
            };
            store_->put(k, env.dump());
            // Drop secondary index entries pointing at the now-deleted
            // doc so range/equality lookups don't surface ghosts.
            if (!local_hlc.empty()) {
                json prev_data;
                try { prev_data = json::parse(existing).value("data", json()); } catch (...) {}
                for (auto& idx : m.indexes) update_index(db, sch, col, idx, id, prev_data, json());
            }
            if (m.document_count > 0 && !local_hlc.empty()) {
                // We only decrement when overwriting a non-tombstone.
                bool was_live = false;
                try { was_live = !json::parse(existing).value("_tombstone", false); } catch (...) {}
                if (was_live) m.document_count--;
            }
            store_->put(meta_key(m.database, m.schema, m.name), m.to_json().dump());
            if (applied_out) *applied_out = true;
            return Status::Ok();
        }
        // Live document write. We accept whatever fields the remote
        // sent verbatim (no field-level merge). Tombstones overwrite a
        // live doc; live docs overwrite a tombstone.
        json env = change;  // already includes _id, _hlc
        if (!env.contains("data") && env.contains("doc")) env["data"] = env["doc"];
        if (!env.contains("data")) {
            // Allow envelopes that put the user payload at the top
            // level (everything except meta keys).
            json data = json::object();
            for (auto it = env.begin(); it != env.end(); ++it) {
                const std::string& k0 = it.key();
                if (k0 == "_id" || k0 == "_hlc" || k0 == "_tombstone" ||
                    k0 == "version" || k0 == "created_at" || k0 == "updated_at" ||
                    k0 == "ttl" || k0 == "data") continue;
                data[k0] = it.value();
            }
            json minimal = {
                {"_id", id}, {"_hlc", remote_hlc}, {"data", data},
                {"version", change.value("version", 1ull)},
                {"created_at", change.value("created_at", now_ms())},
                {"updated_at", change.value("updated_at", now_ms())},
                {"ttl", change.value("ttl", 0u)}
            };
            env = minimal;
        }
        std::string serialized = env.dump();
        if (serialized.size() > constants::COLLECTION_MAX_DOC_BYTES)
            return Status::Invalid("document exceeds maximum size");
        // Maintain secondary indexes with old-vs-new diff.
        json prev_data = json();
        bool prev_was_live = false;
        if (!local_hlc.empty()) {
            try {
                json prev_env = json::parse(existing);
                prev_was_live = !prev_env.value("_tombstone", false);
                prev_data = prev_env.value("data", json());
            } catch (...) {}
        }
        store_->put(k, serialized);
        for (auto& idx : m.indexes) update_index(db, sch, col, idx, id, prev_data, env["data"]);
        if (!prev_was_live) m.document_count++;
        store_->put(meta_key(m.database, m.schema, m.name), m.to_json().dump());
        if (applied_out) *applied_out = true;
        return Status::Ok();
    }

    // Return every change in (db, sch, col) with `_hlc` strictly greater
    // than `since_hlc`, bounded by `limit`. The result is sorted by HLC
    // ascending so a peer can resume by max(_hlc) of the previous batch.
    // Includes tombstones — they are part of the change set.
    struct ChangeBatch {
        std::vector<json> changes;     // each carries _id, _hlc, ...
        std::string       max_hlc;     // empty if no rows returned
    };
    ChangeBatch pull_changes_since(const std::string& db,
                                   const std::string& sch,
                                   const std::string& col,
                                   const std::string& since_hlc,
                                   size_t limit = 1024) {
        std::shared_lock<std::shared_mutex> lk(mu_);
        ChangeBatch out;
        CollectionMeta m;
        if (!get_collection_unlocked(db, sch, col, &m)) return out;
        if (!m.multi_master) return out;
        // Full keyspace scan over `doc:{db}:{sch}:{col}:` is acceptable
        // here because the puller is rate-limited (one peer-pull per
        // poll interval). Future optimisation: maintain a secondary
        // `hlcidx:` keyspace so this becomes a true O(batch) scan.
        std::string prefix = doc_prefix(db, sch, col);
        for (auto& [k, v] : store_->prefix_scan(prefix, 1000000)) {
            (void)k;
            try {
                json env = json::parse(v);
                std::string hlc_s = env.value("_hlc", std::string());
                if (hlc_s.empty()) continue;       // single-master leftover
                if (!since_hlc.empty() && hlc_s <= since_hlc) continue;
                out.changes.push_back(env);
            } catch (...) {}
        }
        std::sort(out.changes.begin(), out.changes.end(),
                  [](const json& a, const json& b) {
                      return a.value("_hlc", std::string())
                           < b.value("_hlc", std::string());
                  });
        if (out.changes.size() > limit) out.changes.resize(limit);
        if (!out.changes.empty())
            out.max_hlc = out.changes.back().value("_hlc", std::string());
        return out;
    }
    // -----------------------------------------------------------------

    // Open a new transaction. The returned Transaction buffers writes in
    // memory; nothing hits the store until commit() is called. See
    // transaction.hpp for the full semantics. The Transaction holds a raw
    // pointer back into this engine, so it MUST NOT outlive *this.
    Transaction begin_transaction();

    static std::string doc_key(const std::string& db, const std::string& sch, const std::string& col, const std::string& id) {
        return "doc:" + db + ":" + sch + ":" + col + ":" + id;
    }
    static std::string doc_prefix(const std::string& db, const std::string& sch, const std::string& col) {
        return "doc:" + db + ":" + sch + ":" + col + ":";
    }
    static std::string meta_key(const std::string& db, const std::string& sch, const std::string& col) {
        return "meta:collection:" + db + ":" + sch + ":" + col;
    }
    static std::string meta_prefix(const std::string& db = "") {
        return db.empty() ? "meta:collection:" : "meta:collection:" + db + ":";
    }
    static std::string idx_key(const std::string& db, const std::string& sch, const std::string& col,
                                const std::string& idx, const std::string& v, const std::string& id) {
        return "idx:" + db + ":" + sch + ":" + col + ":" + idx + ":" + v + ":" + id;
    }
    static std::string idx_prefix(const std::string& db, const std::string& sch, const std::string& col, const std::string& idx) {
        return "idx:" + db + ":" + sch + ":" + col + ":" + idx + ":";
    }

    // Collection meta
    Status create_collection(const CollectionMeta& m) {
        std::unique_lock<std::shared_mutex> lk(mu_);
        std::string k = meta_key(m.database, m.schema, m.name);
        std::string existing;
        if (store_->get(k, &existing)) return Status::Duplicate("collection exists");
        CollectionMeta meta = m;
        if (meta.created_at == 0) meta.created_at = now_sec();
        store_->put(k, meta.to_json().dump());
        return Status::Ok();
    }
    bool get_collection(const std::string& db, const std::string& sch, const std::string& name, CollectionMeta* out) {
        std::shared_lock<std::shared_mutex> lk(mu_);
        return get_collection_unlocked(db, sch, name, out);
    }
    Status update_collection(const CollectionMeta& m) {
        std::unique_lock<std::shared_mutex> lk(mu_);
        store_->put(meta_key(m.database, m.schema, m.name), m.to_json().dump());
        return Status::Ok();
    }
    Status drop_collection(const std::string& db, const std::string& sch, const std::string& col) {
        std::unique_lock<std::shared_mutex> lk(mu_);
        // delete all docs
        for (auto& [k, _] : store_->prefix_scan(doc_prefix(db, sch, col), 100000)) store_->del(k);
        // delete all index entries
        for (auto& [k, _] : store_->prefix_scan("idx:" + db + ":" + sch + ":" + col + ":", 1000000)) store_->del(k);
        store_->del(meta_key(db, sch, col));
        return Status::Ok();
    }
    std::vector<CollectionMeta> list_collections(const std::string& db = "") {
        std::shared_lock<std::shared_mutex> lk(mu_);
        std::vector<CollectionMeta> out;
        for (auto& [k, v] : store_->prefix_scan(meta_prefix(db), 10000)) {
            try { out.push_back(CollectionMeta::from_json(json::parse(v))); } catch(...) {}
        }
        return out;
    }

    // Document CRUD
    Status insert(const std::string& db, const std::string& sch, const std::string& col,
                  json& doc, std::string& id_out, uint32_t ttl = 0) {
        std::unique_lock<std::shared_mutex> lk(mu_);  // P0-5
        CollectionMeta m;
        if (!get_collection_unlocked(db, sch, col, &m)) return Status::NotFound("collection not found");
        if (!doc.is_object()) return Status::Invalid("document must be object");
        std::string id = doc.value("_id", std::string());
        if (id.empty()) { id = gen_id(); doc["_id"] = id; }
        std::string k = doc_key(db, sch, col, id);
        std::string existing;
        if (store_->get(k, &existing)) {
            // For multi-master collections, an existing tombstone is the
            // only legitimate way an insert can land on top of a
            // populated key (delete-then-insert during sync convergence
            // or reuse of an id). Reject anything else as Duplicate.
            bool was_tombstone = false;
            if (m.multi_master) {
                try { was_tombstone = json::parse(existing).value("_tombstone", false); }
                catch (...) {}
            }
            if (!was_tombstone) return Status::Duplicate("document exists");
        }
        // Reject oversize docs before they reach the WAL framing layer.
        std::string serialized;
        // unique-index check uses the live document; serialize once after.
        // Check unique indexes (now race-free under unique_lock).
        for (auto& idx : m.indexes) {
            if (idx.unique) {
                std::string v = index_value(doc, idx);
                if (!v.empty() || !idx.sparse) {
                    auto found = store_->prefix_scan(idx_prefix(db, sch, col, idx.name) + v + ":", 1);
                    if (!found.empty()) return Status::Duplicate("unique index violation: " + idx.name);
                }
            }
        }
        json envelope = {{"_id", id},{"data", doc},{"version", 1},{"created_at", now_ms()},{"updated_at", now_ms()},{"ttl", ttl}};
        // B.2: stamp every multi-master write with an HLC so the
        // bidirectional puller (and any peer it syncs with) can resolve
        // last-writer-wins correctly across nodes.
        if (m.multi_master) envelope["_hlc"] = hlc_.now_string();
        serialized = envelope.dump();
        if (serialized.size() > constants::COLLECTION_MAX_DOC_BYTES)
            return Status::Invalid("document exceeds maximum size");
        // For multi-master collections, an existing tombstone for this id
        // means a previous delete on this node — overwrite is fine, but
        // we must not double-count document_count.
        bool overwrites_tombstone = false;
        if (m.multi_master) {
            std::string prev;
            if (store_->get(k, &prev)) {
                try { overwrites_tombstone = json::parse(prev).value("_tombstone", false); }
                catch (...) {}
            }
        }
        store_->put(k, serialized);
        // index insertions
        for (auto& idx : m.indexes) update_index(db, sch, col, idx, id, json(), doc);
        if (!overwrites_tombstone) m.document_count++;
        // update_collection() takes the lock recursively otherwise; inline it.
        store_->put(meta_key(m.database, m.schema, m.name), m.to_json().dump());
        id_out = id;
        return Status::Ok();
    }

    bool get(const std::string& db, const std::string& sch, const std::string& col, const std::string& id, json* out) {
        std::shared_lock<std::shared_mutex> lk(mu_);  // P0-5
        std::string s;
        if (!store_->get(doc_key(db, sch, col, id), &s)) return false;
        json env;
        try { env = json::parse(s); } catch (...) { return false; }
        // B.2: tombstones are physically present so the puller can ship
        // them, but reads must surface them as "not found".
        if (env.value("_tombstone", false)) return false;
        // ttl check
        uint32_t ttl = env.value("ttl", 0u);
        uint64_t created = env.value("created_at", 0ull);
        if (ttl > 0 && now_ms() > created + ttl * 1000ull) return false;
        *out = env;
        return true;
    }

    // update() with optional optimistic-lock (P1-6). `expected_version`:
    //   0  -> no check (legacy last-write-wins; preserved for backwards compat)
    //   N  -> only succeed if the document's current version == N; otherwise
    //         return Status::Conflict("version mismatch"). Caller should
    //         re-read, merge, and retry.
    //
    // Because update() already holds a unique_lock on the CollectionEngine's
    // mu_, the read-check-write is atomic against other collection writes,
    // so the version check can't race with another update landing between
    // our load and store. What it *does* protect against is the classic
    // "lost update": two clients both read version=5, client A writes
    // version=6, client B's stale update is rejected instead of silently
    // clobbering A.
    Status update(const std::string& db, const std::string& sch, const std::string& col,
                  const std::string& id, const json& update_ops, json* updated_out = nullptr,
                  uint64_t expected_version = 0) {
        std::unique_lock<std::shared_mutex> lk(mu_);  // P0-5
        CollectionMeta m;
        if (!get_collection_unlocked(db, sch, col, &m)) return Status::NotFound("collection not found");
        json env;
        // get_unlocked: don't recursively lock under unique_lock.
        std::string s;
        if (!store_->get(doc_key(db, sch, col, id), &s)) return Status::NotFound("document not found");
        try { env = json::parse(s); } catch (...) { return Status::Invalid("corrupt document"); }
        // B.2: a tombstone reads as "deleted" — refuse the update so the
        // caller can re-create via insert if they really meant to.
        if (env.value("_tombstone", false)) return Status::NotFound("document not found");
        {
            uint32_t ttl = env.value("ttl", 0u);
            uint64_t created = env.value("created_at", 0ull);
            if (ttl > 0 && now_ms() > created + ttl * 1000ull) return Status::NotFound("document expired");
        }
        if (expected_version != 0) {
            uint64_t current = env.value("version", 1ull);
            if (current != expected_version) {
                return Status::Conflict("version mismatch: expected " +
                    std::to_string(expected_version) + ", found " +
                    std::to_string(current));
            }
        }
        json old = env["data"];
        json newd = UpdateApplier::apply(old, update_ops);
        newd["_id"] = id;
        // unique index check
        for (auto& idx : m.indexes) {
            if (idx.unique) {
                std::string nv = index_value(newd, idx);
                std::string ov = index_value(old, idx);
                if (nv != ov && (!nv.empty() || !idx.sparse)) {
                    auto found = store_->prefix_scan(idx_prefix(db, sch, col, idx.name) + nv + ":", 1);
                    if (!found.empty()) return Status::Duplicate("unique index violation: " + idx.name);
                }
            }
        }
        env["data"] = newd;
        env["version"] = env.value("version", 1ull) + 1;
        env["updated_at"] = now_ms();
        if (m.multi_master) env["_hlc"] = hlc_.now_string();
        std::string serialized = env.dump();
        if (serialized.size() > constants::COLLECTION_MAX_DOC_BYTES)
            return Status::Invalid("document exceeds maximum size");
        store_->put(doc_key(db, sch, col, id), serialized);
        for (auto& idx : m.indexes) update_index(db, sch, col, idx, id, old, newd);
        if (updated_out) *updated_out = env;
        return Status::Ok();
    }

    Status remove(const std::string& db, const std::string& sch, const std::string& col, const std::string& id) {
        std::unique_lock<std::shared_mutex> lk(mu_);  // P0-5
        CollectionMeta m;
        if (!get_collection_unlocked(db, sch, col, &m)) return Status::NotFound("collection not found");
        std::string s;
        if (!store_->get(doc_key(db, sch, col, id), &s)) return Status::NotFound("document not found");
        json env;
        try { env = json::parse(s); } catch (...) { return Status::Invalid("corrupt document"); }
        // Already-tombstoned: idempotent success on multi-master, NotFound otherwise.
        if (env.value("_tombstone", false)) {
            return m.multi_master ? Status::Ok() : Status::NotFound("document not found");
        }
        if (m.multi_master) {
            // Replace the live envelope with a tombstone the puller will
            // ship to peers. We keep `_hlc` so LWW resolves correctly.
            json tomb = {
                {"_id", id},
                {"_hlc", hlc_.now_string()},
                {"_tombstone", true},
                {"_deleted", true},
                {"updated_at", now_ms()}
            };
            store_->put(doc_key(db, sch, col, id), tomb.dump());
        } else {
            store_->del(doc_key(db, sch, col, id));
        }
        for (auto& idx : m.indexes) update_index(db, sch, col, idx, id, env["data"], json());
        if (m.document_count > 0) m.document_count--;
        store_->put(meta_key(m.database, m.schema, m.name), m.to_json().dump());
        return Status::Ok();
    }

    // Find with filter, sort, skip, limit, projection
    std::vector<json> find(const std::string& db, const std::string& sch, const std::string& col,
                           const json& filter, const json& sort_obj, size_t skip, size_t limit,
                           const json& projection, size_t* total = nullptr) {
        std::shared_lock<std::shared_mutex> lk(mu_);  // P0-5
        std::vector<json> matched;
        CollectionMeta cm;
        bool have_meta = get_collection_unlocked(db, sch, col, &cm);
        // P0-4: try an index-driven plan first. plan_query() returns a
        // bounded candidate set; we then re-apply the full filter so any
        // additional clauses (range / regex / $or) are still honoured.
        std::vector<std::string> candidate_ids;
        bool used_index = have_meta && plan_query(cm, filter, candidate_ids);
        if (used_index) {
            for (auto& id : candidate_ids) {
                std::string s;
                if (!store_->get(doc_key(db, sch, col, id), &s)) continue;
                try {
                    json env = json::parse(s);
                    if (env.value("_tombstone", false)) continue;  // B.2
                    uint32_t ttl = env.value("ttl", 0u);
                    uint64_t created = env.value("created_at", 0ull);
                    if (ttl > 0 && now_ms() > created + ttl * 1000ull) continue;
                    if (FilterMatcher::match(env["data"], filter)) matched.push_back(env);
                } catch (...) {}
            }
        } else {
        for (auto& [k, v] : store_->prefix_scan(doc_prefix(db, sch, col), 1000000)) {
            try {
                json env = json::parse(v);
                if (env.value("_tombstone", false)) continue;  // B.2
                uint32_t ttl = env.value("ttl", 0u);
                uint64_t created = env.value("created_at", 0ull);
                if (ttl > 0 && now_ms() > created + ttl * 1000ull) continue;
                if (FilterMatcher::match(env["data"], filter)) matched.push_back(env);
            } catch(...) {}
        }
        }
        if (total) *total = matched.size();
        // sort
        if (sort_obj.is_object() && !sort_obj.empty()) {
            std::vector<std::pair<std::string, int>> rules;
            for (auto& [k, v] : sort_obj.items()) rules.push_back({k, v.get<int>()});
            std::sort(matched.begin(), matched.end(), [&](const json& a, const json& b) {
                for (auto& [f, dir] : rules) {
                    json va = FilterMatcher::get_field(a["data"], f);
                    json vb = FilterMatcher::get_field(b["data"], f);
                    if (va == vb) continue;
                    return dir > 0 ? va < vb : va > vb;
                }
                return false;
            });
        }
        if (skip > matched.size()) return {};
        size_t end = std::min(matched.size(), skip + limit);
        std::vector<json> result(matched.begin() + skip, matched.begin() + end);
        // projection
        if (projection.is_object() && !projection.empty()) {
            for (auto& env : result) {
                json& d = env["data"];
                json out = json::object();
                bool include_mode = false;
                for (auto& [k, v] : projection.items()) if (v.is_number() && v.get<int>() == 1) { include_mode = true; break; }
                if (include_mode) {
                    out["_id"] = d.value("_id", std::string());
                    for (auto& [k, v] : projection.items()) if (v.get<int>() == 1) {
                        json fv = FilterMatcher::get_field(d, k);
                        if (!fv.is_null()) UpdateApplier::set_field(out, k, fv);
                    }
                    d = out;
                } else {
                    for (auto& [k, v] : projection.items()) if (v.get<int>() == 0) UpdateApplier::unset_field(d, k);
                }
            }
        }
        return result;
    }

    size_t count(const std::string& db, const std::string& sch, const std::string& col, const json& filter) {
        std::shared_lock<std::shared_mutex> lk(mu_);  // P0-5
        CollectionMeta cm;
        bool have_meta = get_collection_unlocked(db, sch, col, &cm);
        std::vector<std::string> candidate_ids;
        if (have_meta && plan_query(cm, filter, candidate_ids)) {
            size_t c = 0;
            for (auto& id : candidate_ids) {
                std::string s;
                if (!store_->get(doc_key(db, sch, col, id), &s)) continue;
                try {
                    json env = json::parse(s);
                    if (env.value("_tombstone", false)) continue;  // B.2
                    if (FilterMatcher::match(env["data"], filter)) c++;
                } catch (...) {}
            }
            return c;
        }
        size_t c = 0;
        for (auto& [k, v] : store_->prefix_scan(doc_prefix(db, sch, col), 1000000)) {
            try {
                json env = json::parse(v);
                if (env.value("_tombstone", false)) continue;  // B.2
                if (FilterMatcher::match(env["data"], filter)) c++;
            } catch(...) {}
        }
        return c;
    }

    // Live diagnostic for tests / EXPLAIN-style debugging. Returns true if a
    // query plan over the current indexes would short-circuit the full scan.
    bool query_uses_index(const std::string& db, const std::string& sch,
                          const std::string& col, const json& filter) {
        std::shared_lock<std::shared_mutex> lk(mu_);
        CollectionMeta cm;
        if (!get_collection_unlocked(db, sch, col, &cm)) return false;
        std::vector<std::string> ids;
        return plan_query(cm, filter, ids);
    }

    // Aggregation pipeline. Stages: $match, $group, $sort, $limit, $skip,
    // $project, $unwind, $lookup. See implementation below for per-stage
    // semantics.
    json aggregate(const std::string& db, const std::string& sch, const std::string& col, const json& pipeline) {
        std::shared_lock<std::shared_mutex> lk(mu_);  // P0-5
        std::vector<json> docs;
        for (auto& [k, v] : store_->prefix_scan(doc_prefix(db, sch, col), 1000000)) {
            try { docs.push_back(json::parse(v)["data"]); } catch(...) {}
        }
        for (auto& stage : pipeline) {
            if (stage.contains("$match")) {
                std::vector<json> out; for (auto& d : docs) if (FilterMatcher::match(d, stage["$match"])) out.push_back(d); docs = out;
            } else if (stage.contains("$group")) {
                json g = stage["$group"];
                std::string id_expr;
                if (g["_id"].is_string()) {
                    id_expr = g["_id"].get<std::string>();
                    if (id_expr.size() && id_expr[0] == '$') id_expr = id_expr.substr(1);
                }
                std::map<json, json> groups;
                for (auto& d : docs) {
                    json key = id_expr.empty() ? json(nullptr) : FilterMatcher::get_field(d, id_expr);
                    if (!groups.count(key)) { groups[key] = json::object(); groups[key]["_id"] = key; }
                    for (auto& [field, expr] : g.items()) {
                        if (field == "_id") continue;
                        if (!expr.is_object()) continue;
                        for (auto& [op, val] : expr.items()) {
                            std::string vfield = val.is_string() && !val.get<std::string>().empty() && val.get<std::string>()[0]=='$' ? val.get<std::string>().substr(1) : "";
                            json fv = vfield.empty() ? val : FilterMatcher::get_field(d, vfield);
                            if (op == "$sum") groups[key][field] = groups[key].value(field, 0.0) + (fv.is_number() ? fv.get<double>() : 1.0);
                            else if (op == "$avg") {
                                double s = groups[key].value(field, 0.0);
                                int cnt = groups[key].value("_count_"+field, 0) + 1;
                                if (fv.is_number()) s += fv.get<double>();
                                groups[key][field] = s;
                                groups[key]["_count_"+field] = cnt;
                            }
                            else if (op == "$min") { if (!groups[key].contains(field) || (fv.is_number() && fv < groups[key][field])) groups[key][field] = fv; }
                            else if (op == "$max") { if (!groups[key].contains(field) || (fv.is_number() && fv > groups[key][field])) groups[key][field] = fv; }
                            else if (op == "$count") groups[key][field] = groups[key].value(field, 0) + 1;
                        }
                    }
                }
                docs.clear();
                for (auto& [k, v] : groups) {
                    json out = v;
                    // resolve avg
                    for (auto it = out.begin(); it != out.end();) {
                        if (it.key().rfind("_count_", 0) == 0) {
                            std::string f = it.key().substr(7);
                            if (out.contains(f) && out[f].is_number()) out[f] = out[f].get<double>() / it.value().get<int>();
                            it = out.erase(it);
                        } else ++it;
                    }
                    docs.push_back(out);
                }
            } else if (stage.contains("$sort")) {
                json s = stage["$sort"];
                std::vector<std::pair<std::string,int>> rules; for (auto& [k,v]:s.items()) rules.push_back({k,v.get<int>()});
                std::sort(docs.begin(), docs.end(), [&](const json& a, const json& b){
                    for (auto& [f, d] : rules) {
                        json va = FilterMatcher::get_field(a, f);
                        json vb = FilterMatcher::get_field(b, f);
                        if (va == vb) continue;
                        return d > 0 ? va < vb : va > vb;
                    }
                    return false;
                });
            } else if (stage.contains("$limit")) {
                size_t n = stage["$limit"].get<size_t>();
                if (docs.size() > n) docs.resize(n);
            } else if (stage.contains("$skip")) {
                size_t n = stage["$skip"].get<size_t>();
                if (docs.size() <= n) docs.clear(); else docs.erase(docs.begin(), docs.begin()+n);
            } else if (stage.contains("$project")) {
                // P1-9: $project. Mongo-style include / exclude rules.
                // {"a": 1, "b.c": 1}   -> include only these (plus _id)
                // {"a": 0, "b": 0}     -> exclude these
                // Renames: {"new": "$old"} copies old → new (include mode).
                json proj = stage["$project"];
                bool include_mode = false;
                std::unordered_map<std::string,std::string> renames;
                for (auto& [k, v] : proj.items()) {
                    if (v.is_number() && v.get<int>() == 1) include_mode = true;
                    else if (v.is_string()) {
                        std::string s = v.get<std::string>();
                        if (!s.empty() && s[0] == '$') {
                            renames[k] = s.substr(1);
                            include_mode = true;
                        }
                    }
                }
                std::vector<json> out; out.reserve(docs.size());
                for (auto& d : docs) {
                    json o = json::object();
                    if (include_mode) {
                        if (d.is_object() && d.contains("_id"))
                            o["_id"] = d["_id"];
                        for (auto& [k, v] : proj.items()) {
                            if (v.is_number() && v.get<int>() == 1) {
                                json fv = FilterMatcher::get_field(d, k);
                                if (!fv.is_null()) UpdateApplier::set_field(o, k, fv);
                            }
                        }
                        for (auto& [newk, oldk] : renames) {
                            json fv = FilterMatcher::get_field(d, oldk);
                            if (!fv.is_null()) UpdateApplier::set_field(o, newk, fv);
                        }
                    } else {
                        o = d;
                        for (auto& [k, v] : proj.items())
                            if (v.is_number() && v.get<int>() == 0)
                                UpdateApplier::unset_field(o, k);
                    }
                    out.push_back(std::move(o));
                }
                docs = std::move(out);
            } else if (stage.contains("$unwind")) {
                // P1-9: $unwind. Spec is either "$field" (string) or
                // {path: "$field", preserveNullAndEmptyArrays: bool}.
                std::string path;
                bool preserve = false;
                const json& spec = stage["$unwind"];
                if (spec.is_string()) {
                    path = spec.get<std::string>();
                } else if (spec.is_object()) {
                    path = spec.value("path", std::string());
                    preserve = spec.value("preserveNullAndEmptyArrays", false);
                }
                if (!path.empty() && path[0] == '$') path = path.substr(1);
                std::vector<json> out;
                for (auto& d : docs) {
                    json fv = FilterMatcher::get_field(d, path);
                    if (fv.is_array()) {
                        if (fv.empty() && preserve) { out.push_back(d); continue; }
                        for (auto& el : fv) {
                            json clone = d;
                            UpdateApplier::set_field(clone, path, el);
                            out.push_back(std::move(clone));
                        }
                    } else if (!fv.is_null()) {
                        // Non-array: pass through (matches Mongo behaviour).
                        out.push_back(d);
                    } else if (preserve) {
                        out.push_back(d);
                    }
                }
                docs = std::move(out);
            } else if (stage.contains("$lookup")) {
                // Equi-join from `from` collection, MongoDB-compatible.
                //   {"$lookup": {
                //       "from":         "<collection>",
                //       "localField":   "<path in current doc>",
                //       "foreignField": "<path in `from` doc>",
                //       "as":           "<output array field>"
                //   }}
                //
                // Semantics:
                //   * `from` is resolved in the same db+schema as the current
                //     pipeline's collection. Cross-db / cross-schema joins are
                //     out of scope for v1.
                //   * If the local value is an array, every element is matched
                //     against `foreignField` (Mongo behaviour).
                //   * `as` always becomes an array, even on a single match
                //     (Mongo behaviour). Empty array means no match.
                //   * The `from` collection is loaded once per stage and
                //     hash-indexed by `foreignField`, so the cost is
                //     O(|from| + |docs| * avg_matches), not the naive
                //     O(|from| * |docs|).
                //
                // Locking: we already hold a shared lock on `mu_` for the
                //   pipeline; the prefix_scan below is read-only and reuses
                //   it, so no extra synchronisation is needed.
                const json& spec = stage["$lookup"];
                std::string from   = spec.value("from",         std::string());
                std::string lfield = spec.value("localField",   std::string());
                std::string ffield = spec.value("foreignField", std::string());
                std::string asfld  = spec.value("as",           std::string());
                if (from.empty() || lfield.empty() || ffield.empty() || asfld.empty()) {
                    // Bad spec: pass docs through unchanged rather than throw,
                    // matching the engine's tolerant style for malformed stages.
                    continue;
                }
                // Build hash index over the `from` collection on `foreignField`.
                // We use json::dump() as the hash key to handle string / number /
                // bool / null uniformly without writing a json hasher.
                std::unordered_map<std::string, std::vector<json>> idx;
                for (auto& [k, v] : store_->prefix_scan(doc_prefix(db, sch, from), 1000000)) {
                    json fdoc;
                    try { fdoc = json::parse(v)["data"]; } catch(...) { continue; }
                    json fv = FilterMatcher::get_field(fdoc, ffield);
                    if (fv.is_array()) {
                        for (auto& el : fv) idx[el.dump()].push_back(fdoc);
                    } else if (!fv.is_null()) {
                        idx[fv.dump()].push_back(fdoc);
                    }
                }
                // Probe each input doc against the hash index.
                for (auto& d : docs) {
                    json lv = FilterMatcher::get_field(d, lfield);
                    json matches = json::array();
                    auto probe = [&](const json& key) {
                        auto it = idx.find(key.dump());
                        if (it != idx.end()) {
                            for (auto& m : it->second) matches.push_back(m);
                        }
                    };
                    if (lv.is_array()) {
                        for (auto& el : lv) probe(el);
                    } else if (!lv.is_null()) {
                        probe(lv);
                    }
                    UpdateApplier::set_field(d, asfld, matches);
                }
            }
        }
        return docs;
    }

    Status create_index(const std::string& db, const std::string& sch, const std::string& col, const IndexDef& idx) {
        std::unique_lock<std::shared_mutex> lk(mu_);  // P0-5
        CollectionMeta m;
        if (!get_collection_unlocked(db, sch, col, &m)) return Status::NotFound("collection not found");
        for (auto& e : m.indexes) if (e.name == idx.name) return Status::Duplicate("index exists");
        m.indexes.push_back(idx);
        store_->put(meta_key(m.database, m.schema, m.name), m.to_json().dump());
        // build index over existing docs
        for (auto& [k, v] : store_->prefix_scan(doc_prefix(db, sch, col), 1000000)) {
            try {
                json env = json::parse(v);
                update_index(db, sch, col, idx, env.value("_id", std::string()), json(), env["data"]);
            } catch(...) {}
        }
        return Status::Ok();
    }
    Status drop_index(const std::string& db, const std::string& sch, const std::string& col, const std::string& name) {
        std::unique_lock<std::shared_mutex> lk(mu_);  // P0-5
        CollectionMeta m;
        if (!get_collection_unlocked(db, sch, col, &m)) return Status::NotFound("collection not found");
        auto it = std::remove_if(m.indexes.begin(), m.indexes.end(), [&](const IndexDef& i){return i.name == name;});
        if (it == m.indexes.end()) return Status::NotFound("index not found");
        m.indexes.erase(it, m.indexes.end());
        store_->put(meta_key(m.database, m.schema, m.name), m.to_json().dump());
        for (auto& [k, _] : store_->prefix_scan(idx_prefix(db, sch, col, name), 1000000)) store_->del(k);
        return Status::Ok();
    }

private:
    // Internal helper used by methods that already hold mu_ to avoid
    // recursive locking. shared_mutex is NOT recursive in the C++ standard.
    bool get_collection_unlocked(const std::string& db, const std::string& sch,
                                 const std::string& name, CollectionMeta* out) {
        std::string s;
        if (!store_->get(meta_key(db, sch, name), &s)) return false;
        try { *out = CollectionMeta::from_json(json::parse(s)); }
        catch (...) { return false; }
        return true;
    }

    std::string index_value(const json& doc, const IndexDef& idx) {
        std::string out;
        for (auto& f : idx.fields) {
            json v = FilterMatcher::get_field(doc, f);
            out += v.is_null() ? "" : (v.is_string() ? v.get<std::string>() : v.dump());
            out += "\x1F";
        }
        return out;
    }
    // P0-4 query planner ---------------------------------------------------
    //
    // Given a (potentially nested) filter and the collection's indexes,
    // decide whether we can produce a tight candidate set by scanning the
    // `idx:` keyspace instead of the full `doc:` keyspace. We support:
    //
    //   * `field: scalar`
    //   * `field: {$eq: scalar}`
    //   * `field: {$in: [v1, v2, ...]}`
    //   * `$and: [<any of the above>...]`  (intersection of candidates)
    //   * Composite index prefix equality + range on the next field:
    //     e.g. index on [a, b] with filter {a: 1, b: {$gt: 5}}
    //     uses the index to scan the range of b values under a=1.
    //
    // Any other operator ($or, $regex, ...) on a non-indexed field makes
    // the planner fall through to the full scan path. We always re-apply
    // the full filter on top of the candidate set so correctness is
    // identical either way.
    bool plan_query(const CollectionMeta& cm, const json& filter,
                    std::vector<std::string>& out_ids) {
        if (!filter.is_object() || filter.empty()) return false;

        // Try composite index plan first (prefix equality + optional range).
        if (plan_composite(cm, filter, out_ids)) return true;

        // Fall back to single-field equality plan.
        // Build a map: field-name → index that indexes ONLY that field.
        std::unordered_map<std::string, const IndexDef*> by_field;
        for (auto& idx : cm.indexes) {
            if (idx.fields.size() == 1) by_field[idx.fields[0]] = &idx;
        }
        if (by_field.empty()) return false;

        // Collect (field, [equality-values]) clauses from the filter.
        std::vector<std::pair<std::string, std::vector<json>>> clauses;
        if (!collect_eq_clauses(filter, clauses)) return false;
        if (clauses.empty()) return false;

        // For every clause that points at an indexed field, fetch its
        // candidate doc-ids. Intersect across clauses (AND semantics).
        bool first = true;
        std::set<std::string> work;
        for (auto& [field, vals] : clauses) {
            auto it = by_field.find(field);
            if (it == by_field.end()) continue;          // skip un-indexed
            std::set<std::string> got;
            for (auto& v : vals) {
                std::string encoded = encode_index_value(v);
                auto rng = store_->prefix_scan(
                    idx_prefix(cm.database, cm.schema, cm.name, it->second->name)
                        + encoded + ":",
                    1'000'000);
                for (auto& [k, idv] : rng) got.insert(idv);
            }
            if (first) { work = std::move(got); first = false; }
            else {
                std::set<std::string> inter;
                std::set_intersection(work.begin(), work.end(),
                                      got.begin(), got.end(),
                                      std::inserter(inter, inter.begin()));
                work = std::move(inter);
            }
            if (work.empty()) break;                      // short-circuit
        }
        if (first) return false;                          // no clause matched an index
        out_ids.assign(work.begin(), work.end());
        return true;
    }

    // -----------------------------------------------------------------------
    // Composite index planner.
    //
    // For a multi-field index [f0, f1, ..., fN], we can use it if the filter
    // provides equality on a leading prefix of fields (f0, f1, ..., fK-1).
    //
    // The approach:
    //   1. Build the index key prefix from the equality values.
    //   2. prefix_scan the equality portion of the index.
    //   3. The full filter is always re-applied on the candidate set, so
    //      range conditions on subsequent fields are handled correctly.
    //
    // This gives us efficient lookups like:
    //   index: [category, price]
    //   filter: {category: "electronics", price: {$gte: 100, $lte: 500}}
    //   → prefix scan on category="electronics", then filter price in-memory.
    //
    // The key insight: even without order-preserving numeric encoding, the
    // equality prefix alone dramatically narrows the candidate set. The
    // post-filter ensures correctness for range conditions.
    // -----------------------------------------------------------------------
    bool plan_composite(const CollectionMeta& cm, const json& filter,
                        std::vector<std::string>& out_ids) {
        if (!filter.is_object()) return false;

        // Flatten the filter into per-field clause maps.
        std::unordered_map<std::string, json> field_clauses;
        if (!flatten_filter(filter, field_clauses)) return false;

        // Try each multi-field index and pick the one with the longest
        // usable equality prefix.
        const IndexDef* best_idx = nullptr;
        size_t best_eq_len = 0;
        bool best_has_range = false;

        for (auto& idx : cm.indexes) {
            if (idx.fields.size() < 2) continue;  // single-field handled elsewhere
            size_t eq_len = 0;
            bool has_range = false;

            for (size_t i = 0; i < idx.fields.size(); ++i) {
                const std::string& f = idx.fields[i];
                auto it = field_clauses.find(f);
                if (it == field_clauses.end()) break;

                if (is_equality_clause(it->second)) {
                    eq_len++;
                } else if (is_range_clause(it->second)) {
                    has_range = true;
                    break;  // range terminates the usable prefix
                } else {
                    break;  // unsupported operator
                }
            }

            // Must have at least one equality field to be useful.
            if (eq_len == 0) continue;

            // Prefer longer equality prefix, then prefer having a range
            // (indicates the index is more relevant to the query).
            if (eq_len > best_eq_len ||
                (eq_len == best_eq_len && has_range && !best_has_range)) {
                best_idx = &idx;
                best_eq_len = eq_len;
                best_has_range = has_range;
            }
        }

        if (!best_idx) return false;

        // Build the index key prefix from equality values.
        std::string prefix = idx_prefix(cm.database, cm.schema, cm.name, best_idx->name);
        for (size_t i = 0; i < best_eq_len; ++i) {
            const std::string& f = best_idx->fields[i];
            json val = get_equality_value(field_clauses[f]);
            prefix += encode_index_value(val);
        }

        // Prefix scan: returns all docs matching the equality prefix.
        // The full filter (including any range conditions) is re-applied
        // by the caller, ensuring correctness.
        std::set<std::string> got;
        auto rng = store_->prefix_scan(prefix, 1'000'000);
        for (auto& [k, idv] : rng) got.insert(idv);

        if (got.empty()) return false;
        out_ids.assign(got.begin(), got.end());
        return true;
    }

    // Flatten a filter object into a map of field → clause value.
    // Handles top-level fields and $and arrays. Returns false if the filter
    // contains structures we can't reason about for composite planning.
    bool flatten_filter(const json& f,
                        std::unordered_map<std::string, json>& out) {
        if (!f.is_object()) return false;
        for (auto it = f.begin(); it != f.end(); ++it) {
            const std::string& k = it.key();
            const json& v = it.value();
            if (k == "$and") {
                if (!v.is_array()) return false;
                for (auto& c : v) {
                    if (!flatten_filter(c, out)) return false;
                }
            } else if (k == "$or" || k == "$nor" || k == "$not") {
                return false;
            } else if (k.size() && k[0] == '$') {
                return false;
            } else {
                // If the same field appears multiple times (e.g. in separate
                // $and clauses), merge them into a single object.
                if (out.count(k) && out[k].is_object() && v.is_object()) {
                    for (auto vit = v.begin(); vit != v.end(); ++vit) {
                        out[k][vit.key()] = vit.value();
                    }
                } else {
                    out[k] = v;
                }
            }
        }
        return true;
    }

    // Check if a clause value represents an equality condition.
    static bool is_equality_clause(const json& v) {
        if (!v.is_object()) return true;  // scalar shorthand
        if (v.contains("$eq")) return true;
        if (v.contains("$in")) return true;
        return false;
    }

    // Check if a clause value represents a range condition.
    static bool is_range_clause(const json& v) {
        if (!v.is_object()) return false;
        return v.contains("$gt") || v.contains("$gte") ||
               v.contains("$lt") || v.contains("$lte");
    }

    // Extract the equality value from a clause (scalar or {$eq: X}).
    static json get_equality_value(const json& v) {
        if (!v.is_object()) return v;
        if (v.contains("$eq")) return v["$eq"];
        // For $in, take the first value (this is a heuristic; the full
        // filter re-check ensures correctness).
        if (v.contains("$in") && v["$in"].is_array() && !v["$in"].empty()) {
            return v["$in"][0];
        }
        return json();
    }

    // Extract range bounds from a clause object (reserved for future
    // order-preserving encoding that would enable true range scans).
    static void extract_range_bounds(const json& v, json& lo, json& hi,
                                     bool& lo_inclusive, bool& hi_inclusive) {
        lo = json();
        hi = json();
        lo_inclusive = true;
        hi_inclusive = true;
        if (!v.is_object()) return;
        if (v.contains("$gte")) { lo = v["$gte"]; lo_inclusive = true; }
        else if (v.contains("$gt")) { lo = v["$gt"]; lo_inclusive = false; }
        if (v.contains("$lte")) { hi = v["$lte"]; hi_inclusive = true; }
        else if (v.contains("$lt")) { hi = v["$lt"]; hi_inclusive = false; }
    }

    // Walk `filter` and pull out `(field, [values])` equality clauses.
    // Returns false if we find a clause we can't safely reason about
    // (range, $or, $regex, etc.) — caller falls back to full scan.
    bool collect_eq_clauses(const json& f,
                            std::vector<std::pair<std::string, std::vector<json>>>& out) {
        if (!f.is_object()) return false;
        for (auto it = f.begin(); it != f.end(); ++it) {
            const std::string& k = it.key();
            const json& v = it.value();
            if (k == "$and") {
                if (!v.is_array()) return false;
                for (auto& c : v) if (!collect_eq_clauses(c, out)) return false;
            } else if (k == "$or" || k == "$nor" || k == "$not") {
                // We could plan $or as a union of candidate sets, but the
                // bookkeeping interacts with AND-intersection in subtle
                // ways; keep the planner conservative.
                return false;
            } else if (k.size() && k[0] == '$') {
                return false;                              // unknown top-level op
            } else if (v.is_object()) {
                // Only {$eq: X} and {$in: [...]} are index-usable.
                std::vector<json> vals;
                bool ok = false;
                for (auto vit = v.begin(); vit != v.end(); ++vit) {
                    if (vit.key() == "$eq") { vals.push_back(vit.value()); ok = true; }
                    else if (vit.key() == "$in" && vit.value().is_array()) {
                        for (auto& x : vit.value()) vals.push_back(x);
                        ok = true;
                    } else { ok = false; break; }
                }
                if (!ok) continue;                         // non-eq operator on this field
                out.emplace_back(k, std::move(vals));
            } else {
                // Scalar equality shorthand.
                out.emplace_back(k, std::vector<json>{v});
            }
        }
        return true;
    }

    // Mirror of index_value() for a single value (without the trailing
    // separator that comes from joining multiple fields).
    static std::string encode_index_value(const json& v) {
        std::string out = v.is_null() ? ""
                          : (v.is_string() ? v.get<std::string>() : v.dump());
        out += "\x1F";
        return out;
    }

    void update_index(const std::string& db, const std::string& sch, const std::string& col,
                       const IndexDef& idx, const std::string& id, const json& old_doc, const json& new_doc) {
        if (!old_doc.is_null()) {
            std::string ov = index_value(old_doc, idx);
            store_->del(idx_key(db, sch, col, idx.name, ov, id));
        }
        if (!new_doc.is_null() && new_doc.is_object()) {
            std::string nv = index_value(new_doc, idx);
            if (!nv.empty() || !idx.sparse) {
                store_->put(idx_key(db, sch, col, idx.name, nv, id), id);
            }
        }
    }
    storage::LSMTree* store_;
    // P0-5: protects all collection meta + document + index mutations.
    // Reads (find / count / aggregate / get / list_collections) take a
    // shared lock; writes take a unique lock.
    mutable std::shared_mutex mu_;
    // B.2: shared across all multi-master collections in this engine.
    // Plain wall-clock-based ticks would lose causality across nodes.
    cluster::HybridLogicalClock hlc_;

    // Friend so multi-document transactions can validate read-set versions
    // and apply the buffered ops while holding mu_ exclusively. See
    // transaction.cpp for the commit implementation.
    friend class Transaction;
};

} // namespace delta
