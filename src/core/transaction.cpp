// =============================================================================
// transaction.cpp — implementation of multi-document ACID transactions.
//
// See transaction.hpp for the full design notes. The short version:
//   * Buffer all mutations in memory.
//   * Track read versions for OCC validation.
//   * On commit, take engine.mu_ exclusively, re-validate everything,
//     then apply the entire write set in a single atomic logical step.
// =============================================================================
#include "transaction.hpp"
#include "collection.hpp"

namespace delta {

// -----------------------------------------------------------------------------
// CollectionEngine::begin_transaction
// -----------------------------------------------------------------------------
Transaction CollectionEngine::begin_transaction() {
    return Transaction(this);
}

// -----------------------------------------------------------------------------
// Transaction — buffered mutations
// -----------------------------------------------------------------------------

const Transaction::Op*
Transaction::last_op_for(const std::string& db, const std::string& sch,
                         const std::string& col, const std::string& id) const {
    // Walk the op log in reverse so the most recent matching op wins.
    for (auto it = ops_.rbegin(); it != ops_.rend(); ++it) {
        if (it->db == db && it->sch == sch && it->col == col && it->id == id) {
            return &*it;
        }
    }
    return nullptr;
}

json Transaction::apply_update_ops(const json& base, const json& ops) {
    return UpdateApplier::apply(base, ops);
}

Status Transaction::insert(const std::string& db, const std::string& sch,
                           const std::string& col, json doc,
                           std::string& id_out, uint32_t ttl) {
    if (committed_ || rolled_back_) {
        return Status::Invalid("transaction already terminated");
    }
    if (!doc.is_object()) {
        return Status::Invalid("document must be object");
    }
    std::string id = doc.value("_id", std::string());
    if (id.empty()) {
        id = gen_id();
        doc["_id"] = id;
    }
    // Reject if a previous op in this same tx already inserted this _id and
    // has not been removed since.
    if (auto* prev = last_op_for(db, sch, col, id)) {
        if (prev->kind == OpKind::INSERT) {
            return Status::Duplicate("document already inserted in this tx");
        }
        if (prev->kind == OpKind::UPDATE) {
            return Status::Duplicate("document already exists in this tx");
        }
        // If prev was REMOVE we accept — re-insert after delete is fine.
    }
    Op op;
    op.kind = OpKind::INSERT;
    op.db = db; op.sch = sch; op.col = col; op.id = id;
    op.payload = std::move(doc);
    op.ttl = ttl;
    ops_.push_back(std::move(op));
    id_out = id;
    return Status::Ok();
}

Status Transaction::update(const std::string& db, const std::string& sch,
                           const std::string& col, const std::string& id,
                           const json& update_ops, uint64_t expected_version) {
    if (committed_ || rolled_back_) {
        return Status::Invalid("transaction already terminated");
    }
    Op op;
    op.kind = OpKind::UPDATE;
    op.db = db; op.sch = sch; op.col = col; op.id = id;
    op.payload = update_ops;
    op.expected_version = expected_version;
    ops_.push_back(std::move(op));
    return Status::Ok();
}

Status Transaction::remove(const std::string& db, const std::string& sch,
                           const std::string& col, const std::string& id) {
    if (committed_ || rolled_back_) {
        return Status::Invalid("transaction already terminated");
    }
    Op op;
    op.kind = OpKind::REMOVE;
    op.db = db; op.sch = sch; op.col = col; op.id = id;
    ops_.push_back(std::move(op));
    return Status::Ok();
}

bool Transaction::get(const std::string& db, const std::string& sch,
                      const std::string& col, const std::string& id, json* out) {
    if (committed_ || rolled_back_) return false;

    // Read-your-writes: replay this tx's op log on top of the underlying
    // document so the caller sees the post-write view.
    json base;
    bool exists = engine_->get(db, sch, col, id, &base);
    uint64_t observed_version = exists ? base.value("version", 1ull) : 0ull;

    // Record the observed version on first read of this key (for OCC).
    std::string k = CollectionEngine::doc_key(db, sch, col, id);
    if (!read_set_.count(k)) read_set_[k] = observed_version;

    // Walk all ops in order, tracking the effective state for this id.
    json effective = exists ? base["data"] : json();
    bool effective_exists = exists;

    for (auto& op : ops_) {
        if (op.db != db || op.sch != sch || op.col != col || op.id != id) continue;
        switch (op.kind) {
            case OpKind::INSERT:
                effective = op.payload;
                effective_exists = true;
                break;
            case OpKind::UPDATE:
                if (effective_exists) {
                    effective = apply_update_ops(effective, op.payload);
                }
                break;
            case OpKind::REMOVE:
                effective = json();
                effective_exists = false;
                break;
        }
    }

    if (!effective_exists) return false;
    if (out) {
        // Match engine.get() shape: return the envelope with the data field.
        // We don't have a real version/created_at for the in-tx view; use
        // sentinels so callers can tell this is uncommitted state.
        *out = {
            {"_id", id},
            {"data", effective},
            {"version", 0},
            {"created_at", 0},
            {"updated_at", 0},
            {"ttl", 0},
            {"_in_transaction", true}
        };
    }
    return true;
}

void Transaction::rollback() {
    if (committed_) return;
    ops_.clear();
    read_set_.clear();
    rolled_back_ = true;
}

// -----------------------------------------------------------------------------
// Transaction::commit — the heart of the ACID guarantee.
//
// Steps:
//   1. Take engine.mu_ exclusively.
//   2. Re-read every key in read_set_ from the store and verify the version
//      matches what we observed. Any mismatch → Conflict.
//   3. Walk ops_ in order, building a "what each key will look like after
//      every op in this tx applies" map. For each terminal state, validate
//      uniqueness and existence constraints (against current store state
//      AND against the in-tx state, so two inserts of the same _id within
//      the tx fail at commit even if neither raced any other writer).
//   4. If all validation passes, apply all writes to the store. Index
//      maintenance mirrors the non-transactional engine path.
// -----------------------------------------------------------------------------
Status Transaction::commit() {
    if (committed_)   return Status::Invalid("transaction already committed");
    if (rolled_back_) return Status::Invalid("transaction already rolled back");

    if (ops_.empty() && read_set_.empty()) {
        committed_ = true;
        return Status::Ok();
    }

    std::unique_lock<std::shared_mutex> lk(engine_->mu_);

    // Step 1: Re-validate the read set (OCC).
    for (auto& [key, observed] : read_set_) {
        std::string s;
        bool exists = engine_->store_->get(key, &s);
        if (!exists) {
            if (observed != 0) {
                return Status::Conflict("read-set key " + key +
                    " was deleted by another transaction");
            }
            continue;
        }
        json env;
        try { env = json::parse(s); } catch (...) {
            return Status::Conflict("read-set key " + key + " corrupted");
        }
        uint64_t current = env.value("version", 1ull);
        if (current != observed) {
            return Status::Conflict("read-set version mismatch on " + key +
                " (observed=" + std::to_string(observed) +
                ", current=" + std::to_string(current) + ")");
        }
    }

    // Step 2: Build the validation context. For each touched (db,sch,col,id)
    // we track:
    //   - the original doc (envelope) before any tx op, if it existed
    //   - the running effective document (for sequential update semantics)
    //   - whether the final effective state is "exists" or "removed"
    struct DocState {
        json original_env;        // the full envelope at tx start, or null
        bool original_exists = false;
        json effective_data;      // post-tx data field
        bool effective_exists = false;
        uint64_t expected_version = 0;  // last UPDATE's expected_version, if any
        bool any_update_with_expected = false;
    };
    std::map<std::string, DocState> states;
    // Also load the relevant CollectionMeta once per (db,sch,col).
    std::map<std::string, CollectionMeta> metas;

    auto col_key = [](const std::string& db, const std::string& sch,
                      const std::string& col) {
        return db + "\x1F" + sch + "\x1F" + col;
    };

    for (auto& op : ops_) {
        std::string ck = col_key(op.db, op.sch, op.col);
        if (!metas.count(ck)) {
            CollectionMeta m;
            if (!engine_->get_collection_unlocked(op.db, op.sch, op.col, &m)) {
                return Status::NotFound("collection not found: " + op.col);
            }
            metas[ck] = std::move(m);
        }

        std::string dk = CollectionEngine::doc_key(op.db, op.sch, op.col, op.id);
        if (!states.count(dk)) {
            DocState st;
            std::string s;
            if (engine_->store_->get(dk, &s)) {
                try {
                    st.original_env = json::parse(s);
                    st.original_exists = true;
                    st.effective_data = st.original_env["data"];
                    st.effective_exists = true;
                } catch (...) {
                    return Status::Invalid("corrupt document at " + dk);
                }
            }
            states[dk] = std::move(st);
        }

        DocState& st = states[dk];
        switch (op.kind) {
            case OpKind::INSERT:
                if (st.effective_exists) {
                    return Status::Duplicate("document already exists: " + op.id);
                }
                st.effective_data = op.payload;
                st.effective_exists = true;
                break;
            case OpKind::UPDATE:
                if (!st.effective_exists) {
                    return Status::NotFound("update target missing: " + op.id);
                }
                if (op.expected_version != 0) {
                    // Check against the original version (the version the
                    // caller said they were working from). If the update
                    // fires after another update in the same tx, the
                    // expected_version should match the original since the
                    // caller hadn't seen the in-tx state. We accept here
                    // and rely on caller discipline; OCC on the read set
                    // is the cross-tx safety net.
                    uint64_t orig_v = st.original_exists
                        ? st.original_env.value("version", 1ull) : 0ull;
                    if (orig_v != op.expected_version) {
                        return Status::Conflict("expected_version mismatch on " +
                            op.id + ": expected=" +
                            std::to_string(op.expected_version) +
                            ", actual=" + std::to_string(orig_v));
                    }
                }
                st.effective_data = apply_update_ops(st.effective_data, op.payload);
                // _id is immutable: re-assert.
                st.effective_data["_id"] = op.id;
                break;
            case OpKind::REMOVE:
                if (!st.effective_exists) {
                    return Status::NotFound("remove target missing: " + op.id);
                }
                st.effective_data = json();
                st.effective_exists = false;
                break;
        }
    }

    // Step 3: Validate unique index constraints across all writes.
    // For each touched doc whose final state exists, walk every unique
    // index on its collection and ensure no other doc (outside this tx)
    // already has the same index value, and no two docs WITHIN this tx
    // collide on the same unique index.
    {
        // map: idx_value_key → first id we saw with that value (within tx)
        std::unordered_map<std::string, std::string> tx_unique_keys;
        for (auto& [dk, st] : states) {
            if (!st.effective_exists) continue;
            // Reverse-engineer (db,sch,col,id) from the doc key. Easier to
            // pass through from ops, but ops_ may have multiple entries
            // for the same dk — pull from the first one.
            const Op* sample = nullptr;
            for (auto& op : ops_) {
                if (CollectionEngine::doc_key(op.db, op.sch, op.col, op.id) == dk) {
                    sample = &op;
                    break;
                }
            }
            if (!sample) continue;
            const CollectionMeta& m = metas[col_key(sample->db, sample->sch, sample->col)];
            for (auto& idx : m.indexes) {
                if (!idx.unique) continue;
                std::string nv = engine_->index_value(st.effective_data, idx);
                if (nv.empty() && idx.sparse) continue;

                // If the original doc already had the same value, it's a
                // no-op for uniqueness (we're not adding a new entry).
                if (st.original_exists) {
                    std::string ov = engine_->index_value(st.original_env["data"], idx);
                    if (ov == nv) continue;
                }

                // In-tx collision check.
                std::string txk = idx.name + "\x1F" + nv;
                auto txit = tx_unique_keys.find(txk);
                if (txit != tx_unique_keys.end() && txit->second != sample->id) {
                    return Status::Duplicate(
                        "unique index violation within transaction: " + idx.name);
                }
                tx_unique_keys[txk] = sample->id;

                // Store-side collision check.
                auto found = engine_->store_->prefix_scan(
                    CollectionEngine::idx_prefix(sample->db, sample->sch,
                                                 sample->col, idx.name) + nv + ":", 8);
                for (auto& [k, owner_id] : found) {
                    // Ignore entries that belong to docs we're rewriting
                    // (the original_env case was handled above; this catches
                    // the case where another doc in this tx is being moved
                    // off this value).
                    if (owner_id == sample->id) continue;
                    auto other_dk = CollectionEngine::doc_key(
                        sample->db, sample->sch, sample->col, owner_id);
                    auto sit = states.find(other_dk);
                    if (sit != states.end()) {
                        // The colliding owner is being modified in this tx.
                        // If its post-tx state no longer has this value,
                        // the conflict will resolve when we apply.
                        if (!sit->second.effective_exists) continue;
                        std::string other_nv = engine_->index_value(
                            sit->second.effective_data, idx);
                        if (other_nv != nv) continue;
                    }
                    return Status::Duplicate(
                        "unique index violation: " + idx.name);
                }
            }

            // Document size cap.
            std::string sample_serialized = json{
                {"_id", sample->id},
                {"data", st.effective_data},
                {"version", st.original_exists
                    ? st.original_env.value("version", 1ull) + 1 : 1ull},
                {"created_at", st.original_exists
                    ? st.original_env.value("created_at", now_ms()) : now_ms()},
                {"updated_at", now_ms()},
                {"ttl", 0}
            }.dump();
            if (sample_serialized.size() > constants::COLLECTION_MAX_DOC_BYTES) {
                return Status::Invalid("document exceeds maximum size: " + sample->id);
            }
        }
    }

    // Step 4: Apply. From here on we don't fail.
    // Track per-collection document_count deltas so we update meta once.
    std::map<std::string, int64_t> count_delta;

    for (auto& [dk, st] : states) {
        const Op* sample = nullptr;
        for (auto& op : ops_) {
            if (CollectionEngine::doc_key(op.db, op.sch, op.col, op.id) == dk) {
                sample = &op;
                break;
            }
        }
        if (!sample) continue;
        const CollectionMeta& m = metas[col_key(sample->db, sample->sch, sample->col)];

        if (st.effective_exists) {
            uint64_t new_version = st.original_exists
                ? st.original_env.value("version", 1ull) + 1 : 1ull;
            uint64_t created = st.original_exists
                ? st.original_env.value("created_at", now_ms()) : now_ms();
            // Pick the largest TTL from any INSERT op for this key (rare:
            // tx with insert + update). Default 0 if no insert.
            uint32_t ttl = 0;
            for (auto& op : ops_) {
                if (op.kind == OpKind::INSERT &&
                    CollectionEngine::doc_key(op.db, op.sch, op.col, op.id) == dk) {
                    ttl = op.ttl;
                }
            }
            json env = {
                {"_id", sample->id},
                {"data", st.effective_data},
                {"version", new_version},
                {"created_at", created},
                {"updated_at", now_ms()},
                {"ttl", ttl}
            };
            engine_->store_->put(dk, env.dump());
            // Index maintenance.
            json old_data = st.original_exists ? st.original_env["data"] : json();
            for (auto& idx : m.indexes) {
                engine_->update_index(sample->db, sample->sch, sample->col,
                                      idx, sample->id, old_data, st.effective_data);
            }
            if (!st.original_exists) {
                count_delta[col_key(sample->db, sample->sch, sample->col)]++;
            }
        } else {
            // Remove.
            if (st.original_exists) {
                engine_->store_->del(dk);
                json old_data = st.original_env["data"];
                for (auto& idx : m.indexes) {
                    engine_->update_index(sample->db, sample->sch, sample->col,
                                          idx, sample->id, old_data, json());
                }
                count_delta[col_key(sample->db, sample->sch, sample->col)]--;
            }
        }
    }

    // Apply count deltas.
    for (auto& [ck, delta] : count_delta) {
        if (delta == 0) continue;
        // Recover (db,sch,col) from ck (\x1F-separated).
        size_t p1 = ck.find('\x1F');
        size_t p2 = ck.find('\x1F', p1 + 1);
        std::string db  = ck.substr(0, p1);
        std::string sch = ck.substr(p1 + 1, p2 - p1 - 1);
        std::string col = ck.substr(p2 + 1);
        CollectionMeta& m = metas[ck];
        if (delta > 0) {
            m.document_count += static_cast<uint64_t>(delta);
        } else {
            uint64_t d = static_cast<uint64_t>(-delta);
            m.document_count = (m.document_count > d)
                ? m.document_count - d : 0ull;
        }
        engine_->store_->put(CollectionEngine::meta_key(db, sch, col),
                             m.to_json().dump());
    }

    committed_ = true;
    return Status::Ok();
}

} // namespace delta
