#pragma once
// =============================================================================
// transaction.hpp — multi-document ACID transactions over CollectionEngine.
//
// Design: optimistic concurrency control (OCC) with commit-time validation.
//
//   * Atomicity:   all buffered mutations apply at commit, or none of them
//                  do. Validation runs first (uniqueness, version match,
//                  document existence). Only if every check passes do we
//                  start writing to the LSM. Once writes start they all
//                  complete (the LSM put/del path is itself crash-safe via
//                  WAL fsync).
//
//   * Consistency: enforced by the same constraint checks that the non-
//                  transactional CollectionEngine path applies (unique
//                  indexes, _id uniqueness, document size cap).
//
//   * Isolation:   a Transaction holds a unique_lock on
//                  CollectionEngine::mu_ for the entire commit, so all
//                  validation and apply happens with no other reader or
//                  writer interleaved. Reads inside an open transaction
//                  see read-your-writes from the buffer; reads not yet in
//                  the buffer fall through to the engine and record the
//                  observed version into the read set. At commit the read
//                  set is re-validated against current versions, so any
//                  document a transaction read but didn't yet modify
//                  cannot be silently changed by a concurrent writer
//                  between read and commit (classic OCC validation).
//
//   * Durability:  same as non-transactional writes: LSMTree fsyncs the
//                  WAL after each put/del. The full transaction is not
//                  written as a single WAL frame, so crash recovery may
//                  observe a prefix of a partially-applied transaction
//                  IF the process is killed mid-commit between the first
//                  and last LSM put. This is documented; cluster-wide
//                  atomicity across many puts within a single WAL frame
//                  is left for a future follow-up (would require an
//                  LSMTree::apply_batch primitive).
//
// Usage:
//
//   auto tx = engine.begin_transaction();
//   tx.insert("default", "public", "orders", order_doc, oid);
//   tx.update("default", "public", "inventory", item_id,
//             {{"$inc", {{"qty", -1}}}});
//   if (auto s = tx.commit(); !s.ok()) { /* handle conflict */ }
//
// The Transaction object is move-only; it cannot outlive its engine.
// =============================================================================
#include "../core/common.hpp"
#include "../core/document.hpp"
#include "../storage/lsm_tree.hpp"
#include <map>
#include <vector>
#include <variant>

namespace delta {

class CollectionEngine;  // forward

class Transaction {
public:
    // Constructed by CollectionEngine::begin_transaction(). Not copyable;
    // moves transfer ownership of the buffered ops and read set.
    explicit Transaction(CollectionEngine* engine) : engine_(engine) {}
    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;
    Transaction(Transaction&&) noexcept = default;
    Transaction& operator=(Transaction&&) noexcept = default;
    ~Transaction() = default;  // dropping without commit/rollback discards.

    // ---- buffered mutations -----------------------------------------------
    //
    // None of these touch the underlying store. They append to the in-memory
    // op log; commit() applies them atomically.

    Status insert(const std::string& db, const std::string& sch,
                  const std::string& col, json doc,
                  std::string& id_out, uint32_t ttl = 0);

    Status update(const std::string& db, const std::string& sch,
                  const std::string& col, const std::string& id,
                  const json& update_ops, uint64_t expected_version = 0);

    Status remove(const std::string& db, const std::string& sch,
                  const std::string& col, const std::string& id);

    // ---- buffered reads ---------------------------------------------------
    //
    // get() returns the document the transaction would see at commit time:
    // the latest buffered write in this transaction, or, failing that, the
    // current store value (whose version is recorded in the read set so
    // commit can re-validate).
    bool get(const std::string& db, const std::string& sch,
             const std::string& col, const std::string& id, json* out);

    // ---- terminal operations ----------------------------------------------

    // Validate read set + apply write set under engine.mu_. Returns
    // Status::Conflict if any read-set version moved or any write-set
    // constraint check (uniqueness, version match) fails.
    Status commit();

    // Drop the buffer. Safe to call multiple times; safe to call after
    // commit (turns into a no-op).
    void rollback();

    // ---- introspection ----------------------------------------------------
    bool committed() const { return committed_; }
    bool rolled_back() const { return rolled_back_; }
    size_t op_count() const { return ops_.size(); }

private:
    friend class CollectionEngine;

    enum class OpKind { INSERT, UPDATE, REMOVE };

    struct Op {
        OpKind kind;
        std::string db;
        std::string sch;
        std::string col;
        std::string id;
        // INSERT carries the full doc; UPDATE carries the update operators
        // dict; REMOVE carries nothing beyond (db,sch,col,id).
        json payload;
        uint64_t expected_version = 0;  // UPDATE only
        uint32_t ttl = 0;               // INSERT only
    };

    // Find the most recent buffered op that targets (db,sch,col,id).
    // Returns nullptr if none. Used for read-your-writes.
    const Op* last_op_for(const std::string& db, const std::string& sch,
                          const std::string& col, const std::string& id) const;

    // Apply update operators on top of an in-memory document, mirroring the
    // engine's update() semantics so reads inside the same tx see the
    // post-update view.
    static json apply_update_ops(const json& base, const json& ops);

    CollectionEngine* engine_;
    std::vector<Op> ops_;
    // doc_key → observed version at first read inside this tx. Re-validated
    // at commit. A version of 0 means "we observed that the doc did NOT
    // exist at read time"; commit re-checks that nobody else inserted it.
    std::map<std::string, uint64_t> read_set_;
    bool committed_ = false;
    bool rolled_back_ = false;
};

} // namespace delta
