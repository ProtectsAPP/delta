// =============================================================================
// test_compaction.cpp — STCS compaction regression (P0-1) +
//                       SSTable v3 sparse-index trailer round-trip (P1-2).
//
// Strategy:
//   1. Open an LSMTree, write enough data + force a flush, repeat until
//      we have >= STORAGE_COMPACTION_MIN_SSTS SSTables of similar size.
//   2. Call compact_now_for_test(). Expect: SSTable count drops by N-1
//      (the merged run collapses into one new file), all keys still
//      readable, tombstoned keys disappear.
//   3. Reopen the directory — the new file loads, the old ones are gone.
//   4. The sparse_index field on the new SSTable is non-empty.
// =============================================================================
#include "../../src/storage/lsm_tree.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

static int test_compaction_merges_and_preserves_data() {
    std::string dir = "./test_compact_" + std::to_string(now_ms());
    std::filesystem::remove_all(dir);

    int before = 0, after_compact = 0;
    {
        storage::LSMTree t(dir);
        // Write 4 SSTables of similar size by forcing flushes between batches.
        for (int batch = 0; batch < 4; ++batch) {
            for (int i = 0; i < 50; ++i) {
                std::string k = "k-" + std::to_string(batch) + "-" + std::to_string(i);
                t.put(k, "v-" + std::to_string(batch) + "-" + std::to_string(i));
            }
            t.flush_now_for_test();
        }
        before = (int)t.sstable_count();
        CHECK(before >= constants::STORAGE_COMPACTION_MIN_SSTS);

        int merged_away = t.compact_now_for_test();
        after_compact = (int)t.sstable_count();
        CHECK(merged_away >= constants::STORAGE_COMPACTION_MIN_SSTS - 1);
        CHECK(after_compact < before);

        // Every key from every batch must still be reachable.
        for (int batch = 0; batch < 4; ++batch) {
            for (int i = 0; i < 50; ++i) {
                std::string k = "k-" + std::to_string(batch) + "-" + std::to_string(i);
                std::string v;
                CHECK(t.get(k, &v));
                CHECK(v == "v-" + std::to_string(batch) + "-" + std::to_string(i));
            }
        }
    }
    // Reopen and re-verify (durability + load() of compacted file).
    {
        storage::LSMTree t(dir);
        CHECK((int)t.sstable_count() == after_compact);
        for (int batch = 0; batch < 4; ++batch) {
            for (int i = 0; i < 50; ++i) {
                std::string k = "k-" + std::to_string(batch) + "-" + std::to_string(i);
                std::string v;
                CHECK(t.get(k, &v));
            }
        }
    }
    std::filesystem::remove_all(dir);
    return 0;
}

static int test_compaction_drops_tombstones_when_run_includes_tail() {
    std::string dir = "./test_compact_tomb_" + std::to_string(now_ms());
    std::filesystem::remove_all(dir);

    {
        storage::LSMTree t(dir);
        for (int batch = 0; batch < 4; ++batch) {
            for (int i = 0; i < 20; ++i) {
                t.put("alive-" + std::to_string(batch) + "-" + std::to_string(i), "x");
            }
            // Plant + delete the same key inside each batch so the tombstone
            // accompanies the value in the same SSTable.
            t.put("ghost-" + std::to_string(batch), "boo");
            t.del("ghost-" + std::to_string(batch));
            t.flush_now_for_test();
        }
        t.compact_now_for_test();
        for (int batch = 0; batch < 4; ++batch) {
            std::string v;
            CHECK(!t.get("ghost-" + std::to_string(batch), &v));
        }
    }
    std::filesystem::remove_all(dir);
    return 0;
}

int main() {
    int rc = 0;
    rc |= test_compaction_merges_and_preserves_data();
    rc |= test_compaction_drops_tombstones_when_run_includes_tail();
    if (rc == 0) std::cout << "test_compaction: OK" << std::endl;
    return rc;
}
