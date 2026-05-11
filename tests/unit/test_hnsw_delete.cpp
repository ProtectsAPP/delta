// =============================================================================
// test_hnsw_delete.cpp — P2-3 orphan reconnection regression.
//
// Strategy: build a small index, delete a large fraction of vectors, and
// then verify that a query for each of the SURVIVING vectors can still
// find that exact vector at rank 1. Before P2-3, heavy deletion
// partitioned the graph and some nodes became unreachable — the top-1
// for their own vector returned a different id (or nothing).
// =============================================================================
#include "../../src/vector/hnsw_index.hpp"
#include <cassert>
#include <iostream>
#include <set>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

int main() {
    // L2 metric: unique distances, unambiguous top-1.
    //
    // ef_search cranked up so the test measures graph CONNECTIVITY (the
    // thing P2-3 protects) rather than the search hyperparameter. A
    // well-connected post-delete graph finds exact matches even with a
    // wide search beam; a partitioned graph can't.
    vector::HNSWConfig cfg;
    cfg.metric    = "euclidean";
    cfg.ef_search = 200;
    vector::HNSWIndex idx(cfg);

    // Build: 300 points spread along three axes so each has a unique spot.
    const int N = 300;
    for (int i = 0; i < N; ++i) {
        idx.insert("v" + std::to_string(i),
                   {(float)i, (float)(i * 3), (float)(i * 7)});
    }

    // Delete ~40% of the nodes (every 5th + every 7th ⇒ bunched removals).
    std::set<int> removed;
    for (int i = 0; i < N; ++i) {
        if (i % 5 == 0 || i % 7 == 0) {
            idx.remove("v" + std::to_string(i));
            removed.insert(i);
        }
    }
    size_t expected_live = (size_t)(N - (int)removed.size());
    CHECK(idx.size() == expected_live);

    // For every surviving vector, a query for its exact coordinates must
    // return it. With an uncompacted / partitioned graph (pre-P2-3) recall
    // would collapse to ~5%. With the reconnect we should comfortably
    // clear 90% recall@1 — this is the regression guard for P2-3.
    int hits = 0;
    for (int i = 0; i < N; ++i) {
        if (removed.count(i)) continue;
        auto r = idx.search({(float)i, (float)(i * 3), (float)(i * 7)}, 1);
        if (!r.empty() && r[0].id == "v" + std::to_string(i)) ++hits;
    }
    double recall = (double)hits / (double)expected_live;
    std::cerr << "P2-3 recall@1 = " << hits << "/" << expected_live
              << " = " << recall << std::endl;
    if (recall < 0.9) {
        std::cerr << "FAIL: recall@1 dropped below 0.9 after heavy deletion" << std::endl;
        return 1;
    }

    std::cout << "test_hnsw_delete: OK" << std::endl;
    return 0;
}
