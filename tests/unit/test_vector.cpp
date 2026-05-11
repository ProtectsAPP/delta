// =============================================================================
// test_vector.cpp — regression tests for the HNSW audit fixes.
//
// Coverage:
//   * insert() locks dimension on first call, rejects mismatches (P2-4)
//   * search() rejects queries with the wrong dimension          (P2-4)
//   * concurrent search() callers do not crash or deadlock       (P1-13)
// =============================================================================
#include "../../src/vector/hnsw_index.hpp"
#include <atomic>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

static int test_insert_dimension_locked() {
    vector::HNSWIndex idx;
    idx.insert("a", {1.0f, 0.0f, 0.0f});
    CHECK(idx.dimension() == 3);
    bool threw = false;
    try { idx.insert("b", {1.0f, 0.0f}); }
    catch (const std::invalid_argument&) { threw = true; }
    CHECK(threw);
    return 0;
}

static int test_search_dimension_check() {
    vector::HNSWIndex idx;
    idx.insert("a", {1.0f, 0.0f});
    idx.insert("b", {0.0f, 1.0f});
    bool threw = false;
    try { idx.search({1.0f, 0.0f, 0.0f}, 1); }
    catch (const std::invalid_argument&) { threw = true; }
    CHECK(threw);
    auto r = idx.search({1.0f, 0.0f}, 1);
    CHECK(!r.empty());
    CHECK(r[0].id == "a");
    return 0;
}

static int test_concurrent_search() {
    vector::HNSWIndex idx;
    for (int i = 0; i < 200; ++i) {
        idx.insert("v" + std::to_string(i),
                   {(float)(i % 10), (float)((i / 10) % 10), (float)(i % 7)});
    }
    std::atomic<int> errors{0};
    auto worker = [&] {
        for (int i = 0; i < 100; ++i) {
            try {
                auto r = idx.search({(float)(i % 10), 1.0f, 2.0f}, 5);
                if (r.empty()) errors.fetch_add(1);
            } catch (...) { errors.fetch_add(1); }
        }
    };
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) threads.emplace_back(worker);
    for (auto& t : threads) t.join();
    CHECK(errors.load() == 0);
    return 0;
}

int main() {
    int rc = 0;
    rc |= test_insert_dimension_locked();
    rc |= test_search_dimension_check();
    rc |= test_concurrent_search();
    if (rc == 0) std::cout << "test_vector: OK" << std::endl;
    return rc;
}
