// =============================================================================
// test_collection_concurrent.cpp — concurrency regression for P0-5.
//
// 200 worker threads race to insert documents that share the same value of
// the "email" field, where "email" carries a UNIQUE index. Before P0-5
// the unique-index check happened outside any lock and two concurrent
// inserts could both pass it; afterward the unique_lock<shared_mutex>
// makes exactly ONE succeed and the rest fail with Status::DUPLICATE.
// =============================================================================
#include "../../src/core/collection.hpp"
#include "../../src/storage/lsm_tree.hpp"
#include <atomic>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

int main() {
    std::string dir = "./test_col_concurrent_" + std::to_string(now_ms());
    std::filesystem::remove_all(dir);

    storage::LSMTree store(dir);
    CollectionEngine col(&store);

    CollectionMeta m;
    m.database = "default"; m.schema = "public"; m.name = "users";
    IndexDef idx;
    idx.name = "uniq_email";
    idx.fields = {"email"};
    idx.unique = true;
    m.indexes.push_back(idx);
    CHECK(col.create_collection(m).ok());

    constexpr int kThreads = 64;
    std::atomic<int> ok_count{0};
    std::atomic<int> dup_count{0};
    std::atomic<int> other_err{0};

    auto worker = [&]() {
        json doc = {{"email", "race@example.com"}, {"name", "racer"}};
        std::string id;
        auto st = col.insert("default", "public", "users", doc, id);
        if (st.ok()) {
            ok_count.fetch_add(1);
        } else if (st.code == Status::DUPLICATE) {
            dup_count.fetch_add(1);
        } else {
            other_err.fetch_add(1);
        }
    };

    std::vector<std::thread> ts;
    ts.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) ts.emplace_back(worker);
    for (auto& t : ts) t.join();

    if (ok_count.load() != 1 || other_err.load() != 0) {
        std::cerr << "FAIL: ok=" << ok_count.load()
                  << " dup=" << dup_count.load()
                  << " other=" << other_err.load() << std::endl;
        std::filesystem::remove_all(dir);
        return 1;
    }

    std::filesystem::remove_all(dir);
    std::cout << "test_collection_concurrent: OK ("
              << ok_count.load() << " ok, "
              << dup_count.load() << " duplicates rejected)" << std::endl;
    return 0;
}
