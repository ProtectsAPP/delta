// =============================================================================
// test_optimistic_lock.cpp — P1-6 optimistic-locking regression.
// =============================================================================
#include "../../src/core/collection.hpp"
#include "../../src/storage/lsm_tree.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

int main() {
    std::string dir = "./test_oplock_" + std::to_string(now_ms());
    std::filesystem::remove_all(dir);
    storage::LSMTree store(dir);
    CollectionEngine col(&store);

    CollectionMeta cm;
    cm.database = "default"; cm.schema = "public"; cm.name = "docs";
    CHECK(col.create_collection(cm).ok());

    std::string id;
    json seed = json{{"counter", 0}};
    CHECK(col.insert("default","public","docs", seed, id).ok());

    // Read current state.
    json env;
    CHECK(col.get("default","public","docs", id, &env));
    uint64_t v0 = env.value("version", 0ull);
    CHECK(v0 == 1);   // first version stamped by insert()

    // ---- legacy path: no expected_version => success regardless ----
    json updated1;
    CHECK(col.update("default","public","docs", id,
                     json{{"$inc", {{"counter", 1}}}}, &updated1).ok());
    CHECK(updated1["version"].get<uint64_t>() == 2);

    // ---- optimistic path: correct expected_version ----
    json updated2;
    auto st2 = col.update("default","public","docs", id,
                          json{{"$inc", {{"counter", 1}}}}, &updated2, 2);
    CHECK(st2.ok());
    CHECK(updated2["version"].get<uint64_t>() == 3);

    // ---- optimistic path: stale expected_version ----
    json updated3;
    auto st3 = col.update("default","public","docs", id,
                          json{{"$inc", {{"counter", 99}}}}, &updated3, 2);
    CHECK(!st3.ok());
    CHECK(st3.code == Status::CONFLICT);

    // Confirm the rejected write did NOT apply.
    json env2;
    CHECK(col.get("default","public","docs", id, &env2));
    CHECK(env2["data"]["counter"].get<int>() == 2);   // 0 -> 1 -> 2 only
    CHECK(env2["version"].get<uint64_t>() == 3);

    std::filesystem::remove_all(dir);
    std::cout << "test_optimistic_lock: OK" << std::endl;
    return 0;
}
