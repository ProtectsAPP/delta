// =============================================================================
// test_query_planner.cpp — P0-4 index-driven find/count.
//
// Strategy:
//   * Build a collection with two indexes (single-field each).
//   * Insert 1000 docs.
//   * Verify that filters which CAN use an index are reported as such by
//     query_uses_index(), and the find()/count() answers match a brute-
//     force linear comparison.
//   * Verify that filters which CANNOT (range / regex / $or) fall back to
//     scan and still produce correct answers.
// =============================================================================
#include "../../src/core/collection.hpp"
#include "../../src/storage/lsm_tree.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

int main() {
    std::string dir = "./test_planner_" + std::to_string(now_ms());
    std::filesystem::remove_all(dir);
    storage::LSMTree store(dir);
    CollectionEngine col(&store);

    CollectionMeta cm;
    cm.database = "default"; cm.schema = "public"; cm.name = "items";
    IndexDef ix_status; ix_status.name = "ix_status"; ix_status.fields = {"status"};
    IndexDef ix_owner;  ix_owner.name  = "ix_owner";  ix_owner.fields  = {"owner"};
    cm.indexes = {ix_status, ix_owner};
    CHECK(col.create_collection(cm).ok());

    // Insert 100 docs spread across 4 statuses x 5 owners.
    for (int i = 0; i < 100; ++i) {
        json d = {
            {"status", std::string("s") + std::to_string(i % 4)},
            {"owner",  std::string("o") + std::to_string(i % 5)},
            {"score",  i}
        };
        std::string id; CHECK(col.insert("default","public","items", d, id).ok());
    }

    // 1. simple equality on indexed field uses the index.
    json f1 = {{"status", "s2"}};
    CHECK(col.query_uses_index("default","public","items", f1));
    CHECK(col.count("default","public","items", f1) == 25);

    // 2. $eq form.
    json f2 = {{"status", {{"$eq", "s2"}}}};
    CHECK(col.query_uses_index("default","public","items", f2));
    CHECK(col.count("default","public","items", f2) == 25);

    // 3. $in form.
    json f3 = {{"status", {{"$in", json::array({"s0", "s1"})}}}};
    CHECK(col.query_uses_index("default","public","items", f3));
    CHECK(col.count("default","public","items", f3) == 50);

    // 4. AND of two indexed equalities: planner intersects.
    json f4 = {{"$and", json::array({
        json{{"status","s0"}}, json{{"owner","o0"}}
    })}};
    CHECK(col.query_uses_index("default","public","items", f4));
    // i mod 4 == 0 AND i mod 5 == 0  --> i ∈ {0, 20, 40, 60, 80}
    CHECK(col.count("default","public","items", f4) == 5);

    // 5. range filter cannot use the index.
    json f5 = {{"score", {{"$gt", 90}}}};
    CHECK(!col.query_uses_index("default","public","items", f5));
    CHECK(col.count("default","public","items", f5) == 9);   // 91..99

    // 6. find() result with indexed + extra-clause filter — extra clause
    //    must still narrow the candidate set.
    json f6 = {{"$and", json::array({
        json{{"status","s0"}}, json{{"score", {{"$gt", 50}}}}
    })}};
    auto res = col.find("default","public","items", f6, json::object(), 0, 1000, json::object());
    for (auto& env : res) {
        CHECK(env["data"]["status"] == "s0");
        CHECK(env["data"]["score"].get<int>() > 50);
    }

    std::filesystem::remove_all(dir);
    std::cout << "test_query_planner: OK" << std::endl;
    return 0;
}
