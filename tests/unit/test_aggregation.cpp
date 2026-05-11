// =============================================================================
// test_aggregation.cpp — P1-9 $project + $unwind regression.
// =============================================================================
#include "../../src/core/collection.hpp"
#include "../../src/storage/lsm_tree.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

int main() {
    std::string dir = "./test_agg_" + std::to_string(now_ms());
    std::filesystem::remove_all(dir);
    storage::LSMTree store(dir);
    CollectionEngine col(&store);

    CollectionMeta cm;
    cm.database = "default"; cm.schema = "public"; cm.name = "orders";
    CHECK(col.create_collection(cm).ok());

    // 3 orders. Each has tags array (for $unwind) and nested customer.name (for $project).
    auto mkdoc = [&](const std::string& id, double total,
                     std::vector<std::string> tags, const std::string& cust){
        json d = {{"_id", id}, {"total", total}, {"tags", tags},
                  {"customer", {{"name", cust}, {"region","EMEA"}}}};
        std::string out;
        CHECK(col.insert("default","public","orders", d, out).ok());
    };
    mkdoc("o1", 100.0, {"vip","retail"}, "Alice");
    mkdoc("o2",  50.0, {"retail"},       "Bob");
    mkdoc("o3", 200.0, {},                "Cara");

    // ---- $unwind tags -----------------------------------------------------
    json pipe1 = json::array({ json{{"$unwind", "$tags"}} });
    json out1 = col.aggregate("default","public","orders", pipe1);
    CHECK(out1.is_array());
    // o1(2) + o2(1) + o3(0) = 3 unwound rows
    CHECK(out1.size() == 3);
    // Each row should have a non-array `tags`.
    for (auto& r : out1) CHECK(!r["tags"].is_array());

    // ---- $unwind with preserve --------------------------------------------
    json pipe2 = json::array({
        json{{"$unwind", json{{"path","$tags"},{"preserveNullAndEmptyArrays",true}}}}
    });
    json out2 = col.aggregate("default","public","orders", pipe2);
    CHECK(out2.size() == 4);                        // o3's empty array preserved

    // ---- $project include-mode + rename via $-prefix ---------------------
    json pipe3 = json::array({ json{{"$project",
        json{{"total", 1}, {"who", "$customer.name"}}}} });
    json out3 = col.aggregate("default","public","orders", pipe3);
    CHECK(out3.size() == 3);
    for (auto& r : out3) {
        CHECK(r.contains("_id"));
        CHECK(r.contains("total"));
        CHECK(r.contains("who"));
        CHECK(!r.contains("tags"));
        CHECK(!r.contains("customer"));
    }

    // ---- $project exclude-mode --------------------------------------------
    json pipe4 = json::array({ json{{"$project",
        json{{"tags", 0}, {"customer", 0}}}} });
    json out4 = col.aggregate("default","public","orders", pipe4);
    for (auto& r : out4) {
        CHECK(!r.contains("tags"));
        CHECK(!r.contains("customer"));
        CHECK(r.contains("total"));
    }

    // ---- $match → $unwind → $group: aggregate per tag ---------------------
    json pipe5 = json::array({
        json{{"$unwind", "$tags"}},
        json{{"$group", json{
            {"_id", "$tags"},
            {"total", json{{"$sum", "$total"}}}
        }}}
    });
    json out5 = col.aggregate("default","public","orders", pipe5);
    // vip: 100; retail: 100 + 50 = 150
    bool seen_vip = false, seen_retail = false;
    for (auto& r : out5) {
        if (r["_id"] == "vip")    { CHECK(r["total"].get<double>() == 100.0); seen_vip = true; }
        if (r["_id"] == "retail") { CHECK(r["total"].get<double>() == 150.0); seen_retail = true; }
    }
    CHECK(seen_vip && seen_retail);

    std::filesystem::remove_all(dir);
    std::cout << "test_aggregation: OK" << std::endl;
    return 0;
}
