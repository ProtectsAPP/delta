// =============================================================================
// test_composite_index.cpp — coverage for composite (multi-field) index range
// queries in the query planner.
//
// We exercise:
//   1. Composite index with equality prefix only (2 fields, both eq)
//   2. Composite index with equality prefix + $gt range on next field
//   3. Composite index with equality prefix + $gte/$lte bounded range
//   4. Composite index with equality prefix + $lt (upper bound only)
//   5. Composite index with 3 fields, 2 eq + 1 range
//   6. Verify that the full filter is still applied (correctness guarantee)
//   7. No usable prefix → falls back to full scan (still correct)
// =============================================================================
#include "../../src/core/collection.hpp"
#include "../../src/storage/lsm_tree.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

int main() {
    std::string dir = "./test_composite_idx_" + std::to_string(now_ms());
    std::filesystem::remove_all(dir);
    storage::LSMTree store(dir);
    CollectionEngine col(&store);

    // Create collection with a composite index on [category, price].
    CollectionMeta cm;
    cm.database = "default"; cm.schema = "public"; cm.name = "products";
    CHECK(col.create_collection(cm).ok());

    IndexDef idx;
    idx.name = "cat_price";
    idx.fields = {"category", "price"};
    idx.type = "btree";
    CHECK(col.create_index("default", "public", "products", idx).ok());

    // Insert test data.
    auto ins = [&](const std::string& id, const std::string& cat, double price, const std::string& name) {
        json d = {{"_id", id}, {"category", cat}, {"price", price}, {"name", name}};
        std::string out;
        CHECK(col.insert("default", "public", "products", d, out).ok());
        return 0;
    };

    CHECK(ins("p1", "electronics", 100.0, "Mouse") == 0);
    CHECK(ins("p2", "electronics", 250.0, "Keyboard") == 0);
    CHECK(ins("p3", "electronics", 500.0, "Monitor") == 0);
    CHECK(ins("p4", "electronics", 1000.0, "Laptop") == 0);
    CHECK(ins("p5", "clothing", 30.0, "T-Shirt") == 0);
    CHECK(ins("p6", "clothing", 80.0, "Jacket") == 0);
    CHECK(ins("p7", "clothing", 200.0, "Coat") == 0);
    CHECK(ins("p8", "food", 5.0, "Apple") == 0);
    CHECK(ins("p9", "food", 15.0, "Steak") == 0);

    // Helper to access the data field from find() results (which return envelopes).
    auto data = [](const json& env) -> const json& { return env["data"]; };

    // ---- 1: Pure equality prefix (both fields eq) -------------------------
    // category=electronics AND price=250 → should find p2 only.
    {
        json filter = {{"category", "electronics"}, {"price", 250.0}};
        json sort_obj = json::object();
        json projection = json::object();
        auto results = col.find("default", "public", "products", filter, sort_obj, 0, 1000, projection);
        CHECK(results.size() == 1);
        CHECK(data(results[0])["name"] == "Keyboard");
    }

    // ---- 2: Equality prefix + $gt range -----------------------------------
    // category=electronics AND price > 250 → p3 (500), p4 (1000).
    {
        json filter = {{"category", "electronics"}, {"price", {{"$gt", 250.0}}}};
        json sort_obj = json::object();
        json projection = json::object();
        auto results = col.find("default", "public", "products", filter, sort_obj, 0, 1000, projection);
        CHECK(results.size() == 2);
        for (auto& r : results) {
            CHECK(data(r)["category"] == "electronics");
            CHECK(data(r)["price"].get<double>() > 250.0);
        }
    }

    // ---- 3: Equality prefix + $gte/$lte bounded range ---------------------
    // category=electronics AND price >= 100 AND price <= 500 → p1, p2, p3.
    {
        json filter = {{"category", "electronics"}, {"price", {{"$gte", 100.0}, {"$lte", 500.0}}}};
        json sort_obj = json::object();
        json projection = json::object();
        auto results = col.find("default", "public", "products", filter, sort_obj, 0, 1000, projection);
        CHECK(results.size() == 3);
        for (auto& r : results) {
            CHECK(data(r)["category"] == "electronics");
            double p = data(r)["price"].get<double>();
            CHECK(p >= 100.0 && p <= 500.0);
        }
    }

    // ---- 4: Equality prefix + $lt (upper bound only) ----------------------
    // category=clothing AND price < 80 → p5 (30).
    {
        json filter = {{"category", "clothing"}, {"price", {{"$lt", 80.0}}}};
        json sort_obj = json::object();
        json projection = json::object();
        auto results = col.find("default", "public", "products", filter, sort_obj, 0, 1000, projection);
        CHECK(results.size() == 1);
        CHECK(data(results[0])["name"] == "T-Shirt");
    }

    // ---- 5: 3-field composite index: 2 eq + 1 range -----------------------
    {
        IndexDef idx3;
        idx3.name = "cat_name_price";
        idx3.fields = {"category", "name", "price"};
        idx3.type = "btree";
        CHECK(col.create_index("default", "public", "products", idx3).ok());

        json filter = {{"category", "electronics"}, {"name", "Monitor"}, {"price", {{"$gt", 0.0}}}};
        json sort_obj = json::object();
        json projection = json::object();
        auto results = col.find("default", "public", "products", filter, sort_obj, 0, 1000, projection);
        CHECK(results.size() == 1);
        CHECK(data(results[0])["_id"] == "p3");
    }

    // ---- 6: Verify correctness — filter is always re-applied ---------------
    {
        json filter = {{"category", "electronics"}, {"price", {{"$gt", 250.0}}}, {"name", "Monitor"}};
        json sort_obj = json::object();
        json projection = json::object();
        auto results = col.find("default", "public", "products", filter, sort_obj, 0, 1000, projection);
        CHECK(results.size() == 1);
        CHECK(data(results[0])["name"] == "Monitor");
    }

    // ---- 7: No usable prefix → full scan still works ----------------------
    {
        json filter = {{"price", {{"$gt", 100.0}}}};
        json sort_obj = json::object();
        json projection = json::object();
        auto results = col.find("default", "public", "products", filter, sort_obj, 0, 1000, projection);
        CHECK(results.size() == 4);
        for (auto& r : results) {
            CHECK(data(r)["price"].get<double>() > 100.0);
        }
    }

    // ---- 8: Equality on first field only (prefix scan, no range) ----------
    {
        json filter = {{"category", "food"}};
        json sort_obj = json::object();
        json projection = json::object();
        auto results = col.find("default", "public", "products", filter, sort_obj, 0, 1000, projection);
        CHECK(results.size() == 2);
        for (auto& r : results) {
            CHECK(data(r)["category"] == "food");
        }
    }

    std::filesystem::remove_all(dir);
    std::cout << "test_composite_index OK\n";
    return 0;
}
