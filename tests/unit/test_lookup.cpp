// =============================================================================
// test_lookup.cpp — coverage for the $lookup aggregation stage.
//
// $lookup performs an equi-join between the pipeline's source collection and
// a `from` collection, attaching matched foreign documents to each input doc
// under the `as` field.
//
// We exercise:
//   1. Scalar localField → scalar foreignField (single match)
//   2. Scalar localField → no match (empty array result)
//   3. Array localField (each element probes the hash index)
//   4. Array foreignField on the joined side (each element registers a key)
//   5. Multiple matches preserved as array
//   6. Malformed spec (missing fields) is tolerated and passes docs through
// =============================================================================
#include "../../src/core/collection.hpp"
#include "../../src/storage/lsm_tree.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

int main() {
    std::string dir = "./test_lookup_" + std::to_string(now_ms());
    std::filesystem::remove_all(dir);
    storage::LSMTree store(dir);
    CollectionEngine col(&store);

    // Two collections in the same (db, schema): orders and customers.
    auto mkc = [&](const std::string& name) {
        CollectionMeta cm;
        cm.database = "default"; cm.schema = "public"; cm.name = name;
        CHECK(col.create_collection(cm).ok());
        return 0;
    };
    CHECK(mkc("orders") == 0);
    CHECK(mkc("customers") == 0);
    CHECK(mkc("tags") == 0);

    auto ins = [&](const std::string& c, json d) {
        std::string out;
        CHECK(col.insert("default","public",c, d, out).ok());
        return 0;
    };

    // customers: simple scalar `cid`.
    CHECK(ins("customers", {{"_id","c1"},{"cid",1},{"name","Alice"}}) == 0);
    CHECK(ins("customers", {{"_id","c2"},{"cid",2},{"name","Bob"}})   == 0);
    CHECK(ins("customers", {{"_id","c3"},{"cid",3},{"name","Cara"}})  == 0);

    // orders: each carries a customer_id; o4 references a non-existent cid.
    CHECK(ins("orders", {{"_id","o1"},{"customer_id",1},{"total",100}}) == 0);
    CHECK(ins("orders", {{"_id","o2"},{"customer_id",2},{"total", 50}}) == 0);
    CHECK(ins("orders", {{"_id","o3"},{"customer_id",1},{"total",200}}) == 0);
    CHECK(ins("orders", {{"_id","o4"},{"customer_id",9},{"total", 10}}) == 0);

    // tags: foreign side has an array field, exercises array-foreignField.
    CHECK(ins("tags", {{"_id","t1"},{"name","vip"},  {"applies_to",{1,3}}}) == 0);
    CHECK(ins("tags", {{"_id","t2"},{"name","new"},  {"applies_to",{2}}})   == 0);
    CHECK(ins("tags", {{"_id","t3"},{"name","cold"}, {"applies_to",{}}})    == 0);

    // ---- 1+2: scalar→scalar, with one no-match -------------------------------
    json pipe1 = json::array({
        json{{"$lookup", {
            {"from","customers"},
            {"localField","customer_id"},
            {"foreignField","cid"},
            {"as","customer"}
        }}}
    });
    json out1 = col.aggregate("default","public","orders", pipe1);
    CHECK(out1.is_array());
    CHECK(out1.size() == 4);

    // Build a quick map by _id for assertions, since aggregate output order
    // is not contractually defined.
    auto by_id = [](const json& arr, const std::string& id) -> json {
        for (auto& d : arr) if (d.value("_id", std::string()) == id) return d;
        return json();
    };

    json o1 = by_id(out1, "o1");
    CHECK(o1.is_object());
    CHECK(o1["customer"].is_array());
    CHECK(o1["customer"].size() == 1);
    CHECK(o1["customer"][0]["name"] == "Alice");

    json o3 = by_id(out1, "o3");
    CHECK(o3["customer"].size() == 1);
    CHECK(o3["customer"][0]["name"] == "Alice");        // same customer

    json o4 = by_id(out1, "o4");
    CHECK(o4["customer"].is_array());
    CHECK(o4["customer"].empty());                      // no match → empty arr

    // ---- 3: array localField probes each element -----------------------------
    // Add an order with customer_ids = [1,2]; both should match.
    CHECK(ins("orders", {{"_id","o5"},{"customer_id",{1,2}},{"total",333}}) == 0);
    json out3 = col.aggregate("default","public","orders", pipe1);
    json o5 = by_id(out3, "o5");
    CHECK(o5["customer"].is_array());
    CHECK(o5["customer"].size() == 2);

    // ---- 4: array foreignField — join orders→tags by customer_id ∈ applies_to
    json pipe4 = json::array({
        json{{"$lookup", {
            {"from","tags"},
            {"localField","customer_id"},
            {"foreignField","applies_to"},
            {"as","tags"}
        }}}
    });
    json out4 = col.aggregate("default","public","orders", pipe4);
    json o1b = by_id(out4, "o1");
    // customer_id=1 should hit tag t1 ("vip", applies_to=[1,3]).
    CHECK(o1b["tags"].is_array());
    CHECK(o1b["tags"].size() == 1);
    CHECK(o1b["tags"][0]["name"] == "vip");

    json o2b = by_id(out4, "o2");
    // customer_id=2 should hit tag t2 ("new", applies_to=[2]).
    CHECK(o2b["tags"].size() == 1);
    CHECK(o2b["tags"][0]["name"] == "new");

    // ---- 5: malformed spec is tolerated --------------------------------------
    json pipe5 = json::array({
        json{{"$lookup", {{"from","customers"}}}}     // missing fields
    });
    json out5 = col.aggregate("default","public","orders", pipe5);
    CHECK(out5.is_array());
    CHECK(out5.size() >= 4);                          // input passed through

    // ---- 6: $lookup composes with $match downstream --------------------------
    json pipe6 = json::array({
        json{{"$lookup", {
            {"from","customers"},
            {"localField","customer_id"},
            {"foreignField","cid"},
            {"as","customer"}
        }}},
        json{{"$match", {{"total", json{{"$gte", 100}}}}}}
    });
    json out6 = col.aggregate("default","public","orders", pipe6);
    for (auto& d : out6) {
        CHECK(d["total"].get<double>() >= 100.0);
        CHECK(d["customer"].is_array());
    }

    std::filesystem::remove_all(dir);
    std::cout << "test_lookup OK\n";
    return 0;
}
