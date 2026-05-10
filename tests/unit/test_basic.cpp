#include "../../src/storage/lsm_tree.hpp"
#include "../../src/core/collection.hpp"
#include "../../src/cache/cache_engine.hpp"
#include "../../src/vector/hnsw_index.hpp"
#include "../../src/auth/auth_manager.hpp"
#include "../../src/database/database_manager.hpp"
#include <iostream>
#include <cassert>
#include <filesystem>

using namespace delta;

#define ASSERT_TRUE(x) do { if (!(x)) { std::cerr << "FAIL: " #x " at " << __LINE__ << std::endl; return 1; } } while(0)
#define ASSERT_EQ(a, b) do { if (!((a)==(b))) { std::cerr << "FAIL: " #a "==" #b " at " << __LINE__ << std::endl; return 1; } } while(0)

int main() {
    std::string dir = "./test_data_" + std::to_string(now_ms());
    std::filesystem::remove_all(dir);
    {
        storage::LSMTree store(dir);
        store.put("hello", "world");
        std::string v;
        ASSERT_TRUE(store.get("hello", &v));
        ASSERT_EQ(v, "world");

        CollectionEngine col(&store);
        CollectionMeta m; m.database = "default"; m.schema = "public"; m.name = "users";
        ASSERT_TRUE(col.create_collection(m).ok());
        json doc = {{"name","alice"},{"age",30}};
        std::string id;
        ASSERT_TRUE(col.insert("default","public","users", doc, id).ok());
        json got;
        ASSERT_TRUE(col.get("default","public","users", id, &got));
        ASSERT_EQ(got["data"]["name"], "alice");

        // search
        size_t total;
        auto results = col.find("default","public","users", json{{"age", json{{"$gte", 18}}}}, json::object(), 0, 100, json::object(), &total);
        ASSERT_EQ(results.size(), 1);

        // update
        col.update("default","public","users", id, json{{"$set", {{"age", 31}}}});
        col.get("default","public","users", id, &got);
        ASSERT_EQ(got["data"]["age"], 31);

        // cache
        cache::CacheEngine ce;
        ce.set("foo", "bar");
        ASSERT_EQ(*ce.get("foo"), "bar");
        ce.lpush("ml", "a"); ce.rpush("ml", "b");
        auto items = ce.lrange("ml", 0, -1);
        ASSERT_EQ(items.size(), 2);
        ce.zadd("z", 1.0, "x"); ce.zadd("z", 0.5, "y");
        auto zr = ce.zrange("z", 0, -1, true);
        ASSERT_EQ(zr.front().first, "y");

        // vector
        vector::HNSWIndex idx;
        idx.set_metric("cosine");
        idx.insert("a", {1.0f, 0.0f, 0.0f});
        idx.insert("b", {0.0f, 1.0f, 0.0f});
        idx.insert("c", {1.0f, 0.1f, 0.0f});
        auto vr = idx.search({1.0f, 0.0f, 0.0f}, 2);
        ASSERT_TRUE(!vr.empty());
        ASSERT_EQ(vr[0].id, "a");

        // auth
        auth::AuthManager am(&store);
        ASSERT_TRUE(am.authenticate("admin", "admin"));
        ASSERT_TRUE(am.create_user("test", "pw").ok());
        ASSERT_TRUE(am.authenticate("test", "pw"));
        ASSERT_TRUE(!am.authenticate("test", "wrong"));
        am.grant("read_only", auth::PRIV_SELECT, {"collection","default","public","items"});
        am.grant_role_to_user("test", "read_only");
        ASSERT_TRUE(am.check("test", auth::PRIV_SELECT, {"collection","default","public","items"}));
        ASSERT_TRUE(!am.check("test", auth::PRIV_INSERT, {"collection","default","public","items"}));

        // database
        database::DatabaseManager dbm(&store);
        ASSERT_TRUE(dbm.create_database("mydb", "admin").ok());
        ASSERT_TRUE(dbm.exists("mydb"));
    }
    std::filesystem::remove_all(dir);
    std::cout << "All tests passed!" << std::endl;
    return 0;
}
