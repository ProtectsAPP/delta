// =============================================================================
// test_admin_reload.cpp — regression test for DatabaseManager::reload() and
// AuthManager::reload(), the in-memory-cache rehydration hooks that the
// /api/v1/admin/restore endpoint relies on.
//
// Without these reloads, a freshly restored process would still serve the
// old (empty) databases / users map and the operator would have to bounce
// the binary to see their data — defeats the purpose of online restore.
//
// We don't spin up the HTTP server here. We exercise the managers directly:
//   1) seed an LSMTree under temp dir A with a fresh admin DBM/Auth pair.
//   2) write some DB rows and a non-system user.
//   3) clone the keys into temp dir B by walking prefix_scan + apply_replicated
//      (mimicking what /admin/restore does internally).
//   4) point fresh DBM/Auth at B → reload() → assert everything shows up.
// =============================================================================
#include "../../src/storage/lsm_tree.hpp"
#include "../../src/database/database_manager.hpp"
#include "../../src/auth/auth_manager.hpp"

#include <cassert>
#include <filesystem>
#include <iostream>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

static std::string scratch(const std::string& tag) {
    std::string p = "./test_admin_reload_" + tag + "_" + std::to_string(now_ms());
    std::filesystem::remove_all(p);
    std::filesystem::create_directories(p);
    return p;
}

int main() {
    // ---- SOURCE deployment --------------------------------------------------
    std::string dir_src = scratch("src");
    {
        storage::LSMTree store(dir_src);
        database::DatabaseManager dbm(&store);
        auth::AuthManager         auth(&store);

        auto st1 = dbm.create_database("shop",     "admin", json::object());
        auto st2 = dbm.create_database("warehouse","admin", json::object());
        auto st3 = auth.create_user("alice", "wonderland", json::object());
        CHECK(st1.ok());
        CHECK(st2.ok());
        CHECK(st3.ok());
        // Writes are durable on WAL append; no explicit flush needed here.
    }

    // ---- "backup" → "restore" → DESTINATION deployment ----------------------
    std::string dir_dst = scratch("dst");
    {
        // First open with empty managers (they'll provision system defaults).
        storage::LSMTree store_src(dir_src);
        storage::LSMTree store_dst(dir_dst);
        database::DatabaseManager dbm_dst(&store_dst);   // only has "default"
        auth::AuthManager         auth_dst(&store_dst);  // only has admin

        // Sanity precondition: "shop" not yet visible on the destination.
        CHECK(dbm_dst.list_databases().size() <= 2);     // default + delta_system
        bool alice_pre = false;
        for (auto& u : auth_dst.list_users()) if (u.username == "alice") alice_pre = true;
        CHECK(!alice_pre);

        // Mimic /admin/restore: pull every key/value out of src, replay into dst.
        uint64_t lsn = store_src.current_lsn();
        size_t  copied = 0;
        for (auto& [k, v] : store_src.prefix_scan("", 100000000)) {
            store_dst.apply_replicated(lsn, k, v, /*tombstone=*/false);
            ++copied;
        }
        CHECK(copied > 0);

        // Without reload(), the in-memory caches on dst still don't know about
        // the keys we just wrote. With reload(), they do.
        dbm_dst.reload();
        auth_dst.reload();

        bool saw_shop = false, saw_warehouse = false;
        for (auto& d : dbm_dst.list_databases()) {
            if (d.name == "shop")      saw_shop = true;
            if (d.name == "warehouse") saw_warehouse = true;
        }
        CHECK(saw_shop);
        CHECK(saw_warehouse);

        bool saw_alice = false;
        for (auto& u : auth_dst.list_users()) if (u.username == "alice") saw_alice = true;
        CHECK(saw_alice);

        // System defaults must still be intact (ensure_default / ensure_system
        // is the part of reload() that runs *after* the cache clear; missing
        // it would orphan the admin account).
        bool saw_admin = false;
        for (auto& u : auth_dst.list_users()) if (u.username == "admin") saw_admin = true;
        CHECK(saw_admin);
    }

    std::filesystem::remove_all(dir_src);
    std::filesystem::remove_all(dir_dst);
    std::cout << "test_admin_reload: OK" << std::endl;
    return 0;
}
