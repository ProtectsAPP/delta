// =============================================================================
// test_rls_expr.cpp — P1-22 RLS expression parser regression.
//
// We exercise the parser by routing real documents through
// check_rls_constraint() — that's the public path. The DatabaseManager's
// parse_simple_expr is private, but the policy-eval pipeline is the
// thing operators actually rely on, so it's the right surface to test.
// =============================================================================
#include "../../src/database/database_manager.hpp"
#include "../../src/storage/lsm_tree.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <set>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

static bool eval(database::DatabaseManager& dm, const std::string& expr,
                 const std::string& user, const json& doc) {
    database::RLSPolicy p;
    p.name = "t"; p.database = "default"; p.schema = "public";
    p.collection = "rows"; p.command = "SELECT"; p.enabled = true;
    p.using_expr = expr;
    p.with_check_expr = expr;
    dm.create_policy(p);
    dm.set_rls_enabled("default","public","rows", true);
    std::set<std::string> roles;
    bool ok = dm.check_rls_constraint(user, "default","public","rows",
                                      "SELECT", doc, roles);
    dm.drop_policy("default","public","rows","t");
    dm.set_rls_enabled("default","public","rows", false);
    return ok;
}

int main() {
    std::string dir = "./test_rls_" + std::to_string(now_ms());
    std::filesystem::remove_all(dir);
    storage::LSMTree store(dir);
    database::DatabaseManager dm(&store);

    // ---- equality with current_user ---------------------------------------
    CHECK( eval(dm, "owner = current_user", "alice", json{{"owner","alice"}}));
    CHECK(!eval(dm, "owner = current_user", "alice", json{{"owner","bob"}}));

    // ---- $user shorthand --------------------------------------------------
    CHECK( eval(dm, "owner = $user", "carol", json{{"owner","carol"}}));

    // ---- string literal ---------------------------------------------------
    CHECK( eval(dm, "status = 'active'", "x", json{{"status","active"}}));
    CHECK(!eval(dm, "status = 'active'", "x", json{{"status","paused"}}));

    // ---- numeric comparison ----------------------------------------------
    CHECK( eval(dm, "age >= 18", "x", json{{"age", 25}}));
    CHECK(!eval(dm, "age >= 18", "x", json{{"age", 12}}));

    // ---- IN ---------------------------------------------------------------
    CHECK( eval(dm, "region IN ('us','eu')", "x", json{{"region","eu"}}));
    CHECK(!eval(dm, "region IN ('us','eu')", "x", json{{"region","ap"}}));

    // ---- AND --------------------------------------------------------------
    CHECK( eval(dm, "owner = current_user AND status = 'active'",
                "alice", json{{"owner","alice"},{"status","active"}}));
    CHECK(!eval(dm, "owner = current_user AND status = 'active'",
                "alice", json{{"owner","alice"},{"status","paused"}}));

    // ---- OR + parens ------------------------------------------------------
    CHECK( eval(dm, "(owner = current_user) OR (visibility = 'public')",
                "alice", json{{"owner","bob"},{"visibility","public"}}));
    CHECK(!eval(dm, "(owner = current_user) OR (visibility = 'public')",
                "alice", json{{"owner","bob"},{"visibility","private"}}));

    // ---- !=  +  <> --------------------------------------------------------
    CHECK( eval(dm, "status != 'banned'", "x", json{{"status","active"}}));
    CHECK( eval(dm, "status <> 'banned'", "x", json{{"status","active"}}));
    CHECK(!eval(dm, "status != 'banned'", "x", json{{"status","banned"}}));

    std::filesystem::remove_all(dir);
    std::cout << "test_rls_expr: OK" << std::endl;
    return 0;
}
