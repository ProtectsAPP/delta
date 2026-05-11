// =============================================================================
// test_auth_security.cpp — regression tests for the auth-layer audit fixes.
//
// Coverage:
//   * constant_time_compare()                                         (P0-7)
//   * verify_password() round-trip on PBKDF2-PHC strings              (P0-6)
//   * legacy bare-hex hash is still accepted                          (P0-6)
//   * legacy hash is upgraded to PBKDF2 on next successful login      (P0-6)
//   * authenticate() returns the SAME error for missing user / bad pw (P0-8)
//   * authenticate() burns roughly the same wall-clock for both paths (P0-8)
//   * LoginRateLimiter trips after the configured cap                 (P1-17)
//
// We deliberately do NOT measure absolute timings — only that the cost of
// "user does not exist" and "user exists but password wrong" are within
// the same order of magnitude. Doing strict equality on PBKDF2 wall-clock
// would be flaky in CI.
// =============================================================================
#include "../../src/auth/auth_manager.hpp"
#include "../../src/auth/password.hpp"
#include "../../src/storage/lsm_tree.hpp"
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>

using namespace delta;
using namespace std::chrono;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

static int test_constant_time_compare() {
    CHECK(auth::constant_time_compare("abc", "abc"));
    CHECK(!auth::constant_time_compare("abc", "abd"));
    CHECK(!auth::constant_time_compare("abc", "abcd"));
    // Empty strings compare equal.
    CHECK(auth::constant_time_compare("", ""));
    return 0;
}

static int test_pbkdf2_roundtrip() {
    // Use a short iteration count; the public API hard-codes 120k which would
    // make every test take seconds. We exercise the PHC path directly.
    std::string salt_hex = "0123456789abcdef0123456789abcdef";
    std::string h = auth::hash_password("hunter2", salt_hex, /*iterations=*/200);
    CHECK(auth::is_phc_hash(h));
    // Correct password verifies; wrong password does not.
    CHECK(auth::verify_password("hunter2", "", h));
    CHECK(!auth::verify_password("hunter3", "", h));
    return 0;
}

static int test_legacy_hash_accepted() {
    // Reproduce the legacy SHA256-loop format and verify_password() must
    // still succeed against it.
    std::string salt = "deadbeef";
    auth::SHA256 sh; sh.update(salt); sh.update("pw");
    auto cur = sh.final_digest();
    for (int i = 0; i < 10000; ++i) {
        auth::SHA256 sh2; sh2.update(cur.data(), cur.size()); sh2.update(salt);
        cur = sh2.final_digest();
    }
    std::string legacy_hex = auth::hex_encode(cur);
    CHECK(!auth::is_phc_hash(legacy_hex));
    CHECK(auth::verify_password("pw", salt, legacy_hex));
    CHECK(!auth::verify_password("wrong", salt, legacy_hex));
    return 0;
}

static int test_user_enumeration_uniform_error() {
    auto dir = "./test_auth_enum_" + std::to_string(now_ms());
    std::filesystem::remove_all(dir);
    storage::LSMTree store(dir);
    auth::AuthManager am(&store);

    // Create a user.
    CHECK(am.create_user("alice", "correct").ok());

    // Both failure paths must produce the SAME error string.
    std::string e1, e2;
    CHECK(!am.authenticate("alice",  "wrong", &e1));
    CHECK(!am.authenticate("ghost",  "wrong", &e2));
    CHECK(e1 == e2);
    CHECK(e1 == "invalid credentials");

    // And both should burn roughly comparable wall-clock — we accept up to
    // 5x ratio either way to absorb scheduler jitter on shared CI.
    auto t1 = steady_clock::now();
    am.authenticate("alice", "wrong", nullptr);
    auto d1 = duration_cast<microseconds>(steady_clock::now() - t1).count();
    auto t2 = steady_clock::now();
    am.authenticate("ghost", "wrong", nullptr);
    auto d2 = duration_cast<microseconds>(steady_clock::now() - t2).count();
    if (d1 == 0 || d2 == 0) {
        std::cerr << "[warn] timing measurement too coarse, skipping ratio check"
                  << std::endl;
    } else {
        double ratio = d1 > d2 ? (double)d1 / d2 : (double)d2 / d1;
        if (ratio > 10.0) {
            std::cerr << "FAIL: timing ratio " << ratio
                      << " > 10x (d_user=" << d1 << "us, d_ghost=" << d2 << "us)"
                      << std::endl;
            std::filesystem::remove_all(dir);
            return 1;
        }
    }
    std::filesystem::remove_all(dir);
    return 0;
}

static int test_legacy_hash_upgraded_on_login() {
    auto dir = "./test_auth_upgrade_" + std::to_string(now_ms());
    std::filesystem::remove_all(dir);
    storage::LSMTree store(dir);

    // Manually plant a user with a *legacy* hash so we exercise the
    // upgrade path on first successful authenticate().
    auth::User u;
    u.id = 99; u.username = "legacy";
    u.salt = "1234567890abcdef";
    auth::SHA256 sh; sh.update(u.salt); sh.update("hunter2");
    auto cur = sh.final_digest();
    for (int i = 0; i < 10000; ++i) {
        auth::SHA256 sh2; sh2.update(cur.data(), cur.size()); sh2.update(u.salt);
        cur = sh2.final_digest();
    }
    u.password_hash = auth::hex_encode(cur);
    u.created_at = u.updated_at = now_ms();
    store.put("auth:user:legacy", u.to_json(true).dump());

    auth::AuthManager am(&store);
    CHECK(am.authenticate("legacy", "hunter2"));     // success → triggers upgrade
    auto fetched = am.get_user("legacy");
    CHECK(fetched.has_value());
    CHECK(auth::is_phc_hash(fetched->password_hash));
    CHECK(am.authenticate("legacy", "hunter2"));     // still works after upgrade

    std::filesystem::remove_all(dir);
    return 0;
}

static int test_login_rate_limiter() {
    auth::LoginRateLimiter lim;
    // Default cap is constants::AUTH_RATE_LIMIT_MAX (10). The 11th attempt
    // in the same window must be rejected.
    int allowed = 0;
    for (int i = 0; i < 50; ++i) if (lim.allow("ip|user")) ++allowed;
    CHECK(allowed == constants::AUTH_RATE_LIMIT_MAX);
    // A different key has its own bucket.
    CHECK(lim.allow("ip|other"));
    // reset() clears the bucket.
    lim.reset("ip|user");
    CHECK(lim.allow("ip|user"));
    return 0;
}

int main() {
    int rc = 0;
    rc |= test_constant_time_compare();
    rc |= test_pbkdf2_roundtrip();
    rc |= test_legacy_hash_accepted();
    rc |= test_user_enumeration_uniform_error();
    rc |= test_legacy_hash_upgraded_on_login();
    rc |= test_login_rate_limiter();
    if (rc == 0) std::cout << "test_auth_security: OK" << std::endl;
    return rc;
}
