// =============================================================================
// test_validation.cpp — input-validator coverage (P1-20).
// =============================================================================
#include "../../src/core/validation.hpp"
#include <iostream>

using namespace delta::validation;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

static int test_identifier() {
    CHECK(is_valid_identifier("abc"));
    CHECK(is_valid_identifier("_abc123"));
    CHECK(!is_valid_identifier(""));
    CHECK(!is_valid_identifier("1abc"));
    CHECK(!is_valid_identifier("ab-c"));
    CHECK(!is_valid_identifier("ab c"));
    CHECK(!is_valid_identifier(std::string(65, 'a')));
    return 0;
}

static int test_collection_name() {
    CHECK(is_valid_collection_name("users"));
    CHECK(!is_valid_collection_name("system"));   // reserved
    CHECK(!is_valid_collection_name("1"));
    return 0;
}

static int test_password() {
    CHECK(!is_valid_password("short"));
    CHECK(is_valid_password("hunter22"));
    CHECK(!is_valid_password(std::string(129, 'x')));
    return 0;
}

static int test_safe_int() {
    CHECK(safe_stoi("42") == 42);
    CHECK(safe_stoi("not-a-number", 7) == 7);
    CHECK(safe_stoi("", 9) == 9);
    CHECK(safe_stoll("9999999999", 0) == 9999999999LL);
    CHECK(safe_stoull("18446744073709551610") != 0);
    CHECK(safe_stoull("garbage", 5) == 5);
    return 0;
}

int main() {
    int rc = 0;
    rc |= test_identifier();
    rc |= test_collection_name();
    rc |= test_password();
    rc |= test_safe_int();
    if (rc == 0) std::cout << "test_validation: OK" << std::endl;
    return rc;
}
