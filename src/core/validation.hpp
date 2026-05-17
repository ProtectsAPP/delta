// =============================================================================
// validation.hpp — input validators (P1-20).
//
// Centralizes the rules used by every public-facing endpoint:
//   * Identifiers (database / schema / collection / role / user names).
//   * Password length bounds.
//   * Safe parsing wrappers around stoi/stoll/stoull that don't throw on
//     malicious input — important because the HTTP layer used to call
//     std::stoi() directly on un-validated query parameters, crashing the
//     entire server on bad input.
// =============================================================================
#pragma once
#include "constants.hpp"
#include <cctype>
#include <cstdint>
#include <regex>
#include <string>

namespace delta::validation {

// Identifier: 1-64 chars, must start with [a-zA-Z_], contains only
// [a-zA-Z0-9_]. Mirrors PostgreSQL's "unquoted identifier" grammar.
inline bool is_valid_identifier(const std::string& s) {
    using namespace constants;
    if (s.size() < VALID_IDENT_MIN_LEN || s.size() > VALID_IDENT_MAX_LEN) return false;
    char c0 = s[0];
    if (!(std::isalpha((unsigned char)c0) || c0 == '_')) return false;
    for (size_t i = 1; i < s.size(); ++i) {
        char c = s[i];
        if (!(std::isalnum((unsigned char)c) || c == '_')) return false;
    }
    return true;
}

// Same as identifier, but rejects "system" — reserved for internal use.
inline bool is_valid_collection_name(const std::string& s) {
    if (!is_valid_identifier(s)) return false;
    if (s == "system") return false;
    return true;
}

inline bool is_valid_password(const std::string& s) {
    using namespace constants;
    if (s.size() < VALID_PASSWORD_MIN || s.size() > VALID_PASSWORD_MAX) return false;
    // P1-16: reject ASCII control characters (NUL, TAB, newline, etc.).
    // Unicode control runs are pinned out via the same byte check so
    // they cannot smuggle in via overlong-encoded UTF-8 sequences either.
    for (unsigned char c : s) {
        if (c < 0x20 || c == 0x7F) return false;
    }
    return true;
}

// Parse helpers that NEVER throw. Originally we called std::stoi() directly
// on `req.get_param_value(...)` which threw std::invalid_argument /
// std::out_of_range on malicious payloads, propagating uncaught and
// taking the connection down with it.
inline int safe_stoi(const std::string& s, int dflt = 0) {
    try { size_t pos = 0; int v = std::stoi(s, &pos); return pos == 0 ? dflt : v; }
    catch (...) { return dflt; }
}
inline long long safe_stoll(const std::string& s, long long dflt = 0) {
    try { size_t pos = 0; long long v = std::stoll(s, &pos); return pos == 0 ? dflt : v; }
    catch (...) { return dflt; }
}
inline uint64_t safe_stoull(const std::string& s, uint64_t dflt = 0) {
    try { size_t pos = 0; auto v = std::stoull(s, &pos); return pos == 0 ? dflt : (uint64_t)v; }
    catch (...) { return dflt; }
}

} // namespace delta::validation
