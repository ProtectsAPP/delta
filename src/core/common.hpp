#pragma once
#include <string>
#include <vector>
#include <chrono>
#include <random>
#include <sstream>
#include <iomanip>
#include <mutex>
#include <atomic>
#include <cstdio>
#include <cstdint>
#include "json.hpp"

namespace delta {

using json = nlohmann::json;

struct Status {
    // Code 501 lines up with HTTP semantics so the gateway can pass it
    // through verbatim (Round 3: cross-shard transactions surface this).
    enum Code { OK = 0, ERROR = 1, NOT_FOUND = 2, DUPLICATE = 3, INVALID = 4,
                UNAUTHORIZED = 5, FORBIDDEN = 6, INTERNAL = 7, CONFLICT = 8,
                UNSUPPORTED = 501 };
    Code code = OK;
    std::string message;
    Status() = default;
    Status(Code c, std::string m = "") : code(c), message(std::move(m)) {}
    bool ok() const { return code == OK; }
    static Status Ok() { return Status(OK); }
    static Status NotFound(const std::string& m = "not found") { return Status(NOT_FOUND, m); }
    static Status Duplicate(const std::string& m = "duplicate") { return Status(DUPLICATE, m); }
    static Status Invalid(const std::string& m = "invalid") { return Status(INVALID, m); }
    static Status Unauth(const std::string& m = "unauthorized") { return Status(UNAUTHORIZED, m); }
    static Status Forbidden(const std::string& m = "forbidden") { return Status(FORBIDDEN, m); }
    // P1-6: optimistic-locking version mismatch / lost-update.
    static Status Conflict(const std::string& m = "conflict") { return Status(CONFLICT, m); }
    static Status Error(const std::string& m) { return Status(ERROR, m); }
    static Status Unsupported(const std::string& m = "unsupported") { return Status(UNSUPPORTED, m); }
};

inline uint64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

inline uint64_t now_sec() { return now_ms() / 1000; }

// P2-01 / P2-11: cryptographically secure random bytes. Backed by
// /dev/urandom (POSIX). We read in chunks; if the read fails for any
// reason we fall back to std::random_device which on Linux/macOS is
// also a CSPRNG.
inline void csprng_bytes(uint8_t* out, size_t n) {
    static thread_local FILE* fp = std::fopen("/dev/urandom", "rb");
    if (fp) {
        size_t got = std::fread(out, 1, n, fp);
        if (got == n) return;
        // partial read: top up with random_device
        for (size_t i = got; i < n; ++i) {
            static thread_local std::random_device rd;
            out[i] = (uint8_t)(rd() & 0xFF);
        }
        return;
    }
    static thread_local std::random_device rd;
    for (size_t i = 0; i < n; ++i) out[i] = (uint8_t)(rd() & 0xFF);
}

inline std::string gen_id() {
    static std::atomic<uint64_t> counter{0};
    uint64_t ts = now_ms();
    uint8_t rnd[6]; csprng_bytes(rnd, 6);
    uint64_t r = 0; for (int i = 0; i < 6; ++i) r = (r << 8) | rnd[i];
    uint64_t c = counter.fetch_add(1);
    std::stringstream ss;
    ss << std::hex << std::setw(12) << std::setfill('0') << ts
       << std::setw(8) << std::setfill('0') << (c & 0xFFFFFFFF)
       << std::setw(12) << std::setfill('0') << (r & 0xFFFFFFFFFFFF);
    return ss.str();
}

inline std::string random_hex(size_t bytes) {
    std::vector<uint8_t> buf(bytes);
    csprng_bytes(buf.data(), bytes);
    std::stringstream ss;
    for (size_t i = 0; i < bytes; ++i) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (unsigned)buf[i];
    }
    return ss.str();
}

} // namespace delta
