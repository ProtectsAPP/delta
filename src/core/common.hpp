#pragma once
#include <string>
#include <vector>
#include <chrono>
#include <random>
#include <sstream>
#include <iomanip>
#include <mutex>
#include <atomic>
#include "json.hpp"

namespace delta {

using json = nlohmann::json;

struct Status {
    enum Code { OK = 0, ERROR = 1, NOT_FOUND = 2, DUPLICATE = 3, INVALID = 4, UNAUTHORIZED = 5, FORBIDDEN = 6, INTERNAL = 7, CONFLICT = 8 };
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
};

inline uint64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

inline uint64_t now_sec() { return now_ms() / 1000; }

inline std::string gen_id() {
    static std::atomic<uint64_t> counter{0};
    static std::mt19937_64 rng(std::random_device{}());
    uint64_t ts = now_ms();
    uint64_t r = rng();
    uint64_t c = counter.fetch_add(1);
    std::stringstream ss;
    ss << std::hex << std::setw(12) << std::setfill('0') << ts
       << std::setw(8) << std::setfill('0') << (c & 0xFFFFFFFF)
       << std::setw(12) << std::setfill('0') << (r & 0xFFFFFFFFFFFF);
    return ss.str();
}

inline std::string random_hex(size_t bytes) {
    static thread_local std::mt19937_64 rng(std::random_device{}());
    std::stringstream ss;
    for (size_t i = 0; i < bytes; ++i) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (rng() & 0xFF);
    }
    return ss.str();
}

} // namespace delta
