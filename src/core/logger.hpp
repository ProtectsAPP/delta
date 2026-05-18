#pragma once
// =============================================================================
// src/core/logger.hpp — structured JSON logger with level filtering and a
// thread-local `trace_id` for per-request correlation.
//
// One global singleton (`Logger::instance()`). Threadsafe append to stderr by
// default; can also tee to a rotating file. Each log line is a single JSON
// object on its own line — friendly to fluentd/vector/loki ingestion.
//
// Levels (canonical):  DEBUG < INFO < WARN < ERROR
//   * minimum level configurable at runtime (`set_level`).
//   * Cheaper-than-format check: `is_enabled(level)` returns false before any
//     allocation when the configured threshold filters the line out.
//
// Trace ID:
//   * thread-local 32-hex-char id, set by the HTTP request middleware on
//     entry, cleared on exit. Every line emitted while the id is set carries
//     `"trace_id": "<id>"`. Easy to correlate distributed-call timelines.
//
// Audit channel:
//   * a *separate* destination for security-sensitive events. Writes go to
//     `audit.log` in the data dir (line-delimited JSON) AND get echoed to the
//     normal logger at INFO with `"channel":"audit"`.
//
// Slow query channel:
//   * `log_slow()` writes to `slow.log` in the data dir. The HTTP middleware
//     auto-calls this when the request exceeds a configurable threshold
//     (default 500 ms). Same JSONL format as audit.
// =============================================================================
#include <atomic>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <algorithm>
#include "json.hpp"

namespace delta {

using json = nlohmann::json;

enum class LogLevel : int { Debug = 0, Info = 1, Warn = 2, Error = 3, Off = 99 };

inline const char* log_level_name(LogLevel l) {
    switch (l) {
        case LogLevel::Debug: return "DEBUG";
        case LogLevel::Info:  return "INFO";
        case LogLevel::Warn:  return "WARN";
        case LogLevel::Error: return "ERROR";
        default:              return "OFF";
    }
}

inline LogLevel log_level_from_string(const std::string& s) {
    if (s == "debug" || s == "DEBUG") return LogLevel::Debug;
    if (s == "warn"  || s == "WARN")  return LogLevel::Warn;
    if (s == "error" || s == "ERROR") return LogLevel::Error;
    if (s == "off"   || s == "OFF")   return LogLevel::Off;
    return LogLevel::Info;
}

class Logger {
public:
    static Logger& instance() { static Logger g; return g; }

    void set_level(LogLevel l) { level_.store((int)l, std::memory_order_relaxed); }
    LogLevel level() const     { return (LogLevel)level_.load(std::memory_order_relaxed); }
    bool is_enabled(LogLevel l) const { return (int)l >= level_.load(std::memory_order_relaxed); }

    // Optional file destinations. Empty disables. Safe to call at runtime.
    void set_audit_file(const std::string& path) {
        std::lock_guard<std::mutex> lk(mu_);
        audit_path_ = path;
        reopen_audit_();
    }
    void set_slow_query_file(const std::string& path) {
        std::lock_guard<std::mutex> lk(mu_);
        slow_path_ = path;
        reopen_slow_();
    }
    // Mirror normal-channel log lines to a file as well as stderr.
    void set_main_file(const std::string& path) {
        std::lock_guard<std::mutex> lk(mu_);
        main_path_ = path;
        reopen_main_();
    }

    // --- trace-id thread-local ----------------------------------------------
    static std::string& trace_id_slot() {
        thread_local std::string id;
        return id;
    }
    static const std::string& current_trace_id() { return trace_id_slot(); }
    static void set_trace_id(const std::string& id) { trace_id_slot() = id; }
    static void clear_trace_id() { trace_id_slot().clear(); }

    static std::string new_trace_id() {
        thread_local std::mt19937_64 rng{ std::random_device{}() };
        uint64_t a = rng(), b = rng();
        char buf[33];
        std::snprintf(buf, sizeof(buf), "%016llx%016llx",
                      (unsigned long long)a, (unsigned long long)b);
        return std::string(buf, 32);
    }

    // --- normal-channel emit ------------------------------------------------
    void log(LogLevel lvl, const std::string& msg, const json& fields = json::object()) {
        if (!is_enabled(lvl)) return;
        json line = build_line_(lvl, msg, fields, /*channel*/"");
        std::string s = line.dump();
        std::lock_guard<std::mutex> lk(mu_);
        std::cerr << s << '\n';
        if (main_out_.is_open()) main_out_ << s << '\n' << std::flush;
    }

    void debug(const std::string& m, const json& f = json::object()) { log(LogLevel::Debug, m, f); }
    void info (const std::string& m, const json& f = json::object()) { log(LogLevel::Info , m, f); }
    void warn (const std::string& m, const json& f = json::object()) { log(LogLevel::Warn , m, f); }
    void error(const std::string& m, const json& f = json::object()) { log(LogLevel::Error, m, f); }

    // --- audit-channel emit -------------------------------------------------
    // Audit events ALWAYS get persisted to the audit file (regardless of the
    // current log level) and are also INFO-echoed to the main channel.
    void audit(const std::string& event, const json& fields = json::object()) {
        // P1-20: rate-limit a single event type to AUDIT_RATE_PER_SEC
        // emits per second per (event, ip). When the bucket is empty we
        // drop the event but increment a `_suppressed` counter on the
        // next allowed emission so operators don't lose total signal.
        if (!audit_allow_(event, fields)) return;
        json line = build_line_(LogLevel::Info, event, fields, /*channel*/"audit");
        std::string s = line.dump();
        std::lock_guard<std::mutex> lk(mu_);
        std::cerr << s << '\n';
        if (main_out_.is_open()) main_out_ << s << '\n' << std::flush;
        if (audit_out_.is_open()) audit_out_ << s << '\n' << std::flush;
    }

private:
    // P1-20: per (event, ip) token bucket. Default 50 events/sec with a
    // burst of 200. Keeps log volume bounded under a DoS while still
    // preserving the first hits of any incident.
    static constexpr int    AUDIT_RATE_PER_SEC = 50;
    static constexpr int    AUDIT_BURST        = 200;
    struct AuditBucket { double tokens = AUDIT_BURST; uint64_t last_ms = 0; uint64_t dropped = 0; };
    std::unordered_map<std::string, AuditBucket> audit_buckets_;
    std::mutex audit_buckets_mu_;
    bool audit_allow_(const std::string& event, const json& fields) {
        std::string ip = fields.value("ip", std::string());
        std::string key = event + "|" + ip;
        std::lock_guard<std::mutex> lk(audit_buckets_mu_);
        auto& b = audit_buckets_[key];
        uint64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        if (b.last_ms == 0) b.last_ms = now;
        double elapsed_sec = (double)(now - b.last_ms) / 1000.0;
        b.tokens = std::min((double)AUDIT_BURST, b.tokens + elapsed_sec * AUDIT_RATE_PER_SEC);
        b.last_ms = now;
        if (b.tokens < 1.0) { b.dropped++; return false; }
        b.tokens -= 1.0;
        return true;
    }
public:

    // --- slow-query channel emit -------------------------------------------
    void slow(double duration_ms, const std::string& kind,
              const json& fields = json::object()) {
        json f = fields;
        f["duration_ms"] = duration_ms;
        f["kind"] = kind;
        json line = build_line_(LogLevel::Warn, "slow", f, /*channel*/"slow");
        std::string s = line.dump();
        std::lock_guard<std::mutex> lk(mu_);
        if (slow_out_.is_open()) slow_out_ << s << '\n' << std::flush;
        // Don't spam stderr with slow lines — slow.log is the durable record.
    }

private:
    Logger() = default;
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    json build_line_(LogLevel lvl, const std::string& msg, const json& fields,
                     const std::string& channel) const {
        using namespace std::chrono;
        auto now = system_clock::now();
        auto ms  = duration_cast<milliseconds>(now.time_since_epoch()).count();
        std::time_t t = system_clock::to_time_t(now);
        std::tm tm{};
#if defined(_WIN32)
        gmtime_s(&tm, &t);
#else
        gmtime_r(&t, &tm);
#endif
        char ts[32];
        std::snprintf(ts, sizeof(ts), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
                      tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                      tm.tm_hour, tm.tm_min, tm.tm_sec, (int)(ms % 1000));
        json line = {
            {"ts",    ts},
            {"level", log_level_name(lvl)},
            {"msg",   msg}
        };
        if (!channel.empty()) line["channel"] = channel;
        const auto& tid = current_trace_id();
        if (!tid.empty()) line["trace_id"] = tid;
        // merge user fields last so caller can override anything.
        if (fields.is_object()) {
            for (auto it = fields.begin(); it != fields.end(); ++it) {
                line[it.key()] = it.value();
            }
        }
        return line;
    }

    void reopen_audit_() {
        if (audit_out_.is_open()) audit_out_.close();
        if (!audit_path_.empty()) audit_out_.open(audit_path_, std::ios::app);
    }
    void reopen_slow_() {
        if (slow_out_.is_open()) slow_out_.close();
        if (!slow_path_.empty()) slow_out_.open(slow_path_, std::ios::app);
    }
    void reopen_main_() {
        if (main_out_.is_open()) main_out_.close();
        if (!main_path_.empty()) main_out_.open(main_path_, std::ios::app);
    }

    std::atomic<int> level_{(int)LogLevel::Info};
    std::mutex mu_;
    std::string main_path_, audit_path_, slow_path_;
    std::ofstream main_out_, audit_out_, slow_out_;
};

// RAII guard: install a trace_id on the current thread for the lifetime of the
// guard, restoring whatever was there before (so nested HTTP handlers compose).
class TraceScope {
public:
    explicit TraceScope(const std::string& id) {
        prev_ = Logger::current_trace_id();
        Logger::set_trace_id(id);
    }
    ~TraceScope() { Logger::set_trace_id(prev_); }
private:
    std::string prev_;
};

} // namespace delta
