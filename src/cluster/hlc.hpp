// SPDX-License-Identifier: MIT
//
// Hybrid Logical Clock (HLC) — B.2 multi-master writes.
//
// We need a clock that:
//   1. Is monotonic on a single node even if the wall clock moves backward
//      (NTP drift, VM migration, time-zone change).
//   2. Captures causal order across nodes — given two events e1 and e2 on
//      different nodes, if e1 → e2 (one happens-before the other), then
//      hlc(e1) < hlc(e2).
//   3. Tracks closely with the physical wall clock so an operator looking
//      at an HLC string can roughly tell when the event occurred.
//
// HLC, introduced by Kulkarni et al. (2014, "Logical Physical Clocks and
// Consistent Snapshots in Globally Distributed Databases"), satisfies
// all three by combining a 48-bit physical millisecond counter with a
// 16-bit logical tie-breaker:
//
//     +------------------- 64 bits -------------------+
//     |   48-bit physical (ms)    |  16-bit counter   |
//     +-------------------------------------------------+
//
// The encoded value is rendered as a fixed-width 16-character hex string
// so lexicographic ordering matches numeric ordering — handy for
// LSM-keyspace scans (`hlc_index/<hlc_hex>` → doc-id).
//
// Concurrency: `now()` and `update()` take an internal mutex and are
// thread-safe. They run in O(1) so even hot paths (every collection
// write under `multi_master=true`) are unaffected by lock contention.
//
// Persistence: callers should treat the rendered string as opaque. The
// only stable contract is that `parse(render(x)) == x` and that the
// strings sort identically to the underlying 64-bit integers.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <string>

namespace delta {
namespace cluster {

class HybridLogicalClock {
public:
    static constexpr uint64_t COUNTER_MASK = 0xFFFFull;     // 16 bits
    static constexpr uint64_t COUNTER_MAX  = COUNTER_MASK;
    static constexpr uint64_t PHYSICAL_SHIFT = 16;

    // Read current wall time in milliseconds. Exposed so tests can stub
    // it; the production code uses a steady-style adjusted clock that
    // respects monotonicity within the process.
    using NowFn = uint64_t(*)();
    static uint64_t default_now_ms() {
        using namespace std::chrono;
        return (uint64_t)duration_cast<milliseconds>(
            system_clock::now().time_since_epoch()).count();
    }

    HybridLogicalClock() : now_ms_(default_now_ms_wrap), state_(0) {}
    explicit HybridLogicalClock(NowFn fn) : now_ms_(fn), state_(0) {}

    // Generate a fresh HLC for an event on THIS node.
    uint64_t tick() {
        std::lock_guard<std::mutex> lk(mu_);
        uint64_t phys = now_ms_();
        uint64_t cur_phys = state_ >> PHYSICAL_SHIFT;
        if (phys > cur_phys) {
            state_ = phys << PHYSICAL_SHIFT;
        } else {
            // wall-clock didn't move OR went backwards → bump counter.
            uint64_t cur_cnt = state_ & COUNTER_MASK;
            if (cur_cnt + 1 > COUNTER_MAX) {
                // 65k events in the same ms — incredibly unlikely on
                // realistic hardware but we still need to behave. Bump
                // the physical part instead, sacrificing strict wall-
                // clock alignment for monotonicity.
                state_ = (cur_phys + 1) << PHYSICAL_SHIFT;
            } else {
                state_ = (cur_phys << PHYSICAL_SHIFT) | (cur_cnt + 1);
            }
        }
        return state_;
    }

    // Update on receipt of a remote HLC. Returns the merged value that
    // should be stamped on whatever caused the update (the response, the
    // applied write, etc.). Strictly greater than both inputs.
    uint64_t update(uint64_t remote) {
        std::lock_guard<std::mutex> lk(mu_);
        uint64_t phys = now_ms_();
        uint64_t cur_phys = state_  >> PHYSICAL_SHIFT;
        uint64_t cur_cnt  = state_  &  COUNTER_MASK;
        uint64_t rem_phys = remote  >> PHYSICAL_SHIFT;
        uint64_t rem_cnt  = remote  &  COUNTER_MASK;
        uint64_t max_phys = std::max({phys, cur_phys, rem_phys});
        uint64_t new_cnt;
        if (max_phys == cur_phys && max_phys == rem_phys) {
            new_cnt = std::max(cur_cnt, rem_cnt) + 1;
        } else if (max_phys == cur_phys) {
            new_cnt = cur_cnt + 1;
        } else if (max_phys == rem_phys) {
            new_cnt = rem_cnt + 1;
        } else {
            new_cnt = 0;
        }
        if (new_cnt > COUNTER_MAX) { max_phys += 1; new_cnt = 0; }
        state_ = (max_phys << PHYSICAL_SHIFT) | (new_cnt & COUNTER_MASK);
        return state_;
    }

    static std::string render(uint64_t v) {
        // 16-char zero-padded hex.
        static const char hex[] = "0123456789abcdef";
        std::string s(16, '0');
        for (int i = 15; i >= 0; --i) {
            s[i] = hex[v & 0xF];
            v >>= 4;
        }
        return s;
    }
    static uint64_t parse(const std::string& s) {
        if (s.size() != 16) throw std::invalid_argument("hlc: bad length");
        uint64_t v = 0;
        for (char c : s) {
            v <<= 4;
            if      (c >= '0' && c <= '9') v |= (uint64_t)(c - '0');
            else if (c >= 'a' && c <= 'f') v |= (uint64_t)(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F') v |= (uint64_t)(c - 'A' + 10);
            else throw std::invalid_argument("hlc: bad hex");
        }
        return v;
    }

    // Convenience: tick() + render in one call.
    std::string now_string() { return render(tick()); }

    // Snapshot of current internal state without advancing it. Mostly
    // useful for tests and /metrics introspection.
    uint64_t peek() const {
        std::lock_guard<std::mutex> lk(mu_);
        return state_;
    }

private:
    static uint64_t default_now_ms_wrap() { return default_now_ms(); }

    NowFn now_ms_;
    mutable std::mutex mu_;
    uint64_t state_;  // (phys << 16) | counter
};

}  // namespace cluster
}  // namespace delta
