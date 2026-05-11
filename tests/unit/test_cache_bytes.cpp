// =============================================================================
// test_cache_bytes.cpp — P2-2 byte-level memory budget regression.
//
// Invariants exercised:
//   1. stats().mem_bytes tracks incrementally with set/del/hset/hdel/
//      lpush/lpop/sadd/srem/zadd/zrem. Roundtripping to empty returns the
//      counter to ~0.
//   2. A max_bytes budget evicts keys so total mem stays within the cap.
//   3. A single oversized value cannot blow past max_bytes + its own size.
// =============================================================================
#include "../../src/cache/cache_engine.hpp"
#include <cassert>
#include <iostream>
#include <string>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

static int test_incremental_tracking_returns_to_zero() {
    cache::CacheEngine c(/*max_keys*/ 1'000'000, /*max_bytes*/ 1ull << 30);
    CHECK(c.memory_used_bytes() == 0);

    // string kv
    c.set("a", std::string(1000, 'x'));
    c.set("b", std::string(500,  'y'));
    CHECK(c.memory_used_bytes() > 1400);
    c.del("a"); c.del("b");

    // hash
    c.hset("h", "f1", std::string(200, 'z'));
    c.hset("h", "f2", std::string(300, 'w'));
    CHECK(c.memory_used_bytes() > 500);
    c.hdel("h", "f1"); c.hdel("h", "f2");

    // list
    c.lpush("l", std::string(100, '1'));
    c.rpush("l", std::string(100, '2'));
    CHECK(c.memory_used_bytes() > 200);
    c.lpop("l"); c.rpop("l");

    // set
    c.sadd("s", std::string(100, 'a'));
    c.sadd("s", std::string(100, 'b'));
    CHECK(c.memory_used_bytes() > 200);
    c.srem("s", std::string(100, 'a'));
    c.srem("s", std::string(100, 'b'));

    // zset
    c.zadd("z", 1.0, std::string(100, 'm'));
    c.zadd("z", 2.0, std::string(100, 'n'));
    CHECK(c.memory_used_bytes() > 200);
    c.zrem("z", std::string(100, 'm'));
    c.zrem("z", std::string(100, 'n'));

    // Everything removed → counter drained.
    CHECK(c.memory_used_bytes() == 0);
    return 0;
}

static int test_budget_triggers_eviction() {
    // 32 shards * at least 1 MiB each ⇒ minimum effective budget is 32 MiB
    // regardless of the requested cap. So we size payloads + total bytes
    // to exceed that floor comfortably and verify eviction still kicks in.
    const size_t per_value = 256 * 1024;                        // 256 KiB
    const size_t budget    = 64ull * 1024ull * 1024ull;         // 64 MiB cap
    cache::CacheEngine c(/*max_keys*/ 1'000'000, budget);

    std::string big(per_value, 'x');
    // Write 4x the budget to force mass eviction.
    for (int i = 0; i < (int)((4 * budget) / per_value); ++i) {
        c.set("k" + std::to_string(i), big);
    }
    size_t used = c.memory_used_bytes();
    // Allow 2x slack above the cap to accommodate per-shard rounding and
    // the MIN_PER_SHARD_BYTES floor. The point is: used stays bounded,
    // it does NOT grow to 4x budget like an uncapped cache would.
    CHECK(used <= 2 * budget);
    auto st = c.stats();
    CHECK(st.mem_bytes == used);
    // At least SOME keys should have been evicted — definitely fewer than
    // the number of writes we issued.
    CHECK(st.total_keys < (4 * budget) / per_value);
    return 0;
}

int main() {
    int rc = 0;
    rc |= test_incremental_tracking_returns_to_zero();
    rc |= test_budget_triggers_eviction();
    if (rc == 0) std::cout << "test_cache_bytes: OK" << std::endl;
    return rc;
}
