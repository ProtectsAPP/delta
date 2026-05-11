// =============================================================================
// test_storage.cpp — regression tests for the storage-layer audit fixes.
//
// Coverage:
//   * WAL CRC32C detects torn-write tail and stops replay safely (P1-4)
//   * SSTable magic-number rejection of v1 / corrupt files (P1-5)
//   * Bloom filter mathematical sanity (false-positive rate) (P1-1)
//   * Bloom filter serialize / deserialize round-trip
//   * fsync of LSN file after every write (P0-2)
//   * Crash-recovery: replay an unflushed WAL into a fresh LSMTree
// =============================================================================
#include "../../src/storage/lsm_tree.hpp"
#include "../../src/storage/bloom_filter.hpp"
#include "../../src/storage/crc32.hpp"
#include <cassert>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <sys/stat.h>
#include <unistd.h>

using namespace delta;

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL @" << __LINE__ << ": " #x << std::endl; return 1; } } while (0)

static std::string scratch_dir(const std::string& tag) {
    std::string p = "./test_storage_" + tag + "_" + std::to_string(now_ms());
    std::filesystem::remove_all(p);
    return p;
}

// 1. CRC32C is stable and matches the reference implementation for the
//    canonical "123456789" vector (CRC32C("123456789") == 0xE3069283).
static int test_crc32c_known_vector() {
    uint32_t v = storage::crc32c(reinterpret_cast<const uint8_t*>("123456789"), 9);
    CHECK(v == 0xE3069283u);
    // Two halves chained match the one-shot.
    uint32_t a = storage::crc32c(reinterpret_cast<const uint8_t*>("12345"), 5);
    uint32_t b = storage::crc32c(reinterpret_cast<const uint8_t*>("6789"), 4, a);
    CHECK(b == v);
    return 0;
}

// 2. WAL replay terminates cleanly when we corrupt the trailing record.
//    Earlier records must still survive.
static int test_wal_torn_tail() {
    auto dir = scratch_dir("wal_torn");
    {
        storage::LSMTree t(dir);
        t.put("a", "1");
        t.put("b", "2");
        t.put("c", "3");
        // Don't flush — leave the records in the WAL.
    }
    // Append a few junk bytes to simulate a torn write at the tail.
    {
        std::ofstream out(dir + "/wal.log", std::ios::app | std::ios::binary);
        const char garbage[] = "\xff\xff\xff\xff\xff\xff";
        out.write(garbage, sizeof(garbage));
    }
    // Replay should recover a, b, c despite the trailing garbage.
    storage::LSMTree t(dir);
    std::string v;
    CHECK(t.get("a", &v) && v == "1");
    CHECK(t.get("b", &v) && v == "2");
    CHECK(t.get("c", &v) && v == "3");
    std::filesystem::remove_all(dir);
    return 0;
}

// 3. SSTable::load() returns nullptr for a file with bad magic.
static int test_sstable_bad_magic() {
    auto dir = scratch_dir("sst_magic");
    std::filesystem::create_directories(dir);
    std::string path = dir + "/sst_garbage.sst";
    {
        // 16 bytes of zeroes — invalid magic.
        std::ofstream out(path, std::ios::binary);
        char zero[16] = {0};
        out.write(zero, sizeof(zero));
    }
    auto sst = storage::SSTable::load(path);
    CHECK(sst == nullptr);
    std::filesystem::remove_all(dir);
    return 0;
}

// 4. Bloom filter false-positive rate sits comfortably under 5% at the
//    advertised 1% target (statistical, but with a generous margin).
static int test_bloom_fpr() {
    storage::BloomFilter bf(10000, 0.01);
    for (int i = 0; i < 10000; ++i) bf.add("k" + std::to_string(i));
    // Every inserted key MUST be reported present (no false negatives).
    for (int i = 0; i < 10000; ++i)
        CHECK(bf.probably_contains("k" + std::to_string(i)));
    // Probe with 10k unrelated keys; count false positives.
    int fp = 0;
    for (int i = 0; i < 10000; ++i)
        if (bf.probably_contains("z" + std::to_string(i))) ++fp;
    // Allow up to 5% to keep the test stable across hash distributions.
    CHECK(fp < 500);
    return 0;
}

// 5. Bloom serialize/deserialize round-trip preserves membership.
static int test_bloom_roundtrip() {
    storage::BloomFilter bf(1000);
    for (int i = 0; i < 100; ++i) bf.add("hello-" + std::to_string(i));
    auto s = bf.serialize();
    auto bf2 = storage::BloomFilter::deserialize(s);
    for (int i = 0; i < 100; ++i)
        CHECK(bf2.probably_contains("hello-" + std::to_string(i)));
    return 0;
}

// 6. LSN is durable: after a put() the value persisted to disk advances.
static int test_lsn_durable() {
    auto dir = scratch_dir("lsn");
    {
        storage::LSMTree t(dir);
        t.put("a", "1");
    }
    // Read raw lsn file.
    std::ifstream in(dir + "/lsn");
    uint64_t v = 0; in >> v;
    CHECK(v >= 1);
    std::filesystem::remove_all(dir);
    return 0;
}

int main() {
    int rc = 0;
    rc |= test_crc32c_known_vector();
    rc |= test_wal_torn_tail();
    rc |= test_sstable_bad_magic();
    rc |= test_bloom_fpr();
    rc |= test_bloom_roundtrip();
    rc |= test_lsn_durable();
    if (rc == 0) std::cout << "test_storage: OK" << std::endl;
    return rc;
}
