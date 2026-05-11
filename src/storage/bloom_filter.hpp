// =============================================================================
// bloom_filter.hpp — fixed-size Bloom filter for SSTable membership tests
// (P1-1).
//
// Why:
//   Before this, every `LSMTree::get(key)` walked sstables_ in reverse order
//   and called std::map::find on each one. A read miss on a key that doesn't
//   exist is therefore O(N_sst * log(M_sst)). After Bloom, a miss on a key
//   that the filter rejects is O(K) where K = hash_count_ (typically 7).
//
// Sizing:
//   Given expected entry count `n` and target false-positive rate `p`, we
//   pick the optimal number of bits and hash functions from the standard
//   formulas:
//       m = -n ln(p) / (ln(2))^2
//       k = m/n * ln(2)
//   At p = 1% the per-key cost is ~9.6 bits, k ≈ 7.
//
// Hashing:
//   We derive K hashes from two FNV-1a seeds via the Kirsch-Mitzenmacher
//   double-hashing trick (h_i(x) = h1(x) + i * h2(x)). Same fp-rate as K
//   independent hashes, half the wall-clock cost.
//
// Persistence:
//   `serialize()` produces a flat byte string that prepends:
//     [u32 bits] [u32 hash_count] [bytes...]
//   `deserialize()` round-trips it. The current SSTable format does NOT yet
//   embed bloom filters in the .sst file — they're rebuilt at flush time
//   and held in memory next to the index. That keeps the on-disk format
//   forward-compatible with v1 SSTables.
// =============================================================================
#pragma once
#include <cmath>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace delta::storage {

class BloomFilter {
public:
    BloomFilter() = default;

    // Build a Bloom filter sized for `expected_items` entries at the given
    // false-positive rate. expected_items == 0 produces an always-false
    // filter (probably_contains() always returns false), which is the
    // desired behavior for an empty SSTable.
    explicit BloomFilter(size_t expected_items, double fp_rate = 0.01) {
        if (expected_items == 0) {
            bits_ = 0; hash_count_ = 0; data_.clear(); return;
        }
        if (fp_rate <= 0 || fp_rate >= 1) fp_rate = 0.01;
        double ln2 = std::log(2.0);
        double m = -static_cast<double>(expected_items) * std::log(fp_rate) / (ln2 * ln2);
        bits_ = std::max<size_t>(64, static_cast<size_t>(std::ceil(m)));
        double k = (static_cast<double>(bits_) / expected_items) * ln2;
        hash_count_ = std::max<size_t>(1, std::min<size_t>(16, static_cast<size_t>(std::round(k))));
        data_.assign((bits_ + 7) / 8, 0);
    }

    void add(const std::string& key) {
        if (bits_ == 0) return;
        uint64_t h1 = fnv1a(key, 0xCBF29CE484222325ull);
        uint64_t h2 = fnv1a(key, 0x100000001B3ull);
        for (size_t i = 0; i < hash_count_; ++i) {
            uint64_t combined = h1 + i * h2;
            size_t bit = combined % bits_;
            data_[bit / 8] |= static_cast<uint8_t>(1u << (bit & 7));
        }
    }

    bool probably_contains(const std::string& key) const {
        if (bits_ == 0) return false;
        uint64_t h1 = fnv1a(key, 0xCBF29CE484222325ull);
        uint64_t h2 = fnv1a(key, 0x100000001B3ull);
        for (size_t i = 0; i < hash_count_; ++i) {
            uint64_t combined = h1 + i * h2;
            size_t bit = combined % bits_;
            if ((data_[bit / 8] & (1u << (bit & 7))) == 0) return false;
        }
        return true;
    }

    size_t bits()       const { return bits_; }
    size_t hash_count() const { return hash_count_; }
    size_t bytes()      const { return data_.size(); }
    bool   empty()      const { return bits_ == 0; }

    // Serialize layout: [u32 bits][u32 hash_count][raw bytes].
    std::string serialize() const {
        std::string out;
        out.resize(8 + data_.size());
        uint32_t b = static_cast<uint32_t>(bits_);
        uint32_t k = static_cast<uint32_t>(hash_count_);
        std::memcpy(out.data() + 0, &b, 4);
        std::memcpy(out.data() + 4, &k, 4);
        if (!data_.empty()) std::memcpy(out.data() + 8, data_.data(), data_.size());
        return out;
    }

    static BloomFilter deserialize(const std::string& s) {
        if (s.size() < 8) throw std::runtime_error("bloom filter blob too short");
        uint32_t b, k;
        std::memcpy(&b, s.data() + 0, 4);
        std::memcpy(&k, s.data() + 4, 4);
        BloomFilter out;
        out.bits_ = b;
        out.hash_count_ = k;
        size_t expected = (b + 7) / 8;
        if (s.size() != 8 + expected) throw std::runtime_error("bloom filter size mismatch");
        out.data_.assign(s.begin() + 8, s.end());
        return out;
    }

private:
    static uint64_t fnv1a(const std::string& s, uint64_t seed) {
        uint64_t h = seed;
        for (unsigned char c : s) { h ^= static_cast<uint64_t>(c); h *= 1099511628211ull; }
        return h;
    }

    size_t                bits_       = 0;
    size_t                hash_count_ = 0;
    std::vector<uint8_t>  data_;
};

} // namespace delta::storage
