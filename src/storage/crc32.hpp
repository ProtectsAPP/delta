// =============================================================================
// crc32.hpp — CRC-32C (Castagnoli) software implementation (P1-4 / P1-5).
//
// Used by:
//   * WAL: every record carries a CRC32C header that's verified on replay.
//     A torn write at the tail of the log is detected and replay stops there.
//   * SSTable: file header stores a CRC32C of the data section so we can
//     refuse to load a corrupt SSTable instead of silently returning gaps.
//
// We implement Castagnoli (poly 0x1EDC6F41 reversed = 0x82F63B78) — same
// polynomial RocksDB / LevelDB use. Software-only table-driven loop;
// switching to the SSE 4.2 _mm_crc32_u64 hardware instruction is a
// one-symbol future change once we add a runtime-CPUID dispatch.
// =============================================================================
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

namespace delta::storage {

namespace detail {
inline std::array<uint32_t, 256> make_crc32c_table() {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int j = 0; j < 8; ++j) c = (c >> 1) ^ (0x82F63B78u & -(int32_t)(c & 1));
        t[i] = c;
    }
    return t;
}
inline const std::array<uint32_t, 256>& crc32c_table() {
    static const auto t = make_crc32c_table();
    return t;
}
} // namespace detail

inline uint32_t crc32c(const uint8_t* data, size_t len, uint32_t init = 0) {
    auto& t = detail::crc32c_table();
    uint32_t c = ~init;
    for (size_t i = 0; i < len; ++i) c = t[(c ^ data[i]) & 0xff] ^ (c >> 8);
    return ~c;
}

inline uint32_t crc32c(const std::string& s, uint32_t init = 0) {
    return crc32c(reinterpret_cast<const uint8_t*>(s.data()), s.size(), init);
}

} // namespace delta::storage
