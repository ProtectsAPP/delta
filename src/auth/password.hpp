// =============================================================================
// password.hpp — password hashing primitives.
//
// Audit fixes wired in here:
//
//   P0-6 (broken "PBKDF2"): the previous implementation hashed
//          SHA256(salt || password)  N times — that is *not* PBKDF2 and
//          provides no defence against length-extension or pre-image attacks
//          using rainbow tables built around a known salt. We now ship a
//          real PBKDF2-HMAC-SHA256 (RFC 8018 §5.2) implemented on top of
//          the existing SHA-256 primitive — no new third-party dependency.
//          Cost factor 120 000 iterations matches OWASP 2023 guidance for
//          PBKDF2-SHA256.
//
//   P0-7 (timing attack): `verify_password()` now defers comparison to
//          `constant_time_compare()` instead of `std::string::operator==`.
//
// Format:
//   New hashes:  "pbkdf2-sha256$<iter>$<salt-hex>$<hash-hex>"
//   Legacy:      bare 64-char hex (the old broken format).
//   `verify_password()` accepts either; `hash_password()` always emits the
//   new PHC-style string. On the next successful login, the auth manager
//   transparently upgrades a legacy entry to the new format.
//
// What is NOT done in this file (deliberate):
//   * Argon2/bcrypt — would pull in a vendored crypto library; the audit
//     calls this out as the long-term plan. PBKDF2-SHA256 with 120k rounds
//     is acceptable per current NIST SP 800-63B for PBKDF2.
// =============================================================================
#pragma once
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace delta::auth {

// ---------- SHA-256 (unchanged from the earlier hand-written impl) ----------
class SHA256 {
public:
    SHA256() { reset(); }
    void reset() {
        bitlen_ = 0; data_len_ = 0;
        state_[0] = 0x6a09e667; state_[1] = 0xbb67ae85; state_[2] = 0x3c6ef372; state_[3] = 0xa54ff53a;
        state_[4] = 0x510e527f; state_[5] = 0x9b05688c; state_[6] = 0x1f83d9ab; state_[7] = 0x5be0cd19;
    }
    void update(const uint8_t* data, size_t len) {
        for (size_t i = 0; i < len; ++i) {
            data_[data_len_++] = data[i];
            if (data_len_ == 64) { transform(); bitlen_ += 512; data_len_ = 0; }
        }
    }
    void update(const std::string& s) { update((const uint8_t*)s.data(), s.size()); }
    std::vector<uint8_t> final_digest() {
        size_t i = data_len_;
        if (data_len_ < 56) { data_[i++] = 0x80; while (i < 56) data_[i++] = 0; }
        else { data_[i++] = 0x80; while (i < 64) data_[i++] = 0; transform(); std::memset(data_, 0, 56); }
        bitlen_ += data_len_ * 8;
        for (int j = 0; j < 8; ++j) data_[63 - j] = (bitlen_ >> (j * 8)) & 0xff;
        transform();
        std::vector<uint8_t> hash(32);
        for (int j = 0; j < 4; ++j)
            for (int k = 0; k < 8; ++k)
                hash[k * 4 + j] = (state_[k] >> (24 - j * 8)) & 0xff;
        return hash;
    }
private:
    static uint32_t rotr(uint32_t x, uint32_t n) { return (x >> n) | (x << (32 - n)); }
    void transform() {
        static const uint32_t K[64] = {
            0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
            0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
            0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
            0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
            0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
            0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
            0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
            0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2 };
        uint32_t m[64];
        for (int i = 0, j = 0; i < 16; ++i, j += 4)
            m[i] = (data_[j] << 24) | (data_[j+1] << 16) | (data_[j+2] << 8) | (data_[j+3]);
        for (int i = 16; i < 64; ++i) {
            uint32_t s0 = rotr(m[i-15], 7) ^ rotr(m[i-15], 18) ^ (m[i-15] >> 3);
            uint32_t s1 = rotr(m[i-2], 17) ^ rotr(m[i-2], 19) ^ (m[i-2] >> 10);
            m[i] = m[i-16] + s0 + m[i-7] + s1;
        }
        uint32_t a=state_[0],b=state_[1],c=state_[2],d=state_[3],e=state_[4],f=state_[5],g=state_[6],h=state_[7];
        for (int i = 0; i < 64; ++i) {
            uint32_t S1 = rotr(e,6)^rotr(e,11)^rotr(e,25);
            uint32_t ch = (e & f) ^ ((~e) & g);
            uint32_t t1 = h + S1 + ch + K[i] + m[i];
            uint32_t S0 = rotr(a,2)^rotr(a,13)^rotr(a,22);
            uint32_t mj = (a & b) ^ (a & c) ^ (b & c);
            uint32_t t2 = S0 + mj;
            h = g; g = f; f = e; e = d + t1; d = c; c = b; b = a; a = t1 + t2;
        }
        state_[0]+=a; state_[1]+=b; state_[2]+=c; state_[3]+=d;
        state_[4]+=e; state_[5]+=f; state_[6]+=g; state_[7]+=h;
    }
    uint8_t  data_[64];
    uint32_t data_len_;
    uint64_t bitlen_;
    uint32_t state_[8];
};

inline std::string hex_encode(const uint8_t* d, size_t n) {
    std::stringstream ss;
    for (size_t i = 0; i < n; ++i) ss << std::hex << std::setw(2) << std::setfill('0') << (int)d[i];
    return ss.str();
}
inline std::string hex_encode(const std::vector<uint8_t>& d) { return hex_encode(d.data(), d.size()); }
inline std::vector<uint8_t> hex_decode(const std::string& s) {
    if (s.size() % 2 != 0) throw std::runtime_error("hex_decode: odd length");
    std::vector<uint8_t> out(s.size() / 2);
    auto hv = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    for (size_t i = 0; i < out.size(); ++i) {
        int hi = hv(s[2*i]), lo = hv(s[2*i+1]);
        if (hi < 0 || lo < 0) throw std::runtime_error("hex_decode: bad char");
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return out;
}

// ---------- HMAC-SHA256 (RFC 2104) -----------------------------------------
inline std::vector<uint8_t> hmac_sha256(const uint8_t* key, size_t klen,
                                        const uint8_t* msg, size_t mlen) {
    constexpr size_t B = 64;          // SHA-256 block size in bytes
    uint8_t k[B];
    if (klen > B) {
        SHA256 h; h.update(key, klen);
        auto kh = h.final_digest();
        std::memcpy(k, kh.data(), kh.size());
        std::memset(k + kh.size(), 0, B - kh.size());
    } else {
        std::memcpy(k, key, klen);
        std::memset(k + klen, 0, B - klen);
    }
    uint8_t ipad[B], opad[B];
    for (size_t i = 0; i < B; ++i) { ipad[i] = k[i] ^ 0x36; opad[i] = k[i] ^ 0x5c; }
    SHA256 inner; inner.update(ipad, B); inner.update(msg, mlen);
    auto inner_d = inner.final_digest();
    SHA256 outer; outer.update(opad, B); outer.update(inner_d.data(), inner_d.size());
    return outer.final_digest();
}

// ---------- PBKDF2-HMAC-SHA256 (RFC 8018 §5.2) -----------------------------
// Derive `dklen` bytes from (password, salt). For our use dklen == 32.
inline std::vector<uint8_t> pbkdf2_hmac_sha256(const std::string& password,
                                               const std::vector<uint8_t>& salt,
                                               int iterations,
                                               size_t dklen = 32) {
    if (iterations < 1) iterations = 1;
    constexpr size_t hLen = 32;
    size_t blocks = (dklen + hLen - 1) / hLen;
    std::vector<uint8_t> out;
    out.reserve(blocks * hLen);
    for (uint32_t i = 1; i <= blocks; ++i) {
        // U_1 = HMAC(P, S || INT(i))
        std::vector<uint8_t> msg(salt);
        msg.push_back((i >> 24) & 0xff);
        msg.push_back((i >> 16) & 0xff);
        msg.push_back((i >> 8)  & 0xff);
        msg.push_back( i        & 0xff);
        auto u = hmac_sha256(reinterpret_cast<const uint8_t*>(password.data()),
                             password.size(), msg.data(), msg.size());
        std::vector<uint8_t> t = u;
        // T_i = U_1 ^ U_2 ^ ... ^ U_c
        for (int j = 1; j < iterations; ++j) {
            u = hmac_sha256(reinterpret_cast<const uint8_t*>(password.data()),
                            password.size(), u.data(), u.size());
            for (size_t b = 0; b < hLen; ++b) t[b] ^= u[b];
        }
        out.insert(out.end(), t.begin(), t.end());
    }
    out.resize(dklen);
    return out;
}

// ---------- Constant-time string comparison (P0-7) -------------------------
// Independent of input length so timing leaks neither the byte position
// of the first difference nor the length of the secret. We deliberately
// use a `volatile` accumulator to deter compiler attempts at short-circuit
// rewriting.
inline bool constant_time_compare(const std::string& a, const std::string& b) {
    // P2-08: do not short-circuit on size mismatch. Walking the longer
    // of the two strings to a fixed bound keeps the runtime independent
    // of where (or whether) the inputs differ. We still fold the size
    // delta into `diff` so unequal-length inputs reliably return false.
    size_t n = a.size() > b.size() ? a.size() : b.size();
    volatile uint8_t diff = static_cast<uint8_t>(a.size() ^ b.size());
    for (size_t i = 0; i < n; ++i) {
        uint8_t ai = (i < a.size()) ? (uint8_t)(unsigned char)a[i] : 0;
        uint8_t bi = (i < b.size()) ? (uint8_t)(unsigned char)b[i] : 0;
        diff = static_cast<uint8_t>(diff | (uint8_t)(ai ^ bi));
    }
    return diff == 0;
}

// ---------- Public API -----------------------------------------------------
// PHC-ish encoding so we can rotate parameters without rebuilding every hash.
//   "pbkdf2-sha256$<iter>$<salt-hex>$<hash-hex>"
inline std::string hash_password(const std::string& password,
                                 const std::string& salt_hex,
                                 int iterations = 120000) {
    auto salt = hex_decode(salt_hex);
    auto dk   = pbkdf2_hmac_sha256(password, salt, iterations);
    std::stringstream ss;
    ss << "pbkdf2-sha256$" << iterations << "$" << salt_hex << "$" << hex_encode(dk);
    return ss.str();
}

// Detect format. Returns true for the new PHC-prefixed string.
inline bool is_phc_hash(const std::string& s) {
    return s.rfind("pbkdf2-sha256$", 0) == 0;
}

// Verify against either the new PHC format or the legacy bare-hex one.
// `salt_for_legacy` is only consulted when `expected_hash` is the legacy
// 64-hex form; PHC-encoded hashes carry their salt inline.
inline bool verify_password(const std::string& password,
                            const std::string& salt_for_legacy,
                            const std::string& expected_hash) {
    if (is_phc_hash(expected_hash)) {
        // pbkdf2-sha256$ITER$SALT$HASH
        size_t p1 = expected_hash.find('$', 0);
        size_t p2 = expected_hash.find('$', p1 + 1);
        size_t p3 = expected_hash.find('$', p2 + 1);
        if (p1 == std::string::npos || p2 == std::string::npos || p3 == std::string::npos) return false;
        int iter = 0;
        try { iter = std::stoi(expected_hash.substr(p1 + 1, p2 - p1 - 1)); }
        catch (...) { return false; }
        std::string salt_hex = expected_hash.substr(p2 + 1, p3 - p2 - 1);
        std::string hash_hex = expected_hash.substr(p3 + 1);
        std::vector<uint8_t> salt;
        try { salt = hex_decode(salt_hex); }
        catch (...) { return false; }
        auto dk  = pbkdf2_hmac_sha256(password, salt, iter);
        std::string got = hex_encode(dk);
        return constant_time_compare(got, hash_hex);
    }
    // Legacy: SHA256-loop with salt prefix. We keep the exact algorithm so
    // existing on-disk users still authenticate; the auth manager will
    // upgrade them to PBKDF2 on the next successful login.
    SHA256 h; h.update(salt_for_legacy); h.update(password);
    auto cur = h.final_digest();
    for (int i = 0; i < 10000; ++i) {
        SHA256 h2; h2.update(cur.data(), cur.size()); h2.update(salt_for_legacy);
        cur = h2.final_digest();
    }
    return constant_time_compare(hex_encode(cur), expected_hash);
}

} // namespace delta::auth
