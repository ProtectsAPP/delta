#pragma once
// =============================================================================
// src/core/backup_crypto.hpp — authenticated encryption for /admin/backup.
//
// We deliberately avoid taking an OpenSSL build dependency (the rest of the
// engine is libssl-free; pulling it in would impact distros without dev
// libraries). Instead we build encrypt-then-MAC from primitives we already
// ship — PBKDF2-HMAC-SHA256 (key derivation), HMAC-SHA256 (both stream
// cipher and authentication) — using a CTR-mode construction:
//
//   ENC_KEY = PBKDF2(pass, salt || "enc", 120000, 32)
//   MAC_KEY = PBKDF2(pass, salt || "mac", 120000, 32)
//   keystream_block_i = HMAC-SHA256(ENC_KEY, nonce || u32_be(i))
//   ciphertext = plaintext XOR keystream (truncated to len(plaintext))
//   tag        = HMAC-SHA256(MAC_KEY, "delta-backup-1" || salt || nonce || ciphertext)
//
// Wire format (single JSON object emitted by /admin/backup when a passphrase
// is configured):
//
//   {
//     "format":    "delta-backup-1-encrypted",
//     "kdf":       "pbkdf2-sha256",
//     "iter":      120000,
//     "salt":      "<32-hex>",
//     "nonce":     "<24-hex>",      // 12 bytes
//     "ciphertext":"<base64 of XOR-encrypted JSON dump>",
//     "tag":       "<64-hex>"       // HMAC-SHA256
//   }
//
// Decryption is constant-time-MAC-checked BEFORE XOR-decoding, so a tampered
// blob never produces plaintext.
//
// This is not AES-GCM. It is a clean encrypt-then-MAC construction over
// HMAC-SHA256, which is well-understood and conservative. Operators who
// require AES-GCM specifically can post-process the unencrypted dump through
// `openssl enc -aes-256-gcm` — but the threat model for backup-at-rest is
// covered by what we ship here.
// =============================================================================
#include "common.hpp"
#include "../auth/password.hpp"   // pbkdf2_hmac_sha256, hmac_sha256, hex_encode, hex_decode
#include <random>
#include <string>
#include <vector>

namespace delta::backup_crypto {

inline std::vector<uint8_t> random_bytes(size_t n) {
    std::vector<uint8_t> out(n);
    std::random_device rd;
    for (size_t i = 0; i < n; ++i) out[i] = (uint8_t)(rd() & 0xFF);
    return out;
}

// Minimal RFC 4648 base64 (no line breaks).
inline std::string b64_encode(const uint8_t* data, size_t len) {
    static const char* tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    size_t i = 0;
    while (i + 3 <= len) {
        uint32_t v = ((uint32_t)data[i] << 16) | ((uint32_t)data[i+1] << 8) | data[i+2];
        out.push_back(tbl[(v >> 18) & 63]);
        out.push_back(tbl[(v >> 12) & 63]);
        out.push_back(tbl[(v >>  6) & 63]);
        out.push_back(tbl[v & 63]);
        i += 3;
    }
    if (i < len) {
        uint32_t v = (uint32_t)data[i] << 16;
        if (i + 1 < len) v |= (uint32_t)data[i+1] << 8;
        out.push_back(tbl[(v >> 18) & 63]);
        out.push_back(tbl[(v >> 12) & 63]);
        out.push_back(i + 1 < len ? tbl[(v >> 6) & 63] : '=');
        out.push_back('=');
    }
    return out;
}
inline std::vector<uint8_t> b64_decode(const std::string& s) {
    auto dv = [](char c) -> int {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return 26 + (c - 'a');
        if (c >= '0' && c <= '9') return 52 + (c - '0');
        if (c == '+') return 62;
        if (c == '/') return 63;
        return -1;
    };
    std::vector<uint8_t> out; out.reserve((s.size() / 4) * 3);
    for (size_t i = 0; i < s.size(); i += 4) {
        if (i + 3 >= s.size()) break;
        int a = dv(s[i]),   b = dv(s[i+1]);
        int c = (s[i+2] == '=') ? -2 : dv(s[i+2]);
        int d = (s[i+3] == '=') ? -2 : dv(s[i+3]);
        if (a < 0 || b < 0) break;
        out.push_back((uint8_t)((a << 2) | (b >> 4)));
        if (c >= 0) out.push_back((uint8_t)(((b & 0xF) << 4) | (c >> 2)));
        if (d >= 0) out.push_back((uint8_t)(((c & 0x3) << 6) | d));
    }
    return out;
}

inline void derive_keys(const std::string& passphrase,
                        const std::vector<uint8_t>& salt,
                        int iterations,
                        std::vector<uint8_t>& enc_key,
                        std::vector<uint8_t>& mac_key) {
    std::vector<uint8_t> salt_enc = salt; for (char c : std::string("enc")) salt_enc.push_back((uint8_t)c);
    std::vector<uint8_t> salt_mac = salt; for (char c : std::string("mac")) salt_mac.push_back((uint8_t)c);
    enc_key = auth::pbkdf2_hmac_sha256(passphrase, salt_enc, iterations, 32);
    mac_key = auth::pbkdf2_hmac_sha256(passphrase, salt_mac, iterations, 32);
}

inline std::vector<uint8_t> keystream(const std::vector<uint8_t>& enc_key,
                                      const std::vector<uint8_t>& nonce,
                                      size_t len) {
    std::vector<uint8_t> out; out.reserve(len);
    for (uint32_t counter = 0; out.size() < len; ++counter) {
        std::vector<uint8_t> in = nonce;
        in.push_back((uint8_t)((counter >> 24) & 0xFF));
        in.push_back((uint8_t)((counter >> 16) & 0xFF));
        in.push_back((uint8_t)((counter >>  8) & 0xFF));
        in.push_back((uint8_t)( counter        & 0xFF));
        auto block = auth::hmac_sha256(enc_key.data(), enc_key.size(),
                                       in.data(), in.size());
        for (uint8_t b : block) {
            if (out.size() >= len) break;
            out.push_back(b);
        }
    }
    return out;
}

inline std::vector<uint8_t> xor_with(const std::vector<uint8_t>& data,
                                     const std::vector<uint8_t>& ks) {
    std::vector<uint8_t> out(data.size());
    for (size_t i = 0; i < data.size(); ++i) out[i] = data[i] ^ ks[i];
    return out;
}

// --- public API -------------------------------------------------------------
// Wraps `plaintext` (the full backup JSON dump) into the envelope described
// in the header comment. `passphrase` is the operator-supplied secret.
inline json encrypt(const std::string& passphrase, const std::string& plaintext) {
    int iter = 120000;
    auto salt  = random_bytes(16);
    auto nonce = random_bytes(12);
    std::vector<uint8_t> enc_key, mac_key;
    derive_keys(passphrase, salt, iter, enc_key, mac_key);
    std::vector<uint8_t> pt(plaintext.begin(), plaintext.end());
    auto ks = keystream(enc_key, nonce, pt.size());
    auto ct = xor_with(pt, ks);
    // tag = HMAC(MAC_KEY, "delta-backup-1" || salt || nonce || ciphertext)
    std::vector<uint8_t> mac_input;
    const std::string ctx = "delta-backup-1";
    mac_input.insert(mac_input.end(), ctx.begin(), ctx.end());
    mac_input.insert(mac_input.end(), salt.begin(), salt.end());
    mac_input.insert(mac_input.end(), nonce.begin(), nonce.end());
    mac_input.insert(mac_input.end(), ct.begin(), ct.end());
    auto tag = auth::hmac_sha256(mac_key.data(), mac_key.size(),
                                 mac_input.data(), mac_input.size());
    return {
        {"format",     "delta-backup-1-encrypted"},
        {"kdf",        "pbkdf2-sha256"},
        {"iter",       iter},
        {"salt",       auth::hex_encode(salt)},
        {"nonce",      auth::hex_encode(nonce)},
        {"ciphertext", b64_encode(ct.data(), ct.size())},
        {"tag",        auth::hex_encode(tag)}
    };
}

// Reverse of encrypt(). Throws std::runtime_error on MAC failure.
inline std::string decrypt(const std::string& passphrase, const json& env) {
    if (env.value("format", "") != "delta-backup-1-encrypted")
        throw std::runtime_error("envelope: bad format");
    auto salt  = auth::hex_decode(env.value("salt",  ""));
    auto nonce = auth::hex_decode(env.value("nonce", ""));
    int iter   = env.value("iter", 0);
    auto ct    = b64_decode(env.value("ciphertext", ""));
    auto tag_x = auth::hex_decode(env.value("tag", ""));
    if (salt.empty() || nonce.empty() || iter <= 0 || tag_x.size() != 32)
        throw std::runtime_error("envelope: missing fields");
    std::vector<uint8_t> enc_key, mac_key;
    derive_keys(passphrase, salt, iter, enc_key, mac_key);
    std::vector<uint8_t> mac_input;
    const std::string ctx = "delta-backup-1";
    mac_input.insert(mac_input.end(), ctx.begin(), ctx.end());
    mac_input.insert(mac_input.end(), salt.begin(), salt.end());
    mac_input.insert(mac_input.end(), nonce.begin(), nonce.end());
    mac_input.insert(mac_input.end(), ct.begin(), ct.end());
    auto tag = auth::hmac_sha256(mac_key.data(), mac_key.size(),
                                 mac_input.data(), mac_input.size());
    // Constant-time tag comparison.
    if (tag.size() != tag_x.size()) throw std::runtime_error("auth: tag size");
    volatile uint8_t diff = 0;
    for (size_t i = 0; i < tag.size(); ++i) diff |= (uint8_t)(tag[i] ^ tag_x[i]);
    if (diff != 0) throw std::runtime_error("auth: bad passphrase or tampered");
    auto ks = keystream(enc_key, nonce, ct.size());
    auto pt = xor_with(ct, ks);
    return std::string(pt.begin(), pt.end());
}

} // namespace delta::backup_crypto
