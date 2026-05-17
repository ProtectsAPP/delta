#pragma once
// =============================================================================
// DeltaQL wire protocol — binary, multiplexed, persistent.
//
// One TCP connection carries many in-flight (request, response) pairs
// correlated by a 32-bit `request_id`. Frames are length-prefixed; clients can
// pipeline freely without waiting for responses, and the server can push
// unsolicited frames (PUBLISH) for pubsub channels at any time.
//
//   ┌──────────────────────── 16-byte fixed header ────────────────────────┐
//   │  magic (4)  │ ver (1) │ flags (1) │ type (1) │ rsvd (1) │ rid (4) │ len (4) │
//   └──────────────────────────────────────────────────────────────────────┘
//   payload (`len` bytes, UTF-8 JSON)
//
//   magic = "DLTA" (0x44 0x4C 0x54 0x41)   — always the same
//   ver   = 1                              — protocol version
//   flags = bit 0: payload is gzip'd       — currently always 0
//   type  = see Type enum below
//   rid   = client-assigned request id (echoed back in RESPONSE/ERROR)
//   len   = payload byte count (max 64 MiB, refused above)
//
// Payload schemas (all UTF-8 JSON):
//
//   HELLO   { "client": "name/version", "features": ["pubsub","stream"] }
//   AUTH    { "username": "...", "password": "..." }   or { "token": "..." }
//   REQUEST { "method": "POST", "path": "/api/v1/...",
//             "query":   {"database":"shop"},     // optional
//             "headers": {"X-Foo": "bar"},        // optional
//             "body":    { ... } }                // optional
//   RESPONSE { "status": 200, "body": { "code":200, "data":{...} } }
//   ERROR    { "code": -1, "message": "..." }
//   PING     {}                                    // server or client
//   PONG     {}                                    // reply to PING
//   PUBLISH  { "channel": "...", "message": "..." }  // server → client
//   SUB      { "channel": "..." }                  // client → server
//   UNSUB    { "channel": "..." }                  // client → server
// =============================================================================
#include <cstdint>
#include <string>
#include <vector>
#include <cstring>
#include <stdexcept>

namespace delta::network::dql {

inline constexpr uint32_t MAGIC      = 0x444C5441u; // 'DLTA' big-endian
inline constexpr uint8_t  VERSION    = 1;
inline constexpr uint32_t MAX_FRAME  = 16u * 1024u * 1024u; // 16 MiB
inline constexpr size_t   HEADER_LEN = 16;

enum class Type : uint8_t {
    HELLO    = 0x01,
    HELLO_OK = 0x02,
    AUTH     = 0x03,
    AUTH_OK  = 0x04,
    REQUEST  = 0x05,
    RESPONSE = 0x06,
    ERROR    = 0x07,
    PING     = 0x08,
    PONG     = 0x09,
    SUB      = 0x0A,
    UNSUB    = 0x0B,
    PUBLISH  = 0x0C,
    BYE      = 0x0F
};

struct Frame {
    uint8_t  flags = 0;
    Type     type  = Type::REQUEST;
    uint32_t rid   = 0;
    std::string payload;        // JSON text
};

// ----- big-endian helpers ------------------------------------------------
inline void put_u32(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)(v);
}
inline uint32_t get_u32(const uint8_t* p) {
    return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16)
         | (uint32_t(p[2]) << 8)  | (uint32_t(p[3]));
}

// Encode one frame into a contiguous buffer, ready for write().
inline std::vector<uint8_t> encode(const Frame& f) {
    std::vector<uint8_t> out(HEADER_LEN + f.payload.size());
    put_u32(out.data(), MAGIC);
    out[4] = VERSION;
    out[5] = f.flags;
    out[6] = (uint8_t)f.type;
    out[7] = 0;                                 // reserved
    put_u32(out.data() + 8, f.rid);
    put_u32(out.data() + 12, (uint32_t)f.payload.size());
    if (!f.payload.empty()) std::memcpy(out.data() + HEADER_LEN, f.payload.data(), f.payload.size());
    return out;
}

// Parse exactly one frame from a fully-buffered header+payload region.
// Throws std::runtime_error on protocol violation.
inline Frame parse_header_and_take_payload(const uint8_t* hdr, std::string&& payload) {
    if (get_u32(hdr) != MAGIC)         throw std::runtime_error("dql: bad magic");
    if (hdr[4] != VERSION)             throw std::runtime_error("dql: bad version");
    Frame f;
    f.flags   = hdr[5];
    f.type    = (Type)hdr[6];
    f.rid     = get_u32(hdr + 8);
    f.payload = std::move(payload);
    return f;
}

inline uint32_t parse_payload_len(const uint8_t* hdr) { return get_u32(hdr + 12); }

} // namespace delta::network::dql
