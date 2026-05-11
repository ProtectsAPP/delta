// =============================================================================
// constants.hpp — magic numbers extracted from across the codebase.
//
// Why this exists:
//   Before this file the codebase had hard-coded `64 * 1024 * 1024`, `100000`,
//   `5`, `16`, `200`, `300` etc. scattered through ~5,000 lines of code, with
//   no single place to tune them. P2-7 collected them here.
//
// Naming convention:
//   * Buckets by subsystem (STORAGE_*, CACHE_*, HNSW_*, AUTH_*, NET_*).
//   * `*_DEFAULT` for values that are user-overridable via config.
//   * `*_MAX` for hard caps the engine refuses to exceed regardless of config.
// =============================================================================
#pragma once
#include <cstddef>
#include <cstdint>

namespace delta::constants {

// ---- Storage (LSM tree, WAL, SSTable) --------------------------------------
inline constexpr size_t   STORAGE_MEMTABLE_DEFAULT_BYTES   = 64 * 1024 * 1024;     // 64 MiB
inline constexpr int      STORAGE_COMPACTION_TRIGGER_SST   = 4;
// P0-1: STCS (Size-Tiered Compaction Strategy) parameters.
//   * MIN_SSTS  — never compact until at least this many tables exist.
//   * MAX_RATIO — within a tier, all tables' sizes must be within this
//                 multiplicative band of each other; otherwise we wait.
//   * INTERVAL  — compactor wakes up this often to re-evaluate.
inline constexpr int      STORAGE_COMPACTION_MIN_SSTS      = 4;
inline constexpr double   STORAGE_COMPACTION_MAX_RATIO     = 4.0;
inline constexpr int      STORAGE_COMPACTION_INTERVAL_MS   = 2000;
inline constexpr uint64_t STORAGE_TOMBSTONE_GC_AGE_MS      = 60ull * 60ull * 1000; // 1 hour
inline constexpr uint32_t STORAGE_WAL_MAGIC                = 0x57414C44u;          // 'WALD'
inline constexpr uint32_t STORAGE_SSTABLE_MAGIC            = 0x44535354u;          // 'DSST'
inline constexpr uint32_t STORAGE_SSTABLE_VERSION          = 2;
// P1-2 sparse block index parameters. Build one (key, file_offset) entry
// per N adjacent keys so point lookups can binary-search the trailer
// then sequentially scan ≤ BLOCK_KEYS records on disk via pread().
inline constexpr int      STORAGE_SSTABLE_BLOCK_KEYS       = 16;

// ---- Document / collection -------------------------------------------------
inline constexpr size_t STORAGE_PREFIX_SCAN_LIMIT   = 1'000'000;
inline constexpr size_t COLLECTION_MAX_BULK_INSERT  = 100'000;
inline constexpr size_t COLLECTION_MAX_INDEX_BUILD  = 1'000'000;
inline constexpr size_t COLLECTION_MAX_DOC_BYTES    = 16 * 1024 * 1024;   // 16 MiB per document

// ---- Cache engine ----------------------------------------------------------
inline constexpr int    CACHE_SHARD_COUNT          = 32;
inline constexpr size_t CACHE_DEFAULT_MAX_KEYS     = 100'000;
inline constexpr size_t CACHE_DEFAULT_MAX_BYTES    = 512 * 1024 * 1024;   // 512 MiB
inline constexpr int    CACHE_PURGE_BATCH_SIZE     = 100;                 // sample N keys / janitor pass

// ---- Vector engine (HNSW) --------------------------------------------------
inline constexpr int  HNSW_DEFAULT_M               = 16;
inline constexpr int  HNSW_DEFAULT_EF_CONSTRUCTION = 200;
inline constexpr int  HNSW_DEFAULT_EF_SEARCH       = 50;
inline constexpr int  HNSW_DEFAULT_MAX_LEVEL       = 16;

// ---- Network ---------------------------------------------------------------
inline constexpr int    NET_DEFAULT_HTTP_PORT      = 16888;
inline constexpr int    NET_DEFAULT_DELTAQL_PORT   = 16889;
inline constexpr int    NET_DEFAULT_WS_PORT        = 16890;
inline constexpr int    NET_DEFAULT_MAX_CONN       = 1024;
inline constexpr size_t NET_MAX_PAYLOAD_BYTES      = 64 * 1024 * 1024;    // 64 MiB

// ---- Auth ------------------------------------------------------------------
inline constexpr int      AUTH_MAX_LOGIN_ATTEMPTS  = 5;
inline constexpr uint64_t AUTH_DEFAULT_SESSION_TTL_SEC = 86400ull;        // 24h
// PBKDF2-HMAC-SHA256 cost. RFC 8018 recommends ≥ 100,000 for SHA-256 in 2023.
inline constexpr int      AUTH_PBKDF2_ITERATIONS   = 120'000;
// Login rate limit window (P1-17): N attempts per IP per window.
inline constexpr int      AUTH_RATE_LIMIT_MAX      = 10;
inline constexpr int      AUTH_RATE_LIMIT_WINDOW_S = 300;                 // 5 min

// ---- Validation ------------------------------------------------------------
inline constexpr size_t VALID_IDENT_MIN_LEN  = 1;
inline constexpr size_t VALID_IDENT_MAX_LEN  = 64;
inline constexpr size_t VALID_PASSWORD_MIN   = 8;
inline constexpr size_t VALID_PASSWORD_MAX   = 128;

} // namespace delta::constants
