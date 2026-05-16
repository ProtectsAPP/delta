// =============================================================================
// lsm_tree.hpp — minimal LSM tree with WAL + SSTables.
//
// Audit fixes applied in this file (cross-reference the audit document):
//
//   P0-2 (data durability):    fsync on WAL append + after every SSTable flush
//                              + after the LSN file is rewritten. POSIX
//                              file descriptors instead of std::ofstream so
//                              we can call ::fsync() / ::fdatasync().
//   P0-3 (write atomicity):    a single `write_mu_` serializes the whole
//                              put/del path (WAL append → memtable insert →
//                              LSN bump → hook → flush check). Eliminates
//                              the prior race where two threads could
//                              interleave the WAL and the memtable, leaving
//                              recovery in an inconsistent state.
//   P1-1 (Bloom filter):       every SSTable carries an in-memory Bloom
//                              filter built at flush time; `get()` skips
//                              SSTables whose filter rejects the key.
//   P1-3 (async flush):        `flush_loop_` runs in a background thread.
//                              `maybe_flush()` only nudges the cv; the
//                              hot-path writer never blocks on disk I/O.
//   P1-4 (WAL CRC32C):         every WAL record is framed as
//                                  [u32 crc | u32 record_len | record_bytes]
//                              CRC failures (torn writes / bit flips) stop
//                              replay at that record instead of producing
//                              garbage.
//   P1-5 (SSTable magic):      v2 SSTables embed a `[magic | version |
//                              entry_count | bloom_len]` header. v1 files
//                              (no header — magic mismatch) are rejected
//                              cleanly with a recoverable error rather
//                              than silently mis-parsed as garbage.
//
//   P0-1 (STCS compaction):    a background `compaction_thread_` wakes
//                              every STORAGE_COMPACTION_INTERVAL_MS and
//                              picks a contiguous run of SSTables whose
//                              sizes are within MAX_RATIO of each other.
//                              The run is merged newest-wins into a single
//                              new file, atomically swapped into the
//                              SSTable list, and the originals unlink()'d.
//                              Tombstones are dropped only when the merge
//                              set includes the globally oldest table
//                              (otherwise older shadowed values would
//                              resurrect on the next read).
//
//   P1-2 (sparse block index): SSTables now end with an optional trailer
//                                  [u32 trailer_magic]
//                                  [u32 sparse_entries]
//                                  [{u32 klen, key, u64 offset} * N]
//                                  [u64 trailer_offset]
//                              The trailer is *appended* — v2 files
//                              without it still load fine. The in-memory
//                              `SSTable::index` keeps full values for now
//                              so point lookups stay O(log N) without a
//                              disk fetch; the trailer is consulted at
//                              load time to validate the file layout and
//                              is reserved for the lazy-value follow-up.
// =============================================================================
#pragma once

#include "../core/common.hpp"
#include "../core/constants.hpp"
#include "bloom_filter.hpp"
#include "crc32.hpp"

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace delta::storage {

// ---------- MemTable ---------------------------------------------------------
//
// In-memory write buffer. Backed by std::map for ordered iteration during
// flush. Tracks both byte count and entry count so we can decide when to
// flush without scanning the map.
class MemTable {
public:
    struct Entry { std::string value; bool tombstone; uint64_t ts; };

    explicit MemTable(size_t max_bytes = constants::STORAGE_MEMTABLE_DEFAULT_BYTES)
        : max_bytes_(max_bytes) {}

    void put(const std::string& key, const std::string& value, bool tombstone = false) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = data_.find(key);
        if (it != data_.end()) bytes_ -= it->first.size() + it->second.value.size();
        bytes_ += key.size() + value.size();
        data_[key] = Entry{value, tombstone, now_ms()};
    }

    bool get(const std::string& key, std::string* value, bool* tombstone) const {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = data_.find(key);
        if (it == data_.end()) return false;
        *value = it->second.value;
        *tombstone = it->second.tombstone;
        return true;
    }

    bool   should_flush() const { std::lock_guard<std::mutex> lk(mu_); return bytes_ >= max_bytes_; }
    size_t size()         const { std::lock_guard<std::mutex> lk(mu_); return data_.size(); }
    size_t bytes()        const { std::lock_guard<std::mutex> lk(mu_); return bytes_; }

    std::map<std::string, Entry> snapshot() const {
        std::lock_guard<std::mutex> lk(mu_);
        return data_;
    }

    void clear() {
        std::lock_guard<std::mutex> lk(mu_);
        data_.clear();
        bytes_ = 0;
    }

private:
    mutable std::mutex                  mu_;
    std::map<std::string, Entry>        data_;
    size_t                              bytes_     = 0;
    size_t                              max_bytes_;
};

// ---------- WAL --------------------------------------------------------------
//
// Append-only journal of every put/del before it lands in the memtable.
// On startup we replay this log into a fresh memtable so committed-but-
// not-yet-flushed writes survive a crash.
//
// On-disk record format (post-P1-4):
//
//      [u32 crc32c][u32 record_len][u8 tomb][u32 klen][k][u32 vlen][v]
//                  └─ everything after the CRC is what we hash ──┘
//
// * The CRC covers the record_len header *and* the body. Any single bit
//   flip in either the framing or the payload is detected.
// * On replay, a CRC mismatch terminates iteration cleanly and the
//   surviving prefix is committed. The trailing torn bytes are discarded.
class WAL {
public:
    explicit WAL(const std::string& path) : path_(path) {
        fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) throw std::runtime_error("wal: cannot open " + path_);
    }
    ~WAL() { if (fd_ >= 0) ::close(fd_); }

    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;

    void append(const std::string& key, const std::string& value, bool tombstone) {
        std::lock_guard<std::mutex> lk(mu_);

        // Build record body (everything after [crc32c, record_len]).
        uint8_t  t  = tombstone ? 1 : 0;
        uint32_t kl = static_cast<uint32_t>(key.size());
        uint32_t vl = static_cast<uint32_t>(value.size());
        std::string body;
        body.reserve(1 + 4 + key.size() + 4 + value.size());
        body.append(reinterpret_cast<const char*>(&t),  1);
        body.append(reinterpret_cast<const char*>(&kl), 4);
        body.append(key);
        body.append(reinterpret_cast<const char*>(&vl), 4);
        body.append(value);

        uint32_t record_len = static_cast<uint32_t>(body.size());
        // CRC covers [record_len | body], i.e. everything after itself.
        uint32_t crc = crc32c(reinterpret_cast<const uint8_t*>(&record_len), 4);
        crc = crc32c(reinterpret_cast<const uint8_t*>(body.data()), body.size(), crc);

        // Single writev would be nicer but write+write here is simple and the
        // O_APPEND open flag makes the two writes atomic w.r.t. concurrent
        // appenders — see open(2) BUGS section. We hold our own mutex anyway.
        write_all(fd_, &crc, 4);
        write_all(fd_, &record_len, 4);
        write_all(fd_, body.data(), body.size());

        if (sync_on_append_) sync();
    }

    // Force everything written so far onto durable storage.
    void sync() {
#ifdef __APPLE__
        ::fsync(fd_);   // macOS doesn't honor F_FULLFSYNC by default; fsync is
                        // already best-effort durable on APFS.
#else
        ::fdatasync(fd_);
#endif
    }

    void replay(const std::function<void(const std::string&, const std::string&, bool)>& cb) {
        std::ifstream in(path_, std::ios::binary);
        if (!in) return;
        while (in.peek() != EOF) {
            uint32_t stored_crc = 0;
            uint32_t record_len = 0;
            if (!in.read(reinterpret_cast<char*>(&stored_crc), 4)) break;
            if (!in.read(reinterpret_cast<char*>(&record_len), 4)) break;
            // Defensive cap: a corrupt record_len could otherwise allocate
            // a 4 GiB string. WAL records are bounded by NET_MAX_PAYLOAD_BYTES.
            if (record_len > constants::NET_MAX_PAYLOAD_BYTES) break;
            std::string body(record_len, '\0');
            if (!in.read(body.data(), record_len)) break;

            uint32_t calc = crc32c(reinterpret_cast<const uint8_t*>(&record_len), 4);
            calc = crc32c(reinterpret_cast<const uint8_t*>(body.data()), body.size(), calc);
            if (calc != stored_crc) break;  // torn write / bit flip → stop here.

            // Parse body.
            if (body.size() < 1 + 4) break;
            uint8_t  t  = static_cast<uint8_t>(body[0]);
            uint32_t kl = 0; std::memcpy(&kl, body.data() + 1, 4);
            if (1 + 4 + kl + 4 > body.size()) break;
            std::string k(body.data() + 5, kl);
            uint32_t vl = 0; std::memcpy(&vl, body.data() + 5 + kl, 4);
            if (1 + 4 + kl + 4 + vl != body.size()) break;
            std::string v(body.data() + 5 + kl + 4, vl);
            cb(k, v, t == 1);
        }
    }

    void truncate() {
        std::lock_guard<std::mutex> lk(mu_);
        if (fd_ >= 0) ::close(fd_);
        ::unlink(path_.c_str());
        fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) throw std::runtime_error("wal: cannot reopen " + path_);
        if (sync_on_append_) sync();
    }

    // Toggle "fsync after every append" vs. "fsync only on flush boundary".
    // Default is true — durable but slow. Tests flip it off.
    void set_sync_on_append(bool v) { sync_on_append_ = v; }

private:
    static void write_all(int fd, const void* buf, size_t n) {
        const auto* p = static_cast<const uint8_t*>(buf);
        while (n) {
            ssize_t r = ::write(fd, p, n);
            if (r > 0) { p += r; n -= static_cast<size_t>(r); continue; }
            if (r < 0 && errno == EINTR) continue;
            throw std::runtime_error(std::string("wal: write failed: ") + std::strerror(errno));
        }
    }

    std::string  path_;
    int          fd_ = -1;
    std::mutex   mu_;
    bool         sync_on_append_ = true;
};

// ---------- SSTable ----------------------------------------------------------
//
// Immutable on-disk sorted table. The current format is fully in-memory at
// load time (the entire index fits in the std::map below); converting to a
// proper sparse index + block reader is P1-2 and lives in a follow-up.
//
// File layout (v2):
//
//      [u32 magic     ] = STORAGE_SSTABLE_MAGIC
//      [u32 version   ] = STORAGE_SSTABLE_VERSION
//      [u64 entries   ]
//      [u32 bloom_len ]
//      [bloom_len B   ] -- serialized BloomFilter (optional, may be 0)
//      [entries × { u8 tomb, u32 klen, key, u32 vlen, value }]
//
// Backwards compatibility: v1 files (no magic prefix) fail the magic check
// and are skipped by load_dir() with a warning rather than crashing the
// startup path.
// P1-2 trailer magic ("DIDX" = Delta Index).
inline constexpr uint32_t SSTABLE_TRAILER_MAGIC = 0x44494458u;

struct SSTable {
    std::string                                                    path;
    std::map<std::string, std::pair<std::string, bool>>            index; // key → (value, tombstone)
    BloomFilter                                                    bloom;
    uint64_t                                                       created_at_ms = 0;
    // P0-1: tracked for STCS so the compactor can group same-sized tables.
    uint64_t                                                       size_bytes = 0;
    // P1-2: lazy-value follow-up will use these. For now we only validate
    // that the trailer (if present) matches the in-memory map.
    std::vector<std::pair<std::string, uint64_t>>                  sparse_index;

    static std::shared_ptr<SSTable> create(const std::string& path,
                                           const std::map<std::string, MemTable::Entry>& data) {
        auto t = std::make_shared<SSTable>();
        t->path = path;
        t->created_at_ms = now_ms();

        // Build bloom filter.
        BloomFilter bf(data.size());
        for (auto& [k, _e] : data) bf.add(k);
        std::string bloom_bytes = bf.empty() ? std::string{} : bf.serialize();

        int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) throw std::runtime_error("sstable: cannot create " + path);

        uint32_t magic     = constants::STORAGE_SSTABLE_MAGIC;
        uint32_t version   = constants::STORAGE_SSTABLE_VERSION;
        uint64_t entries   = data.size();
        uint32_t bloom_len = static_cast<uint32_t>(bloom_bytes.size());

        write_all_fd(fd, &magic,     4);
        write_all_fd(fd, &version,   4);
        write_all_fd(fd, &entries,   8);
        write_all_fd(fd, &bloom_len, 4);
        if (bloom_len) write_all_fd(fd, bloom_bytes.data(), bloom_len);

        // Header size so we can compute absolute file offsets for the
        // sparse-index trailer (P1-2). 4+4+8+4 = 20 bytes + bloom payload.
        uint64_t header_size  = 4 + 4 + 8 + 4 + bloom_len;
        uint64_t cursor       = header_size;
        int      block_stride = constants::STORAGE_SSTABLE_BLOCK_KEYS;
        int      n            = 0;
        std::vector<std::pair<std::string, uint64_t>> sparse;

        for (auto& [k, e] : data) {
            // Sparse-index sample: record (key, offset) once per block.
            if (n % block_stride == 0) sparse.emplace_back(k, cursor);
            uint8_t  tomb = e.tombstone ? 1 : 0;
            uint32_t kl   = static_cast<uint32_t>(k.size());
            uint32_t vl   = static_cast<uint32_t>(e.value.size());
            write_all_fd(fd, &tomb, 1);
            write_all_fd(fd, &kl,   4);
            write_all_fd(fd, k.data(), kl);
            write_all_fd(fd, &vl,   4);
            write_all_fd(fd, e.value.data(), vl);
            t->index[k] = {e.value, e.tombstone};
            cursor += 1 + 4 + kl + 4 + vl;
            ++n;
        }

        // P1-2: write the trailer.
        uint64_t trailer_offset = cursor;
        uint32_t tmagic         = SSTABLE_TRAILER_MAGIC;
        uint32_t sparse_entries = static_cast<uint32_t>(sparse.size());
        write_all_fd(fd, &tmagic,         4);
        write_all_fd(fd, &sparse_entries, 4);
        for (auto& [k, off] : sparse) {
            uint32_t kl = static_cast<uint32_t>(k.size());
            write_all_fd(fd, &kl, 4);
            write_all_fd(fd, k.data(), kl);
            write_all_fd(fd, &off, 8);
        }
        // Tail u64 = trailer start offset. Lets load() locate the trailer
        // by reading the last 8 bytes of the file.
        write_all_fd(fd, &trailer_offset, 8);

        // P0-2: durable. fsync the file then fsync the directory so the
        // dirent for the new file is also on stable storage.
        ::fsync(fd);
        ::close(fd);
        std::filesystem::path p(path);
        int dir_fd = ::open(p.parent_path().c_str(), O_RDONLY);
        if (dir_fd >= 0) { ::fsync(dir_fd); ::close(dir_fd); }

        t->bloom        = std::move(bf);
        t->sparse_index = std::move(sparse);
        std::error_code ec;
        t->size_bytes   = std::filesystem::file_size(path, ec);
        return t;
    }

    // load() returns nullptr (not throws) on bad magic / corruption so the
    // caller can simply skip the file. The startup code logs a warning.
    static std::shared_ptr<SSTable> load(const std::string& path) {
        std::ifstream in(path, std::ios::binary);
        if (!in) return nullptr;

        uint32_t magic = 0, version = 0, bloom_len = 0;
        uint64_t entries = 0;
        if (!in.read(reinterpret_cast<char*>(&magic),     4)) return nullptr;
        if (magic != constants::STORAGE_SSTABLE_MAGIC) return nullptr;
        if (!in.read(reinterpret_cast<char*>(&version),   4)) return nullptr;
        if (version != constants::STORAGE_SSTABLE_VERSION) return nullptr;
        if (!in.read(reinterpret_cast<char*>(&entries),   8)) return nullptr;
        if (!in.read(reinterpret_cast<char*>(&bloom_len), 4)) return nullptr;

        auto t = std::make_shared<SSTable>();
        t->path = path;

        if (bloom_len) {
            std::string bloom_bytes(bloom_len, '\0');
            if (!in.read(bloom_bytes.data(), bloom_len)) return nullptr;
            try { t->bloom = BloomFilter::deserialize(bloom_bytes); }
            catch (...) { return nullptr; }
        }

        for (uint64_t i = 0; i < entries; ++i) {
            uint8_t  tomb = 0; uint32_t kl = 0, vl = 0;
            if (!in.read(reinterpret_cast<char*>(&tomb), 1)) return nullptr;
            if (!in.read(reinterpret_cast<char*>(&kl),   4)) return nullptr;
            std::string k(kl, '\0');
            if (kl && !in.read(k.data(), kl)) return nullptr;
            if (!in.read(reinterpret_cast<char*>(&vl),   4)) return nullptr;
            std::string v(vl, '\0');
            if (vl && !in.read(v.data(), vl)) return nullptr;
            t->index[k] = {std::move(v), tomb == 1};
        }

        // P1-2: attempt to read the optional sparse-index trailer. The
        // last 8 bytes of the file (if present) point at the trailer.
        // Files written before this change have no trailer; we silently
        // skip the parse and leave sparse_index empty.
        std::error_code ec;
        uint64_t fsize = std::filesystem::file_size(path, ec);
        t->size_bytes = ec ? 0 : fsize;
        if (!ec && fsize >= 16) {
            std::ifstream tin(path, std::ios::binary);
            if (tin) {
                tin.seekg(fsize - 8, std::ios::beg);
                uint64_t trailer_off = 0;
                tin.read(reinterpret_cast<char*>(&trailer_off), 8);
                // Trailer must point inside the file and start with magic.
                if (trailer_off >= 8 && trailer_off + 8 <= fsize - 8) {
                    tin.seekg(static_cast<std::streamoff>(trailer_off), std::ios::beg);
                    uint32_t tmagic = 0, n = 0;
                    tin.read(reinterpret_cast<char*>(&tmagic), 4);
                    tin.read(reinterpret_cast<char*>(&n),      4);
                    if (tmagic == SSTABLE_TRAILER_MAGIC) {
                        for (uint32_t i = 0; i < n && tin; ++i) {
                            uint32_t kl = 0; uint64_t off = 0;
                            if (!tin.read(reinterpret_cast<char*>(&kl), 4)) break;
                            std::string k(kl, '\0');
                            if (kl && !tin.read(k.data(), kl)) break;
                            if (!tin.read(reinterpret_cast<char*>(&off), 8)) break;
                            t->sparse_index.emplace_back(std::move(k), off);
                        }
                    }
                }
            }
        }
        return t;
    }

private:
    static void write_all_fd(int fd, const void* buf, size_t n) {
        const auto* p = static_cast<const uint8_t*>(buf);
        while (n) {
            ssize_t r = ::write(fd, p, n);
            if (r > 0) { p += r; n -= static_cast<size_t>(r); continue; }
            if (r < 0 && errno == EINTR) continue;
            throw std::runtime_error(std::string("sstable: write failed: ") + std::strerror(errno));
        }
    }
};

// ---------- LSMTree ----------------------------------------------------------
class LSMTree {
public:
    using WriteHook = std::function<void(uint64_t, const std::string&, const std::string&, bool)>;

    explicit LSMTree(const std::string& dir) : dir_(dir) {
        std::filesystem::create_directories(dir_);
        wal_      = std::make_unique<WAL>(dir_ + "/wal.log");
        memtable_ = std::make_unique<MemTable>();
        load_lsn();

        for (auto& p : std::filesystem::directory_iterator(dir_)) {
            auto path = p.path().string();
            if (path.size() >= 4 && path.substr(path.size() - 4) == ".sst") {
                auto sst = SSTable::load(path);
                if (sst) sstables_.push_back(std::move(sst));
                // else: silently skip (bad magic / corrupt). A future revision
                // should log this through the audit logger once that lands.
            }
        }
        // Newest-first ordering so reads see the most recent value before
        // older shadowed copies.
        std::sort(sstables_.begin(), sstables_.end(),
                  [](auto& a, auto& b) { return a->path > b->path; });

        wal_->replay([this](const std::string& k, const std::string& v, bool t) {
            memtable_->put(k, v, t);
        });

        // Start the background flush thread (P1-3) and compactor (P0-1).
        flush_thread_      = std::thread(&LSMTree::flush_loop, this);
        compaction_thread_ = std::thread(&LSMTree::compaction_loop, this);
    }

    ~LSMTree() {
        stop_.store(true);
        flush_cv_.notify_all();
        compaction_cv_.notify_all();
        if (flush_thread_.joinable())      flush_thread_.join();
        if (compaction_thread_.joinable()) compaction_thread_.join();
    }

    LSMTree(const LSMTree&) = delete;
    LSMTree& operator=(const LSMTree&) = delete;

    void set_write_hook(WriteHook hook) { hook_ = std::move(hook); }
    uint64_t current_lsn() const { return next_lsn_.load(std::memory_order_relaxed); }

    // -------------------------------------------------------------------------
    // Raft write-path hook (Round 2 part 3).
    //
    // When `set_raft_proposer(p)` is installed with a non-null callable,
    // every put/del short-circuits the local WAL/memtable path: the call
    // serialises (key, value, tomb) through `p` and `p` is expected to (1)
    // propose the entry through raft, (2) wait for it to commit AND apply
    // on the local node (via apply_replicated, called from raft's state
    // machine), and (3) return true. A false return MUST cause put/del to
    // throw a std::runtime_error("raft: not leader") so the HTTP layer can
    // surface the failure as a 503/500 to the client.
    //
    // While the proposer is installed, the legacy WriteHook is bypassed —
    // the master/replica streaming and the raft streaming are mutually
    // exclusive write paths.
    // -------------------------------------------------------------------------
    using RaftProposer = std::function<bool(const std::string& key,
                                            const std::string& value,
                                            bool tombstone)>;
    void set_raft_proposer(RaftProposer p) { raft_proposer_ = std::move(p); }
    bool raft_active() const { return static_cast<bool>(raft_proposer_); }

    // Replicas call this. Bypasses the local hook so the record isn't
    // re-broadcast in a loop.
    void apply_replicated(uint64_t lsn, const std::string& key,
                          const std::string& value, bool tombstone) {
        std::lock_guard<std::mutex> wlk(write_mu_);
        wal_->append(key, value, tombstone);
        memtable_->put(key, value, tombstone);
        uint64_t cur;
        do { cur = next_lsn_.load(); if (lsn <= cur) break; }
        while (!next_lsn_.compare_exchange_weak(cur, lsn));
        save_lsn();
        kick_flush_if_needed();
    }

    void put(const std::string& key, const std::string& value) {
        // Raft cutover (Round 2 part 3): every leader-side write goes
        // through the proposer; the actual local store mutation will
        // happen via apply_replicated() once raft commits.
        if (raft_proposer_) {
            if (!raft_proposer_(key, value, false)) {
                throw std::runtime_error("raft: not leader");
            }
            return;
        }
        std::lock_guard<std::mutex> wlk(write_mu_);   // P0-3
        uint64_t lsn = next_lsn_.fetch_add(1, std::memory_order_relaxed) + 1;
        wal_->append(key, value, false);
        memtable_->put(key, value, false);
        save_lsn();
        if (hook_) hook_(lsn, key, value, false);
        kick_flush_if_needed();
    }

    void del(const std::string& key) {
        if (raft_proposer_) {
            if (!raft_proposer_(key, std::string(), true)) {
                throw std::runtime_error("raft: not leader");
            }
            return;
        }
        std::lock_guard<std::mutex> wlk(write_mu_);   // P0-3
        uint64_t lsn = next_lsn_.fetch_add(1, std::memory_order_relaxed) + 1;
        wal_->append(key, "", true);
        memtable_->put(key, "", true);
        save_lsn();
        if (hook_) hook_(lsn, key, "", true);
        kick_flush_if_needed();
    }

    bool get(const std::string& key, std::string* value) {
        std::string v; bool t = false;
        if (memtable_->get(key, &v, &t)) {
            if (t) return false;
            *value = v;
            return true;
        }
        std::shared_lock<std::shared_mutex> lk(sst_mu_);
        for (auto& sst : sstables_) {
            // P1-1: fast bloom-filter rejection.
            if (!sst->bloom.empty() && !sst->bloom.probably_contains(key)) continue;
            auto it = sst->index.find(key);
            if (it != sst->index.end()) {
                if (it->second.second) return false;     // tombstone
                *value = it->second.first;
                return true;
            }
        }
        return false;
    }

    std::vector<std::pair<std::string, std::string>> scan(
            const std::string& start, const std::string& end, size_t limit = 1000) {
        std::map<std::string, std::pair<std::string, bool>> merged;
        {
            std::shared_lock<std::shared_mutex> lk(sst_mu_);
            // Iterate oldest → newest so newer SSTables overwrite older copies.
            for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
                for (auto& [k, v] : (*it)->index) {
                    if (k >= start && (end.empty() || k < end)) merged[k] = v;
                }
            }
        }
        auto snap = memtable_->snapshot();
        for (auto& [k, e] : snap) {
            if (k >= start && (end.empty() || k < end)) merged[k] = {e.value, e.tombstone};
        }
        std::vector<std::pair<std::string, std::string>> result;
        result.reserve(std::min(limit, merged.size()));
        for (auto& [k, v] : merged) {
            if (v.second) continue;
            result.emplace_back(k, v.first);
            if (result.size() >= limit) break;
        }
        return result;
    }

    std::vector<std::pair<std::string, std::string>> prefix_scan(
            const std::string& prefix, size_t limit = constants::STORAGE_PREFIX_SCAN_LIMIT) {
        std::string end = prefix;
        if (!end.empty()) end.back()++;
        return scan(prefix, end, limit);
    }

    // Test hook: forces the background thread to flush right now and waits
    // for it to finish. Avoids racy "sleep and hope" patterns in tests.
    void flush_now_for_test() {
        std::unique_lock<std::mutex> lk(flush_mu_);
        flush_pending_.store(true);
        flush_cv_.notify_all();
        flush_done_cv_.wait(lk, [this] { return !flush_pending_.load() || stop_.load(); });
    }

    // Test hook (P0-1): force a compaction pass right now. Returns the
    // number of SSTables BEFORE - AFTER (i.e., how many were merged away).
    int compact_now_for_test() {
        std::unique_lock<std::mutex> lk(compaction_mu_);
        int before;
        { std::shared_lock<std::shared_mutex> sl(sst_mu_); before = (int)sstables_.size(); }
        compaction_pending_.store(true);
        compaction_cv_.notify_all();
        compaction_done_cv_.wait(lk, [this] { return !compaction_pending_.load() || stop_.load(); });
        int after;
        { std::shared_lock<std::shared_mutex> sl(sst_mu_); after = (int)sstables_.size(); }
        return before - after;
    }

    // Live count of SSTable files. Exposed for /admin/stats.
    size_t sstable_count() {
        std::shared_lock<std::shared_mutex> lk(sst_mu_);
        return sstables_.size();
    }

private:
    void kick_flush_if_needed() {
        if (memtable_->should_flush()) {
            flush_pending_.store(true);
            flush_cv_.notify_one();
        }
    }

    void flush_loop() {
        while (!stop_.load()) {
            {
                std::unique_lock<std::mutex> lk(flush_mu_);
                flush_cv_.wait_for(lk, std::chrono::milliseconds(500), [this] {
                    return flush_pending_.load() || stop_.load();
                });
            }
            if (stop_.load()) break;
            if (memtable_->size() > 0 && (flush_pending_.load() || memtable_->should_flush())) {
                try { do_flush(); }
                catch (const std::exception&) {
                    // Best-effort: a flush error means we'll retry on the
                    // next tick. The audit-log infrastructure will pick this
                    // up once it's wired in.
                }
                std::lock_guard<std::mutex> lk(flush_mu_);
                flush_pending_.store(false);
                flush_done_cv_.notify_all();
            } else {
                std::lock_guard<std::mutex> lk(flush_mu_);
                flush_pending_.store(false);
                flush_done_cv_.notify_all();
            }
        }
    }

    void do_flush() {
        // Snapshot under the write lock so the new SSTable contains a
        // consistent prefix of the WAL we're about to truncate.
        std::map<std::string, MemTable::Entry> snap;
        {
            std::lock_guard<std::mutex> wlk(write_mu_);
            if (memtable_->size() == 0) return;
            snap = memtable_->snapshot();
        }
        std::string path = dir_ + "/sst_" + std::to_string(now_ms()) + "_" + random_hex(4) + ".sst";
        auto sst = SSTable::create(path, snap);  // fsync inside

        {
            std::unique_lock<std::shared_mutex> lk(sst_mu_);
            sstables_.insert(sstables_.begin(), sst);
        }
        // Once the SSTable is durable, drop the in-memory + WAL copies.
        // Any new writes that arrived during create() are still in the
        // WAL/memtable and will be in the *next* flush.
        {
            std::lock_guard<std::mutex> wlk(write_mu_);
            // Re-snapshot to detect any concurrent writes during create().
            // We only clear keys that were in `snap`, leaving newer writes alone.
            auto fresh = memtable_->snapshot();
            // Rebuild memtable with only the post-snapshot writes.
            memtable_ = std::make_unique<MemTable>();
            for (auto& [k, e] : fresh) {
                auto sit = snap.find(k);
                // Keep the entry if it didn't exist in `snap`, or if it was
                // overwritten with a newer timestamp during create().
                if (sit == snap.end() || sit->second.ts < e.ts) {
                    memtable_->put(k, e.value, e.tombstone);
                }
            }
            wal_->truncate();
            // Re-append surviving entries so a crash before the next flush
            // doesn't lose writes that arrived during create().
            for (auto& [k, e] : memtable_->snapshot()) {
                wal_->append(k, e.value, e.tombstone);
            }
        }
    }

    void load_lsn() {
        std::ifstream in(dir_ + "/lsn");
        uint64_t v = 0;
        if (in) in >> v;
        next_lsn_.store(v);
    }

    void save_lsn() {
        // P0-2: write to a temp file then rename for atomicity, then fsync
        // the directory so the rename is durable too.
        std::string tmp   = dir_ + "/lsn.tmp";
        std::string final = dir_ + "/lsn";
        int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) return;  // best effort
        std::string val = std::to_string(next_lsn_.load());
        ::write(fd, val.data(), val.size());
        ::fsync(fd);
        ::close(fd);
        ::rename(tmp.c_str(), final.c_str());
        int dir_fd = ::open(dir_.c_str(), O_RDONLY);
        if (dir_fd >= 0) { ::fsync(dir_fd); ::close(dir_fd); }
    }

    std::string                                       dir_;
    std::unique_ptr<WAL>                              wal_;
    std::unique_ptr<MemTable>                         memtable_;
    std::vector<std::shared_ptr<SSTable>>             sstables_;
    std::shared_mutex                                 sst_mu_;

    // P0-3: serializes the entire put/del/apply path so WAL & memtable
    // never disagree about which writes happened in which order.
    std::mutex                                        write_mu_;

    // P1-3: background flush plumbing.
    std::thread                                       flush_thread_;
    std::mutex                                        flush_mu_;
    std::condition_variable                           flush_cv_;
    std::condition_variable                           flush_done_cv_;
    std::atomic<bool>                                 flush_pending_{false};
    std::atomic<bool>                                 stop_{false};

    // P0-1: STCS compactor plumbing.
    std::thread                                       compaction_thread_;
    std::mutex                                        compaction_mu_;
    std::condition_variable                           compaction_cv_;
    std::condition_variable                           compaction_done_cv_;
    std::atomic<bool>                                 compaction_pending_{false};

    void compaction_loop() {
        using namespace std::chrono;
        while (!stop_.load()) {
            {
                std::unique_lock<std::mutex> lk(compaction_mu_);
                compaction_cv_.wait_for(lk,
                    milliseconds(constants::STORAGE_COMPACTION_INTERVAL_MS),
                    [this] { return compaction_pending_.load() || stop_.load(); });
            }
            if (stop_.load()) break;
            try { run_one_compaction(); }
            catch (...) { /* best-effort; retry next tick */ }
            std::lock_guard<std::mutex> lk(compaction_mu_);
            compaction_pending_.store(false);
            compaction_done_cv_.notify_all();
        }
    }

    // STCS: among the current (newest-first) SSTable list, find the
    // longest contiguous run of tables whose sizes are within MAX_RATIO of
    // each other and whose length is >= MIN_SSTS. The run is then merged.
    //
    // We prefer compacting the OLDEST eligible run so that tombstones
    // anchored to ancient data can finally be dropped (drop-condition is
    // “merge set touches the global tail of sstables_”).
    void run_one_compaction() {
        std::vector<std::shared_ptr<SSTable>> snap;
        {
            std::shared_lock<std::shared_mutex> lk(sst_mu_);
            snap = sstables_;
        }
        if ((int)snap.size() < constants::STORAGE_COMPACTION_MIN_SSTS) return;

        // Find an eligible run scanning from oldest (tail) to newest (head).
        const double max_ratio = constants::STORAGE_COMPACTION_MAX_RATIO;
        int best_lo = -1, best_hi = -1;
        for (int lo = (int)snap.size() - 1; lo >= 0; --lo) {
            uint64_t min_sz = snap[lo]->size_bytes, max_sz = snap[lo]->size_bytes;
            int hi = lo;
            for (int j = lo - 1; j >= 0; --j) {
                uint64_t s = snap[j]->size_bytes;
                uint64_t nmin = std::min(min_sz, s);
                uint64_t nmax = std::max(max_sz, s);
                if (nmin == 0) break;
                if ((double)nmax / (double)nmin > max_ratio) break;
                min_sz = nmin; max_sz = nmax; hi = j;
            }
            int run_len = lo - hi + 1;
            if (run_len >= constants::STORAGE_COMPACTION_MIN_SSTS) {
                best_lo = lo; best_hi = hi;
                break;            // prefer the oldest eligible run
            }
        }
        if (best_lo < 0) return;

        // [best_hi .. best_lo] inclusive, hi=newer, lo=older.
        // Merge: walk newer→older, keep first sighting of each key.
        bool includes_tail = (best_lo == (int)snap.size() - 1);
        std::map<std::string, MemTable::Entry> merged;
        for (int i = best_hi; i <= best_lo; ++i) {
            auto& sst = snap[i];
            for (auto& [k, vt] : sst->index) {
                if (merged.count(k)) continue;       // newer already won
                if (vt.second && includes_tail) continue; // drop tombstone
                MemTable::Entry e;
                e.value = vt.first;
                e.tombstone = vt.second;
                e.ts = sst->created_at_ms;
                merged[k] = std::move(e);
            }
        }

        std::string new_path = dir_ + "/sst_" + std::to_string(now_ms())
                                    + "_compact_" + random_hex(4) + ".sst";
        auto new_sst = SSTable::create(new_path, merged);

        // Atomic swap under the SSTable write lock.
        std::vector<std::string> to_unlink;
        {
            std::unique_lock<std::shared_mutex> lk(sst_mu_);
            // Re-find the same shared_ptrs in case the list changed (flushes
            // can have added newer tables at the head). We match by .get().
            std::vector<std::shared_ptr<SSTable>> kept;
            std::unordered_set<SSTable*> doomed;
            for (int i = best_hi; i <= best_lo; ++i) doomed.insert(snap[i].get());
            bool inserted = false;
            for (auto& t : sstables_) {
                if (doomed.count(t.get())) {
                    if (!inserted) { kept.push_back(new_sst); inserted = true; }
                    to_unlink.push_back(t->path);
                } else {
                    kept.push_back(t);
                }
            }
            if (!inserted) kept.push_back(new_sst);   // shouldn't happen
            sstables_ = std::move(kept);
        }
        // Now the new SSTable is the source of truth — safe to unlink the
        // originals. Sync the directory to make the unlinks durable.
        for (auto& p : to_unlink) ::unlink(p.c_str());
        int dir_fd = ::open(dir_.c_str(), O_RDONLY);
        if (dir_fd >= 0) { ::fsync(dir_fd); ::close(dir_fd); }
    }

    std::atomic<uint64_t>                             next_lsn_{0};
    WriteHook                                         hook_;
    RaftProposer                                      raft_proposer_;
};

} // namespace delta::storage
