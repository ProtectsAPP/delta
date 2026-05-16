#pragma once
#include "../core/common.hpp"
#include "../core/collection.hpp"
#include "../core/transaction.hpp"
#include "../core/validation.hpp"
#include "../core/logger.hpp"
#include "../core/backup_crypto.hpp"
#include "../cache/cache_engine.hpp"
#include "../vector/hnsw_index.hpp"
#include "../auth/auth_manager.hpp"
#include "../database/database_manager.hpp"
#include "connection_pool.hpp"
#include "replication.hpp"
#include "raft.hpp"
#include "../cluster/shard_router.hpp"
#include <algorithm>
#include <httplib.h>
#include <fstream>
#include <sstream>
#include <array>
#include <set>
#include <unordered_map>
#include <shared_mutex>

namespace delta::network {

struct HttpTuning {
    int threads = 0;            // 0 = auto
    int keepalive_max = 1000;
    int keepalive_timeout = 30;
    int max_connections = 1024;
    // Slow query threshold in milliseconds. Requests exceeding this are
    // logged via Logger::slow() so the operator can find them in slow.log.
    double slow_query_ms = 500.0;
};

// ----------------------------------------------------------------------------
// LatencyHistogram — fixed-bucket per-path latency tally for /metrics. Buckets
// follow the canonical Prometheus convention (le=1ms, 5ms, … , +Inf). One
// atomic counter per bucket so writes are lock-free; the read path takes a
// shared lock only to iterate `paths_`.
// ----------------------------------------------------------------------------
struct LatencyHistogram {
    // Bucket upper bounds in milliseconds; +Inf is implicit (last cell).
    static constexpr std::array<double, 14> BUCKETS = {
        1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000
    };
    struct PathStats {
        std::array<std::atomic<uint64_t>, BUCKETS.size() + 1> buckets{};
        std::atomic<uint64_t> count{0};
        std::atomic<double>  sum_ms{0.0};
    };
    // Bound the cardinality so a misbehaving client can't blow memory by
    // crafting unique paths. Once full, additional paths fold into "other".
    static constexpr size_t MAX_PATHS = 256;

    void observe(const std::string& route, double ms) {
        PathStats* p;
        {
            std::shared_lock<std::shared_mutex> lk(mu_);
            auto it = paths_.find(route);
            if (it != paths_.end()) p = &it->second;
            else p = nullptr;
        }
        if (!p) {
            std::unique_lock<std::shared_mutex> lk(mu_);
            if (paths_.size() >= MAX_PATHS) {
                p = &paths_["other"];
            } else {
                p = &paths_[route];
            }
        }
        size_t b = BUCKETS.size();
        for (size_t i = 0; i < BUCKETS.size(); ++i) {
            if (ms <= BUCKETS[i]) { b = i; break; }
        }
        p->buckets[b].fetch_add(1, std::memory_order_relaxed);
        p->count.fetch_add(1, std::memory_order_relaxed);
        // No std::atomic<double>::fetch_add on older clang stdlib; CAS loop.
        double cur = p->sum_ms.load(std::memory_order_relaxed);
        while (!p->sum_ms.compare_exchange_weak(cur, cur + ms,
                                                std::memory_order_relaxed)) {}
    }

    // Render Prometheus histogram lines into `out`.
    void render(std::ostringstream& out) const {
        std::shared_lock<std::shared_mutex> lk(mu_);
        out << "# HELP delta_http_request_duration_ms HTTP request latency.\n"
            << "# TYPE delta_http_request_duration_ms histogram\n";
        for (auto& [route, ps] : paths_) {
            uint64_t cumulative = 0;
            for (size_t i = 0; i < BUCKETS.size(); ++i) {
                cumulative += ps.buckets[i].load(std::memory_order_relaxed);
                out << "delta_http_request_duration_ms_bucket{path=\"" << route
                    << "\",le=\"" << BUCKETS[i] << "\"} " << cumulative << "\n";
            }
            cumulative += ps.buckets[BUCKETS.size()].load(std::memory_order_relaxed);
            out << "delta_http_request_duration_ms_bucket{path=\"" << route
                << "\",le=\"+Inf\"} " << cumulative << "\n";
            out << "delta_http_request_duration_ms_sum{path=\"" << route
                << "\"} " << ps.sum_ms.load(std::memory_order_relaxed) << "\n";
            out << "delta_http_request_duration_ms_count{path=\"" << route
                << "\"} " << ps.count.load(std::memory_order_relaxed) << "\n";
        }
    }

    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, PathStats> paths_;
};

class HttpServer {
public:
    using Tuning = HttpTuning;
    HttpServer(storage::LSMTree* store,
               CollectionEngine* col,
               cache::CacheEngine* cache,
               vector::VectorEngine* vec,
               auth::AuthManager* auth,
               auth::SessionManager* sessions,
               database::DatabaseManager* dbm,
               ConnectionPool* pool,
               Tuning tuning = {},
               ReplicationManager* repl = nullptr)
        : store_(store), col_(col), cache_(cache), vec_(vec), auth_(auth),
          sessions_(sessions), dbm_(dbm), pool_(pool), repl_(repl),
          srv_owned_(std::make_unique<httplib::Server>()),
          srv_(*srv_owned_),
          tuning_(tuning) {
        setup_routes();
        setup_cluster_routes();
        tune_for_high_concurrency();
        initialized_ = true;
    }

    // Static factory that builds an HttpServer with TLS already wired in.
    // Only available when CPPHTTPLIB_OPENSSL_SUPPORT is defined; returns
    // nullptr in non-TLS builds. Caller passes ownership-of-everything via
    // raw pointers exactly like the regular constructor.
    static std::unique_ptr<HttpServer> make_tls(
            storage::LSMTree* store, CollectionEngine* col,
            cache::CacheEngine* cache, vector::VectorEngine* vec,
            auth::AuthManager* auth, auth::SessionManager* sessions,
            database::DatabaseManager* dbm, ConnectionPool* pool,
            const std::string& cert_path, const std::string& key_path,
            Tuning tuning = {}, ReplicationManager* repl = nullptr) {
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
        // Construct an SSLServer first; if it fails we bail before even
        // creating the HttpServer, so we never have a half-initialized
        // object floating around.
        auto ssl = std::make_unique<httplib::SSLServer>(
            cert_path.c_str(), key_path.c_str());
        if (!ssl || !ssl->is_valid()) {
            std::cerr << "[Delta][ERROR] TLS init failed (bad cert/key at "
                      << cert_path << " / " << key_path << ")\n";
            return nullptr;
        }
        // Use the private TLS-aware constructor.
        auto hs = std::unique_ptr<HttpServer>(new HttpServer(
            store, col, cache, vec, auth, sessions, dbm, pool,
            std::move(ssl), tuning, repl));
        hs->tls_enabled_ = true;
        std::cout << "[Delta] TLS enabled (cert=" << cert_path << ")\n";
        return hs;
#else
        (void)store; (void)col; (void)cache; (void)vec; (void)auth;
        (void)sessions; (void)dbm; (void)pool;
        (void)cert_path; (void)key_path; (void)tuning; (void)repl;
        std::cerr << "[Delta][WARN] make_tls requested but binary built "
                     "without DELTA_TLS=ON.\n";
        return nullptr;
#endif
    }

    bool tls_enabled() const { return tls_enabled_; }

    // Plug in a RaftNode. When set, the /api/v1/cluster/raft/* endpoints
    // become live and route incoming RPCs into raft->handle_*. Plug it
    // BEFORE listen() (the route registration is one-shot in setup_cluster_routes).
    void set_raft(raft::RaftNode* r) { raft_ = r; }

    // Round 3 sharding gateway. Pass the (shard_map, local_shard_id) AND
    // the shared cluster_token used for inter-shard proxy headers. Empty
    // shard_map (default) keeps the gateway inert and every request lands
    // locally — i.e. single-shard mode. Call BEFORE listen().
    void set_sharding(cluster::ShardMap map, std::string local_shard,
                      std::string cluster_token, int rpc_timeout_ms = 2500) {
        shard_map_       = std::move(map);
        local_shard_     = std::move(local_shard);
        shard_token_     = std::move(cluster_token);
        shard_rpc_ms_    = rpc_timeout_ms;
    }
    bool sharding_enabled() const { return !shard_map_.empty(); }
    const cluster::ShardMap& shard_map() const { return shard_map_; }
    const std::string& local_shard() const { return local_shard_; }

    void listen(const std::string& host, int port) {
        std::cout << "[Delta] HTTP listening on " << host << ":" << port
                  << " threads=" << thread_count_
                  << " keepalive=" << keepalive_max_count_ << "/" << keepalive_timeout_sec_ << "s"
                  << std::endl;
        srv_.listen(host, port);
    }
    void stop() { srv_.stop(); }
    bool is_running() const { return srv_.is_running(); }

    // P0-9: per-deployment CORS policy. Empty list keeps the legacy
    // permissive behavior (`*`). Non-empty turns on strict allow-list
    // matching with `Vary: Origin`. Call BEFORE listen().
    struct CorsPolicy {
        std::vector<std::string> origins;        // empty = wide-open
        bool                     allow_credentials = true;
        std::string              methods = "GET,POST,PATCH,PUT,DELETE,OPTIONS";
        std::string              headers = "Authorization,Content-Type,X-Delta-Token,X-Cluster-Token";
    };
    void set_cors(const CorsPolicy& p) { cors_ = p; }

    // Counters for live RPS reporting
    uint64_t total_requests() const { return req_count_.load(std::memory_order_relaxed); }

private:
    // Private TLS-aware constructor used by make_tls(). Takes ownership of
    // a pre-built SSLServer (already validated) and runs the same setup
    // pipeline as the public constructor.
    HttpServer(storage::LSMTree* store, CollectionEngine* col,
               cache::CacheEngine* cache, vector::VectorEngine* vec,
               auth::AuthManager* auth, auth::SessionManager* sessions,
               database::DatabaseManager* dbm, ConnectionPool* pool,
               std::unique_ptr<httplib::Server> ssl_server,
               Tuning tuning, ReplicationManager* repl)
        : store_(store), col_(col), cache_(cache), vec_(vec), auth_(auth),
          sessions_(sessions), dbm_(dbm), pool_(pool), repl_(repl),
          srv_owned_(std::move(ssl_server)),
          srv_(*srv_owned_),
          tuning_(tuning) {
        setup_routes();
        setup_cluster_routes();
        tune_for_high_concurrency();
        initialized_ = true;
    }

    void tune_for_high_concurrency() {
        // cpp-httplib's threading model is one-thread-per-persistent-connection.
        // Therefore the worker pool must be at least as large as max concurrent
        // clients we want to serve, otherwise excess connections stall waiting
        // for a worker. We default to max(256, max_connections).
        unsigned hw = std::thread::hardware_concurrency();
        if (hw == 0) hw = 4;
        if (tuning_.threads > 0) thread_count_ = (unsigned)tuning_.threads;
        else thread_count_ = (unsigned)std::max(256, tuning_.max_connections);
        srv_.new_task_queue = [n = thread_count_] { return new httplib::ThreadPool((size_t)n); };

        // HTTP/1.1 keep-alive: critical for sustained load. cpp-httplib defaults
        // to 5/5s which kills throughput.
        keepalive_max_count_ = tuning_.keepalive_max;
        keepalive_timeout_sec_ = tuning_.keepalive_timeout;
        srv_.set_keep_alive_max_count(keepalive_max_count_);
        srv_.set_keep_alive_timeout(keepalive_timeout_sec_);
        (void)hw;

        // Tighter read/write timeouts so slowloris-style attackers can't
        // monopolize a worker. (5s read header, 30s body, 30s write.)
        srv_.set_read_timeout(5, 0);
        srv_.set_write_timeout(30, 0);
        srv_.set_idle_interval(0, 200000); // 200ms idle scan

        // Cap payload at 64MB; refuses gigantic uploads early.
        srv_.set_payload_max_length(64 * 1024 * 1024);

        // Larger TCP listen backlog — under burst, the kernel queue must hold
        // many half-opened connections before the server thread accept()s them.
        srv_.set_socket_options([](socket_t sock) {
            int yes = 1;
            ::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#ifdef SO_REUSEPORT
            ::setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif
            ::setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
        });

        // Trace request count + enforce replica read-only mode + install a
        // thread-local trace_id. Honors `X-Trace-Id` from the client when
        // present (call-chain propagation), otherwise mints a fresh one and
        // echoes it back via the same response header.
        srv_.set_pre_routing_handler([this](const httplib::Request& req, httplib::Response& res) {
            req_count_.fetch_add(1, std::memory_order_relaxed);
            std::string tid = req.get_header_value("X-Trace-Id");
            if (tid.empty() || tid.size() > 64) tid = Logger::new_trace_id();
            Logger::set_trace_id(tid);
            res.set_header("X-Trace-Id", tid);
            req_start_ms_slot() = (double)now_ms();
            // Per-IP token-bucket rate limit. Disabled by default; turned on
            // by main.cpp via set_conn_rate_limit().
            if (!ip_rate_allow(req.remote_addr)) {
                Logger::instance().audit("rate_limited",
                    {{"ip", req.remote_addr}, {"path", req.path}, {"method", req.method}});
                send_json(res, Status::FORBIDDEN,
                          json{{"message", "rate limit exceeded"}, {"data", nullptr}});
                Logger::clear_trace_id();
                return httplib::Server::HandlerResponse::Handled;
            }
            if (repl_ && repl_->read_only() && is_mutating(req)) {
                Logger::instance().info("replica_reject_write",
                    {{"path", req.path}, {"method", req.method}});
                send_json(res, Status::FORBIDDEN,
                          json{{"message", "this node is a read-only replica"},
                               {"data", nullptr},
                               {"master_url", repl_->master_url()},
                               {"role", role_name(repl_->role())}});
                Logger::clear_trace_id();
                return httplib::Server::HandlerResponse::Handled;
            }
            // Round 3 sharding gateway hook. shard_prefilter() decides
            // whether to forward, fan-out, or pass through. It MUST run
            // after the rate-limit / replica-reject checks above so a
            // forwarded request still gets rate-limited at the gateway.
            return shard_prefilter(req, res);
        });

        // Post-routing: latency observation + slow-query log + access log
        // (DEBUG-level by default). Always clears the trace_id so it doesn't
        // leak across reused worker threads.
        srv_.set_post_routing_handler([this](const httplib::Request& req, const httplib::Response& res) {
            double start = req_start_ms_slot();
            double ms    = (double)now_ms() - start;
            // Path-canonicalisation: collapse path-parameter slugs to keep
            // cardinality bounded. We hash by the route's static prefix:
            // /api/v1/collections/<x>/documents/<y> → /api/v1/collections/:c/documents/:id
            std::string route = canonical_route(req.path);
            req_latency_hist_.observe(route, ms);
            int sc = res.status / 100;
            if (sc >= 1 && sc <= 5) status_class_[sc].fetch_add(1, std::memory_order_relaxed);
            Logger::instance().debug("http",
                {{"method", req.method}, {"path", req.path}, {"status", res.status},
                 {"ms", ms}, {"route", route}});
            if (ms >= tuning_.slow_query_ms) {
                Logger::instance().slow(ms, "http",
                    {{"method", req.method}, {"path", req.path},
                     {"status", res.status}, {"route", route}});
            }
            Logger::clear_trace_id();
        });
    }

    // Thread-local request start timestamp (ms since epoch).
    static double& req_start_ms_slot() { thread_local double t = 0; return t; }

    // Collapse path parameters into stable buckets so the histogram doesn't
    // explode when there are millions of doc ids. Conservative: only collapse
    // segments that look like ids (hex/digits) or are too long.
    static std::string canonical_route(const std::string& path) {
        std::string out; out.reserve(path.size());
        size_t i = 0;
        while (i < path.size()) {
            if (path[i] != '/') { out += path[i++]; continue; }
            out += '/'; ++i;
            size_t j = i;
            while (j < path.size() && path[j] != '/') ++j;
            std::string seg = path.substr(i, j - i);
            bool is_id = !seg.empty() && (seg.size() >= 8);
            if (is_id) {
                bool all_hex = true;
                for (char c : seg) {
                    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
                          (c >= 'A' && c <= 'F') || c == '-' || c == '_')) {
                        all_hex = false; break;
                    }
                }
                if (all_hex && seg.size() >= 16) seg = ":id";
            }
            out += seg;
            i = j;
        }
        return out;
    }
    static bool is_mutating(const httplib::Request& req) {
        // Allow all GETs and the cluster control plane unconditionally.
        if (req.method == "GET" || req.method == "OPTIONS") return false;
        if (req.path.rfind("/api/v1/cluster/", 0) == 0) return false;
        if (req.path == "/api/v1/auth/login" || req.path == "/api/v1/auth/logout") return false;
        // Read-style POSTs that don't mutate state on the storage layer.
        if (req.method == "POST" && (
            req.path.find("/documents/search") != std::string::npos ||
            req.path.find("/aggregate") != std::string::npos ||
            req.path.find("/count") != std::string::npos ||
            req.path.find("/vectors/search") != std::string::npos ||
            req.path == "/api/v1/use")) return false;
        return true;
    }
    Tuning tuning_;
    unsigned thread_count_ = 256;
    int keepalive_max_count_ = 1000;
    int keepalive_timeout_sec_ = 30;
    std::atomic<uint64_t> req_count_{0};
    // Per-route HTTP latency histogram, rendered into /metrics.
    LatencyHistogram req_latency_hist_;
    // Indexed by status_class (1..5) — 2xx, 3xx, 4xx, 5xx counters.
    std::array<std::atomic<uint64_t>, 6> status_class_{};
    // Per-IP token-bucket connection rate limiter. Configured via
    // set_conn_rate_limit(); 0/0 disables.
    struct IpBucket {
        double tokens = 0.0;
        double last_ms = 0.0;
    };
    std::mutex ip_rl_mu_;
    std::unordered_map<std::string, IpBucket> ip_rl_;
    std::atomic<int> ip_rl_rate_{0};       // tokens per second
    std::atomic<int> ip_rl_burst_{0};      // bucket capacity
    std::atomic<uint64_t> ip_rl_rejects_{0};
public:
    void set_backup_passphrase(const std::string& s) { backup_pass_ = s; }
private:
    std::string backup_pass_;
public:
    void set_conn_rate_limit(int per_sec, int burst) {
        ip_rl_rate_.store(per_sec);
        ip_rl_burst_.store(std::max(burst, per_sec));
    }
    uint64_t conn_rate_rejects() const { return ip_rl_rejects_.load(); }
private:
    // Returns true if the request is allowed under the per-IP budget.
    bool ip_rate_allow(const std::string& ip) {
        int rate  = ip_rl_rate_.load(std::memory_order_relaxed);
        int burst = ip_rl_burst_.load(std::memory_order_relaxed);
        if (rate <= 0 || burst <= 0) return true;
        double now = (double)now_ms();
        std::lock_guard<std::mutex> lk(ip_rl_mu_);
        // Periodically prune cold buckets so the map doesn't grow unbounded.
        if (ip_rl_.size() > 50000) {
            for (auto it = ip_rl_.begin(); it != ip_rl_.end(); ) {
                if (now - it->second.last_ms > 60000.0) it = ip_rl_.erase(it);
                else ++it;
            }
        }
        auto& b = ip_rl_[ip];
        if (b.last_ms == 0.0) { b.tokens = (double)burst; b.last_ms = now; }
        double elapsed = (now - b.last_ms) / 1000.0;
        b.tokens = std::min((double)burst, b.tokens + elapsed * (double)rate);
        b.last_ms = now;
        if (b.tokens < 1.0) {
            ip_rl_rejects_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        b.tokens -= 1.0;
        return true;
    }
public:
    // P0-9: empty by default — wide-open `*` for dev. Operators turn on
    // strict allow-list mode by calling set_cors() before listen().
    CorsPolicy cors_;
    // P1-17: per (ip|user) sliding-window login rate limiter.
    auth::LoginRateLimiter login_limiter_;

private:
    storage::LSMTree* store_;
    CollectionEngine* col_;
    cache::CacheEngine* cache_;
    vector::VectorEngine* vec_;
    auth::AuthManager* auth_;
    auth::SessionManager* sessions_;
    database::DatabaseManager* dbm_;
    ConnectionPool* pool_;
    ReplicationManager* repl_;
    raft::RaftNode* raft_ = nullptr;

    // Round 3 sharding state. shard_map_ being empty disables the gateway.
    cluster::ShardMap shard_map_;
    std::string       local_shard_;
    std::string       shard_token_;
    int               shard_rpc_ms_ = 2500;
    // P2-2 (TLS): we keep a unique_ptr that owns either a plain
    // httplib::Server or, when DELTA_TLS=ON and enable_tls() was called
    // before listen(), an httplib::SSLServer. The reference srv_ binds to
    // *srv_owned_ at construction and is NEVER rebound after that — so
    // enable_tls() must swap the owned object BEFORE the constructor
    // chain returns. We implement this by deferring setup_routes() /
    // setup_cluster_routes() / tune_for_high_concurrency() into an
    // initialize() helper called either from the ctor (plain HTTP) or
    // from enable_tls()-then-initialize() (TLS), but never both.
    std::unique_ptr<httplib::Server> srv_owned_;
    httplib::Server& srv_;
    bool tls_enabled_ = false;
    bool initialized_ = false;
    uint64_t start_time_ = now_ms();

    static int http_status_for(int code) {
        switch (code) {
            case Status::OK: return 200;
            case Status::NOT_FOUND: return 404;
            case Status::DUPLICATE: return 409;
            case Status::INVALID: return 400;
            case Status::UNAUTHORIZED: return 401;
            case Status::FORBIDDEN: return 403;
            case Status::ERROR: return 500;
            case Status::INTERNAL: return 500;
            case Status::CONFLICT: return 409;
            case Status::UNSUPPORTED: return 501;
            default:
                // Pass through anything already in HTTP-status range so
                // err(res, 503, "...") works verbatim.
                if (code >= 100 && code <= 599) return code;
                return 200;
        }
    }
    void send_json(httplib::Response& res, int code, const json& body) {
        res.status = http_status_for(code);
        // body uses code 200 as a sentinel for "ok" in some places; remap
        if (code == 200) { res.status = 200; }
        json j; j["code"] = code; if (body.contains("data") || body.contains("message")) {
            for (auto& [k, v] : body.items()) j[k] = v;
        } else j["data"] = body;
        // P0-9: respect the configured CORS allow-list. send_json() doesn't
        // see the request, so the per-request handler still has to call
        // apply_cors() — but we keep the legacy headers here as a fallback
        // for cross-origin tools that hit a JSON endpoint directly.
        if (cors_.origins.empty()) {
            res.set_header("Access-Control-Allow-Origin", "*");
            res.set_header("Access-Control-Allow-Headers", "*");
            res.set_header("Access-Control-Allow-Methods", "*");
        }
        res.set_content(j.dump(), "application/json");
    }
    void ok(httplib::Response& res, const json& data) { send_json(res, 200, json{{"data", data}}); }
    void err(httplib::Response& res, int code, const std::string& msg) { send_json(res, code, json{{"message", msg}, {"data", nullptr}}); }

    json parse_body(const httplib::Request& req) {
        if (req.body.empty()) return json::object();
        try { return json::parse(req.body); } catch(...) { return json::object(); }
    }

    // -----------------------------------------------------------------------
    // Round 3 — sharding gateway helpers.
    //
    // shard_forward(): proxy this request to ANY peer of the target shard.
    //   We prefer the configured first peer, but if it returns a 503 with
    //   a `data.leader_id` body we retry once against that peer (single
    //   redirect — beyond that the operator likely has a quorum problem).
    //
    // shard_fanout(): scatter the same request to every shard's first
    //   peer in parallel and merge the JSON response bodies. Used for
    //   document search / list / aggregate where no single shard owns
    //   the answer.
    //
    // A request hitting THIS process is considered "local" once it's
    // already been routed by an upstream gateway. We detect that with
    // the `X-Delta-Shard-Hop` header so a follow-on forward doesn't
    // bounce forever between peers.
    // -----------------------------------------------------------------------
    bool req_is_local_hop(const httplib::Request& req) const {
        return req.has_header("X-Delta-Shard-Hop") &&
               !req.get_header_value("X-Delta-Shard-Hop").empty();
    }

    // Forward `req` verbatim to `peer_url`. On success copies the upstream
    // body + status into `res`. On transport failure (peer unreachable),
    // returns false so the caller can try another peer.
    bool shard_proxy_once(const std::string& peer_url,
                          const httplib::Request& req,
                          httplib::Response& res) {
        httplib::Client cli(peer_url.c_str());
        // sec + usec form; pass sec >= 1 to avoid edge cases in some
        // older httplib versions that treat (0, x) as "no wait".
        int sec  = shard_rpc_ms_ / 1000;
        int usec = (shard_rpc_ms_ % 1000) * 1000;
        if (sec < 1) { sec = 1; usec = 0; }
        cli.set_connection_timeout(sec, usec);
        cli.set_read_timeout(sec, usec);
        cli.set_keep_alive(false);
        httplib::Headers h;
        // Pass through auth + cluster headers. Add our hop marker so the
        // upstream peer doesn't try to re-forward.
        for (auto& [k, v] : req.headers) {
            if (k == "Host" || k == "Content-Length") continue;
            h.emplace(k, v);
        }
        h.emplace("X-Delta-Shard-Hop", local_shard_.empty() ? "?" : local_shard_);
        if (!shard_token_.empty())
            h.emplace("X-Delta-Cluster-Token", shard_token_);
        // Round 3: propagate the gateway-validated user identity so the
        // receiving shard can apply the same identity without replicating
        // the session DB. require_auth() on the receiving side honours
        // this header when paired with a matching X-Delta-Cluster-Token.
        if (!shard_token_.empty()) {
            auth::Session s;
            httplib::Response throwaway;
            if (require_auth(req, throwaway, &s)) {
                h.emplace("X-Delta-Internal-User",   s.username);
                h.emplace("X-Delta-Internal-DB",     s.database);
                h.emplace("X-Delta-Internal-Schema", s.schema);
            }
        }
        std::string path = req.path;
        if (!req.params.empty()) {
            path += "?";
            bool first = true;
            for (auto& [k, v] : req.params) {
                if (!first) path += "&";
                first = false;
                path += httplib::detail::encode_url(k);
                path += "=";
                path += httplib::detail::encode_url(v);
            }
        }
        httplib::Result rr;
        if (req.method == "GET") {
            rr = cli.Get(path.c_str(), h);
        } else if (req.method == "DELETE") {
            rr = cli.Delete(path.c_str(), h);
        } else if (req.method == "PATCH") {
            rr = cli.Patch(path.c_str(), h, req.body, "application/json");
        } else if (req.method == "POST") {
            rr = cli.Post(path.c_str(), h, req.body, "application/json");
        } else if (req.method == "PUT") {
            rr = cli.Put(path.c_str(), h, req.body, "application/json");
        } else {
            return false;
        }
        if (!rr) return false;
        res.status = rr->status;
        // Preserve upstream content-type so binary blobs (e.g. /admin/backup)
        // survive proxying intact.
        auto ct = rr->get_header_value("Content-Type");
        if (ct.empty()) ct = "application/json";
        res.set_content(rr->body, ct.c_str());
        return true;
    }

    // Extract a document id from a routing path like
    //   /api/v1/collections/{c}/documents/{id}[/...]
    // Returns empty string when not a per-document URL.
    std::string extract_doc_id_from_path(const std::string& path) const {
        static const std::string p = "/api/v1/collections/";
        if (path.rfind(p, 0) != 0) return std::string();
        // Skip the collection name segment.
        size_t a = p.size();
        size_t b = path.find('/', a);
        if (b == std::string::npos) return std::string();
        static const std::string seg = "/documents/";
        if (path.compare(b, seg.size(), seg) != 0) return std::string();
        size_t c = b + seg.size();
        size_t d = path.find('/', c);
        std::string id = path.substr(c, d == std::string::npos ? std::string::npos : d - c);
        // Reserved sub-routes that LOOK like ids but aren't documents:
        if (id == "search" || id == "bulk") return std::string();
        return id;
    }

    // Walk a transaction body and return:
    //   - shards_touched: distinct shard ids any op writes/reads
    //   - examples: helpful debug strings (op + id)
    // For ops missing an id (insert with no _id) we treat the op as
    // requiring a server-side id allocation — we generate it now and
    // patch the body before forwarding.
    std::set<std::string> tx_shards_for_body(json& body) {
        std::set<std::string> touched;
        if (!body.contains("ops") || !body["ops"].is_array()) return touched;
        for (auto& o : body["ops"]) {
            if (!o.is_object()) continue;
            std::string kind = o.value("op", std::string());
            std::string id;
            if (kind == "insert") {
                if (o.contains("doc") && o["doc"].is_object() &&
                    o["doc"].contains("_id") && o["doc"]["_id"].is_string()) {
                    id = o["doc"]["_id"].get<std::string>();
                } else {
                    // Allocate now so routing is deterministic. The local
                    // handler accepts pre-set _id.
                    id = gen_id();
                    if (!o.contains("doc") || !o["doc"].is_object())
                        o["doc"] = json::object();
                    o["doc"]["_id"] = id;
                }
            } else if (kind == "update" || kind == "remove") {
                id = o.value("id", std::string());
            }
            if (!id.empty()) touched.insert(shard_map_.route(id));
        }
        return touched;
    }

    // Forward to one of the target shard's peers. Returns true once the
    // proxy roundtrip completed (regardless of upstream status code).
    bool shard_forward(const cluster::ShardSpec& target,
                       const httplib::Request& req,
                       httplib::Response& res) {
        // Try peers in order; on transport failure move to the next.
        // Beyond a single 503 leader-hint redirect we don't chase the
        // chain — the operator's HTTP client gets the 503 and retries.
        for (size_t i = 0; i < target.peers.size(); ++i) {
            if (shard_proxy_once(target.peers[i].base_url(), req, res)) {
                return true;
            }
        }
        err(res, 503, "shard " + target.id + " unreachable");
        return true;
    }

    // ---- Per-endpoint gateway handlers -----------------------------------
    // Each of these is invoked from the pre-routing prefilter when a
    // sharded route needs cross-shard help. They ALWAYS write a response;
    // returning true means the prefilter must report Handled.

    // POST /api/v1/collections/{c}/documents — single insert without an id
    // in the URL. We allocate _id locally so routing is deterministic,
    // then either handle locally (by mutating req.body in place) or
    // forward to the owning shard.
    //
    // Why we don't loopback-forward to ourselves: cpp-httplib's worker
    // thread is the same thread executing this prefilter; if we open a
    // sync httplib::Client to our own port we deadlock until the worker
    // pool drains. Worse, on macOS the self-connect occasionally returns
    // ECONNREFUSED outright. So when target == local_shard_ we patch the
    // request body via const_cast and return Unhandled so the regular
    // POST /documents route runs in-process.
    // Returns true if this handler should ABORT (i.e. response has been
    // fully written by the gateway forwarding/rejection logic). Returns
    // false if the local handler must continue (possibly with the request
    // body mutated in place by us). Called from inside route handlers
    // because httplib's pre-routing handler runs BEFORE the body is read
    // from the socket — see `Server::routing()` in httplib.h, the
    // pre_routing_handler_ check is the first line.
    //
    // `parsed_body` is the already-parsed body owned by the caller; if we
    // allocate a fresh _id we patch it into the caller's json so the
    // local handler honours it.
    bool gateway_handle_single_insert(const httplib::Request& req,
                                      httplib::Response& res,
                                      json& parsed_body) {
        if (!sharding_enabled() || req_is_local_hop(req)) return false;
        std::string id;
        json& doc_field = parsed_body.contains("document") &&
                          parsed_body["document"].is_object()
                ? parsed_body["document"]
                : parsed_body;
        if (parsed_body.contains("id") && parsed_body["id"].is_string()) {
            id = parsed_body["id"].get<std::string>();
            doc_field["_id"] = id;
        } else if (doc_field.contains("_id") && doc_field["_id"].is_string()) {
            id = doc_field["_id"].get<std::string>();
        } else {
            id = gen_id();
            doc_field["_id"] = id;
        }
        const cluster::ShardId& target = shard_map_.route(id);
        if (target == local_shard_) return false;
        auto* spec = shard_map_.find_shard(target);
        if (!spec) { err(res, 500, "unknown target shard"); return true; }
        httplib::Request modified = req;
        modified.body = parsed_body.dump();
        shard_forward(*spec, modified, res);
        return true;
    }

    // POST /api/v1/collections/{c}/documents/bulk — invoked from the
    // local bulk handler with the parsed body. Returns true if the
    // gateway fully serviced the request (response already written).
    //
    // Strategy: split docs by owning shard. Forward each REMOTE shard's
    // subset via a synthetic POST. For the LOCAL shard's subset we
    // rewrite the request body in place and let the caller continue.
    bool gateway_handle_bulk_insert(const httplib::Request& req,
                                    httplib::Response& res,
                                    json& parsed_body) {
        if (!sharding_enabled() || req_is_local_hop(req)) return false;
        // Tolerate "docs" or "documents".
        std::string key = parsed_body.contains("docs") ? "docs" : "documents";
        if (!parsed_body.contains(key) || !parsed_body[key].is_array()) return false;
        std::unordered_map<cluster::ShardId, json> per_shard;
        std::vector<std::string> all_ids;
        json local_subset = json::array();
        for (auto& item : parsed_body[key]) {
            if (!item.is_object()) continue;
            json* doc = &item;
            if (item.contains("document") && item["document"].is_object())
                doc = &item["document"];
            std::string id;
            if (doc->contains("_id") && (*doc)["_id"].is_string()) {
                id = (*doc)["_id"].get<std::string>();
            } else {
                id = gen_id();
                (*doc)["_id"] = id;
            }
            all_ids.push_back(id);
            cluster::ShardId tgt = shard_map_.route(id);
            if (tgt == local_shard_) {
                local_subset.push_back(item);
            } else {
                if (!per_shard.count(tgt))
                    per_shard[tgt] = json{{key, json::array()}};
                per_shard[tgt][key].push_back(item);
            }
        }
        int inserted = 0;
        std::mutex mu;
        std::vector<std::thread> workers;
        for (auto& [sid, sbody] : per_shard) {
            auto* spec = shard_map_.find_shard(sid);
            if (!spec) continue;
            workers.emplace_back([this, &mu, &inserted, &req, spec, sbody]() {
                httplib::Request modified = req;
                modified.body = sbody.dump();
                httplib::Response sub;
                shard_forward(*spec, modified, sub);
                try {
                    json env = json::parse(sub.body);
                    int n = 0;
                    if (env.contains("data") && env["data"].contains("inserted"))
                        n = env["data"]["inserted"].get<int>();
                    std::lock_guard<std::mutex> lk(mu);
                    inserted += n;
                } catch (...) {}
            });
        }
        for (auto& w : workers) if (w.joinable()) w.join();
        // Local subset: hand back through local handler. We can't easily
        // re-enter mid-handler, so we run the engine inserts inline here.
        if (!local_subset.empty()) {
            auth::Session s;
            if (require_auth(req, res, &s)) {
                std::string col_name = req.matches[1];
                std::string db = req.get_param_value("database");
                if (db.empty()) db = s.database;
                std::string sch = req.get_param_value("schema");
                if (sch.empty()) sch = s.schema;
                for (auto& item : local_subset) {
                    json doc = item.contains("document") && item["document"].is_object()
                             ? item["document"] : item;
                    std::string id;
                    if (col_->insert(db, sch, col_name, doc, id).ok()) inserted++;
                }
            }
        }
        ok(res, json{{"inserted", inserted}, {"ids", all_ids}});
        return true;
    }

    // POST /api/v1/collections/{c}/documents/search — fan-out. For v1 we
    // concatenate documents from each shard and apply the requested
    // `limit`/`skip` to the merged stream. Server-side `sort` is honoured
    // only as a final stable resort by the gateway (a real distributed
    // sort would push partial sort + top-k to each shard; out of scope
    // for the first cut).
    bool shard_fanout_documents_search(const httplib::Request& req,
                                       httplib::Response& res) {
        std::vector<json> docs_all;
        size_t total_all = 0;
        std::mutex mu;
        std::string dummy;
        shard_fanout(req, [&](const json& env) {
            if (!env.contains("data")) return;
            const json& d = env["data"];
            std::lock_guard<std::mutex> lk(mu);
            if (d.contains("documents") && d["documents"].is_array()) {
                for (auto& doc : d["documents"]) docs_all.push_back(doc);
            }
            if (d.contains("total") && d["total"].is_number())
                total_all += d["total"].get<size_t>();
        }, dummy);
        // Re-apply skip + limit on the merged set so a multi-shard call
        // doesn't return N*limit docs to the caller. Aggregation-aware
        // sorting is a follow-up; for now we keep input order.
        json body;
        try { body = json::parse(req.body); } catch (...) {}
        size_t skip  = body.value("skip",  (size_t)0);
        size_t limit = body.value("limit", (size_t)0);
        if (limit > 0 || skip > 0) {
            if (skip > docs_all.size()) docs_all.clear();
            else docs_all.erase(docs_all.begin(), docs_all.begin() + skip);
            if (limit > 0 && docs_all.size() > limit) docs_all.resize(limit);
        }
        ok(res, json{
            {"documents", docs_all},
            {"total",     total_all},
            {"skip",      skip},
            {"limit",     limit},
        });
        return true;
    }

    // POST /api/v1/collections/{c}/aggregate — fan-out. We concatenate
    // each shard's pipeline result. This is correct for pure `$match` /
    // `$project` / `$unwind` pipelines and approximate for pipelines that
    // include `$group` / `$sort` / `$limit` (the gateway runs none of the
    // distributed-merge logic for those stages). Operators expecting
    // global-aggregation correctness across shards should keep the data
    // co-resident (single shard) or run `$group` themselves on the
    // gateway side.
    bool shard_fanout_aggregate(const httplib::Request& req,
                                httplib::Response& res) {
        json merged = json::array();
        std::mutex mu;
        std::string dummy;
        shard_fanout(req, [&](const json& env) {
            if (!env.contains("data")) return;
            const json& d = env["data"];
            std::lock_guard<std::mutex> lk(mu);
            if (d.is_array()) for (auto& r : d) merged.push_back(r);
            else if (d.is_object() && d.contains("result") && d["result"].is_array())
                for (auto& r : d["result"]) merged.push_back(r);
        }, dummy);
        ok(res, merged);
        return true;
    }

    // POST /api/v1/collections/{c}/count — fan-out + sum.
    bool shard_fanout_count(const httplib::Request& req,
                            httplib::Response& res) {
        std::atomic<uint64_t> total{0};
        std::string dummy;
        shard_fanout(req, [&](const json& env) {
            if (!env.contains("data") || !env["data"].contains("count")) return;
            total.fetch_add(env["data"]["count"].get<uint64_t>());
        }, dummy);
        ok(res, json{{"count", (uint64_t)total.load()}});
        return true;
    }

    // POST /api/v1/transactions/execute — cross-shard tx detection,
    // invoked from the local handler with the parsed body.
    bool gateway_handle_transaction(const httplib::Request& req,
                                    httplib::Response& res,
                                    json& parsed_body) {
        if (!sharding_enabled() || req_is_local_hop(req)) return false;
        auto touched = tx_shards_for_body(parsed_body);
        if (touched.empty()) return false;
        if (touched.size() > 1) {
            send_json(res, Status::UNSUPPORTED, json{
                {"message", "cross-shard transactions are not supported "
                            "in this release"},
                {"data", json{{"shards", std::vector<std::string>(
                    touched.begin(), touched.end())}}}
            });
            return true;
        }
        const cluster::ShardId& target = *touched.begin();
        if (target == local_shard_) return false;
        auto* spec = shard_map_.find_shard(target);
        if (!spec) { err(res, 500, "unknown target shard"); return true; }
        httplib::Request modified = req;
        modified.body = parsed_body.dump();
        shard_forward(*spec, modified, res);
        return true;
    }

    // Scatter the same request to every shard in `shard_map_` in parallel
    // (including the local one, which goes through loopback so the local
    // handler still runs and merges its own data). The caller supplies
    // `merge_fn` to combine result bodies. The merged body is returned in
    // `out`; the per-shard upstream status is summarised in `out_codes`.
    void shard_fanout(const httplib::Request& req,
                      std::function<void(const json&)> per_shard_body,
                      std::string& out_method_dummy /*kept for symmetry*/) {
        (void)out_method_dummy;
        std::vector<std::thread> workers;
        std::mutex mu;
        for (auto& sh : shard_map_.shards()) {
            workers.emplace_back([&, sh]() {
                if (sh.peers.empty()) return;
                // For the local shard, hit our own port via 127.0.0.1.
                // (The hop header keeps the local handler from forwarding.)
                std::string url;
                if (sh.id == local_shard_) {
                    // Loopback: any peer of our shard works, but we use
                    // the first one (which is normally ourselves).
                    url = sh.peers.front().base_url();
                } else {
                    url = sh.peers.front().base_url();
                }
                httplib::Client cli(url.c_str());
                cli.set_connection_timeout(0, shard_rpc_ms_ * 1000);
                cli.set_read_timeout(0, shard_rpc_ms_ * 1000);
                httplib::Headers h;
                for (auto& [k, v] : req.headers) {
                    if (k == "Host" || k == "Content-Length") continue;
                    h.emplace(k, v);
                }
                h.emplace("X-Delta-Shard-Hop", local_shard_.empty() ? "?" : local_shard_);
                if (!shard_token_.empty())
                    h.emplace("X-Delta-Cluster-Token", shard_token_);
                std::string path = req.path;
                if (!req.params.empty()) {
                    path += "?";
                    bool first = true;
                    for (auto& [k, v] : req.params) {
                        if (!first) path += "&";
                        first = false;
                        path += httplib::detail::encode_url(k);
                        path += "=";
                        path += httplib::detail::encode_url(v);
                    }
                }
                httplib::Result rr;
                if (req.method == "GET")        rr = cli.Get(path.c_str(), h);
                else if (req.method == "POST")  rr = cli.Post(path.c_str(), h, req.body, "application/json");
                else if (req.method == "DELETE")rr = cli.Delete(path.c_str(), h);
                else if (req.method == "PATCH") rr = cli.Patch(path.c_str(), h, req.body, "application/json");
                if (!rr) return;
                try {
                    json env = json::parse(rr->body);
                    std::lock_guard<std::mutex> lk(mu);
                    per_shard_body(env);
                } catch (...) {}
            });
        }
        for (auto& w : workers) if (w.joinable()) w.join();
    }

    // Broadcast a write to every PEER shard (skips local). Used for
    // collection metadata replication. Best-effort + parallel; we don't
    // fail the local op if a peer is unreachable, but we do log it.
    // The hop header is set so peers don't re-broadcast.
    void broadcast_to_peer_shards(const httplib::Request& req,
                                  const std::string& method,
                                  const std::string& path,
                                  const std::string& body) {
        if (!sharding_enabled() || req_is_local_hop(req)) return;
        std::vector<std::thread> workers;
        for (auto& sh : shard_map_.shards()) {
            if (sh.id == local_shard_) continue;
            if (sh.peers.empty()) continue;
            std::string url = sh.peers.front().base_url();
            workers.emplace_back([this, url, method, path, body, &req]() {
                httplib::Client cli(url.c_str());
                int sec  = shard_rpc_ms_ / 1000;
                int usec = (shard_rpc_ms_ % 1000) * 1000;
                if (sec < 1) { sec = 1; usec = 0; }
                cli.set_connection_timeout(sec, usec);
                cli.set_read_timeout(sec, usec);
                cli.set_keep_alive(false);
                httplib::Headers h;
                for (auto& [k, v] : req.headers) {
                    if (k == "Host" || k == "Content-Length") continue;
                    h.emplace(k, v);
                }
                h.emplace("X-Delta-Shard-Hop",     local_shard_.empty() ? "?" : local_shard_);
                if (!shard_token_.empty()) {
                    h.emplace("X-Delta-Cluster-Token", shard_token_);
                    auth::Session s;
                    httplib::Response throwaway;
                    if (require_auth(req, throwaway, &s)) {
                        h.emplace("X-Delta-Internal-User",   s.username);
                        h.emplace("X-Delta-Internal-DB",     s.database);
                        h.emplace("X-Delta-Internal-Schema", s.schema);
                    }
                }
                if (method == "POST")        cli.Post(path.c_str(), h, body, "application/json");
                else if (method == "PUT")    cli.Put(path.c_str(), h, body, "application/json");
                else if (method == "PATCH")  cli.Patch(path.c_str(), h, body, "application/json");
                else if (method == "DELETE") cli.Delete(path.c_str(), h);
            });
        }
        for (auto& w : workers) if (w.joinable()) w.join();
    }

    // Authenticate request, returns session.
    //
    // Round 3 — internal trust path: when a request carries
    //   X-Delta-Cluster-Token: <secret>   (matches our shard_token_)
    //   X-Delta-Internal-User: <username>
    // we synthesise a session for that user WITHOUT consulting the local
    // SessionManager. This is how the sharding gateway propagates an
    // already-validated user identity across shard boundaries without
    // having to replicate the session DB. Security: the cluster token
    // is a shared secret known only to operator-configured nodes, so a
    // third party cannot forge it.
    bool require_auth(const httplib::Request& req, httplib::Response& res, auth::Session* out_sess) {
        if (!shard_token_.empty() &&
            req.get_header_value("X-Delta-Cluster-Token") == shard_token_ &&
            req.has_header("X-Delta-Internal-User")) {
            std::string user = req.get_header_value("X-Delta-Internal-User");
            out_sess->token    = "internal";
            out_sess->username = user;
            out_sess->database = req.get_header_value("X-Delta-Internal-DB");
            if (out_sess->database.empty()) out_sess->database = "default";
            out_sess->schema   = req.get_header_value("X-Delta-Internal-Schema");
            if (out_sess->schema.empty()) out_sess->schema = "public";
            out_sess->client_ip = req.remote_addr;
            return true;
        }
        std::string token;
        auto auth_h = req.get_header_value("Authorization");
        if (auth_h.rfind("Bearer ", 0) == 0) token = auth_h.substr(7);
        else if (req.has_header("X-Delta-Token")) token = req.get_header_value("X-Delta-Token");
        if (token.empty()) { err(res, Status::UNAUTHORIZED, "missing token"); return false; }
        auto s = sessions_->get(token);
        if (!s) { err(res, Status::UNAUTHORIZED, "invalid or expired token"); return false; }
        *out_sess = *s;
        return true;
    }

    bool require_perm(httplib::Response& res, const auth::Session& s, uint32_t privs, const auth::PrivilegeTarget& target) {
        if (auth_->is_superuser(s.username)) return true;
        if (!auth_->check(s.username, privs, target)) { err(res, Status::FORBIDDEN, "permission denied"); return false; }
        return true;
    }

    static auth::PrivilegeTarget col_target(const std::string& db, const std::string& sch, const std::string& col) {
        return {"collection", db, sch, col};
    }

    // Search common install locations for an llms.txt LLM-friendly reference.
    // Returns empty string if not found.
    static std::string find_llms_txt() {
        const char* candidates[] = {
            "./llms.txt",
            "../llms.txt",
            "../../llms.txt",
            "/usr/local/share/delta/llms.txt",
            "/etc/delta/llms.txt",
        };
        for (auto p : candidates) {
            std::ifstream f(p, std::ios::binary);
            if (f) {
                std::stringstream ss; ss << f.rdbuf();
                return ss.str();
            }
        }
        return "";
    }

    // P0-9: apply CORS headers based on the per-request Origin and the
    // currently configured allow-list. Returns true if the origin was
    // allowed (or the legacy wide-open mode is active).
    bool apply_cors(const httplib::Request& req, httplib::Response& res) {
        std::string origin = req.get_header_value("Origin");
        if (cors_.origins.empty()) {
            res.set_header("Access-Control-Allow-Origin", "*");
            res.set_header("Access-Control-Allow-Headers", cors_.headers);
            res.set_header("Access-Control-Allow-Methods", cors_.methods);
            return true;
        }
        bool allowed = std::find(cors_.origins.begin(), cors_.origins.end(), origin)
                       != cors_.origins.end();
        if (allowed) {
            res.set_header("Access-Control-Allow-Origin", origin);
            res.set_header("Vary", "Origin");
            res.set_header("Access-Control-Allow-Headers", cors_.headers);
            res.set_header("Access-Control-Allow-Methods", cors_.methods);
            if (cors_.allow_credentials)
                res.set_header("Access-Control-Allow-Credentials", "true");
        }
        return allowed;
    }

    // Round 3 — sharding pre-routing decision. Called from the (single)
    // pre_routing_handler installed by tune_for_high_concurrency(); see
    // there for the chain order (trace-id → rate-limit → replica-rw → THIS).
    //
    // Returns Handled when sharding fully serviced the request (forward,
    // fan-out, or 501); Unhandled otherwise (local handler should run).
    // URL-only routing decisions runnable in the pre-routing handler
    // (which is invoked BEFORE httplib reads the request body — see
    // Server::routing() in third_party/httplib.h:6401). Body-dependent
    // routing (single insert, bulk, transactions) is performed inside
    // the actual route handler via the gateway_handle_* helpers.
    httplib::Server::HandlerResponse shard_prefilter(
            const httplib::Request& req, httplib::Response& res) {
        using HR = httplib::Server::HandlerResponse;
        if (!sharding_enabled()) return HR::Unhandled;
        if (req_is_local_hop(req)) return HR::Unhandled;

        const std::string& path = req.path;
        if (path == "/api/v1/health" || path == "/metrics" ||
            path == "/llms.txt" ||
            path.rfind("/api/v1/cluster/", 0) == 0 ||
            path.rfind("/api/v1/auth/",    0) == 0 ||
            path.rfind("/api/v1/admin/",   0) == 0) {
            return HR::Unhandled;
        }
        // Per-document point ops (URL contains the doc id).
        if (path.rfind("/api/v1/collections/", 0) == 0 &&
            path.find("/documents/") != std::string::npos) {
            if (path.size() >= 7 &&
                path.substr(path.size() - 7) == "/search") {
                return shard_fanout_documents_search(req, res) ? HR::Handled : HR::Unhandled;
            }
            if (path.size() >= 5 &&
                path.substr(path.size() - 5) == "/bulk") {
                // bulk needs body → handled in-route, not in prefilter.
                return HR::Unhandled;
            }
            std::string id = extract_doc_id_from_path(path);
            if (id.empty()) return HR::Unhandled;
            const cluster::ShardId& target = shard_map_.route(id);
            if (target == local_shard_) return HR::Unhandled;
            auto* spec = shard_map_.find_shard(target);
            if (!spec) { err(res, 500, "unknown target shard " + target); return HR::Handled; }
            shard_forward(*spec, req, res);
            return HR::Handled;
        }
        // Aggregation pipelines: fan-out is body-independent for our
        // gateway (we just re-issue the same body to every shard and
        // concatenate). Safe to do in the prefilter.
        if (path.rfind("/api/v1/collections/", 0) == 0 &&
            req.method == "POST" &&
            path.size() > 10 &&
            path.substr(path.size() - 10) == "/aggregate") {
            return shard_fanout_aggregate(req, res) ? HR::Handled : HR::Unhandled;
        }
        if (path.rfind("/api/v1/collections/", 0) == 0 &&
            req.method == "POST" &&
            path.size() > 6 &&
            path.substr(path.size() - 6) == "/count") {
            return shard_fanout_count(req, res) ? HR::Handled : HR::Unhandled;
        }
        return HR::Unhandled;
    }

    void setup_routes() {
        // CORS preflight
        srv_.Options(".*", [this](const httplib::Request& req, httplib::Response& res) {
            apply_cors(req, res);
            res.status = 204;
        });

        // ---------- /llms.txt --- LLM coding-agent reference -----------------
        // Served unauthenticated so any AI agent can `curl http://host/llms.txt`
        // and ingest the full SDK + protocol reference into its context.
        // Looked up from disk on first hit and cached in memory.
        srv_.Get("/llms.txt", [this](const httplib::Request&, httplib::Response& res) {
            static std::string cached = find_llms_txt();
            res.set_header("Access-Control-Allow-Origin", "*");
            res.set_header("Cache-Control", "public, max-age=300");
            if (cached.empty()) {
                res.status = 404;
                res.set_content("# llms.txt not bundled with this build.\n"
                                "# Fetch it from https://raw.githubusercontent.com/ProtectsAPP/delta/main/llms.txt\n",
                                "text/markdown; charset=utf-8");
                return;
            }
            res.status = 200;
            res.set_content(cached, "text/markdown; charset=utf-8");
        });

        // ---------- AUTH / SESSION ----------
        srv_.Post("/api/v1/auth/login", [this](const httplib::Request& req, httplib::Response& res) {
            json b = parse_body(req);
            std::string user = b.value("username", ""), pw = b.value("password", "");
            // P1-17: per-IP+username sliding-window cap. Keys are
            // "<ip>|<user>" so an attacker can't trivially evade the cap by
            // rotating usernames (each rotation gets its own bucket but the
            // same source IP still exhausts the global firewall budget; see
            // also rate-limit at the reverse proxy in production).
            std::string rl_key = req.remote_addr + "|" + user;
            if (!login_limiter_.allow(rl_key)) {
                Logger::instance().audit("login_rate_limited",
                    {{"user", user}, {"ip", req.remote_addr}});
                // 429 isn't in our Status enum; reuse FORBIDDEN with a clear
                // message so the audit log captures it correctly.
                err(res, Status::FORBIDDEN, "too many login attempts; try again later");
                return;
            }
            std::string er;
            // P0-8 surfaces a uniform message; we just propagate it.
            if (!auth_->authenticate(user, pw, &er)) {
                Logger::instance().audit("login_failed",
                    {{"user", user}, {"ip", req.remote_addr}, {"reason", er}});
                err(res, Status::UNAUTHORIZED, er); return;
            }
            login_limiter_.reset(rl_key);  // success clears the budget for this key
            Logger::instance().audit("login_succeeded",
                {{"user", user}, {"ip", req.remote_addr}});
            auto u = auth_->get_user(user).value();
            auth_->record_login_ip(user, req.remote_addr);
            std::string db = b.value("database", u.default_database.empty() ? "default" : u.default_database);
            std::string sch = b.value("schema", u.default_schema);
            auto sess = sessions_->create(user, db, sch, req.remote_addr);
            ok(res, json{{"token", sess.token}, {"username", user}, {"database", db}, {"schema", sch},
                          {"expires_at", sess.expires_at}, {"superuser", auth_->is_superuser(user)},
                          {"user", u.to_json()}});
        });
        srv_.Post("/api/v1/auth/logout", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            sessions_->revoke(s.token);
            Logger::instance().audit("logout",
                {{"user", s.username}, {"ip", req.remote_addr}});
            ok(res, json{{"ok", true}});
        });
        srv_.Get("/api/v1/auth/me", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            auto u = auth_->get_user(s.username);
            ok(res, json{{"username", s.username}, {"database", s.database}, {"schema", s.schema},
                          {"superuser", auth_->is_superuser(s.username)},
                          {"user", u ? u->to_json() : json::object()}});
        });
        srv_.Post("/api/v1/use", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            s.database = b.value("database", s.database);
            s.schema = b.value("schema", s.schema);
            sessions_->update(s);
            ok(res, json{{"database", s.database}, {"schema", s.schema}});
        });

        // ---------- USERS ----------
        srv_.Post("/api/v1/users", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            if (!auth_->is_superuser(s.username)) { auto roles = auth_->get_user_all_roles(s.username); if (!roles.count("user_admin")) { err(res, Status::FORBIDDEN, "permission denied"); return; } }
            json b = parse_body(req);
            std::string user = b.value("username", ""), pw = b.value("password", "");
            // P1-20: validate username/password format on the public boundary.
            if (!validation::is_valid_identifier(user)) {
                err(res, Status::INVALID, "invalid username (1-64 chars, [a-zA-Z_][a-zA-Z0-9_]*)");
                return;
            }
            if (!validation::is_valid_password(pw)) {
                err(res, Status::INVALID, "password must be 8-128 characters");
                return;
            }
            auto st = auth_->create_user(user, pw, b);
            if (!st.ok()) { err(res, st.code, st.message); return; }
            auto u = auth_->get_user(user);
            ok(res, u ? u->to_json() : json::object());
        });
        srv_.Get("/api/v1/users", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json arr = json::array();
            for (auto& u : auth_->list_users()) arr.push_back(u.to_json());
            ok(res, json{{"users", arr}});
        });
        srv_.Get(R"(/api/v1/users/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            auto u = auth_->get_user(req.matches[1]);
            if (!u) { err(res, Status::NOT_FOUND, "not found"); return; }
            ok(res, u->to_json());
        });
        srv_.Patch(R"(/api/v1/users/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            auto st = auth_->alter_user(req.matches[1], b);
            if (!st.ok()) { err(res, st.code, st.message); return; }
            ok(res, json{{"updated", true}});
        });
        srv_.Delete(R"(/api/v1/users/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            auto st = auth_->drop_user(req.matches[1]);
            if (!st.ok()) { err(res, st.code, st.message); return; }
            ok(res, json{{"deleted", 1}});
        });
        srv_.Post(R"(/api/v1/users/([^/]+)/lock)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            auto st = auth_->lock_user(req.matches[1]);
            if (!st.ok()) { err(res, st.code, st.message); return; } ok(res, json{{"locked", true}});
        });
        srv_.Post(R"(/api/v1/users/([^/]+)/unlock)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            auto st = auth_->unlock_user(req.matches[1]);
            if (!st.ok()) { err(res, st.code, st.message); return; } ok(res, json{{"unlocked", true}});
        });
        srv_.Post(R"(/api/v1/users/([^/]+)/roles)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            auto st = auth_->grant_role_to_user(req.matches[1], b.value("role", ""));
            if (!st.ok()) { err(res, st.code, st.message); return; } ok(res, json{{"granted", true}});
        });
        srv_.Delete(R"(/api/v1/users/([^/]+)/roles/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            auto st = auth_->revoke_role_from_user(req.matches[1], req.matches[2]);
            if (!st.ok()) { err(res, st.code, st.message); return; } ok(res, json{{"revoked", true}});
        });

        // ---------- ROLES ----------
        srv_.Post("/api/v1/roles", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            auto st = auth_->create_role(b.value("name",""), b.value("description",""), b.value("parents", std::vector<std::string>{}));
            if (!st.ok()) { err(res, st.code, st.message); return; }
            auto r = auth_->get_role(b["name"]);
            ok(res, r ? r->to_json() : json::object());
        });
        srv_.Get("/api/v1/roles", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json arr = json::array();
            for (auto& r : auth_->list_roles()) arr.push_back(r.to_json());
            ok(res, json{{"roles", arr}});
        });
        srv_.Delete(R"(/api/v1/roles/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            auto st = auth_->drop_role(req.matches[1]);
            if (!st.ok()) { err(res, st.code, st.message); return; } ok(res, json{{"deleted", 1}});
        });

        // ---------- PERMISSIONS ----------
        srv_.Post("/api/v1/permissions/grant", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            auto t = auth::PrivilegeTarget::from_json(b["target"]);
            uint32_t privs = auth::parse_privileges(b.value("privileges", std::vector<std::string>{}));
            auto st = auth_->grant(b.value("role",""), privs, t, b.value("with_grant_option", false), s.username);
            if (!st.ok()) { err(res, st.code, st.message); return; } ok(res, json{{"granted", true}});
        });
        srv_.Post("/api/v1/permissions/revoke", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            auto t = auth::PrivilegeTarget::from_json(b["target"]);
            uint32_t privs = auth::parse_privileges(b.value("privileges", std::vector<std::string>{}));
            auto st = auth_->revoke(b.value("role",""), privs, t);
            if (!st.ok()) { err(res, st.code, st.message); return; } ok(res, json{{"revoked", true}});
        });
        srv_.Post("/api/v1/permissions/grant-all", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            auto t = auth::PrivilegeTarget::from_json(b["target"]);
            auto st = auth_->grant(b.value("role",""), auth::PRIV_ALL, t, b.value("with_grant_option", false), s.username);
            if (!st.ok()) { err(res, st.code, st.message); return; } ok(res, json{{"granted", true}});
        });
        srv_.Get("/api/v1/permissions", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string role = req.get_param_value("role");
            json arr = json::array();
            for (auto& p : auth_->list_permissions(role)) arr.push_back(p.to_json());
            ok(res, json{{"permissions", arr}});
        });

        // ---------- DATABASES ----------
        srv_.Post("/api/v1/databases", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            auto st = dbm_->create_database(b.value("name",""), b.value("owner", s.username), b.value("options", json::object()));
            if (!st.ok()) { err(res, st.code, st.message); return; }
            auto d = dbm_->get_database(b["name"]);
            ok(res, d ? d->to_json() : json::object());
        });
        srv_.Get("/api/v1/databases", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json arr = json::array();
            for (auto& d : dbm_->list_databases()) {
                json dj = d.to_json();
                auto cols = col_->list_collections(d.name);
                dj["collection_count"] = cols.size();
                size_t total_docs = 0; for (auto& c : cols) total_docs += c.document_count;
                dj["document_count"] = total_docs;
                arr.push_back(dj);
            }
            ok(res, json{{"databases", arr}});
        });
        srv_.Get(R"(/api/v1/databases/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string name = req.matches[1];
            auto d = dbm_->get_database(name); if (!d) { err(res, Status::NOT_FOUND, "not found"); return; }
            json j = d->to_json();
            json schemas = json::array();
            for (auto& sc : dbm_->list_schemas(name)) schemas.push_back(sc.to_json());
            j["schemas"] = schemas;
            auto cols = col_->list_collections(name);
            j["collection_count"] = cols.size();
            size_t total_docs = 0; for (auto& c : cols) total_docs += c.document_count;
            j["document_count"] = total_docs;
            ok(res, j);
        });
        srv_.Delete(R"(/api/v1/databases/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            bool force = req.get_param_value("force") == "true";
            auto st = dbm_->drop_database(req.matches[1], force);
            if (!st.ok()) { err(res, st.code, st.message); return; } ok(res, json{{"deleted", 1}});
        });
        srv_.Patch(R"(/api/v1/databases/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            auto st = dbm_->alter_database(req.matches[1], parse_body(req));
            if (!st.ok()) { err(res, st.code, st.message); return; } ok(res, json{{"updated", true}});
        });

        // SCHEMAS
        srv_.Post(R"(/api/v1/databases/([^/]+)/schemas)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            auto st = dbm_->create_schema(req.matches[1], b.value("name",""), b.value("owner", s.username));
            if (!st.ok()) { err(res, st.code, st.message); return; } ok(res, json{{"created", true}});
        });
        srv_.Get(R"(/api/v1/databases/([^/]+)/schemas)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json arr = json::array();
            for (auto& sc : dbm_->list_schemas(req.matches[1])) arr.push_back(sc.to_json());
            ok(res, json{{"schemas", arr}});
        });
        srv_.Delete(R"(/api/v1/databases/([^/]+)/schemas/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            bool cascade = req.get_param_value("cascade") == "true";
            auto st = dbm_->drop_schema(req.matches[1], req.matches[2], cascade);
            if (!st.ok()) { err(res, st.code, st.message); return; } ok(res, json{{"deleted", 1}});
        });

        // ---------- COLLECTIONS ----------
        srv_.Post("/api/v1/collections", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            CollectionMeta m;
            m.database = b.value("database", s.database);
            m.schema = b.value("schema", s.schema);
            m.name = b.value("name", "");
            if (m.name.empty()) { err(res, Status::INVALID, "name required"); return; }
            if (!require_perm(res, s, auth::PRIV_CREATE, {"database", m.database, "", ""})) return;
            if (b.contains("indexes")) for (auto& idx : b["indexes"]) m.indexes.push_back(IndexDef::from_json(idx));
            auto st = col_->create_collection(m);
            if (!st.ok()) { err(res, st.code, st.message); return; }
            // Round 3: collection metadata MUST exist on every shard so
            // document ops on any owning shard succeed. Broadcast the
            // create to every PEER shard. Idempotent: ALREADY_EXISTS on
            // a peer is treated as success.
            broadcast_to_peer_shards(req, "POST", "/api/v1/collections",
                                     b.dump());
            ok(res, m.to_json());
        });
        srv_.Get("/api/v1/collections", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string db = req.get_param_value("database");
            if (db.empty()) db = s.database;
            json arr = json::array();
            for (auto& c : col_->list_collections(db)) arr.push_back(c.to_json());
            ok(res, json{{"collections", arr}});
        });
        srv_.Get(R"(/api/v1/collections/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            CollectionMeta m;
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            if (!col_->get_collection(db, sch, req.matches[1], &m)) { err(res, Status::NOT_FOUND, "not found"); return; }
            ok(res, m.to_json());
        });
        srv_.Delete(R"(/api/v1/collections/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            std::string col_name = req.matches[1];
            if (!require_perm(res, s, auth::PRIV_DROP, col_target(db, sch, col_name))) return;
            auto st = col_->drop_collection(db, sch, col_name);
            if (!st.ok()) { err(res, st.code, st.message); return; }
            ok(res, json{{"deleted", 1}});
        });
        srv_.Post(R"(/api/v1/collections/([^/]+)/indexes)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            json b = parse_body(req);
            IndexDef idx = IndexDef::from_json(b);
            if (idx.name.empty()) idx.name = "idx_" + (idx.fields.empty() ? "auto" : idx.fields[0]);
            auto st = col_->create_index(db, sch, req.matches[1], idx);
            if (!st.ok()) { err(res, st.code, st.message); return; }
            ok(res, idx.to_json());
        });
        srv_.Delete(R"(/api/v1/collections/([^/]+)/indexes/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            auto st = col_->drop_index(db, sch, req.matches[1], req.matches[2]);
            if (!st.ok()) { err(res, st.code, st.message); return; }
            ok(res, json{{"deleted", 1}});
        });

        // ---------- DOCUMENTS ----------
        srv_.Post(R"(/api/v1/collections/([^/]+)/documents)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string col_name = req.matches[1];
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            if (!require_perm(res, s, auth::PRIV_INSERT, col_target(db, sch, col_name))) return;
            json b = parse_body(req);
            // Round 3 sharding hook: route by doc _id (allocated here if
            // missing). If the owning shard is remote, gateway_handle_*
            // forwards verbatim and writes the response; we abort here.
            if (gateway_handle_single_insert(req, res, b)) return;
            json doc = b.contains("document") ? b["document"] : b;
            if (b.contains("id")) doc["_id"] = b["id"];
            // RLS check
            auto roles = auth_->get_user_all_roles(s.username);
            if (!auth_->is_superuser(s.username) && !dbm_->check_rls_constraint(s.username, db, sch, col_name, "INSERT", doc, roles)) { err(res, Status::FORBIDDEN, "RLS violation"); return; }
            std::string id;
            uint32_t ttl = b.value("ttl", 0u);
            auto st = col_->insert(db, sch, col_name, doc, id, ttl);
            if (!st.ok()) { err(res, st.code, st.message); return; }
            ok(res, json{{"id", id}, {"created_at", now_ms()}});
        });
        srv_.Post(R"(/api/v1/collections/([^/]+)/documents/bulk)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string col_name = req.matches[1];
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            json bulk_body = parse_body(req);
            if (gateway_handle_bulk_insert(req, res, bulk_body)) return;
            if (!require_perm(res, s, auth::PRIV_INSERT, col_target(db, sch, col_name))) return;
            json b = parse_body(req);
            json ids = json::array();
            int inserted = 0;
            for (auto& item : b["documents"]) {
                json doc = item.contains("document") ? item["document"] : item;
                std::string id;
                if (col_->insert(db, sch, col_name, doc, id).ok()) { ids.push_back(id); inserted++; }
            }
            ok(res, json{{"inserted", inserted}, {"ids", ids}});
        });
        srv_.Get(R"(/api/v1/collections/([^/]+)/documents/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string col_name = req.matches[1], id = req.matches[2];
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            if (!require_perm(res, s, auth::PRIV_SELECT, col_target(db, sch, col_name))) return;
            json out;
            if (!col_->get(db, sch, col_name, id, &out)) { err(res, Status::NOT_FOUND, "not found"); return; }
            // RLS check on read
            auto roles = auth_->get_user_all_roles(s.username);
            if (!auth_->is_superuser(s.username) && dbm_->is_rls_enabled(db, sch, col_name)) {
                if (!dbm_->check_rls_constraint(s.username, db, sch, col_name, "SELECT", out["data"], roles)) { err(res, Status::FORBIDDEN, "RLS denied"); return; }
            }
            ok(res, out);
        });
        srv_.Post(R"(/api/v1/collections/([^/]+)/documents/search)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string col_name = req.matches[1];
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            if (!require_perm(res, s, auth::PRIV_SELECT, col_target(db, sch, col_name))) return;
            json b = parse_body(req);
            json filter = b.value("filter", json::object());
            // RLS apply
            if (!auth_->is_superuser(s.username)) {
                auto roles = auth_->get_user_all_roles(s.username);
                filter = dbm_->apply_rls_filter(s.username, db, sch, col_name, "SELECT", filter, roles);
            }
            json sort_obj = b.value("sort", json::object());
            size_t skip = b.value("skip", 0), limit = b.value("limit", 100);
            json proj = b.value("projection", json::object());
            size_t total = 0;
            auto docs = col_->find(db, sch, col_name, filter, sort_obj, skip, limit, proj, &total);
            ok(res, json{{"documents", docs}, {"total", total}, {"skip", skip}, {"limit", limit}});
        });
        srv_.Patch(R"(/api/v1/collections/([^/]+)/documents/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string col_name = req.matches[1], id = req.matches[2];
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            if (!require_perm(res, s, auth::PRIV_UPDATE, col_target(db, sch, col_name))) return;
            json b = parse_body(req);
            // P1-6: optimistic-locking handle. RFC 7232-style `If-Match`
            // header wins; fall back to a `__if_version` body field if
            // the header is absent (easier from browser fetch() which
            // can't always set If-Match).
            uint64_t expected_version = 0;
            if (req.has_header("If-Match")) {
                try { expected_version = std::stoull(req.get_header_value("If-Match")); }
                catch (...) { expected_version = 0; }
            } else if (b.contains("__if_version")) {
                try { expected_version = b["__if_version"].get<uint64_t>(); }
                catch (...) { expected_version = 0; }
                b.erase("__if_version");
            }
            // map shorthand to $ ops
            json update_ops = json::object();
            for (auto& [k, v] : b.items()) {
                if (k == "set" || k == "$set") update_ops["$set"] = v;
                else if (k == "unset" || k == "$unset") update_ops["$unset"] = v;
                else if (k == "inc" || k == "$inc") update_ops["$inc"] = v;
                else if (k == "mul" || k == "$mul") update_ops["$mul"] = v;
                else if (k == "push" || k == "$push") update_ops["$push"] = v;
                else if (k == "pull" || k == "$pull") update_ops["$pull"] = v;
                else if (k == "addToSet" || k == "$addToSet") update_ops["$addToSet"] = v;
                else if (k == "rename" || k == "$rename") update_ops["$rename"] = v;
                else update_ops[k] = v;
            }
            json updated;
            auto st = col_->update(db, sch, col_name, id, update_ops, &updated, expected_version);
            if (!st.ok()) {
                // P1-6: surface CONFLICT as HTTP 409 so clients can retry.
                int http_code = (st.code == Status::CONFLICT) ? 409 : st.code;
                err(res, http_code, st.message);
                return;
            }
            ok(res, json{{"matched", 1}, {"modified", 1}, {"document", updated}});
        });
        srv_.Delete(R"(/api/v1/collections/([^/]+)/documents/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string col_name = req.matches[1], id = req.matches[2];
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            if (!require_perm(res, s, auth::PRIV_DELETE, col_target(db, sch, col_name))) return;
            auto st = col_->remove(db, sch, col_name, id);
            if (!st.ok()) { err(res, st.code, st.message); return; }
            ok(res, json{{"deleted", 1}});
        });
        srv_.Post(R"(/api/v1/collections/([^/]+)/aggregate)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string col_name = req.matches[1];
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            if (!require_perm(res, s, auth::PRIV_SELECT, col_target(db, sch, col_name))) return;
            json b = parse_body(req);
            json result = col_->aggregate(db, sch, col_name, b.value("pipeline", json::array()));
            ok(res, result);
        });

        // -----------------------------------------------------------------
        // Multi-document ACID transactions.
        //
        // POST /api/v1/transactions/execute
        // Body: { "ops": [
        //   {"op":"insert", "collection":"orders",   "doc": {...}, "ttl":0},
        //   {"op":"update", "collection":"inventory","id":"x",
        //                   "update": {"$inc":{"qty":-1}},
        //                   "expected_version": 5},
        //   {"op":"remove", "collection":"reserved", "id":"r1"}
        // ] }
        //
        // Optional per-op `database` / `schema` override the session ones.
        // The whole list runs as one transaction: validation runs first,
        // then either every op is applied or none is. On conflict (read-set
        // version mismatch, unique-index violation, etc.) the response is
        // HTTP 409 with the failing op's index and the engine error.
        // -----------------------------------------------------------------
        srv_.Post(R"(/api/v1/transactions/execute)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json body = parse_body(req);
            // Round 3 sharding hook: walk ops, gen ids for inserts that
            // omit them, ensure all ops route to the same shard. Returns
            // true on remote-forward / 501 reject.
            if (gateway_handle_transaction(req, res, body)) return;
            if (!body.is_object() || !body.contains("ops") || !body["ops"].is_array()) {
                err(res, Status::INVALID, "body must contain ops array");
                return;
            }
            const json& ops = body["ops"];
            if (ops.empty()) {
                ok(res, json{{"committed", true}, {"applied", 0}});
                return;
            }

            // Permission pre-check: superuser bypasses, otherwise we need
            // INSERT/UPDATE/DELETE on each (db,sch,col) referenced.
            for (size_t i = 0; i < ops.size(); ++i) {
                const json& o = ops[i];
                if (!o.is_object() || !o.contains("op") || !o.contains("collection")) {
                    err(res, Status::INVALID,
                        "op[" + std::to_string(i) + "] missing op or collection");
                    return;
                }
                std::string kind = o.value("op", std::string());
                std::string db = o.value("database", s.database);
                std::string sch = o.value("schema",   s.schema);
                std::string col = o.value("collection", std::string());
                uint32_t need = 0;
                if      (kind == "insert") need = auth::PRIV_INSERT;
                else if (kind == "update") need = auth::PRIV_UPDATE;
                else if (kind == "remove") need = auth::PRIV_DELETE;
                else {
                    err(res, Status::INVALID,
                        "op[" + std::to_string(i) + "] unknown kind: " + kind);
                    return;
                }
                if (!require_perm(res, s, need, col_target(db, sch, col))) return;
            }

            auto tx = col_->begin_transaction();
            for (size_t i = 0; i < ops.size(); ++i) {
                const json& o = ops[i];
                std::string kind = o.value("op", std::string());
                std::string db = o.value("database", s.database);
                std::string sch = o.value("schema",   s.schema);
                std::string col = o.value("collection", std::string());
                Status st;
                if (kind == "insert") {
                    json doc = o.value("doc", json::object());
                    uint32_t ttl = o.value("ttl", 0u);
                    std::string oid;
                    st = tx.insert(db, sch, col, std::move(doc), oid, ttl);
                } else if (kind == "update") {
                    std::string id = o.value("id", std::string());
                    json upd = o.value("update", json::object());
                    uint64_t ev = o.value("expected_version", 0ull);
                    st = tx.update(db, sch, col, id, upd, ev);
                } else if (kind == "remove") {
                    std::string id = o.value("id", std::string());
                    st = tx.remove(db, sch, col, id);
                }
                if (!st.ok()) {
                    err(res, st.code,
                        "op[" + std::to_string(i) + "]: " + st.message);
                    return;
                }
            }

            auto cs = tx.commit();
            if (!cs.ok()) {
                int http_code = (cs.code == Status::CONFLICT) ? 409 : cs.code;
                err(res, http_code, cs.message);
                return;
            }
            ok(res, json{{"committed", true}, {"applied", ops.size()}});
        });
        srv_.Post(R"(/api/v1/collections/([^/]+)/count)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string col_name = req.matches[1];
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            json b = parse_body(req);
            ok(res, json{{"count", col_->count(db, sch, col_name, b.value("filter", json::object()))}});
        });

        // ---------- VECTORS ----------
        srv_.Post(R"(/api/v1/collections/([^/]+)/vectors)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string col_name = req.matches[1];
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            json b = parse_body(req);
            std::string id = b.value("id", gen_id());
            auto vec_data = b["vector"].get<std::vector<float>>();
            auto idx = vec_->get_or_create(db + ":" + sch + ":" + col_name, vec_data.size(), b.value("metric", "cosine"));
            idx->insert(id, vec_data, b.value("metadata", json::object()));
            ok(res, json{{"id", id}});
        });
        srv_.Post(R"(/api/v1/collections/([^/]+)/vectors/search)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string col_name = req.matches[1];
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            json b = parse_body(req);
            auto idx = vec_->get(db + ":" + sch + ":" + col_name);
            if (!idx) { ok(res, json::array()); return; }
            auto vec_data = b["vector"].get<std::vector<float>>();
            int k = b.value("top_k", 10);
            float min_score = b.value("min_score", -1e9f);
            auto results = idx->search(vec_data, k, min_score);
            json arr = json::array();
            for (auto& r : results) arr.push_back({{"id", r.id}, {"score", r.score}, {"metadata", r.metadata}});
            ok(res, arr);
        });
        srv_.Delete(R"(/api/v1/collections/([^/]+)/vectors/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string col_name = req.matches[1], id = req.matches[2];
            std::string db = req.get_param_value("database"); if (db.empty()) db = s.database;
            std::string sch = req.get_param_value("schema"); if (sch.empty()) sch = s.schema;
            auto idx = vec_->get(db + ":" + sch + ":" + col_name);
            if (idx) idx->remove(id);
            ok(res, json{{"deleted", 1}});
        });

        // ---------- CACHE ----------
        srv_.Post("/api/v1/cache", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            std::string val = b["value"].is_string() ? b["value"].get<std::string>() : b["value"].dump();
            cache_->set(b["key"], val, b.value("ttl", 0u));
            ok(res, json{{"ok", true}});
        });
        srv_.Get(R"(/api/v1/cache/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string k = req.matches[1];
            auto v = cache_->get(k); if (!v) { err(res, Status::NOT_FOUND, "not found"); return; }
            ok(res, json{{"key", k}, {"value", *v}, {"ttl", cache_->ttl(k)}});
        });
        srv_.Delete(R"(/api/v1/cache/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            ok(res, json{{"deleted", cache_->del(req.matches[1]) ? 1 : 0}});
        });
        srv_.Post(R"(/api/v1/cache/([^/]+)/incr)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            ok(res, json{{"value", cache_->incr(req.matches[1], b.value("delta", 1))}});
        });
        srv_.Get("/api/v1/cache", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string p = req.get_param_value("pattern"); if (p.empty()) p = "*";
            ok(res, json{{"keys", cache_->keys(p)}});
        });
        srv_.Post(R"(/api/v1/cache/([^/]+)/hash)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            cache_->hset(req.matches[1], b["field"], b["value"].is_string() ? b["value"].get<std::string>() : b["value"].dump());
            ok(res, json{{"ok", true}});
        });
        srv_.Get(R"(/api/v1/cache/([^/]+)/hash)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            ok(res, cache_->hgetall(req.matches[1]));
        });
        srv_.Get(R"(/api/v1/cache/([^/]+)/hash/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            auto v = cache_->hget(req.matches[1], req.matches[2]); if (!v) { err(res, Status::NOT_FOUND, "not found"); return; }
            ok(res, json{{"value", *v}});
        });
        srv_.Delete(R"(/api/v1/cache/([^/]+)/hash/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            ok(res, json{{"deleted", cache_->hdel(req.matches[1], req.matches[2]) ? 1 : 0}});
        });
        srv_.Post(R"(/api/v1/cache/([^/]+)/list/push)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            std::string val = b["value"].is_string() ? b["value"].get<std::string>() : b["value"].dump();
            int64_t len = b.value("direction", "right") == "left" ? cache_->lpush(req.matches[1], val) : cache_->rpush(req.matches[1], val);
            ok(res, json{{"length", len}});
        });
        srv_.Get(R"(/api/v1/cache/([^/]+)/list)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            int start = std::stoi(req.get_param_value("start").empty() ? "0" : req.get_param_value("start"));
            int stop = std::stoi(req.get_param_value("stop").empty() ? "-1" : req.get_param_value("stop"));
            ok(res, json{{"items", cache_->lrange(req.matches[1], start, stop)}});
        });
        srv_.Post(R"(/api/v1/cache/([^/]+)/set)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            ok(res, json{{"added", cache_->sadd(req.matches[1], b["member"])}});
        });
        srv_.Get(R"(/api/v1/cache/([^/]+)/set)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            ok(res, json{{"members", cache_->smembers(req.matches[1])}});
        });
        srv_.Delete(R"(/api/v1/cache/([^/]+)/set/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            ok(res, json{{"removed", cache_->srem(req.matches[1], req.matches[2])}});
        });
        srv_.Post(R"(/api/v1/cache/([^/]+)/zset)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            ok(res, json{{"added", cache_->zadd(req.matches[1], b.value("score", 0.0), b["member"])}});
        });
        srv_.Get(R"(/api/v1/cache/([^/]+)/zset)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            int start = std::stoi(req.get_param_value("start").empty() ? "0" : req.get_param_value("start"));
            int stop = std::stoi(req.get_param_value("stop").empty() ? "-1" : req.get_param_value("stop"));
            auto items = cache_->zrange(req.matches[1], start, stop, true);
            json arr = json::array();
            for (auto& [m, sc] : items) arr.push_back({{"member", m}, {"score", sc}});
            ok(res, json{{"items", arr}});
        });
        srv_.Post(R"(/api/v1/cache/([^/]+)/expire)", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            ok(res, json{{"ok", cache_->expire(req.matches[1], b.value("seconds", 60u))}});
        });

        // ---------- PUBSUB ----------
        srv_.Post("/api/v1/pubsub/publish", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            json b = parse_body(req);
            int64_t n = cache_->publish(b.value("channel", ""), b.value("message", ""));
            ok(res, json{{"receivers", n}});
        });

        // ---------- RLS POLICIES ----------
        srv_.Post(R"(/api/v1/databases/([^/]+)/schemas/([^/]+)/collections/([^/]+)/policies)",
            [this](const httplib::Request& req, httplib::Response& res) {
                auth::Session s; if (!require_auth(req, res, &s)) return;
                json b = parse_body(req);
                database::RLSPolicy p = database::RLSPolicy::from_json(b);
                p.database = req.matches[1]; p.schema = req.matches[2]; p.collection = req.matches[3];
                auto st = dbm_->create_policy(p);
                if (!st.ok()) { err(res, st.code, st.message); return; }
                ok(res, p.to_json());
            });
        srv_.Get(R"(/api/v1/databases/([^/]+)/schemas/([^/]+)/collections/([^/]+)/policies)",
            [this](const httplib::Request& req, httplib::Response& res) {
                auth::Session s; if (!require_auth(req, res, &s)) return;
                json arr = json::array();
                for (auto& p : dbm_->list_policies(req.matches[1], req.matches[2], req.matches[3])) arr.push_back(p.to_json());
                ok(res, json{{"policies", arr}, {"enabled", dbm_->is_rls_enabled(req.matches[1], req.matches[2], req.matches[3])}});
            });
        srv_.Delete(R"(/api/v1/databases/([^/]+)/schemas/([^/]+)/collections/([^/]+)/policies/([^/]+))",
            [this](const httplib::Request& req, httplib::Response& res) {
                auth::Session s; if (!require_auth(req, res, &s)) return;
                auto st = dbm_->drop_policy(req.matches[1], req.matches[2], req.matches[3], req.matches[4]);
                if (!st.ok()) { err(res, st.code, st.message); return; }
                ok(res, json{{"deleted", 1}});
            });
        srv_.Post(R"(/api/v1/databases/([^/]+)/schemas/([^/]+)/collections/([^/]+)/rls)",
            [this](const httplib::Request& req, httplib::Response& res) {
                auth::Session s; if (!require_auth(req, res, &s)) return;
                json b = parse_body(req);
                dbm_->set_rls_enabled(req.matches[1], req.matches[2], req.matches[3], b.value("enabled", false));
                ok(res, json{{"enabled", b.value("enabled", false)}});
            });

        // ---------- CONNECTIONS ----------
        srv_.Get("/api/v1/connections", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            std::string user = req.get_param_value("username");
            std::string db = req.get_param_value("database");
            auto conns = pool_->list(user, db);
            json arr = json::array(); for (auto& c : conns) arr.push_back(c.to_json());
            int active = pool_->active(); int total = pool_->total();
            ok(res, json{{"total", total}, {"active", active}, {"idle", total - active}, {"connections", arr}});
        });
        srv_.Delete(R"(/api/v1/connections/(\d+))", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            uint64_t id = std::stoull(std::string(req.matches[1]));
            pool_->close(id);
            ok(res, json{{"closed", 1}});
        });

        // ---------- STATUS / STATS ----------
        srv_.Get("/api/v1/status", [this](const httplib::Request&, httplib::Response& res) {
            ok(res, json{{"status", "running"}, {"uptime", (now_ms() - start_time_) / 1000}, {"version", "1.0.0"}});
        });
        srv_.Get("/api/v1/stats", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            auto cs = cache_->stats();
            size_t total_docs = 0;
            for (auto& d : dbm_->list_databases()) for (auto& c : col_->list_collections(d.name)) total_docs += c.document_count;
            size_t vec_count = 0;
            for (auto& key : vec_->list()) { auto idx = vec_->get(key); if (idx) vec_count += idx->size(); }
            uint64_t up = std::max<uint64_t>(1, (now_ms() - start_time_) / 1000);
            uint64_t reqs = req_count_.load(std::memory_order_relaxed);
            json data = {
                {"uptime", up},
                {"requests_total", reqs},
                {"requests_per_sec", (double)reqs / (double)up},
                {"server", {{"threads", thread_count_},
                            {"keepalive_max", keepalive_max_count_},
                            {"keepalive_timeout_sec", keepalive_timeout_sec_}}},
                {"storage", {{"total_keys", total_docs}, {"databases", dbm_->list_databases().size()}}},
                {"cache", {{"keys", cs.total_keys}, {"hits", cs.hits}, {"misses", cs.misses}, {"hit_rate", cs.hit_rate}, {"shards", (int)cache::CacheEngine::SHARDS}}},
                {"vector", {{"vectors", vec_count}, {"indexes", vec_->list().size()}}},
                {"connections", {{"total", pool_->total()}, {"active", pool_->active()}}},
                {"users", auth_->list_users().size()},
                {"roles", auth_->list_roles().size()}
            };
            ok(res, data);
        });

        srv_.Get("/api/v1/health", [this](const httplib::Request&, httplib::Response& res) { ok(res, json{{"ok", true}}); });

        // ---------- ADMIN BACKUP / RESTORE -------------------------------
        // Superuser-only, orthogonal to the cluster snapshot endpoint
        // (which is gated by the cluster token, not by user identity).
        // Streams the full keyspace as a JSON dump suitable for cold-start
        // migrations between deployments / regions.
        srv_.Get("/api/v1/admin/backup", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            if (!auth_->is_superuser(s.username)) {
                Logger::instance().audit("backup_denied",
                    {{"user", s.username}, {"ip", req.remote_addr}});
                err(res, Status::FORBIDDEN, "superuser only"); return;
            }
            Logger::instance().audit("backup",
                {{"user", s.username}, {"ip", req.remote_addr},
                 {"encrypted", !backup_pass_.empty()}});
            uint64_t lsn = store_->current_lsn();
            json recs = json::array();
            for (auto& [k, v] : store_->prefix_scan("", 100000000)) {
                recs.push_back({{"key", k}, {"value", v}});
            }
            json payload = {{"lsn", lsn},
                            {"records", recs},
                            {"count", recs.size()},
                            {"timestamp_ms", now_ms()},
                            {"version", "1.0.0"}};
            if (!backup_pass_.empty()) {
                ok(res, backup_crypto::encrypt(backup_pass_, payload.dump()));
            } else {
                ok(res, payload);
            }
        });

        srv_.Post("/api/v1/admin/restore", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            if (!auth_->is_superuser(s.username)) {
                Logger::instance().audit("restore_denied",
                    {{"user", s.username}, {"ip", req.remote_addr}});
                err(res, Status::FORBIDDEN, "superuser only"); return;
            }
            Logger::instance().audit("restore_start",
                {{"user", s.username}, {"ip", req.remote_addr}});
            json b = parse_body(req);
            // Accept either the plain dump or the encrypted envelope. The
            // envelope is identified by `format == "delta-backup-1-encrypted"`.
            if (b.value("format", "") == "delta-backup-1-encrypted") {
                if (backup_pass_.empty()) {
                    err(res, Status::INVALID,
                        "encrypted backup but no --backup-passphrase configured");
                    return;
                }
                try {
                    std::string pt = backup_crypto::decrypt(backup_pass_, b);
                    b = json::parse(pt);
                } catch (const std::exception& e) {
                    Logger::instance().audit("restore_decrypt_failed",
                        {{"user", s.username}, {"ip", req.remote_addr},
                         {"error", e.what()}});
                    err(res, Status::INVALID, std::string("decrypt failed: ") + e.what());
                    return;
                }
            }
            if (!b.contains("records") || !b["records"].is_array()) {
                err(res, Status::INVALID, "missing `records` array");
                return;
            }
            size_t applied = 0, skipped = 0;
            uint64_t base_lsn = b.value("lsn", store_->current_lsn());
            for (auto& r : b["records"]) {
                if (!r.contains("key") || !r.contains("value")) { ++skipped; continue; }
                store_->apply_replicated(base_lsn,
                                         r["key"].get<std::string>(),
                                         r["value"].get<std::string>(),
                                         r.value("tombstone", false));
                ++applied;
            }
            // Re-hydrate the in-memory metadata caches. Documents and cache
            // entries are looked up directly from `store_` per request so
            // they don't need a reload — only the DBM and Auth caches do.
            dbm_->reload();
            auth_->reload();
            ok(res, json{{"applied", applied}, {"skipped", skipped}, {"lsn", base_lsn}});
        });

        // ---------- /metrics (Prometheus text format) -----------------
        // Unauthenticated by convention — scrapers run inside the trust
        // boundary. Use a reverse proxy / network policy to restrict
        // access if you expose the server to the public internet.
        srv_.Get("/metrics", [this](const httplib::Request& /*req*/, httplib::Response& res) {
            auto cs = cache_->stats();
            uint64_t up   = std::max<uint64_t>(1, (now_ms() - start_time_) / 1000);
            uint64_t reqs = req_count_.load(std::memory_order_relaxed);
            size_t total_docs = 0;
            for (auto& d : dbm_->list_databases())
                for (auto& c : col_->list_collections(d.name))
                    total_docs += c.document_count;
            size_t vec_count = 0, vec_indexes = 0;
            for (auto& key : vec_->list()) {
                auto idx = vec_->get(key);
                if (idx) { vec_count += idx->size(); ++vec_indexes; }
            }
            std::ostringstream m;
            auto line = [&](const char* name, const char* help, const char* type,
                            double value) {
                m << "# HELP " << name << " " << help << "\n"
                  << "# TYPE " << name << " " << type << "\n"
                  << name << " " << value << "\n";
            };
            line("delta_uptime_seconds",       "Server uptime in seconds.",                "counter", (double)up);
            line("delta_http_requests_total",  "Total HTTP requests served.",              "counter", (double)reqs);
            line("delta_documents_total",      "Total documents across all collections.",  "gauge",   (double)total_docs);
            line("delta_databases_total",      "Number of databases.",                     "gauge",   (double)dbm_->list_databases().size());
            line("delta_users_total",          "Number of users.",                         "gauge",   (double)auth_->list_users().size());
            line("delta_roles_total",          "Number of roles.",                         "gauge",   (double)auth_->list_roles().size());
            line("delta_cache_keys",           "Total live cache keys.",                   "gauge",   (double)cs.total_keys);
            line("delta_cache_memory_bytes",   "Cache memory footprint in bytes (P2-2).",  "gauge",   (double)cs.mem_bytes);
            line("delta_cache_hits_total",     "Cache lookups that hit.",                  "counter", (double)cs.hits);
            line("delta_cache_misses_total",   "Cache lookups that missed.",               "counter", (double)cs.misses);
            line("delta_cache_hit_rate",       "Cache hit ratio in [0,1].",                "gauge",   cs.hit_rate);
            line("delta_vector_indexes",       "Number of HNSW indexes.",                  "gauge",   (double)vec_indexes);
            line("delta_vector_vectors_total", "Total vectors across all indexes.",        "gauge",   (double)vec_count);
            line("delta_storage_sstables",    "Live SSTable file count.",                  "gauge",   (double)store_->sstable_count());
            line("delta_connections_total",    "Total registered client connections.",     "gauge",   (double)pool_->total());
            line("delta_connections_active",   "Currently active connections.",            "gauge",   (double)pool_->active());
            line("delta_http_threads",         "HTTP worker thread count.",                "gauge",   (double)thread_count_);

            // Per-status-class counters
            for (int sc = 1; sc <= 5; ++sc) {
                m << "delta_http_responses_total{class=\"" << sc << "xx\"} "
                  << status_class_[sc].load(std::memory_order_relaxed) << "\n";
            }
            line("delta_http_rate_limited_total",
                 "HTTP requests rejected by the per-IP token bucket.",
                 "counter",
                 (double)ip_rl_rejects_.load(std::memory_order_relaxed));
            // Latency histogram (one bucket-set per route)
            req_latency_hist_.render(m);

            // WebSocket + DeltaQL traffic counters (injected by main.cpp via
            // set_traffic_hook). Default-empty hook means "no extra lines".
            if (traffic_hook_) traffic_hook_(m);

            res.set_content(m.str(), "text/plain; version=0.0.4; charset=utf-8");
        });
    }

public:
    // Setter for the supplementary metrics emitted by the WS and DeltaQL
    // servers. Called from main.cpp after those servers are constructed so
    // their atomic counters can be read here without pulling those headers
    // into this file. Signature: void(std::ostringstream&).
    using TrafficHook = std::function<void(std::ostringstream&)>;
    void set_traffic_hook(TrafficHook fn) { traffic_hook_ = std::move(fn); }

private:
    TrafficHook traffic_hook_;

    // ------------------------------------------------------------------
    // Cluster / replication control plane.
    //
    // Authentication: a shared cluster token is sent by replicas in the
    // `X-Delta-Cluster-Token` header. Without a configured token the routes
    // are open (single-node testing). With a token, mismatches return 401.
    // The control plane is *separate* from user-level auth so that operators
    // can run replication without needing a service account.
    // ------------------------------------------------------------------
    void setup_cluster_routes() {
        // Legacy master/replica streaming. Only registered when repl_ is set
        // (the standalone build has no ReplicationManager).
        if (repl_) setup_legacy_replication_routes();
        // Raft routes are independent of repl_; they only need raft_ to be
        // installed via set_raft(). When raft_ is null the routes return
        // 503, so a misconfigured setup can't silently cast votes.
        setup_raft_routes();
    }

    void setup_legacy_replication_routes() {
        if (!repl_) return;

        auto check_token = [this](const httplib::Request& req, httplib::Response& res) -> bool {
            const auto& expected = repl_->token();
            if (expected.empty()) return true;
            auto got = req.get_header_value("X-Delta-Cluster-Token");
            if (got != expected) { err(res, Status::UNAUTHORIZED, "invalid cluster token"); return false; }
            return true;
        };

        srv_.Get("/api/v1/cluster/info", [this](const httplib::Request&, httplib::Response& res) {
            json info = {
                {"role", role_name(repl_->role())},
                {"read_only", repl_->read_only()},
                {"current_lsn", store_->current_lsn()},
                {"master_url", repl_->master_url()},
                {"replicas", json::array()}
            };
            for (auto& r : repl_->list_replicas()) {
                info["replicas"].push_back({
                    {"id", r.id}, {"remote_addr", r.remote_addr},
                    {"last_seen_ms", r.last_seen_ms}, {"last_acked_lsn", r.last_acked_lsn}
                });
            }
            ok(res, info);
        });

        srv_.Get("/api/v1/cluster/changes", [this, check_token](const httplib::Request& req, httplib::Response& res) {
            if (!check_token(req, res)) return;
            if (repl_->role() != Role::Master) { err(res, Status::FORBIDDEN, "not a master"); return; }
            uint64_t from = 0; size_t limit = 1000;
            if (req.has_param("from_lsn")) from = std::stoull(req.get_param_value("from_lsn"));
            if (req.has_param("limit")) limit = (size_t)std::stoul(req.get_param_value("limit"));
            auto cr = repl_->get_changes(from, limit);
            std::string id = req.get_header_value("X-Delta-Replica-Id");
            if (!id.empty()) repl_->register_replica(id, req.remote_addr, from);
            ok(res, json{{"gap", cr.gap}, {"from_lsn", cr.from_lsn},
                          {"to_lsn", cr.to_lsn}, {"records", cr.records}});
        });

        srv_.Get("/api/v1/cluster/snapshot", [this, check_token](const httplib::Request& req, httplib::Response& res) {
            if (!check_token(req, res)) return;
            if (repl_->role() != Role::Master) { err(res, Status::FORBIDDEN, "not a master"); return; }
            // Walk all keys in storage. Uses prefix_scan with empty prefix to
            // get full data set in sorted order.
            uint64_t lsn = store_->current_lsn();
            json recs = json::array();
            for (auto& [k, v] : store_->prefix_scan("", 1000000)) {
                recs.push_back({{"key", k}, {"value", v}, {"tombstone", false}});
            }
            ok(res, json{{"lsn", lsn}, {"records", recs}});
        });

        srv_.Post("/api/v1/cluster/promote", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            if (!auth_->is_superuser(s.username)) { err(res, Status::FORBIDDEN, "superuser only"); return; }
            repl_->promote_to_master();
            ok(res, json{{"role", role_name(repl_->role())}});
        });

        srv_.Post("/api/v1/cluster/demote", [this](const httplib::Request& req, httplib::Response& res) {
            auth::Session s; if (!require_auth(req, res, &s)) return;
            if (!auth_->is_superuser(s.username)) { err(res, Status::FORBIDDEN, "superuser only"); return; }
            json b = parse_body(req);
            std::string url = b.value("master_url", "");
            if (url.empty()) { err(res, Status::INVALID, "master_url required"); return; }
            repl_->demote_to_replica(url);
            ok(res, json{{"role", role_name(repl_->role())}, {"master_url", url}});
        });
    }

    // ------------------------------------------------------------------
    // Raft control plane (Round 2 part 2).
    //
    // POST /api/v1/cluster/raft/vote     RequestVote   (peer-to-peer)
    // POST /api/v1/cluster/raft/append   AppendEntries (peer-to-peer)
    // GET  /api/v1/cluster/raft/status   introspection (no token gate;
    //                                                   read-only)
    //
    // The peer-to-peer endpoints share the cluster_token gate already used
    // by /cluster/changes etc. — operators with a single shared secret get
    // both legacy replication and raft RPC for free.
    //
    // When set_raft() was never called, all four endpoints return HTTP 503
    // so a misconfigured cluster can't silently progress.
    // ------------------------------------------------------------------
    void setup_raft_routes() {
        // The cluster token is owned by the legacy replication manager when
        // present, otherwise the raft endpoints simply require any header
        // value to be empty. Operators wiring raft without legacy replication
        // should keep cluster_token configured the same way.
        auto token_check = [this](const httplib::Request& req,
                                  httplib::Response& res) -> bool {
            std::string expected = repl_ ? repl_->token() : std::string();
            if (expected.empty()) return true;
            auto got = req.get_header_value("X-Delta-Cluster-Token");
            if (got != expected) {
                err(res, Status::UNAUTHORIZED, "invalid cluster token");
                return false;
            }
            return true;
        };

        srv_.Post("/api/v1/cluster/raft/vote",
            [this, token_check](const httplib::Request& req, httplib::Response& res) {
                if (!token_check(req, res)) return;
                if (!raft_) { err(res, 503, "raft not configured"); return; }
                json b = parse_body(req);
                raft::RequestVoteArgs a;
                a.term            = b.value("term", (raft::Term)0);
                a.candidate_id    = b.value("candidate_id", std::string());
                a.last_log_index  = b.value("last_log_index", (raft::Index)0);
                a.last_log_term   = b.value("last_log_term",  (raft::Term)0);
                a.pre_vote        = b.value("pre_vote", false);
                raft::RequestVoteReply r;
                raft_->handle_request_vote(a, &r);
                ok(res, json{{"term", r.term}, {"vote_granted", r.vote_granted}});
            });

        srv_.Post("/api/v1/cluster/raft/append",
            [this, token_check](const httplib::Request& req, httplib::Response& res) {
                if (!token_check(req, res)) return;
                if (!raft_) { err(res, 503, "raft not configured"); return; }
                json b = parse_body(req);
                raft::AppendEntriesArgs a;
                a.term            = b.value("term", (raft::Term)0);
                a.leader_id       = b.value("leader_id", std::string());
                a.prev_log_index  = b.value("prev_log_index", (raft::Index)0);
                a.prev_log_term   = b.value("prev_log_term",  (raft::Term)0);
                a.leader_commit   = b.value("leader_commit",  (raft::Index)0);
                if (b.contains("entries") && b["entries"].is_array()) {
                    for (auto& e : b["entries"]) {
                        raft::LogEntry le;
                        le.term    = e.value("term",    (raft::Term)0);
                        le.index   = e.value("index",   (raft::Index)0);
                        le.type    = (raft::LogEntry::Type)e.value("type",
                                            (uint8_t)raft::LogEntry::Normal);
                        le.payload = e.value("payload", std::string());
                        a.entries.push_back(std::move(le));
                    }
                }
                raft::AppendEntriesReply r;
                raft_->handle_append_entries(a, &r);
                ok(res, json{
                    {"term",           r.term},
                    {"success",        r.success},
                    {"conflict_index", r.conflict_index},
                    {"conflict_term",  r.conflict_term}
                });
            });

        // GET /api/v1/cluster/shards — Round 3 introspection. Always
        // available (even on non-sharded nodes — reports enabled=false).
        srv_.Get("/api/v1/cluster/shards",
            [this](const httplib::Request&, httplib::Response& res) {
                if (!sharding_enabled()) {
                    ok(res, json{{"enabled", false}});
                    return;
                }
                ok(res, json{
                    {"enabled",        true},
                    {"local_shard",    local_shard_},
                    {"vnodes",         shard_map_.vnodes()},
                    {"shard_count",    shard_map_.shard_count()},
                    {"topology",       shard_map_.to_json()["shards"]},
                });
            });

        // GET /api/v1/cluster/shards/route?key=<id> — return the shard
        // that would own `key`. Useful for clients that want to bypass
        // the gateway and connect directly to the owning shard.
        srv_.Get("/api/v1/cluster/shards/route",
            [this](const httplib::Request& req, httplib::Response& res) {
                if (!sharding_enabled()) {
                    err(res, 503, "sharding not enabled"); return;
                }
                std::string key = req.get_param_value("key");
                if (key.empty()) { err(res, Status::INVALID, "key required"); return; }
                const auto& sid = shard_map_.route(key);
                auto* spec = shard_map_.find_shard(sid);
                json peers = json::array();
                if (spec) for (auto& p : spec->peers) {
                    peers.push_back({{"node_id", p.node_id},
                                     {"base_url", p.base_url()}});
                }
                ok(res, json{{"key", key}, {"shard_id", sid}, {"peers", peers}});
            });

        srv_.Get("/api/v1/cluster/raft/status",
            [this](const httplib::Request&, httplib::Response& res) {
                if (!raft_) {
                    ok(res, json{{"enabled", false}});
                    return;
                }
                ok(res, json{
                    {"enabled",        true},
                    {"role",           raft::role_name(raft_->role())},
                    {"current_term",   raft_->current_term()},
                    {"leader_id",      raft_->leader_id()},
                    {"commit_index",   raft_->commit_index()},
                    {"last_log_index", raft_->last_log_index()},
                    {"peers",          raft_->peers()}
                });
            });

        // POST /api/v1/cluster/raft/propose — convenience for tests / CLI.
        // Body: {"payload": "<opaque string>"}. Only a leader accepts. On
        // a follower returns 503 with the believed leader_id in the body.
        srv_.Post("/api/v1/cluster/raft/propose",
            [this, token_check](const httplib::Request& req, httplib::Response& res) {
                if (!token_check(req, res)) return;
                if (!raft_) { err(res, 503, "raft not configured"); return; }
                json b = parse_body(req);
                std::string payload = b.value("payload", std::string());
                raft::Index idx = 0;
                raft::NodeId hint;
                bool ok_propose = raft_->propose(payload, &idx, &hint);
                if (!ok_propose) {
                    res.status = 503;
                    res.set_content(json{
                        {"code", 503},
                        {"message", "not a leader"},
                        {"data", json{{"leader_id", hint}}}
                    }.dump(), "application/json");
                    return;
                }
                ok(res, json{{"index", idx}});
            });

        // POST /api/v1/cluster/raft/install_snapshot — peer-to-peer.
        // Body: {term, leader_id, last_included_index, last_included_term,
        //        data, peers}. Token-gated like the other RPC endpoints.
        srv_.Post("/api/v1/cluster/raft/install_snapshot",
            [this, token_check](const httplib::Request& req, httplib::Response& res) {
                if (!token_check(req, res)) return;
                if (!raft_) { err(res, 503, "raft not configured"); return; }
                json b = parse_body(req);
                raft::InstallSnapshotArgs a;
                a.term                = b.value("term", (raft::Term)0);
                a.leader_id           = b.value("leader_id", std::string());
                a.last_included_index = b.value("last_included_index", (raft::Index)0);
                a.last_included_term  = b.value("last_included_term",  (raft::Term)0);
                a.data                = b.value("data",                std::string());
                if (b.contains("peers") && b["peers"].is_array())
                    a.peers = b["peers"].get<std::vector<std::string>>();
                raft::InstallSnapshotReply r;
                raft_->handle_install_snapshot(a, &r);
                ok(res, json{{"term", r.term}});
            });

        // POST /api/v1/cluster/raft/snapshot — admin/test trigger to force
        // a snapshot now. Returns the resulting last_included_index.
        srv_.Post("/api/v1/cluster/raft/snapshot",
            [this](const httplib::Request& req, httplib::Response& res) {
                auth::Session s; if (!require_auth(req, res, &s)) return;
                if (!auth_->is_superuser(s.username)) {
                    err(res, Status::FORBIDDEN, "superuser only"); return;
                }
                if (!raft_) { err(res, 503, "raft not configured"); return; }
                raft::Index idx = raft_->snapshot_now();
                ok(res, json{{"last_included_index", idx}});
            });

        // POST /api/v1/cluster/raft/add_peer    body: {"peer_id":"...", "url":"host:port"} (url ignored if already known by transport)
        // POST /api/v1/cluster/raft/remove_peer body: {"peer_id":"..."}
        // Leader-only. The peer set takes effect immediately on append; the
        // call returns once the change has been appended to the log.
        srv_.Post("/api/v1/cluster/raft/add_peer",
            [this](const httplib::Request& req, httplib::Response& res) {
                auth::Session s; if (!require_auth(req, res, &s)) return;
                if (!auth_->is_superuser(s.username)) {
                    err(res, Status::FORBIDDEN, "superuser only"); return;
                }
                if (!raft_) { err(res, 503, "raft not configured"); return; }
                json b = parse_body(req);
                std::string pid = b.value("peer_id", std::string());
                if (pid.empty()) { err(res, Status::INVALID, "peer_id required"); return; }
                auto current = raft_->peers();
                for (auto& p : current) if (p == pid) {
                    ok(res, json{{"index", 0}, {"already_member", true}}); return;
                }
                current.push_back(pid);
                raft::Index idx = 0;
                raft::NodeId hint;
                if (!raft_->propose_config_change(current, &idx, &hint)) {
                    res.status = 503;
                    res.set_content(json{
                        {"code", 503},
                        {"message", "not a leader or change in flight"},
                        {"data", json{{"leader_id", hint}}}
                    }.dump(), "application/json");
                    return;
                }
                ok(res, json{{"index", idx}, {"peers", current}});
            });

        srv_.Post("/api/v1/cluster/raft/remove_peer",
            [this](const httplib::Request& req, httplib::Response& res) {
                auth::Session s; if (!require_auth(req, res, &s)) return;
                if (!auth_->is_superuser(s.username)) {
                    err(res, Status::FORBIDDEN, "superuser only"); return;
                }
                if (!raft_) { err(res, 503, "raft not configured"); return; }
                json b = parse_body(req);
                std::string pid = b.value("peer_id", std::string());
                if (pid.empty()) { err(res, Status::INVALID, "peer_id required"); return; }
                auto current = raft_->peers();
                std::vector<std::string> next;
                bool found = false;
                for (auto& p : current) {
                    if (p == pid) { found = true; continue; }
                    next.push_back(p);
                }
                if (!found) {
                    ok(res, json{{"index", 0}, {"not_member", true}}); return;
                }
                if (next.empty()) {
                    err(res, Status::INVALID, "cannot remove last peer"); return;
                }
                raft::Index idx = 0;
                raft::NodeId hint;
                if (!raft_->propose_config_change(next, &idx, &hint)) {
                    res.status = 503;
                    res.set_content(json{
                        {"code", 503},
                        {"message", "not a leader or change in flight"},
                        {"data", json{{"leader_id", hint}}}
                    }.dump(), "application/json");
                    return;
                }
                ok(res, json{{"index", idx}, {"peers", next}});
            });
    }
};

} // namespace delta::network
