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
#include <algorithm>
#include <httplib.h>
#include <fstream>
#include <sstream>
#include <array>
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
            return httplib::Server::HandlerResponse::Unhandled;
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
            default: return 200;
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

    // Authenticate request, returns session
    bool require_auth(const httplib::Request& req, httplib::Response& res, auth::Session* out_sess) {
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
};

} // namespace delta::network
