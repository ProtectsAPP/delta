#pragma once
// =============================================================================
// DeltaQL TCP server. Listens for `deltaql://` clients and forwards their
// REQUEST frames to the local HTTP dispatcher via an in-process loopback
// httplib::Client (kept alive between calls). This way:
//
//   * Clients pay no per-request TCP+HTTP setup cost — they hold one
//     persistent multiplexed socket and pipeline freely.
//   * The server reuses 100 % of the existing route logic that lives behind
//     cpp-httplib without any duplication.
//
// Each accepted TCP connection runs on its own worker thread. Within a
// connection there are *two* further threads:
//
//   * the `reader` thread (this thread) decodes frames and dispatches each
//     REQUEST to a worker pool;
//   * the `writer` thread serializes outbound frames to the socket so concurrent
//     responses don't interleave.
//
// PubSub PUBLISH frames are produced by a CacheEngine listener that is wired
// up here and pushed into every connection's writer queue so subscribers
// receive messages with no extra HTTP poll.
// =============================================================================
#include "deltaql_protocol.hpp"
#include "../core/common.hpp"
#include "../auth/auth_manager.hpp"
#include "../cache/cache_engine.hpp"
#include <httplib.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace delta::network {

// ---------------------------------------------------------------------------
// Helpers shared with the WebSocket server below (they both turn an inbound
// REQUEST frame into an HTTP loopback call).
// ---------------------------------------------------------------------------
struct DispatchedResponse {
    int status = 500;
    std::string body;
    std::string content_type = "application/json";
};

class HttpLoopback {
public:
    HttpLoopback(const std::string& host, int port)
        : host_(host == "0.0.0.0" ? "127.0.0.1" : host), port_(port) {}

    // Build a fresh client per call: cpp-httplib's Client isn't thread-safe
    // for concurrent use of the same instance, but it does keep the underlying
    // socket alive for the lifetime of *one* Client. For sustained per-thread
    // throughput we cache one Client per std::thread::id.
    DispatchedResponse dispatch(const std::string& method,
                                const std::string& path_with_query,
                                const httplib::Headers& headers,
                                const std::string& body) {
        auto& cli = local_client();
        httplib::Result res;
        if      (method == "GET")    res = cli.Get(path_with_query.c_str(), headers);
        else if (method == "DELETE") res = cli.Delete(path_with_query.c_str(), headers, body, "application/json");
        else if (method == "POST")   res = cli.Post(path_with_query.c_str(), headers, body, "application/json");
        else if (method == "PUT")    res = cli.Put(path_with_query.c_str(), headers, body, "application/json");
        else if (method == "PATCH")  res = cli.Patch(path_with_query.c_str(), headers, body, "application/json");
        else                         return {400, R"({"code":400,"message":"unsupported method"})"};
        if (!res) {
            return {500, std::string("{\"code\":500,\"message\":\"loopback error: ")
                          + httplib::to_string(res.error()) + "\"}"};
        }
        DispatchedResponse out;
        out.status       = res->status;
        out.body         = std::move(res->body);
        auto it = res->headers.find("Content-Type");
        if (it != res->headers.end()) out.content_type = it->second;
        return out;
    }
private:
    httplib::Client& local_client() {
        thread_local std::unordered_map<HttpLoopback*, std::unique_ptr<httplib::Client>> tls;
        auto it = tls.find(this);
        if (it == tls.end()) {
            auto c = std::make_unique<httplib::Client>(host_, port_);
            c->set_keep_alive(true);
            c->set_read_timeout(30, 0);
            c->set_write_timeout(30, 0);
            c->set_connection_timeout(2, 0);
            it = tls.emplace(this, std::move(c)).first;
        }
        return *it->second;
    }
    std::string host_;
    int         port_;
};

// Namespace-scope traffic counters surfaced by HttpServer::set_traffic_hook.
struct DqlTrafficCounters {
    std::atomic<uint64_t> frames_sent{0};
    std::atomic<uint64_t> frames_recv{0};
    std::atomic<uint64_t> bytes_sent{0};
    std::atomic<uint64_t> bytes_recv{0};
    std::atomic<uint64_t> total_conns{0};
    std::atomic<uint64_t> active_conns{0};
};
inline DqlTrafficCounters& dql_traffic() { static DqlTrafficCounters g; return g; }

// ---------------------------------------------------------------------------
// One DeltaQL connection. Owns a reader and a writer thread.
// ---------------------------------------------------------------------------
class DqlConnection : public std::enable_shared_from_this<DqlConnection> {
public:
    DqlConnection(int fd, HttpLoopback* lb, auth::SessionManager* sessions, cache::CacheEngine* cache)
        : fd_(fd), lb_(lb), sessions_(sessions), cache_(cache) {
        int yes = 1;
        ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
        ::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes));
        struct timeval tv;
        tv.tv_sec = 30; // 30s read timeout
        tv.tv_usec = 0;
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    ~DqlConnection() { shutdown_close(); }

    void start() {
        running_.store(true);
        // Writer thread serializes all outbound frames. Reader thread runs
        // in the caller's std::thread (server's accept loop spawns it).
        writer_ = std::thread([self = shared_from_this()]() { self->writer_loop(); });
        reader_loop();
        // reader exited → tell writer to drain & exit, then join.
        {
            std::lock_guard<std::mutex> g(out_mu_);
            running_ = false;
        }
        out_cv_.notify_all();
        if (writer_.joinable()) writer_.join();
        shutdown_close();
    }

    // Enqueue an outbound frame. Returns false if the connection is dead.
    bool enqueue(dql::Frame f) {
        std::lock_guard<std::mutex> g(out_mu_);
        if (!running_) return false;
        outq_.emplace_back(std::move(f));
        out_cv_.notify_one();
        return true;
    }

    bool has_subscription(const std::string& channel) {
        std::lock_guard<std::mutex> g(sub_mu_);
        return subs_.count(channel) > 0;
    }

private:
    void shutdown_close() {
        int fd = fd_.exchange(-1);
        if (fd >= 0) { ::shutdown(fd, SHUT_RDWR); ::close(fd); }
    }

    // Read exactly `n` bytes into buf, return false on EOF/error.
    bool read_exact(uint8_t* buf, size_t n) {
        size_t got = 0;
        while (got < n) {
            int fd = fd_.load();
            if (fd < 0) return false;
            ssize_t r = ::recv(fd, buf + got, n - got, 0);
            if (r == 0) return false;
            if (r < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) return false; // timeout
                return false;
            }
            got += (size_t)r;
        }
        return true;
    }

    void reader_loop() {
        uint8_t hdr[dql::HEADER_LEN];
        while (running_) {
            if (!read_exact(hdr, dql::HEADER_LEN)) return;
            if (dql::get_u32(hdr) != dql::MAGIC) {
                send_error(0, "bad magic");
                return;
            }
            uint32_t plen = dql::parse_payload_len(hdr);
            if (plen > dql::MAX_FRAME) { send_error(0, "frame too large"); return; }
            std::string payload(plen, '\0');
            if (plen && !read_exact((uint8_t*)payload.data(), plen)) return;
            dql_traffic().frames_recv.fetch_add(1, std::memory_order_relaxed);
            dql_traffic().bytes_recv.fetch_add(dql::HEADER_LEN + plen,
                                               std::memory_order_relaxed);
            dql::Frame in;
            try {
                in = dql::parse_header_and_take_payload(hdr, std::move(payload));
            } catch (const std::exception& e) {
                send_error(0, e.what());
                return;
            }
            handle_frame(std::move(in));
        }
    }

    void writer_loop() {
        while (true) {
            std::deque<dql::Frame> batch;
            {
                std::unique_lock<std::mutex> g(out_mu_);
                out_cv_.wait(g, [&]{ return !outq_.empty() || !running_; });
                if (outq_.empty() && !running_) return;
                batch.swap(outq_);
            }
            for (auto& f : batch) write_one(f);
        }
    }
    void write_one(const dql::Frame& f) {
        auto buf = dql::encode(f);
        size_t off = 0;
        while (off < buf.size()) {
            int fd = fd_.load();
            if (fd < 0) return;
            ssize_t w = ::send(fd, buf.data() + off, buf.size() - off, 0);
            if (w <= 0) {
                if (w < 0 && errno == EINTR) continue;
                running_ = false; return;
            }
            off += (size_t)w;
        }
        dql_traffic().frames_sent.fetch_add(1, std::memory_order_relaxed);
        dql_traffic().bytes_sent.fetch_add(buf.size(), std::memory_order_relaxed);
    }
    void send_error(uint32_t rid, const std::string& msg) {
        json j = {{"code", -1}, {"message", msg}};
        enqueue({0, dql::Type::ERROR, rid, j.dump()});
    }
    void send_response(uint32_t rid, int status, std::string body) {
        json env;
        // Try to parse the loopback body as JSON; if it is, embed it as object.
        try {
            env = {{"status", status}, {"body", json::parse(body)}};
        } catch (...) {
            env = {{"status", status}, {"body", body}};
        }
        enqueue({0, dql::Type::RESPONSE, rid, env.dump()});
    }

    void handle_frame(dql::Frame f) {
        switch (f.type) {
            case dql::Type::HELLO: {
                json p; try { p = f.payload.empty() ? json::object() : json::parse(f.payload); } catch (...) {}
                json reply = {{"server", "delta"}, {"version", "1.0"},
                              {"protocol", 1},
                              {"features", {"pubsub", "auth", "request"}}};
                enqueue({0, dql::Type::HELLO_OK, f.rid, reply.dump()});
                break;
            }
            case dql::Type::AUTH: {
                json p; try { p = json::parse(f.payload); } catch(...) { send_error(f.rid,"bad json"); break; }
                std::string body = p.dump();
                httplib::Headers hdrs;
                if (p.contains("token")) {
                    std::string t = p["token"].get<std::string>();
                    hdrs.emplace("Authorization", "Bearer " + t);
                    auto r = lb_->dispatch("GET", "/api/v1/auth/me", hdrs, "");
                    if (r.status == 200) {
                        token_ = t;
                        enqueue({0, dql::Type::AUTH_OK, f.rid, json{{"token", token_}}.dump()});
                    } else {
                        send_error(f.rid, "invalid token");
                    }
                    break;
                }
                auto r = lb_->dispatch("POST", "/api/v1/auth/login", {}, body);
                try {
                    auto j = json::parse(r.body);
                    if (j.value("code", 0) == 200) token_ = j["data"]["token"].get<std::string>();
                } catch(...) {}
                send_response(f.rid, r.status, std::move(r.body));
                if (r.status == 200 && !token_.empty()) {
                    enqueue({0, dql::Type::AUTH_OK, f.rid + 1u, json{{"token", token_}}.dump()});
                }
                break;
            }
            case dql::Type::PING:
                enqueue({0, dql::Type::PONG, f.rid, "{}"});
                break;
            case dql::Type::SUB: {
                if (token_.empty() || !sessions_->get(token_)) {
                    send_error(f.rid, "unauthorized");
                    break;
                }
                json p; try { p = json::parse(f.payload); } catch(...) { send_error(f.rid,"bad json"); break; }
                auto ch = p.value("channel", std::string());
                if (ch.empty()) { send_error(f.rid, "channel required"); break; }
                {
                    std::lock_guard<std::mutex> g(sub_mu_);
                    subs_.insert(ch);
                }
                send_response(f.rid, 200, json{{"code",200},{"data",{{"subscribed",ch}}}}.dump());
                break;
            }
            case dql::Type::UNSUB: {
                json p; try { p = json::parse(f.payload); } catch(...) { send_error(f.rid,"bad json"); break; }
                auto ch = p.value("channel", std::string());
                {
                    std::lock_guard<std::mutex> g(sub_mu_);
                    subs_.erase(ch);
                }
                send_response(f.rid, 200, json{{"code",200},{"data",{{"unsubscribed",ch}}}}.dump());
                break;
            }
            case dql::Type::REQUEST: {
                if (!token_.empty() && !sessions_->get(token_)) {
                    token_.clear();
                }
                json p; try { p = json::parse(f.payload); } catch(...) { send_error(f.rid,"bad json"); break; }
                auto method = p.value("method", std::string("GET"));
                auto path   = p.value("path",   std::string("/"));
                std::string url = path;
                // Append query string if provided as object.
                if (p.contains("query") && p["query"].is_object()) {
                    bool first = url.find('?') == std::string::npos;
                    for (auto it = p["query"].begin(); it != p["query"].end(); ++it) {
                        url += first ? "?" : "&";
                        url += it.key() + "=" +
                               httplib::detail::encode_query_param(it.value().is_string()
                                                                    ? it.value().get<std::string>()
                                                                    : it.value().dump());
                        first = false;
                    }
                }
                httplib::Headers hdrs;
                if (!token_.empty()) hdrs.emplace("Authorization", "Bearer " + token_);
                if (p.contains("headers") && p["headers"].is_object()) {
                    for (auto it = p["headers"].begin(); it != p["headers"].end(); ++it) {
                        if (it.value().is_string())
                            hdrs.emplace(it.key(), it.value().get<std::string>());
                    }
                }
                std::string body;
                if (p.contains("body")) {
                    if (p["body"].is_string()) body = p["body"].get<std::string>();
                    else                       body = p["body"].dump();
                }
                auto r = lb_->dispatch(method, url, hdrs, body);
                send_response(f.rid, r.status, std::move(r.body));
                break;
            }
            case dql::Type::BYE:
                running_ = false;
                break;
            default:
                send_error(f.rid, "unsupported frame type");
        }
    }

    std::atomic<int>                      fd_;
    HttpLoopback*                         lb_;
    auth::SessionManager*                 sessions_;
    cache::CacheEngine*                   cache_;
    std::string                           token_;       // sticky auth
    std::atomic<bool>                     running_{false};
    std::thread                           writer_;
    std::deque<dql::Frame>                outq_;
    std::mutex                            out_mu_;
    std::condition_variable               out_cv_;
    std::mutex                            sub_mu_;
    std::set<std::string>                 subs_;
};

// ---------------------------------------------------------------------------
// DeltaQL listener. Accepts connections and starts one worker thread each.
// ---------------------------------------------------------------------------
class DeltaQLServer {
public:
    DeltaQLServer(const std::string& host, int port, HttpLoopback* lb,
                  auth::SessionManager* sessions, cache::CacheEngine* cache)
        : host_(host), port_(port), lb_(lb), sessions_(sessions), cache_(cache) {}

    void start() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) throw std::runtime_error("socket()");
        int yes = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#ifdef SO_REUSEPORT
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons((uint16_t)port_);
        if (host_ == "0.0.0.0" || host_.empty()) addr.sin_addr.s_addr = INADDR_ANY;
        else inet_pton(AF_INET, host_.c_str(), &addr.sin_addr);
        if (::bind(listen_fd_, (sockaddr*)&addr, sizeof(addr)) < 0)
            throw std::runtime_error("bind() deltaql");
        if (::listen(listen_fd_, 256) < 0) throw std::runtime_error("listen()");
        std::cout << "[Delta] DeltaQL TCP listening on " << host_ << ":" << port_ << std::endl;

        accept_thread_ = std::thread([this]{ accept_loop(); });

        // Hook the cache pubsub: mirror messages into matching connections.
        if (cache_) {
            cache_->on_publish([this](const std::string& ch, const std::string& msg) {
                fanout_publish(ch, msg);
            });
        }
    }

    void stop() {
        running_ = false;
        if (listen_fd_ >= 0) { ::shutdown(listen_fd_, SHUT_RDWR); ::close(listen_fd_); listen_fd_ = -1; }
        if (accept_thread_.joinable()) accept_thread_.join();
    }

    ~DeltaQLServer() { stop(); }

private:
    void accept_loop() {
        while (running_) {
            sockaddr_in cli{};
            socklen_t   clen = sizeof(cli);
            int fd = ::accept(listen_fd_, (sockaddr*)&cli, &clen);
            if (fd < 0) {
                if (!running_) return;
                if (errno == EINTR) continue;
                continue;
            }
            auto conn = std::make_shared<DqlConnection>(fd, lb_, sessions_, cache_);
            register_conn(conn);
            dql_traffic().total_conns.fetch_add(1, std::memory_order_relaxed);
            dql_traffic().active_conns.fetch_add(1, std::memory_order_relaxed);
            std::thread([this, conn]() {
                conn->start();
                unregister_conn(conn);
                dql_traffic().active_conns.fetch_sub(1, std::memory_order_relaxed);
            }).detach();
        }
    }
    void register_conn(std::shared_ptr<DqlConnection> c) {
        std::lock_guard<std::mutex> g(conns_mu_);
        conns_.push_back(std::move(c));
    }
    void unregister_conn(const std::shared_ptr<DqlConnection>& c) {
        std::lock_guard<std::mutex> g(conns_mu_);
        conns_.erase(std::remove_if(conns_.begin(), conns_.end(),
            [&](auto& p){ return p.get() == c.get(); }), conns_.end());
    }
    void fanout_publish(const std::string& channel, const std::string& message) {
        json p = {{"channel", channel}, {"message", message}};
        std::vector<std::shared_ptr<DqlConnection>> snapshot;
        {
            std::lock_guard<std::mutex> g(conns_mu_);
            snapshot = conns_;
        }
        for (auto& c : snapshot) {
            if (c->has_subscription(channel))
                c->enqueue({0, dql::Type::PUBLISH, 0, p.dump()});
        }
    }

    std::string host_;
    int         port_;
    HttpLoopback* lb_;
    auth::SessionManager* sessions_;
    cache::CacheEngine* cache_;
    int                 listen_fd_  = -1;
    std::atomic<bool>   running_{true};
    std::thread         accept_thread_;
    std::mutex          conns_mu_;
    std::vector<std::shared_ptr<DqlConnection>> conns_;
};

} // namespace delta::network
