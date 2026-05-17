#pragma once
// =============================================================================
// WebSocket bridge.
//
// Speaks the same logical protocol as DeltaQL TCP (HELLO/AUTH/REQUEST/...),
// but transmits each frame as one WebSocket text message containing the
// payload's JSON. The frame *type* is carried as the `type` field in the JSON,
// and `rid` correlates request and response. This keeps the browser-side
// client trivial: just `JSON.stringify({type:"request", rid, method, path,
// body})` over `new WebSocket(...)`.
//
// Endpoint: ws://host:<ws-port>/   (single path; everything is multiplexed)
// =============================================================================
#include "../core/common.hpp"
#include "../cache/cache_engine.hpp"
#include "../auth/auth_manager.hpp"
#include "deltaql_server.hpp"   // pulls HttpLoopback

#include <httplib.h>             // for query encoding helper

#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>
#include <cctype>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace delta::network::ws {

// ---- tiny SHA-1 + base64, no external dependency ----
inline void sha1(const uint8_t* msg, size_t len, uint8_t out[20]) {
    uint32_t h0=0x67452301, h1=0xEFCDAB89, h2=0x98BADCFE, h3=0x10325476, h4=0xC3D2E1F0;
    auto rotl = [](uint32_t x, int n){ return (x<<n)|(x>>(32-n)); };
    uint64_t bits = (uint64_t)len * 8;
    size_t plen = ((len + 9 + 63) / 64) * 64;
    std::vector<uint8_t> buf(plen, 0);
    std::memcpy(buf.data(), msg, len);
    buf[len] = 0x80;
    for (int i = 0; i < 8; i++) buf[plen - 1 - i] = (uint8_t)(bits >> (i * 8));
    for (size_t off = 0; off < plen; off += 64) {
        uint32_t w[80];
        for (int i = 0; i < 16; i++) {
            w[i] = (uint32_t(buf[off+4*i])<<24) | (uint32_t(buf[off+4*i+1])<<16)
                 | (uint32_t(buf[off+4*i+2])<<8) | uint32_t(buf[off+4*i+3]);
        }
        for (int i = 16; i < 80; i++) w[i] = rotl(w[i-3]^w[i-8]^w[i-14]^w[i-16], 1);
        uint32_t a=h0,b=h1,c=h2,d=h3,e=h4;
        for (int i = 0; i < 80; i++) {
            uint32_t f, k;
            if (i < 20)      { f = (b & c) | ((~b) & d); k = 0x5A827999; }
            else if (i < 40) { f = b ^ c ^ d;             k = 0x6ED9EBA1; }
            else if (i < 60) { f = (b & c) | (b & d) | (c & d); k = 0x8F1BBCDC; }
            else             { f = b ^ c ^ d;             k = 0xCA62C1D6; }
            uint32_t t = rotl(a, 5) + f + e + k + w[i];
            e = d; d = c; c = rotl(b, 30); b = a; a = t;
        }
        h0+=a; h1+=b; h2+=c; h3+=d; h4+=e;
    }
    auto put = [&](uint8_t* p, uint32_t v){ p[0]=v>>24; p[1]=v>>16; p[2]=v>>8; p[3]=v; };
    put(out, h0); put(out+4, h1); put(out+8, h2); put(out+12, h3); put(out+16, h4);
}

inline std::string base64(const uint8_t* p, size_t n) {
    static const char* tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string s;
    s.reserve(((n + 2) / 3) * 4);
    for (size_t i = 0; i < n; i += 3) {
        uint32_t v = uint32_t(p[i]) << 16;
        if (i+1 < n) v |= uint32_t(p[i+1]) << 8;
        if (i+2 < n) v |= uint32_t(p[i+2]);
        s.push_back(tbl[(v >> 18) & 0x3F]);
        s.push_back(tbl[(v >> 12) & 0x3F]);
        s.push_back(i+1 < n ? tbl[(v >> 6) & 0x3F] : '=');
        s.push_back(i+2 < n ? tbl[v & 0x3F]        : '=');
    }
    return s;
}

inline std::string accept_key(const std::string& client_key) {
    static const char* MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    std::string concat = client_key + MAGIC;
    uint8_t digest[20];
    sha1((const uint8_t*)concat.data(), concat.size(), digest);
    return base64(digest, 20);
}

// ---- one connection ----
// Namespace-scope traffic counters surfaced by HttpServer::set_traffic_hook.
struct WsTrafficCounters {
    std::atomic<uint64_t> frames_sent{0};
    std::atomic<uint64_t> frames_recv{0};
    std::atomic<uint64_t> bytes_sent{0};
    std::atomic<uint64_t> bytes_recv{0};
    std::atomic<uint64_t> total_conns{0};
    std::atomic<uint64_t> active_conns{0};
};
inline WsTrafficCounters& ws_traffic() { static WsTrafficCounters g; return g; }

class WsConnection : public std::enable_shared_from_this<WsConnection> {
public:
    WsConnection(int fd, HttpLoopback* lb, auth::SessionManager* sessions, cache::CacheEngine* cache)
        : fd_(fd), lb_(lb), sessions_(sessions), cache_(cache) {
        int yes = 1;
        ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
        struct timeval tv;
        tv.tv_sec = 30; // 30s read timeout
        tv.tv_usec = 0;
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    ~WsConnection() { close_fd(); }

    void start() {
        if (!handshake()) { close_fd(); return; }
        running_ = true;
        loop();
        close_fd();
    }

    bool send_text(const std::string& payload) {
        std::lock_guard<std::mutex> g(write_mu_);
        return write_frame(0x1, payload);
    }
    bool has_subscription(const std::string& ch) {
        std::lock_guard<std::mutex> g(sub_mu_);
        return subs_.count(ch) > 0;
    }

private:
    void close_fd() {
        int fd = fd_.exchange(-1);
        if (fd >= 0) { ::shutdown(fd, SHUT_RDWR); ::close(fd); }
    }
    bool read_exact(uint8_t* buf, size_t n) {
        size_t got = 0;
        while (got < n) {
            int fd = fd_.load();
            if (fd < 0) return false;
            ssize_t r = ::recv(fd, buf + got, n - got, 0);
            if (r == 0) return false;
            if (r < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) return false;
                return false;
            }
            got += (size_t)r;
        }
        return true;
    }
    bool write_all(const uint8_t* p, size_t n) {
        size_t off = 0;
        while (off < n) {
            int fd = fd_.load();
            if (fd < 0) return false;
            ssize_t w = ::send(fd, p + off, n - off, 0);
            if (w <= 0) { if (w<0&&errno==EINTR) continue; return false; }
            off += (size_t)w;
        }
        return true;
    }

    bool handshake() {
        std::string buf;
        buf.reserve(2048);
        char b[1];
        while (buf.size() < 16384) {
            ssize_t r = ::recv(fd_.load(), b, 1, 0);
            if (r <= 0) return false;
            buf.push_back(b[0]);
            if (buf.size() >= 4 && buf.compare(buf.size()-4, 4, "\r\n\r\n") == 0) break;
        }
        std::string key;
        size_t p = 0;
        while (p < buf.size()) {
            size_t end = buf.find("\r\n", p);
            if (end == std::string::npos) break;
            std::string line = buf.substr(p, end - p);
            std::string lower = line;
            for (auto& c : lower) c = (char)std::tolower((unsigned char)c);
            const std::string needle = "sec-websocket-key:";
            if (lower.rfind(needle, 0) == 0) {
                size_t s = needle.size();
                while (s < line.size() && (line[s] == ' ' || line[s] == '\t')) s++;
                key = line.substr(s);
            }
            p = end + 2;
        }
        if (key.empty()) {
            const char* bad = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
            write_all((const uint8_t*)bad, std::strlen(bad));
            return false;
        }
        std::string acc = accept_key(key);
        std::string resp =
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Accept: " + acc + "\r\n\r\n";
        return write_all((const uint8_t*)resp.data(), resp.size());
    }

    bool write_frame(uint8_t opcode, const std::string& payload) {
        std::vector<uint8_t> hdr;
        hdr.push_back(0x80 | (opcode & 0x0F));
        size_t n = payload.size();
        if (n < 126)            hdr.push_back((uint8_t)n);
        else if (n <= 0xFFFF) {
            hdr.push_back(126);
            hdr.push_back((uint8_t)(n >> 8));
            hdr.push_back((uint8_t)(n & 0xFF));
        } else {
            hdr.push_back(127);
            for (int i = 7; i >= 0; --i) hdr.push_back((uint8_t)((n >> (i*8)) & 0xFF));
        }
        if (!write_all(hdr.data(), hdr.size())) return false;
        if (!payload.empty() && !write_all((const uint8_t*)payload.data(), payload.size())) return false;
        ws_traffic().frames_sent.fetch_add(1, std::memory_order_relaxed);
        ws_traffic().bytes_sent.fetch_add(hdr.size() + payload.size(),
                                          std::memory_order_relaxed);
        return true;
    }

    // returns final opcode of message, or -1 on error / close
    int read_message(std::string& out) {
        out.clear();
        bool fin = false;
        int first_op = -1;
        while (!fin) {
            uint8_t h[2];
            if (!read_exact(h, 2)) return -1;
            fin             = (h[0] & 0x80) != 0;
            int  op         = h[0] & 0x0F;
            bool masked     = (h[1] & 0x80) != 0;
            uint64_t plen   = h[1] & 0x7F;
            if (plen == 126) {
                uint8_t e[2]; if (!read_exact(e, 2)) return -1;
                plen = ((uint64_t)e[0] << 8) | e[1];
            } else if (plen == 127) {
                uint8_t e[8]; if (!read_exact(e, 8)) return -1;
                plen = 0;
                for (int i = 0; i < 8; i++) plen = (plen << 8) | e[i];
            }
            if (plen > 16 * 1024 * 1024) return -1; // 16MB max
            uint8_t mask[4] = {0,0,0,0};
            if (masked && !read_exact(mask, 4)) return -1;
            std::string chunk(plen, '\0');
            if (plen && !read_exact((uint8_t*)chunk.data(), plen)) return -1;
            if (masked) for (size_t i = 0; i < plen; i++) chunk[i] ^= mask[i & 3];
            if (op == 0x8) return 0x8;                     // close
            if (op == 0x9) {                               // ping → pong
                std::lock_guard<std::mutex> g(write_mu_);
                write_frame(0xA, chunk);
                continue;
            }
            if (op == 0xA) continue;                       // pong, ignore
            // P0-14: enforce fragmentation rules from RFC 6455 §5.4.
            //   * First frame must carry a non-continuation opcode.
            //   * Continuation frames must carry opcode 0x0.
            //   * Control frames (already short-circuited above) must
            //     not appear here.
            //   * Mid-stream opcode flips reject the connection rather
            //     than concatenating mismatched data.
            if (first_op == -1) {
                if (op == 0x0) return -1;   // first frame may not be cont.
                first_op = op;
            } else {
                if (op != 0x0) return -1;   // mid-stream opcode change.
            }
            // Cap reassembled message size separately from per-frame.
            if (out.size() + chunk.size() > 16 * 1024 * 1024) return -1;
            out.append(chunk);
            ws_traffic().frames_recv.fetch_add(1, std::memory_order_relaxed);
            ws_traffic().bytes_recv.fetch_add(plen, std::memory_order_relaxed);
        }
        return first_op;
    }

    void loop() {
        while (running_) {
            std::string msg;
            int op = read_message(msg);
            if (op < 0 || op == 0x8) return;
            if (op != 0x1 && op != 0x2) continue; // text or binary only
            handle_message(msg);
        }
    }

    void send_json(const json& j) {
        send_text(j.dump());
    }
    void send_error(uint64_t rid, const std::string& msg) {
        send_json({{"type","error"},{"rid",rid},{"code",-1},{"message",msg}});
    }

    void handle_message(const std::string& s) {
        json p;
        try { p = json::parse(s); } catch (...) { send_error(0, "invalid json"); return; }
        std::string type = p.value("type", std::string("request"));
        uint64_t rid = p.value("rid", (uint64_t)0);

        if (type == "hello") {
            send_json({{"type","hello_ok"},{"rid",rid},
                       {"server","delta"},{"version","1.0"},
                       {"features", {"pubsub","auth","request"}}});
        } else if (type == "ping") {
            send_json({{"type","pong"},{"rid",rid}});
        } else if (type == "auth") {
            if (p.contains("token")) {
                std::string t = p["token"].get<std::string>();
                httplib::Headers hdrs = {{"Authorization", "Bearer " + t}};
                auto r = lb_->dispatch("GET", "/api/v1/auth/me", hdrs, "");
                if (r.status == 200) {
                    token_ = t;
                    send_json({{"type","auth_ok"},{"rid",rid},{"token",token_}});
                } else {
                    json bj; try { bj = json::parse(r.body); } catch(...) {}
                    send_json({{"type","error"},{"rid",rid},{"message","invalid token"},{"status",r.status},{"body",bj}});
                }
                return;
            }
            json body;
            body["username"] = p.value("username", std::string());
            body["password"] = p.value("password", std::string());
            auto r = lb_->dispatch("POST", "/api/v1/auth/login", {}, body.dump());
            json bj; try { bj = json::parse(r.body); } catch(...) {}
            if (r.status == 200) {
                try { token_ = bj["data"]["token"].get<std::string>(); } catch (...) {}
            }
            send_json({{"type","response"},{"rid",rid},{"status",r.status},{"body",bj}});
        } else if (type == "subscribe") {
            if (token_.empty() || !sessions_->get(token_)) {
                send_error(rid, "unauthorized");
                return;
            }
            auto ch = p.value("channel", std::string());
            if (ch.empty()) { send_error(rid, "channel required"); return; }
            { std::lock_guard<std::mutex> g(sub_mu_); subs_.insert(ch); }
            send_json({{"type","response"},{"rid",rid},{"status",200},
                       {"body", {{"code",200},{"data",{{"subscribed",ch}}}}}});
        } else if (type == "unsubscribe") {
            auto ch = p.value("channel", std::string());
            { std::lock_guard<std::mutex> g(sub_mu_); subs_.erase(ch); }
            send_json({{"type","response"},{"rid",rid},{"status",200},
                       {"body", {{"code",200},{"data",{{"unsubscribed",ch}}}}}});
        } else if (type == "request") {
            if (!token_.empty() && !sessions_->get(token_)) {
                token_.clear(); // session became invalid
            }
            auto method = p.value("method", std::string("GET"));
            auto path   = p.value("path",   std::string("/"));
            std::string url = path;
            if (p.contains("query") && p["query"].is_object()) {
                bool first = url.find('?') == std::string::npos;
                for (auto it = p["query"].begin(); it != p["query"].end(); ++it) {
                    url += first ? "?" : "&";
                    url += it.key() + "=" +
                           httplib::detail::encode_query_param(
                                it.value().is_string()
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
            json bj; try { bj = json::parse(r.body); } catch(...) { bj = r.body; }
            send_json({{"type","response"},{"rid",rid},{"status",r.status},{"body",bj}});
        } else {
            send_error(rid, "unknown message type: " + type);
        }
    }

    std::atomic<int>      fd_;
    HttpLoopback*         lb_;
    auth::SessionManager* sessions_;
    cache::CacheEngine*   cache_;
    std::string           token_;
    std::atomic<bool>     running_{false};
    std::mutex            write_mu_;
    std::mutex            sub_mu_;
    std::set<std::string> subs_;
};

// ---- listener ----
class WebSocketServer {
public:
    WebSocketServer(const std::string& host, int port, HttpLoopback* lb,
                    auth::SessionManager* sessions, cache::CacheEngine* cache)
        : host_(host), port_(port), lb_(lb), sessions_(sessions), cache_(cache) {}

    void start() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
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
            throw std::runtime_error("bind() ws");
        if (::listen(listen_fd_, 256) < 0) throw std::runtime_error("listen() ws");
        std::cout << "[Delta] WebSocket listening on " << host_ << ":" << port_ << std::endl;

        accept_thread_ = std::thread([this]{ accept_loop(); });

        if (cache_) {
            cache_->on_publish([this](const std::string& ch, const std::string& msg) {
                fanout(ch, msg);
            });
        }
    }
    void stop() {
        running_ = false;
        if (listen_fd_ >= 0) { ::shutdown(listen_fd_, SHUT_RDWR); ::close(listen_fd_); listen_fd_ = -1; }
        if (accept_thread_.joinable()) accept_thread_.join();
    }
    ~WebSocketServer() { stop(); }

private:
    void accept_loop() {
        while (running_) {
            sockaddr_in cli{}; socklen_t clen = sizeof(cli);
            int fd = ::accept(listen_fd_, (sockaddr*)&cli, &clen);
            if (fd < 0) { if (!running_) return; continue; }
            auto conn = std::make_shared<WsConnection>(fd, lb_, sessions_, cache_);
            { std::lock_guard<std::mutex> g(conns_mu_); conns_.push_back(conn); }
            ws_traffic().total_conns.fetch_add(1, std::memory_order_relaxed);
            ws_traffic().active_conns.fetch_add(1, std::memory_order_relaxed);
            std::thread([this, conn]() {
                conn->start();
                std::lock_guard<std::mutex> g(conns_mu_);
                conns_.erase(std::remove_if(conns_.begin(), conns_.end(),
                    [&](auto& p){ return p.get() == conn.get(); }), conns_.end());
                ws_traffic().active_conns.fetch_sub(1, std::memory_order_relaxed);
            }).detach();
        }
    }
    void fanout(const std::string& ch, const std::string& msg) {
        json p = {{"type","publish"},{"channel",ch},{"message",msg}};
        std::string s = p.dump();
        std::vector<std::shared_ptr<WsConnection>> snapshot;
        { std::lock_guard<std::mutex> g(conns_mu_); snapshot = conns_; }
        for (auto& c : snapshot) {
            if (c->has_subscription(ch)) c->send_text(s);
        }
    }

    std::string host_;
    int         port_;
    HttpLoopback* lb_;
    auth::SessionManager* sessions_;
    cache::CacheEngine* cache_;
    int                 listen_fd_ = -1;
    std::atomic<bool>   running_{true};
    std::thread         accept_thread_;
    std::mutex          conns_mu_;
    std::vector<std::shared_ptr<WsConnection>> conns_;
};

} // namespace delta::network::ws
