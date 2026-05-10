#pragma once
#include "../core/common.hpp"
#include "../core/collection.hpp"
#include "../cache/cache_engine.hpp"
#include "../vector/hnsw_index.hpp"
#include "../auth/auth_manager.hpp"
#include "../database/database_manager.hpp"
#include "connection_pool.hpp"
#include "replication.hpp"
#include <httplib.h>
#include <fstream>

namespace delta::network {

struct HttpTuning {
    int threads = 0;            // 0 = auto
    int keepalive_max = 1000;
    int keepalive_timeout = 30;
    int max_connections = 1024;
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
          sessions_(sessions), dbm_(dbm), pool_(pool), repl_(repl), tuning_(tuning) {
        setup_routes();
        setup_cluster_routes();
        tune_for_high_concurrency();
    }

    void listen(const std::string& host, int port) {
        std::cout << "[Delta] HTTP listening on " << host << ":" << port
                  << " threads=" << thread_count_
                  << " keepalive=" << keepalive_max_count_ << "/" << keepalive_timeout_sec_ << "s"
                  << std::endl;
        srv_.listen(host, port);
    }
    void stop() { srv_.stop(); }
    bool is_running() const { return srv_.is_running(); }

    // Counters for live RPS reporting
    uint64_t total_requests() const { return req_count_.load(std::memory_order_relaxed); }

private:
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

        // Trace request count + enforce replica read-only mode.
        srv_.set_pre_routing_handler([this](const httplib::Request& req, httplib::Response& res) {
            req_count_.fetch_add(1, std::memory_order_relaxed);
            if (repl_ && repl_->read_only() && is_mutating(req)) {
                // Replicas reject writes — caller should retry against master.
                send_json(res, Status::FORBIDDEN,
                          json{{"message", "this node is a read-only replica"},
                               {"data", nullptr},
                               {"master_url", repl_->master_url()},
                               {"role", role_name(repl_->role())}});
                return httplib::Server::HandlerResponse::Handled;
            }
            return httplib::Server::HandlerResponse::Unhandled;
        });
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
    httplib::Server srv_;
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
    static void send_json(httplib::Response& res, int code, const json& body) {
        res.status = http_status_for(code);
        // body uses code 200 as a sentinel for "ok" in some places; remap
        if (code == 200) { res.status = 200; }
        json j; j["code"] = code; if (body.contains("data") || body.contains("message")) {
            for (auto& [k, v] : body.items()) j[k] = v;
        } else j["data"] = body;
        res.set_header("Access-Control-Allow-Origin", "*");
        res.set_header("Access-Control-Allow-Headers", "*");
        res.set_header("Access-Control-Allow-Methods", "*");
        res.set_content(j.dump(), "application/json");
    }
    static void ok(httplib::Response& res, const json& data) { send_json(res, 200, json{{"data", data}}); }
    static void err(httplib::Response& res, int code, const std::string& msg) { send_json(res, code, json{{"message", msg}, {"data", nullptr}}); }

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

    void setup_routes() {
        // CORS preflight
        srv_.Options(".*", [](const httplib::Request&, httplib::Response& res) {
            res.set_header("Access-Control-Allow-Origin", "*");
            res.set_header("Access-Control-Allow-Headers", "*");
            res.set_header("Access-Control-Allow-Methods", "GET,POST,PATCH,PUT,DELETE,OPTIONS");
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
            std::string er;
            if (!auth_->authenticate(user, pw, &er)) { err(res, Status::UNAUTHORIZED, er); return; }
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
            sessions_->revoke(s.token); ok(res, json{{"ok", true}});
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
            if (user.empty() || pw.empty()) { err(res, Status::INVALID, "username/password required"); return; }
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
            auto st = col_->update(db, sch, col_name, id, update_ops, &updated);
            if (!st.ok()) { err(res, st.code, st.message); return; }
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

        srv_.Get("/api/v1/health", [](const httplib::Request&, httplib::Response& res) { ok(res, json{{"ok", true}}); });
    }

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
