#include "config.hpp"
#include "../core/validation.hpp"
#include <fstream>
#include <cstring>
#include <iostream>
#include <sstream>

namespace delta::server {

namespace {
// Split a comma-separated list (used for --cors-origin a,b,c).
std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        // trim ascii whitespace
        size_t l = tok.find_first_not_of(" \t");
        size_t r = tok.find_last_not_of(" \t");
        if (l == std::string::npos) continue;
        out.emplace_back(tok.substr(l, r - l + 1));
    }
    return out;
}
}

ServerConfig ServerConfig::from_args(int argc, char** argv) {
    using namespace delta::validation;
    ServerConfig c;
    // P1-20 / P1-25: every numeric flag goes through safe_stoi/safe_stoll
    // so a malformed value (e.g. `--port abc`) prints a warning and uses
    // the default rather than throwing std::invalid_argument from main().
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string { return (i + 1 < argc) ? std::string(argv[++i]) : std::string(); };
        if (a == "--data") c.data_dir = next();
        else if (a == "--host") c.http_host = next();
        else if (a == "--port") c.http_port = safe_stoi(next(), c.http_port);
        else if (a == "--threads") c.http_threads = safe_stoi(next(), c.http_threads);
        else if (a == "--max-conn") c.max_connections = safe_stoi(next(), c.max_connections);
        else if (a == "--idle-timeout") c.idle_timeout_sec = safe_stoi(next(), c.idle_timeout_sec);
        else if (a == "--keepalive-max") c.keepalive_max_count = safe_stoi(next(), c.keepalive_max_count);
        else if (a == "--keepalive-timeout") c.keepalive_timeout_sec = safe_stoi(next(), c.keepalive_timeout_sec);
        else if (a == "--cache-max-keys") c.cache_max_keys = (size_t)safe_stoll(next(), (long long)c.cache_max_keys);
        else if (a == "--deltaql-port") c.deltaql_port = safe_stoi(next(), c.deltaql_port);
        else if (a == "--ws-port") c.ws_port = safe_stoi(next(), c.ws_port);
        else if (a == "--no-deltaql") c.deltaql_port = 0;
        else if (a == "--no-ws") c.ws_port = 0;
        else if (a == "--reset-admin") c.reset_admin = true;
        else if (a == "--role") c.role = next();
        else if (a == "--master-url") c.master_url = next();
        else if (a == "--cluster-token") c.cluster_token = next();
        else if (a == "--cors-origin") {
            // Repeatable flag: comma-separated or one origin per occurrence.
            for (auto& o : split_csv(next())) c.cors.allowed_origins.push_back(o);
        }
        else if (a == "--config") { c = from_file(next()); }
        else if (a == "--help" || a == "-h") {
            std::cout << "Delta Server\n"
                "Usage: delta_server [options]\n"
                "  --data DIR              Data directory (./data)\n"
                "  --host HOST             Bind host (0.0.0.0)\n"
                "  --port PORT             Bind port (16888)\n"
                "  --threads N             HTTP worker threads (auto = max(256, max-conn))\n"
                "  --max-conn N            Max concurrent connections (1024)\n"
                "  --keepalive-max N       Max keep-alive requests per connection (1000)\n"
                "  --keepalive-timeout N   Keep-alive timeout seconds (30)\n"
                "  --cache-max-keys N      Total cache key budget (1,000,000)\n"
                "  --deltaql-port PORT     DeltaQL native TCP port (16889, 0=disable)\n"
                "  --ws-port PORT          WebSocket bridge port (16890, 0=disable)\n"
                "  --no-deltaql            Disable native deltaql:// listener\n"
                "  --no-ws                 Disable WebSocket listener\n"
                "  --role ROLE             standalone | master | replica\n"
                "  --master-url URL        Master URL (required for replica)\n"
                "  --cluster-token TOKEN   Shared cluster secret\n"
                "  --idle-timeout N        Idle connection timeout seconds (300)\n"
                "  --cors-origin LIST      CORS allowlist (comma-sep or repeatable)\n"
                "  --reset-admin           Unlock + clear failed_attempts on admin user\n"
                "  --config FILE           Load JSON config\n";
            std::exit(0);
        }
    }
    return c;
}
// P1-25: full-coverage JSON loader. Every public field on ServerConfig is
// readable from the file. Unknown keys are ignored — nlohmann/json's
// `value()` swallows them — so old configs keep loading after schema
// extensions.
ServerConfig ServerConfig::from_file(const std::string& path) {
    ServerConfig c;
    std::ifstream f(path);
    if (!f) {
        std::cerr << "[Delta][WARN] config file not found: " << path << std::endl;
        return c;
    }
    json j;
    try { f >> j; }
    catch (const std::exception& e) {
        std::cerr << "[Delta][ERR] config parse failed: " << e.what() << std::endl;
        return c;
    }

    c.data_dir              = j.value("data_dir",               c.data_dir);
    c.http_host             = j.value("http_host",              c.http_host);
    c.http_port             = j.value("http_port",              c.http_port);
    c.cache_max_keys        = j.value("cache_max_keys",         c.cache_max_keys);
    c.max_connections       = j.value("max_connections",        c.max_connections);
    c.idle_timeout_sec      = j.value("idle_timeout_sec",       c.idle_timeout_sec);
    c.http_threads          = j.value("http_threads",           c.http_threads);
    c.keepalive_max_count   = j.value("keepalive_max_count",    c.keepalive_max_count);
    c.keepalive_timeout_sec = j.value("keepalive_timeout_sec",  c.keepalive_timeout_sec);
    c.deltaql_port          = j.value("deltaql_port",           c.deltaql_port);
    c.ws_port               = j.value("ws_port",                c.ws_port);
    c.role                  = j.value("role",                   c.role);
    c.master_url            = j.value("master_url",             c.master_url);
    c.cluster_token         = j.value("cluster_token",          c.cluster_token);
    c.reset_admin           = j.value("reset_admin",            c.reset_admin);

    if (j.contains("cors") && j["cors"].is_object()) {
        const auto& co = j["cors"];
        if (co.contains("allowed_origins") && co["allowed_origins"].is_array())
            c.cors.allowed_origins = co["allowed_origins"].get<std::vector<std::string>>();
        if (co.contains("allowed_methods") && co["allowed_methods"].is_array())
            c.cors.allowed_methods = co["allowed_methods"].get<std::vector<std::string>>();
        if (co.contains("allowed_headers") && co["allowed_headers"].is_array())
            c.cors.allowed_headers = co["allowed_headers"].get<std::vector<std::string>>();
        c.cors.allow_credentials = co.value("allow_credentials", c.cors.allow_credentials);
    }
    return c;
}
}
