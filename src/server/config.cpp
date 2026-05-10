#include "config.hpp"
#include <fstream>
#include <cstring>
#include <iostream>

namespace delta::server {

ServerConfig ServerConfig::from_args(int argc, char** argv) {
    ServerConfig c;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string { return (i + 1 < argc) ? std::string(argv[++i]) : std::string(); };
        if (a == "--data") c.data_dir = next();
        else if (a == "--host") c.http_host = next();
        else if (a == "--port") c.http_port = std::stoi(next());
        else if (a == "--threads") c.http_threads = std::stoi(next());
        else if (a == "--max-conn") c.max_connections = std::stoi(next());
        else if (a == "--keepalive-max") c.keepalive_max_count = std::stoi(next());
        else if (a == "--keepalive-timeout") c.keepalive_timeout_sec = std::stoi(next());
        else if (a == "--cache-max-keys") c.cache_max_keys = (size_t)std::stoll(next());
        else if (a == "--deltaql-port") c.deltaql_port = std::stoi(next());
        else if (a == "--ws-port") c.ws_port = std::stoi(next());
        else if (a == "--no-deltaql") c.deltaql_port = 0;
        else if (a == "--no-ws") c.ws_port = 0;
        else if (a == "--reset-admin") c.reset_admin = true;
        else if (a == "--role") c.role = next();
        else if (a == "--master-url") c.master_url = next();
        else if (a == "--cluster-token") c.cluster_token = next();
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
                "  --config FILE           Load JSON config\n";
            std::exit(0);
        }
    }
    return c;
}
ServerConfig ServerConfig::from_file(const std::string& path) {
    ServerConfig c;
    std::ifstream f(path); if (!f) return c;
    json j; f >> j;
    c.data_dir = j.value("data_dir", c.data_dir);
    c.http_host = j.value("http_host", c.http_host);
    c.http_port = j.value("http_port", c.http_port);
    c.cache_max_keys = j.value("cache_max_keys", c.cache_max_keys);
    c.max_connections = j.value("max_connections", c.max_connections);
    return c;
}
}
