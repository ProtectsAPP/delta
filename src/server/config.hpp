#pragma once
#include "../core/common.hpp"

namespace delta::server {

struct ServerConfig {
    std::string data_dir = "./data";
    std::string http_host = "0.0.0.0";
    int http_port = 16888;
    size_t cache_max_keys = 1000000;
    int max_connections = 1024;
    int idle_timeout_sec = 300;
    // HTTP worker threads. cpp-httplib pins one thread per persistent
    // connection, so this should be >= expected concurrent clients.
    // 0 = auto: max(256, max_connections).
    int http_threads = 0;
    int keepalive_max_count = 1000;
    int keepalive_timeout_sec = 30;

    // One-shot recovery: on startup, force admin status=active + reset
    // failed_attempts. Use after a lockout from too many bad password tries.
    bool reset_admin = false;

    // Native deltaql:// TCP protocol. 0 disables.
    int deltaql_port = 16889;
    // WebSocket bridge (browsers / JS clients). 0 disables.
    int ws_port = 16890;

    // Replication / clustering. role: "standalone" | "master" | "replica".
    std::string role = "standalone";
    std::string master_url;       // required when role == "replica"
    std::string cluster_token;    // shared secret between master and replicas
    json to_json() const { return {{"data_dir",data_dir},{"http_host",http_host},{"http_port",http_port},
        {"cache_max_keys",cache_max_keys},{"max_connections",max_connections},{"idle_timeout_sec",idle_timeout_sec},
        {"http_threads",http_threads},{"keepalive_max_count",keepalive_max_count},{"keepalive_timeout_sec",keepalive_timeout_sec},
        {"deltaql_port",deltaql_port},{"ws_port",ws_port},
        {"role",role},{"master_url",master_url}}; }
    static ServerConfig from_args(int argc, char** argv);
    static ServerConfig from_file(const std::string& path);
};

} // namespace delta::server
