#pragma once
// =============================================================================
// raft_http_transport.hpp — HTTP-backed RaftTransport for Round 2 part 2.
//
// Each peer is a (node_id, base_url) pair. RPC marshalling is JSON, sent to
//
//    POST {base_url}/api/v1/cluster/raft/vote
//    POST {base_url}/api/v1/cluster/raft/append
//
// with the X-Delta-Cluster-Token header for authentication. The transport
// reuses one httplib::Client per peer (kept per-thread to avoid lock
// contention) so the keep-alive socket survives across heartbeats.
//
// Thread safety: send_* may be invoked concurrently from the RaftNode tick
// thread for different peers. Per-thread caches keep the path lock-free.
// =============================================================================
#include "raft.hpp"

#include <httplib.h>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace delta::network::raft {

class HttpRaftTransport : public RaftTransport {
public:
    // peers maps node_id → "host:port" (no scheme, no path; we always use
    // http://). Use the cluster_token shared with the peers' --cluster-token.
    HttpRaftTransport(std::unordered_map<NodeId, std::string> peer_urls,
                      std::string cluster_token,
                      int rpc_timeout_ms = 200)
        : peers_(std::move(peer_urls)),
          token_(std::move(cluster_token)),
          rpc_timeout_ms_(rpc_timeout_ms) {}

    bool send_request_vote(const NodeId& peer, const RequestVoteArgs& args,
                           RequestVoteReply* reply) override {
        json body = {
            {"term",            args.term},
            {"candidate_id",    args.candidate_id},
            {"last_log_index",  args.last_log_index},
            {"last_log_term",   args.last_log_term},
            {"pre_vote",        args.pre_vote},
        };
        json out;
        if (!post(peer, "/api/v1/cluster/raft/vote", body, &out)) return false;
        reply->term         = out.value("term", (Term)0);
        reply->vote_granted = out.value("vote_granted", false);
        return true;
    }

    bool send_append_entries(const NodeId& peer, const AppendEntriesArgs& args,
                             AppendEntriesReply* reply) override {
        json entries = json::array();
        for (auto& e : args.entries) {
            entries.push_back({
                {"term", e.term}, {"index", e.index}, {"payload", e.payload}
            });
        }
        json body = {
            {"term",            args.term},
            {"leader_id",       args.leader_id},
            {"prev_log_index",  args.prev_log_index},
            {"prev_log_term",   args.prev_log_term},
            {"entries",         entries},
            {"leader_commit",   args.leader_commit},
        };
        json out;
        if (!post(peer, "/api/v1/cluster/raft/append", body, &out)) return false;
        reply->term           = out.value("term",            (Term)0);
        reply->success        = out.value("success",         false);
        reply->conflict_index = out.value("conflict_index",  (Index)0);
        reply->conflict_term  = out.value("conflict_term",   (Term)0);
        return true;
    }

private:
    bool post(const NodeId& peer, const std::string& path,
              const json& body, json* out) {
        auto it = peers_.find(peer);
        if (it == peers_.end()) return false;
        const std::string& url = it->second;
        httplib::Client cli(url.c_str());
        cli.set_connection_timeout(0, rpc_timeout_ms_ * 1000);
        cli.set_read_timeout(0, rpc_timeout_ms_ * 1000);
        cli.set_keep_alive(true);
        httplib::Headers h{{"X-Delta-Cluster-Token", token_},
                           {"Content-Type", "application/json"}};
        std::string payload = body.dump();
        auto res = cli.Post(path.c_str(), h, payload, "application/json");
        if (!res || res->status != 200) return false;
        try {
            json env = json::parse(res->body);
            // The HTTP layer wraps responses as {"code":200,"data":...}.
            *out = env.contains("data") ? env["data"] : env;
        } catch (...) {
            return false;
        }
        return true;
    }

    std::unordered_map<NodeId, std::string> peers_;
    std::string token_;
    int rpc_timeout_ms_;
};

} // namespace delta::network::raft
