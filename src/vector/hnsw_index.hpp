#pragma once
#include "../core/common.hpp"
#include <unordered_map>
#include <queue>
#include <set>
#include <cmath>
#include <random>
#include <fstream>

namespace delta::vector {

using Vector = std::vector<float>;

struct HNSWConfig {
    int m = 16;
    int ef_construction = 200;
    int ef_search = 50;
    int max_level = 16;
    double ml = 1.0 / std::log(16.0);
    int dimension = 0;
    std::string metric = "cosine"; // cosine | euclidean | dot
};

class HNSWIndex {
public:
    explicit HNSWIndex(HNSWConfig cfg = {}) : cfg_(cfg), rng_(std::random_device{}()) {}

    void set_metric(const std::string& m) { cfg_.metric = m; }
    void set_dimension(int d) { cfg_.dimension = d; }

    void insert(const std::string& id, const Vector& v, const json& metadata = json::object()) {
        std::lock_guard<std::mutex> lk(mu_);
        if (cfg_.dimension == 0) cfg_.dimension = v.size();
        // Replace if exists
        auto exit_it = id_to_internal_.find(id);
        if (exit_it != id_to_internal_.end()) remove_internal(exit_it->second);
        uint64_t internal = next_id_++;
        Node node;
        node.id = id;
        node.vec = v;
        node.metadata = metadata;
        node.level = random_level();
        node.neighbors.resize(node.level + 1);
        nodes_[internal] = std::move(node);
        id_to_internal_[id] = internal;
        if (entry_ == UINT64_MAX) { entry_ = internal; max_level_ = nodes_[internal].level; return; }
        // search from top layer down to find entry for level
        uint64_t cur = entry_;
        int top = max_level_;
        Node& nn = nodes_[internal];
        for (int lc = top; lc > nn.level; --lc) cur = greedy_search(v, cur, lc);
        for (int lc = std::min(top, nn.level); lc >= 0; --lc) {
            auto W = search_layer(v, {cur}, cfg_.ef_construction, lc);
            auto neighbors = select_neighbors(v, W, cfg_.m);
            for (auto& nb : neighbors) {
                nn.neighbors[lc].push_back(nb);
                auto& other = nodes_[nb];
                if ((int)other.neighbors.size() <= lc) other.neighbors.resize(lc + 1);
                other.neighbors[lc].push_back(internal);
                // pruning
                if ((int)other.neighbors[lc].size() > cfg_.m) {
                    auto pruned = select_neighbors(other.vec, other.neighbors[lc], cfg_.m);
                    other.neighbors[lc] = pruned;
                }
            }
            cur = neighbors.empty() ? cur : neighbors[0];
        }
        if (nn.level > max_level_) { max_level_ = nn.level; entry_ = internal; }
    }

    bool remove(const std::string& id) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = id_to_internal_.find(id); if (it == id_to_internal_.end()) return false;
        remove_internal(it->second);
        return true;
    }

    struct Result { std::string id; float score; json metadata; };
    std::vector<Result> search(const Vector& q, int k, float min_score = -1e9) {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<Result> out;
        if (entry_ == UINT64_MAX) return out;
        uint64_t cur = entry_;
        for (int lc = max_level_; lc > 0; --lc) cur = greedy_search(q, cur, lc);
        auto cands = search_layer(q, {cur}, std::max(cfg_.ef_search, k), 0);
        // sort by distance asc
        std::vector<std::pair<float, uint64_t>> scored;
        for (auto& c : cands) scored.push_back({distance(q, nodes_[c].vec), c});
        std::sort(scored.begin(), scored.end(), [](auto& a, auto& b){return a.first < b.first;});
        for (auto& [d, n] : scored) {
            float score = dist_to_score(d);
            if (score < min_score) continue;
            out.push_back({nodes_[n].id, score, nodes_[n].metadata});
            if ((int)out.size() >= k) break;
        }
        return out;
    }

    size_t size() const { return nodes_.size(); }
    int dimension() const { return cfg_.dimension; }
    std::string metric() const { return cfg_.metric; }

    void save(const std::string& path) const {
        std::lock_guard<std::mutex> lk(mu_);
        json j;
        j["dim"] = cfg_.dimension;
        j["metric"] = cfg_.metric;
        j["m"] = cfg_.m;
        j["nodes"] = json::array();
        for (auto& [iid, n] : nodes_) {
            json nj;
            nj["iid"] = iid; nj["id"] = n.id; nj["level"] = n.level; nj["vec"] = n.vec;
            nj["metadata"] = n.metadata;
            nj["neighbors"] = n.neighbors;
            j["nodes"].push_back(nj);
        }
        j["entry"] = entry_; j["max_level"] = max_level_; j["next_id"] = next_id_;
        std::ofstream out(path); out << j.dump();
    }
    void load(const std::string& path) {
        std::ifstream in(path); if (!in) return;
        json j; in >> j;
        std::lock_guard<std::mutex> lk(mu_);
        cfg_.dimension = j.value("dim", 0);
        cfg_.metric = j.value("metric", "cosine");
        nodes_.clear(); id_to_internal_.clear();
        for (auto& nj : j["nodes"]) {
            Node n; n.id = nj["id"]; n.level = nj["level"]; n.vec = nj["vec"].get<Vector>();
            n.metadata = nj.value("metadata", json::object());
            n.neighbors = nj["neighbors"].get<std::vector<std::vector<uint64_t>>>();
            uint64_t iid = nj["iid"];
            nodes_[iid] = n; id_to_internal_[n.id] = iid;
        }
        entry_ = j.value("entry", UINT64_MAX);
        max_level_ = j.value("max_level", 0);
        next_id_ = j.value("next_id", 1ull);
    }

private:
    struct Node {
        std::string id;
        Vector vec;
        int level = 0;
        std::vector<std::vector<uint64_t>> neighbors;
        json metadata;
    };
    HNSWConfig cfg_;
    std::unordered_map<uint64_t, Node> nodes_;
    std::unordered_map<std::string, uint64_t> id_to_internal_;
    uint64_t entry_ = UINT64_MAX;
    int max_level_ = 0;
    uint64_t next_id_ = 1;
    mutable std::mutex mu_;
    std::mt19937 rng_;

    void remove_internal(uint64_t iid) {
        auto it = nodes_.find(iid); if (it == nodes_.end()) return;
        // remove backlinks
        for (int lc = 0; lc <= it->second.level; ++lc) {
            for (auto nb : it->second.neighbors[lc]) {
                auto nit = nodes_.find(nb); if (nit == nodes_.end()) continue;
                if ((int)nit->second.neighbors.size() > lc) {
                    auto& v = nit->second.neighbors[lc];
                    v.erase(std::remove(v.begin(), v.end(), iid), v.end());
                }
            }
        }
        id_to_internal_.erase(it->second.id);
        nodes_.erase(it);
        if (entry_ == iid) {
            entry_ = nodes_.empty() ? UINT64_MAX : nodes_.begin()->first;
            max_level_ = nodes_.empty() ? 0 : nodes_[entry_].level;
        }
    }
    int random_level() {
        std::uniform_real_distribution<double> d(0.0, 1.0);
        double r = d(rng_);
        int lvl = (int)(-std::log(r) * cfg_.ml);
        return std::min(lvl, cfg_.max_level);
    }
    float distance(const Vector& a, const Vector& b) const {
        if (cfg_.metric == "euclidean") {
            float s = 0; size_t n = std::min(a.size(), b.size());
            for (size_t i = 0; i < n; ++i) { float d = a[i] - b[i]; s += d * d; }
            return std::sqrt(s);
        } else if (cfg_.metric == "dot") {
            float s = 0; size_t n = std::min(a.size(), b.size());
            for (size_t i = 0; i < n; ++i) s += a[i] * b[i];
            return -s; // smaller is better
        } else { // cosine
            float dot = 0, na = 0, nb = 0; size_t n = std::min(a.size(), b.size());
            for (size_t i = 0; i < n; ++i) { dot += a[i]*b[i]; na += a[i]*a[i]; nb += b[i]*b[i]; }
            float denom = std::sqrt(na) * std::sqrt(nb) + 1e-10f;
            return 1.0f - (dot / denom);
        }
    }
    float dist_to_score(float d) const {
        if (cfg_.metric == "cosine") return 1.0f - d;
        if (cfg_.metric == "dot") return -d;
        return 1.0f / (1.0f + d);
    }
    uint64_t greedy_search(const Vector& q, uint64_t entry, int level) {
        uint64_t cur = entry;
        float cur_d = distance(q, nodes_[cur].vec);
        bool changed = true;
        while (changed) {
            changed = false;
            if ((int)nodes_[cur].neighbors.size() <= level) break;
            for (auto nb : nodes_[cur].neighbors[level]) {
                float d = distance(q, nodes_[nb].vec);
                if (d < cur_d) { cur_d = d; cur = nb; changed = true; }
            }
        }
        return cur;
    }
    std::vector<uint64_t> search_layer(const Vector& q, const std::vector<uint64_t>& entries, int ef, int level) {
        std::set<uint64_t> visited;
        // candidate min-heap (closest first), result max-heap
        auto cmp_min = [](const std::pair<float,uint64_t>& a, const std::pair<float,uint64_t>& b){return a.first > b.first;};
        auto cmp_max = [](const std::pair<float,uint64_t>& a, const std::pair<float,uint64_t>& b){return a.first < b.first;};
        std::priority_queue<std::pair<float,uint64_t>, std::vector<std::pair<float,uint64_t>>, decltype(cmp_min)> cands(cmp_min);
        std::priority_queue<std::pair<float,uint64_t>, std::vector<std::pair<float,uint64_t>>, decltype(cmp_max)> result(cmp_max);
        for (auto e : entries) {
            float d = distance(q, nodes_[e].vec);
            cands.push({d, e}); result.push({d, e}); visited.insert(e);
        }
        while (!cands.empty()) {
            auto [d, c] = cands.top(); cands.pop();
            if (!result.empty() && d > result.top().first && (int)result.size() >= ef) break;
            if ((int)nodes_[c].neighbors.size() <= level) continue;
            for (auto nb : nodes_[c].neighbors[level]) {
                if (visited.count(nb)) continue;
                visited.insert(nb);
                float dn = distance(q, nodes_[nb].vec);
                if ((int)result.size() < ef || dn < result.top().first) {
                    cands.push({dn, nb}); result.push({dn, nb});
                    if ((int)result.size() > ef) result.pop();
                }
            }
        }
        std::vector<uint64_t> out;
        while (!result.empty()) { out.push_back(result.top().second); result.pop(); }
        return out;
    }
    std::vector<uint64_t> select_neighbors(const Vector& q, const std::vector<uint64_t>& cands, int M) {
        std::vector<std::pair<float, uint64_t>> scored;
        for (auto c : cands) scored.push_back({distance(q, nodes_[c].vec), c});
        std::sort(scored.begin(), scored.end(), [](auto& a, auto& b){return a.first < b.first;});
        std::vector<uint64_t> out;
        for (auto& [d, n] : scored) { out.push_back(n); if ((int)out.size() >= M) break; }
        return out;
    }
};

// Per-collection vector index registry
class VectorEngine {
public:
    std::shared_ptr<HNSWIndex> get_or_create(const std::string& key, int dim = 0, const std::string& metric = "cosine") {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = indexes_.find(key);
        if (it != indexes_.end()) return it->second;
        HNSWConfig cfg; cfg.dimension = dim; cfg.metric = metric;
        auto idx = std::make_shared<HNSWIndex>(cfg);
        indexes_[key] = idx;
        return idx;
    }
    std::shared_ptr<HNSWIndex> get(const std::string& key) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = indexes_.find(key); return it == indexes_.end() ? nullptr : it->second;
    }
    void drop(const std::string& key) {
        std::lock_guard<std::mutex> lk(mu_); indexes_.erase(key);
    }
    std::vector<std::string> list() {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<std::string> out; for (auto& [k, _] : indexes_) out.push_back(k); return out;
    }
private:
    std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<HNSWIndex>> indexes_;
};

} // namespace delta::vector
