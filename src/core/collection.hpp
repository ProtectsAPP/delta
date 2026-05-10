#pragma once
#include "document.hpp"
#include "../storage/lsm_tree.hpp"
#include <set>

namespace delta {

// Document storage layered over LSM-Tree.
// Key format: doc:{db}:{schema}:{collection}:{id}
// Meta key: meta:collection:{db}:{schema}:{collection}
// Index key: idx:{db}:{schema}:{collection}:{indexname}:{value}:{docid}
class CollectionEngine {
public:
    explicit CollectionEngine(storage::LSMTree* store) : store_(store) {}

    static std::string doc_key(const std::string& db, const std::string& sch, const std::string& col, const std::string& id) {
        return "doc:" + db + ":" + sch + ":" + col + ":" + id;
    }
    static std::string doc_prefix(const std::string& db, const std::string& sch, const std::string& col) {
        return "doc:" + db + ":" + sch + ":" + col + ":";
    }
    static std::string meta_key(const std::string& db, const std::string& sch, const std::string& col) {
        return "meta:collection:" + db + ":" + sch + ":" + col;
    }
    static std::string meta_prefix(const std::string& db = "") {
        return db.empty() ? "meta:collection:" : "meta:collection:" + db + ":";
    }
    static std::string idx_key(const std::string& db, const std::string& sch, const std::string& col,
                                const std::string& idx, const std::string& v, const std::string& id) {
        return "idx:" + db + ":" + sch + ":" + col + ":" + idx + ":" + v + ":" + id;
    }
    static std::string idx_prefix(const std::string& db, const std::string& sch, const std::string& col, const std::string& idx) {
        return "idx:" + db + ":" + sch + ":" + col + ":" + idx + ":";
    }

    // Collection meta
    Status create_collection(const CollectionMeta& m) {
        std::string k = meta_key(m.database, m.schema, m.name);
        std::string existing;
        if (store_->get(k, &existing)) return Status::Duplicate("collection exists");
        CollectionMeta meta = m;
        if (meta.created_at == 0) meta.created_at = now_sec();
        store_->put(k, meta.to_json().dump());
        return Status::Ok();
    }
    bool get_collection(const std::string& db, const std::string& sch, const std::string& name, CollectionMeta* out) {
        std::string s;
        if (!store_->get(meta_key(db, sch, name), &s)) return false;
        *out = CollectionMeta::from_json(json::parse(s));
        return true;
    }
    Status update_collection(const CollectionMeta& m) {
        store_->put(meta_key(m.database, m.schema, m.name), m.to_json().dump());
        return Status::Ok();
    }
    Status drop_collection(const std::string& db, const std::string& sch, const std::string& col) {
        // delete all docs
        for (auto& [k, _] : store_->prefix_scan(doc_prefix(db, sch, col), 100000)) store_->del(k);
        // delete all index entries
        for (auto& [k, _] : store_->prefix_scan("idx:" + db + ":" + sch + ":" + col + ":", 1000000)) store_->del(k);
        store_->del(meta_key(db, sch, col));
        return Status::Ok();
    }
    std::vector<CollectionMeta> list_collections(const std::string& db = "") {
        std::vector<CollectionMeta> out;
        for (auto& [k, v] : store_->prefix_scan(meta_prefix(db), 10000)) {
            try { out.push_back(CollectionMeta::from_json(json::parse(v))); } catch(...) {}
        }
        return out;
    }

    // Document CRUD
    Status insert(const std::string& db, const std::string& sch, const std::string& col,
                  json& doc, std::string& id_out, uint32_t ttl = 0) {
        CollectionMeta m; if (!get_collection(db, sch, col, &m)) return Status::NotFound("collection not found");
        if (!doc.is_object()) return Status::Invalid("document must be object");
        std::string id = doc.value("_id", std::string());
        if (id.empty()) { id = gen_id(); doc["_id"] = id; }
        std::string k = doc_key(db, sch, col, id);
        std::string existing;
        if (store_->get(k, &existing)) return Status::Duplicate("document exists");
        // Check unique indexes
        for (auto& idx : m.indexes) {
            if (idx.unique) {
                std::string v = index_value(doc, idx);
                if (!v.empty() || !idx.sparse) {
                    auto found = store_->prefix_scan(idx_prefix(db, sch, col, idx.name) + v + ":", 1);
                    if (!found.empty()) return Status::Duplicate("unique index violation: " + idx.name);
                }
            }
        }
        json envelope = {{"_id", id},{"data", doc},{"version", 1},{"created_at", now_ms()},{"updated_at", now_ms()},{"ttl", ttl}};
        store_->put(k, envelope.dump());
        // index insertions
        for (auto& idx : m.indexes) update_index(db, sch, col, idx, id, json(), doc);
        m.document_count++;
        update_collection(m);
        id_out = id;
        return Status::Ok();
    }

    bool get(const std::string& db, const std::string& sch, const std::string& col, const std::string& id, json* out) {
        std::string s;
        if (!store_->get(doc_key(db, sch, col, id), &s)) return false;
        json env = json::parse(s);
        // ttl check
        uint32_t ttl = env.value("ttl", 0u);
        uint64_t created = env.value("created_at", 0ull);
        if (ttl > 0 && now_ms() > created + ttl * 1000ull) return false;
        *out = env;
        return true;
    }

    Status update(const std::string& db, const std::string& sch, const std::string& col,
                  const std::string& id, const json& update_ops, json* updated_out = nullptr) {
        CollectionMeta m; if (!get_collection(db, sch, col, &m)) return Status::NotFound("collection not found");
        json env;
        if (!get(db, sch, col, id, &env)) return Status::NotFound("document not found");
        json old = env["data"];
        json newd = UpdateApplier::apply(old, update_ops);
        newd["_id"] = id;
        // unique index check
        for (auto& idx : m.indexes) {
            if (idx.unique) {
                std::string nv = index_value(newd, idx);
                std::string ov = index_value(old, idx);
                if (nv != ov && (!nv.empty() || !idx.sparse)) {
                    auto found = store_->prefix_scan(idx_prefix(db, sch, col, idx.name) + nv + ":", 1);
                    if (!found.empty()) return Status::Duplicate("unique index violation: " + idx.name);
                }
            }
        }
        env["data"] = newd;
        env["version"] = env.value("version", 1ull) + 1;
        env["updated_at"] = now_ms();
        store_->put(doc_key(db, sch, col, id), env.dump());
        for (auto& idx : m.indexes) update_index(db, sch, col, idx, id, old, newd);
        if (updated_out) *updated_out = env;
        return Status::Ok();
    }

    Status remove(const std::string& db, const std::string& sch, const std::string& col, const std::string& id) {
        CollectionMeta m; if (!get_collection(db, sch, col, &m)) return Status::NotFound("collection not found");
        json env;
        if (!get(db, sch, col, id, &env)) return Status::NotFound("document not found");
        store_->del(doc_key(db, sch, col, id));
        for (auto& idx : m.indexes) update_index(db, sch, col, idx, id, env["data"], json());
        if (m.document_count > 0) m.document_count--;
        update_collection(m);
        return Status::Ok();
    }

    // Find with filter, sort, skip, limit, projection
    std::vector<json> find(const std::string& db, const std::string& sch, const std::string& col,
                           const json& filter, const json& sort_obj, size_t skip, size_t limit,
                           const json& projection, size_t* total = nullptr) {
        std::vector<json> matched;
        for (auto& [k, v] : store_->prefix_scan(doc_prefix(db, sch, col), 1000000)) {
            try {
                json env = json::parse(v);
                uint32_t ttl = env.value("ttl", 0u);
                uint64_t created = env.value("created_at", 0ull);
                if (ttl > 0 && now_ms() > created + ttl * 1000ull) continue;
                if (FilterMatcher::match(env["data"], filter)) matched.push_back(env);
            } catch(...) {}
        }
        if (total) *total = matched.size();
        // sort
        if (sort_obj.is_object() && !sort_obj.empty()) {
            std::vector<std::pair<std::string, int>> rules;
            for (auto& [k, v] : sort_obj.items()) rules.push_back({k, v.get<int>()});
            std::sort(matched.begin(), matched.end(), [&](const json& a, const json& b) {
                for (auto& [f, dir] : rules) {
                    json va = FilterMatcher::get_field(a["data"], f);
                    json vb = FilterMatcher::get_field(b["data"], f);
                    if (va == vb) continue;
                    return dir > 0 ? va < vb : va > vb;
                }
                return false;
            });
        }
        if (skip > matched.size()) return {};
        size_t end = std::min(matched.size(), skip + limit);
        std::vector<json> result(matched.begin() + skip, matched.begin() + end);
        // projection
        if (projection.is_object() && !projection.empty()) {
            for (auto& env : result) {
                json& d = env["data"];
                json out = json::object();
                bool include_mode = false;
                for (auto& [k, v] : projection.items()) if (v.is_number() && v.get<int>() == 1) { include_mode = true; break; }
                if (include_mode) {
                    out["_id"] = d.value("_id", std::string());
                    for (auto& [k, v] : projection.items()) if (v.get<int>() == 1) {
                        json fv = FilterMatcher::get_field(d, k);
                        if (!fv.is_null()) UpdateApplier::set_field(out, k, fv);
                    }
                    d = out;
                } else {
                    for (auto& [k, v] : projection.items()) if (v.get<int>() == 0) UpdateApplier::unset_field(d, k);
                }
            }
        }
        return result;
    }

    size_t count(const std::string& db, const std::string& sch, const std::string& col, const json& filter) {
        size_t c = 0;
        for (auto& [k, v] : store_->prefix_scan(doc_prefix(db, sch, col), 1000000)) {
            try {
                json env = json::parse(v);
                if (FilterMatcher::match(env["data"], filter)) c++;
            } catch(...) {}
        }
        return c;
    }

    // simple aggregation pipeline: $match, $group, $sort, $limit, $project
    json aggregate(const std::string& db, const std::string& sch, const std::string& col, const json& pipeline) {
        std::vector<json> docs;
        for (auto& [k, v] : store_->prefix_scan(doc_prefix(db, sch, col), 1000000)) {
            try { docs.push_back(json::parse(v)["data"]); } catch(...) {}
        }
        for (auto& stage : pipeline) {
            if (stage.contains("$match")) {
                std::vector<json> out; for (auto& d : docs) if (FilterMatcher::match(d, stage["$match"])) out.push_back(d); docs = out;
            } else if (stage.contains("$group")) {
                json g = stage["$group"];
                std::string id_expr;
                if (g["_id"].is_string()) {
                    id_expr = g["_id"].get<std::string>();
                    if (id_expr.size() && id_expr[0] == '$') id_expr = id_expr.substr(1);
                }
                std::map<json, json> groups;
                for (auto& d : docs) {
                    json key = id_expr.empty() ? json(nullptr) : FilterMatcher::get_field(d, id_expr);
                    if (!groups.count(key)) { groups[key] = json::object(); groups[key]["_id"] = key; }
                    for (auto& [field, expr] : g.items()) {
                        if (field == "_id") continue;
                        if (!expr.is_object()) continue;
                        for (auto& [op, val] : expr.items()) {
                            std::string vfield = val.is_string() && !val.get<std::string>().empty() && val.get<std::string>()[0]=='$' ? val.get<std::string>().substr(1) : "";
                            json fv = vfield.empty() ? val : FilterMatcher::get_field(d, vfield);
                            if (op == "$sum") groups[key][field] = groups[key].value(field, 0.0) + (fv.is_number() ? fv.get<double>() : 1.0);
                            else if (op == "$avg") {
                                double s = groups[key].value(field, 0.0);
                                int cnt = groups[key].value("_count_"+field, 0) + 1;
                                if (fv.is_number()) s += fv.get<double>();
                                groups[key][field] = s;
                                groups[key]["_count_"+field] = cnt;
                            }
                            else if (op == "$min") { if (!groups[key].contains(field) || (fv.is_number() && fv < groups[key][field])) groups[key][field] = fv; }
                            else if (op == "$max") { if (!groups[key].contains(field) || (fv.is_number() && fv > groups[key][field])) groups[key][field] = fv; }
                            else if (op == "$count") groups[key][field] = groups[key].value(field, 0) + 1;
                        }
                    }
                }
                docs.clear();
                for (auto& [k, v] : groups) {
                    json out = v;
                    // resolve avg
                    for (auto it = out.begin(); it != out.end();) {
                        if (it.key().rfind("_count_", 0) == 0) {
                            std::string f = it.key().substr(7);
                            if (out.contains(f) && out[f].is_number()) out[f] = out[f].get<double>() / it.value().get<int>();
                            it = out.erase(it);
                        } else ++it;
                    }
                    docs.push_back(out);
                }
            } else if (stage.contains("$sort")) {
                json s = stage["$sort"];
                std::vector<std::pair<std::string,int>> rules; for (auto& [k,v]:s.items()) rules.push_back({k,v.get<int>()});
                std::sort(docs.begin(), docs.end(), [&](const json& a, const json& b){
                    for (auto& [f, d] : rules) {
                        json va = FilterMatcher::get_field(a, f);
                        json vb = FilterMatcher::get_field(b, f);
                        if (va == vb) continue;
                        return d > 0 ? va < vb : va > vb;
                    }
                    return false;
                });
            } else if (stage.contains("$limit")) {
                size_t n = stage["$limit"].get<size_t>();
                if (docs.size() > n) docs.resize(n);
            } else if (stage.contains("$skip")) {
                size_t n = stage["$skip"].get<size_t>();
                if (docs.size() <= n) docs.clear(); else docs.erase(docs.begin(), docs.begin()+n);
            }
        }
        return docs;
    }

    Status create_index(const std::string& db, const std::string& sch, const std::string& col, const IndexDef& idx) {
        CollectionMeta m;
        if (!get_collection(db, sch, col, &m)) return Status::NotFound("collection not found");
        for (auto& e : m.indexes) if (e.name == idx.name) return Status::Duplicate("index exists");
        m.indexes.push_back(idx);
        update_collection(m);
        // build index over existing docs
        for (auto& [k, v] : store_->prefix_scan(doc_prefix(db, sch, col), 1000000)) {
            try {
                json env = json::parse(v);
                update_index(db, sch, col, idx, env.value("_id", std::string()), json(), env["data"]);
            } catch(...) {}
        }
        return Status::Ok();
    }
    Status drop_index(const std::string& db, const std::string& sch, const std::string& col, const std::string& name) {
        CollectionMeta m;
        if (!get_collection(db, sch, col, &m)) return Status::NotFound("collection not found");
        auto it = std::remove_if(m.indexes.begin(), m.indexes.end(), [&](const IndexDef& i){return i.name == name;});
        if (it == m.indexes.end()) return Status::NotFound("index not found");
        m.indexes.erase(it, m.indexes.end());
        update_collection(m);
        for (auto& [k, _] : store_->prefix_scan(idx_prefix(db, sch, col, name), 1000000)) store_->del(k);
        return Status::Ok();
    }

private:
    std::string index_value(const json& doc, const IndexDef& idx) {
        std::string out;
        for (auto& f : idx.fields) {
            json v = FilterMatcher::get_field(doc, f);
            out += v.is_null() ? "" : (v.is_string() ? v.get<std::string>() : v.dump());
            out += "\x1F";
        }
        return out;
    }
    void update_index(const std::string& db, const std::string& sch, const std::string& col,
                       const IndexDef& idx, const std::string& id, const json& old_doc, const json& new_doc) {
        if (!old_doc.is_null()) {
            std::string ov = index_value(old_doc, idx);
            store_->del(idx_key(db, sch, col, idx.name, ov, id));
        }
        if (!new_doc.is_null() && new_doc.is_object()) {
            std::string nv = index_value(new_doc, idx);
            if (!nv.empty() || !idx.sparse) {
                store_->put(idx_key(db, sch, col, idx.name, nv, id), id);
            }
        }
    }
    storage::LSMTree* store_;
};

} // namespace delta
