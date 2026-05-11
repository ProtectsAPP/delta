#pragma once
#include "common.hpp"
#include "../storage/lsm_tree.hpp"
#include <regex>
#include <shared_mutex>

namespace delta {

// P1-7: a process-wide cache of compiled std::regex objects keyed by
// pattern string. Compilation is *expensive* (orders of magnitude more
// than evaluation), so without this cache a `{$regex: ".*foo.*"}` query
// over 10³ documents pays 10³ compilations. The cache uses a shared_mutex
// so concurrent matchers can read in parallel and only contend on first
// insertion of a new pattern. We don't evict — the working set of distinct
// patterns in a workload is small and bounded by query templates.
class RegexCache {
public:
    static const std::regex* get(const std::string& pat) {
        auto& self = instance();
        {
            std::shared_lock<std::shared_mutex> rl(self.mu_);
            auto it = self.cache_.find(pat);
            if (it != self.cache_.end()) return &it->second;
        }
        std::unique_lock<std::shared_mutex> wl(self.mu_);
        // Double-check after acquiring the write lock.
        auto it = self.cache_.find(pat);
        if (it != self.cache_.end()) return &it->second;
        try {
            auto [ins, ok] = self.cache_.emplace(pat,
                std::regex(pat, std::regex::ECMAScript | std::regex::optimize));
            return &ins->second;
        } catch (const std::regex_error&) {
            return nullptr;        // bad pattern — caller treats as "no match".
        }
    }
private:
    static RegexCache& instance() { static RegexCache c; return c; }
    std::shared_mutex                          mu_;
    std::unordered_map<std::string, std::regex> cache_;
};

struct IndexDef {
    std::string name;
    std::vector<std::string> fields;
    std::string type = "btree"; // btree, hash, fulltext, vector
    bool unique = false;
    bool sparse = false;
    json to_json() const { return {{"name",name},{"fields",fields},{"type",type},{"unique",unique},{"sparse",sparse}}; }
    static IndexDef from_json(const json& j) {
        IndexDef i;
        i.name = j.value("name", "");
        i.fields = j.value("fields", std::vector<std::string>{});
        i.type = j.value("type", "btree");
        i.unique = j.value("unique", false);
        i.sparse = j.value("sparse", false);
        return i;
    }
};

struct CollectionMeta {
    std::string database;
    std::string schema;
    std::string name;
    std::vector<IndexDef> indexes;
    uint64_t created_at = 0;
    uint64_t document_count = 0;
    bool rls_enabled = false;
    json to_json() const {
        json arr = json::array();
        for (auto& i : indexes) arr.push_back(i.to_json());
        return {{"database",database},{"schema",schema},{"name",name},{"indexes",arr},
                {"created_at",created_at},{"document_count",document_count},{"rls_enabled",rls_enabled}};
    }
    static CollectionMeta from_json(const json& j) {
        CollectionMeta m;
        m.database = j.value("database","");
        m.schema = j.value("schema","public");
        m.name = j.value("name","");
        m.created_at = j.value("created_at", 0ull);
        m.document_count = j.value("document_count", 0ull);
        m.rls_enabled = j.value("rls_enabled", false);
        if (j.contains("indexes")) for (auto& x : j["indexes"]) m.indexes.push_back(IndexDef::from_json(x));
        return m;
    }
};

// MongoDB-like filter matcher
class FilterMatcher {
public:
    static json get_field(const json& doc, const std::string& path) {
        size_t pos = 0; json cur = doc;
        while (pos < path.size()) {
            size_t dot = path.find('.', pos);
            std::string seg = path.substr(pos, dot == std::string::npos ? std::string::npos : dot - pos);
            if (cur.is_object() && cur.contains(seg)) cur = cur[seg];
            else return nullptr;
            if (dot == std::string::npos) break;
            pos = dot + 1;
        }
        return cur;
    }
    static bool match(const json& doc, const json& filter) {
        if (filter.is_null() || (filter.is_object() && filter.empty())) return true;
        if (!filter.is_object()) return false;
        for (auto it = filter.begin(); it != filter.end(); ++it) {
            const std::string& k = it.key();
            const json& v = it.value();
            if (k == "$and") {
                for (auto& c : v) if (!match(doc, c)) return false;
            } else if (k == "$or") {
                bool any = false;
                for (auto& c : v) if (match(doc, c)) { any = true; break; }
                if (!any) return false;
            } else if (k == "$nor") {
                for (auto& c : v) if (match(doc, c)) return false;
            } else if (k == "$not") {
                if (match(doc, v)) return false;
            } else {
                json field_val = get_field(doc, k);
                if (!match_value(field_val, v)) return false;
            }
        }
        return true;
    }
private:
    static bool match_value(const json& field, const json& cond) {
        if (!cond.is_object()) return field == cond;
        for (auto it = cond.begin(); it != cond.end(); ++it) {
            const std::string& op = it.key();
            const json& v = it.value();
            if (op == "$eq") { if (field != v) return false; }
            else if (op == "$ne") { if (field == v) return false; }
            else if (op == "$gt") { if (!(field > v)) return false; }
            else if (op == "$gte") { if (!(field >= v)) return false; }
            else if (op == "$lt") { if (!(field < v)) return false; }
            else if (op == "$lte") { if (!(field <= v)) return false; }
            else if (op == "$in") { bool found=false; for (auto& x : v) if (field==x){found=true;break;} if(!found) return false; }
            else if (op == "$nin") { for (auto& x : v) if (field==x) return false; }
            else if (op == "$exists") { bool e = !field.is_null(); if (e != v.get<bool>()) return false; }
            else if (op == "$regex") {
                if (!field.is_string()) return false;
                // P1-7: cached compile.
                auto* re = RegexCache::get(v.get<std::string>());
                if (!re) return false;
                if (!std::regex_search(field.get<std::string>(), *re)) return false;
            }
            else if (op == "$contains") {
                if (field.is_array()) { bool f=false; for(auto&x:field) if(x==v){f=true;break;} if(!f) return false; }
                else if (field.is_string()) { if (field.get<std::string>().find(v.get<std::string>())==std::string::npos) return false; }
                else return false;
            }
            else { /* unknown -> equality */ if (field != cond) return false; }
        }
        return true;
    }
};

// Update operator applier
class UpdateApplier {
public:
    static json apply(json doc, const json& update) {
        for (auto it = update.begin(); it != update.end(); ++it) {
            const std::string& op = it.key();
            const json& v = it.value();
            if (op == "$set" || op == "set") for (auto& [k, val] : v.items()) set_field(doc, k, val);
            else if (op == "$unset" || op == "unset") {
                if (v.is_object()) for (auto& [k, _] : v.items()) unset_field(doc, k);
                else if (v.is_array()) for (auto& k : v) unset_field(doc, k.get<std::string>());
            }
            else if (op == "$inc" || op == "inc") for (auto& [k, val] : v.items()) {
                json cur = FilterMatcher::get_field(doc, k);
                double base = cur.is_number() ? cur.get<double>() : 0.0;
                set_field(doc, k, base + val.get<double>());
            }
            else if (op == "$mul" || op == "mul") for (auto& [k, val] : v.items()) {
                json cur = FilterMatcher::get_field(doc, k);
                double base = cur.is_number() ? cur.get<double>() : 0.0;
                set_field(doc, k, base * val.get<double>());
            }
            else if (op == "$push" || op == "push") for (auto& [k, val] : v.items()) {
                json cur = FilterMatcher::get_field(doc, k);
                if (!cur.is_array()) cur = json::array();
                cur.push_back(val);
                set_field(doc, k, cur);
            }
            else if (op == "$pull" || op == "pull") for (auto& [k, val] : v.items()) {
                json cur = FilterMatcher::get_field(doc, k);
                if (cur.is_array()) {
                    json out = json::array();
                    for (auto& x : cur) if (x != val) out.push_back(x);
                    set_field(doc, k, out);
                }
            }
            else if (op == "$addToSet" || op == "addToSet") for (auto& [k, val] : v.items()) {
                json cur = FilterMatcher::get_field(doc, k);
                if (!cur.is_array()) cur = json::array();
                bool found = false; for (auto& x : cur) if (x == val) { found = true; break; }
                if (!found) cur.push_back(val);
                set_field(doc, k, cur);
            }
            else if (op == "$rename" || op == "rename") for (auto& [k, val] : v.items()) {
                json cur = FilterMatcher::get_field(doc, k);
                if (!cur.is_null()) { unset_field(doc, k); set_field(doc, val.get<std::string>(), cur); }
            }
            else { // top-level field assignment fallback (treat update as $set)
                set_field(doc, op, v);
            }
        }
        return doc;
    }
    static void set_field(json& doc, const std::string& path, const json& val) {
        size_t pos = 0; json* cur = &doc;
        while (true) {
            size_t dot = path.find('.', pos);
            std::string seg = path.substr(pos, dot == std::string::npos ? std::string::npos : dot - pos);
            if (dot == std::string::npos) { (*cur)[seg] = val; return; }
            if (!cur->is_object()) *cur = json::object();
            if (!cur->contains(seg) || !(*cur)[seg].is_object()) (*cur)[seg] = json::object();
            cur = &(*cur)[seg];
            pos = dot + 1;
        }
    }
    static void unset_field(json& doc, const std::string& path) {
        size_t dot = path.rfind('.');
        if (dot == std::string::npos) { if (doc.is_object()) doc.erase(path); return; }
        json* cur = &doc;
        size_t pos = 0;
        while (pos < dot) {
            size_t d = path.find('.', pos);
            std::string seg = path.substr(pos, d - pos);
            if (!cur->is_object() || !cur->contains(seg)) return;
            cur = &(*cur)[seg];
            pos = d + 1;
        }
        if (cur->is_object()) cur->erase(path.substr(dot + 1));
    }
};

} // namespace delta
