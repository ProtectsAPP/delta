#pragma once
#include "../core/common.hpp"
#include "../storage/lsm_tree.hpp"
#include "../core/document.hpp"
#include <set>

namespace delta::database {

struct Database {
    uint64_t id = 0;
    std::string name;
    std::string owner;
    uint64_t created_at = 0;
    uint64_t updated_at = 0;
    int max_connections = 100;
    int default_ttl = 0;
    json options = json::object();
    json to_json() const { return {{"id",id},{"name",name},{"owner",owner},{"created_at",created_at},
        {"updated_at",updated_at},{"max_connections",max_connections},{"default_ttl",default_ttl},{"options",options}}; }
    static Database from_json(const json& j) {
        Database d; d.id=j.value("id",0ull); d.name=j.value("name",""); d.owner=j.value("owner","");
        d.created_at=j.value("created_at",0ull); d.updated_at=j.value("updated_at",0ull);
        d.max_connections=j.value("max_connections",100); d.default_ttl=j.value("default_ttl",0);
        d.options=j.value("options",json::object()); return d;
    }
};

struct Schema {
    uint64_t id = 0;
    std::string database;
    std::string name;
    std::string owner;
    uint64_t created_at = 0;
    json to_json() const { return {{"id",id},{"database",database},{"name",name},
        {"owner",owner},{"created_at",created_at}}; }
    static Schema from_json(const json& j) {
        Schema s; s.id=j.value("id",0ull); s.database=j.value("database",""); s.name=j.value("name","");
        s.owner=j.value("owner",""); s.created_at=j.value("created_at",0ull); return s;
    }
};

struct RLSPolicy {
    uint64_t id = 0;
    std::string name;
    std::string database;
    std::string schema;
    std::string collection;
    std::string command = "ALL"; // ALL, SELECT, INSERT, UPDATE, DELETE
    std::vector<std::string> roles; // empty => all
    std::string using_expr;
    std::string with_check_expr;
    bool enabled = true;
    uint64_t created_at = 0;
    json to_json() const { return {{"id",id},{"name",name},{"database",database},{"schema",schema},
        {"collection",collection},{"command",command},{"roles",roles},
        {"using_expr",using_expr},{"with_check_expr",with_check_expr},
        {"enabled",enabled},{"created_at",created_at}}; }
    static RLSPolicy from_json(const json& j) {
        RLSPolicy p; p.id=j.value("id",0ull); p.name=j.value("name","");
        p.database=j.value("database",""); p.schema=j.value("schema","public");
        p.collection=j.value("collection",""); p.command=j.value("command","ALL");
        p.roles=j.value("roles",std::vector<std::string>{});
        p.using_expr=j.value("using_expr",""); p.with_check_expr=j.value("with_check_expr","");
        p.enabled=j.value("enabled",true); p.created_at=j.value("created_at",0ull);
        return p;
    }
};

class DatabaseManager {
public:
    explicit DatabaseManager(storage::LSMTree* store) : store_(store) { load(); ensure_default(); }

    Status create_database(const std::string& name, const std::string& owner, const json& opts = json::object()) {
        std::lock_guard<std::mutex> lk(mu_);
        if (databases_.count(name)) return Status::Duplicate("database exists");
        Database d; d.id = next_db_id_++; d.name = name; d.owner = owner;
        d.created_at = d.updated_at = now_ms();
        d.max_connections = opts.value("max_connections", 100);
        d.default_ttl = opts.value("default_ttl", 0);
        d.options = opts;
        databases_[name] = d;
        save_db(d);
        // create default schema "public"
        create_schema_locked(name, "public", owner);
        return Status::Ok();
    }
    Status drop_database(const std::string& name, bool force = false) {
        std::lock_guard<std::mutex> lk(mu_);
        if (name == "default" || name == "delta_system") return Status::Forbidden("cannot drop system database");
        auto it = databases_.find(name); if (it == databases_.end()) return Status::NotFound("database not found");
        store_->del("db:" + name);
        databases_.erase(it);
        // drop schemas
        if (schemas_.count(name)) {
            for (auto& [sn, _] : schemas_[name]) store_->del("schema:" + name + ":" + sn);
            schemas_.erase(name);
        }
        return Status::Ok();
    }
    Status alter_database(const std::string& name, const json& opts) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = databases_.find(name); if (it == databases_.end()) return Status::NotFound("database not found");
        Database& d = it->second;
        if (opts.contains("max_connections")) d.max_connections = opts["max_connections"];
        if (opts.contains("default_ttl")) d.default_ttl = opts["default_ttl"];
        if (opts.contains("owner")) d.owner = opts["owner"];
        d.updated_at = now_ms();
        save_db(d);
        return Status::Ok();
    }
    Status rename_database(const std::string& old_name, const std::string& new_name) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = databases_.find(old_name); if (it == databases_.end()) return Status::NotFound("not found");
        if (databases_.count(new_name)) return Status::Duplicate("name exists");
        Database d = it->second; d.name = new_name; d.updated_at = now_ms();
        store_->del("db:" + old_name);
        databases_.erase(it);
        databases_[new_name] = d;
        save_db(d);
        return Status::Ok();
    }
    std::optional<Database> get_database(const std::string& name) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = databases_.find(name); if (it == databases_.end()) return std::nullopt;
        return it->second;
    }
    std::vector<Database> list_databases() {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<Database> out; for (auto& [_, d] : databases_) out.push_back(d); return out;
    }
    bool exists(const std::string& name) {
        std::lock_guard<std::mutex> lk(mu_); return databases_.count(name) > 0;
    }

    // ---- Schema ----
    Status create_schema(const std::string& db, const std::string& name, const std::string& owner) {
        std::lock_guard<std::mutex> lk(mu_);
        return create_schema_locked(db, name, owner);
    }
    Status drop_schema(const std::string& db, const std::string& name, bool cascade = false) {
        std::lock_guard<std::mutex> lk(mu_);
        if (name == "public") return Status::Forbidden("cannot drop public schema");
        auto it = schemas_.find(db); if (it == schemas_.end()) return Status::NotFound("database not found");
        auto sit = it->second.find(name); if (sit == it->second.end()) return Status::NotFound("schema not found");
        store_->del("schema:" + db + ":" + name);
        it->second.erase(sit);
        return Status::Ok();
    }
    std::vector<Schema> list_schemas(const std::string& db) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = schemas_.find(db); if (it == schemas_.end()) return {};
        std::vector<Schema> out; for (auto& [_, s] : it->second) out.push_back(s); return out;
    }
    std::optional<Schema> get_schema(const std::string& db, const std::string& name) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = schemas_.find(db); if (it == schemas_.end()) return std::nullopt;
        auto sit = it->second.find(name); if (sit == it->second.end()) return std::nullopt;
        return sit->second;
    }

    // ---- RLS ----
    Status create_policy(const RLSPolicy& p_in) {
        std::lock_guard<std::mutex> lk(mu_);
        RLSPolicy p = p_in;
        std::string key = make_key(p.database, p.schema, p.collection);
        for (auto& x : policies_[key]) if (x.name == p.name) return Status::Duplicate("policy exists");
        p.id = next_policy_id_++; p.created_at = now_ms();
        policies_[key].push_back(p);
        save_policies(key);
        return Status::Ok();
    }
    Status drop_policy(const std::string& db, const std::string& sch, const std::string& col, const std::string& name) {
        std::lock_guard<std::mutex> lk(mu_);
        std::string key = make_key(db, sch, col);
        auto& v = policies_[key];
        auto sz = v.size();
        v.erase(std::remove_if(v.begin(), v.end(), [&](const RLSPolicy& p){return p.name == name;}), v.end());
        if (v.size() == sz) return Status::NotFound("policy not found");
        save_policies(key);
        return Status::Ok();
    }
    Status set_rls_enabled(const std::string& db, const std::string& sch, const std::string& col, bool enabled) {
        std::lock_guard<std::mutex> lk(mu_);
        rls_enabled_[make_key(db, sch, col)] = enabled;
        store_->put("rls_enabled:" + make_key(db, sch, col), enabled ? "1" : "0");
        return Status::Ok();
    }
    bool is_rls_enabled(const std::string& db, const std::string& sch, const std::string& col) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = rls_enabled_.find(make_key(db, sch, col));
        return it != rls_enabled_.end() && it->second;
    }
    std::vector<RLSPolicy> list_policies(const std::string& db, const std::string& sch, const std::string& col) {
        std::lock_guard<std::mutex> lk(mu_);
        return policies_[make_key(db, sch, col)];
    }
    // Apply RLS filter to an existing query filter and return augmented filter
    // Simple expression support: "field = current_user", "field = 'value'", "field = current_user_id()"
    json apply_rls_filter(const std::string& username, const std::string& db, const std::string& sch, const std::string& col,
                          const std::string& cmd, const json& orig_filter,
                          const std::set<std::string>& user_roles) {
        if (!is_rls_enabled(db, sch, col)) return orig_filter;
        std::lock_guard<std::mutex> lk(mu_);
        auto& pols = policies_[make_key(db, sch, col)];
        json or_clause = json::array();
        bool found_applicable = false;
        for (auto& p : pols) {
            if (!p.enabled) continue;
            if (p.command != "ALL" && p.command != cmd) continue;
            if (!p.roles.empty()) {
                bool match = false;
                for (auto& r : p.roles) if (user_roles.count(r)) { match = true; break; }
                if (!match) continue;
            }
            found_applicable = true;
            if (!p.using_expr.empty()) {
                json expr_filter = parse_simple_expr(p.using_expr, username);
                if (!expr_filter.is_null()) or_clause.push_back(expr_filter);
            } else {
                // permissive default
                or_clause.push_back(json::object());
            }
        }
        if (!found_applicable) {
            // RLS enabled but no applicable policies => deny all (PostgreSQL behavior)
            return json{{"_id", json{{"$eq", "____delta_rls_deny____"}}}};
        }
        json combined = orig_filter.is_null() ? json::object() : orig_filter;
        if (or_clause.size() == 1) {
            json merged = combined;
            for (auto& [k, v] : or_clause[0].items()) merged[k] = v;
            return merged;
        }
        return json{{"$and", json::array({combined, json{{"$or", or_clause}}})}};
    }
    bool check_rls_constraint(const std::string& username, const std::string& db, const std::string& sch, const std::string& col,
                              const std::string& cmd, const json& doc, const std::set<std::string>& user_roles) {
        if (!is_rls_enabled(db, sch, col)) return true;
        std::lock_guard<std::mutex> lk(mu_);
        auto& pols = policies_[make_key(db, sch, col)];
        for (auto& p : pols) {
            if (!p.enabled) continue;
            if (p.command != "ALL" && p.command != cmd) continue;
            if (!p.roles.empty()) {
                bool match = false;
                for (auto& r : p.roles) if (user_roles.count(r)) { match = true; break; }
                if (!match) continue;
            }
            std::string check = p.with_check_expr.empty() ? p.using_expr : p.with_check_expr;
            if (check.empty()) return true;
            json f = parse_simple_expr(check, username);
            if (::delta::FilterMatcher::match(doc, f)) return true;
        }
        return false;
    }

private:
    Status create_schema_locked(const std::string& db, const std::string& name, const std::string& owner) {
        if (!databases_.count(db)) return Status::NotFound("database not found");
        if (schemas_[db].count(name)) return Status::Duplicate("schema exists");
        Schema s; s.id = next_schema_id_++; s.database = db; s.name = name; s.owner = owner;
        s.created_at = now_ms();
        schemas_[db][name] = s;
        store_->put("schema:" + db + ":" + name, s.to_json().dump());
        return Status::Ok();
    }
    void save_db(const Database& d) { store_->put("db:" + d.name, d.to_json().dump()); }
    void save_policies(const std::string& key) {
        json arr = json::array();
        for (auto& p : policies_[key]) arr.push_back(p.to_json());
        store_->put("rls_policies:" + key, arr.dump());
    }
    static std::string make_key(const std::string& db, const std::string& sch, const std::string& col) {
        return db + ":" + sch + ":" + col;
    }
    void load() {
        for (auto& [k, v] : store_->prefix_scan("db:", 10000)) {
            try { auto d = Database::from_json(json::parse(v)); databases_[d.name] = d; if (d.id >= next_db_id_) next_db_id_ = d.id + 1; } catch(...) {}
        }
        for (auto& [k, v] : store_->prefix_scan("schema:", 100000)) {
            try { auto s = Schema::from_json(json::parse(v)); schemas_[s.database][s.name] = s; if (s.id >= next_schema_id_) next_schema_id_ = s.id + 1; } catch(...) {}
        }
        for (auto& [k, v] : store_->prefix_scan("rls_policies:", 100000)) {
            std::string key = k.substr(13);
            try {
                auto arr = json::parse(v);
                for (auto& x : arr) { auto p = RLSPolicy::from_json(x); policies_[key].push_back(p); if (p.id >= next_policy_id_) next_policy_id_ = p.id + 1; }
            } catch(...) {}
        }
        for (auto& [k, v] : store_->prefix_scan("rls_enabled:", 100000)) {
            rls_enabled_[k.substr(12)] = (v == "1");
        }
    }
    void ensure_default() {
        std::lock_guard<std::mutex> lk(mu_);
        if (!databases_.count("default")) {
            Database d; d.id = next_db_id_++; d.name = "default"; d.owner = "admin";
            d.created_at = d.updated_at = now_ms(); d.max_connections = 1000;
            databases_["default"] = d;
            save_db(d);
            create_schema_locked("default", "public", "admin");
        }
        if (!databases_.count("delta_system")) {
            Database d; d.id = next_db_id_++; d.name = "delta_system"; d.owner = "admin";
            d.created_at = d.updated_at = now_ms(); d.max_connections = 100;
            databases_["delta_system"] = d;
            save_db(d);
            create_schema_locked("delta_system", "public", "admin");
        }
    }
    json parse_simple_expr(const std::string& expr, const std::string& username) {
        // Very simple: "field = current_user" | "field = 'value'" | "field = current_user_id()"
        // Returns equivalent JSON filter.
        std::string e = expr;
        // strip whitespace
        auto trim = [](std::string& s){ while(!s.empty() && isspace((unsigned char)s.front())) s.erase(0,1); while(!s.empty() && isspace((unsigned char)s.back())) s.pop_back(); };
        trim(e);
        size_t eq = e.find("=");
        if (eq == std::string::npos) return json::object();
        std::string lhs = e.substr(0, eq); trim(lhs);
        std::string rhs = e.substr(eq + 1); trim(rhs);
        json val;
        if (rhs == "current_user" || rhs == "current_user()" || rhs == "current_user_id()") val = username;
        else if (rhs.size() >= 2 && rhs.front() == '\'' && rhs.back() == '\'') val = rhs.substr(1, rhs.size() - 2);
        else if (rhs.size() >= 2 && rhs.front() == '"' && rhs.back() == '"') val = rhs.substr(1, rhs.size() - 2);
        else { try { val = std::stod(rhs); } catch(...) { val = rhs; } }
        return json{{lhs, val}};
    }

    storage::LSMTree* store_;
    std::mutex mu_;
    std::unordered_map<std::string, Database> databases_;
    std::unordered_map<std::string, std::unordered_map<std::string, Schema>> schemas_;
    std::unordered_map<std::string, std::vector<RLSPolicy>> policies_;
    std::unordered_map<std::string, bool> rls_enabled_;
    uint64_t next_db_id_ = 1, next_schema_id_ = 1, next_policy_id_ = 1;
};

} // namespace delta::database
