// =============================================================================
// auth_manager.hpp — users, roles, permissions, sessions.
//
// Audit fixes wired in here:
//
//   P0-8 (user enumeration): `authenticate()` no longer leaks the
//          existence of a username through either the error string OR the
//          response time. We always run a PBKDF2 verification — against a
//          pre-computed dummy hash if the user doesn't exist — and we
//          collapse "user not found", "user locked", "user expired" and
//          "bad password" into a single uniform message.
//
//   P0-7 (timing): comparison happens inside `verify_password()` using
//          `constant_time_compare()` (see password.hpp).
//
//   P1-17 (login rate limit): `LoginRateLimiter` caps attempts per
//          (IP, username) tuple inside a sliding window. The HTTP layer
//          calls `allow()` before forwarding to `authenticate()`.
//
//   P0-6 (legacy hash upgrade): if a user's stored hash is the old
//          bare-hex format and they successfully log in, we transparently
//          re-hash their password with PBKDF2-HMAC-SHA256 and persist the
//          PHC-encoded result. After ~one login per user the legacy format
//          is gone.
// =============================================================================
#pragma once
#include "../core/common.hpp"
#include "../core/constants.hpp"
#include "../storage/lsm_tree.hpp"
#include "password.hpp"
#include <deque>
#include <set>
#include <bitset>

namespace delta::auth {

// In-process sliding-window login rate limiter (P1-17).
// Keys are arbitrary strings — the HTTP layer typically uses
// "<ip>|<username>" so a single attacker bot can't rotate usernames to
// evade the cap. Window and max are read from constants.hpp.
class LoginRateLimiter {
public:
    bool allow(const std::string& key) {
        using namespace constants;
        std::lock_guard<std::mutex> lk(mu_);
        uint64_t now    = now_sec();
        uint64_t cutoff = now > (uint64_t)AUTH_RATE_LIMIT_WINDOW_S
                              ? now - (uint64_t)AUTH_RATE_LIMIT_WINDOW_S : 0;
        auto& q = buckets_[key];
        while (!q.empty() && q.front() < cutoff) q.pop_front();
        if ((int)q.size() >= AUTH_RATE_LIMIT_MAX) return false;
        q.push_back(now);
        return true;
    }
    void reset(const std::string& key) {
        std::lock_guard<std::mutex> lk(mu_); buckets_.erase(key);
    }
private:
    std::mutex mu_;
    std::unordered_map<std::string, std::deque<uint64_t>> buckets_;
};

enum class UserStatus { ACTIVE, LOCKED, EXPIRED, PENDING };
inline std::string status_str(UserStatus s) {
    switch (s) { case UserStatus::ACTIVE: return "active"; case UserStatus::LOCKED: return "locked";
                 case UserStatus::EXPIRED: return "expired"; default: return "pending"; }
}
inline UserStatus parse_status(const std::string& s) {
    if (s == "locked") return UserStatus::LOCKED;
    if (s == "expired") return UserStatus::EXPIRED;
    if (s == "pending") return UserStatus::PENDING;
    return UserStatus::ACTIVE;
}

struct User {
    uint64_t id = 0;
    std::string username;
    std::string password_hash;
    std::string salt;
    std::vector<std::string> roles;
    UserStatus status = UserStatus::ACTIVE;
    uint64_t created_at = 0;
    uint64_t updated_at = 0;
    uint64_t valid_until = 0;
    uint64_t last_login = 0;
    std::string last_login_ip;
    int failed_attempts = 0;
    std::string default_database;
    std::string default_schema = "public";
    int connection_limit = -1;
    bool superuser = false;
    bool can_create_db = false;
    bool can_create_role = false;
    json options = json::object();

    json to_json(bool include_secret = false) const {
        json j = {{"id",id},{"username",username},{"roles",roles},{"status",status_str(status)},
                  {"created_at",created_at},{"updated_at",updated_at},{"valid_until",valid_until},
                  {"last_login",last_login},{"last_login_ip",last_login_ip},
                  {"failed_attempts",failed_attempts},{"default_database",default_database},
                  {"default_schema",default_schema},{"connection_limit",connection_limit},
                  {"superuser",superuser},{"can_create_db",can_create_db},{"can_create_role",can_create_role},
                  {"options",options}};
        if (include_secret) { j["password_hash"] = password_hash; j["salt"] = salt; }
        return j;
    }
    static User from_json(const json& j) {
        User u;
        u.id = j.value("id", 0ull); u.username = j.value("username","");
        u.password_hash = j.value("password_hash",""); u.salt = j.value("salt","");
        u.roles = j.value("roles", std::vector<std::string>{});
        u.status = parse_status(j.value("status","active"));
        u.created_at = j.value("created_at",0ull); u.updated_at = j.value("updated_at",0ull);
        u.valid_until = j.value("valid_until",0ull); u.last_login = j.value("last_login",0ull);
        u.last_login_ip = j.value("last_login_ip","");
        u.failed_attempts = j.value("failed_attempts",0);
        u.default_database = j.value("default_database",""); u.default_schema = j.value("default_schema","public");
        u.connection_limit = j.value("connection_limit",-1);
        u.superuser = j.value("superuser", false);
        u.can_create_db = j.value("can_create_db", false);
        u.can_create_role = j.value("can_create_role", false);
        u.options = j.value("options", json::object());
        return u;
    }
};

struct Role {
    uint64_t id = 0;
    std::string name;
    std::string description;
    std::vector<std::string> parents;
    bool is_system = false;
    uint64_t created_at = 0;
    json to_json() const { return {{"id",id},{"name",name},{"description",description},
        {"parents",parents},{"is_system",is_system},{"created_at",created_at}}; }
    static Role from_json(const json& j) { Role r; r.id=j.value("id",0ull); r.name=j.value("name","");
        r.description=j.value("description",""); r.parents=j.value("parents",std::vector<std::string>{});
        r.is_system=j.value("is_system",false); r.created_at=j.value("created_at",0ull); return r; }
};

enum Privilege : uint32_t {
    PRIV_SELECT     = 1u << 0,
    PRIV_INSERT     = 1u << 1,
    PRIV_UPDATE     = 1u << 2,
    PRIV_DELETE     = 1u << 3,
    PRIV_CREATE     = 1u << 4,
    PRIV_DROP       = 1u << 5,
    PRIV_ALTER      = 1u << 6,
    PRIV_INDEX      = 1u << 7,
    PRIV_TRUNCATE   = 1u << 8,
    PRIV_REFERENCES = 1u << 9,
    PRIV_TRIGGER    = 1u << 10,
    PRIV_EXECUTE    = 1u << 11,
    PRIV_USAGE      = 1u << 12,
    PRIV_CREATE_TEMP= 1u << 13,
    PRIV_CONNECT    = 1u << 14,
    PRIV_ALL        = 0xFFFFFFFFu
};

inline uint32_t parse_privileges(const std::vector<std::string>& names) {
    uint32_t bits = 0;
    for (auto& n : names) {
        std::string u; for (auto c : n) u += std::toupper(c);
        if (u == "SELECT") bits |= PRIV_SELECT;
        else if (u == "INSERT") bits |= PRIV_INSERT;
        else if (u == "UPDATE") bits |= PRIV_UPDATE;
        else if (u == "DELETE") bits |= PRIV_DELETE;
        else if (u == "CREATE") bits |= PRIV_CREATE;
        else if (u == "DROP") bits |= PRIV_DROP;
        else if (u == "ALTER") bits |= PRIV_ALTER;
        else if (u == "INDEX") bits |= PRIV_INDEX;
        else if (u == "TRUNCATE") bits |= PRIV_TRUNCATE;
        else if (u == "REFERENCES") bits |= PRIV_REFERENCES;
        else if (u == "TRIGGER") bits |= PRIV_TRIGGER;
        else if (u == "EXECUTE") bits |= PRIV_EXECUTE;
        else if (u == "USAGE") bits |= PRIV_USAGE;
        else if (u == "CONNECT") bits |= PRIV_CONNECT;
        else if (u == "ALL" || u == "ALL PRIVILEGES") bits |= PRIV_ALL;
    }
    return bits;
}
inline std::vector<std::string> privileges_to_names(uint32_t bits) {
    std::vector<std::string> out;
    if (bits == PRIV_ALL) return {"ALL"};
    if (bits & PRIV_SELECT) out.push_back("SELECT");
    if (bits & PRIV_INSERT) out.push_back("INSERT");
    if (bits & PRIV_UPDATE) out.push_back("UPDATE");
    if (bits & PRIV_DELETE) out.push_back("DELETE");
    if (bits & PRIV_CREATE) out.push_back("CREATE");
    if (bits & PRIV_DROP) out.push_back("DROP");
    if (bits & PRIV_ALTER) out.push_back("ALTER");
    if (bits & PRIV_INDEX) out.push_back("INDEX");
    if (bits & PRIV_TRUNCATE) out.push_back("TRUNCATE");
    if (bits & PRIV_USAGE) out.push_back("USAGE");
    if (bits & PRIV_CONNECT) out.push_back("CONNECT");
    if (bits & PRIV_EXECUTE) out.push_back("EXECUTE");
    return out;
}

struct PrivilegeTarget {
    std::string type; // database | schema | collection | role | function
    std::string database;
    std::string schema;
    std::string name;
    json to_json() const { return {{"type",type},{"database",database},{"schema",schema},{"name",name}}; }
    static PrivilegeTarget from_json(const json& j) {
        PrivilegeTarget t; t.type=j.value("type","collection"); t.database=j.value("database","");
        t.schema=j.value("schema",""); t.name=j.value("name",""); return t;
    }
    std::string key() const { return type + ":" + database + ":" + schema + ":" + name; }
    bool matches(const PrivilegeTarget& other) const {
        if (type != other.type) return false;
        if (!database.empty() && !other.database.empty() && database != other.database) return false;
        if (!schema.empty() && !other.schema.empty() && schema != other.schema) return false;
        if (!name.empty() && !other.name.empty() && name != other.name) return false;
        return true;
    }
};

struct Permission {
    uint64_t id = 0;
    std::string role;
    PrivilegeTarget target;
    uint32_t privileges = 0;
    bool with_grant_option = false;
    std::string granted_by;
    uint64_t granted_at = 0;
    json to_json() const { return {{"id",id},{"role",role},{"target",target.to_json()},
        {"privileges",privileges_to_names(privileges)},
        {"privilege_bits",privileges},
        {"with_grant_option",with_grant_option},
        {"granted_by",granted_by},{"granted_at",granted_at}}; }
    static Permission from_json(const json& j) {
        Permission p; p.id=j.value("id",0ull); p.role=j.value("role","");
        p.target=PrivilegeTarget::from_json(j["target"]);
        p.privileges=j.value("privilege_bits",0u);
        p.with_grant_option=j.value("with_grant_option",false);
        p.granted_by=j.value("granted_by",""); p.granted_at=j.value("granted_at",0ull);
        return p;
    }
};

class AuthManager {
public:
    explicit AuthManager(storage::LSMTree* store) : store_(store) {
        load_all();
        ensure_system();
    }

    // ---- Users ----
    Status create_user(const std::string& name, const std::string& password, const json& opts = json::object()) {
        std::lock_guard<std::mutex> lk(mu_);
        if (users_.count(name)) return Status::Duplicate("user exists");
        User u;
        u.id = next_user_id_++;
        u.username = name;
        u.salt = random_hex(16);
        // hash_password() emits the new PHC-prefixed PBKDF2 string. The
        // legacy `salt` field is still persisted so old code paths that
        // happen to compare against it don't crash, but verify_password()
        // now reads the salt out of the PHC string itself.
        u.password_hash = hash_password(password, u.salt);
        u.created_at = u.updated_at = now_ms();
        if (opts.contains("roles") && opts["roles"].is_array()) u.roles = opts["roles"].get<std::vector<std::string>>();
        u.default_database = opts.value("default_database", "");
        u.default_schema = opts.value("default_schema", "public");
        u.connection_limit = opts.value("connection_limit", -1);
        u.valid_until = opts.value("valid_until", 0ull);
        u.superuser = opts.value("superuser", false);
        u.can_create_db = opts.value("can_create_db", false);
        u.can_create_role = opts.value("can_create_role", false);
        users_[name] = u;
        save_user(u);
        return Status::Ok();
    }
    Status alter_user(const std::string& name, const json& opts) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = users_.find(name); if (it == users_.end()) return Status::NotFound("user not found");
        User& u = it->second;
        if (opts.contains("password")) {
            u.salt = random_hex(16);
            std::string new_pw = opts["password"];
            u.password_hash = hash_password(new_pw, u.salt);
        }
        if (opts.contains("connection_limit")) u.connection_limit = opts["connection_limit"];
        if (opts.contains("default_database")) u.default_database = opts["default_database"];
        if (opts.contains("default_schema")) u.default_schema = opts["default_schema"];
        if (opts.contains("valid_until")) u.valid_until = opts["valid_until"];
        if (opts.contains("superuser")) u.superuser = opts["superuser"];
        if (opts.contains("can_create_db")) u.can_create_db = opts["can_create_db"];
        if (opts.contains("can_create_role")) u.can_create_role = opts["can_create_role"];
        if (opts.contains("status")) u.status = parse_status(opts["status"]);
        u.updated_at = now_ms();
        save_user(u);
        return Status::Ok();
    }
    Status drop_user(const std::string& name) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = users_.find(name); if (it == users_.end()) return Status::NotFound("user not found");
        store_->del("auth:user:" + name);
        users_.erase(it);
        return Status::Ok();
    }
    Status rename_user(const std::string& old_name, const std::string& new_name) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = users_.find(old_name); if (it == users_.end()) return Status::NotFound("user not found");
        if (users_.count(new_name)) return Status::Duplicate("name exists");
        User u = it->second; u.username = new_name; u.updated_at = now_ms();
        users_.erase(it); users_[new_name] = u;
        store_->del("auth:user:" + old_name);
        save_user(u);
        return Status::Ok();
    }
    Status lock_user(const std::string& name) { return alter_user(name, {{"status","locked"}}); }
    Status unlock_user(const std::string& name) {
        Status s = alter_user(name, {{"status","active"}});
        if (s.ok()) { std::lock_guard<std::mutex> lk(mu_); users_[name].failed_attempts = 0; save_user(users_[name]); }
        return s;
    }
    std::optional<User> get_user(const std::string& name) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = users_.find(name); if (it == users_.end()) return std::nullopt;
        return it->second;
    }
    std::vector<User> list_users() {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<User> out; for (auto& [_, u] : users_) out.push_back(u);
        return out;
    }
    // P0-8: every failure path returns the SAME error and burns the SAME
    // amount of time as a real failed verification, so an attacker probing
    // /api/v1/auth/login can't tell "user not found" from "wrong password".
    bool authenticate(const std::string& username, const std::string& password, std::string* err = nullptr) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = users_.find(username);
        const User* uptr = (it == users_.end()) ? nullptr : &it->second;

        // Always verify against *something* so the wall-clock cost of a
        // login attempt does not depend on whether the username exists.
        bool ok = false;
        if (uptr) {
            ok = verify_password(password, uptr->salt, uptr->password_hash);
        } else {
            // Lazily compute a dummy hash once (still PBKDF2, same cost).
            if (dummy_hash_.empty()) {
                std::string dsalt = random_hex(16);
                dummy_hash_ = hash_password("this-account-does-not-exist", dsalt);
            }
            (void)verify_password(password, std::string{}, dummy_hash_);
            ok = false;
        }

        if (!uptr) { if (err) *err = "invalid credentials"; return false; }

        User& u = *const_cast<User*>(uptr);
        if (u.status == UserStatus::LOCKED) { if (err) *err = "invalid credentials"; return false; }
        if (u.valid_until > 0 && now_ms() > u.valid_until) { if (err) *err = "invalid credentials"; return false; }

        if (!ok) {
            u.failed_attempts++;
            if (u.failed_attempts >= constants::AUTH_MAX_LOGIN_ATTEMPTS) u.status = UserStatus::LOCKED;
            save_user(u);
            if (err) *err = "invalid credentials";
            return false;
        }

        // Successful login. Transparently upgrade legacy hashes (P0-6).
        if (!is_phc_hash(u.password_hash)) {
            u.salt = random_hex(16);
            u.password_hash = hash_password(password, u.salt);
        }
        u.failed_attempts = 0;
        u.last_login = now_ms();
        save_user(u);
        return true;
    }
    void record_login_ip(const std::string& username, const std::string& ip) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = users_.find(username); if (it == users_.end()) return;
        it->second.last_login_ip = ip; save_user(it->second);
    }

    // ---- Roles ----
    Status create_role(const std::string& name, const std::string& desc = "", const std::vector<std::string>& parents = {}) {
        std::lock_guard<std::mutex> lk(mu_);
        if (roles_.count(name)) return Status::Duplicate("role exists");
        Role r; r.id = next_role_id_++; r.name = name; r.description = desc; r.parents = parents;
        r.created_at = now_ms();
        roles_[name] = r;
        save_role(r);
        return Status::Ok();
    }
    Status drop_role(const std::string& name) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = roles_.find(name); if (it == roles_.end()) return Status::NotFound("role not found");
        if (it->second.is_system) return Status::Forbidden("cannot drop system role");
        store_->del("auth:role:" + name);
        roles_.erase(it);
        // remove from users and permissions
        for (auto& [_, u] : users_) {
            auto& v = u.roles; v.erase(std::remove(v.begin(), v.end(), name), v.end()); save_user(u);
        }
        permissions_.erase(std::remove_if(permissions_.begin(), permissions_.end(),
            [&](const Permission& p){ return p.role == name; }), permissions_.end());
        save_permissions();
        return Status::Ok();
    }
    Status grant_role_to_user(const std::string& username, const std::string& role) {
        std::lock_guard<std::mutex> lk(mu_);
        auto uit = users_.find(username); if (uit == users_.end()) return Status::NotFound("user not found");
        if (!roles_.count(role)) return Status::NotFound("role not found");
        for (auto& r : uit->second.roles) if (r == role) return Status::Ok();
        uit->second.roles.push_back(role);
        save_user(uit->second);
        return Status::Ok();
    }
    Status revoke_role_from_user(const std::string& username, const std::string& role) {
        std::lock_guard<std::mutex> lk(mu_);
        auto uit = users_.find(username); if (uit == users_.end()) return Status::NotFound("user not found");
        auto& v = uit->second.roles;
        v.erase(std::remove(v.begin(), v.end(), role), v.end());
        save_user(uit->second);
        return Status::Ok();
    }
    std::vector<Role> list_roles() {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<Role> out; for (auto& [_, r] : roles_) out.push_back(r); return out;
    }
    std::optional<Role> get_role(const std::string& name) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = roles_.find(name); if (it == roles_.end()) return std::nullopt;
        return it->second;
    }

    // ---- Permissions (GRANT/REVOKE) ----
    Status grant(const std::string& role, uint32_t privs, const PrivilegeTarget& target, bool with_grant = false, const std::string& by = "") {
        std::lock_guard<std::mutex> lk(mu_);
        if (!roles_.count(role)) return Status::NotFound("role not found");
        for (auto& p : permissions_) {
            if (p.role == role && p.target.key() == target.key()) {
                p.privileges |= privs;
                p.with_grant_option = p.with_grant_option || with_grant;
                save_permissions(); return Status::Ok();
            }
        }
        Permission p; p.id = next_perm_id_++; p.role = role; p.target = target;
        p.privileges = privs; p.with_grant_option = with_grant; p.granted_by = by; p.granted_at = now_ms();
        permissions_.push_back(p);
        save_permissions();
        return Status::Ok();
    }
    Status revoke(const std::string& role, uint32_t privs, const PrivilegeTarget& target) {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto it = permissions_.begin(); it != permissions_.end();) {
            if (it->role == role && it->target.key() == target.key()) {
                it->privileges &= ~privs;
                if (it->privileges == 0) { it = permissions_.erase(it); continue; }
            }
            ++it;
        }
        save_permissions();
        return Status::Ok();
    }
    std::vector<Permission> list_permissions(const std::string& role = "") {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<Permission> out;
        for (auto& p : permissions_) if (role.empty() || p.role == role) out.push_back(p);
        return out;
    }

    // get all roles (recursive parents) of a user
    std::set<std::string> get_user_all_roles(const std::string& username) {
        std::set<std::string> out;
        auto it = users_.find(username); if (it == users_.end()) return out;
        std::function<void(const std::string&)> dfs = [&](const std::string& r) {
            if (out.count(r)) return; out.insert(r);
            auto rit = roles_.find(r); if (rit == roles_.end()) return;
            for (auto& p : rit->second.parents) dfs(p);
        };
        for (auto& r : it->second.roles) dfs(r);
        dfs("public");
        return out;
    }

    // check user permission
    bool check(const std::string& username, uint32_t privs, const PrivilegeTarget& target) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = users_.find(username); if (it == users_.end()) return false;
        if (it->second.superuser) return true;
        auto roles = get_user_all_roles(username);
        for (auto& p : permissions_) {
            if (!roles.count(p.role)) continue;
            if (!p.target.matches(target)) continue;
            if ((p.privileges & privs) == privs) return true;
        }
        return false;
    }

    bool is_superuser(const std::string& username) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = users_.find(username); if (it == users_.end()) return false;
        if (it->second.superuser) return true;
        auto roles = get_user_all_roles(username);
        return roles.count("superuser") > 0;
    }

private:
    void save_user(const User& u) { store_->put("auth:user:" + u.username, u.to_json(true).dump()); }
    void save_role(const Role& r) { store_->put("auth:role:" + r.name, r.to_json().dump()); }
    void save_permissions() {
        json arr = json::array();
        for (auto& p : permissions_) arr.push_back(p.to_json());
        store_->put("auth:permissions", arr.dump());
    }
    void load_all() {
        for (auto& [k, v] : store_->prefix_scan("auth:user:", 100000)) {
            try { auto u = User::from_json(json::parse(v)); users_[u.username] = u; if (u.id >= next_user_id_) next_user_id_ = u.id + 1; } catch(...) {}
        }
        for (auto& [k, v] : store_->prefix_scan("auth:role:", 100000)) {
            try { auto r = Role::from_json(json::parse(v)); roles_[r.name] = r; if (r.id >= next_role_id_) next_role_id_ = r.id + 1; } catch(...) {}
        }
        std::string p;
        if (store_->get("auth:permissions", &p)) {
            try {
                auto arr = json::parse(p);
                for (auto& x : arr) { auto pp = Permission::from_json(x); permissions_.push_back(pp); if (pp.id >= next_perm_id_) next_perm_id_ = pp.id + 1; }
            } catch(...) {}
        }
    }
    void ensure_system() {
        std::lock_guard<std::mutex> lk(mu_);
        auto ensure_role = [&](const std::string& n, const std::string& d) {
            if (!roles_.count(n)) { Role r; r.id=next_role_id_++; r.name=n; r.description=d; r.is_system=true; r.created_at=now_ms(); roles_[n]=r; save_role(r); }
        };
        ensure_role("superuser", "超级管理员");
        ensure_role("db_admin", "数据库管理员");
        ensure_role("user_admin", "用户管理员");
        ensure_role("read_write", "读写用户");
        ensure_role("read_only", "只读用户");
        ensure_role("public", "公共角色");
        // default admin user
        if (!users_.count("admin")) {
            User u; u.id = next_user_id_++; u.username = "admin"; u.salt = random_hex(16);
            u.password_hash = hash_password("admin", u.salt);
            u.created_at = u.updated_at = now_ms();
            u.roles = {"superuser"};
            u.superuser = true; u.can_create_db = true; u.can_create_role = true;
            u.default_database = "default"; u.default_schema = "public";
            users_["admin"] = u;
            save_user(u);
        }
    }
    storage::LSMTree* store_;
    std::mutex mu_;
    std::unordered_map<std::string, User> users_;
    std::unordered_map<std::string, Role> roles_;
    std::vector<Permission> permissions_;
    uint64_t next_user_id_ = 1, next_role_id_ = 1, next_perm_id_ = 1;
    // Memoized dummy hash for the user-enumeration mitigation (P0-8).
    std::string dummy_hash_;
};

// Session management
struct Session {
    std::string token;
    std::string username;
    std::string database;
    std::string schema;
    uint64_t created_at;
    uint64_t expires_at;
    std::string client_ip;
};

class SessionManager {
public:
    Session create(const std::string& username, const std::string& db, const std::string& sch, const std::string& ip, uint32_t ttl_seconds = 86400) {
        std::lock_guard<std::mutex> lk(mu_);
        Session s;
        s.token = random_hex(32);
        s.username = username; s.database = db; s.schema = sch; s.client_ip = ip;
        s.created_at = now_ms(); s.expires_at = now_ms() + (uint64_t)ttl_seconds * 1000;
        sessions_[s.token] = s;
        return s;
    }
    std::optional<Session> get(const std::string& token) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = sessions_.find(token); if (it == sessions_.end()) return std::nullopt;
        if (now_ms() > it->second.expires_at) { sessions_.erase(it); return std::nullopt; }
        return it->second;
    }
    void revoke(const std::string& token) {
        std::lock_guard<std::mutex> lk(mu_); sessions_.erase(token);
    }
    void update(const Session& s) {
        std::lock_guard<std::mutex> lk(mu_); sessions_[s.token] = s;
    }
    std::vector<Session> list() {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<Session> out; for (auto& [_, s] : sessions_) out.push_back(s);
        return out;
    }
private:
    std::mutex mu_;
    std::unordered_map<std::string, Session> sessions_;
};

} // namespace delta::auth
