import axios, { AxiosInstance } from 'axios';

const API_BASE = '/api/v1';

class DeltaClient {
  private client: AxiosInstance;
  public token: string = '';

  constructor() {
    this.client = axios.create({ baseURL: API_BASE, headers: { 'Content-Type': 'application/json' } });
    this.client.interceptors.request.use((cfg) => {
      const t = localStorage.getItem('delta_token') || this.token;
      if (t) cfg.headers['Authorization'] = `Bearer ${t}`;
      return cfg;
    });
    this.client.interceptors.response.use(
      (r) => r,
      (e) => {
        if (e.response?.status === 401 || e.response?.data?.code === 401) {
          if (location.pathname !== '/login') {
            localStorage.removeItem('delta_token');
            localStorage.removeItem('delta_user');
            location.href = '/login';
          }
        }
        return Promise.reject(e);
      }
    );
  }

  setToken(t: string) { this.token = t; localStorage.setItem('delta_token', t); }
  clearToken() { this.token = ''; localStorage.removeItem('delta_token'); localStorage.removeItem('delta_user'); }
  async request(method: string, path: string, data?: any, params?: any) {
    const r = await this.client.request({ method, url: path, data, params });
    return r.data;
  }
  // Auth
  login(username: string, password: string, database?: string) {
    return this.request('POST', '/auth/login', { username, password, database });
  }
  logout() { return this.request('POST', '/auth/logout'); }
  me() { return this.request('GET', '/auth/me'); }
  use(database: string, schema: string) { return this.request('POST', '/use', { database, schema }); }

  // Users
  listUsers() { return this.request('GET', '/users'); }
  createUser(data: any) { return this.request('POST', '/users', data); }
  getUser(name: string) { return this.request('GET', `/users/${name}`); }
  updateUser(name: string, data: any) { return this.request('PATCH', `/users/${name}`, data); }
  deleteUser(name: string) { return this.request('DELETE', `/users/${name}`); }
  lockUser(name: string) { return this.request('POST', `/users/${name}/lock`); }
  unlockUser(name: string) { return this.request('POST', `/users/${name}/unlock`); }
  grantUserRole(name: string, role: string) { return this.request('POST', `/users/${name}/roles`, { role }); }
  revokeUserRole(name: string, role: string) { return this.request('DELETE', `/users/${name}/roles/${role}`); }

  // Roles
  listRoles() { return this.request('GET', '/roles'); }
  createRole(data: any) { return this.request('POST', '/roles', data); }
  deleteRole(name: string) { return this.request('DELETE', `/roles/${name}`); }

  // Permissions
  listPermissions(role?: string) { return this.request('GET', '/permissions', undefined, role ? { role } : {}); }
  grantPermission(data: any) { return this.request('POST', '/permissions/grant', data); }
  revokePermission(data: any) { return this.request('POST', '/permissions/revoke', data); }
  grantAll(role: string, target: any) { return this.request('POST', '/permissions/grant-all', { role, target }); }

  // Databases
  listDatabases() { return this.request('GET', '/databases'); }
  createDatabase(data: any) { return this.request('POST', '/databases', data); }
  getDatabase(name: string) { return this.request('GET', `/databases/${name}`); }
  deleteDatabase(name: string, force = false) { return this.request('DELETE', `/databases/${name}`, undefined, { force }); }
  updateDatabase(name: string, data: any) { return this.request('PATCH', `/databases/${name}`, data); }
  listSchemas(db: string) { return this.request('GET', `/databases/${db}/schemas`); }
  createSchema(db: string, data: any) { return this.request('POST', `/databases/${db}/schemas`, data); }
  deleteSchema(db: string, name: string, cascade = false) { return this.request('DELETE', `/databases/${db}/schemas/${name}`, undefined, { cascade }); }

  // Collections
  listCollections(database?: string) { return this.request('GET', '/collections', undefined, database ? { database } : {}); }
  createCollection(data: any) { return this.request('POST', '/collections', data); }
  getCollection(name: string, database?: string, schema?: string) { return this.request('GET', `/collections/${name}`, undefined, { database, schema }); }
  deleteCollection(name: string, database?: string, schema?: string) { return this.request('DELETE', `/collections/${name}`, undefined, { database, schema }); }
  createIndex(col: string, data: any, database?: string, schema?: string) { return this.request('POST', `/collections/${col}/indexes`, data, { database, schema }); }
  deleteIndex(col: string, name: string, database?: string, schema?: string) { return this.request('DELETE', `/collections/${col}/indexes/${name}`, undefined, { database, schema }); }

  // Documents
  insertDocument(col: string, document: any, database?: string, schema?: string, id?: string) {
    return this.request('POST', `/collections/${col}/documents`, { document, id }, { database, schema });
  }
  bulkInsert(col: string, documents: any[], database?: string, schema?: string) {
    return this.request('POST', `/collections/${col}/documents/bulk`, { documents }, { database, schema });
  }
  getDocument(col: string, id: string, database?: string, schema?: string) {
    return this.request('GET', `/collections/${col}/documents/${id}`, undefined, { database, schema });
  }
  searchDocuments(col: string, body: any, database?: string, schema?: string) {
    return this.request('POST', `/collections/${col}/documents/search`, body, { database, schema });
  }
  updateDocument(col: string, id: string, body: any, database?: string, schema?: string) {
    return this.request('PATCH', `/collections/${col}/documents/${id}`, body, { database, schema });
  }
  deleteDocument(col: string, id: string, database?: string, schema?: string) {
    return this.request('DELETE', `/collections/${col}/documents/${id}`, undefined, { database, schema });
  }
  aggregate(col: string, pipeline: any[], database?: string, schema?: string) {
    return this.request('POST', `/collections/${col}/aggregate`, { pipeline }, { database, schema });
  }
  countDocuments(col: string, filter: any, database?: string, schema?: string) {
    return this.request('POST', `/collections/${col}/count`, { filter }, { database, schema });
  }

  // Vectors
  insertVector(col: string, id: string, vector: number[], metadata: any = {}, metric = 'cosine', database?: string, schema?: string) {
    return this.request('POST', `/collections/${col}/vectors`, { id, vector, metadata, metric }, { database, schema });
  }
  searchVectors(col: string, body: any, database?: string, schema?: string) {
    return this.request('POST', `/collections/${col}/vectors/search`, body, { database, schema });
  }
  deleteVector(col: string, id: string, database?: string, schema?: string) {
    return this.request('DELETE', `/collections/${col}/vectors/${id}`, undefined, { database, schema });
  }

  // Cache
  cacheKeys(pattern = '*') { return this.request('GET', '/cache', undefined, { pattern }); }
  cacheSet(key: string, value: any, ttl = 0) { return this.request('POST', '/cache', { key, value, ttl }); }
  cacheGet(key: string) { return this.request('GET', `/cache/${key}`); }
  cacheDel(key: string) { return this.request('DELETE', `/cache/${key}`); }
  cacheIncr(key: string, delta = 1) { return this.request('POST', `/cache/${key}/incr`, { delta }); }
  cacheHSet(key: string, field: string, value: any) { return this.request('POST', `/cache/${key}/hash`, { field, value }); }
  cacheHGetAll(key: string) { return this.request('GET', `/cache/${key}/hash`); }
  cacheLPush(key: string, value: any, direction: 'left' | 'right' = 'right') { return this.request('POST', `/cache/${key}/list/push`, { value, direction }); }
  cacheLRange(key: string, start = 0, stop = -1) { return this.request('GET', `/cache/${key}/list`, undefined, { start, stop }); }
  cacheSAdd(key: string, member: string) { return this.request('POST', `/cache/${key}/set`, { member }); }
  cacheSMembers(key: string) { return this.request('GET', `/cache/${key}/set`); }
  cacheZAdd(key: string, score: number, member: string) { return this.request('POST', `/cache/${key}/zset`, { score, member }); }
  cacheZRange(key: string, start = 0, stop = -1) { return this.request('GET', `/cache/${key}/zset`, undefined, { start, stop }); }
  publish(channel: string, message: string) { return this.request('POST', '/pubsub/publish', { channel, message }); }

  // RLS
  listPolicies(db: string, sch: string, col: string) { return this.request('GET', `/databases/${db}/schemas/${sch}/collections/${col}/policies`); }
  createPolicy(db: string, sch: string, col: string, data: any) { return this.request('POST', `/databases/${db}/schemas/${sch}/collections/${col}/policies`, data); }
  deletePolicy(db: string, sch: string, col: string, name: string) { return this.request('DELETE', `/databases/${db}/schemas/${sch}/collections/${col}/policies/${name}`); }
  setRLSEnabled(db: string, sch: string, col: string, enabled: boolean) { return this.request('POST', `/databases/${db}/schemas/${sch}/collections/${col}/rls`, { enabled }); }

  // Connections / Stats
  listConnections() { return this.request('GET', '/connections'); }
  closeConnection(id: number) { return this.request('DELETE', `/connections/${id}`); }
  status() { return this.request('GET', '/status'); }
  stats() { return this.request('GET', '/stats'); }

  // Cluster / replication
  clusterInfo() { return this.request('GET', '/cluster/info'); }
  promote() { return this.request('POST', '/cluster/promote'); }
  demote(masterUrl: string) { return this.request('POST', '/cluster/demote', { master_url: masterUrl }); }

  // Sharding (Round 3)
  shardInfo() { return this.request('GET', '/cluster/shards'); }
  shardRoute(key: string) {
    return this.request('GET',
      `/cluster/shards/route?key=${encodeURIComponent(key)}`);
  }
}

export const delta = new DeltaClient();
export default delta;
