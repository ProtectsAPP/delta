<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Settings</div>
        <div class="page-subtitle">Server information and current session</div>
      </div>
    </div>

    <n-card title="Server" :bordered="true">
      <n-descriptions bordered :column="2">
        <n-descriptions-item label="Status">{{ status.status }}</n-descriptions-item>
        <n-descriptions-item label="Version">{{ status.version }}</n-descriptions-item>
        <n-descriptions-item label="Uptime (sec)">{{ status.uptime }}</n-descriptions-item>
        <n-descriptions-item label="Database">{{ auth.database }} / {{ auth.schema }}</n-descriptions-item>
        <n-descriptions-item label="Current user">{{ auth.user?.username }}</n-descriptions-item>
        <n-descriptions-item label="Superuser">{{ auth.isSuperuser ? 'yes' : 'no' }}</n-descriptions-item>
      </n-descriptions>
    </n-card>

    <n-card title="REST API endpoints" style="margin-top: 16px;" :bordered="true">
      <pre class="json-viewer">{{ apiList }}</pre>
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue';
import { NCard, NDescriptions, NDescriptionsItem } from 'naive-ui';
import { useAuthStore } from '@/stores/auth';
import delta from '@/api/delta';

const auth = useAuthStore();
const status = ref<any>({});
onMounted(async () => { try { status.value = (await delta.status()).data; } catch {} });

const apiList = `# Authentication
POST  /api/v1/auth/login                 Sign in
POST  /api/v1/auth/logout                Sign out
GET   /api/v1/auth/me                    Current session
POST  /api/v1/use                        Switch database / schema

# Databases & Schemas
GET   /api/v1/databases                  List databases
POST  /api/v1/databases                  Create database
GET   /api/v1/databases/{db}             Get database
PATCH /api/v1/databases/{db}             Alter database
DELETE /api/v1/databases/{db}            Drop database
GET   /api/v1/databases/{db}/schemas     List schemas
POST  /api/v1/databases/{db}/schemas     Create schema
DELETE /api/v1/databases/{db}/schemas/{name}

# Collections & Indexes
GET    /api/v1/collections                List collections
POST   /api/v1/collections                Create collection
GET    /api/v1/collections/{name}         Get collection
DELETE /api/v1/collections/{name}         Drop collection
POST   /api/v1/collections/{c}/indexes    Create index
DELETE /api/v1/collections/{c}/indexes/{i} Drop index

# Documents
POST   /api/v1/collections/{c}/documents             Insert
POST   /api/v1/collections/{c}/documents/bulk        Bulk insert
GET    /api/v1/collections/{c}/documents/{id}        Get
PATCH  /api/v1/collections/{c}/documents/{id}        Update
DELETE /api/v1/collections/{c}/documents/{id}        Delete
POST   /api/v1/collections/{c}/documents/search      Search
POST   /api/v1/collections/{c}/aggregate             Aggregate
POST   /api/v1/collections/{c}/count                 Count

# Vectors
POST   /api/v1/collections/{c}/vectors            Insert vector
POST   /api/v1/collections/{c}/vectors/search     Similarity search
DELETE /api/v1/collections/{c}/vectors/{id}       Delete vector

# Cache
POST   /api/v1/cache                  SET
GET    /api/v1/cache/{key}            GET
DELETE /api/v1/cache/{key}            DEL
POST   /api/v1/cache/{k}/incr         INCR
POST   /api/v1/cache/{k}/expire       EXPIRE
POST/GET/DELETE /api/v1/cache/{k}/hash[/{f}]
POST/GET /api/v1/cache/{k}/list[/push]
POST/GET/DELETE /api/v1/cache/{k}/set[/{m}]
POST/GET /api/v1/cache/{k}/zset
POST   /api/v1/pubsub/publish

# Users / Roles / Permissions
GET/POST/PATCH/DELETE /api/v1/users[/{name}]
POST   /api/v1/users/{name}/lock|unlock
GET/POST/DELETE /api/v1/roles[/{name}]
POST   /api/v1/permissions/grant   /revoke   /grant-all
GET    /api/v1/permissions

# Row-level security
POST/GET/DELETE /api/v1/databases/{db}/schemas/{s}/collections/{c}/policies[/{p}]
POST   /api/v1/databases/{db}/schemas/{s}/collections/{c}/rls

# System
GET    /api/v1/connections                List connections
DELETE /api/v1/connections/{id}           Terminate
GET    /api/v1/status                     Server status
GET    /api/v1/stats                      Statistics
GET    /api/v1/health                     Health probe`;
</script>
