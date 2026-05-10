<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Dashboard</div>
        <div class="page-subtitle">Cluster overview and live statistics</div>
      </div>
      <n-tag :type="status.status === 'running' ? 'success' : 'warning'" :bordered="false">
        {{ status.status || 'unknown' }}
      </n-tag>
    </div>

    <n-grid :cols="4" :x-gap="16" :y-gap="16" responsive="screen">
      <n-gi><n-card :bordered="true"><div class="stat-label">Databases</div><div class="stat-value">{{ stats.databases }}</div></n-card></n-gi>
      <n-gi><n-card :bordered="true"><div class="stat-label">Collections</div><div class="stat-value">{{ stats.collections }}</div></n-card></n-gi>
      <n-gi><n-card :bordered="true"><div class="stat-label">Documents</div><div class="stat-value">{{ stats.documents }}</div></n-card></n-gi>
      <n-gi><n-card :bordered="true"><div class="stat-label">Vectors</div><div class="stat-value">{{ stats.vectors }}</div></n-card></n-gi>
      <n-gi><n-card :bordered="true"><div class="stat-label">Cache keys</div><div class="stat-value">{{ stats.cache_keys }}</div></n-card></n-gi>
      <n-gi><n-card :bordered="true"><div class="stat-label">Cache hit rate</div><div class="stat-value">{{ (stats.hit_rate * 100).toFixed(1) }}%</div></n-card></n-gi>
      <n-gi><n-card :bordered="true"><div class="stat-label">Users / Roles</div><div class="stat-value">{{ stats.users }} / {{ stats.roles }}</div></n-card></n-gi>
      <n-gi><n-card :bordered="true"><div class="stat-label">Connections (active/total)</div><div class="stat-value">{{ stats.active }} / {{ stats.total_conn }}</div></n-card></n-gi>
    </n-grid>

    <n-card title="Server" style="margin-top: 16px;" :bordered="true">
      <n-descriptions :column="3" label-placement="left">
        <n-descriptions-item label="Status">{{ status.status }}</n-descriptions-item>
        <n-descriptions-item label="Version">{{ status.version }}</n-descriptions-item>
        <n-descriptions-item label="Uptime">{{ formatUptime(status.uptime) }}</n-descriptions-item>
      </n-descriptions>
    </n-card>

    <n-card title="Databases" style="margin-top: 16px;" :bordered="true">
      <n-data-table :columns="cols" :data="databases" :pagination="false" size="small" />
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue';
import { NCard, NGrid, NGi, NDescriptions, NDescriptionsItem, NDataTable, NTag } from 'naive-ui';
import delta from '@/api/delta';

const stats = ref<any>({ databases: 0, collections: 0, documents: 0, vectors: 0, cache_keys: 0, hit_rate: 0, users: 0, roles: 0, active: 0, total_conn: 0 });
const status = ref<any>({});
const databases = ref<any[]>([]);
let timer: any = null;

const cols = [
  { title: 'Name', key: 'name' },
  { title: 'Owner', key: 'owner' },
  { title: 'Collections', key: 'collection_count' },
  { title: 'Documents', key: 'document_count' },
  { title: 'Max connections', key: 'max_connections' }
];

function formatUptime(s: number) {
  if (!s) return '-';
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  return `${d}d ${h}h ${m}m`;
}

async function load() {
  try {
    const [s, st, dbs] = await Promise.all([delta.stats(), delta.status(), delta.listDatabases()]);
    const sd = s.data;
    stats.value = {
      databases: sd.storage?.databases || 0,
      collections: 0,
      documents: sd.storage?.total_keys || 0,
      vectors: sd.vector?.vectors || 0,
      cache_keys: sd.cache?.keys || 0,
      hit_rate: sd.cache?.hit_rate || 0,
      users: sd.users || 0,
      roles: sd.roles || 0,
      active: sd.connections?.active || 0,
      total_conn: sd.connections?.total || 0
    };
    status.value = st.data;
    databases.value = dbs.data.databases || [];
    stats.value.collections = databases.value.reduce((a, d) => a + (d.collection_count || 0), 0);
  } catch {}
}
onMounted(() => { load(); timer = setInterval(load, 5000); });
onUnmounted(() => clearInterval(timer));
</script>
