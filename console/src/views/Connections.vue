<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Connections</div>
        <div class="page-subtitle">Active client connections and session statistics</div>
      </div>
      <n-button @click="load">Refresh</n-button>
    </div>

    <n-space style="margin-bottom: 16px;">
      <n-tag :bordered="false">Total: {{ total }}</n-tag>
      <n-tag type="success" :bordered="false">Active: {{ active }}</n-tag>
      <n-tag type="info" :bordered="false">Idle: {{ idle }}</n-tag>
    </n-space>
    <n-data-table :columns="cols" :data="conns" />
  </div>
</template>

<script setup lang="ts">
import { h, ref, onMounted, onUnmounted } from 'vue';
import { NSpace, NButton, NTag, NDataTable, NPopconfirm, useMessage } from 'naive-ui';
import delta from '@/api/delta';

const conns = ref<any[]>([]);
const total = ref(0);
const active = ref(0);
const idle = ref(0);
const message = useMessage();
let timer: any = null;

async function load() {
  try {
    const r = await delta.listConnections();
    conns.value = r.data.connections || [];
    total.value = r.data.total;
    active.value = r.data.active;
    idle.value = r.data.idle;
  } catch (e: any) { message.error(e.message); }
}
async function close(id: number) {
  await delta.closeConnection(id); load(); message.success('Connection closed');
}

const cols = [
  { title: 'ID', key: 'id', width: 80 },
  { title: 'Client IP', key: 'client_ip' },
  { title: 'Username', key: 'username' },
  { title: 'Database', key: 'database' },
  { title: 'State', key: 'state', render: (r: any) => h(NTag, { size: 'small', bordered: false, type: r.state === 'active' ? 'success' : 'default' }, { default: () => r.state }) },
  { title: 'Queries', key: 'queries_executed' },
  { title: 'Connected', key: 'connected_at', render: (r: any) => new Date(r.connected_at).toLocaleString() },
  { title: 'Last active', key: 'last_active', render: (r: any) => new Date(r.last_active).toLocaleString() },
  { title: 'Actions', key: 'a', width: 120, render: (r: any) => h(NPopconfirm, { onPositiveClick: () => close(r.id) },
    { trigger: () => h(NButton, { size: 'small', type: 'error', ghost: true }, { default: () => 'Terminate' }), default: () => 'Terminate this connection?' }) }
];
onMounted(() => { load(); timer = setInterval(load, 5000); });
onUnmounted(() => clearInterval(timer));
</script>
