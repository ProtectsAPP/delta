<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Cluster</div>
        <div class="page-subtitle">Replication topology, LSN progress and failover</div>
      </div>
      <n-button @click="load">Refresh</n-button>
    </div>

    <n-grid :cols="3" :x-gap="16" :y-gap="16" responsive="screen">
      <n-gi>
        <n-card :bordered="true">
          <div class="stat-label">Role</div>
          <div class="stat-value">
            <n-tag :type="roleType" size="large" :bordered="false" strong>
              {{ info.role || '—' }}
            </n-tag>
          </div>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card :bordered="true">
          <div class="stat-label">Current LSN</div>
          <div class="stat-value mono">{{ info.current_lsn ?? '—' }}</div>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card :bordered="true">
          <div class="stat-label">{{ info.role === 'replica' ? 'Master URL' : 'Read-only' }}</div>
          <div class="stat-value" style="font-size: 16px;">
            <span class="mono" v-if="info.role === 'replica'">{{ info.master_url || '—' }}</span>
            <span v-else>{{ info.read_only ? 'yes' : 'no' }}</span>
          </div>
        </n-card>
      </n-gi>
    </n-grid>

    <n-card title="Connected replicas" style="margin-top: 16px;" :bordered="true">
      <n-data-table :columns="cols" :data="info.replicas || []" :pagination="false" />
    </n-card>

    <n-card title="Failover controls" style="margin-top: 16px;" :bordered="true">
      <n-alert v-if="info.role === 'standalone'" type="info" :show-icon="false" style="margin-bottom: 12px;">
        This node is running standalone. Start it with <code class="mono">--role master</code> or <code class="mono">--role replica</code> to enable replication.
      </n-alert>

      <n-space v-if="info.role === 'replica'">
        <n-popconfirm @positive-click="onPromote">
          <template #trigger>
            <n-button type="warning" :loading="busy">Promote to master</n-button>
          </template>
          Promote this replica to master? The replica will start accepting writes.
          Make sure the previous master is offline before doing this.
        </n-popconfirm>
      </n-space>

      <div v-if="info.role === 'master'">
        <n-form inline label-placement="top">
          <n-form-item label="New master URL">
            <n-input v-model:value="newMasterUrl" placeholder="http://host:port" style="width: 280px" />
          </n-form-item>
          <n-form-item label=" ">
            <n-popconfirm @positive-click="onDemote">
              <template #trigger>
                <n-button :loading="busy" :disabled="!newMasterUrl">Demote to replica</n-button>
              </template>
              Demote this master to a replica of {{ newMasterUrl }}? It will become read-only.
            </n-popconfirm>
          </n-form-item>
        </n-form>
      </div>
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue';
import { NCard, NGrid, NGi, NTag, NButton, NSpace, NPopconfirm, NDataTable, NForm, NFormItem, NInput, NAlert, useMessage } from 'naive-ui';
import delta from '@/api/delta';

const info = ref<any>({});
const newMasterUrl = ref('');
const busy = ref(false);
const message = useMessage();
let timer: any = null;

const roleType = computed(() => {
  if (info.value.role === 'master') return 'success';
  if (info.value.role === 'replica') return 'info';
  return 'default';
});

async function load() {
  try {
    const r = await delta.clusterInfo();
    info.value = r.data;
  } catch (e: any) {
    message.error(e.response?.data?.message || e.message);
  }
}
async function onPromote() {
  busy.value = true;
  try {
    await delta.promote();
    message.success('Promoted to master');
    await load();
  } catch (e: any) {
    message.error(e.response?.data?.message || e.message);
  } finally { busy.value = false; }
}
async function onDemote() {
  busy.value = true;
  try {
    await delta.demote(newMasterUrl.value);
    message.success('Demoted to replica of ' + newMasterUrl.value);
    await load();
  } catch (e: any) {
    message.error(e.response?.data?.message || e.message);
  } finally { busy.value = false; }
}

const cols = [
  { title: 'Replica ID', key: 'id', render: (r: any) => r.id || '—' },
  { title: 'Address', key: 'remote_addr' },
  { title: 'Last acked LSN', key: 'last_acked_lsn' },
  { title: 'Lag (LSN behind)', key: 'lag', render: (r: any) => Math.max(0, (info.value.current_lsn || 0) - (r.last_acked_lsn || 0)) },
  { title: 'Last seen', key: 'last_seen_ms', render: (r: any) => r.last_seen_ms ? new Date(r.last_seen_ms).toLocaleTimeString() : '—' }
];

onMounted(() => { load(); timer = setInterval(load, 3000); });
onUnmounted(() => clearInterval(timer));
</script>
