<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Shards</div>
        <div class="page-subtitle">Consistent-hash topology, virtual nodes, and route lookup</div>
      </div>
      <n-button @click="load" :loading="busy">Refresh</n-button>
    </div>

    <n-alert v-if="!info.enabled" type="info" :show-icon="false">
      This node was started without <code class="mono">--enable-sharding</code>.
      To run a sharded cluster, configure every node with
      <code class="mono">--shard-id</code> and one
      <code class="mono">--shard sid=node@host:port,...</code> per shard.
    </n-alert>

    <template v-else>
      <n-grid :cols="3" :x-gap="16" :y-gap="16" responsive="screen">
        <n-gi>
          <n-card :bordered="true">
            <div class="stat-label">Local shard</div>
            <div class="stat-value">
              <n-tag type="success" size="large" :bordered="false" strong>
                {{ info.local_shard }}
              </n-tag>
            </div>
          </n-card>
        </n-gi>
        <n-gi>
          <n-card :bordered="true">
            <div class="stat-label">Total shards</div>
            <div class="stat-value mono">{{ info.shard_count }}</div>
          </n-card>
        </n-gi>
        <n-gi>
          <n-card :bordered="true">
            <div class="stat-label">Virtual nodes / shard</div>
            <div class="stat-value mono">{{ info.vnodes }}</div>
          </n-card>
        </n-gi>
      </n-grid>

      <n-card title="Topology" style="margin-top: 16px;" :bordered="true">
        <n-data-table :columns="topoCols" :data="topoRows" :pagination="false" />
      </n-card>

      <n-card title="Route lookup" style="margin-top: 16px;" :bordered="true">
        <n-space>
          <n-input
            v-model:value="probeKey"
            placeholder="document _id or key to hash"
            style="width: 360px;"
            @keyup.enter="onProbe"
          />
          <n-button type="primary" :loading="probing" @click="onProbe">
            Route
          </n-button>
        </n-space>
        <n-alert
          v-if="probeResult"
          :type="probeResult.error ? 'error' : 'success'"
          :show-icon="false"
          style="margin-top: 12px;"
        >
          <template v-if="probeResult.error">{{ probeResult.error }}</template>
          <template v-else>
            <code class="mono">{{ probeResult.key }}</code>
            &rarr; shard
            <n-tag type="info" :bordered="false" strong>
              {{ probeResult.shard_id }}
            </n-tag>
            via {{ probeResult.peers.length }} peer(s):
            <span class="mono" v-for="p in probeResult.peers" :key="p.node_id">
              {{ p.node_id }}@{{ p.base_url }}&nbsp;
            </span>
          </template>
        </n-alert>
      </n-card>
    </template>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue';
import {
  NCard, NGrid, NGi, NTag, NDataTable, NButton, NSpace,
  NInput, NAlert, useMessage,
} from 'naive-ui';
import { delta } from '@/api/delta';

const msg = useMessage();
const busy = ref(false);
const probing = ref(false);
const info = ref<any>({ enabled: false });
const probeKey = ref('');
const probeResult = ref<any>(null);

const topoCols = [
  { title: 'Shard ID', key: 'id' },
  { title: 'Peer count', key: 'peer_count' },
  { title: 'Peers (node@host:port)', key: 'peers_str' },
];

const topoRows = computed(() => {
  if (!info.value.enabled) return [];
  return (info.value.topology || []).map((s: any) => ({
    id: s.id,
    peer_count: s.peers.length,
    peers_str: s.peers
      .map((p: any) => `${p.node_id}@${p.host}:${p.port}`)
      .join(', '),
  }));
});

async function load() {
  busy.value = true;
  try {
    const r = await delta.shardInfo();
    info.value = r;
  } catch (e: any) {
    msg.error(e?.message || 'failed to load shard topology');
  } finally {
    busy.value = false;
  }
}

async function onProbe() {
  if (!probeKey.value.trim()) {
    probeResult.value = { error: 'enter a key' };
    return;
  }
  probing.value = true;
  try {
    const r = await delta.shardRoute(probeKey.value.trim());
    probeResult.value = r;
  } catch (e: any) {
    probeResult.value = { error: e?.message || 'route lookup failed' };
  } finally {
    probing.value = false;
  }
}

onMounted(load);
</script>

<style scoped>
.stat-label { color: #888; font-size: 12px; margin-bottom: 4px; }
.stat-value { font-size: 24px; font-weight: 600; }
.mono { font-family: ui-monospace, 'SF Mono', Menlo, monospace; }
</style>
