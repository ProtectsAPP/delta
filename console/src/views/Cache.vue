<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Cache</div>
        <div class="page-subtitle">In-memory key-value store with Redis-like data structures</div>
      </div>
    </div>

    <n-tabs type="line" animated>
      <n-tab-pane name="keys" tab="Keys">
        <n-card :bordered="true">
          <n-space style="margin-bottom: 12px;">
            <n-input v-model:value="pattern" placeholder="Pattern (e.g. user:*)" style="width:240px" @keydown.enter="loadKeys" />
            <n-button type="primary" @click="loadKeys">Scan</n-button>
          </n-space>
          <n-data-table :columns="keyCols" :data="keys" :pagination="{pageSize: 50}" size="small" />
        </n-card>
      </n-tab-pane>

      <n-tab-pane name="string" tab="String">
        <n-card title="String / Counter / TTL" :bordered="true">
          <n-form inline label-placement="top">
            <n-form-item label="Key"><n-input v-model:value="kv.k" /></n-form-item>
            <n-form-item label="Value"><n-input v-model:value="kv.v" /></n-form-item>
            <n-form-item label="TTL (sec)"><n-input-number v-model:value="kv.ttl" /></n-form-item>
            <n-form-item label=" ">
              <n-space>
                <n-button type="primary" @click="setKv">SET</n-button>
                <n-button @click="getKv">GET</n-button>
                <n-button @click="incrKv">INCR</n-button>
                <n-button @click="expireKv">EXPIRE</n-button>
              </n-space>
            </n-form-item>
          </n-form>
          <pre class="json-viewer">{{ kvResult || '— no result —' }}</pre>
        </n-card>
      </n-tab-pane>

      <n-tab-pane name="hash" tab="Hash">
        <n-card title="Hash" :bordered="true">
          <n-form inline label-placement="top">
            <n-form-item label="Key"><n-input v-model:value="h.k" /></n-form-item>
            <n-form-item label="Field"><n-input v-model:value="h.f" /></n-form-item>
            <n-form-item label="Value"><n-input v-model:value="h.v" /></n-form-item>
            <n-form-item label=" ">
              <n-space>
                <n-button type="primary" @click="setH">HSET</n-button>
                <n-button @click="getHF">HGET</n-button>
                <n-button @click="getH">HGETALL</n-button>
                <n-button @click="delHF">HDEL</n-button>
              </n-space>
            </n-form-item>
          </n-form>
          <pre class="json-viewer">{{ hResult || '— no result —' }}</pre>
        </n-card>
      </n-tab-pane>

      <n-tab-pane name="list" tab="List">
        <n-card title="List" :bordered="true">
          <n-form inline label-placement="top">
            <n-form-item label="Key"><n-input v-model:value="l.k" /></n-form-item>
            <n-form-item label="Value"><n-input v-model:value="l.v" /></n-form-item>
            <n-form-item label=" ">
              <n-space>
                <n-button @click="lpush('left')">LPUSH</n-button>
                <n-button @click="lpush('right')">RPUSH</n-button>
                <n-button @click="lr">LRANGE</n-button>
              </n-space>
            </n-form-item>
          </n-form>
          <pre class="json-viewer">{{ lResult || '— no result —' }}</pre>
        </n-card>
      </n-tab-pane>

      <n-tab-pane name="set" tab="Set">
        <n-card title="Set" :bordered="true">
          <n-form inline label-placement="top">
            <n-form-item label="Key"><n-input v-model:value="s.k" /></n-form-item>
            <n-form-item label="Member"><n-input v-model:value="s.m" /></n-form-item>
            <n-form-item label=" ">
              <n-space>
                <n-button type="primary" @click="sadd">SADD</n-button>
                <n-button @click="smem">SMEMBERS</n-button>
                <n-button @click="srem">SREM</n-button>
              </n-space>
            </n-form-item>
          </n-form>
          <pre class="json-viewer">{{ sResult || '— no result —' }}</pre>
        </n-card>
      </n-tab-pane>

      <n-tab-pane name="zset" tab="Sorted Set">
        <n-card title="Sorted Set" :bordered="true">
          <n-form inline label-placement="top">
            <n-form-item label="Key"><n-input v-model:value="z.k" /></n-form-item>
            <n-form-item label="Score"><n-input-number v-model:value="z.score" :step="0.1" /></n-form-item>
            <n-form-item label="Member"><n-input v-model:value="z.m" /></n-form-item>
            <n-form-item label=" ">
              <n-space>
                <n-button type="primary" @click="zadd">ZADD</n-button>
                <n-button @click="zr">ZRANGE</n-button>
              </n-space>
            </n-form-item>
          </n-form>
          <pre class="json-viewer">{{ zResult || '— no result —' }}</pre>
        </n-card>
      </n-tab-pane>

      <n-tab-pane name="pubsub" tab="Pub/Sub">
        <n-card title="Publish a message" :bordered="true">
          <n-form inline label-placement="top">
            <n-form-item label="Channel"><n-input v-model:value="pub.ch" /></n-form-item>
            <n-form-item label="Message"><n-input v-model:value="pub.msg" /></n-form-item>
            <n-form-item label=" "><n-button type="primary" @click="onPub">PUBLISH</n-button></n-form-item>
          </n-form>
        </n-card>
      </n-tab-pane>
    </n-tabs>
  </div>
</template>

<script setup lang="ts">
import { h, ref } from 'vue';
import { NCard, NSpace, NInput, NInputNumber, NButton, NForm, NFormItem, NDataTable, NPopconfirm, NTabs, NTabPane, useMessage } from 'naive-ui';
import delta from '@/api/delta';

const message = useMessage();
const pattern = ref('*');
const keys = ref<any[]>([]);
const kv = ref({ k: '', v: '', ttl: 0 });
const kvResult = ref('');
const h = ref({ k: '', f: '', v: '' });
const hResult = ref('');
const l = ref({ k: '', v: '' });
const lResult = ref('');
const s = ref({ k: '', m: '' });
const sResult = ref('');
const z = ref({ k: '', score: 0, m: '' });
const zResult = ref('');
const pub = ref({ ch: '', msg: '' });

async function loadKeys() {
  try {
    const r = await delta.cacheKeys(pattern.value);
    keys.value = (r.data.keys || []).map((k: string) => ({ k }));
  } catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function delKey(k: string) { await delta.cacheDel(k); loadKeys(); }
async function setKv() { try { await delta.cacheSet(kv.value.k, kv.value.v, kv.value.ttl); message.success('OK'); } catch (e: any) { message.error(e.message); } }
async function getKv() { try { const r = await delta.cacheGet(kv.value.k); kvResult.value = JSON.stringify(r.data, null, 2); } catch (e: any) { message.error(e.response?.data?.message || e.message); } }
async function incrKv() { try { const r = await delta.cacheIncr(kv.value.k); kvResult.value = JSON.stringify(r.data, null, 2); } catch (e: any) { message.error(e.message); } }
async function expireKv() { try { const r = await fetch(`/api/v1/cache/${kv.value.k}/expire`, { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + (localStorage.getItem('delta_token') || '') }, body: JSON.stringify({ seconds: kv.value.ttl || 60 }) }); kvResult.value = await r.text(); } catch (e: any) { message.error(e.message); } }
async function setH() { try { await delta.cacheHSet(h.value.k, h.value.f, h.value.v); message.success('OK'); } catch (e: any) { message.error(e.message); } }
async function getHF() { try { const r = await fetch(`/api/v1/cache/${h.value.k}/hash/${h.value.f}`, { headers: { Authorization: 'Bearer ' + (localStorage.getItem('delta_token') || '') } }); hResult.value = JSON.stringify(JSON.parse(await r.text()), null, 2); } catch (e: any) { message.error(e.message); } }
async function getH() { const r = await delta.cacheHGetAll(h.value.k); hResult.value = JSON.stringify(r.data, null, 2); }
async function delHF() { try { const r = await fetch(`/api/v1/cache/${h.value.k}/hash/${h.value.f}`, { method: 'DELETE', headers: { Authorization: 'Bearer ' + (localStorage.getItem('delta_token') || '') } }); hResult.value = await r.text(); } catch (e: any) { message.error(e.message); } }
async function lpush(dir: 'left'|'right') { try { await delta.cacheLPush(l.value.k, l.value.v, dir); message.success('OK'); } catch (e: any) { message.error(e.message); } }
async function lr() { const r = await delta.cacheLRange(l.value.k); lResult.value = JSON.stringify(r.data, null, 2); }
async function sadd() { try { await delta.cacheSAdd(s.value.k, s.value.m); message.success('OK'); } catch (e: any) { message.error(e.message); } }
async function smem() { const r = await delta.cacheSMembers(s.value.k); sResult.value = JSON.stringify(r.data, null, 2); }
async function srem() { try { const r = await fetch(`/api/v1/cache/${s.value.k}/set/${s.value.m}`, { method: 'DELETE', headers: { Authorization: 'Bearer ' + (localStorage.getItem('delta_token') || '') } }); sResult.value = await r.text(); } catch (e: any) { message.error(e.message); } }
async function zadd() { try { await delta.cacheZAdd(z.value.k, z.value.score, z.value.m); message.success('OK'); } catch (e: any) { message.error(e.message); } }
async function zr() { const r = await delta.cacheZRange(z.value.k); zResult.value = JSON.stringify(r.data, null, 2); }
async function onPub() { const r = await delta.publish(pub.value.ch, pub.value.msg); message.success(`Delivered to ${r.data.receivers} subscribers`); }

const keyCols = [
  { title: 'Key', key: 'k' },
  { title: 'Actions', key: 'a', width: 120, render: (r: any) => h(NPopconfirm, { onPositiveClick: () => delKey(r.k) },
    { trigger: () => h(NButton, { size: 'small', type: 'error', ghost: true }, { default: () => 'Delete' }), default: () => 'Delete this key?' }) }
];
</script>
