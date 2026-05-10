<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Databases</div>
        <div class="page-subtitle">Multi-tenant logical databases and schemas</div>
      </div>
      <n-space>
        <n-button @click="load">Refresh</n-button>
        <n-button type="primary" @click="showCreate=true">
          <template #icon><n-icon><AddOutline /></n-icon></template>
          New database
        </n-button>
      </n-space>
    </div>

    <n-data-table :columns="cols" :data="dbs" :row-key="(r:any)=>r.name" />

    <n-modal v-model:show="showCreate" preset="dialog" title="Create database" style="width:500px">
      <n-form :model="form" label-placement="top">
        <n-form-item label="Name"><n-input v-model:value="form.name" /></n-form-item>
        <n-form-item label="Owner"><n-input v-model:value="form.owner" /></n-form-item>
        <n-form-item label="Max connections"><n-input-number v-model:value="form.options.max_connections" /></n-form-item>
        <n-form-item label="Default TTL (sec)"><n-input-number v-model:value="form.options.default_ttl" /></n-form-item>
      </n-form>
      <template #action>
        <n-button @click="showCreate=false">Cancel</n-button>
        <n-button type="primary" @click="create">Create</n-button>
      </template>
    </n-modal>

    <n-modal v-model:show="showSchemas" preset="card" :title="'Schemas — ' + currentDb" style="width:720px">
      <n-space style="margin-bottom: 12px;">
        <n-input v-model:value="newSchema" placeholder="Schema name" style="width:240px" />
        <n-button type="primary" @click="createSchema">Create schema</n-button>
      </n-space>
      <n-data-table :columns="schemaCols" :data="schemas" :pagination="false" size="small" />
    </n-modal>
  </div>
</template>

<script setup lang="ts">
import { h, ref, onMounted } from 'vue';
import { NDataTable, NButton, NSpace, NModal, NForm, NFormItem, NInput, NInputNumber, NIcon, useMessage, NPopconfirm } from 'naive-ui';
import { AddOutline } from '@vicons/ionicons5';
import delta from '@/api/delta';

const dbs = ref<any[]>([]);
const showCreate = ref(false);
const form = ref<any>({ name: '', owner: 'admin', options: { max_connections: 100, default_ttl: 0 } });
const message = useMessage();
const showSchemas = ref(false);
const currentDb = ref('');
const schemas = ref<any[]>([]);
const newSchema = ref('');

async function load() { dbs.value = (await delta.listDatabases()).data.databases || []; }
async function create() {
  try { await delta.createDatabase(form.value); showCreate.value = false; message.success('Created'); load(); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function del(name: string) {
  try { await delta.deleteDatabase(name, true); message.success('Deleted'); load(); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function openSchemas(name: string) {
  currentDb.value = name; showSchemas.value = true;
  schemas.value = (await delta.listSchemas(name)).data.schemas || [];
}
async function createSchema() {
  try { await delta.createSchema(currentDb.value, { name: newSchema.value, owner: 'admin' }); newSchema.value = ''; openSchemas(currentDb.value); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function delSchema(name: string) {
  try { await delta.deleteSchema(currentDb.value, name, true); openSchemas(currentDb.value); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}

const cols = [
  { title: 'Name', key: 'name' },
  { title: 'Owner', key: 'owner' },
  { title: 'Collections', key: 'collection_count' },
  { title: 'Documents', key: 'document_count' },
  { title: 'Max conn.', key: 'max_connections' },
  { title: 'Created', key: 'created_at', render: (r: any) => r.created_at ? new Date(r.created_at).toLocaleString() : '-' },
  { title: 'Actions', key: 'a', width: 200, render: (r: any) => h(NSpace, null, { default: () => [
    h(NButton, { size: 'small', onClick: () => openSchemas(r.name) }, { default: () => 'Schemas' }),
    h(NPopconfirm, { onPositiveClick: () => del(r.name) },
      { trigger: () => h(NButton, { size: 'small', type: 'error', ghost: true }, { default: () => 'Delete' }),
        default: () => 'Delete this database?' })
  ]})}
];
const schemaCols = [
  { title: 'Name', key: 'name' },
  { title: 'Owner', key: 'owner' },
  { title: 'Created', key: 'created_at', render: (r: any) => new Date(r.created_at).toLocaleString() },
  { title: 'Actions', key: 'a', render: (r: any) => h(NPopconfirm, { onPositiveClick: () => delSchema(r.name) },
    { trigger: () => h(NButton, { size: 'small', type: 'error', ghost: true }, { default: () => 'Delete' }), default: () => 'Delete?' })}
];

onMounted(load);
</script>
