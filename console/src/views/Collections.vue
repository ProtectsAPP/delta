<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Collections</div>
        <div class="page-subtitle">Document collections in {{ auth.database }} / {{ auth.schema }}</div>
      </div>
      <n-space>
        <n-button @click="load">Refresh</n-button>
        <n-button type="primary" @click="showCreate=true">
          <template #icon><n-icon><AddOutline /></n-icon></template>
          New collection
        </n-button>
      </n-space>
    </div>

    <n-data-table :columns="cols" :data="cols_data" />

    <n-modal v-model:show="showCreate" preset="dialog" title="Create collection" style="width:500px">
      <n-form :model="form" label-placement="top">
        <n-form-item label="Name"><n-input v-model:value="form.name" /></n-form-item>
        <n-form-item label="Database"><n-input v-model:value="form.database" /></n-form-item>
        <n-form-item label="Schema"><n-input v-model:value="form.schema" /></n-form-item>
      </n-form>
      <template #action>
        <n-button @click="showCreate=false">Cancel</n-button>
        <n-button type="primary" @click="create">Create</n-button>
      </template>
    </n-modal>

    <n-modal v-model:show="showIdx" preset="card" :title="'Indexes — ' + currentCol" style="width:780px">
      <n-form inline label-placement="top">
        <n-form-item label="Fields (comma separated)"><n-input v-model:value="idxForm.fields" placeholder="email, age" style="width:240px" /></n-form-item>
        <n-form-item label="Type">
          <n-select v-model:value="idxForm.type" :options="[
            {label:'btree',value:'btree'},{label:'hash',value:'hash'},
            {label:'fulltext',value:'fulltext'},{label:'vector',value:'vector'}]" style="width:140px" />
        </n-form-item>
        <n-form-item label="Unique"><n-switch v-model:value="idxForm.unique" /></n-form-item>
        <n-form-item label=" "><n-button type="primary" @click="addIdx">Add index</n-button></n-form-item>
      </n-form>
      <n-data-table :columns="idxCols" :data="idxList" :pagination="false" size="small" />
    </n-modal>
  </div>
</template>

<script setup lang="ts">
import { h, ref, onMounted } from 'vue';
import { useRouter } from 'vue-router';
import { NDataTable, NButton, NSpace, NModal, NForm, NFormItem, NInput, NSelect, NSwitch, NPopconfirm, NIcon, useMessage } from 'naive-ui';
import { AddOutline } from '@vicons/ionicons5';
import { useAuthStore } from '@/stores/auth';
import delta from '@/api/delta';

const auth = useAuthStore();
const message = useMessage();
const router = useRouter();
const cols_data = ref<any[]>([]);
const showCreate = ref(false);
const form = ref<any>({ name: '', database: auth.database, schema: auth.schema });
const showIdx = ref(false);
const currentCol = ref('');
const idxList = ref<any[]>([]);
const idxForm = ref<any>({ fields: '', type: 'btree', unique: false });

async function load() { cols_data.value = (await delta.listCollections(auth.database)).data.collections || []; }
async function create() {
  try { await delta.createCollection(form.value); showCreate.value=false; message.success('Created'); load(); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function del(name: string) {
  try { await delta.deleteCollection(name, auth.database, auth.schema); message.success('Deleted'); load(); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function openIdx(c: any) {
  currentCol.value = c.name; showIdx.value = true;
  const r = await delta.getCollection(c.name, auth.database, auth.schema);
  idxList.value = r.data.indexes || [];
}
async function addIdx() {
  try {
    const fields = idxForm.value.fields.split(',').map((s: string) => s.trim()).filter(Boolean);
    if (!fields.length) { message.error('Specify at least one field'); return; }
    const name = 'idx_' + fields.join('_');
    await delta.createIndex(currentCol.value, { name, fields, type: idxForm.value.type, unique: idxForm.value.unique }, auth.database, auth.schema);
    idxForm.value.fields = '';
    openIdx({ name: currentCol.value });
  } catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function delIdx(name: string) {
  await delta.deleteIndex(currentCol.value, name, auth.database, auth.schema);
  openIdx({ name: currentCol.value });
}

const cols = [
  { title: 'Name', key: 'name' },
  { title: 'Database', key: 'database' },
  { title: 'Schema', key: 'schema' },
  { title: 'Documents', key: 'document_count' },
  { title: 'Indexes', key: 'indexes', render: (r: any) => (r.indexes || []).length },
  { title: 'RLS', key: 'rls_enabled', render: (r: any) => r.rls_enabled ? 'enabled' : 'disabled' },
  { title: 'Actions', key: 'actions', width: 240, render: (r: any) => h(NSpace, null, { default: () => [
    h(NButton, { size: 'small', onClick: () => router.push('/collections/' + r.name) }, { default: () => 'Browse' }),
    h(NButton, { size: 'small', onClick: () => openIdx(r) }, { default: () => 'Indexes' }),
    h(NPopconfirm, { onPositiveClick: () => del(r.name) },
      { trigger: () => h(NButton, { size: 'small', type: 'error', ghost: true }, { default: () => 'Delete' }),
        default: () => 'Delete this collection?' })
  ]})}
];
const idxCols = [
  { title: 'Name', key: 'name' },
  { title: 'Fields', key: 'fields', render: (r: any) => (r.fields || []).join(', ') },
  { title: 'Type', key: 'type' },
  { title: 'Unique', key: 'unique', render: (r: any) => r.unique ? 'yes' : 'no' },
  { title: 'Actions', key: 'a', render: (r: any) => h(NPopconfirm, { onPositiveClick: () => delIdx(r.name) },
    { trigger: () => h(NButton, { size: 'small', type: 'error', ghost: true }, { default: () => 'Delete' }), default: () => 'Delete?' })}
];
onMounted(load);
</script>
