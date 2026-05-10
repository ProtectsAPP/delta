<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">{{ name }}</div>
        <div class="page-subtitle">Documents in {{ auth.database }} / {{ auth.schema }} / {{ name }}</div>
      </div>
      <n-space>
        <n-button @click="$router.back()">Back</n-button>
        <n-button @click="load">Refresh</n-button>
        <n-button type="primary" @click="showInsert = true">
          <template #icon><n-icon><AddOutline /></n-icon></template>
          Insert document
        </n-button>
      </n-space>
    </div>

    <n-card title="Filter" size="small" style="margin-bottom: 16px;" :bordered="true">
      <n-input class="code-input" type="textarea" v-model:value="filterText"
               placeholder='{"field":"value"}  or  {"age":{"$gte":18}}'
               :autosize="{minRows:2,maxRows:6}" />
      <n-space style="margin-top:12px;" align="center">
        <n-button type="primary" @click="load">Search</n-button>
        <span style="color:#86909c;">Limit:</span><n-input-number v-model:value="limit" style="width:120px" />
        <span style="color:#86909c;">Skip:</span><n-input-number v-model:value="skip" style="width:120px" />
        <span style="color:#86909c;">Total: {{ total }}</span>
      </n-space>
    </n-card>

    <n-data-table :columns="cols" :data="docs" :pagination="{pageSize: 50}" />

    <n-modal v-model:show="showInsert" preset="card" title="Insert document" style="width:640px">
      <n-input class="code-input" type="textarea" v-model:value="newDocText" :autosize="{minRows:8,maxRows:24}" />
      <n-space style="margin-top:16px;" justify="end">
        <n-button @click="showInsert=false">Cancel</n-button>
        <n-button type="primary" @click="doInsert">Insert</n-button>
      </n-space>
    </n-modal>

    <n-modal v-model:show="showEdit" preset="card" title="Edit document" style="width:640px">
      <n-input class="code-input" type="textarea" v-model:value="editDocText" :autosize="{minRows:8,maxRows:24}" />
      <n-space style="margin-top:16px;" justify="end">
        <n-button @click="showEdit=false">Cancel</n-button>
        <n-button type="primary" @click="doUpdate">Save</n-button>
      </n-space>
    </n-modal>
  </div>
</template>

<script setup lang="ts">
import { h, ref, onMounted } from 'vue';
import { useRoute } from 'vue-router';
import { NCard, NInput, NInputNumber, NButton, NSpace, NDataTable, NModal, NPopconfirm, NIcon, useMessage } from 'naive-ui';
import { AddOutline } from '@vicons/ionicons5';
import { useAuthStore } from '@/stores/auth';
import delta from '@/api/delta';

const route = useRoute();
const auth = useAuthStore();
const message = useMessage();
const name = route.params.name as string;
const docs = ref<any[]>([]);
const total = ref(0);
const limit = ref(50);
const skip = ref(0);
const filterText = ref('{}');
const showInsert = ref(false);
const newDocText = ref('{\n  "name": "example",\n  "value": 100\n}');
const showEdit = ref(false);
const editId = ref('');
const editDocText = ref('');

async function load() {
  let filter = {};
  try { filter = JSON.parse(filterText.value || '{}'); } catch { message.error('Filter is not valid JSON'); return; }
  try {
    const r = await delta.searchDocuments(name, { filter, limit: limit.value, skip: skip.value }, auth.database, auth.schema);
    docs.value = r.data.documents || [];
    total.value = r.data.total || 0;
  } catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function doInsert() {
  try { const d = JSON.parse(newDocText.value);
    await delta.insertDocument(name, d, auth.database, auth.schema);
    showInsert.value = false; message.success('Inserted'); load();
  } catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
function openEdit(r: any) {
  editId.value = r.data._id; editDocText.value = JSON.stringify(r.data, null, 2); showEdit.value = true;
}
async function doUpdate() {
  try { const d = JSON.parse(editDocText.value);
    delete d._id;
    await delta.updateDocument(name, editId.value, { $set: d }, auth.database, auth.schema);
    showEdit.value = false; message.success('Updated'); load();
  } catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function del(id: string) {
  try { await delta.deleteDocument(name, id, auth.database, auth.schema); message.success('Deleted'); load(); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}

const cols = [
  { title: 'ID', key: '_id', render: (r: any) => r.data?._id || r._id, width: 240, ellipsis: true },
  { title: 'Version', key: 'version', width: 80 },
  { title: 'Updated', key: 'updated_at', width: 180, render: (r: any) => r.updated_at ? new Date(r.updated_at).toLocaleString() : '-' },
  { title: 'Data', key: 'data', render: (r: any) => h('pre', { class: 'json-viewer', style: 'max-height: 200px; margin: 0; border: none; background: transparent; padding: 0;' }, JSON.stringify(r.data, null, 2)) },
  { title: 'Actions', key: 'a', width: 180, render: (r: any) => h(NSpace, null, { default: () => [
    h(NButton, { size: 'small', onClick: () => openEdit(r) }, { default: () => 'Edit' }),
    h(NPopconfirm, { onPositiveClick: () => del(r.data._id) },
      { trigger: () => h(NButton, { size: 'small', type: 'error', ghost: true }, { default: () => 'Delete' }), default: () => 'Delete?' })
  ]})}
];
onMounted(load);
</script>
