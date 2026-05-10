<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">RLS Policies</div>
        <div class="page-subtitle">Row-level security policies (USING / WITH CHECK expressions)</div>
      </div>
    </div>

    <n-card title="Target collection" :bordered="true">
      <n-form inline label-placement="top">
        <n-form-item label="Database"><n-input v-model:value="db" placeholder="default" style="width:160px" /></n-form-item>
        <n-form-item label="Schema"><n-input v-model:value="schema" placeholder="public" style="width:160px" /></n-form-item>
        <n-form-item label="Collection"><n-input v-model:value="collection" placeholder="users" style="width:200px" /></n-form-item>
        <n-form-item label=" ">
          <n-space>
            <n-button type="primary" @click="load">Load</n-button>
            <n-button :type="enabled ? 'error' : 'success'" :ghost="enabled" @click="toggle">{{ enabled ? 'Disable RLS' : 'Enable RLS' }}</n-button>
            <n-button @click="showCreate=true" :disabled="!collection">New policy</n-button>
          </n-space>
        </n-form-item>
      </n-form>
      <n-tag :type="enabled ? 'success' : 'warning'" :bordered="false">
        Row-level security: {{ enabled ? 'enabled' : 'disabled' }}
      </n-tag>
    </n-card>

    <n-card title="Policies" style="margin-top: 16px;" :bordered="true">
      <n-data-table :columns="cols" :data="policies" />
    </n-card>

    <n-modal v-model:show="showCreate" preset="dialog" title="Create RLS policy" style="width:640px">
      <n-form :model="form" label-placement="top">
        <n-form-item label="Name"><n-input v-model:value="form.name" /></n-form-item>
        <n-form-item label="Command">
          <n-select v-model:value="form.command" :options="[
            {label:'ALL',value:'ALL'},{label:'SELECT',value:'SELECT'},
            {label:'INSERT',value:'INSERT'},{label:'UPDATE',value:'UPDATE'},
            {label:'DELETE',value:'DELETE'}]" />
        </n-form-item>
        <n-form-item label="Apply to roles (empty = all roles)">
          <n-select v-model:value="form.roles" multiple :options="roleOpts" />
        </n-form-item>
        <n-form-item label="USING expression (filters rows)">
          <n-input v-model:value="form.using_expr" placeholder="owner = current_user" />
        </n-form-item>
        <n-form-item label="WITH CHECK expression (validates writes)">
          <n-input v-model:value="form.with_check_expr" placeholder="owner = current_user" />
        </n-form-item>
      </n-form>
      <template #action>
        <n-button @click="showCreate=false">Cancel</n-button>
        <n-button type="primary" @click="create">Create</n-button>
      </template>
    </n-modal>
  </div>
</template>

<script setup lang="ts">
import { h, ref, onMounted } from 'vue';
import { NCard, NSpace, NInput, NButton, NTag, NDataTable, NModal, NForm, NFormItem, NSelect, NPopconfirm, useMessage } from 'naive-ui';
import { useAuthStore } from '@/stores/auth';
import delta from '@/api/delta';

const auth = useAuthStore();
const message = useMessage();
const db = ref(auth.database);
const schema = ref(auth.schema);
const collection = ref('');
const enabled = ref(false);
const policies = ref<any[]>([]);
const showCreate = ref(false);
const roleOpts = ref<any[]>([]);
const form = ref<any>({ name: '', command: 'ALL', roles: [], using_expr: '', with_check_expr: '' });

async function load() {
  if (!collection.value) { message.error('Specify collection'); return; }
  const r = await delta.listPolicies(db.value, schema.value, collection.value);
  policies.value = r.data.policies || [];
  enabled.value = !!r.data.enabled;
}
async function toggle() {
  if (!collection.value) return;
  await delta.setRLSEnabled(db.value, schema.value, collection.value, !enabled.value);
  load();
}
async function create() {
  try { await delta.createPolicy(db.value, schema.value, collection.value, form.value); showCreate.value=false; message.success('Created'); load(); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function del(name: string) {
  await delta.deletePolicy(db.value, schema.value, collection.value, name); load();
}
const cols = [
  { title: 'ID', key: 'id', width: 60 },
  { title: 'Name', key: 'name' },
  { title: 'Command', key: 'command' },
  { title: 'Roles', key: 'roles', render: (r: any) => (r.roles || []).join(', ') || '*' },
  { title: 'USING', key: 'using_expr' },
  { title: 'WITH CHECK', key: 'with_check_expr' },
  { title: 'Enabled', key: 'enabled', render: (r: any) => r.enabled ? 'yes' : 'no' },
  { title: 'Actions', key: 'a', width: 120, render: (r: any) => h(NPopconfirm, { onPositiveClick: () => del(r.name) },
    { trigger: () => h(NButton, { size:'small', type:'error', ghost: true }, { default: () => 'Delete' }), default: () => 'Delete this policy?' })}
];
onMounted(async () => {
  const r = await delta.listRoles();
  roleOpts.value = (r.data.roles || []).map((x: any) => ({ label: x.name, value: x.name }));
});
</script>
