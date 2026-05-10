<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Users</div>
        <div class="page-subtitle">Manage database users and their roles</div>
      </div>
      <n-space>
        <n-button @click="load">Refresh</n-button>
        <n-button type="primary" @click="showCreate=true">
          <template #icon><n-icon><AddOutline /></n-icon></template>
          New user
        </n-button>
      </n-space>
    </div>

    <n-data-table :columns="cols" :data="users" />

    <n-modal v-model:show="showCreate" preset="dialog" title="Create user" style="width:540px">
      <n-form :model="form" label-placement="top">
        <n-form-item label="Username"><n-input v-model:value="form.username" /></n-form-item>
        <n-form-item label="Password"><n-input v-model:value="form.password" type="password" show-password-on="click" /></n-form-item>
        <n-form-item label="Roles"><n-select v-model:value="form.roles" multiple :options="roleOpts" /></n-form-item>
        <n-form-item label="Default database"><n-input v-model:value="form.default_database" /></n-form-item>
        <n-form-item label="Connection limit (-1 = unlimited)"><n-input-number v-model:value="form.connection_limit" /></n-form-item>
        <n-form-item label="Superuser"><n-switch v-model:value="form.superuser" /></n-form-item>
        <n-form-item label="Can create databases"><n-switch v-model:value="form.can_create_db" /></n-form-item>
        <n-form-item label="Can create roles"><n-switch v-model:value="form.can_create_role" /></n-form-item>
      </n-form>
      <template #action>
        <n-button @click="showCreate=false">Cancel</n-button>
        <n-button type="primary" @click="create">Create</n-button>
      </template>
    </n-modal>

    <n-modal v-model:show="showEdit" preset="dialog" title="Edit user" style="width:540px">
      <n-form :model="editForm" label-placement="top">
        <n-form-item label="New password (leave empty to keep)"><n-input v-model:value="editForm.password" type="password" show-password-on="click" /></n-form-item>
        <n-form-item label="Connection limit"><n-input-number v-model:value="editForm.connection_limit" /></n-form-item>
        <n-form-item label="Default database"><n-input v-model:value="editForm.default_database" /></n-form-item>
      </n-form>
      <template #action>
        <n-button @click="showEdit=false">Cancel</n-button>
        <n-button type="primary" @click="doEdit">Save</n-button>
      </template>
    </n-modal>

    <n-modal v-model:show="showRoles" preset="card" :title="'Roles — ' + editTarget" style="width:520px">
      <n-space style="margin-bottom: 12px;">
        <n-select v-model:value="grantRole" :options="roleOpts" placeholder="Select role" style="width:240px" />
        <n-button type="primary" @click="doGrantRole">Grant</n-button>
      </n-space>
      <n-data-table :columns="userRoleCols" :data="userRoles.map((r:string)=>({name:r}))" :pagination="false" size="small" />
    </n-modal>
  </div>
</template>

<script setup lang="ts">
import { h, ref, onMounted } from 'vue';
import { NDataTable, NSpace, NButton, NModal, NForm, NFormItem, NInput, NInputNumber, NSelect, NSwitch, NPopconfirm, NTag, NIcon, useMessage } from 'naive-ui';
import { AddOutline } from '@vicons/ionicons5';
import delta from '@/api/delta';

const users = ref<any[]>([]);
const showCreate = ref(false);
const showEdit = ref(false);
const showRoles = ref(false);
const editTarget = ref('');
const userRoles = ref<string[]>([]);
const grantRole = ref('');
const form = ref<any>({ username: '', password: '', roles: ['read_write'], default_database: 'default', connection_limit: -1, superuser: false, can_create_db: false, can_create_role: false });
const editForm = ref<any>({ password: '', connection_limit: -1, default_database: '' });
const roleOpts = ref<any[]>([]);
const message = useMessage();

async function load() {
  users.value = (await delta.listUsers()).data.users || [];
  const r = await delta.listRoles();
  roleOpts.value = (r.data.roles || []).map((x: any) => ({ label: x.name, value: x.name }));
}
async function create() {
  try { await delta.createUser(form.value); showCreate.value=false; message.success('Created'); load(); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function del(name: string) {
  try { await delta.deleteUser(name); message.success('Deleted'); load(); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function lock(name: string) { await delta.lockUser(name); load(); }
async function unlock(name: string) { await delta.unlockUser(name); load(); }
function openEdit(u: any) {
  editTarget.value = u.username;
  editForm.value = { password: '', connection_limit: u.connection_limit, default_database: u.default_database };
  showEdit.value = true;
}
async function doEdit() {
  const data: any = {};
  if (editForm.value.password) data.password = editForm.value.password;
  data.connection_limit = editForm.value.connection_limit;
  data.default_database = editForm.value.default_database;
  await delta.updateUser(editTarget.value, data);
  showEdit.value = false; load(); message.success('Saved');
}
async function openRoles(u: any) {
  editTarget.value = u.username; userRoles.value = u.roles || []; showRoles.value = true;
}
async function doGrantRole() {
  if (!grantRole.value) return;
  try { await delta.grantUserRole(editTarget.value, grantRole.value); message.success('Granted');
    const u = (await delta.listUsers()).data.users.find((x:any)=>x.username===editTarget.value);
    userRoles.value = u?.roles || [];
    load();
  } catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function revokeRole(role: string) {
  try { await delta.revokeUserRole(editTarget.value, role); userRoles.value = userRoles.value.filter(r => r !== role); load(); }
  catch (e: any) { message.error(e.message); }
}

const cols = [
  { title: 'ID', key: 'id', width: 60 },
  { title: 'Username', key: 'username' },
  { title: 'Status', key: 'status', render: (r: any) => h(NTag, { type: r.status === 'active' ? 'success' : 'warning', bordered: false, size: 'small' }, { default: () => r.status }) },
  { title: 'Roles', key: 'roles', render: (r: any) => (r.roles || []).map((role: string) => h(NTag, { size: 'small', style: 'margin-right: 4px', bordered: false }, { default: () => role })) },
  { title: 'Superuser', key: 'superuser', render: (r: any) => r.superuser ? 'yes' : 'no' },
  { title: 'Conn. limit', key: 'connection_limit' },
  { title: 'Default DB', key: 'default_database' },
  { title: 'Last login', key: 'last_login', render: (r: any) => r.last_login ? new Date(r.last_login).toLocaleString() : '-' },
  { title: 'Actions', key: 'a', width: 280, render: (r: any) => h(NSpace, null, { default: () => [
    h(NButton, { size: 'small', onClick: () => openEdit(r) }, { default: () => 'Edit' }),
    h(NButton, { size: 'small', onClick: () => openRoles(r) }, { default: () => 'Roles' }),
    r.status === 'locked'
      ? h(NButton, { size: 'small', onClick: () => unlock(r.username) }, { default: () => 'Unlock' })
      : h(NButton, { size: 'small', onClick: () => lock(r.username) }, { default: () => 'Lock' }),
    h(NPopconfirm, { onPositiveClick: () => del(r.username) },
      { trigger: () => h(NButton, { size: 'small', type: 'error', ghost: true }, { default: () => 'Delete' }), default: () => 'Delete this user?' })
  ]})}
];
const userRoleCols = [
  { title: 'Role', key: 'name' },
  { title: 'Actions', key: 'a', width: 120, render: (r: any) => h(NPopconfirm, { onPositiveClick: () => revokeRole(r.name) },
    { trigger: () => h(NButton, { size: 'small', type: 'error', ghost: true }, { default: () => 'Revoke' }), default: () => 'Revoke this role?' }) }
];
onMounted(load);
</script>
