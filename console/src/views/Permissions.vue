<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Permissions</div>
        <div class="page-subtitle">GRANT and REVOKE privileges to roles on database objects</div>
      </div>
    </div>

    <n-card title="GRANT / REVOKE" :bordered="true">
      <n-form :model="form" label-placement="top">
        <n-grid :cols="2" :x-gap="16">
          <n-gi>
            <n-form-item label="Role"><n-select v-model:value="form.role" :options="roleOpts" filterable /></n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item label="Object type">
              <n-select v-model:value="form.target.type" :options="[
                {label:'Database',value:'database'},
                {label:'Schema',value:'schema'},
                {label:'Collection',value:'collection'},
                {label:'Function',value:'function'}]" />
            </n-form-item>
          </n-gi>
        </n-grid>

        <n-form-item label="Privileges">
          <n-checkbox-group v-model:value="form.privileges">
            <n-space>
              <n-checkbox value="SELECT" />
              <n-checkbox value="INSERT" />
              <n-checkbox value="UPDATE" />
              <n-checkbox value="DELETE" />
              <n-checkbox value="CREATE" />
              <n-checkbox value="DROP" />
              <n-checkbox value="ALTER" />
              <n-checkbox value="INDEX" />
              <n-checkbox value="TRUNCATE" />
              <n-checkbox value="USAGE" />
              <n-checkbox value="CONNECT" />
              <n-checkbox value="EXECUTE" />
              <n-checkbox value="ALL" />
            </n-space>
          </n-checkbox-group>
        </n-form-item>

        <n-grid :cols="3" :x-gap="16">
          <n-gi><n-form-item label="Database"><n-input v-model:value="form.target.database" /></n-form-item></n-gi>
          <n-gi><n-form-item label="Schema"><n-input v-model:value="form.target.schema" /></n-form-item></n-gi>
          <n-gi><n-form-item label="Object name"><n-input v-model:value="form.target.name" /></n-form-item></n-gi>
        </n-grid>

        <n-form-item label="WITH GRANT OPTION"><n-switch v-model:value="form.with_grant_option" /></n-form-item>
        <n-space>
          <n-button type="primary" @click="grant">GRANT</n-button>
          <n-button type="error" ghost @click="revoke">REVOKE</n-button>
          <n-button @click="grantAll">GRANT ALL</n-button>
        </n-space>
      </n-form>
    </n-card>

    <n-card title="Permission catalog" style="margin-top: 16px;" :bordered="true">
      <n-space style="margin-bottom: 12px;">
        <n-select v-model:value="filterRole" :options="[{label:'All roles', value:''}, ...roleOpts]" style="width:240px" @update:value="load" />
        <n-button @click="load">Refresh</n-button>
      </n-space>
      <n-data-table :columns="cols" :data="perms" />
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue';
import { NCard, NForm, NFormItem, NInput, NSelect, NSwitch, NSpace, NButton, NCheckbox, NCheckboxGroup, NDataTable, NGrid, NGi, useMessage } from 'naive-ui';
import delta from '@/api/delta';

const message = useMessage();
const roleOpts = ref<any[]>([]);
const form = ref<any>({ role: '', privileges: ['SELECT'], target: { type: 'collection', database: 'default', schema: 'public', name: '' }, with_grant_option: false });
const filterRole = ref('');
const perms = ref<any[]>([]);

async function loadRoles() {
  const r = await delta.listRoles();
  roleOpts.value = (r.data.roles || []).map((x: any) => ({ label: x.name, value: x.name }));
}
async function load() {
  const r = await delta.listPermissions(filterRole.value);
  perms.value = r.data.permissions || [];
}
async function grant() {
  try { await delta.grantPermission(form.value); message.success('Granted'); load(); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function revoke() {
  try { await delta.revokePermission(form.value); message.success('Revoked'); load(); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function grantAll() {
  try { await delta.grantAll(form.value.role, form.value.target); message.success('Granted ALL'); load(); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
const cols = [
  { title: 'Role', key: 'role' },
  { title: 'Object', key: 'target', render: (r: any) => `${r.target.type}: ${r.target.database || '*'}.${r.target.schema || '*'}.${r.target.name || '*'}` },
  { title: 'Privileges', key: 'privileges', render: (r: any) => (r.privileges || []).join(', ') },
  { title: 'Grant option', key: 'with_grant_option', render: (r: any) => r.with_grant_option ? 'yes' : 'no' },
  { title: 'Granted by', key: 'granted_by' },
  { title: 'Granted at', key: 'granted_at', render: (r: any) => new Date(r.granted_at).toLocaleString() }
];
onMounted(async () => { await loadRoles(); await load(); });
</script>
