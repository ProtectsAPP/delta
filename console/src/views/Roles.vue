<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Roles</div>
        <div class="page-subtitle">PostgreSQL-style roles with inheritance</div>
      </div>
      <n-space>
        <n-button @click="load">Refresh</n-button>
        <n-button type="primary" @click="showCreate=true">
          <template #icon><n-icon><AddOutline /></n-icon></template>
          New role
        </n-button>
      </n-space>
    </div>

    <n-data-table :columns="cols" :data="roles" />

    <n-modal v-model:show="showCreate" preset="dialog" title="Create role" style="width:500px">
      <n-form :model="form" label-placement="top">
        <n-form-item label="Name"><n-input v-model:value="form.name" /></n-form-item>
        <n-form-item label="Description"><n-input v-model:value="form.description" /></n-form-item>
        <n-form-item label="Inherits from"><n-select v-model:value="form.parents" multiple :options="parentOpts" /></n-form-item>
      </n-form>
      <template #action>
        <n-button @click="showCreate=false">Cancel</n-button>
        <n-button type="primary" @click="create">Create</n-button>
      </template>
    </n-modal>
  </div>
</template>

<script setup lang="ts">
import { h, ref, computed, onMounted } from 'vue';
import { NDataTable, NSpace, NButton, NModal, NForm, NFormItem, NInput, NSelect, NPopconfirm, NTag, NIcon, useMessage } from 'naive-ui';
import { AddOutline } from '@vicons/ionicons5';
import delta from '@/api/delta';

const roles = ref<any[]>([]);
const showCreate = ref(false);
const form = ref<any>({ name: '', description: '', parents: [] });
const message = useMessage();

async function load() { roles.value = (await delta.listRoles()).data.roles || []; }
const parentOpts = computed(() => roles.value.map(r => ({ label: r.name, value: r.name })));

async function create() {
  try { await delta.createRole(form.value); showCreate.value=false; message.success('Created'); load(); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function del(name: string) {
  try { await delta.deleteRole(name); message.success('Deleted'); load(); }
  catch (e: any) { message.error(e.response?.data?.message || e.message); }
}

const cols = [
  { title: 'ID', key: 'id', width: 60 },
  { title: 'Name', key: 'name' },
  { title: 'Description', key: 'description' },
  { title: 'Inherits', key: 'parents', render: (r: any) => (r.parents || []).join(', ') || '-' },
  { title: 'System', key: 'is_system', render: (r: any) => r.is_system ? h(NTag, { type: 'info', size: 'small', bordered: false }, { default: () => 'system' }) : '-' },
  { title: 'Actions', key: 'a', width: 160, render: (r: any) => r.is_system ? '-' : h(NPopconfirm, { onPositiveClick: () => del(r.name) },
    { trigger: () => h(NButton, { size: 'small', type: 'error', ghost: true }, { default: () => 'Delete' }), default: () => 'Delete this role?' }) }
];
onMounted(load);
</script>
