<template>
  <n-layout has-sider style="height: 100vh">
    <n-layout-sider
      bordered
      :width="220"
      collapse-mode="width"
      :collapsed-width="64"
      :collapsed="collapsed"
      show-trigger="bar"
      @collapse="collapsed = true"
      @expand="collapsed = false"
    >
      <div class="logo">
        <span class="logo-icon">Δ</span>
        <span class="logo-text" v-if="!collapsed">Delta</span>
      </div>
      <n-menu
        :options="menuOptions"
        :value="activeKey"
        :collapsed="collapsed"
        :collapsed-width="64"
        :collapsed-icon-size="22"
        @update:value="onSelect"
      />
    </n-layout-sider>
    <n-layout>
      <n-layout-header bordered class="topbar">
        <div class="left">
          <h1 class="page-heading">{{ pageTitle }}</h1>
          <div class="ctx">
            <span class="ctx-label">DB</span>
            <span class="ctx-value mono">{{ auth.database }}</span>
            <span class="ctx-sep">/</span>
            <span class="ctx-value mono">{{ auth.schema }}</span>
          </div>
        </div>
        <div class="right">
          <n-button tertiary size="small" round @click="switchDb = true">
            <template #icon><n-icon><SwapHorizontalOutline /></n-icon></template>
            Switch
          </n-button>
          <n-dropdown :options="userMenu" @select="onUser" trigger="click">
            <button class="user-btn">
              <span class="avatar">{{ username.charAt(0).toUpperCase() }}</span>
              <span class="user-name">{{ username }}</span>
              <n-icon size="14" style="color:#86909c"><ChevronDownOutline /></n-icon>
            </button>
          </n-dropdown>
        </div>
      </n-layout-header>
      <n-layout-content style="padding: 0; overflow: auto;" :native-scrollbar="false">
        <router-view />
      </n-layout-content>
    </n-layout>

    <n-modal v-model:show="switchDb" preset="dialog" title="Switch database">
      <n-form label-placement="top" style="margin-top: 8px;">
        <n-form-item label="Database">
          <n-select v-model:value="dbValue" :options="dbOptions" filterable />
        </n-form-item>
        <n-form-item label="Schema">
          <n-input v-model:value="schemaValue" />
        </n-form-item>
      </n-form>
      <template #action>
        <n-button @click="switchDb = false">Cancel</n-button>
        <n-button type="primary" @click="doSwitch">Switch</n-button>
      </template>
    </n-modal>
  </n-layout>
</template>

<script setup lang="ts">
import { h, ref, computed, onMounted, watch } from 'vue';
import { useRouter, useRoute } from 'vue-router';
import {
  NLayout, NLayoutSider, NLayoutHeader, NLayoutContent, NMenu, NIcon, NButton,
  NDropdown, NModal, NForm, NFormItem, NSelect, NInput, useMessage
} from 'naive-ui';
import {
  HomeOutline, ServerOutline, FolderOutline, SearchOutline, GitBranchOutline,
  FlashOutline, PeopleOutline, KeyOutline, ShieldCheckmarkOutline, LayersOutline,
  PulseOutline, SettingsOutline, LinkOutline, SwapHorizontalOutline, ChevronDownOutline,
  GitNetworkOutline
} from '@vicons/ionicons5';
import { useAuthStore } from '@/stores/auth';
import delta from '@/api/delta';

const router = useRouter();
const route = useRoute();
const auth = useAuthStore();
const message = useMessage();
const collapsed = ref(false);
const username = computed(() => auth.user?.username || 'guest');
const activeKey = computed(() => route.path.split('/')[1] || 'dashboard');

const titleMap: Record<string, string> = {
  dashboard: 'Dashboard', databases: 'Databases', collections: 'Collections',
  query: 'Query', vector: 'Vector Search', cache: 'Cache',
  users: 'Users', roles: 'Roles', permissions: 'Permissions',
  policies: 'RLS Policies', connections: 'Connections', monitor: 'Monitor', settings: 'Settings'
};
const pageTitle = computed(() => titleMap[activeKey.value] || 'Delta');
const renderIcon = (icon: any) => () => h(NIcon, null, { default: () => h(icon) });

const menuOptions = [
  { label: 'Dashboard', key: 'dashboard', icon: renderIcon(HomeOutline) },
  { label: 'Databases', key: 'databases', icon: renderIcon(ServerOutline) },
  { label: 'Collections', key: 'collections', icon: renderIcon(FolderOutline) },
  { label: 'Query', key: 'query', icon: renderIcon(SearchOutline) },
  { label: 'Vector Search', key: 'vector', icon: renderIcon(GitBranchOutline) },
  { label: 'Cache', key: 'cache', icon: renderIcon(FlashOutline) },
  { key: 'div1', type: 'divider' as const },
  { label: 'Users', key: 'users', icon: renderIcon(PeopleOutline) },
  { label: 'Roles', key: 'roles', icon: renderIcon(KeyOutline) },
  { label: 'Permissions', key: 'permissions', icon: renderIcon(ShieldCheckmarkOutline) },
  { label: 'RLS Policies', key: 'policies', icon: renderIcon(LayersOutline) },
  { key: 'div2', type: 'divider' as const },
  { label: 'Connections', key: 'connections', icon: renderIcon(LinkOutline) },
  { label: 'Cluster', key: 'cluster', icon: renderIcon(GitNetworkOutline) },
  { label: 'Monitor', key: 'monitor', icon: renderIcon(PulseOutline) },
  { label: 'Settings', key: 'settings', icon: renderIcon(SettingsOutline) }
];

const userMenu = [{ label: 'Sign out', key: 'logout' }];

function onSelect(key: string) { router.push('/' + key); }
async function onUser(key: string) {
  if (key === 'logout') { await auth.logout(); router.push('/login'); }
}

const switchDb = ref(false);
const dbValue = ref(auth.database);
const schemaValue = ref(auth.schema);
const dbOptions = ref<any[]>([]);

async function loadDbs() {
  try {
    const r = await delta.listDatabases();
    dbOptions.value = (r.data.databases || []).map((d: any) => ({ label: d.name, value: d.name }));
  } catch {}
}
async function doSwitch() {
  try {
    await auth.setDatabase(dbValue.value, schemaValue.value);
    switchDb.value = false;
    message.success('Switched to ' + dbValue.value);
    location.reload();
  } catch (e: any) { message.error(e.message); }
}
watch(switchDb, (v) => { if (v) loadDbs(); });
onMounted(loadDbs);
</script>

<style scoped>
.logo {
  height: 60px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  border-bottom: 1px solid var(--border);
  background: var(--bg-1);
}
.logo-icon {
  font-size: 16px;
  font-weight: 800;
  color: #0a0b0d;
  background: var(--accent);
  width: 30px; height: 30px;
  display: inline-flex; align-items: center; justify-content: center;
  border-radius: 7px;
  letter-spacing: 0;
  box-shadow: 0 0 16px -4px rgba(74,222,128,0.5);
}
.logo-text {
  font-size: 17px;
  font-weight: 700;
  color: var(--text);
  letter-spacing: -0.01em;
}

.topbar {
  height: 60px;
  padding: 0 24px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  background: var(--bg-1);
}
.topbar .left { display: flex; align-items: center; gap: 16px; min-width: 0; }
.topbar .right { display: flex; align-items: center; gap: 10px; }

.page-heading {
  font-size: 15.5px;
  font-weight: 600;
  color: var(--text);
  margin: 0;
  letter-spacing: -0.01em;
}
.ctx {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 5px 12px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 999px;
  font-size: 12px;
  line-height: 1;
}
.ctx-label {
  color: var(--accent);
  font-weight: 600;
  letter-spacing: 0.08em;
  font-size: 10.5px;
  text-transform: uppercase;
}
.ctx-value { color: var(--text); font-weight: 500; font-size: 12.5px; }
.ctx-sep { color: var(--muted); font-weight: 400; }

.user-btn {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 4px 12px 4px 4px;
  background: transparent;
  border: 1px solid transparent;
  border-radius: 999px;
  cursor: pointer;
  font-family: inherit;
  font-size: 13px;
  color: var(--text);
  transition: background 0.15s, border-color 0.15s;
}
.user-btn:hover { background: var(--surface); border-color: var(--border); }
.avatar {
  width: 26px; height: 26px;
  border-radius: 50%;
  background: var(--accent);
  color: #0a0b0d;
  display: inline-flex; align-items: center; justify-content: center;
  font-size: 12px; font-weight: 700;
}
.user-name { font-weight: 500; }
</style>
