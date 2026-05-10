<template>
  <div class="login-page">
    <div class="login-card">
      <div class="brand">
        <div class="brand-logo">Δ</div>
        <div class="brand-name">Delta</div>
        <div class="brand-sub">Document database with cache, vectors and full RBAC</div>
      </div>
      <n-form label-placement="top">
        <n-form-item label="Username">
          <n-input v-model:value="username" placeholder="admin" @keydown.enter="onLogin" />
        </n-form-item>
        <n-form-item label="Password">
          <n-input v-model:value="password" type="password" show-password-on="click" placeholder="Password" @keydown.enter="onLogin" />
        </n-form-item>
        <n-form-item label="Database">
          <n-input v-model:value="database" placeholder="default" />
        </n-form-item>
        <n-button type="primary" block :loading="loading" @click="onLogin" size="large">Sign in</n-button>
        <div class="hint">Default credentials: <code>admin / admin</code></div>
      </n-form>
    </div>
    <div class="footer">Delta Server &middot; v1.0.0</div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import { useRouter } from 'vue-router';
import { NForm, NFormItem, NInput, NButton, useMessage } from 'naive-ui';
import { useAuthStore } from '@/stores/auth';

const username = ref('admin');
const password = ref('admin');
const database = ref('default');
const loading = ref(false);
const auth = useAuthStore();
const router = useRouter();
const message = useMessage();

async function onLogin() {
  loading.value = true;
  try {
    await auth.login(username.value, password.value, database.value);
    message.success('Signed in');
    router.push('/dashboard');
  } catch (e: any) {
    message.error(e.response?.data?.message || e.message || 'Sign-in failed');
  } finally { loading.value = false; }
}
</script>

<style scoped>
.login-page {
  min-height: 100vh;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  background: var(--bg);
  padding: 24px;
  position: relative;
  overflow: hidden;
}
.login-page::before {
  content: '';
  position: absolute; inset: 0;
  background-image: radial-gradient(circle at center, rgba(255,255,255,0.04) 1px, transparent 1.5px);
  background-size: 28px 28px;
  pointer-events: none;
  mask-image: linear-gradient(to bottom, black 30%, transparent 100%);
  -webkit-mask-image: linear-gradient(to bottom, black 30%, transparent 100%);
}
.login-card {
  position: relative;
  width: 400px;
  background: var(--surface);
  border-radius: 12px;
  padding: 36px 40px;
  border: 1px solid var(--border);
  box-shadow: 0 30px 80px -30px rgba(0,0,0,0.6);
}
.brand { text-align: center; margin-bottom: 28px; }
.brand-logo {
  font-size: 30px;
  font-weight: 800;
  color: #0a0b0d;
  background: var(--accent);
  width: 56px; height: 56px; line-height: 56px;
  margin: 0 auto;
  border-radius: 12px;
  box-shadow: 0 0 30px -6px rgba(74,222,128,0.6);
}
.brand-name { font-size: 22px; font-weight: 700; margin-top: 14px; letter-spacing: -0.02em; color: var(--text); }
.brand-sub { color: var(--text-2); font-size: 13px; margin-top: 4px; }
.hint { text-align: center; color: var(--muted); font-size: 12px; margin-top: 18px; }
.hint code {
  background: var(--bg-1);
  border: 1px solid var(--border);
  padding: 2px 8px;
  border-radius: 4px;
  font-family: 'JetBrains Mono', monospace;
  color: var(--accent);
}
.footer { color: var(--muted); font-size: 12px; margin-top: 18px; position: relative; }
</style>
