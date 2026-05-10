import { defineStore } from 'pinia';
import { ref, computed } from 'vue';
import delta from '@/api/delta';

export const useAuthStore = defineStore('auth', () => {
  const token = ref(localStorage.getItem('delta_token') || '');
  const user = ref<any>(JSON.parse(localStorage.getItem('delta_user') || 'null'));
  const database = ref(localStorage.getItem('delta_db') || 'default');
  const schema = ref(localStorage.getItem('delta_schema') || 'public');

  const isLoggedIn = computed(() => !!token.value);
  const isSuperuser = computed(() => user.value?.superuser === true || (user.value?.user?.superuser === true));

  async function login(username: string, password: string, db?: string) {
    const r = await delta.login(username, password, db);
    token.value = r.data.token;
    user.value = r.data;
    database.value = r.data.database;
    schema.value = r.data.schema;
    localStorage.setItem('delta_token', token.value);
    localStorage.setItem('delta_user', JSON.stringify(user.value));
    localStorage.setItem('delta_db', database.value);
    localStorage.setItem('delta_schema', schema.value);
    delta.setToken(token.value);
  }
  async function logout() {
    try { await delta.logout(); } catch {}
    token.value = ''; user.value = null;
    delta.clearToken();
    localStorage.removeItem('delta_db');
    localStorage.removeItem('delta_schema');
  }
  async function setDatabase(db: string, sch = 'public') {
    await delta.use(db, sch);
    database.value = db; schema.value = sch;
    localStorage.setItem('delta_db', db);
    localStorage.setItem('delta_schema', sch);
  }
  return { token, user, database, schema, isLoggedIn, isSuperuser, login, logout, setDatabase };
});
