import { createRouter, createWebHistory, type RouteRecordRaw } from 'vue-router';
import { useAuthStore } from '@/stores/auth';

const routes: RouteRecordRaw[] = [
  { path: '/login', component: () => import('@/views/Login.vue'), meta: { public: true } },
  {
    path: '/',
    component: () => import('@/components/Layout.vue'),
    children: [
      { path: '', redirect: '/dashboard' },
      { path: 'dashboard', component: () => import('@/views/Dashboard.vue') },
      { path: 'databases', component: () => import('@/views/Databases.vue') },
      { path: 'collections', component: () => import('@/views/Collections.vue') },
      { path: 'collections/:name', component: () => import('@/views/Documents.vue') },
      { path: 'query', component: () => import('@/views/Query.vue') },
      { path: 'vector', component: () => import('@/views/Vector.vue') },
      { path: 'cache', component: () => import('@/views/Cache.vue') },
      { path: 'users', component: () => import('@/views/Users.vue') },
      { path: 'roles', component: () => import('@/views/Roles.vue') },
      { path: 'permissions', component: () => import('@/views/Permissions.vue') },
      { path: 'policies', component: () => import('@/views/Policies.vue') },
      { path: 'connections', component: () => import('@/views/Connections.vue') },
      { path: 'cluster', component: () => import('@/views/Cluster.vue') },
      { path: 'monitor', component: () => import('@/views/Monitor.vue') },
      { path: 'settings', component: () => import('@/views/Settings.vue') }
    ]
  }
];

const router = createRouter({ history: createWebHistory(), routes });

router.beforeEach((to, _from, next) => {
  const auth = useAuthStore();
  if (!to.meta.public && !auth.isLoggedIn) next('/login');
  else if (to.path === '/login' && auth.isLoggedIn) next('/dashboard');
  else next();
});

export default router;
