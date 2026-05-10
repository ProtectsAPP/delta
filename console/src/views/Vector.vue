<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Vector Search</div>
        <div class="page-subtitle">HNSW vector index — insert and similarity search</div>
      </div>
    </div>

    <n-grid :cols="2" :x-gap="16">
      <n-gi>
        <n-card title="Insert vector" :bordered="true">
          <n-form label-placement="top">
            <n-form-item label="Collection"><n-input v-model:value="collection" /></n-form-item>
            <n-form-item label="ID (optional)"><n-input v-model:value="id" placeholder="auto-generated when empty" /></n-form-item>
            <n-form-item label="Vector (JSON array)">
              <n-input class="code-input" type="textarea" v-model:value="vecText" :autosize="{minRows:3,maxRows:8}" />
            </n-form-item>
            <n-form-item label="Metadata (JSON)">
              <n-input class="code-input" type="textarea" v-model:value="metaText" :autosize="{minRows:3,maxRows:6}" />
            </n-form-item>
            <n-form-item label="Distance metric">
              <n-select v-model:value="metric" :options="[
                {label:'Cosine',value:'cosine'},
                {label:'Euclidean',value:'euclidean'},
                {label:'Dot product',value:'dot'}]" />
            </n-form-item>
            <n-button type="primary" @click="onInsert">Insert</n-button>
          </n-form>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card title="Search" :bordered="true">
          <n-form label-placement="top">
            <n-form-item label="Collection"><n-input v-model:value="searchCol" /></n-form-item>
            <n-form-item label="Query vector (JSON)">
              <n-input class="code-input" type="textarea" v-model:value="queryText" :autosize="{minRows:3,maxRows:8}" />
            </n-form-item>
            <n-form-item label="Top K"><n-input-number v-model:value="topK" style="width:100%" /></n-form-item>
            <n-form-item label="Min score"><n-input-number v-model:value="minScore" :step="0.01" style="width:100%" /></n-form-item>
            <n-button type="primary" @click="onSearch">Search</n-button>
          </n-form>
        </n-card>
      </n-gi>
    </n-grid>

    <n-card title="Results" style="margin-top: 16px;" :bordered="true">
      <n-data-table :columns="cols" :data="results" />
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { h, ref } from 'vue';
import { NCard, NGrid, NGi, NForm, NFormItem, NInput, NInputNumber, NSelect, NButton, NDataTable, useMessage } from 'naive-ui';
import { useAuthStore } from '@/stores/auth';
import delta from '@/api/delta';

const auth = useAuthStore();
const message = useMessage();
const collection = ref('vec_demo');
const id = ref('');
const vecText = ref('[0.1, 0.2, 0.3]');
const metaText = ref('{}');
const metric = ref('cosine');
const searchCol = ref('vec_demo');
const queryText = ref('[0.1, 0.2, 0.3]');
const topK = ref(10);
const minScore = ref(0);
const results = ref<any[]>([]);

function genId() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
}

async function onInsert() {
  try {
    const v = JSON.parse(vecText.value);
    const m = JSON.parse(metaText.value || '{}');
    const r = await delta.insertVector(collection.value, id.value || genId(), v, m, metric.value, auth.database, auth.schema);
    message.success('Inserted: ' + r.data.id);
  } catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
async function onSearch() {
  try {
    const v = JSON.parse(queryText.value);
    const r = await delta.searchVectors(searchCol.value, { vector: v, top_k: topK.value, min_score: minScore.value }, auth.database, auth.schema);
    results.value = r.data || [];
  } catch (e: any) { message.error(e.response?.data?.message || e.message); }
}
const cols = [
  { title: 'ID', key: 'id' },
  { title: 'Score', key: 'score', render: (r: any) => Number(r.score).toFixed(6) },
  { title: 'Metadata', key: 'metadata', render: (r: any) => h('pre', { class: 'json-viewer', style: 'margin:0; border:none; background:transparent; padding:0;' }, JSON.stringify(r.metadata)) }
];
</script>
