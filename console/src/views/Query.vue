<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Query</div>
        <div class="page-subtitle">Run find / aggregate / count queries</div>
      </div>
    </div>

    <n-card title="Query builder" :bordered="true">
      <n-form label-placement="top">
        <n-grid :cols="2" :x-gap="16">
          <n-gi>
            <n-form-item label="Collection">
              <n-select v-model:value="collection" :options="colOpts" filterable />
            </n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item label="Operation">
              <n-radio-group v-model:value="mode">
                <n-radio-button value="find">Find</n-radio-button>
                <n-radio-button value="aggregate">Aggregate</n-radio-button>
                <n-radio-button value="count">Count</n-radio-button>
              </n-radio-group>
            </n-form-item>
          </n-gi>
        </n-grid>

        <n-form-item :label="mode==='aggregate' ? 'Pipeline (JSON array)' : 'Filter (JSON object)'">
          <n-input class="code-input" type="textarea" v-model:value="bodyText" :autosize="{minRows:6,maxRows:18}" />
        </n-form-item>

        <n-grid :cols="3" :x-gap="16" v-if="mode==='find'">
          <n-gi>
            <n-form-item label="Sort"><n-input class="code-input" v-model:value="sortText" placeholder='{"field":1}' /></n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item label="Skip"><n-input-number v-model:value="skip" style="width:100%" /></n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item label="Limit"><n-input-number v-model:value="limit" style="width:100%" /></n-form-item>
          </n-gi>
        </n-grid>

        <n-button type="primary" @click="run" :loading="loading" size="large">
          <template #icon><n-icon><PlayOutline /></n-icon></template>
          Execute
        </n-button>
      </n-form>
    </n-card>

    <n-card title="Result" style="margin-top: 16px;" :bordered="true">
      <pre class="json-viewer">{{ resultText || '— no result —' }}</pre>
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue';
import { NCard, NForm, NFormItem, NSelect, NInput, NInputNumber, NButton, NRadioGroup, NRadioButton, NGrid, NGi, NIcon, useMessage } from 'naive-ui';
import { PlayOutline } from '@vicons/ionicons5';
import { useAuthStore } from '@/stores/auth';
import delta from '@/api/delta';

const auth = useAuthStore();
const message = useMessage();
const colOpts = ref<any[]>([]);
const collection = ref('');
const mode = ref('find');
const bodyText = ref('{}');
const sortText = ref('{}');
const skip = ref(0);
const limit = ref(50);
const resultText = ref('');
const loading = ref(false);

onMounted(async () => {
  const r = await delta.listCollections(auth.database);
  colOpts.value = (r.data.collections || []).map((c: any) => ({ label: c.name, value: c.name }));
  if (colOpts.value[0]) collection.value = colOpts.value[0].value;
});

async function run() {
  if (!collection.value) { message.error('Select a collection'); return; }
  loading.value = true;
  try {
    let body: any;
    try { body = JSON.parse(bodyText.value); } catch { message.error('Body is not valid JSON'); loading.value = false; return; }
    if (mode.value === 'find') {
      const sort = JSON.parse(sortText.value || '{}');
      const r = await delta.searchDocuments(collection.value, { filter: body, sort, skip: skip.value, limit: limit.value }, auth.database, auth.schema);
      resultText.value = JSON.stringify(r.data, null, 2);
    } else if (mode.value === 'aggregate') {
      const r = await delta.aggregate(collection.value, body, auth.database, auth.schema);
      resultText.value = JSON.stringify(r.data, null, 2);
    } else {
      const r = await delta.countDocuments(collection.value, body, auth.database, auth.schema);
      resultText.value = JSON.stringify(r.data, null, 2);
    }
  } catch (e: any) {
    resultText.value = JSON.stringify(e.response?.data || { error: e.message }, null, 2);
    message.error(e.response?.data?.message || e.message);
  } finally { loading.value = false; }
}
</script>
