<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">Monitor</div>
        <div class="page-subtitle">Real-time metrics (refreshes every 5s)</div>
      </div>
    </div>

    <n-grid :cols="2" :x-gap="16" :y-gap="16">
      <n-gi><n-card title="Cache operations" :bordered="true"><div ref="opsRef" style="height: 300px"></div></n-card></n-gi>
      <n-gi><n-card title="Cache hit rate (%)" :bordered="true"><div ref="hitRef" style="height: 300px"></div></n-card></n-gi>
      <n-gi><n-card title="Documents" :bordered="true"><div ref="docRef" style="height: 300px"></div></n-card></n-gi>
      <n-gi><n-card title="Connections" :bordered="true"><div ref="connRef" style="height: 300px"></div></n-card></n-gi>
    </n-grid>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue';
import { NGrid, NGi, NCard } from 'naive-ui';
import * as echarts from 'echarts';
import delta from '@/api/delta';

const opsRef = ref<HTMLElement>();
const hitRef = ref<HTMLElement>();
const docRef = ref<HTMLElement>();
const connRef = ref<HTMLElement>();
let opsChart: any, hitChart: any, docChart: any, connChart: any;
const labels: string[] = [];
const ops: number[] = [];
const hit: number[] = [];
const docs: number[] = [];
const conns: number[] = [];
let timer: any = null;

function baseOption(data: number[], name: string, color: string) {
  return {
    grid: { top: 24, right: 16, bottom: 32, left: 56 },
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'category', data: labels, axisLine: { lineStyle: { color: '#c9cdd4' } } },
    yAxis: { type: 'value', splitLine: { lineStyle: { color: '#f2f3f5' } } },
    series: [{
      name, type: 'line', smooth: true, data, showSymbol: false,
      lineStyle: { color, width: 2 },
      areaStyle: { color, opacity: 0.08 }
    }]
  };
}

function init() {
  if (!opsRef.value) return;
  opsChart = echarts.init(opsRef.value);
  hitChart = echarts.init(hitRef.value!);
  docChart = echarts.init(docRef.value!);
  connChart = echarts.init(connRef.value!);
  opsChart.setOption(baseOption(ops, 'Ops', '#1f2329'));
  hitChart.setOption(baseOption(hit, 'Hit %', '#0fc6c2'));
  docChart.setOption(baseOption(docs, 'Docs', '#7b61ff'));
  connChart.setOption(baseOption(conns, 'Conns', '#f7ba1e'));
}

async function tick() {
  try {
    const r = await delta.stats();
    const d = r.data;
    if (labels.length > 30) { labels.shift(); ops.shift(); hit.shift(); docs.shift(); conns.shift(); }
    labels.push(new Date().toLocaleTimeString());
    ops.push((d.cache?.hits || 0) + (d.cache?.misses || 0));
    hit.push(((d.cache?.hit_rate || 0) * 100));
    docs.push(d.storage?.total_keys || 0);
    conns.push(d.connections?.total || 0);
    opsChart && opsChart.setOption({ xAxis: { data: labels }, series: [{ data: ops }] });
    hitChart && hitChart.setOption({ xAxis: { data: labels }, series: [{ data: hit }] });
    docChart && docChart.setOption({ xAxis: { data: labels }, series: [{ data: docs }] });
    connChart && connChart.setOption({ xAxis: { data: labels }, series: [{ data: conns }] });
  } catch {}
}

window.addEventListener('resize', () => {
  opsChart?.resize(); hitChart?.resize(); docChart?.resize(); connChart?.resize();
});

onMounted(() => { init(); tick(); timer = setInterval(tick, 5000); });
onUnmounted(() => {
  clearInterval(timer);
  opsChart?.dispose(); hitChart?.dispose(); docChart?.dispose(); connChart?.dispose();
});
</script>
