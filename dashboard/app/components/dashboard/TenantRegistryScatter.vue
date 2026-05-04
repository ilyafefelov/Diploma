<script setup lang="ts">
import { computed } from 'vue'

import { ScatterChart } from 'echarts/charts'
import { GridComponent, TooltipComponent } from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

import type { TenantSummary } from '~/types/control-plane'
import { buildTenantRegistryChartOption } from '~/utils/dashboardChartTheme'

use([CanvasRenderer, ScatterChart, GridComponent, TooltipComponent])

const props = defineProps<{
  tenants: TenantSummary[]
  selectedTenantId: string
}>()

const option = computed(() => {
  return buildTenantRegistryChartOption(props.tenants, props.selectedTenantId)
})
</script>

<template>
  <VChart
    :option="option"
    autoresize
    class="registry-chart"
  />
</template>

<style scoped>
.registry-chart {
  width: 100%;
  min-height: 30rem;
  border: 1px solid rgba(255, 255, 255, 0.66);
  border-radius: 0.78rem;
  background:
    radial-gradient(circle at top right, rgba(126, 211, 33, 0.08), transparent 24%),
    linear-gradient(180deg, rgba(222, 245, 255, 0.94), rgba(191, 229, 250, 0.9));
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.86),
    0 12px 24px rgba(0, 48, 95, 0.12);
}
</style>
