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
  min-height: 25rem;
  border: 3px solid rgba(255, 255, 255, 0.94);
  border-radius: 2rem;
  background:
    radial-gradient(circle at top right, rgba(126, 211, 33, 0.08), transparent 24%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.92), rgba(245, 251, 255, 0.9));
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.92);
}
</style>
