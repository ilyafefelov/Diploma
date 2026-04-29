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
  <VChart :option="option" autoresize class="registry-chart" />
</template>

<style scoped>
.registry-chart {
  width: 100%;
  min-height: 25rem;
  border: 1px solid rgba(21, 36, 61, 0.1);
  border-radius: 1.5rem;
  background: rgba(255, 253, 249, 0.54);
}
</style>