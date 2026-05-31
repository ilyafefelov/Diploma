<script setup lang="ts">
import { computed } from 'vue'

import ClientVChart from '~/components/dashboard/ClientVChart.vue'
import type { TenantSummary } from '~/types/control-plane'
import { buildTenantRegistryChartOption } from '~/utils/dashboardChartTheme'

const props = defineProps<{
  tenants: TenantSummary[]
  selectedTenantId: string
}>()

const option = computed(() => {
  return buildTenantRegistryChartOption(props.tenants, props.selectedTenantId)
})
</script>

<template>
  <ClientVChart
    :option="option"
    autoresize
    class="registry-chart"
  />
</template>

<style scoped>
.registry-chart {
  width: 100%;
  min-height: 30rem;
  border: 1px solid color-mix(in oklab, var(--panel-strong) 72%, transparent);
  border-radius: 0.78rem;
  background:
    radial-gradient(circle at top right, color-mix(in oklab, var(--plumbob-green) 9%, transparent), transparent 24%),
    linear-gradient(
      180deg,
      color-mix(in oklab, var(--canvas-top) 94%, var(--accent-cyan) 6%),
      color-mix(in oklab, var(--canvas-base) 88%, var(--accent-cyan) 12%)
    );
  box-shadow:
    inset 0 1px 0 color-mix(in oklab, var(--panel-strong) 92%, transparent),
    0 12px 24px color-mix(in oklab, var(--accent-cyan-strong) 14%, transparent);
}
</style>
