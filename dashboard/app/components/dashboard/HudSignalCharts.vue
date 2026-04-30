<script setup lang="ts">
import { computed } from 'vue'

import { BarChart, LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

import type { TenantSummary } from '~/types/control-plane'
import { buildDispatchBalanceChartOption, buildMarketPulseChartOption } from '~/utils/dashboardChartTheme'

use([CanvasRenderer, LineChart, BarChart, GridComponent, TooltipComponent, LegendComponent])

const props = defineProps<{
  tenants: TenantSummary[]
  selectedTenantId: string
}>()

const marketOption = computed(() => buildMarketPulseChartOption(props.tenants, props.selectedTenantId))
const dispatchOption = computed(() => buildDispatchBalanceChartOption(props.tenants, props.selectedTenantId))
</script>

<template>
  <div class="signal-grid">
    <section class="signal-card">
      <div class="signal-card__header">
        <div>
          <p class="signal-card__eyebrow">Market pulse</p>
          <h3 class="signal-card__title">Baseline price vs weather bias</h3>
        </div>
      </div>

      <VChart :option="marketOption" autoresize class="signal-chart" />
    </section>

    <section class="signal-card">
      <div class="signal-card__header">
        <div>
          <p class="signal-card__eyebrow">Dispatch balance</p>
          <h3 class="signal-card__title">Charge intent and regret preview</h3>
        </div>
      </div>

      <VChart :option="dispatchOption" autoresize class="signal-chart" />
    </section>
  </div>
</template>

<style scoped>
.signal-grid {
  display: grid;
  gap: 1rem;
}

.signal-card {
  display: grid;
  gap: 0.75rem;
  padding: 1rem;
  border-radius: 1.5rem;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.84), rgba(243, 250, 255, 0.84));
  border: 2px solid rgba(255, 255, 255, 0.92);
}

.signal-card__eyebrow {
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.signal-card__title {
  margin-top: 0.4rem;
  font-size: 1.15rem;
  line-height: 1.15;
  color: var(--ink-strong);
}

.signal-chart {
  min-height: 16rem;
}

@media (min-width: 960px) {
  .signal-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>