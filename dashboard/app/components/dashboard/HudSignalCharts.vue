<script setup lang="ts">
import { computed } from 'vue'

import { BarChart, LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

import type { SignalPreview } from '~/types/control-plane'
import { buildDispatchBalanceChartOption, buildMarketPulseChartOption } from '~/utils/dashboardChartTheme'

use([CanvasRenderer, LineChart, BarChart, GridComponent, TooltipComponent, LegendComponent])

const props = defineProps<{
  signalPreview: SignalPreview | null
  isLoading: boolean
  lastLoadedLabel: string
}>()

const marketOption = computed(() => buildMarketPulseChartOption(props.signalPreview))
const dispatchOption = computed(() => buildDispatchBalanceChartOption(props.signalPreview))
</script>

<template>
  <div class="signal-grid">
    <section class="signal-card">
      <div class="signal-card__header">
        <div>
          <p class="signal-card__eyebrow">Market pulse</p>
          <h3 class="signal-card__title">Baseline price vs weather bias</h3>
        </div>

        <p class="signal-card__meta">Updated {{ lastLoadedLabel }}</p>
      </div>

      <div v-if="isLoading" class="signal-chart signal-chart-fallback">Loading market pulse...</div>
      <VChart v-else :option="marketOption" autoresize class="signal-chart" />
    </section>

    <section class="signal-card">
      <div class="signal-card__header">
        <div>
          <p class="signal-card__eyebrow">Dispatch balance</p>
          <h3 class="signal-card__title">Charge intent and regret preview</h3>
        </div>

        <p class="signal-card__meta">API-backed preview</p>
      </div>

      <div v-if="isLoading" class="signal-chart signal-chart-fallback">Loading dispatch preview...</div>
      <VChart v-else :option="dispatchOption" autoresize class="signal-chart" />
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

.signal-card__meta {
  font-size: 0.82rem;
  color: var(--ink-soft);
}

.signal-chart {
  min-height: 16rem;
}

.signal-chart-fallback {
  display: flex;
  align-items: center;
  justify-content: center;
  border: 2px dashed rgba(0, 121, 193, 0.16);
  border-radius: 1.25rem;
  color: var(--ink-soft);
}

@media (min-width: 960px) {
  .signal-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>