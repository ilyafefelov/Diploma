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
          <p class="signal-card__summary">
            Both lines use <strong>UAH/MWh</strong>, so weather uplift is readable against the baseline DAM price without
            guessing the units.
          </p>
        </div>

        <p class="signal-card__meta">Updated {{ lastLoadedLabel }}</p>
      </div>

      <div class="signal-card__guide">
        <span class="signal-guide-pill">Y-axis: UAH/MWh</span>
        <span class="signal-guide-pill">Time: tenant local buckets</span>
      </div>

      <div v-if="isLoading" class="signal-chart signal-chart-fallback">Loading market pulse...</div>
      <VChart v-else :option="marketOption" autoresize class="signal-chart" />
    </section>

    <section class="signal-card">
      <div class="signal-card__header">
        <div>
          <p class="signal-card__eyebrow">Dispatch balance</p>
          <h3 class="signal-card__title">Charge intent and regret preview</h3>
          <p class="signal-card__summary">
            Blue bars are signed battery power in <strong>MW</strong>. Pink line is preview regret in
            <strong>UAH</strong> for the same hour.
          </p>
        </div>

        <p class="signal-card__meta">API-backed preview</p>
      </div>

      <div class="signal-card__guide">
        <span class="signal-guide-pill signal-guide-pill-blue">Bars: MW</span>
        <span class="signal-guide-pill signal-guide-pill-berry">Line: UAH</span>
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
  gap: 0.9rem;
  padding: 1.05rem;
  border-radius: 1.5rem;
  background:
    radial-gradient(circle at top right, rgba(126, 211, 33, 0.14), transparent 34%),
    radial-gradient(circle at bottom left, rgba(83, 178, 234, 0.12), transparent 30%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.84), rgba(243, 250, 255, 0.84));
  border: 2px solid rgba(255, 255, 255, 0.92);
  box-shadow: 0 20px 45px rgba(0, 121, 193, 0.08);
  transition: transform 180ms ease, box-shadow 180ms ease;
}

.signal-card:hover {
  transform: translateY(-2px);
  box-shadow: 0 26px 52px rgba(0, 121, 193, 0.12);
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

.signal-card__summary {
  margin-top: 0.45rem;
  max-width: 38rem;
  font-size: 0.92rem;
  line-height: 1.55;
  color: var(--ink-soft);
}

.signal-card__meta {
  font-size: 0.82rem;
  color: var(--ink-soft);
}

.signal-card__guide {
  display: flex;
  flex-wrap: wrap;
  gap: 0.55rem;
}

.signal-guide-pill {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 0.45rem 0.75rem;
  background: rgba(126, 211, 33, 0.12);
  color: var(--ink-strong);
  font-size: 0.78rem;
  font-weight: 700;
}

.signal-guide-pill-blue {
  background: rgba(0, 121, 193, 0.12);
}

.signal-guide-pill-berry {
  background: rgba(255, 111, 174, 0.14);
}

.signal-chart {
  min-height: 16rem;
  padding: 0.2rem 0;
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