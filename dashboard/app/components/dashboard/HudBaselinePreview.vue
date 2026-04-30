<script setup lang="ts">
import { computed } from 'vue'

import { BarChart, LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

import type { BaselineLpPreview } from '~/types/control-plane'
import { buildBaselineForecastChartOption, buildBaselineScheduleChartOption } from '~/utils/dashboardChartTheme'

use([CanvasRenderer, LineChart, BarChart, GridComponent, TooltipComponent, LegendComponent])

const props = defineProps<{
  baselinePreview: BaselineLpPreview | null
  isLoading: boolean
  lastLoadedLabel: string
}>()

const forecastOption = computed(() => buildBaselineForecastChartOption(props.baselinePreview))
const scheduleOption = computed(() => buildBaselineScheduleChartOption(props.baselinePreview))

const economicsItems = computed(() => {
  if (!props.baselinePreview) {
    return [
      { label: 'Gross value', value: 'Waiting' },
      { label: 'Degradation', value: 'Waiting' },
      { label: 'Net value', value: 'Waiting' },
      { label: 'Throughput', value: 'Waiting' }
    ]
  }

  const economics = props.baselinePreview.economics

  return [
    { label: 'Gross value', value: `${Math.round(economics.total_gross_market_value_uah).toLocaleString('en-GB')} UAH` },
    { label: 'Degradation', value: `${Math.round(economics.total_degradation_penalty_uah).toLocaleString('en-GB')} UAH` },
    { label: 'Net value', value: `${Math.round(economics.total_net_value_uah).toLocaleString('en-GB')} UAH` },
    { label: 'Throughput', value: `${economics.total_throughput_mwh.toFixed(2)} MWh` }
  ]
})
</script>

<template>
  <section class="baseline-slab">
    <div class="baseline-slab__header">
      <div>
        <p class="baseline-slab__eyebrow">Slice 2 preview</p>
        <h3 class="baseline-slab__title">Baseline LP recommendation surface</h3>
      </div>

      <div class="baseline-slab__meta-block">
        <p class="baseline-slab__meta">Updated {{ lastLoadedLabel }}</p>
        <p class="baseline-slab__meta baseline-slab__meta-soft">Recommendation preview only, not bid intent</p>
      </div>
    </div>

    <div class="baseline-slab__economics">
      <article v-for="item in economicsItems" :key="item.label" class="economics-pill">
        <p class="economics-pill__label">{{ item.label }}</p>
        <p class="economics-pill__value">{{ item.value }}</p>
      </article>
    </div>

    <div class="baseline-slab__grid">
      <section class="baseline-card baseline-card-forecast">
        <div class="baseline-card__header">
          <div>
            <p class="baseline-card__eyebrow">Forecast horizon</p>
            <h4 class="baseline-card__title">Hourly DAM baseline forecast</h4>
          </div>
        </div>

        <div v-if="isLoading" class="baseline-chart baseline-chart-fallback">Loading baseline forecast...</div>
        <VChart v-else :option="forecastOption" autoresize class="baseline-chart" />
      </section>

      <section class="baseline-card baseline-card-balance">
        <div class="baseline-card__header">
          <div>
            <p class="baseline-card__eyebrow">Feasible plan</p>
            <h4 class="baseline-card__title">Signed MW schedule and projected SOC</h4>
          </div>
        </div>

        <div v-if="isLoading" class="baseline-chart baseline-chart-fallback">Loading projected state...</div>
        <VChart v-else :option="scheduleOption" autoresize class="baseline-chart" />
      </section>
    </div>

    <div class="baseline-boundary">
      <p class="baseline-boundary__title">Planning boundary</p>
      <p class="baseline-boundary__copy">
        This surface shows a feasible hourly recommendation derived from the baseline LP and constrained battery state.
        It is for operator review and demo planning, not market-order or dispatch semantics.
      </p>
    </div>
  </section>
</template>

<style scoped>
.baseline-slab {
  display: grid;
  gap: 1rem;
  padding: 1.15rem;
  border-radius: 1.7rem;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.9), rgba(240, 249, 255, 0.92)),
    linear-gradient(135deg, rgba(0, 121, 193, 0.05), rgba(126, 211, 33, 0.05));
  border: 2px solid rgba(255, 255, 255, 0.92);
}

.baseline-slab__header,
.baseline-card__header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 0.85rem;
}

.baseline-slab__eyebrow,
.baseline-card__eyebrow,
.economics-pill__label,
.baseline-boundary__title {
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.baseline-slab__title,
.baseline-card__title {
  margin-top: 0.35rem;
  color: var(--ink-strong);
  line-height: 1.05;
}

.baseline-slab__title {
  font-size: 1.55rem;
}

.baseline-card__title {
  font-size: 1.08rem;
}

.baseline-slab__meta-block {
  display: grid;
  gap: 0.15rem;
  text-align: right;
}

.baseline-slab__meta {
  font-size: 0.84rem;
  color: var(--ink-strong);
}

.baseline-slab__meta-soft {
  color: var(--ink-soft);
}

.baseline-slab__economics,
.baseline-slab__grid {
  display: grid;
  gap: 0.9rem;
}

.economics-pill,
.baseline-card,
.baseline-boundary {
  display: grid;
  gap: 0.35rem;
  padding: 0.95rem;
  border-radius: 1.35rem;
  background: rgba(255, 255, 255, 0.78);
  border: 1px solid rgba(0, 121, 193, 0.08);
}

.economics-pill__value {
  font-size: 1.15rem;
  font-weight: 800;
  color: var(--sims-blue-deep);
}

.baseline-chart {
  min-height: 15rem;
}

.baseline-chart-fallback {
  display: flex;
  align-items: center;
  justify-content: center;
  border: 2px dashed rgba(0, 121, 193, 0.16);
  border-radius: 1.2rem;
  color: var(--ink-soft);
}

.baseline-boundary__copy {
  line-height: 1.65;
  color: var(--ink-soft);
}

@media (min-width: 860px) {
  .baseline-slab__economics {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }

  .baseline-slab__grid {
    grid-template-columns: minmax(0, 1.1fr) minmax(0, 1fr);
  }
}
</style>