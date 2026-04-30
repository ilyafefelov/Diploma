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
      {
        label: 'Gross value',
        value: 'Waiting',
        tooltipTitle: 'Gross market value',
        tooltipBody: 'Projected market revenue before degradation cost is applied.',
        tooltipFormula: 'Calculated by summing hourly market value across the recommendation schedule.'
      },
      {
        label: 'Degradation',
        value: 'Waiting',
        tooltipTitle: 'Degradation penalty',
        tooltipBody: 'Estimated battery wear cost from moving energy through the pack.',
        tooltipFormula: 'Calculated from simulated throughput and the configured cost per full cycle.'
      },
      {
        label: 'Net value',
        value: 'Waiting',
        tooltipTitle: 'Net plan value',
        tooltipBody: 'Projected economic outcome after subtracting battery wear from gross market value.',
        tooltipFormula: 'Gross value minus degradation penalty.'
      },
      {
        label: 'Throughput',
        value: 'Waiting',
        tooltipTitle: 'Battery throughput',
        tooltipBody: 'Total energy expected to pass through the battery during the feasible plan.',
        tooltipFormula: 'Sum of hourly charge and discharge energy handled by the projected state model.'
      }
    ]
  }

  const economics = props.baselinePreview.economics

  return [
    {
      label: 'Gross value',
      value: `${Math.round(economics.total_gross_market_value_uah).toLocaleString('en-GB')} UAH`,
      tooltipTitle: 'Gross market value',
      tooltipBody: 'This is the projected market-facing revenue from the baseline LP schedule before battery wear is charged against it.',
      tooltipFormula: 'Built by summing the hourly gross market value of every scheduled recommendation point.'
    },
    {
      label: 'Degradation',
      value: `${Math.round(economics.total_degradation_penalty_uah).toLocaleString('en-GB')} UAH`,
      tooltipTitle: 'Degradation penalty',
      tooltipBody: 'This is the expected battery wear cost caused by executing the feasible plan through the projected battery model.',
      tooltipFormula: 'Built from total simulated throughput and the configured degradation cost per equivalent full cycle.'
    },
    {
      label: 'Net value',
      value: `${Math.round(economics.total_net_value_uah).toLocaleString('en-GB')} UAH`,
      tooltipTitle: 'Net plan value',
      tooltipBody: 'This is the operator-facing value left after the battery wear penalty is deducted from gross market value.',
      tooltipFormula: 'Built as gross value minus degradation penalty across the full recommendation horizon.'
    },
    {
      label: 'Throughput',
      value: `${economics.total_throughput_mwh.toFixed(2)} MWh`,
      tooltipTitle: 'Battery throughput',
      tooltipBody: 'This is the total energy volume that the battery is expected to process while following the feasible plan.',
      tooltipFormula: 'Built by summing hourly charge and discharge energy from the projected state trace.'
    }
  ]
})

const feasiblePlanItems = computed(() => {
  if (!props.baselinePreview) {
    return [
      { label: 'Power corridor', value: 'Waiting', note: 'Signed dispatch envelope in MW.' },
      { label: 'SOC guardrails', value: 'Waiting', note: 'Projected battery band in %.' },
      { label: 'Planning grain', value: 'Waiting', note: 'Hourly review step for the preview.' }
    ]
  }

  const metrics = props.baselinePreview.battery_metrics

  return [
    {
      label: 'Power corridor',
      value: `-${metrics.max_power_mw.toFixed(1)} to +${metrics.max_power_mw.toFixed(1)} MW`,
      note: 'Negative values mean charging, positive values mean discharge.'
    },
    {
      label: 'SOC guardrails',
      value: `${Math.round(metrics.soc_min_fraction * 100)}% to ${Math.round(metrics.soc_max_fraction * 100)}%`,
      note: 'Projected state must stay inside the feasible battery window.'
    },
    {
      label: 'Planning grain',
      value: `${props.baselinePreview.interval_minutes} min`,
      note: 'Every recommendation point is one operator review bucket.'
    }
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
      <article
        v-for="item in economicsItems"
        :key="item.label"
        class="economics-pill economics-pill-interactive"
        tabindex="0"
      >
        <p class="economics-pill__label">{{ item.label }}</p>
        <p class="economics-pill__value">{{ item.value }}</p>

        <div class="sims-tooltip" role="tooltip">
          <p class="sims-tooltip__eyebrow">Metric explainer</p>
          <p class="sims-tooltip__title">{{ item.tooltipTitle }}</p>
          <p class="sims-tooltip__body">{{ item.tooltipBody }}</p>
          <p class="sims-tooltip__formula">{{ item.tooltipFormula }}</p>
        </div>
      </article>
    </div>

    <div class="baseline-feasible-strip">
      <article v-for="item in feasiblePlanItems" :key="item.label" class="feasible-pill">
        <p class="feasible-pill__label">{{ item.label }}</p>
        <p class="feasible-pill__value">{{ item.value }}</p>
        <p class="feasible-pill__note">{{ item.note }}</p>
      </article>
    </div>

    <div class="baseline-slab__grid">
      <section class="baseline-card baseline-card-forecast">
        <div class="baseline-card__header">
          <div>
            <p class="baseline-card__eyebrow">Forecast horizon</p>
            <h4 class="baseline-card__title">Hourly DAM baseline forecast</h4>
            <p class="baseline-card__summary">Y-axis values are quoted in <strong>UAH/MWh</strong>.</p>
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
            <p class="baseline-card__summary">
              Bars use signed <strong>MW</strong>; the pink line is projected <strong>SOC %</strong> after each feasible step.
            </p>
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
      <p class="baseline-boundary__copy baseline-boundary__copy-strong">
        Feasible plan means the preview already respects the visible power corridor, SOC guardrails, interval grain,
        and degradation-aware projected state.
      </p>
    </div>
  </section>
</template>

<style scoped>
.baseline-slab {
  position: relative;
  display: grid;
  gap: 1rem;
  padding: 1.15rem;
  border-radius: 1.7rem;
  background:
    radial-gradient(circle at top right, rgba(83, 178, 234, 0.16), transparent 28%),
    radial-gradient(circle at top left, rgba(126, 211, 33, 0.14), transparent 24%),
    radial-gradient(circle at bottom right, rgba(255, 255, 255, 0.4), transparent 22%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.9), rgba(240, 249, 255, 0.92)),
    linear-gradient(135deg, rgba(0, 121, 193, 0.05), rgba(126, 211, 33, 0.05));
  border: 2px solid rgba(255, 255, 255, 0.92);
  box-shadow: 0 24px 54px rgba(0, 121, 193, 0.08);
  overflow: hidden;
}

.baseline-slab::after {
  content: '';
  position: absolute;
  inset: 0;
  pointer-events: none;
  background: linear-gradient(115deg, rgba(255, 255, 255, 0) 24%, rgba(255, 255, 255, 0.22) 34%, rgba(255, 255, 255, 0) 44%);
  transform: translateX(-120%);
  animation: slab-sheen 8s ease-in-out infinite;
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
.baseline-feasible-strip,
.baseline-slab__grid {
  display: grid;
  gap: 0.9rem;
}

.economics-pill,
.feasible-pill,
.baseline-card,
.baseline-boundary {
  display: grid;
  gap: 0.35rem;
  padding: 0.95rem;
  border-radius: 1.35rem;
  background: rgba(255, 255, 255, 0.78);
  border: 1px solid rgba(0, 121, 193, 0.08);
  transition: transform 180ms ease, box-shadow 180ms ease, border-color 180ms ease;
}

.economics-pill-interactive {
  position: relative;
  cursor: help;
  isolation: isolate;
}

.economics-pill:hover,
.feasible-pill:hover,
.baseline-card:hover,
.baseline-boundary:hover {
  transform: translateY(-2px);
  border-color: rgba(0, 121, 193, 0.16);
  box-shadow: 0 16px 30px rgba(0, 121, 193, 0.08);
}

.economics-pill__value {
  font-size: 1.15rem;
  font-weight: 800;
  color: var(--sims-blue-deep);
}

.sims-tooltip {
  position: absolute;
  left: 0;
  right: auto;
  bottom: calc(100% + 0.8rem);
  z-index: 4;
  width: min(18rem, calc(100vw - 3rem));
  display: grid;
  gap: 0.35rem;
  padding: 0.9rem 1rem;
  border: 1px solid rgba(0, 121, 193, 0.16);
  border-radius: 1.15rem;
  background:
    radial-gradient(circle at top right, rgba(126, 211, 33, 0.14), transparent 28%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(236, 248, 255, 0.98));
  box-shadow:
    0 18px 44px rgba(0, 121, 193, 0.16),
    inset 0 1px 0 rgba(255, 255, 255, 0.92);
  opacity: 0;
  visibility: hidden;
  transform: translateY(0.35rem) scale(0.98);
  transform-origin: bottom left;
  transition: opacity 160ms ease, transform 160ms ease, visibility 160ms ease;
  pointer-events: none;
}

.sims-tooltip::after {
  content: '';
  position: absolute;
  left: 1.2rem;
  top: calc(100% - 0.08rem);
  width: 1rem;
  height: 1rem;
  background: linear-gradient(135deg, rgba(236, 248, 255, 0.98), rgba(255, 255, 255, 0.98));
  border-right: 1px solid rgba(0, 121, 193, 0.16);
  border-bottom: 1px solid rgba(0, 121, 193, 0.16);
  transform: rotate(45deg);
}

.economics-pill-interactive:hover .sims-tooltip,
.economics-pill-interactive:focus-visible .sims-tooltip {
  opacity: 1;
  visibility: visible;
  transform: translateY(0) scale(1);
}

.economics-pill-interactive:focus-visible {
  outline: none;
  border-color: rgba(0, 121, 193, 0.24);
  box-shadow: 0 0 0 4px rgba(83, 178, 234, 0.16);
}

.sims-tooltip__eyebrow {
  font-size: 0.68rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.sims-tooltip__title {
  font-size: 0.96rem;
  font-weight: 800;
  color: var(--ink-strong);
  line-height: 1.25;
}

.sims-tooltip__body,
.sims-tooltip__formula {
  font-size: 0.84rem;
  line-height: 1.5;
  color: var(--ink-soft);
}

.sims-tooltip__formula {
  color: var(--sims-blue-deep);
}

.feasible-pill {
  align-content: start;
  background:
    radial-gradient(circle at top right, rgba(126, 211, 33, 0.12), transparent 26%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.84), rgba(249, 252, 255, 0.92));
}

.feasible-pill__label {
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.feasible-pill__value {
  font-size: 1rem;
  font-weight: 800;
  color: var(--ink-strong);
}

.feasible-pill__note,
.baseline-card__summary {
  font-size: 0.88rem;
  line-height: 1.5;
  color: var(--ink-soft);
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

.baseline-boundary__copy-strong {
  color: var(--ink-strong);
  font-weight: 600;
}

@keyframes slab-sheen {
  0%, 100% {
    transform: translateX(-120%);
  }

  45%, 55% {
    transform: translateX(120%);
  }
}

@media (min-width: 860px) {
  .baseline-slab__economics {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }

  .baseline-feasible-strip {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .baseline-slab__grid {
    grid-template-columns: minmax(0, 1.1fr) minmax(0, 1fr);
  }
}
</style>