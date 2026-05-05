<script setup lang="ts">
import { computed } from 'vue'

import { BarChart, LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

import type {
  BaselineLpPreview,
  DashboardBatteryStateResponse,
  DashboardExogenousSignalsResponse,
  ForecastDispatchSensitivityResponse,
  RealDataBenchmarkResponse
} from '~/types/control-plane'
import type { DefenseModelRow } from '~/utils/defenseDataset'
import {
  buildControlRegretTimeline,
  buildOperatorDecisionReadinessItems,
  buildOperatorDecisionStateCards,
  buildOperatorStrategyEvidenceRows,
  buildSensitivityEvidenceRows
} from '~/utils/operatorDecisionEvidence'

use([CanvasRenderer, BarChart, LineChart, GridComponent, TooltipComponent, LegendComponent])

const props = defineProps<{
  benchmark: RealDataBenchmarkResponse | null
  modelRows: DefenseModelRow[]
  sensitivity: ForecastDispatchSensitivityResponse | null
  batteryState: DashboardBatteryStateResponse | null
  baselinePreview: BaselineLpPreview | null
  exogenousSignals: DashboardExogenousSignalsResponse | null
  isLoading: boolean
}>()

const strategyRows = computed(() => buildOperatorStrategyEvidenceRows(props.modelRows))
const controlTimeline = computed(() => buildControlRegretTimeline(props.benchmark, 24))
const sensitivityRows = computed(() => buildSensitivityEvidenceRows(props.sensitivity))
const stateCards = computed(() => buildOperatorDecisionStateCards({
  batteryState: props.batteryState,
  baselinePreview: props.baselinePreview,
  exogenousSignals: props.exogenousSignals,
  modelRows: props.modelRows
}))
const readinessItems = computed(() => buildOperatorDecisionReadinessItems({
  batteryState: props.batteryState,
  baselinePreview: props.baselinePreview,
  exogenousSignals: props.exogenousSignals
}))

const strategyOption = computed(() => ({
  animationDuration: 650,
  backgroundColor: 'transparent',
  tooltip: {
    trigger: 'axis',
    backgroundColor: 'rgba(0, 50, 104, 0.98)',
    borderColor: 'rgba(202, 249, 255, 0.9)',
    borderWidth: 2,
    textStyle: { color: '#f0fbff' },
    formatter: (params: Array<{ axisValueLabel?: string, marker?: string, seriesName?: string, value?: number }>) => {
      const modelName = params[0]?.axisValueLabel || 'model'
      const rows = params.map(item => `${item.marker || ''}${item.seriesName}: ${item.value}${item.seriesName === 'Win rate' ? '%' : ' UAH'}`)
      return [`<strong>${modelName}</strong>`, ...rows, 'Mean regret = lost value vs oracle. Win rate = share of anchors ranked best.'].join('<br/>')
    }
  },
  legend: {
    top: 0,
    textStyle: { color: 'rgba(236, 250, 255, 0.88)', fontWeight: 800 }
  },
  grid: { left: 54, right: 46, top: 42, bottom: 44, containLabel: true },
  xAxis: {
    type: 'category',
    data: strategyRows.value.map(row => row.modelName),
    axisLabel: {
      color: 'rgba(219, 245, 255, 0.9)',
      fontWeight: 800,
      interval: 0,
      rotate: 18
    }
  },
  yAxis: [
    {
      type: 'value',
      name: 'UAH',
      axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
    },
    {
      type: 'value',
      name: 'win %',
      min: 0,
      max: 100,
      axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
    }
  ],
  series: [
    {
      type: 'bar',
      name: 'Mean regret',
      data: strategyRows.value.map(row => Math.round(row.meanRegretUah)),
      itemStyle: { color: '#53b2ea', borderRadius: [8, 8, 0, 0] }
    },
    {
      type: 'line',
      name: 'Win rate',
      yAxisIndex: 1,
      data: strategyRows.value.map(row => Math.round(row.winRate * 100)),
      symbol: 'diamond',
      symbolSize: 8,
      lineStyle: { width: 3, color: '#b8ff32' },
      itemStyle: { color: '#b8ff32' }
    }
  ]
}))

const regretTimelineOption = computed(() => ({
  animationDuration: 650,
  backgroundColor: 'transparent',
  tooltip: {
    trigger: 'axis',
    backgroundColor: 'rgba(0, 50, 104, 0.98)',
    borderColor: 'rgba(202, 249, 255, 0.9)',
    borderWidth: 2,
    textStyle: { color: '#f0fbff' }
  },
  legend: {
    top: 0,
    textStyle: { color: 'rgba(236, 250, 255, 0.88)', fontWeight: 800 }
  },
  grid: { left: 54, right: 44, top: 42, bottom: 34, containLabel: true },
  xAxis: {
    type: 'category',
    data: controlTimeline.value.map(point => point.anchorLabel),
    axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
  },
  yAxis: [
    {
      type: 'value',
      name: 'UAH',
      axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
    },
    {
      type: 'value',
      name: 'MWh',
      axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
    }
  ],
  series: [
    {
      type: 'line',
      name: 'Control regret',
      smooth: true,
      data: controlTimeline.value.map(point => Math.round(point.regretUah)),
      symbol: 'circle',
      symbolSize: 7,
      lineStyle: { width: 4, color: '#ff6fae' },
      itemStyle: { color: '#ff6fae' }
    },
    {
      type: 'bar',
      name: 'Throughput',
      yAxisIndex: 1,
      data: controlTimeline.value.map(point => Number(point.throughputMwh.toFixed(3))),
      itemStyle: { color: 'rgba(184, 255, 50, 0.58)', borderRadius: [8, 8, 0, 0] }
    }
  ]
}))

const sensitivityOption = computed(() => ({
  animationDuration: 650,
  backgroundColor: 'transparent',
  tooltip: {
    trigger: 'axis',
    backgroundColor: 'rgba(0, 50, 104, 0.98)',
    borderColor: 'rgba(202, 249, 255, 0.9)',
    borderWidth: 2,
    textStyle: { color: '#f0fbff' }
  },
  legend: {
    top: 0,
    textStyle: { color: 'rgba(236, 250, 255, 0.88)', fontWeight: 800 }
  },
  grid: { left: 54, right: 44, top: 42, bottom: 44, containLabel: true },
  xAxis: {
    type: 'category',
    data: sensitivityRows.value.map(row => row.bucket),
    axisLabel: {
      color: 'rgba(219, 245, 255, 0.9)',
      fontWeight: 800,
      interval: 0,
      rotate: 12
    }
  },
  yAxis: [
    {
      type: 'value',
      name: 'UAH',
      axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
    },
    {
      type: 'value',
      name: 'rows',
      axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
    }
  ],
  series: [
    {
      type: 'bar',
      name: 'Mean regret',
      data: sensitivityRows.value.map(row => Math.round(row.meanRegretUah)),
      itemStyle: { color: '#f5a623', borderRadius: [8, 8, 0, 0] }
    },
    {
      type: 'line',
      name: 'Rows',
      yAxisIndex: 1,
      data: sensitivityRows.value.map(row => row.rows),
      symbol: 'diamond',
      symbolSize: 8,
      lineStyle: { width: 3, color: '#b8ff32' },
      itemStyle: { color: '#b8ff32' }
    }
  ]
}))
</script>

<template>
  <section class="surface-panel operator-decision-panel">
    <div class="console-heading">
      <div>
        <p class="eyebrow">
          Decision evidence
        </p>
        <h2 class="section-title">
          Control, regret, and operating state
        </h2>
      </div>
      <UBadge
        class="status-badge"
        :label="isLoading ? 'Refreshing' : 'FastAPI live'"
        color="success"
        variant="soft"
      />
    </div>

    <div class="decision-state-grid">
      <article
        v-for="card in stateCards"
        :key="card.label"
        class="decision-state-card"
        tabindex="0"
      >
        <span>{{ card.label }}</span>
        <strong>{{ card.value }}</strong>
        <small>{{ card.meta }}</small>
        <span
          class="decision-state-tooltip"
          role="tooltip"
        >
          <strong>{{ card.tooltipTitle }}</strong>
          <span>{{ card.tooltipBody }}</span>
          <em>{{ card.tooltipFormula }}</em>
        </span>
      </article>
    </div>

    <div class="decision-readiness-strip">
      <article
        v-for="item in readinessItems"
        :key="item.label"
        class="decision-readiness-card"
        :class="`decision-readiness-card--${item.tone}`"
      >
        <span>{{ item.label }}</span>
        <strong>{{ item.status }}</strong>
        <small>{{ item.detail }}</small>
      </article>
    </div>

    <div class="decision-chart-grid">
      <article class="decision-chart-card">
        <div>
          <p class="decision-chart-card__eyebrow">
            Comparator graph
          </p>
          <h3>Mean regret and win rate</h3>
          <p>Lower regret is better. Win rate means rank-1 share within returned benchmark rows.</p>
        </div>
        <div class="decision-chart-guide">
          <span><strong>Model</strong> = forecast/control candidate name</span>
          <span><strong>Mean regret</strong> = average lost UAH versus oracle</span>
          <span><strong>Win rate</strong> = share of anchors ranked best by regret</span>
        </div>
        <ClientOnly>
          <VChart
            :option="strategyOption"
            autoresize
            class="decision-chart"
          />
        </ClientOnly>
      </article>

      <article class="decision-chart-card">
        <div>
          <p class="decision-chart-card__eyebrow">
            Control graph
          </p>
          <h3>Strict control regret rate</h3>
          <p>Rolling anchor view of the default comparator against the oracle upper bound.</p>
        </div>
        <ClientOnly>
          <VChart
            :option="regretTimelineOption"
            autoresize
            class="decision-chart"
          />
        </ClientOnly>
      </article>

      <article class="decision-chart-card decision-chart-card-wide">
        <div>
          <p class="decision-chart-card__eyebrow">
            Diagnosis graph
          </p>
          <h3>Why value is lost</h3>
          <p>Forecast error, spread mismatch, and LP sensitivity buckets explain the regret source.</p>
        </div>
        <ClientOnly>
          <VChart
            :option="sensitivityOption"
            autoresize
            class="decision-chart decision-chart-compact"
          />
        </ClientOnly>
      </article>
    </div>

    <div class="decision-explainer-grid">
      <article>
        <span>Gatekeeper meaning</span>
        <p>
          Current BUY/SELL/HOLD scores are operator previews. Future Bid Gatekeeper validates Proposed Bids before
          market submission; final dispatch still requires Battery Telemetry safety checks.
        </p>
      </article>
      <article>
        <span>Weather slice meaning</span>
        <p>
          Prepare builds the Dagster run config. Materialize refreshes selected Bronze/Silver sources. Including DAM
          history lets the weather slice join price context for forecast features.
        </p>
      </article>
      <article>
        <span>Business use</span>
        <p>
          Operator should compare physical readiness, expected plan value, regret evidence, and grid risk before treating
          any schedule as a candidate dispatch review.
        </p>
      </article>
    </div>
  </section>
</template>

<style scoped>
.operator-decision-panel {
  display: grid;
  gap: 0.85rem;
  padding: 0.8rem;
  min-width: 0;
  overflow: visible;
}

.decision-state-grid,
.decision-readiness-strip,
.decision-chart-grid,
.decision-explainer-grid {
  display: grid;
  gap: 0.65rem;
}

.decision-state-grid {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.decision-readiness-strip {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.decision-state-card,
.decision-readiness-card,
.decision-chart-card,
.decision-explainer-grid article {
  border: 1px solid rgba(255, 255, 255, 0.28);
  border-radius: 0.72rem;
  background:
    radial-gradient(circle at top right, rgba(126, 211, 33, 0.12), transparent 30%),
    linear-gradient(180deg, rgba(13, 151, 218, 0.74), rgba(6, 82, 147, 0.78));
  padding: 0.72rem;
}

.decision-state-card {
  position: relative;
  display: grid;
  gap: 0.3rem;
  overflow: visible;
}

.decision-readiness-card {
  display: grid;
  gap: 0.25rem;
  min-height: 4.8rem;
}

.decision-readiness-card--green {
  background: linear-gradient(180deg, rgba(52, 164, 28, 0.82), rgba(22, 101, 34, 0.84));
}

.decision-readiness-card--orange {
  background: linear-gradient(180deg, rgba(236, 134, 14, 0.86), rgba(166, 74, 5, 0.86));
}

.decision-readiness-card--red {
  background: linear-gradient(180deg, rgba(210, 60, 68, 0.86), rgba(129, 22, 38, 0.88));
}

.decision-readiness-card--blue {
  background: linear-gradient(180deg, rgba(13, 151, 218, 0.74), rgba(6, 82, 147, 0.78));
}

.decision-state-card span,
.decision-readiness-card span,
.decision-chart-card__eyebrow,
.decision-explainer-grid span {
  color: rgba(215, 255, 79, 0.84);
  font-size: 0.68rem;
  font-weight: 900;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.decision-state-card strong {
  overflow-wrap: anywhere;
  color: #b8ff32;
  font-size: 1.22rem;
  line-height: 1.08;
}

.decision-readiness-card strong {
  color: white;
  font-size: 1rem;
  font-weight: 900;
  line-height: 1.08;
  text-transform: uppercase;
}

.decision-state-card small,
.decision-readiness-card small,
.decision-chart-card p,
.decision-explainer-grid p {
  color: rgba(229, 249, 255, 0.82);
  font-size: 0.78rem;
  font-weight: 700;
  line-height: 1.45;
}

.decision-state-tooltip {
  position: absolute;
  left: 0.45rem;
  bottom: calc(100% + 0.42rem);
  z-index: 170;
  display: grid;
  width: min(20rem, calc(100vw - 2rem));
  gap: 0.26rem;
  border: 1px solid rgba(202, 249, 255, 0.9);
  border-radius: 0.72rem;
  background: linear-gradient(180deg, rgba(0, 129, 204, 0.98), rgba(0, 56, 112, 0.98));
  padding: 0.62rem 0.7rem;
  color: rgba(238, 250, 255, 0.92);
  box-shadow: 0 18px 32px rgba(0, 39, 82, 0.32);
  opacity: 0;
  pointer-events: none;
  transform: translateY(0.24rem) scale(0.97);
  transition: opacity 150ms ease, transform 150ms ease;
}

.decision-state-tooltip strong {
  color: #d7ff4f;
  font-size: 0.74rem;
  font-weight: 900;
}

.decision-state-tooltip span,
.decision-state-tooltip em {
  color: rgba(238, 250, 255, 0.88);
  font-size: 0.68rem;
  font-style: normal;
  font-weight: 700;
  line-height: 1.34;
}

.decision-state-tooltip em {
  color: #d7ff4f;
}

.decision-state-card:hover .decision-state-tooltip,
.decision-state-card:focus-visible .decision-state-tooltip {
  opacity: 1;
  transform: translateY(0) scale(1);
}

.decision-chart-guide {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 0.35rem;
}

.decision-chart-guide span {
  border: 1px solid rgba(202, 249, 255, 0.24);
  border-radius: 0.55rem;
  background: rgba(0, 61, 119, 0.28);
  padding: 0.42rem 0.5rem;
  color: rgba(229, 249, 255, 0.86);
  font-size: 0.68rem;
  font-weight: 750;
  line-height: 1.3;
}

.decision-chart-guide strong {
  color: #d7ff4f;
}

.decision-chart-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.decision-chart-card {
  display: grid;
  gap: 0.55rem;
  min-width: 0;
}

.decision-chart-card h3 {
  margin: 0.15rem 0 0.2rem;
  color: white;
  font-size: 1rem;
  line-height: 1.2;
}

.decision-chart-card-wide {
  grid-column: 1 / -1;
}

.decision-chart {
  min-height: 18rem;
}

.decision-chart-compact {
  min-height: 15rem;
}

.decision-explainer-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.decision-explainer-grid article {
  display: grid;
  gap: 0.35rem;
}

@media (max-width: 1320px) {
  .decision-state-grid,
  .decision-readiness-strip,
  .decision-chart-grid,
  .decision-explainer-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .decision-state-grid,
  .decision-readiness-strip,
  .decision-chart-grid,
  .decision-explainer-grid {
    grid-template-columns: 1fr;
  }
}
</style>
