<script setup lang="ts">
import { computed } from 'vue'

import { BarChart, LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

import type {
  DecisionPolicyPreviewResponse,
  FutureStackPreviewResponse,
  OperatorRecommendationResponse
} from '~/types/control-plane'
import {
  formatForecastWindowLabel,
  sortFutureForecastSeries
} from '~/utils/operatorFutureStack'

use([CanvasRenderer, BarChart, LineChart, GridComponent, TooltipComponent, LegendComponent])

const props = defineProps<{
  futureStack: FutureStackPreviewResponse | null
  decisionPolicy: DecisionPolicyPreviewResponse | null
  operatorRecommendation: OperatorRecommendationResponse | null
  isLoading: boolean
}>()

const forecastSeries = computed(() => {
  const apiSeries = props.futureStack?.forecast_series?.length
    ? props.futureStack.forecast_series
    : props.operatorRecommendation?.forecast_model_series ?? []

  return sortFutureForecastSeries(
    apiSeries.filter(series => series.model_name.includes('nbeatsx') || series.model_name.includes('tft'))
  )
})

const forecastLabels = computed(() => {
  const firstSeries = forecastSeries.value[0]
  if (!firstSeries) {
    return []
  }

  return firstSeries.points.map(point => formatHour(point.interval_start))
})

const forecastOption = computed(() => ({
  animationDuration: 500,
  backgroundColor: 'transparent',
  tooltip: {
    trigger: 'axis',
    backgroundColor: 'rgba(0, 50, 104, 0.98)',
    borderColor: 'rgba(202, 249, 255, 0.9)',
    borderWidth: 2,
    textStyle: { color: '#f0fbff' },
    formatter: (params: Array<{ marker?: string, seriesName?: string, value?: number, axisValue?: string }>) => {
      const lines = params.map(item => `${item.marker || ''}${item.seriesName}: ${Math.round(item.value ?? 0).toLocaleString('en-GB')} UAH/MWh`)
      return [`<strong>${params[0]?.axisValue || 'hour'}</strong>`, ...lines, 'NBEATSx/TFT forecast evidence; not a bid.'].join('<br/>')
    }
  },
  legend: {
    top: 0,
    textStyle: { color: 'rgba(236, 250, 255, 0.88)', fontWeight: 800 }
  },
  grid: { left: 58, right: 36, top: 44, bottom: 42, containLabel: true },
  xAxis: {
    type: 'category',
    data: forecastLabels.value,
    axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
  },
  yAxis: {
    type: 'value',
    name: 'UAH/MWh',
    axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
  },
  series: forecastSeries.value.flatMap((series) => {
    const baseLine = {
      type: 'line',
      name: series.model_name,
      smooth: true,
      symbol: series.model_family === 'TFT' ? 'diamond' : 'circle',
      symbolSize: 7,
      lineStyle: {
        width: 3,
        color: series.model_family === 'TFT' ? '#ff6fae' : '#b8ff32'
      },
      itemStyle: { color: series.model_family === 'TFT' ? '#ff6fae' : '#b8ff32' },
      data: series.points.map(point => Math.round(point.p50_price_uah_mwh ?? point.forecast_price_uah_mwh))
    }

    if (series.model_family !== 'TFT') {
      return [baseLine]
    }

    return [
      {
        ...baseLine,
        name: `${series.model_name} p50`
      },
      {
        type: 'line',
        name: `${series.model_name} p10`,
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 1.5, color: 'rgba(255, 111, 174, 0.45)', type: 'dashed' },
        data: series.points.map(point => Math.round(point.p10_price_uah_mwh ?? point.forecast_price_uah_mwh))
      },
      {
        type: 'line',
        name: `${series.model_name} p90`,
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 1.5, color: 'rgba(255, 111, 174, 0.45)', type: 'dashed' },
        data: series.points.map(point => Math.round(point.p90_price_uah_mwh ?? point.forecast_price_uah_mwh))
      }
    ]
  })
}))

const policyRows = computed(() => props.decisionPolicy?.rows ?? [])
const valueGapRows = computed(() => props.operatorRecommendation?.value_gap_series ?? [])
const policyLabels = computed(() => {
  if (policyRows.value.length > 0) {
    return policyRows.value.map(row => formatHour(row.interval_start))
  }

  return valueGapRows.value.map(row => formatHour(row.interval_start))
})

const policyOption = computed(() => ({
  animationDuration: 500,
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
  grid: { left: 58, right: 44, top: 44, bottom: 42, containLabel: true },
  xAxis: {
    type: 'category',
    data: policyLabels.value,
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
      name: 'MW',
      axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
    }
  ],
  series: policyRows.value.length > 0
    ? [
        {
          type: 'line',
          name: 'DT value gap',
          smooth: true,
          data: policyRows.value.map(row => Math.round(row.value_gap_uah)),
          lineStyle: { width: 4, color: '#f5a623' },
          itemStyle: { color: '#f5a623' }
        },
        {
          type: 'bar',
          name: 'Projected action',
          yAxisIndex: 1,
          data: policyRows.value.map(row => Number(row.projected_net_power_mw.toFixed(3))),
          itemStyle: { color: 'rgba(83, 178, 234, 0.78)', borderRadius: [8, 8, 0, 0] }
        }
      ]
    : [
        {
          type: 'line',
          name: 'Visible value gap',
          smooth: true,
          data: valueGapRows.value.map(row => Math.round(row.value_gap_uah)),
          lineStyle: { width: 4, color: '#f5a623' },
          itemStyle: { color: '#f5a623' }
        }
      ]
}))

const statusCards = computed(() => [
  {
    label: 'Forecast head',
    value: props.futureStack?.selected_forecast_model || props.operatorRecommendation?.forecast_source || 'waiting',
    meta: 'NBEATSx/TFT graph source'
  },
  {
    label: 'DT preview',
    value: props.decisionPolicy?.policy_readiness || props.operatorRecommendation?.policy_readiness || 'not materialized',
    meta: props.decisionPolicy ? `${props.decisionPolicy.constraint_violation_count} safety violations` : 'policy endpoint optional'
  },
  {
    label: 'Policy mode',
    value: props.operatorRecommendation?.policy_mode || 'waiting',
    meta: props.operatorRecommendation?.selected_policy_id || 'no selected policy'
  },
  {
    label: 'Execution boundary',
    value: props.decisionPolicy?.market_execution_enabled ? 'market enabled' : 'preview only',
    meta: 'deterministic gatekeeper still required'
  }
])

const backendStatusItems = computed(() => Object.entries(props.futureStack?.backend_status ?? {}))
const forecastWindowLabel = computed(() => formatForecastWindowLabel(
  props.futureStack?.forecast_window_start,
  props.futureStack?.forecast_window_end
))

const formatHour = (timestamp: string): string => new Date(timestamp).toLocaleString('en-GB', {
  day: '2-digit',
  month: 'short',
  hour: '2-digit',
  minute: '2-digit'
})
</script>

<template>
  <section class="surface-panel operator-future-panel">
    <div class="console-heading">
      <div>
        <p class="eyebrow">
          Future stack / live read model
        </p>
        <h2 class="section-title">
          NBEATSx, TFT, and DT policy evidence
        </h2>
      </div>
      <UBadge
        class="status-badge"
        :label="isLoading ? 'Refreshing' : 'FastAPI live'"
        color="success"
        variant="soft"
      />
    </div>

    <div class="future-status-grid">
      <article
        v-for="card in statusCards"
        :key="card.label"
        class="future-status-card"
      >
        <span>{{ card.label }}</span>
        <strong>{{ card.value }}</strong>
        <small>{{ card.meta }}</small>
      </article>
    </div>

    <div class="future-chart-grid">
      <article class="future-chart-card">
        <div>
          <p class="decision-chart-card__eyebrow">
            Forecast stack
          </p>
          <h3>NBEATSx/TFT forecast paths</h3>
          <p>Prediction window: <strong>{{ forecastWindowLabel }}</strong>. Shows official model rows first; compact/calibrated rows remain fallback evidence.</p>
        </div>
        <ClientOnly>
          <VChart
            :option="forecastOption"
            autoresize
            class="future-chart"
          />
        </ClientOnly>
      </article>

      <article class="future-chart-card">
        <div>
          <p class="decision-chart-card__eyebrow">
            Policy value
          </p>
          <h3>DT action and value gap</h3>
          <p>Value gap is counterfactual lost value; action bars are projected through battery feasibility before display.</p>
        </div>
        <ClientOnly>
          <VChart
            :option="policyOption"
            autoresize
            class="future-chart"
          />
        </ClientOnly>
      </article>
    </div>

    <div class="future-explainer-grid">
      <article>
        <span>Forecast source</span>
        <p>
          Target production price inputs come from NBEATSx/TFT over price history, weather, calendar, market rules,
          grid-event context, and battery state features.
        </p>
      </article>
      <article>
        <span>Decision source</span>
        <p>
          DT preview consumes forecast state, SOC, economic context, and return target. The raw action is never trusted;
          it is projected and checked before the operator sees it.
        </p>
      </article>
      <article>
        <span>Backend status</span>
        <p>
          {{ backendStatusItems.length > 0 ? backendStatusItems.map(([name, status]) => `${name}: ${status}`).join(' / ') : 'Official backend status not loaded yet.' }}
        </p>
      </article>
    </div>
  </section>
</template>

<style scoped>
.operator-future-panel {
  display: grid;
  gap: 0.85rem;
  padding: 0.8rem;
  min-width: 0;
  overflow: visible;
}

.future-status-grid,
.future-chart-grid,
.future-explainer-grid {
  display: grid;
  gap: 0.65rem;
}

.future-status-grid {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.future-chart-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.future-explainer-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.future-status-card,
.future-chart-card,
.future-explainer-grid article {
  border: 1px solid rgba(255, 255, 255, 0.28);
  border-radius: 0.72rem;
  background:
    radial-gradient(circle at top right, rgba(184, 255, 50, 0.12), transparent 30%),
    linear-gradient(180deg, rgba(13, 151, 218, 0.74), rgba(6, 82, 147, 0.78));
  padding: 0.72rem;
}

.future-status-card {
  display: grid;
  gap: 0.28rem;
}

.future-status-card span,
.decision-chart-card__eyebrow,
.future-explainer-grid span {
  color: rgba(215, 255, 79, 0.84);
  font-size: 0.68rem;
  font-weight: 900;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.future-status-card strong {
  overflow-wrap: anywhere;
  color: #b8ff32;
  font-size: 1.06rem;
  line-height: 1.08;
}

.future-status-card small,
.future-chart-card p,
.future-explainer-grid p {
  color: rgba(229, 249, 255, 0.84);
  font-size: 0.78rem;
  font-weight: 720;
  line-height: 1.42;
}

.future-chart-card {
  display: grid;
  gap: 0.55rem;
  min-width: 0;
}

.future-chart-card h3 {
  margin: 0.15rem 0 0.2rem;
  color: white;
  font-size: 1rem;
  line-height: 1.2;
}

.future-chart {
  min-height: 18rem;
}

.future-explainer-grid article {
  display: grid;
  gap: 0.35rem;
}

@media (max-width: 1320px) {
  .future-status-grid,
  .future-chart-grid,
  .future-explainer-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .future-status-grid,
  .future-chart-grid,
  .future-explainer-grid {
    grid-template-columns: 1fr;
  }
}
</style>
