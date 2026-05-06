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
  buildPolicyForecastContextPoints,
  buildStrategyReadinessItems,
  buildStrategySelectItems,
  formatForecastQualityLabel,
  formatForecastWindowLabel,
  formatOperatorPolicyForecastContextLabel,
  formatPolicyForecastContextLabel,
  formatRuntimeAccelerationLabel,
  sortFutureForecastSeries
} from '~/utils/operatorFutureStack'

use([CanvasRenderer, BarChart, LineChart, GridComponent, TooltipComponent, LegendComponent])

const props = defineProps<{
  futureStack: FutureStackPreviewResponse | null
  decisionPolicy: DecisionPolicyPreviewResponse | null
  operatorRecommendation: OperatorRecommendationResponse | null
  selectedStrategyId: string
  isLoading: boolean
}>()

const emit = defineEmits<{
  'update:selectedStrategyId': [value: string]
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

const forecastQualityItems = computed(() => forecastSeries.value.map(series => ({
  modelName: series.model_name,
  label: formatForecastQualityLabel(series),
  needsCalibration: series.out_of_dam_cap_rows > 0
})))

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
const policyForecastContextRows = computed(() => buildPolicyForecastContextPoints(policyRows.value))
const policyForecastContextLabel = computed(() => props.decisionPolicy
  ? formatPolicyForecastContextLabel(props.decisionPolicy)
  : formatOperatorPolicyForecastContextLabel(props.operatorRecommendation))
const policyProjectionSummary = computed(() => {
  if (policyRows.value.length === 0) {
    return []
  }

  const projectedRows = policyRows.value.filter(row => row.projection_status !== 'accepted_without_projection').length
  const meanValueGapRatio = policyRows.value.reduce((total, row) => total + (row.value_gap_ratio ?? 0), 0) / policyRows.value.length

  return [
    {
      label: 'Safety projection',
      value: `${projectedRows}/${policyRows.value.length}`,
      meta: 'DT rows changed by feasibility layer'
    },
    {
      label: 'Mean value gap',
      value: `${Math.round(meanValueGapRatio * 100)}%`,
      meta: 'oracle-normalized counterfactual gap'
    }
  ]
})
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
      name: 'UAH / UAH/MWh',
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
        },
        {
          type: 'line',
          name: 'State NBEATSx forecast',
          smooth: true,
          data: policyForecastContextRows.value.map(row => Math.round(row.nbeatsxForecastUahMwh)),
          lineStyle: { width: 2.5, color: '#b8ff32', type: 'dashed' },
          itemStyle: { color: '#b8ff32' }
        },
        {
          type: 'line',
          name: 'State TFT forecast',
          smooth: true,
          data: policyForecastContextRows.value.map(row => Math.round(row.tftForecastUahMwh)),
          lineStyle: { width: 2.5, color: '#ff6fae', type: 'dashed' },
          itemStyle: { color: '#ff6fae' }
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
    meta: props.decisionPolicy
      ? `${props.decisionPolicy.constraint_violation_count} safety violations / ${policyForecastContextLabel.value}`
      : policyForecastContextLabel.value
  },
  {
    label: 'Policy mode',
    value: props.operatorRecommendation?.policy_mode || 'waiting',
    meta: props.operatorRecommendation?.selected_policy_id || 'no selected policy'
  },
  {
    label: 'Runtime',
    value: formatRuntimeAccelerationLabel(props.futureStack?.runtime_acceleration),
    meta: props.futureStack?.runtime_acceleration?.recommended_scope || 'training runtime not reported'
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
const strategySelectItems = computed(() => buildStrategySelectItems(
  props.operatorRecommendation?.available_strategies ?? []
))
const strategyReadinessItems = computed(() => buildStrategyReadinessItems(
  props.operatorRecommendation?.available_strategies ?? []
))

const updateSelectedStrategy = (value: string | number | boolean | Record<string, unknown>): void => {
  if (typeof value === 'string') {
    emit('update:selectedStrategyId', value)
  }
}

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
      <div class="future-control-stack">
        <label>
          <span>Strategy preview</span>
          <USelect
            class="future-strategy-select"
            :model-value="selectedStrategyId"
            :items="strategySelectItems"
            value-key="value"
            label-key="label"
            color="info"
            variant="none"
            @update:model-value="updateSelectedStrategy"
          />
        </label>
        <UBadge
          class="status-badge"
          :label="isLoading ? 'Refreshing' : 'FastAPI live'"
          color="success"
          variant="soft"
        />
      </div>
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

    <div
      v-if="strategyReadinessItems.length"
      class="strategy-readiness-strip"
    >
      <article
        v-for="item in strategyReadinessItems"
        :key="item.strategyId"
        :class="{ 'strategy-readiness-strip__item--blocked': item.status === 'blocked' }"
      >
        <span>{{ item.label }}</span>
        <strong>{{ item.status }}</strong>
        <small>{{ item.reason }}</small>
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
          <div class="forecast-quality-strip">
            <span
              v-for="item in forecastQualityItems"
              :key="item.modelName"
              :class="{ 'forecast-quality-strip__item--warn': item.needsCalibration }"
            >
              {{ item.modelName }}: {{ item.label }}
            </span>
          </div>
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
          <p>Value gap is counterfactual lost value; action bars are projected through battery feasibility. Dashed forecast lines show the NBEATSx/TFT state seen by the DT preview.</p>
          <div
            v-if="policyProjectionSummary.length"
            class="forecast-quality-strip"
          >
            <span
              v-for="item in policyProjectionSummary"
              :key="item.label"
            >
              {{ item.label }}: {{ item.value }} / {{ item.meta }}
            </span>
          </div>
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
          Forecast context: {{ policyForecastContextLabel }}.
          {{ decisionPolicy?.policy_state_features?.length ? `State features: ${decisionPolicy.policy_state_features.join(', ')}.` : '' }}
          {{ decisionPolicy?.policy_value_interpretation || '' }}
        </p>
      </article>
      <article>
        <span>Backend status</span>
        <p>
          {{ backendStatusItems.length > 0 ? backendStatusItems.map(([name, status]) => `${name}: ${status}`).join(' / ') : 'Official backend status not loaded yet.' }}
          Runtime: {{ formatRuntimeAccelerationLabel(futureStack?.runtime_acceleration) }}.
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
.strategy-readiness-strip,
.future-chart-grid,
.future-explainer-grid {
  display: grid;
  gap: 0.65rem;
}

.future-status-grid {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.strategy-readiness-strip {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.future-control-stack {
  display: flex;
  align-items: flex-end;
  justify-content: flex-end;
  gap: 0.55rem;
  min-width: min(100%, 26rem);
}

.future-control-stack label {
  display: grid;
  gap: 0.22rem;
  min-width: 18rem;
}

.future-control-stack span {
  color: rgba(215, 255, 79, 0.84);
  font-size: 0.64rem;
  font-weight: 900;
  text-transform: uppercase;
}

.future-strategy-select {
  min-height: 2.4rem;
  border: 1px solid rgba(202, 249, 255, 0.34);
  border-radius: 0.55rem;
  background: rgba(4, 67, 119, 0.84);
}

.future-chart-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.future-explainer-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.future-status-card,
.strategy-readiness-strip article,
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

.strategy-readiness-strip article {
  display: grid;
  gap: 0.18rem;
  min-width: 0;
}

.strategy-readiness-strip__item--blocked {
  border-color: rgba(255, 191, 82, 0.66) !important;
  background:
    radial-gradient(circle at top right, rgba(255, 191, 82, 0.18), transparent 30%),
    linear-gradient(180deg, rgba(183, 100, 17, 0.78), rgba(119, 65, 9, 0.82)) !important;
}

.future-status-card span,
.strategy-readiness-strip span,
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

.strategy-readiness-strip strong {
  overflow-wrap: anywhere;
  color: #f2fbff;
  font-size: 1rem;
  line-height: 1.08;
  text-transform: capitalize;
}

.future-status-card small,
.strategy-readiness-strip small,
.future-chart-card p,
.future-explainer-grid p {
  color: rgba(229, 249, 255, 0.84);
  font-size: 0.78rem;
  font-weight: 720;
  line-height: 1.42;
}

.strategy-readiness-strip small {
  overflow-wrap: anywhere;
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

.forecast-quality-strip {
  display: flex;
  flex-wrap: wrap;
  gap: 0.38rem;
  margin-top: 0.45rem;
}

.forecast-quality-strip span {
  border: 1px solid rgba(202, 249, 255, 0.34);
  border-radius: 999px;
  background: rgba(4, 67, 119, 0.74);
  color: rgba(236, 250, 255, 0.9);
  padding: 0.22rem 0.48rem;
  font-size: 0.68rem;
  font-weight: 900;
}

.forecast-quality-strip__item--warn {
  border-color: rgba(255, 191, 82, 0.72) !important;
  background: rgba(151, 82, 8, 0.74) !important;
  color: #fff0c7 !important;
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
  .strategy-readiness-strip,
  .future-chart-grid,
  .future-explainer-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .future-control-stack {
    width: 100%;
    flex-direction: column;
    align-items: stretch;
  }

  .future-control-stack label {
    min-width: 0;
  }

  .future-status-grid,
  .strategy-readiness-strip,
  .future-chart-grid,
  .future-explainer-grid {
    grid-template-columns: 1fr;
  }
}
</style>
