<script setup lang="ts">
import { computed, ref } from 'vue'

import ClientVChart from '~/components/dashboard/ClientVChart.vue'
import type {
  BaselineRecommendationPoint,
  DecisionPolicyPreviewResponse,
  FutureStackPreviewResponse,
  OperatorValueGapPointResponse,
  OperatorRecommendationResponse
} from '~/types/control-plane'
import {
  buildPolicyForecastContextPoints,
  buildRecommendationStrategySelectItems,
  buildStrategyReadinessItems,
  filterOfficialPolicyValueSeries,
  formatForecastQualityLabel,
  formatForecastWindowLabel,
  formatOperatorPolicyForecastContextLabel,
  formatPolicyForecastContextLabel,
  formatRuntimeAccelerationLabel,
  isChartSafeForecastSeries,
  sortFutureForecastSeries
} from '~/utils/operatorFutureStack'

const props = defineProps<{
  futureStack: FutureStackPreviewResponse | null
  decisionPolicy: DecisionPolicyPreviewResponse | null
  operatorRecommendation: OperatorRecommendationResponse | null
  selectedStrategyId: string
  isLoading: boolean
  activeErrorCount: number
}>()

const emit = defineEmits<{
  'update:selectedStrategyId': [value: string]
}>()

type PolicyValueMode = 'selected' | 'official'

interface SelectedRecommendationChartRow {
  label: string
  netPowerMw: number
  forecastPriceUahMwh: number
  valueGapUah: number
}

const policyValueMode = ref<PolicyValueMode>('selected')

const readModelBadgeLabel = computed(() => {
  if (props.isLoading) {
    return 'Refreshing'
  }

  return props.activeErrorCount > 0 ? `${props.activeErrorCount} read-model gap(s)` : 'FastAPI read model'
})

const forecastSeries = computed(() => {
  const apiSeries = props.futureStack?.forecast_series?.length
    ? props.futureStack.forecast_series
    : props.operatorRecommendation?.forecast_model_series ?? []

  return sortFutureForecastSeries(
    apiSeries
      .filter(series => series.model_name.includes('nbeatsx') || series.model_name.includes('tft'))
      .filter(isChartSafeForecastSeries)
  )
})

const forecastChartSeries = computed(() => forecastSeries.value.slice(0, 3))

const hiddenUnsafeForecastItems = computed(() => {
  const apiSeries = props.futureStack?.forecast_series?.length
    ? props.futureStack.forecast_series
    : props.operatorRecommendation?.forecast_model_series ?? []

  return apiSeries
    .filter(series => series.model_name.includes('nbeatsx') || series.model_name.includes('tft'))
    .filter(series => !isChartSafeForecastSeries(series))
    .map(series => ({
      modelName: series.model_name,
      label: formatForecastQualityLabel(series)
    }))
})

const forecastLabels = computed(() => {
  const firstSeries = forecastChartSeries.value[0]
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
      return [`<strong>${params[0]?.axisValue || 'hour'}</strong>`, ...lines, 'Price context only; selected strategy is shown in the schedule chart.'].join('<br/>')
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
  series: forecastChartSeries.value.map(series => ({
    type: 'line',
    name: formatForecastSeriesLabel(series.model_name),
    smooth: true,
    symbol: series.model_family === 'TFT' ? 'diamond' : 'circle',
    symbolSize: 7,
    lineStyle: {
      width: 3,
      color: series.model_family === 'TFT' ? '#ff6fae' : '#b8ff32'
    },
    itemStyle: { color: series.model_family === 'TFT' ? '#ff6fae' : '#b8ff32' },
    data: series.points.map(point => Math.round(point.p50_price_uah_mwh ?? point.forecast_price_uah_mwh))
  }))
}))

const policyRows = computed(() => props.decisionPolicy?.rows ?? [])
const recommendationScheduleRows = computed(() => props.operatorRecommendation?.recommendation_schedule ?? [])
const valueGapRows = computed(() => props.operatorRecommendation?.value_gap_series ?? [])
const officialPolicyForecastSeries = computed(() => filterOfficialPolicyValueSeries(forecastSeries.value))
const hasOfficialPolicyRows = computed(() => officialPolicyForecastSeries.value.length > 0)
const isOfficialPolicyMode = computed(() => policyValueMode.value === 'official' && hasOfficialPolicyRows.value)
const selectedStrategyKey = computed(() => props.operatorRecommendation?.selected_strategy_id || props.selectedStrategyId)
const selectedStrategyLabel = computed(() => {
  const selectedStrategyId = props.operatorRecommendation?.selected_strategy_id || props.selectedStrategyId
  const option = props.operatorRecommendation?.available_strategies.find(strategy => strategy.strategy_id === selectedStrategyId)
  return option?.label || selectedStrategyId || 'strict_similar_day'
})
const usesDecisionPolicyPreview = computed(() => selectedStrategyKey.value === 'decision_transformer' && policyRows.value.length > 0)
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
      meta: 'policy rows changed by feasibility layer'
    },
    {
      label: 'Mean value gap',
      value: `${Math.round(meanValueGapRatio * 100)}%`,
      meta: 'oracle-normalized counterfactual gap'
    }
  ]
})
const officialPolicyProjectionSummary = computed(() => officialPolicyForecastSeries.value.map(series => ({
  label: series.model_name,
  value: `${series.points.length} row${series.points.length === 1 ? '' : 's'}`,
  meta: series.out_of_dam_cap_rows > 0 ? `${series.out_of_dam_cap_rows} out-of-cap` : formatForecastQualityLabel(series)
})))
const selectedRecommendationChartRows = computed(() => buildSelectedRecommendationChartRows(
  recommendationScheduleRows.value,
  valueGapRows.value
))
const selectedRecommendationProjectionSummary = computed(() => {
  if (selectedRecommendationChartRows.value.length === 0) {
    return []
  }

  const nonIdleRows = selectedRecommendationChartRows.value.filter(row => Math.abs(row.netPowerMw) >= 0.005).length
  const meanValueGap = selectedRecommendationChartRows.value.reduce((total, row) => total + row.valueGapUah, 0) / selectedRecommendationChartRows.value.length
  return [
    {
      label: 'DAM schedule',
      value: `${nonIdleRows}/${selectedRecommendationChartRows.value.length}`,
      meta: 'non-idle delivery windows'
    },
    {
      label: 'Mean visible gap',
      value: `${Math.round(meanValueGap).toLocaleString('en-GB')} UAH`,
      meta: selectedStrategyLabel.value
    }
  ]
})
const policyChartSummary = computed(() => isOfficialPolicyMode.value
  ? officialPolicyProjectionSummary.value
  : usesDecisionPolicyPreview.value
    ? policyProjectionSummary.value
    : selectedRecommendationProjectionSummary.value)
const officialPolicyLabels = computed(() => officialPolicyForecastSeries.value[0]?.points.map(point => formatHour(point.interval_start)) ?? [])
const policyLabels = computed(() => {
  if (isOfficialPolicyMode.value) {
    return officialPolicyLabels.value
  }

  if (usesDecisionPolicyPreview.value) {
    return policyRows.value.map(row => formatHour(row.interval_start))
  }

  return selectedRecommendationChartRows.value.map(row => row.label)
})
const officialPolicyChartSeries = computed(() => officialPolicyForecastSeries.value.flatMap((series) => {
  const isTft = series.model_family === 'TFT'
  const color = isTft ? '#ff6fae' : '#b8ff32'
  const baseLine = {
    type: 'line',
    name: isTft ? `${series.model_name} p50` : series.model_name,
    smooth: true,
    symbol: isTft ? 'diamond' : 'circle',
    symbolSize: 7,
    lineStyle: { width: 3, color },
    itemStyle: { color },
    data: series.points.map(point => Math.round(point.p50_price_uah_mwh ?? point.forecast_price_uah_mwh))
  }

  const quantileLines = []
  if (series.points.some(point => point.p10_price_uah_mwh !== null)) {
    quantileLines.push({
      type: 'line',
      name: `${series.model_name} p10`,
      smooth: true,
      symbol: 'none',
      lineStyle: { width: 1.5, color: 'rgba(255, 111, 174, 0.45)', type: 'dashed' },
      data: series.points.map(point => roundOptionalPrice(point.p10_price_uah_mwh))
    })
  }
  if (series.points.some(point => point.p90_price_uah_mwh !== null)) {
    quantileLines.push({
      type: 'line',
      name: `${series.model_name} p90`,
      smooth: true,
      symbol: 'none',
      lineStyle: { width: 1.5, color: 'rgba(255, 111, 174, 0.45)', type: 'dashed' },
      data: series.points.map(point => roundOptionalPrice(point.p90_price_uah_mwh))
    })
  }

  return [baseLine, ...quantileLines]
}))
const policyChartTitle = computed(() => isOfficialPolicyMode.value
  ? 'Safe DAM forecast rows'
  : usesDecisionPolicyPreview.value
    ? 'DAM policy preview'
    : 'DAM delivery schedule review')
const policyChartDescription = computed(() => isOfficialPolicyMode.value
  ? 'Forecast-store rows that are inside DAM caps. Hidden raw out-of-cap rows remain diagnostics, not schedule inputs.'
  : usesDecisionPolicyPreview.value
    ? 'Counterfactual value gap and projected DAM delivery-hour action rows for the selected policy preview; review only, not live dispatch.'
    : 'Bars are proposed DAM delivery-hour charge/discharge review rows for the selected preview strategy. Lines show visible value gap and DAM price context used by the same LP feasibility layer. Review only: no live IDM bid or market submission.')

const selectedRecommendationChartSeries = computed(() => [
  {
    type: 'bar',
    name: 'Selected DAM net power',
    yAxisIndex: 1,
    data: selectedRecommendationChartRows.value.map(row => row.netPowerMw),
    itemStyle: { color: 'rgba(83, 178, 234, 0.8)', borderRadius: [8, 8, 0, 0] }
  },
  {
    type: 'line',
    name: 'Visible value gap',
    smooth: true,
    data: selectedRecommendationChartRows.value.map(row => row.valueGapUah),
    lineStyle: { width: 4, color: '#f5a623' },
    itemStyle: { color: '#f5a623' }
  },
  {
    type: 'line',
    name: 'DAM price context',
    smooth: true,
    data: selectedRecommendationChartRows.value.map(row => row.forecastPriceUahMwh),
    lineStyle: { width: 3, color: '#b8ff32', type: 'dashed' },
    itemStyle: { color: '#b8ff32' }
  }
])

const decisionPolicyChartSeries = computed(() => [
  {
    type: 'line',
    name: 'Policy value gap',
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
    name: 'NBEATSx state forecast',
    smooth: true,
    data: policyForecastContextRows.value.map(row => Math.round(row.nbeatsxForecastUahMwh)),
    lineStyle: { width: 2.5, color: '#b8ff32', type: 'dashed' },
    itemStyle: { color: '#b8ff32' }
  },
  {
    type: 'line',
    name: 'TFT state forecast',
    smooth: true,
    data: policyForecastContextRows.value.map(row => Math.round(row.tftForecastUahMwh)),
    lineStyle: { width: 2.5, color: '#ff6fae', type: 'dashed' },
    itemStyle: { color: '#ff6fae' }
  }
])

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
  yAxis: isOfficialPolicyMode.value
    ? {
        type: 'value',
        name: 'UAH/MWh',
        axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
      }
    : [
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
  series: isOfficialPolicyMode.value
    ? officialPolicyChartSeries.value
    : usesDecisionPolicyPreview.value
      ? decisionPolicyChartSeries.value
      : selectedRecommendationChartSeries.value
}))

const statusCards = computed(() => [
  {
    label: 'Headline result',
    value: 'V2+ offline',
    meta: '174.77 UAH mean regret / 4 of 4 rolling windows'
  },
  {
    label: 'Strategy preview',
    value: selectedStrategyLabel.value,
    meta: props.operatorRecommendation?.selection_reason || 'strict fallback/control'
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
    meta: 'read-model evidence; no live dispatch'
  }
])

const backendStatusItems = computed(() => Object.entries(props.futureStack?.backend_status ?? {}))
const decisionSourceFacts = computed(() => {
  const facts = [
    `Strategy: ${selectedStrategyLabel.value}`,
    `Forecast context: ${policyForecastContextLabel.value}`,
    'Boundary: review candidate only, no live IDM bid or market submission.'
  ]

  if (props.operatorRecommendation?.selection_reason) {
    facts.unshift(`Selection reason: ${props.operatorRecommendation.selection_reason}`)
  }

  if (props.operatorRecommendation?.policy_explanation) {
    facts.push(`Policy note: ${props.operatorRecommendation.policy_explanation}`)
  }

  if (props.decisionPolicy?.policy_value_interpretation) {
    facts.push(`Value note: ${props.decisionPolicy.policy_value_interpretation}`)
  }

  return facts
})
const backendStatusFacts = computed(() => {
  const items = backendStatusItems.value.length > 0
    ? backendStatusItems.value.map(([name, status]) => `${name}: ${status}`)
    : ['Official backend status not loaded yet.']

  items.push(`Runtime: ${formatRuntimeAccelerationLabel(props.futureStack?.runtime_acceleration)}.`)
  return items
})
const forecastWindowLabel = computed(() => formatForecastWindowLabel(
  props.futureStack?.forecast_window_start,
  props.futureStack?.forecast_window_end
))
const strategySelectItems = computed(() => buildRecommendationStrategySelectItems(
  props.operatorRecommendation?.available_strategies ?? []
))
const strategyReadinessItems = computed(() => buildStrategyReadinessItems(
  props.operatorRecommendation?.available_strategies ?? []
))

const updateSelectedStrategy = (value: string | number | boolean | Record<string, unknown>): void => {
  if (typeof value === 'string') {
    emit('update:selectedStrategyId', value)
    return
  }

  if (typeof value === 'object' && value !== null && typeof value.value === 'string') {
    emit('update:selectedStrategyId', value.value)
  }
}

const setPolicyValueMode = (mode: PolicyValueMode): void => {
  if (mode === 'official' && !hasOfficialPolicyRows.value) {
    return
  }

  policyValueMode.value = mode
}

function buildSelectedRecommendationChartRows(
  scheduleRows: BaselineRecommendationPoint[],
  gapRows: OperatorValueGapPointResponse[]
): SelectedRecommendationChartRow[] {
  return scheduleRows.map((row, index) => {
    const gapRow = gapRows[index]
    return {
      label: formatHour(row.interval_start),
      netPowerMw: Number(row.recommended_net_power_mw.toFixed(3)),
      forecastPriceUahMwh: Math.round(row.forecast_price_uah_mwh),
      valueGapUah: Math.round(gapRow?.value_gap_uah ?? 0)
    }
  })
}

function formatForecastSeriesLabel(modelName: string): string {
  if (modelName === 'nbeatsx_silver_v0') {
    return 'Compact NBEATSx'
  }
  if (modelName === 'tft_silver_v0') {
    return 'Compact TFT p50'
  }
  if (modelName === 'nbeatsx_official_v0') {
    return 'Official NBEATSx'
  }
  if (modelName === 'tft_official_v0') {
    return 'Official TFT p50'
  }
  return modelName
}

const roundOptionalPrice = (value: number | null): number | null => value === null ? null : Math.round(value)

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
          Forecast evidence / read model
        </p>
        <h2 class="section-title">
          V2+ schedule evidence, TFT portfolio, and strategy preview
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
          :label="readModelBadgeLabel"
          :color="activeErrorCount > 0 ? 'warning' : 'success'"
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
          <h3>Price-model context</h3>
          <p>DAM forecast window: <strong>{{ forecastWindowLabel }}</strong>. These are day-ahead forecast context lines only; the DAM delivery review schedule is shown in the policy chart and bottom dock.</p>
          <div class="forecast-quality-strip">
            <span
              v-for="item in forecastQualityItems"
              :key="item.modelName"
              :class="{ 'forecast-quality-strip__item--warn': item.needsCalibration }"
            >
              {{ item.modelName }}: {{ item.label }}
            </span>
            <span
              v-for="item in hiddenUnsafeForecastItems"
              :key="`hidden-${item.modelName}`"
              class="forecast-quality-strip__item--warn"
            >
              hidden {{ item.modelName }}: {{ item.label }}
            </span>
          </div>
        </div>
        <ClientOnly>
          <ClientVChart
            :option="forecastOption"
            autoresize
            class="future-chart"
          />
        </ClientOnly>
      </article>

      <article class="future-chart-card">
        <div class="policy-chart-heading">
          <div>
            <p class="decision-chart-card__eyebrow">
              Policy value
            </p>
            <h3>{{ policyChartTitle }}</h3>
            <p>{{ policyChartDescription }}</p>
          </div>
          <div
            class="policy-chart-toggle"
            role="group"
            aria-label="Policy value source"
          >
            <button
              type="button"
              :aria-pressed="!isOfficialPolicyMode"
              :class="{ 'policy-chart-toggle__button--active': !isOfficialPolicyMode }"
              @click="setPolicyValueMode('selected')"
            >
              <UIcon name="i-lucide-brain" />
              <span>DAM schedule</span>
            </button>
            <button
              type="button"
              :aria-pressed="isOfficialPolicyMode"
              :disabled="!hasOfficialPolicyRows"
              :class="{ 'policy-chart-toggle__button--active': isOfficialPolicyMode }"
              @click="setPolicyValueMode('official')"
            >
              <UIcon name="i-lucide-database" />
              <span>Forecast rows</span>
            </button>
          </div>
        </div>
        <div>
          <div
            v-if="policyChartSummary.length"
            class="forecast-quality-strip"
          >
            <span
              v-for="item in policyChartSummary"
              :key="item.label"
            >
              {{ item.label }}: {{ item.value }} / {{ item.meta }}
            </span>
          </div>
        </div>
        <ClientOnly>
          <ClientVChart
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
        <p class="future-explainer-lead">
          Headline evidence is Ukrainian-only V2+ schedule/value evidence.
        </p>
        <div class="future-explainer-facts">
          <span>The price chart is context, not a bidding command.</span>
          <span>Unsafe out-of-cap forecast rows are filtered out from schedule inputs.</span>
          <span>Selected policy rows stay in DAM delivery-hour review mode.</span>
        </div>
      </article>
      <article>
        <span>Decision source</span>
        <p class="future-explainer-lead">
          Selected strategy preview comes from the operator recommendation endpoint and is re-checked by the same
          battery feasibility LP layer.
        </p>
        <div class="future-explainer-facts">
          <span
            v-for="fact in decisionSourceFacts"
            :key="fact"
          >{{ fact }}</span>
        </div>
      </article>
      <article>
        <span>Backend status</span>
        <p class="future-explainer-lead">
          Current read-model and runtime health signals.
        </p>
        <div class="future-explainer-facts">
          <span
            v-for="item in backendStatusFacts"
            :key="item"
          >{{ item }}</span>
        </div>
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

.policy-chart-heading {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 0.7rem;
  min-width: 0;
}

.policy-chart-heading > div:first-child {
  min-width: 0;
}

.policy-chart-toggle {
  display: inline-flex;
  flex: 0 0 auto;
  overflow: hidden;
  border: 1px solid rgba(202, 249, 255, 0.34);
  border-radius: 0.48rem;
  background: rgba(4, 67, 119, 0.78);
}

.policy-chart-toggle button {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
  min-height: 1.85rem;
  border: 0;
  border-right: 1px solid rgba(202, 249, 255, 0.22);
  background: transparent;
  color: rgba(236, 250, 255, 0.9);
  cursor: pointer;
  font-size: 0.66rem;
  font-weight: 900;
  letter-spacing: 0;
  line-height: 1;
  padding: 0 0.55rem;
  white-space: nowrap;
}

.policy-chart-toggle button:last-child {
  border-right: 0;
}

.policy-chart-toggle button:disabled {
  cursor: not-allowed;
  opacity: 0.48;
}

.policy-chart-toggle__button--active {
  background: rgba(215, 255, 79, 0.22) !important;
  color: #eaff6b !important;
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
  height: 20rem;
  min-height: 20rem;
}

.future-explainer-grid article {
  display: grid;
  gap: 0.35rem;
}

.future-explainer-lead {
  margin: 0;
}

.future-explainer-facts {
  display: flex;
  flex-wrap: wrap;
  gap: 0.36rem;
}

.future-explainer-facts span {
  display: inline-flex;
  align-items: center;
  border: 1px solid rgba(202, 249, 255, 0.3);
  border-radius: 999px;
  background: rgba(4, 67, 119, 0.66);
  color: rgba(236, 250, 255, 0.9);
  padding: 0.2rem 0.45rem;
  font-size: 0.67rem;
  font-weight: 820;
  line-height: 1.35;
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

  .policy-chart-heading {
    flex-direction: column;
  }
}
</style>
