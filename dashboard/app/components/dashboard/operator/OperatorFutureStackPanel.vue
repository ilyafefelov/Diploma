<script setup lang="ts">
import { computed, ref } from 'vue'

import ClientVChart from '~/components/dashboard/ClientVChart.vue'
import type {
  AcademicMvpReadinessResponse,
  BaselineRecommendationPoint,
  DecisionPolicyPreviewResponse,
  FutureStackPreviewResponse,
  OperatorRecommendationResponse,
  OperatorValueGapPointResponse,
  ShadowRecommendationPreviewResponse
} from '~/types/control-plane'
import {
  buildStrategyComparisonRows,
  previewSourceDisplayLabel,
  type OperatorPreviewSourceId,
  type StrategyComparisonRow
} from '~/utils/operatorShadowPreview'
import {
  buildAcademicMvpGatePassportItems,
  buildRecommendationInputSignalRows,
  buildPolicyForecastContextPoints,
  buildStrategyReadinessItems,
  buildV13ReadinessItems,
  filterOfficialPolicyValueSeries,
  formatForecastQualityLabel,
  formatForecastWindowLabel,
  formatOperatorPolicyForecastContextLabel,
  formatPolicyForecastContextLabel,
  formatRuntimeAccelerationLabel,
  isChartSafeForecastSeries,
  selectOperatorForecastChartSource,
  sortFutureForecastSeries
} from '~/utils/operatorFutureStack'

const props = defineProps<{
  futureStack: FutureStackPreviewResponse | null
  decisionPolicy: DecisionPolicyPreviewResponse | null
  operatorRecommendation: OperatorRecommendationResponse | null
  bestValidRecommendation: OperatorRecommendationResponse | null
  shadowPreview: ShadowRecommendationPreviewResponse | null
  shadowComparisonPreviews: ShadowRecommendationPreviewResponse[]
  academicMvpReadiness: AcademicMvpReadinessResponse | null
  selectedStrategyId: string
  selectedPreviewSourceId: OperatorPreviewSourceId
  isLoading: boolean
  shadowPreviewLastLoadedLabel: string
  activeErrorCount: number
}>()

const emit = defineEmits<{
  'update:selectedStrategyId': [value: string]
  'update:selectedPreviewSourceId': [value: OperatorPreviewSourceId]
  'refresh:shadowPreview': []
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

const forecastChartSource = computed(() => selectOperatorForecastChartSource({
  futureStack: props.futureStack,
  operatorRecommendation: props.operatorRecommendation
}))

const forecastSeries = computed(() => {
  return sortFutureForecastSeries(
    forecastChartSource.value.series
      .filter(series => series.model_name.includes('nbeatsx') || series.model_name.includes('tft'))
      .filter(isChartSafeForecastSeries)
  )
})

const forecastChartSeries = computed(() => forecastSeries.value.slice(0, 3))

const hiddenUnsafeForecastItems = computed(() => {
  if (hasRecommendationInputSignalRows.value) {
    return []
  }

  return forecastChartSource.value.series
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

const recommendationInputSignalRows = computed(() => buildRecommendationInputSignalRows(
  recommendationScheduleRows.value,
  props.operatorRecommendation?.soc_projection ?? [],
  props.operatorRecommendation?.load_forecast ?? []
))

const hasRecommendationInputSignalRows = computed(() => recommendationInputSignalRows.value.length > 0)

const recommendationInputSummaryItems = computed(() => {
  if (!hasRecommendationInputSignalRows.value) {
    return forecastQualityItems.value
  }

  const modelFallbackCount = (props.operatorRecommendation?.forecast_model_series ?? [])
    .filter(series => series.source_status.includes('compact_fallback')).length
  const items = [
    {
      modelName: 'price_source',
      label: `price source: ${props.operatorRecommendation?.forecast_source || 'operator recommendation'}`,
      needsCalibration: false
    },
    {
      modelName: 'schedule_source',
      label: `schedule source: ${selectedStrategyLabel.value}`,
      needsCalibration: false
    },
    {
      modelName: 'soc_source',
      label: `SOC source: ${props.operatorRecommendation?.soc_source || 'not reported'}`,
      needsCalibration: props.operatorRecommendation?.soc_source !== 'telemetry'
    },
    {
      modelName: 'rows',
      label: `${recommendationInputSignalRows.value.length} delivery-hour rows`,
      needsCalibration: false
    }
  ]

  if (modelFallbackCount > 0) {
    items.push({
      modelName: 'compact_fallback_note',
      label: 'compact NBEATSx/TFT rows are source context, not independent model proof',
      needsCalibration: true
    })
  }

  return items
})

const recommendationInputChartSeries = computed(() => {
  const rows = recommendationInputSignalRows.value
  const series: Array<Record<string, unknown>> = [
    {
      type: 'line',
      name: 'Recommendation price context (UAH/MWh)',
      smooth: true,
      data: rows.map(row => row.forecastPriceUahMwh),
      lineStyle: { width: 3, color: '#b8ff32' },
      itemStyle: { color: '#b8ff32' }
    },
    {
      type: 'bar',
      name: 'Selected battery net power (MW)',
      yAxisIndex: 1,
      data: rows.map(row => row.selectedNetPowerMw),
      itemStyle: { color: 'rgba(83, 178, 234, 0.78)', borderRadius: [7, 7, 0, 0] }
    },
    {
      type: 'line',
      name: 'Projected SOC (%)',
      yAxisIndex: 2,
      smooth: true,
      data: rows.map(row => row.projectedSocPercent),
      lineStyle: { width: 3, color: '#ff6fae' },
      itemStyle: { color: '#ff6fae' }
    }
  ]

  if (rows.some(row => row.siteNetLoadMw != null)) {
    series.push({
      type: 'line',
      name: 'Site net load estimate (MW)',
      yAxisIndex: 1,
      smooth: true,
      data: rows.map(row => row.siteNetLoadMw),
      lineStyle: { width: 2.5, color: '#f5a623', type: 'dashed' },
      itemStyle: { color: '#f5a623' }
    })
  }

  return series
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
      const lines = params.map((item) => {
        const value = hasRecommendationInputSignalRows.value
          ? formatInputSignalTooltipValue(item.seriesName, item.value)
          : `${Math.round(item.value ?? 0).toLocaleString('en-GB')} UAH/MWh`
        return `${item.marker || ''}${item.seriesName}: ${value}`
      })
      const footer = hasRecommendationInputSignalRows.value
        ? 'These are the selected recommendation inputs: price context, battery action, SOC path, and site-load estimate where available.'
        : 'Forecast context only; selected strategy is shown in the schedule chart.'
      return [`<strong>${params[0]?.axisValue || 'hour'}</strong>`, ...lines, footer].join('<br/>')
    }
  },
  legend: {
    top: 0,
    textStyle: { color: 'rgba(236, 250, 255, 0.88)', fontWeight: 800 }
  },
  grid: {
    left: 58,
    right: hasRecommendationInputSignalRows.value ? 78 : 36,
    top: 44,
    bottom: 42,
    containLabel: true
  },
  xAxis: {
    type: 'category',
    data: hasRecommendationInputSignalRows.value
      ? recommendationInputSignalRows.value.map(row => row.label)
      : forecastLabels.value,
    axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
  },
  yAxis: hasRecommendationInputSignalRows.value
    ? [
        {
          type: 'value',
          name: 'UAH/MWh',
          axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
        },
        {
          type: 'value',
          name: 'MW',
          axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
        },
        {
          type: 'value',
          name: 'SOC %',
          offset: 46,
          min: 0,
          max: 100,
          axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
        }
      ]
    : {
        type: 'value',
        name: 'UAH/MWh',
        axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
      },
  series: hasRecommendationInputSignalRows.value
    ? recommendationInputChartSeries.value
    : forecastChartSeries.value.map(series => ({
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
const forecastWindowLabel = computed(() => formatForecastWindowLabel(
  forecastChartSource.value.windowStart,
  forecastChartSource.value.windowEnd
))
const isShadowRecommendationMode = computed(() => props.selectedPreviewSourceId !== 'best_valid')
const selectedScheduleWindowLabel = computed(() => formatForecastWindowLabel(
  props.operatorRecommendation?.target_delivery_window_start,
  props.operatorRecommendation?.target_delivery_window_end
))
const shadowPreviewLabel = computed(() => previewSourceDisplayLabel(
  props.selectedPreviewSourceId,
  props.shadowPreview?.preview_source_label
))
const forecastChartTitle = computed(() => {
  if (hasRecommendationInputSignalRows.value) {
    return 'Recommendation input signals'
  }
  if (forecastChartSource.value.kind === 'operator_delivery_day') {
    return 'Delivery-day price context'
  }
  if (forecastChartSource.value.kind === 'future_stack_context') {
    return 'Live forecast context'
  }
  return 'Price context pending'
})
const forecastStackDescription = computed(() => {
  if (hasRecommendationInputSignalRows.value) {
    return `Shows the selected delivery-day recommendation inputs for ${selectedScheduleWindowLabel.value}: price context, selected battery net power, projected SOC, and configured site-load estimate where available. Compact fallback NBEATSx/TFT rows are not plotted as independent model evidence.`
  }

  if (forecastChartSource.value.kind === 'operator_delivery_day') {
    return `Delivery-day price/model window: ${forecastWindowLabel.value}. This is the same DAM horizon used by the selected schedule chart and bottom dock.`
  }

  if (isShadowRecommendationMode.value) {
    return `Current forecast context remains the live read-model window: ${forecastWindowLabel.value}. The selected ${shadowPreviewLabel.value} action pattern is projected onto ${selectedScheduleWindowLabel.value} for diagnostic delivery-day preview.`
  }

  return `DAM forecast window: ${forecastWindowLabel.value}. These are day-ahead forecast context lines only; the DAM delivery review schedule is shown in the policy chart and bottom dock.`
})
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
  const meanStrictShortfall = selectedRecommendationChartRows.value.reduce((total, row) => total + row.valueGapUah, 0) / selectedRecommendationChartRows.value.length
  return [
    {
      label: isShadowRecommendationMode.value ? 'Shadow preview' : 'DAM schedule',
      value: `${nonIdleRows}/${selectedRecommendationChartRows.value.length}`,
      meta: isShadowRecommendationMode.value ? 'projected diagnostic windows' : 'non-idle delivery windows'
    },
    {
      label: 'Mean shortfall vs strict',
      value: `${Math.round(meanStrictShortfall).toLocaleString('en-GB')} UAH`,
      meta: `${selectedStrategyLabel.value}; strict LP/reference value`
    }
  ]
})
const selectedRecommendationGuideItems = computed(() => {
  if (isOfficialPolicyMode.value || usesDecisionPolicyPreview.value) {
    return []
  }

  return [
    {
      label: isShadowRecommendationMode.value ? 'Blue bars' : 'Blue bars',
      detail: isShadowRecommendationMode.value
        ? 'selected shadow schedule MW; positive is discharge/sell, negative is charge/buy'
        : 'selected best-valid schedule MW; positive is discharge/sell, negative is charge/buy'
    },
    {
      label: 'Orange line',
      detail: 'value shortfall vs strict LP/reference = max(0, strict value - selected value), UAH'
    },
    {
      label: 'Green line',
      detail: isShadowRecommendationMode.value
        ? 'artifact forecast price context, UAH/MWh; not a value metric'
        : 'DAM forecast price context, UAH/MWh; not a value metric'
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
    : isShadowRecommendationMode.value
      ? `${shadowPreviewLabel.value} projected schedule preview`
      : 'DAM delivery schedule review')
const policyChartDescription = computed(() => isOfficialPolicyMode.value
  ? 'Forecast-store rows that are inside DAM caps. Hidden raw out-of-cap rows remain diagnostics, not schedule inputs.'
  : usesDecisionPolicyPreview.value
    ? 'Counterfactual value gap and projected DAM delivery-hour action rows for the selected policy preview; review only, not live dispatch.'
    : isShadowRecommendationMode.value
      ? `Bars show the manually selected ${shadowPreviewLabel.value} action pattern projected onto ${selectedScheduleWindowLabel.value}. The orange line is value shortfall versus strict LP/reference, not promotion and not market execution.`
      : 'Bars are proposed DAM delivery-hour charge/discharge review rows for the selected preview strategy. The orange line is value shortfall versus strict LP/reference; the green line is DAM price context. Review only: no live IDM bid or market submission.')

const selectedNetPowerSeriesLabel = computed(() => isShadowRecommendationMode.value ? 'Selected shadow net power (MW)' : 'Selected DAM net power (MW)')
const selectedValueGapSeriesLabel = computed(() => 'Value shortfall vs strict (UAH)')
const selectedPriceContextSeriesLabel = computed(() => isShadowRecommendationMode.value ? 'Artifact forecast price (UAH/MWh)' : 'DAM forecast price (UAH/MWh)')
const selectedRecommendationChartSeries = computed(() => [
  {
    type: 'bar',
    name: selectedNetPowerSeriesLabel.value,
    yAxisIndex: 1,
    data: selectedRecommendationChartRows.value.map(row => row.netPowerMw),
    itemStyle: { color: 'rgba(83, 178, 234, 0.8)', borderRadius: [8, 8, 0, 0] }
  },
  {
    type: 'line',
    name: selectedValueGapSeriesLabel.value,
    smooth: true,
    data: selectedRecommendationChartRows.value.map(row => row.valueGapUah),
    lineStyle: { width: 4, color: '#f5a623' },
    itemStyle: { color: '#f5a623' }
  },
  {
    type: 'line',
    name: selectedPriceContextSeriesLabel.value,
    smooth: true,
    data: selectedRecommendationChartRows.value.map(row => row.forecastPriceUahMwh),
    lineStyle: { width: 3, color: '#b8ff32', type: 'dashed' },
    itemStyle: { color: '#b8ff32' }
  }
])
const strategyComparisonRows = computed(() => buildStrategyComparisonRows(
  props.bestValidRecommendation,
  props.shadowComparisonPreviews
))
const strategyComparisonLabels = computed(() => strategyComparisonRows.value.map(row => shortStrategyLabel(row)))
const strategyComparisonSummary = computed(() => strategyComparisonRows.value.map(row => ({
  label: row.label,
  value: row.isBlocked
    ? 'blocked'
    : `${formatEnergy(row.totalChargeMwh)} charge / ${formatEnergy(row.totalDischargeMwh)} discharge`,
  meta: `${row.status}; ${row.scheduleRows} row${row.scheduleRows === 1 ? '' : 's'}; ${formatOptionalUah(row.meanRegretVsStrictUah)} vs strict`
})))
const strategyComparisonOption = computed(() => ({
  animationDuration: 500,
  backgroundColor: 'transparent',
  tooltip: {
    trigger: 'axis',
    backgroundColor: 'rgba(0, 50, 104, 0.98)',
    borderColor: 'rgba(202, 249, 255, 0.9)',
    borderWidth: 2,
    textStyle: { color: '#f0fbff' },
    formatter: (params: Array<{ marker?: string, seriesName?: string, value?: number | null, axisValue?: string }>) => {
      const lines = params.map((item) => {
        const value = item.value == null
          ? 'n/a'
          : item.seriesName?.includes('regret')
            ? `${Math.round(item.value).toLocaleString('en-GB')} UAH`
            : `${Number(item.value).toFixed(2)} MWh`
        return `${item.marker || ''}${item.seriesName}: ${value}`
      })
      return [`<strong>${params[0]?.axisValue || 'strategy'}</strong>`, ...lines, 'All entries are preview-only; market execution remains false.'].join('<br/>')
    }
  },
  legend: {
    top: 0,
    textStyle: { color: 'rgba(236, 250, 255, 0.88)', fontWeight: 800 }
  },
  grid: { left: 58, right: 54, top: 48, bottom: 54, containLabel: true },
  xAxis: {
    type: 'category',
    data: strategyComparisonLabels.value,
    axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
  },
  yAxis: [
    {
      type: 'value',
      name: 'MWh',
      axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
    },
    {
      type: 'value',
      name: 'UAH regret',
      axisLabel: { color: 'rgba(219, 245, 255, 0.9)', fontWeight: 800 }
    }
  ],
  series: [
    {
      type: 'bar',
      name: 'Charge MWh',
      data: strategyComparisonRows.value.map(row => row.totalChargeMwh),
      itemStyle: { color: 'rgba(245, 166, 35, 0.82)', borderRadius: [7, 7, 0, 0] }
    },
    {
      type: 'bar',
      name: 'Discharge MWh',
      data: strategyComparisonRows.value.map(row => row.totalDischargeMwh),
      itemStyle: { color: 'rgba(83, 234, 141, 0.82)', borderRadius: [7, 7, 0, 0] }
    },
    {
      type: 'line',
      name: 'Mean regret vs strict',
      yAxisIndex: 1,
      smooth: true,
      data: strategyComparisonRows.value.map(row => row.meanRegretVsStrictUah),
      lineStyle: { width: 4, color: '#ff6fae' },
      itemStyle: { color: '#ff6fae' }
    },
    {
      type: 'line',
      name: 'Mean regret vs V2+',
      yAxisIndex: 1,
      smooth: true,
      data: strategyComparisonRows.value.map(row => row.meanRegretVsV2Uah),
      lineStyle: { width: 3, color: '#b8ff32', type: 'dashed' },
      itemStyle: { color: '#b8ff32' }
    }
  ]
}))

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
    textStyle: { color: '#f0fbff' },
    formatter: (params: Array<{ marker?: string, seriesName?: string, value?: number | null, axisValue?: string }>) => {
      const lines = params.map((item) => {
        const value = formatPolicyTooltipValue(item.seriesName, item.value, isOfficialPolicyMode.value)
        return `${item.marker || ''}${item.seriesName}: ${value}`
      })
      if (isOfficialPolicyMode.value) {
        return [`<strong>${params[0]?.axisValue || 'hour'}</strong>`, ...lines, 'Forecast rows only; no schedule command.'].join('<br/>')
      }
      if (usesDecisionPolicyPreview.value) {
        return [`<strong>${params[0]?.axisValue || 'hour'}</strong>`, ...lines, 'Policy value gap is oracle-normalized diagnostic evidence.'].join('<br/>')
      }
      return [
        `<strong>${params[0]?.axisValue || 'hour'}</strong>`,
        ...lines,
        'Orange = max(0, strict LP/reference value - selected schedule value).',
        'High shortfall means the selected preview is worse than strict for that hour; it is not market execution.'
      ].join('<br/>')
    }
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
    label: 'Shown schedule',
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
const previewSourceSelectItems = computed(() => {
  const sourceOptions = props.shadowPreview?.available_preview_sources
  const options = sourceOptions?.length
    ? sourceOptions
    : [
        {
          preview_source_id: 'best_valid',
          label: 'Best valid recommendation',
          status: 'default_v2_plus_fallback',
          reason: 'V2+ remains default/fallback.',
          is_default_strategy: true,
          is_promoted_strategy: true,
          market_execution_enabled: false
        },
        {
          preview_source_id: 'dt_shadow',
          label: 'DT Shadow',
          status: 'research_shadow_not_promoted',
          reason: 'Preview only.',
          is_default_strategy: false,
          is_promoted_strategy: false,
          market_execution_enabled: false
        },
        {
          preview_source_id: 'dt_direct_candidate_shadow',
          label: 'Direct DT Shadow',
          status: 'direct_candidate_shadow_not_promoted',
          reason: 'Direct candidate-index/schedule-family DT preview only.',
          is_default_strategy: false,
          is_promoted_strategy: false,
          market_execution_enabled: false
        },
        {
          preview_source_id: 'poland_tft_shadow',
          label: 'Poland-TFT Shadow',
          status: 'positive_not_promoted',
          reason: 'Shadow challenger.',
          is_default_strategy: false,
          is_promoted_strategy: false,
          market_execution_enabled: false
        },
        {
          preview_source_id: 'dfl_diagnostics',
          label: 'DFL diagnostics',
          status: 'diagnostic_only',
          reason: 'Diagnostic only.',
          is_default_strategy: false,
          is_promoted_strategy: false,
          market_execution_enabled: false
        },
        {
          preview_source_id: 'v13_dt_lava_promoted_training',
          label: 'V13/DT/LAVA blocked',
          status: 'blocked_source_readiness_roadmap',
          reason: 'Blocked roadmap.',
          is_default_strategy: false,
          is_promoted_strategy: false,
          market_execution_enabled: false
        }
      ]
  return options.map(option => ({
    label: formatPreviewSourceOptionLabel(option.preview_source_id, option.status, option.label),
    value: option.preview_source_id
  }))
})
const strategyReadinessItems = computed(() => buildStrategyReadinessItems(
  props.operatorRecommendation?.available_strategies ?? []
))
const v13ReadinessItems = computed(() => buildV13ReadinessItems(
  props.operatorRecommendation?.v13_readiness
))
const academicMvpGatePassportItems = computed(() => buildAcademicMvpGatePassportItems(
  props.academicMvpReadiness
))

const updateSelectedPreviewSource = (value: string | number | boolean | Record<string, unknown>): void => {
  if (typeof value === 'string') {
    emit('update:selectedPreviewSourceId', value as OperatorPreviewSourceId)
    return
  }

  if (typeof value === 'object' && value !== null && typeof value.value === 'string') {
    emit('update:selectedPreviewSourceId', value.value as OperatorPreviewSourceId)
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

function formatPreviewSourceOptionLabel(
  previewSourceId: string,
  status: string,
  fallbackLabel: string
): string {
  if (previewSourceId === 'best_valid') {
    return 'Best valid schedule (V2+ default/fallback)'
  }
  if (previewSourceId === 'dt_shadow') {
    return 'DT Shadow preview (not promoted)'
  }
  if (previewSourceId === 'dt_direct_candidate_shadow') {
    return 'Direct DT shadow (not promoted)'
  }
  if (previewSourceId === 'poland_tft_shadow') {
    return 'Poland/TFT shadow (positive, not promoted)'
  }
  if (previewSourceId === 'dfl_diagnostics') {
    return 'DFL diagnostics (not production)'
  }
  if (previewSourceId === 'v13_dt_lava_promoted_training') {
    return 'V13/DT/LAVA blocked (no schedule)'
  }
  return `${previewSourceDisplayLabel(previewSourceId, fallbackLabel)} / ${status}`
}

function formatInputSignalTooltipValue(
  seriesName: string | undefined,
  value: number | null | undefined
): string {
  if (value == null) {
    return 'n/a'
  }

  const normalizedSeriesName = seriesName?.toLowerCase() ?? ''
  if (normalizedSeriesName.includes('soc')) {
    return `${Math.round(value).toLocaleString('en-GB')}%`
  }
  if (normalizedSeriesName.includes('mw')) {
    return `${Number(value).toFixed(3)} MW`
  }
  if (normalizedSeriesName.includes('price') || normalizedSeriesName.includes('uah/mwh')) {
    return `${Math.round(value).toLocaleString('en-GB')} UAH/MWh`
  }
  return Math.round(value).toLocaleString('en-GB')
}

function formatPolicyTooltipValue(
  seriesName: string | undefined,
  value: number | null | undefined,
  forcePriceUnit = false
): string {
  if (value == null) {
    return 'n/a'
  }
  const normalizedSeriesName = seriesName?.toLowerCase() ?? ''
  if (normalizedSeriesName.includes('mw') || normalizedSeriesName.includes('action')) {
    return `${Number(value).toFixed(3)} MW`
  }
  if (forcePriceUnit || normalizedSeriesName.includes('price') || normalizedSeriesName.includes('forecast')) {
    return `${Math.round(value).toLocaleString('en-GB')} UAH/MWh`
  }
  return `${Math.round(value).toLocaleString('en-GB')} UAH`
}

function shortStrategyLabel(row: StrategyComparisonRow): string {
  if (row.sourceId === 'best_valid') {
    return 'Best valid'
  }
  if (row.sourceId === 'dt_shadow') {
    return 'DT Shadow'
  }
  if (row.sourceId === 'dt_direct_candidate_shadow') {
    return 'Direct DT'
  }
  if (row.sourceId === 'poland_tft_shadow') {
    return 'Poland/TFT'
  }
  if (row.sourceId === 'dfl_diagnostics') {
    return 'DFL diag'
  }
  return 'V13 blocked'
}

function formatEnergy(value: number): string {
  return `${value.toFixed(2)} MWh`
}

function formatOptionalUah(value: number | null): string {
  if (value == null) {
    return 'n/a'
  }
  return `${Math.round(value).toLocaleString('en-GB')} UAH`
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
          Delivery-day schedule preview and evidence gates
        </h2>
      </div>
      <div class="future-control-stack">
        <label class="future-schedule-source-control">
          <span>Schedule shown</span>
          <USelect
            class="future-strategy-select"
            :model-value="selectedPreviewSourceId"
            :items="previewSourceSelectItems"
            value-key="value"
            label-key="label"
            color="info"
            variant="none"
            @update:model-value="updateSelectedPreviewSource"
          />
        </label>
        <div
          class="future-baseline-context"
          aria-label="Default comparator context"
        >
          <span>Comparator baseline</span>
          <strong>Strict similar-day baseline</strong>
          <small>Frozen LP/oracle regret reference; not a second selector.</small>
        </div>
        <div
          class="future-baseline-context future-baseline-context--default"
          aria-label="Default fallback context"
        >
          <span>Default/fallback</span>
          <strong>V2+ schedule/value learner</strong>
          <small>DT and diagnostics stay manual preview only.</small>
        </div>
        <UButton
          class="future-refresh-button"
          icon="i-lucide-refresh-cw"
          :label="`Loaded ${shadowPreviewLastLoadedLabel}`"
          color="info"
          variant="soft"
          size="xs"
          @click="emit('refresh:shadowPreview')"
        />
        <UBadge
          class="status-badge"
          :label="readModelBadgeLabel"
          :color="activeErrorCount > 0 ? 'warning' : 'success'"
          variant="soft"
        />
      </div>
    </div>

    <div
      v-if="selectedPreviewSourceId !== 'best_valid'"
      class="shadow-preview-boundary-strip"
    >
      <span>Manual preview: {{ shadowPreviewLabel }}</span>
      <span>{{ shadowPreview?.preview_status || 'loading shadow packet' }}</span>
      <span>Not promoted</span>
      <span>No market execution</span>
      <span>V2+ remains default/fallback</span>
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

    <div class="v13-readiness-strip">
      <article
        v-for="item in v13ReadinessItems"
        :key="item.label"
        :class="{ 'v13-readiness-strip__item--blocked': item.status === 'blocked' }"
      >
        <span>{{ item.label }}</span>
        <strong>{{ item.value }}</strong>
        <small>{{ item.reason }}</small>
      </article>
    </div>

    <div class="academic-mvp-gate-strip">
      <article
        v-for="item in academicMvpGatePassportItems"
        :key="item.label"
        :class="{ 'academic-mvp-gate-strip__item--blocked': item.status === 'blocked' }"
      >
        <span>{{ item.label }}</span>
        <strong>{{ item.value }}</strong>
        <small>{{ item.reason }}</small>
      </article>
    </div>

    <div class="future-chart-grid">
      <article class="future-chart-card">
        <div>
          <p class="decision-chart-card__eyebrow">
            Forecast stack
          </p>
          <h3>{{ forecastChartTitle }}</h3>
          <p>{{ forecastStackDescription }}</p>
          <div class="forecast-quality-strip">
            <span
              v-for="item in recommendationInputSummaryItems"
              :key="item.modelName"
              :class="{ 'forecast-quality-strip__item--warn': item.needsCalibration }"
            >
              {{ item.label }}
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
              <span>{{ isShadowRecommendationMode ? 'Shadow preview' : 'DAM schedule' }}</span>
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
          <div
            v-if="selectedRecommendationGuideItems.length"
            class="policy-chart-guide"
          >
            <span
              v-for="item in selectedRecommendationGuideItems"
              :key="item.label"
            >
              <strong>{{ item.label }}</strong>: {{ item.detail }}
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

      <article class="future-chart-card future-chart-card--wide">
        <div>
          <p class="decision-chart-card__eyebrow">
            Strategy comparison
          </p>
          <h3>Five-strategy delivery-day comparison</h3>
          <p>
            Charge/discharge totals and regret metrics are shown for the selected DAM delivery window
            {{ selectedScheduleWindowLabel }}. Shadow and diagnostic strategies stay preview-only; blocked V13/DT/LAVA
            remains visible as gate evidence with no schedule rows.
          </p>
          <div
            v-if="strategyComparisonSummary.length"
            class="forecast-quality-strip"
          >
            <span
              v-for="item in strategyComparisonSummary"
              :key="item.label"
            >
              {{ item.label }}: {{ item.value }} / {{ item.meta }}
            </span>
          </div>
        </div>
        <ClientOnly>
          <ClientVChart
            :option="strategyComparisonOption"
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
.v13-readiness-strip,
.academic-mvp-gate-strip,
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

.v13-readiness-strip {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.academic-mvp-gate-strip {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.future-control-stack {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  justify-content: flex-end;
  gap: 0.55rem;
  min-width: min(100%, 26rem);
}

.future-control-stack label {
  display: grid;
  gap: 0.22rem;
  min-width: min(18rem, 100%);
}

.future-schedule-source-control {
  border: 4px solid rgba(255, 255, 255, 0.86);
  border-radius: 14px;
  color: #fff;
  font-weight: 600;
  padding: 4px 0.45rem 0.42rem 7px;
}

.future-baseline-context {
  display: grid;
  gap: 0.12rem;
  min-width: min(13.5rem, 100%);
  border: 1px solid rgba(202, 249, 255, 0.28);
  border-radius: 0.55rem;
  background: rgba(4, 67, 119, 0.58);
  padding: 0.42rem 0.55rem;
}

.future-baseline-context--default {
  border-color: rgba(215, 255, 79, 0.32);
}

.future-baseline-context strong {
  overflow-wrap: anywhere;
  color: #f2fbff;
  font-size: 0.76rem;
  font-weight: 900;
  line-height: 1.15;
}

.future-baseline-context small {
  color: rgba(229, 249, 255, 0.76);
  font-size: 0.62rem;
  font-weight: 760;
  line-height: 1.24;
}

.future-control-stack span {
  color: rgba(215, 255, 79, 0.84);
  font-size: 0.64rem;
  font-weight: 900;
  text-transform: uppercase;
}

.future-schedule-source-control > span {
  color: #fff;
  font-weight: 600;
}

.future-strategy-select {
  min-height: 2.4rem;
  border: 1px solid rgba(202, 249, 255, 0.34);
  border-radius: 0.55rem;
  background: rgba(4, 67, 119, 0.84);
  color: #fff;
  font-size: clamp(1.25rem, 2vw, 2rem);
  font-weight: 900;
  line-height: 1.05;
}

.future-schedule-source-control :deep(.future-strategy-select) {
  min-height: 2.4rem;
  border: 1px solid rgba(202, 249, 255, 0.34);
  border-radius: 0.55rem;
  background: rgba(4, 67, 119, 0.84);
  color: #fff !important;
  font-size: clamp(1.25rem, 2vw, 2rem) !important;
  font-weight: 900 !important;
  line-height: 1.05;
}

.future-strategy-select :deep([data-slot="value"]) {
  color: #fff;
  font-size: inherit;
  font-weight: 900;
  line-height: inherit;
}

.future-schedule-source-control :deep(.future-strategy-select [data-slot="value"]) {
  color: #fff !important;
  font-size: inherit;
  font-weight: 900 !important;
  line-height: inherit;
}

.future-refresh-button {
  min-height: 2.4rem;
  border: 2px solid #fff !important;
  border-radius: 3px !important;
  color: #f2f2f2 !important;
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  font-size: 1rem;
  font-weight: 900;
  padding: 4px !important;
  white-space: nowrap;
}

.future-refresh-button :deep(.truncate) {
  color: #f2f2f2;
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  font-size: 1rem;
  font-weight: 900;
}

.shadow-preview-boundary-strip {
  display: flex;
  flex-wrap: wrap;
  gap: 0.38rem;
}

.shadow-preview-boundary-strip span {
  border: 1px solid rgba(255, 191, 82, 0.58);
  border-radius: 999px;
  background: rgba(119, 65, 9, 0.58);
  color: #fff0c7;
  padding: 0.22rem 0.5rem;
  font-size: 0.68rem;
  font-weight: 900;
}

.future-chart-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.future-chart-card--wide {
  grid-column: 1 / -1;
}

.future-explainer-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.future-status-card,
.strategy-readiness-strip article,
.v13-readiness-strip article,
.academic-mvp-gate-strip article,
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

.v13-readiness-strip article {
  display: grid;
  gap: 0.18rem;
  min-width: 0;
}

.academic-mvp-gate-strip article {
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

.v13-readiness-strip__item--blocked {
  border-color: rgba(255, 191, 82, 0.66) !important;
  background:
    radial-gradient(circle at top right, rgba(255, 191, 82, 0.18), transparent 30%),
    linear-gradient(180deg, rgba(183, 100, 17, 0.78), rgba(119, 65, 9, 0.82)) !important;
}

.academic-mvp-gate-strip__item--blocked {
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

.v13-readiness-strip span {
  color: rgba(215, 255, 79, 0.84);
  font-size: 0.68rem;
  font-weight: 900;
  letter-spacing: 0;
  text-transform: uppercase;
}

.academic-mvp-gate-strip span {
  color: rgba(215, 255, 79, 0.84);
  font-size: 0.62rem;
  font-weight: 900;
  letter-spacing: 0;
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

.v13-readiness-strip strong {
  overflow-wrap: anywhere;
  color: #f2fbff;
  font-size: 1rem;
  line-height: 1.08;
  text-transform: none;
}

.academic-mvp-gate-strip strong {
  overflow-wrap: anywhere;
  color: #f2fbff;
  font-size: 0.9rem;
  line-height: 1.08;
  text-transform: none;
}

.future-status-card small,
.strategy-readiness-strip small,
.v13-readiness-strip small,
.academic-mvp-gate-strip small,
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

.v13-readiness-strip small {
  overflow-wrap: anywhere;
}

.academic-mvp-gate-strip small {
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

.policy-chart-guide {
  display: grid;
  gap: 0.28rem;
  margin-top: 0.45rem;
}

.policy-chart-guide span {
  display: block;
  border-left: 2px solid rgba(215, 255, 79, 0.54);
  padding-left: 0.45rem;
  color: rgba(229, 249, 255, 0.86);
  font-size: 0.68rem;
  font-weight: 760;
  line-height: 1.32;
}

.policy-chart-guide strong {
  color: #d7ff4f;
  font-weight: 950;
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
  .v13-readiness-strip,
  .academic-mvp-gate-strip,
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

  .future-baseline-context {
    min-width: 0;
  }

  .future-status-grid,
  .strategy-readiness-strip,
  .v13-readiness-strip,
  .academic-mvp-gate-strip,
  .future-chart-grid,
  .future-explainer-grid {
    grid-template-columns: 1fr;
  }

  .policy-chart-heading {
    flex-direction: column;
  }
}
</style>
