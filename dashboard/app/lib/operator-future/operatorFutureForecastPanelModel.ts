import { computed, type ComputedRef } from 'vue'

import type {
  FutureStackPreviewResponse,
  OperatorRecommendationResponse,
  ShadowRecommendationPreviewResponse
} from '~/types/control-plane'
import type { OperatorChartHorizon } from '~/types/operator-dashboard'
import {
  buildRecommendationInputSignalRows,
  filterOfficialPolicyValueSeries,
  formatForecastQualityLabel,
  formatForecastWindowLabel,
  isChartSafeForecastSeries,
  selectOperatorForecastChartSource,
  sortFutureForecastSeries
} from '~/utils/operatorFutureStack'
import {
  buildForecastOption,
  buildRecommendationInputChartSeries
} from '~/utils/operatorFutureStackChartOptions'
import { formatHour } from '~/utils/operatorFutureStackPresentation'
import {
  previewSourceDisplayLabel,
  type OperatorPreviewSourceId
} from '~/utils/operatorShadowPreview'
import { sliceArrayForChartHorizon } from '~/utils/operatorPreviewControls'

export interface OperatorFutureForecastPanelInput {
  futureStack: FutureStackPreviewResponse | null
  operatorRecommendation: OperatorRecommendationResponse | null
  shadowPreview: ShadowRecommendationPreviewResponse | null
  selectedPreviewSourceId: OperatorPreviewSourceId
  selectedChartHorizon?: OperatorChartHorizon
}

export const buildOperatorFutureForecastPanelModel = (
  input: Readonly<OperatorFutureForecastPanelInput>,
  context: {
    selectedStrategyLabel: ComputedRef<string>
  }
) => {
  const forecastChartSource = computed(() => selectOperatorForecastChartSource({
    futureStack: input.futureStack,
    operatorRecommendation: input.operatorRecommendation
  }))
  const forecastSeries = computed(() => sortFutureForecastSeries(
    forecastChartSource.value.series
      .filter(series => series.model_name.includes('nbeatsx') || series.model_name.includes('tft'))
      .filter(isChartSafeForecastSeries)
  ))
  const chartHorizon = computed(() => input.selectedChartHorizon ?? '24h')
  const forecastChartSeries = computed(() => forecastSeries.value.slice(0, 3).map(series => ({
    ...series,
    points: sliceArrayForChartHorizon(series.points, chartHorizon.value)
  })))
  const forecastLabels = computed(() => forecastChartSeries.value[0]?.points.map(point => formatHour(point.interval_start)) ?? [])
  const recommendationScheduleRows = computed(() => sliceArrayForChartHorizon(
    input.operatorRecommendation?.recommendation_schedule ?? [],
    chartHorizon.value
  ))
  const officialPolicyForecastSeries = computed(() => filterOfficialPolicyValueSeries(forecastSeries.value).map(series => ({
    ...series,
    points: sliceArrayForChartHorizon(series.points, chartHorizon.value)
  })))
  const hasOfficialPolicyRows = computed(() => officialPolicyForecastSeries.value.length > 0)
  const forecastWindowLabel = computed(() => formatForecastWindowLabel(
    forecastChartSource.value.windowStart,
    forecastChartSource.value.windowEnd
  ))
  const isShadowRecommendationMode = computed(() => input.selectedPreviewSourceId !== 'best_valid')
  const selectedScheduleWindowLabel = computed(() => formatForecastWindowLabel(
    input.operatorRecommendation?.target_delivery_window_start,
    input.operatorRecommendation?.target_delivery_window_end
  ))
  const shadowPreviewLabel = computed(() => previewSourceDisplayLabel(
    input.selectedPreviewSourceId,
    input.shadowPreview?.preview_source_label
  ))
  const recommendationInputSignalRows = computed(() => buildRecommendationInputSignalRows(
    recommendationScheduleRows.value,
    input.operatorRecommendation?.soc_projection ?? [],
    input.operatorRecommendation?.load_forecast ?? []
  ))
  const hasRecommendationInputSignalRows = computed(() => recommendationInputSignalRows.value.length > 0)
  const forecastQualityItems = computed(() => forecastSeries.value.map(series => ({
    modelName: series.model_name,
    label: formatForecastQualityLabel(series),
    needsCalibration: series.out_of_dam_cap_rows > 0
  })))
  const recommendationInputSummaryItems = computed(() => {
    if (!hasRecommendationInputSignalRows.value) {
      return forecastQualityItems.value
    }

    const modelFallbackCount = (input.operatorRecommendation?.forecast_model_series ?? [])
      .filter(series => series.source_status.includes('compact_fallback')).length
    const items = [
      { modelName: 'price_source', label: `price source: ${input.operatorRecommendation?.forecast_source || 'operator recommendation'}`, needsCalibration: false },
      { modelName: 'schedule_source', label: `schedule source: ${context.selectedStrategyLabel.value}`, needsCalibration: false },
      { modelName: 'soc_source', label: `SOC source: ${input.operatorRecommendation?.soc_source || 'not reported'}`, needsCalibration: input.operatorRecommendation?.soc_source !== 'telemetry' },
      { modelName: 'rows', label: `${recommendationInputSignalRows.value.length} delivery-hour rows`, needsCalibration: false }
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
  const hiddenUnsafeForecastItems = computed(() => {
    if (hasRecommendationInputSignalRows.value) {
      return []
    }

    return forecastChartSource.value.series
      .filter(series => series.model_name.includes('nbeatsx') || series.model_name.includes('tft'))
      .filter(series => !isChartSafeForecastSeries(series))
      .map(series => ({ modelName: series.model_name, label: formatForecastQualityLabel(series) }))
  })
  const recommendationInputChartSeries = computed(() => buildRecommendationInputChartSeries(recommendationInputSignalRows.value))
  const forecastOption = computed(() => buildForecastOption({
    hasRecommendationInputSignalRows: hasRecommendationInputSignalRows.value,
    recommendationInputSignalRows: recommendationInputSignalRows.value,
    recommendationInputChartSeries: recommendationInputChartSeries.value,
    forecastLabels: forecastLabels.value,
    forecastChartSeries: forecastChartSeries.value
  }))
  const forecastChartTitle = computed(() => hasRecommendationInputSignalRows.value
    ? 'Recommendation input signals'
    : forecastChartSource.value.kind === 'operator_delivery_day'
      ? 'Delivery-day price context'
      : forecastChartSource.value.kind === 'future_stack_context'
        ? 'Live forecast context'
        : 'Price context pending')
  const forecastStackDescription = computed(() => {
    if (hasRecommendationInputSignalRows.value) {
      return `Shows the selected delivery-day recommendation inputs for ${selectedScheduleWindowLabel.value}: price context, selected battery net power, projected SOC, and configured site-load estimate where available. Compact fallback NBEATSx/TFT rows are not plotted as independent model evidence.`
    }
    if (forecastChartSource.value.kind === 'operator_delivery_day') {
      return `Delivery-day price/model window: ${forecastWindowLabel.value}. This is the same selected market horizon used by the schedule chart and bottom dock.`
    }
    if (isShadowRecommendationMode.value) {
      return `Current forecast context remains the live read-model window: ${forecastWindowLabel.value}. The selected ${shadowPreviewLabel.value} action pattern is projected onto ${selectedScheduleWindowLabel.value} for diagnostic delivery-day preview.`
    }
    return `Market forecast window: ${forecastWindowLabel.value}. These are forecast context lines only; the delivery review schedule is shown in the policy chart and bottom dock.`
  })

  return {
    forecastChartTitle,
    forecastOption,
    forecastStackDescription,
    hasOfficialPolicyRows,
    hiddenUnsafeForecastItems,
    isShadowRecommendationMode,
    officialPolicyForecastSeries,
    recommendationInputSummaryItems,
    selectedScheduleWindowLabel,
    shadowPreviewLabel
  }
}
