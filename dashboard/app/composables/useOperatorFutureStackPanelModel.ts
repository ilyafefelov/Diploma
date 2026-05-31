import { computed, ref } from 'vue'

import type {
  AcademicMvpReadinessResponse,
  DecisionPolicyPreviewResponse,
  FutureStackPreviewResponse,
  OperatorRecommendationResponse,
  ShadowRecommendationPreviewResponse
} from '~/types/control-plane'
import type { OperatorChartHorizon } from '~/types/operator-dashboard'
import { buildOperatorFutureForecastPanelModel } from '~/lib/operator-future/operatorFutureForecastPanelModel'
import {
  buildAcademicMvpGatePassportItems,
  buildPolicyForecastContextPoints,
  buildStrategyReadinessItems,
  buildV13ReadinessItems,
  formatForecastQualityLabel,
  formatOperatorPolicyForecastContextLabel,
  formatPolicyForecastContextLabel
} from '~/utils/operatorFutureStack'
import {
  buildDecisionPolicyChartSeries,
  buildOfficialPolicyChartSeries,
  buildPolicyOption,
  buildSelectedRecommendationChartSeries,
  buildStrategyComparisonOption
} from '~/utils/operatorFutureStackChartOptions'
import {
  buildBackendStatusFacts,
  buildDecisionSourceFacts,
  buildFutureStatusCards,
  buildShadowModelStoryItems,
  buildSelectedRecommendationChartRows,
  formatEnergy,
  formatHour,
  formatOptionalUah,
  shortStrategyLabel,
  type PolicyValueMode
} from '~/utils/operatorFutureStackPresentation'
import {
  buildStrategyComparisonRows,
  type OperatorPreviewSourceId
} from '~/utils/operatorShadowPreview'
import { sliceArrayForChartHorizon } from '~/utils/operatorPreviewControls'

export interface OperatorFutureStackPanelModelInput {
  futureStack: FutureStackPreviewResponse | null
  decisionPolicy: DecisionPolicyPreviewResponse | null
  operatorRecommendation: OperatorRecommendationResponse | null
  bestValidRecommendation: OperatorRecommendationResponse | null
  shadowPreview: ShadowRecommendationPreviewResponse | null
  shadowComparisonPreviews: ShadowRecommendationPreviewResponse[]
  academicMvpReadiness: AcademicMvpReadinessResponse | null
  selectedStrategyId: string
  selectedPreviewSourceId: OperatorPreviewSourceId
  selectedChartHorizon?: OperatorChartHorizon
}

export const useOperatorFutureStackPanelModel = (input: Readonly<OperatorFutureStackPanelModelInput>) => {
  const policyValueMode = ref<PolicyValueMode>('selected')
  const policyRows = computed(() => sliceArrayForChartHorizon(
    input.decisionPolicy?.rows ?? [],
    input.selectedChartHorizon ?? '24h'
  ))
  const recommendationScheduleRows = computed(() => sliceArrayForChartHorizon(
    input.operatorRecommendation?.recommendation_schedule ?? [],
    input.selectedChartHorizon ?? '24h'
  ))
  const valueGapRows = computed(() => sliceArrayForChartHorizon(
    input.operatorRecommendation?.value_gap_series ?? [],
    input.selectedChartHorizon ?? '24h'
  ))
  const selectedStrategyLabel = computed(() => {
    const selectedStrategyId = input.operatorRecommendation?.selected_strategy_id || input.selectedStrategyId
    const option = input.operatorRecommendation?.available_strategies.find(strategy => strategy.strategy_id === selectedStrategyId)
    return option?.label || selectedStrategyId || 'strict_similar_day'
  })
  const {
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
  } = buildOperatorFutureForecastPanelModel(input, {
    selectedStrategyLabel
  })
  const isOfficialPolicyMode = computed(() => policyValueMode.value === 'official' && hasOfficialPolicyRows.value)
  const usesDecisionPolicyPreview = computed(() => {
    return (input.operatorRecommendation?.selected_strategy_id || input.selectedStrategyId) === 'decision_transformer'
      && policyRows.value.length > 0
  })
  const policyForecastContextRows = computed(() => buildPolicyForecastContextPoints(policyRows.value))
  const policyForecastContextLabel = computed(() => input.decisionPolicy
    ? formatPolicyForecastContextLabel(input.decisionPolicy)
    : formatOperatorPolicyForecastContextLabel(input.operatorRecommendation))
  const policyProjectionSummary = computed(() => {
    if (policyRows.value.length === 0) {
      return []
    }

    const projectedRows = policyRows.value.filter(row => row.projection_status !== 'accepted_without_projection').length
    const meanValueGapRatio = policyRows.value.reduce((total, row) => total + (row.value_gap_ratio ?? 0), 0) / policyRows.value.length
    return [
      { label: 'Safety projection', value: `${projectedRows}/${policyRows.value.length}`, meta: 'policy rows changed by feasibility layer' },
      { label: 'Mean value gap', value: `${Math.round(meanValueGapRatio * 100)}%`, meta: 'oracle-normalized counterfactual gap' }
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
      { label: isShadowRecommendationMode.value ? 'Shadow preview' : 'Selected schedule', value: `${nonIdleRows}/${selectedRecommendationChartRows.value.length}`, meta: isShadowRecommendationMode.value ? 'projected diagnostic windows' : 'non-idle delivery windows' },
      { label: 'Mean shortfall vs strict', value: `${Math.round(meanStrictShortfall).toLocaleString('en-GB')} UAH`, meta: `${selectedStrategyLabel.value}; strict LP/reference value` }
    ]
  })
  const selectedRecommendationGuideItems = computed(() => {
    if (isOfficialPolicyMode.value || usesDecisionPolicyPreview.value) {
      return []
    }

    return [
      { label: 'Blue bars', detail: isShadowRecommendationMode.value ? 'selected shadow schedule MW; positive is discharge/sell, negative is charge/buy' : 'selected best-valid schedule MW; positive is discharge/sell, negative is charge/buy' },
      { label: 'Orange line', detail: 'value shortfall vs strict LP/reference = max(0, strict value - selected preview value), UAH' },
      { label: 'Green line', detail: isShadowRecommendationMode.value ? 'artifact forecast price context, UAH/MWh; not a value metric' : 'selected market forecast price context, UAH/MWh; not a value metric' }
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
  const officialPolicyChartSeries = computed(() => buildOfficialPolicyChartSeries(officialPolicyForecastSeries.value))
  const policyChartTitle = computed(() => isOfficialPolicyMode.value
    ? 'Safe market forecast rows'
    : usesDecisionPolicyPreview.value
      ? 'DAM/IDM policy preview'
      : isShadowRecommendationMode.value
        ? `${shadowPreviewLabel.value} projected schedule preview`
        : 'DAM/IDM hourly schedule review')
  const policyChartDescription = computed(() => isOfficialPolicyMode.value
    ? 'Forecast-store rows that are inside DAM caps. Hidden raw out-of-cap rows remain diagnostics, not schedule inputs.'
    : usesDecisionPolicyPreview.value
      ? 'Counterfactual value gap and projected DAM/IDM delivery-hour action rows for the selected policy preview; review only, not live dispatch.'
      : isShadowRecommendationMode.value
        ? `Bars show the manually selected ${shadowPreviewLabel.value} action pattern projected onto ${selectedScheduleWindowLabel.value}. The orange line is value shortfall versus strict LP/reference, not promotion and not market execution.`
        : 'Bars show selected DAM/IDM delivery-hour charge/discharge review rows for the selected preview strategy. The orange line is value shortfall versus strict LP/reference; the green line is official or scenario price context. Review only: no live IDM bid or market submission.')
  const selectedRecommendationChartSeries = computed(() => buildSelectedRecommendationChartSeries(
    selectedRecommendationChartRows.value,
    {
      netPower: isShadowRecommendationMode.value ? 'Selected shadow net power (MW)' : 'Selected DAM/IDM net power (MW)',
      valueGap: 'Value shortfall vs strict (UAH)',
      priceContext: isShadowRecommendationMode.value ? 'Artifact forecast price (UAH/MWh)' : 'Official/scenario price context (UAH/MWh)'
    }
  ))
  const strategyComparisonRows = computed(() => buildStrategyComparisonRows(
    input.bestValidRecommendation,
    input.shadowComparisonPreviews
  ))
  const strategyComparisonLabels = computed(() => strategyComparisonRows.value.map(row => shortStrategyLabel(row)))
  const strategyComparisonSummary = computed(() => strategyComparisonRows.value.map(row => ({
    label: row.label,
    value: row.isBlocked ? 'blocked' : `${formatEnergy(row.totalChargeMwh)} charge / ${formatEnergy(row.totalDischargeMwh)} discharge`,
    meta: `${row.status}; ${row.scheduleRows} row${row.scheduleRows === 1 ? '' : 's'}; ${formatOptionalUah(row.meanRegretVsStrictUah)} vs strict`
  })))
  const shadowModelStoryItems = computed(() => buildShadowModelStoryItems(input.shadowComparisonPreviews))
  const strategyComparisonOption = computed(() => buildStrategyComparisonOption({
    strategyComparisonLabels: strategyComparisonLabels.value,
    strategyComparisonRows: strategyComparisonRows.value
  }))
  const decisionPolicyChartSeries = computed(() => buildDecisionPolicyChartSeries(
    policyRows.value,
    policyForecastContextRows.value
  ))
  const policyOption = computed(() => buildPolicyOption({
    isOfficialPolicyMode: isOfficialPolicyMode.value,
    usesDecisionPolicyPreview: usesDecisionPolicyPreview.value,
    policyLabels: policyLabels.value,
    officialPolicyChartSeries: officialPolicyChartSeries.value,
    decisionPolicyChartSeries: decisionPolicyChartSeries.value,
    selectedRecommendationChartSeries: selectedRecommendationChartSeries.value
  }))
  const statusCards = computed(() => buildFutureStatusCards({
    selectedStrategyLabel: selectedStrategyLabel.value,
    operatorRecommendation: input.operatorRecommendation,
    futureStack: input.futureStack,
    decisionPolicy: input.decisionPolicy
  }))
  const decisionSourceFacts = computed(() => buildDecisionSourceFacts({
    selectedStrategyLabel: selectedStrategyLabel.value,
    policyForecastContextLabel: policyForecastContextLabel.value,
    operatorRecommendation: input.operatorRecommendation,
    decisionPolicy: input.decisionPolicy
  }))
  const backendStatusFacts = computed(() => buildBackendStatusFacts(input.futureStack))
  const strategyReadinessItems = computed(() => buildStrategyReadinessItems(
    input.operatorRecommendation?.available_strategies ?? []
  ))
  const v13ReadinessItems = computed(() => buildV13ReadinessItems(input.operatorRecommendation?.v13_readiness))
  const academicMvpGatePassportItems = computed(() => buildAcademicMvpGatePassportItems(input.academicMvpReadiness))

  const setPolicyValueMode = (mode: PolicyValueMode): void => {
    if (mode === 'official' && !hasOfficialPolicyRows.value) {
      return
    }
    policyValueMode.value = mode
  }

  return {
    academicMvpGatePassportItems,
    backendStatusFacts,
    decisionSourceFacts,
    forecastChartTitle,
    forecastOption,
    forecastStackDescription,
    hasOfficialPolicyRows,
    hiddenUnsafeForecastItems,
    isOfficialPolicyMode,
    isShadowRecommendationMode,
    policyChartDescription,
    policyChartSummary,
    policyChartTitle,
    policyOption,
    recommendationInputSummaryItems,
    selectedRecommendationGuideItems,
    selectedScheduleWindowLabel,
    setPolicyValueMode,
    shadowModelStoryItems,
    shadowPreviewLabel,
    statusCards,
    strategyComparisonOption,
    strategyComparisonSummary,
    strategyReadinessItems,
    v13ReadinessItems
  }
}
