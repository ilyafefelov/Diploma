import type {
  OperatorRecommendationResponse,
  ShadowRecommendationPreviewResponse
} from '../../types/control-plane'
import { DAM_REVIEW_ACTION_THRESHOLD_MW } from '../../utils/operatorTimeline'
import type {
  OperatorPreviewSourceId,
  ShadowHourlyRecommendationRow,
  StrategyComparisonRow
} from './operatorShadowPreviewSources'
import { previewSourceDisplayLabel } from './operatorShadowPreviewSources'

export const buildShadowHourlyRecommendationRows = (
  shadowPreview: ShadowRecommendationPreviewResponse | null,
  batteryCapacityMwh: number | null = null,
  intervalMinutes = 60
): ShadowHourlyRecommendationRow[] => {
  return prioritizeReviewRows(
    shadowPreview?.recommendation_schedule ?? [],
    point => isActionLabel(point.action) || isActionPower(point.recommended_net_power_mw)
  ).map(point => ({
    timestamp: point.interval_start,
    action: point.action,
    quantityLabel: formatQuantity(point.quantity_mw, batteryCapacityMwh, intervalMinutes),
    socPathLabel: formatSocPath(point.soc_before_fraction, point.soc_after_fraction),
    candidateLabel: point.selected_candidate_id,
    scheduleFamily: point.schedule_family,
    expectedValueLabel: `${Math.round(point.expected_value_uah).toLocaleString('en-GB')} UAH`,
    regretVsV2Label: formatSignedRegret(point.regret_vs_v2_plus_uah, 'V2+'),
    regretVsStrictLabel: `${formatSignedRegret(point.regret_vs_strict_uah, 'strict regret')}; ${formatValueShortfall(point.value_vs_strict_uah, 'strict value')}`,
    gateStatus: `${point.gate_status} / ${point.safety_status}`
  })) ?? []
}

export const buildOperatorHourlyRecommendationRows = (
  recommendation: OperatorRecommendationResponse | null,
  batteryCapacityMwh: number | null = null
): ShadowHourlyRecommendationRow[] => {
  if (!recommendation) {
    return []
  }

  const bidPreviewByInterval = new Map(
    recommendation.bid_recommendation_preview.map(point => [point.interval_start, point])
  )
  const valueGapByInterval = new Map(
    recommendation.value_gap_series.map(point => [point.interval_start, point])
  )
  const selectedStrategy = recommendation.available_strategies.find((strategy) => {
    return strategy.strategy_id === recommendation.selected_strategy_id
  })
  const isV2PlusRecommendation = recommendation.selected_strategy_id.includes('v2_plus')
    || recommendation.selected_policy_id.includes('v2_plus')

  return prioritizeReviewRows(recommendation.recommendation_schedule, (point) => {
    const bidPreview = bidPreviewByInterval.get(point.interval_start)
    return isActionLabel(bidPreview?.operator_action)
      || bidPreview?.side === 'BUY'
      || bidPreview?.side === 'SELL'
      || isActionPower(point.recommended_net_power_mw)
  }).map((point) => {
    const bidPreview = bidPreviewByInterval.get(point.interval_start)
    const valueGap = valueGapByInterval.get(point.interval_start)

    return {
      timestamp: point.interval_start,
      action: bidPreview?.operator_action ?? netPowerToAction(point.recommended_net_power_mw),
      quantityLabel: formatQuantity(
        point.recommended_net_power_mw,
        batteryCapacityMwh,
        recommendation.interval_minutes
      ),
      socPathLabel: formatSocPath(
        point.projected_soc_before_fraction,
        point.projected_soc_after_fraction
      ),
      candidateLabel: recommendation.selected_policy_id || recommendation.selected_strategy_id,
      scheduleFamily: selectedStrategy?.label || recommendation.policy_mode || recommendation.selected_strategy_id,
      expectedValueLabel: `${Math.round(point.net_value_uah).toLocaleString('en-GB')} UAH`,
      regretVsV2Label: formatSignedRegret(isV2PlusRecommendation ? 0 : null, 'V2+'),
      regretVsStrictLabel: formatStrictValueShortfall(valueGap?.value_gap_uah ?? null),
      gateStatus: `${recommendation.market_gate_status} / ${bidPreview?.proposed_bid_status ?? recommendation.proposed_bid_status}`
    }
  })
}

const prioritizeReviewRows = <T>(
  rows: T[],
  isActionful: (row: T) => boolean
): T[] => {
  const actionRows = rows.filter(isActionful)

  if (actionRows.length === 0) {
    return rows
  }

  const holdRows = rows.filter(row => !isActionful(row))
  return [...actionRows, ...holdRows]
}

const isActionLabel = (action: string | undefined): boolean => action === 'charge' || action === 'discharge'

const isActionPower = (powerMw: number): boolean => Math.abs(powerMw) >= DAM_REVIEW_ACTION_THRESHOLD_MW

export const buildStrategyComparisonRows = (
  baseRecommendation: OperatorRecommendationResponse | null,
  shadowPreviews: ShadowRecommendationPreviewResponse[]
): StrategyComparisonRow[] => {
  const rows: StrategyComparisonRow[] = []

  if (baseRecommendation) {
    rows.push(buildBestValidComparisonRow(baseRecommendation))
  } else {
    const metricOnlyComparator = buildMetricOnlyBestValidComparisonRow(shadowPreviews)
    if (metricOnlyComparator) {
      rows.push(metricOnlyComparator)
    }
  }

  rows.push(...shadowPreviews.map(buildShadowComparisonRow))
  return rows
}

const buildMetricOnlyBestValidComparisonRow = (
  shadowPreviews: ShadowRecommendationPreviewResponse[]
): StrategyComparisonRow | null => {
  const metricSource = shadowPreviews.find(preview => (
    preview.preview_source_id === 'hf_live_safe_switch_value_aligned_shadow'
    || preview.preview_source_id === 'hf_live_safe_switch_shadow'
    || preview.preview_source_id === 'hfdt_live_shadow_preview'
  )) ?? shadowPreviews.find(preview => typeof preview.comparison_metrics.v2_plus_mean_regret_uah === 'number')

  if (!metricSource) {
    return null
  }

  const meanRegret = numericMetric(metricSource.comparison_metrics.v2_plus_baseline_mean_regret_uah)
    ?? numericMetric(metricSource.comparison_metrics.v2_plus_mean_regret_uah)
    ?? null
  const totalValue = numericMetric(metricSource.comparison_metrics.v2_plus_mean_value_uah)
    ?? null

  return {
    sourceId: 'best_valid',
    label: metricSource.default_strategy_label || 'Offline V2+ schedule/value learner',
    status: 'same_window_comparator_metric_only',
    scheduleRows: 0,
    totalChargeMwh: 0,
    totalDischargeMwh: 0,
    meanRegretVsV2Uah: 0,
    meanRegretVsStrictUah: meanRegret,
    totalValueUah: totalValue,
    marketExecutionEnabled: false,
    isDefault: true,
    isPromoted: false,
    isBlocked: false
  }
}

const buildBestValidComparisonRow = (
  baseRecommendation: OperatorRecommendationResponse
): StrategyComparisonRow => {
  const selectedStrategy = baseRecommendation.available_strategies.find((strategy) => {
    return strategy.strategy_id === baseRecommendation.selected_strategy_id
  })
  const energy = summarizeNetPower(
    baseRecommendation.recommendation_schedule.map(point => point.recommended_net_power_mw),
    baseRecommendation.interval_minutes
  )
  const meanRegretVsStrict = averageNullable(
    baseRecommendation.value_gap_series.map(point => point.value_gap_uah)
  )

  return {
    sourceId: 'best_valid',
    label: selectedStrategy?.label || baseRecommendation.selected_strategy_id || 'Best valid recommendation',
    status: 'default_v2_plus_fallback',
    scheduleRows: baseRecommendation.recommendation_schedule.length,
    totalChargeMwh: energy.totalChargeMwh,
    totalDischargeMwh: energy.totalDischargeMwh,
    meanRegretVsV2Uah: 0,
    meanRegretVsStrictUah: meanRegretVsStrict,
    totalValueUah: baseRecommendation.daily_value_uah,
    marketExecutionEnabled: baseRecommendation.market_execution_enabled,
    isDefault: true,
    isPromoted: true,
    isBlocked: false
  }
}

const buildShadowComparisonRow = (
  shadowPreview: ShadowRecommendationPreviewResponse
): StrategyComparisonRow => {
  const energy = summarizeNetPower(
    shadowPreview.recommendation_schedule.map(point => point.recommended_net_power_mw),
    shadowPreview.interval_minutes
  )
  const meanRegretVsV2 = averageNullable(
    shadowPreview.recommendation_schedule.map(point => point.regret_vs_v2_plus_uah)
  ) ?? shadowPreview.comparison_metrics.dt_minus_v2_plus_regret_uah ?? null
  const meanRegretVsStrict = averageNullable(
    shadowPreview.recommendation_schedule.map(point => point.regret_vs_strict_uah)
  ) ?? shadowPreview.comparison_metrics.dt_minus_strict_regret_uah ?? null

  return {
    sourceId: shadowPreview.preview_source_id as OperatorPreviewSourceId,
    label: previewSourceDisplayLabel(shadowPreview.preview_source_id, shadowPreview.preview_source_label),
    status: shadowPreview.preview_status,
    scheduleRows: shadowPreview.recommendation_schedule.length,
    totalChargeMwh: energy.totalChargeMwh,
    totalDischargeMwh: energy.totalDischargeMwh,
    meanRegretVsV2Uah: meanRegretVsV2,
    meanRegretVsStrictUah: meanRegretVsStrict,
    totalValueUah: shadowPreview.comparison_metrics.dt_selected_mean_value_uah
      ?? shadowPreview.recommendation_schedule[0]?.expected_value_uah
      ?? null,
    marketExecutionEnabled: shadowPreview.market_execution_enabled,
    isDefault: shadowPreview.is_default_strategy,
    isPromoted: shadowPreview.is_promoted_strategy,
    isBlocked: shadowPreview.recommendation_schedule.length === 0 || shadowPreview.preview_status.includes('blocked')
  }
}

const formatSocPath = (before: number | null, after: number | null): string => {
  if (before == null || after == null) {
    return 'SOC unavailable'
  }
  return `${Math.round(before * 100)}% -> ${Math.round(after * 100)}%`
}

const formatQuantity = (
  quantityMw: number,
  batteryCapacityMwh: number | null,
  intervalMinutes: number
): string => {
  const quantityMwh = Math.abs(quantityMw) * intervalMinutes / 60
  if (!batteryCapacityMwh || batteryCapacityMwh <= 0) {
    return `${quantityMw.toFixed(2)} MW / ${quantityMwh.toFixed(2)} MWh`
  }

  const capacityShare = quantityMwh / batteryCapacityMwh * 100
  return `${quantityMw.toFixed(2)} MW / ${quantityMwh.toFixed(2)} MWh (${Math.round(capacityShare)}% cap)`
}

const summarizeNetPower = (
  netPowerMw: number[],
  intervalMinutes: number
): { totalChargeMwh: number, totalDischargeMwh: number } => {
  const intervalHours = intervalMinutes / 60
  return {
    totalChargeMwh: roundEnergy(netPowerMw.reduce((total, value) => total + (value < 0 ? Math.abs(value) * intervalHours : 0), 0)),
    totalDischargeMwh: roundEnergy(netPowerMw.reduce((total, value) => total + (value > 0 ? value * intervalHours : 0), 0))
  }
}

const averageNullable = (values: Array<number | null | undefined>): number | null => {
  const numericValues = values.filter((value): value is number => typeof value === 'number' && Number.isFinite(value))
  if (numericValues.length === 0) {
    return null
  }
  return Math.round(numericValues.reduce((total, value) => total + value, 0) / numericValues.length)
}

const numericMetric = (value: number | null | undefined): number | null => {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

const roundEnergy = (value: number): number => Number(value.toFixed(3))

const formatSignedRegret = (value: number | null, referenceLabel: string): string => {
  if (value == null) {
    return `not compared vs ${referenceLabel}`
  }
  const rounded = Math.round(value).toLocaleString('en-GB')
  return `${value >= 0 ? '+' : ''}${rounded} UAH vs ${referenceLabel}`
}

const formatValueShortfall = (valueVsReference: number | null, referenceLabel: string): string => {
  if (valueVsReference == null) {
    return `not compared vs ${referenceLabel}`
  }
  const shortfall = Math.max(0, -valueVsReference)
  return `${Math.round(shortfall).toLocaleString('en-GB')} UAH shortfall vs ${referenceLabel}`
}

const formatStrictValueShortfall = (valueGapUah: number | null): string => {
  if (valueGapUah == null) {
    return 'not compared vs strict value'
  }
  return `${Math.round(valueGapUah).toLocaleString('en-GB')} UAH shortfall vs strict value`
}

const netPowerToAction = (netPowerMw: number): string => {
  if (netPowerMw > 0.005) {
    return 'discharge'
  }
  if (netPowerMw < -0.005) {
    return 'charge'
  }
  return 'hold'
}
