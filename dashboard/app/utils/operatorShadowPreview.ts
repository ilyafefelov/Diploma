import type {
  BaselineRecommendationPoint,
  BidRecommendationPreviewPoint,
  OperatorRecommendationResponse,
  OperatorValueGapPointResponse,
  ShadowRecommendationPreviewResponse
} from '../types/control-plane'

export type OperatorPreviewSourceId
  = | 'best_valid'
    | 'dt_shadow'
    | 'dt_direct_candidate_shadow'
    | 'dt_v2_plus_apples_to_apples_shadow'
    | 'dt_v2_plus_distillation_shadow'
    | 'dt_decision_aware_shadow'
    | 'regret_aware_v2_plus_selector_shadow'
    | 'dt_v2_plus_safe_switch_selector_shadow'
    | 'poland_tft_shadow'
    | 'dfl_diagnostics'
    | 'v13_dt_lava_promoted_training'

export const SHADOW_PREVIEW_SOURCE_IDS: OperatorPreviewSourceId[] = [
  'dt_shadow',
  'dt_direct_candidate_shadow',
  'dt_v2_plus_apples_to_apples_shadow',
  'dt_v2_plus_distillation_shadow',
  'dt_decision_aware_shadow',
  'regret_aware_v2_plus_selector_shadow',
  'dt_v2_plus_safe_switch_selector_shadow',
  'poland_tft_shadow',
  'dfl_diagnostics',
  'v13_dt_lava_promoted_training'
]

export interface ShadowHourlyRecommendationRow {
  timestamp: string
  action: string
  quantityLabel: string
  socPathLabel: string
  candidateLabel: string
  scheduleFamily: string
  expectedValueLabel: string
  regretVsV2Label: string
  regretVsStrictLabel: string
  gateStatus: string
}

export interface StrategyComparisonRow {
  sourceId: OperatorPreviewSourceId
  label: string
  status: string
  scheduleRows: number
  totalChargeMwh: number
  totalDischargeMwh: number
  meanRegretVsV2Uah: number | null
  meanRegretVsStrictUah: number | null
  totalValueUah: number | null
  marketExecutionEnabled: boolean
  isDefault: boolean
  isPromoted: boolean
  isBlocked: boolean
}

export const shouldLoadShadowPreview = (previewSourceId: OperatorPreviewSourceId): boolean => {
  return previewSourceId !== 'best_valid'
}

export const previewModeLabel = (
  previewSourceId: OperatorPreviewSourceId,
  shadowPreview: ShadowRecommendationPreviewResponse | null
): string => {
  if (previewSourceId === 'best_valid') {
    return 'Best valid schedule (V2+ default/fallback)'
  }
  return previewSourceDisplayLabel(previewSourceId, shadowPreview?.preview_source_label)
}

export const previewSourceDisplayLabel = (
  previewSourceId: string,
  fallbackLabel?: string | null
): string => {
  if (previewSourceId === 'v13_dt_lava_promoted_training') {
    return 'V13/DT/LAVA blocked'
  }
  if (previewSourceId === 'regret_aware_v2_plus_selector_shadow') {
    return fallbackLabel || 'Regret-aware V2+ selector'
  }
  if (previewSourceId === 'dt_v2_plus_safe_switch_selector_shadow') {
    return fallbackLabel || 'DT V2+ safe-switch selector'
  }
  if (previewSourceId === 'dt_v2_plus_distillation_shadow') {
    return fallbackLabel || 'DT V2+ distillation shadow'
  }

  return fallbackLabel || previewSourceId
}

export const adaptShadowPreviewToOperatorRecommendation = (
  baseRecommendation: OperatorRecommendationResponse | null,
  shadowPreview: ShadowRecommendationPreviewResponse | null,
  previewSourceId: OperatorPreviewSourceId
): OperatorRecommendationResponse | null => {
  if (previewSourceId === 'best_valid' || !shadowPreview) {
    return baseRecommendation
  }

  const targetDeliveryWindowStart = shadowPreview.recommendation_schedule.length > 0
    ? shadowPreview.target_delivery_window_start
    : baseRecommendation?.target_delivery_window_start ?? shadowPreview.target_delivery_window_start ?? null
  const targetDeliveryWindowEnd = shadowPreview.recommendation_schedule.length > 0
    ? shadowPreview.target_delivery_window_end
    : baseRecommendation?.target_delivery_window_end ?? shadowPreview.target_delivery_window_end ?? null
  const recommendationSchedule = shadowPreview.recommendation_schedule.map((point): BaselineRecommendationPoint => ({
    step_index: point.step_index,
    interval_start: point.interval_start,
    forecast_price_uah_mwh: point.forecast_price_uah_mwh,
    recommended_net_power_mw: point.recommended_net_power_mw,
    projected_soc_before_fraction: point.soc_before_fraction ?? 0,
    projected_soc_after_fraction: point.soc_after_fraction ?? point.soc_before_fraction ?? 0,
    throughput_mwh: Math.abs(point.recommended_net_power_mw),
    degradation_penalty_uah: 0,
    gross_market_value_uah: point.expected_value_uah,
    net_value_uah: point.expected_value_uah
  }))
  const bidPreview = shadowPreview.recommendation_schedule.map((point): BidRecommendationPreviewPoint => ({
    step_index: point.step_index,
    interval_start: point.interval_start,
    market_venue: shadowPreview.market_venue,
    side: point.action === 'discharge' ? 'SELL' : point.action === 'charge' ? 'BUY' : 'HOLD',
    operator_action: point.action === 'discharge' ? 'discharge' : point.action === 'charge' ? 'charge' : 'hold',
    quantity_mw: point.quantity_mw,
    indicative_limit_price_uah_mwh: point.forecast_price_uah_mwh,
    preview_only: true,
    market_execution_enabled: false,
    market_order_payload_emitted: false,
    proposed_bid_status: shadowPreview.proposed_bid_status,
    read_model_boundary: 'operator_preview_no_market_submission'
  }))
  const valueGapSeries = shadowPreview.recommendation_schedule.map((point): OperatorValueGapPointResponse => ({
    step_index: point.step_index,
    interval_start: point.interval_start,
    chosen_value_uah: point.expected_value_uah,
    best_visible_value_uah: point.value_vs_strict_uah == null
      ? point.expected_value_uah
      : point.expected_value_uah - point.value_vs_strict_uah,
    value_gap_uah: point.value_vs_strict_uah == null ? 0 : Math.max(0, -point.value_vs_strict_uah),
    metric_source: `${shadowPreview.preview_source_id}_vs_strict_reference`
  }))
  const totalThroughputMwh = recommendationSchedule.reduce((total, point) => total + point.throughput_mwh, 0)
  const totalNetValueUah = recommendationSchedule.length
    ? recommendationSchedule[0]?.net_value_uah ?? 0
    : 0
  const warningSuffix = comparisonWarning(shadowPreview)
  const shadowDisplayLabel = previewSourceDisplayLabel(shadowPreview.preview_source_id, shadowPreview.preview_source_label)

  return {
    ...(baseRecommendation ?? minimalBaseRecommendation(shadowPreview)),
    tenant_id: shadowPreview.tenant_id,
    market_scope: shadowPreview.market_scope,
    market_venue: shadowPreview.market_venue,
    interval_minutes: shadowPreview.interval_minutes,
    anchor_timestamp: shadowPreview.anchor_timestamp ?? baseRecommendation?.anchor_timestamp ?? '',
    target_delivery_window_start: targetDeliveryWindowStart,
    target_delivery_window_end: targetDeliveryWindowEnd,
    market_execution_enabled: false,
    proposed_bid_status: shadowPreview.proposed_bid_status,
    selected_strategy_id: shadowPreview.preview_source_id,
    selection_reason: `${shadowDisplayLabel} research-shadow preview; ${shadowPreview.preview_status}`,
    forecast_source: `${shadowDisplayLabel} candidate/schedule-family artifact`,
    review_required: true,
    readiness_warnings: [
      ...(baseRecommendation?.readiness_warnings ?? []),
      `${shadowDisplayLabel} is preview only, not promoted, and no market execution is enabled.`,
      'V2+ remains default/fallback.',
      ...(warningSuffix ? [warningSuffix] : []),
      ...shadowPreview.readiness_warnings
    ],
    policy_mode: `${shadowPreview.preview_source_id}_preview`,
    selected_policy_id: shadowPreview.selected_candidate_id ?? shadowPreview.preview_source_id,
    policy_explanation: `${shadowDisplayLabel} is manually selected diagnostic evidence over candidate schedules. It never emits ProposedBid or market order payloads.`,
    policy_readiness: shadowPreview.preview_status,
    value_gap_series: valueGapSeries,
    recommendation_schedule: recommendationSchedule,
    bid_recommendation_preview: bidPreview,
    daily_value_uah: totalNetValueUah,
    value_vs_hold_uah: totalNetValueUah,
    economics: {
      total_gross_market_value_uah: totalNetValueUah,
      total_degradation_penalty_uah: 0,
      total_net_value_uah: totalNetValueUah,
      total_throughput_mwh: totalThroughputMwh
    }
  }
}

export const buildShadowHourlyRecommendationRows = (
  shadowPreview: ShadowRecommendationPreviewResponse | null,
  batteryCapacityMwh: number | null = null,
  intervalMinutes = 60
): ShadowHourlyRecommendationRow[] => {
  return shadowPreview?.recommendation_schedule.map(point => ({
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

  return recommendation.recommendation_schedule.map((point) => {
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

export const buildStrategyComparisonRows = (
  baseRecommendation: OperatorRecommendationResponse | null,
  shadowPreviews: ShadowRecommendationPreviewResponse[]
): StrategyComparisonRow[] => {
  const rows: StrategyComparisonRow[] = []

  if (baseRecommendation) {
    rows.push(buildBestValidComparisonRow(baseRecommendation))
  }

  rows.push(...shadowPreviews.map(buildShadowComparisonRow))
  return rows
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

const minimalBaseRecommendation = (
  shadowPreview: ShadowRecommendationPreviewResponse
): OperatorRecommendationResponse => ({
  tenant_id: shadowPreview.tenant_id,
  market_scope: shadowPreview.market_scope,
  market_venue: shadowPreview.market_venue,
  interval_minutes: shadowPreview.interval_minutes,
  anchor_timestamp: shadowPreview.anchor_timestamp ?? '',
  forecast_generated_at: null,
  target_delivery_window_start: shadowPreview.target_delivery_window_start,
  target_delivery_window_end: shadowPreview.target_delivery_window_end,
  market_execution_enabled: false,
  read_model_boundary: 'operator_preview_no_market_submission',
  market_gate_status: 'not_evaluated_preview_only',
  bid_eligibility_status: 'not_applicable_no_proposed_bid',
  proposed_bid_status: shadowPreview.proposed_bid_status,
  v13_readiness: {} as OperatorRecommendationResponse['v13_readiness'],
  selected_strategy_id: shadowPreview.preview_source_id,
  selection_reason: shadowPreview.preview_status,
  forecast_source: shadowPreview.preview_source_label,
  soc_source: 'shadow_preview_artifact',
  review_required: true,
  readiness_warnings: [],
  policy_mode: `${shadowPreview.preview_source_id}_preview`,
  selected_policy_id: shadowPreview.selected_candidate_id ?? shadowPreview.preview_source_id,
  policy_explanation: shadowPreview.preview_source_label,
  policy_readiness: shadowPreview.preview_status,
  policy_forecast_context_source: 'shadow_preview_artifact',
  policy_forecast_context_row_count: shadowPreview.recommendation_schedule.length,
  policy_forecast_context_coverage_ratio: shadowPreview.recommendation_schedule.length > 0 ? 1 : 0,
  policy_forecast_context_warning: null,
  available_strategies: [],
  forecast_model_series: [],
  value_gap_series: [],
  load_forecast: [],
  soc_projection: [],
  recommendation_schedule: [],
  bid_recommendation_preview: [],
  daily_value_uah: 0,
  hold_baseline_value_uah: 0,
  value_vs_hold_uah: 0,
  economics: {
    total_gross_market_value_uah: 0,
    total_degradation_penalty_uah: 0,
    total_net_value_uah: 0,
    total_throughput_mwh: 0
  }
})

const comparisonWarning = (shadowPreview: ShadowRecommendationPreviewResponse): string => {
  const regretDelta = shadowPreview.comparison_metrics.dt_minus_v2_plus_regret_uah
  if (typeof regretDelta !== 'number') {
    return ''
  }
  if (regretDelta > 0) {
    return `DT is worse than V2+ by ${Math.round(regretDelta).toLocaleString('en-GB')} UAH mean regret.`
  }
  if (regretDelta < 0) {
    if (shadowPreview.preview_source_id === 'dt_v2_plus_safe_switch_selector_shadow') {
      const switches = shadowPreview.comparison_metrics.non_v2_plus_switch_count
      const recovered = shadowPreview.comparison_metrics.recovered_safe_switch_opportunity_count
      const switchLabel = typeof switches === 'number'
        ? `${Math.round(switches).toLocaleString('en-GB')} non-V2+ switches`
        : 'non-V2+ switches'
      const recoveredLabel = typeof recovered === 'number'
        ? `${Math.round(recovered).toLocaleString('en-GB')} recovered safe-switch wins`
        : 'recovered safe-switch evidence'
      return `Safe-switch DT shadow improves V2+ by ${Math.round(Math.abs(regretDelta)).toLocaleString('en-GB')} UAH mean regret (${switchLabel}, ${recoveredLabel}).`
    }
    return `DT is better than V2+ by ${Math.round(Math.abs(regretDelta)).toLocaleString('en-GB')} UAH mean regret on this shadow packet.`
  }
  if (shadowPreview.preview_source_id === 'dt_v2_plus_distillation_shadow') {
    return 'Distillation shadow mirrors the V2+ selector decisions on this shadow packet.'
  }
  if (shadowPreview.preview_source_id === 'dt_direct_candidate_shadow') {
    return 'DT ties the V13 fallback row on this shadow packet.'
  }
  if (shadowPreview.preview_source_id === 'regret_aware_v2_plus_selector_shadow') {
    const abstentions = shadowPreview.comparison_metrics.abstention_count
    const switches = shadowPreview.comparison_metrics.non_v2_plus_switch_count
    if (typeof abstentions === 'number' && typeof switches === 'number') {
      return `Regret-aware selector abstained to V2+ (${Math.round(abstentions).toLocaleString('en-GB')} abstentions, ${Math.round(switches).toLocaleString('en-GB')} non-V2+ switches).`
    }
    return 'Regret-aware selector abstained to V2+ on this shadow packet.'
  }
  return 'DT ties the comparator mean regret on this shadow packet.'
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
