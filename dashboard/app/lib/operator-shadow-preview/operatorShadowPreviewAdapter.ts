import type {
  BaselineRecommendationPoint,
  BidRecommendationPreviewPoint,
  OperatorRecommendationResponse,
  OperatorValueGapPointResponse,
  ShadowRecommendationPreviewResponse
} from '../../types/control-plane'
import type { OperatorPreviewSourceId } from './operatorShadowPreviewSources'
import { isLiveHfSafeSwitchPreviewSource, previewSourceDisplayLabel } from './operatorShadowPreviewSources'

export const adaptShadowPreviewToOperatorRecommendation = (
  baseRecommendation: OperatorRecommendationResponse | null,
  shadowPreview: ShadowRecommendationPreviewResponse | null,
  previewSourceId: OperatorPreviewSourceId
): OperatorRecommendationResponse | null => {
  if (previewSourceId === 'best_valid') {
    return baseRecommendation
  }
  if (!shadowPreview) {
    return null
  }

  const usesLiveHfPreview = isLiveHfSafeSwitchPreviewSource(previewSourceId)
  const baseForShadow = usesLiveHfPreview
    ? null
    : baseRecommendation
  const preferShadowWindow = usesLiveHfPreview || shadowPreview.recommendation_schedule.length > 0
  const targetDeliveryWindowStart = preferShadowWindow
    ? shadowPreview.target_delivery_window_start ?? baseForShadow?.target_delivery_window_start ?? null
    : baseForShadow?.target_delivery_window_start ?? shadowPreview.target_delivery_window_start ?? null
  const targetDeliveryWindowEnd = preferShadowWindow
    ? shadowPreview.target_delivery_window_end ?? baseForShadow?.target_delivery_window_end ?? null
    : baseForShadow?.target_delivery_window_end ?? shadowPreview.target_delivery_window_end ?? null
  const targetDeliveryDate = targetDeliveryWindowStart
    ? targetDeliveryWindowStart.slice(0, 10)
    : baseForShadow?.target_delivery_date ?? null
  const recommendationSchedule = shadowPreview.recommendation_schedule.map((point): BaselineRecommendationPoint => {
    const rowNetValueUah = usesLiveHfPreview
      ? point.recommended_net_power_mw * point.forecast_price_uah_mwh
      : point.expected_value_uah

    return {
      step_index: point.step_index,
      interval_start: point.interval_start,
      forecast_price_uah_mwh: point.forecast_price_uah_mwh,
      recommended_net_power_mw: point.recommended_net_power_mw,
      projected_soc_before_fraction: point.soc_before_fraction ?? 0,
      projected_soc_after_fraction: point.soc_after_fraction ?? point.soc_before_fraction ?? 0,
      throughput_mwh: Math.abs(point.recommended_net_power_mw),
      degradation_penalty_uah: 0,
      gross_market_value_uah: rowNetValueUah,
      net_value_uah: rowNetValueUah
    }
  })
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
  const liveHfMeanRegretEvidenceUah = previewSourceId === 'hf_live_safe_switch_value_aligned_shadow'
    || previewSourceId === 'hfdt_live_shadow_preview'
    ? numericComparisonMetric(shadowPreview.comparison_metrics?.hf_mean_regret_uah)
    : null
  const valueGapSeries = shadowPreview.recommendation_schedule.map((point): OperatorValueGapPointResponse => ({
    ...buildShadowValueGapPoint(point, shadowPreview.preview_source_id, liveHfMeanRegretEvidenceUah)
  }))
  const totalThroughputMwh = recommendationSchedule.reduce((total, point) => total + point.throughput_mwh, 0)
  const totalNetValueUah = usesLiveHfPreview
    ? recommendationSchedule.reduce((total, point) => total + point.net_value_uah, 0)
    : (recommendationSchedule.length ? recommendationSchedule[0]?.net_value_uah ?? 0 : 0)
  const warningSuffix = comparisonWarning(shadowPreview)
  const shadowDisplayLabel = previewSourceDisplayLabel(shadowPreview.preview_source_id, shadowPreview.preview_source_label)

  return {
    ...(baseForShadow ?? minimalBaseRecommendation(shadowPreview)),
    tenant_id: shadowPreview.tenant_id,
    market_scope: shadowPreview.market_scope,
    market_venue: shadowPreview.market_venue,
    interval_minutes: shadowPreview.interval_minutes,
    target_delivery_date: targetDeliveryDate,
    anchor_timestamp: shadowPreview.anchor_timestamp ?? baseForShadow?.anchor_timestamp ?? '',
    target_delivery_window_start: targetDeliveryWindowStart,
    target_delivery_window_end: targetDeliveryWindowEnd,
    market_execution_enabled: false,
    proposed_bid_status: shadowPreview.proposed_bid_status,
    selected_strategy_id: shadowPreview.preview_source_id,
    selection_reason: `${shadowDisplayLabel} research-shadow preview; ${shadowPreview.preview_status}`,
    forecast_source: `${shadowDisplayLabel} candidate/schedule-family artifact`,
    review_required: true,
    readiness_warnings: [
      ...(baseForShadow?.readiness_warnings ?? []),
      `${shadowDisplayLabel} is preview only, not promoted, and no market execution is enabled.`,
      'V2+ remains confirmed offline schedule-value comparator.',
      ...(warningSuffix ? [warningSuffix] : []),
      ...(shadowPreview.readiness_warnings ?? [])
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

const minimalBaseRecommendation = (
  shadowPreview: ShadowRecommendationPreviewResponse
): OperatorRecommendationResponse => ({
  tenant_id: shadowPreview.tenant_id,
  market_scope: shadowPreview.market_scope,
  market_venue: shadowPreview.market_venue,
  interval_minutes: shadowPreview.interval_minutes,
  price_context_status: 'shadow_preview_artifact',
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
  decision_advisor: {
    advisor_source_id: shadowPreview.preview_source_id,
    advisor_status: shadowPreview.preview_status,
    candidate_decision: 'review_only',
    selected_candidate_id: shadowPreview.selected_candidate_id,
    selected_schedule_family: null,
    reason: shadowPreview.preview_source_label,
    evidence_layers: ['shadow_recommendation_preview'],
    comparison_metrics: shadowPreview.comparison_metrics,
    market_execution_enabled: false,
    market_order_payload_emitted: false,
    promotion_gate_passed: shadowPreview.is_promoted_strategy,
    dt_lava_ready: false
  },
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

const buildShadowValueGapPoint = (
  point: ShadowRecommendationPreviewResponse['recommendation_schedule'][number],
  previewSourceId: string,
  liveHfMeanRegretEvidenceUah: number | null
): OperatorValueGapPointResponse => {
  const strictValueGapUah = point.value_vs_strict_uah == null
    ? null
    : Math.max(0, -point.value_vs_strict_uah)
  const strictRegretGapUah = point.regret_vs_strict_uah == null
    ? null
    : Math.max(0, point.regret_vs_strict_uah)
  const fallbackMeanRegretUah = strictValueGapUah == null && strictRegretGapUah == null
    ? liveHfMeanRegretEvidenceUah
    : null
  const valueGapUah = strictValueGapUah ?? strictRegretGapUah ?? fallbackMeanRegretUah ?? 0
  const metricSource = fallbackMeanRegretUah == null
    ? `${previewSourceId}_vs_strict_reference`
    : `${previewSourceId}_comparison_metrics_mean_regret_evidence`

  return {
    step_index: point.step_index,
    interval_start: point.interval_start,
    chosen_value_uah: point.expected_value_uah,
    best_visible_value_uah: point.expected_value_uah + valueGapUah,
    value_gap_uah: valueGapUah,
    metric_source: metricSource
  }
}

const numericComparisonMetric = (value: unknown): number | null => {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null
  }

  return value
}

const comparisonWarning = (shadowPreview: ShadowRecommendationPreviewResponse): string => {
  const comparisonMetrics = shadowPreview.comparison_metrics ?? {}
  if (
    shadowPreview.preview_source_id === 'hf_live_safe_switch_shadow'
    || shadowPreview.preview_source_id === 'hf_live_safe_switch_value_aligned_shadow'
    || shadowPreview.preview_source_id === 'hfdt_live_shadow_preview'
  ) {
    const threshold = comparisonMetrics.selected_operating_threshold_uah
    const predictedDelta = comparisonMetrics.predicted_regret_delta_vs_v2_plus_uah
    const thresholdLabel = typeof threshold === 'number'
      ? `${Math.round(threshold).toLocaleString('en-GB')} UAH`
      : 'the robust threshold'
    const deltaLabel = typeof predictedDelta === 'number'
      ? `; live predicted regret delta vs fallback ${Math.round(predictedDelta).toLocaleString('en-GB')} UAH`
      : ''
    const guardMargin = comparisonMetrics.threshold_margin_to_switch_uah
    const valueGap = comparisonMetrics.selected_vs_best_template_value_gap_uah
    const tailFailures = comparisonMetrics.predicted_tail_guard_failed_count
    const thresholdFailures = comparisonMetrics.threshold_guard_failed_count
    const forecastAbstained = comparisonMetrics.forecast_guard_abstained_to_safe_fallback
    const shadowGatePassed = comparisonMetrics.shadow_promotion_gate_passed
    const shadowSourceDays = comparisonMetrics.shadow_promotion_source_backed_day_count
    const shadowSwitchDays = comparisonMetrics.shadow_promotion_nonfallback_day_count
    const shadowRegretDelta = comparisonMetrics.shadow_promotion_hf_minus_v2_plus_mean_regret_uah
    const guardMarginLabel = typeof guardMargin === 'number'
      ? `; guard margin ${Math.round(guardMargin).toLocaleString('en-GB')} UAH`
      : ''
    const valueGapLabel = typeof valueGap === 'number'
      ? `; template value gap ${Math.round(valueGap).toLocaleString('en-GB')} UAH`
      : ''
    const failureLabels = [
      typeof thresholdFailures === 'number'
        ? `${Math.round(thresholdFailures).toLocaleString('en-GB')} threshold`
        : null,
      typeof tailFailures === 'number'
        ? `${Math.round(tailFailures).toLocaleString('en-GB')} tail-risk`
        : null
    ].filter(Boolean)
    const failureLabel = failureLabels.length > 0
      ? `; guard failures ${failureLabels.join(', ')}`
      : ''
    const shadowGateLabel = shadowGatePassed === 1
      ? `; shadow gate passed over ${Math.round(shadowSourceDays ?? 0).toLocaleString('en-GB')} source-backed days / ${Math.round(shadowSwitchDays ?? 0).toLocaleString('en-GB')} non-fallback days`
      : ''
    const shadowRegretLabel = typeof shadowRegretDelta === 'number'
      ? `; frozen HF vs V2+ ${Math.round(shadowRegretDelta).toLocaleString('en-GB')} UAH`
      : ''
    const forecastAbstentionLabel = forecastAbstained === 1
      ? '; forecast-date guarded abstention: selected recommendation remains HOLD'
      : ''
    const sourceLabel = shadowPreview.preview_source_id === 'hfdt_live_shadow_preview'
      ? 'HFDT live shadow preview'
      : shadowPreview.preview_source_id === 'hf_live_safe_switch_value_aligned_shadow'
        ? 'Value-aligned HF live safe-switch shadow preview'
        : 'HF live safe-switch shadow preview'
    return `${sourceLabel} uses live source-backed prices with ${thresholdLabel} guard${deltaLabel}${guardMarginLabel}${valueGapLabel}${failureLabel}${forecastAbstentionLabel}${shadowGateLabel}${shadowRegretLabel}.`
  }
  const regretDelta = comparisonMetrics.dt_minus_v2_plus_regret_uah
  if (typeof regretDelta !== 'number') {
    return ''
  }
  if (regretDelta > 0) {
    return `DT is worse than V2+ by ${Math.round(regretDelta).toLocaleString('en-GB')} UAH mean regret.`
  }
  if (regretDelta < 0) {
    if (shadowPreview.preview_source_id === 'dt_v2_plus_safe_switch_selector_shadow') {
      const switches = comparisonMetrics.non_v2_plus_switch_count
      const recovered = comparisonMetrics.recovered_safe_switch_opportunity_count
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
    const abstentions = comparisonMetrics.abstention_count
    const switches = comparisonMetrics.non_v2_plus_switch_count
    if (typeof abstentions === 'number' && typeof switches === 'number') {
      return `Regret-aware selector abstained to V2+ (${Math.round(abstentions).toLocaleString('en-GB')} abstentions, ${Math.round(switches).toLocaleString('en-GB')} non-V2+ switches).`
    }
    return 'Regret-aware selector abstained to V2+ on this shadow packet.'
  }
  return 'DT ties the comparator mean regret on this shadow packet.'
}
