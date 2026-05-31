import type {
  BaselineRecommendationPoint,
  BidRecommendationPreviewPoint,
  OperatorRecommendationResponse,
  OperatorValueGapPointResponse,
  ShadowRecommendationPreviewResponse
} from '../../types/control-plane'
import type { OperatorPreviewSourceId } from './operatorShadowPreviewSources'
import { previewSourceDisplayLabel } from './operatorShadowPreviewSources'

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
      'V2+ remains confirmed offline schedule-value comparator.',
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
