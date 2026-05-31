import type {
  OperatorRecommendationResponse,
  ShadowRecommendationPreviewResponse
} from '../../types/control-plane'

export function baseRecommendation(): OperatorRecommendationResponse {
  return {
    tenant_id: 'client_003_dnipro_factory',
    market_scope: 'dam_hourly_planning_preview',
    market_venue: 'DAM',
    interval_minutes: 60,
    price_context_status: 'loaded',
    anchor_timestamp: '2026-05-05T23:00:00Z',
    forecast_generated_at: null,
    target_delivery_window_start: '2026-05-06T00:00:00Z',
    target_delivery_window_end: '2026-05-07T00:00:00Z',
    market_execution_enabled: false,
    read_model_boundary: 'operator_preview_no_market_submission',
    market_gate_status: 'not_evaluated_preview_only',
    bid_eligibility_status: 'not_applicable_no_proposed_bid',
    proposed_bid_status: 'not_emitted_operator_preview',
    v13_readiness: {} as OperatorRecommendationResponse['v13_readiness'],
    selected_strategy_id: 'schedule_value_learner_v2_plus',
    selection_reason: 'manual strategy: Offline V2+ schedule/value learner',
    forecast_source: 'V2+ read-model preview adapter',
    soc_source: 'configured_default',
    review_required: false,
    readiness_warnings: [],
    policy_mode: 'offline_strategy_promotion_preview',
    selected_policy_id: 'schedule_value_learner_v2_plus',
    policy_explanation: 'V2+ remains confirmed offline schedule-value comparator.',
    policy_readiness: 'offline_strategy_promotion_ready',
    policy_forecast_context_source: 'not_applicable',
    policy_forecast_context_row_count: 0,
    policy_forecast_context_coverage_ratio: 0,
    policy_forecast_context_warning: null,
    available_strategies: [],
    forecast_model_series: [],
    value_gap_series: [],
    decision_advisor: {
      advisor_source_id: 'schedule_value_learner_v2_plus',
      advisor_status: 'read_model_preview',
      candidate_decision: 'review_v2_plus_schedule',
      selected_candidate_id: 'schedule_value_learner_v2_plus',
      selected_schedule_family: 'schedule_value_learner_v2_plus',
      reason: 'V2+ remains confirmed offline schedule-value comparator.',
      evidence_layers: ['offline_strategy_promotion'],
      comparison_metrics: {},
      market_execution_enabled: false,
      market_order_payload_emitted: false,
      promotion_gate_passed: false,
      dt_lava_ready: false
    },
    load_forecast: [],
    soc_projection: [],
    recommendation_schedule: [],
    bid_recommendation_preview: [],
    daily_value_uah: 745,
    hold_baseline_value_uah: 0,
    value_vs_hold_uah: 745,
    economics: {
      total_gross_market_value_uah: 760,
      total_degradation_penalty_uah: 15,
      total_net_value_uah: 745,
      total_throughput_mwh: 0.2
    }
  }
}

export function baseRecommendationWithSchedule(): OperatorRecommendationResponse {
  return {
    ...baseRecommendation(),
    available_strategies: [
      {
        strategy_id: 'schedule_value_learner_v2_plus',
        label: 'Offline V2+ schedule/value learner',
        reason: 'default/fallback',
        enabled: true,
        mean_regret_uah: 0,
        win_rate: 1
      }
    ],
    recommendation_schedule: [
      {
        step_index: 0,
        interval_start: '2026-05-26T00:00:00',
        forecast_price_uah_mwh: 1500,
        recommended_net_power_mw: -0.1,
        projected_soc_before_fraction: 0.5,
        projected_soc_after_fraction: 0.7,
        throughput_mwh: 0.1,
        degradation_penalty_uah: 1,
        gross_market_value_uah: 20,
        net_value_uah: 19
      },
      {
        step_index: 1,
        interval_start: '2026-05-26T01:00:00',
        forecast_price_uah_mwh: 5000,
        recommended_net_power_mw: 0.2,
        projected_soc_before_fraction: 0.7,
        projected_soc_after_fraction: 0.3,
        throughput_mwh: 0.2,
        degradation_penalty_uah: 1,
        gross_market_value_uah: 50,
        net_value_uah: 49
      }
    ],
    value_gap_series: [
      {
        step_index: 0,
        interval_start: '2026-05-26T00:00:00',
        chosen_value_uah: 19,
        best_visible_value_uah: 19,
        value_gap_uah: 0,
        metric_source: 'strict_reference'
      },
      {
        step_index: 1,
        interval_start: '2026-05-26T01:00:00',
        chosen_value_uah: 49,
        best_visible_value_uah: 69,
        value_gap_uah: 20,
        metric_source: 'strict_reference'
      }
    ]
  }
}

export function dtShadowPreview(): ShadowRecommendationPreviewResponse {
  return {
    tenant_id: 'client_003_dnipro_factory',
    preview_source_id: 'dt_shadow',
    preview_source_label: 'DT Shadow',
    preview_status: 'research_shadow_not_promoted',
    preview_only: true,
    is_default_strategy: false,
    is_promoted_strategy: false,
    research_shadow_not_promotable: true,
    default_strategy_id: 'schedule_value_learner_v2_plus',
    default_strategy_label: 'Offline V2+ schedule/value learner',
    selected_candidate_id: 'dt-candidate-worse-than-v2',
    selected_schedule_family: 'dt_tail_risk_aware_schedule',
    selected_candidate_index: 7,
    market_scope: 'dam_hourly_planning_preview',
    market_venue: 'DAM',
    interval_minutes: 60,
    anchor_timestamp: '2026-05-05T23:00:00Z',
    target_delivery_window_start: '2026-05-06T00:00:00Z',
    target_delivery_window_end: '2026-05-06T03:00:00Z',
    market_execution_enabled: false,
    proposed_bid_status: 'not_emitted_operator_preview',
    market_order_payload_emitted: false,
    promotion_gate_passed: false,
    dt_lava_ready: false,
    source_readiness_gate_passed: false,
    comparison_metrics: {
      dt_selected_mean_regret_uah: 245,
      dt_selected_mean_value_uah: 700,
      v2_plus_mean_regret_uah: 200,
      v2_plus_mean_value_uah: 745,
      strict_mean_regret_uah: 165,
      strict_mean_value_uah: 780,
      dt_minus_v2_plus_regret_uah: 45,
      dt_minus_strict_regret_uah: 80
    },
    available_preview_sources: [],
    recommendation_schedule: [
      {
        step_index: 0,
        interval_start: '2026-05-06T00:00:00Z',
        action: 'discharge',
        quantity_mw: 0.12,
        recommended_net_power_mw: 0.12,
        forecast_price_uah_mwh: 4300,
        soc_before_fraction: 0.52,
        soc_after_fraction: 0.47,
        selected_candidate_id: 'dt-candidate-worse-than-v2',
        schedule_family: 'dt_tail_risk_aware_schedule',
        expected_value_uah: 700,
        regret_uah: 245,
        regret_vs_v2_plus_uah: 45,
        regret_vs_strict_uah: 80,
        value_vs_v2_plus_uah: -45,
        value_vs_strict_uah: -80,
        gate_status: 'accepted_shadow_preview',
        safety_status: 'no_safety_violations_recorded',
        market_execution_enabled: false,
        market_order_payload_emitted: false,
        proposed_bid_status: 'not_emitted_operator_preview'
      }
    ],
    boundary_labels: ['DT Shadow', 'Not promoted', 'Preview only', 'No market execution'],
    readiness_warnings: ['DT shadow is diagnostic evidence only.'],
    artifact_paths: {}
  }
}

export function directDtShadowPreview(): ShadowRecommendationPreviewResponse {
  return {
    ...dtShadowPreview(),
    preview_source_id: 'dt_direct_candidate_shadow',
    preview_source_label: 'Direct DT Shadow',
    preview_status: 'direct_candidate_shadow_not_promoted',
    boundary_labels: ['Direct DT Shadow', 'Not promoted', 'Preview only', 'No market execution'],
    readiness_warnings: ['Direct DT shadow is diagnostic evidence only.']
  }
}

export function applesToApplesDtShadowPreview(): ShadowRecommendationPreviewResponse {
  return {
    ...dtShadowPreview(),
    preview_source_id: 'dt_v2_plus_apples_to_apples_shadow',
    preview_source_label: 'DT vs real V2+ Shadow',
    preview_status: 'apples_to_apples_not_promoted',
    boundary_labels: ['DT vs real V2+', 'Not promoted', 'Preview only', 'No market execution'],
    readiness_warnings: ['DT/V2+ apples-to-apples shadow is diagnostic evidence only.']
  }
}

export function regretAwareSelectorPreview(): ShadowRecommendationPreviewResponse {
  return {
    ...dtShadowPreview(),
    preview_source_id: 'regret_aware_v2_plus_selector_shadow',
    preview_source_label: 'Regret-aware V2+ selector',
    preview_status: 'regret_aware_abstention_not_promoted',
    selected_candidate_id: 'v2-plus-candidate',
    selected_schedule_family: 'schedule_value_learner_v2_plus',
    comparison_metrics: {
      selector_mean_regret_uah: 174.77,
      selector_mean_value_uah: 825,
      v2_plus_mean_regret_uah: 174.77,
      v2_plus_mean_value_uah: 825,
      strict_mean_regret_uah: 310.58,
      strict_mean_value_uah: 700,
      dt_selected_mean_regret_uah: 174.77,
      dt_selected_mean_value_uah: 825,
      dt_minus_v2_plus_regret_uah: 0,
      dt_minus_v2_plus_value_uah: 0,
      dt_minus_strict_regret_uah: -135.81,
      dt_minus_strict_value_uah: 125,
      non_v2_plus_switch_count: 0,
      abstention_count: 90
    },
    recommendation_schedule: dtShadowPreview().recommendation_schedule.map(point => ({
      ...point,
      selected_candidate_id: 'v2-plus-candidate',
      schedule_family: 'schedule_value_learner_v2_plus',
      expected_value_uah: 825,
      regret_uah: 174.77,
      regret_vs_v2_plus_uah: 0,
      regret_vs_strict_uah: -135.81,
      value_vs_v2_plus_uah: 0,
      value_vs_strict_uah: 125
    })),
    boundary_labels: ['Regret-aware V2+ selector', 'Not promoted', 'Preview only', 'No market execution'],
    readiness_warnings: ['Regret-aware selector abstained to V2+.']
  }
}

export function safeSwitchSelectorPreview(): ShadowRecommendationPreviewResponse {
  return {
    ...dtShadowPreview(),
    preview_source_id: 'dt_v2_plus_safe_switch_selector_shadow',
    preview_source_label: 'DT V2+ safe-switch selector',
    preview_status: 'safe_switch_evidence_not_promoted',
    selected_candidate_id: 'strict-candidate',
    selected_schedule_family: 'strict_reference',
    comparison_metrics: {
      selector_mean_regret_uah: 168.15664125116336,
      selector_mean_value_uah: 3743.327643562355,
      v2_plus_mean_regret_uah: 174.7683983151615,
      v2_plus_mean_value_uah: 3736.715886498357,
      strict_mean_regret_uah: 310.58280814033515,
      strict_mean_value_uah: 3600.901476666783,
      dt_selected_mean_regret_uah: 168.15664125116336,
      dt_selected_mean_value_uah: 3743.327643562355,
      dt_minus_v2_plus_regret_uah: -6.611757063998141,
      dt_minus_v2_plus_value_uah: 6.611757063998084,
      dt_minus_strict_regret_uah: -142.4261668891718,
      dt_minus_strict_value_uah: 142.42616689557218,
      non_v2_plus_switch_count: 4,
      abstention_count: 86,
      observed_safe_switch_opportunity_count: 15,
      recovered_safe_switch_opportunity_count: 3,
      safe_switch_win_count: 3,
      safe_switch_loss_count: 0,
      safe_switch_tie_count: 1,
      tail_risk_loss_count: 0
    },
    recommendation_schedule: dtShadowPreview().recommendation_schedule.map(point => ({
      ...point,
      selected_candidate_id: 'strict-candidate',
      schedule_family: 'strict_reference',
      expected_value_uah: 855,
      regret_uah: 168.16,
      regret_vs_v2_plus_uah: -6.61,
      regret_vs_strict_uah: -142.43,
      value_vs_v2_plus_uah: 6.61,
      value_vs_strict_uah: 142.43
    })),
    boundary_labels: ['DT V2+ safe-switch selector', 'Not promoted', 'Preview only', 'No market execution'],
    readiness_warnings: ['Recovered 3 of 15 safe-switch opportunities; V2+ remains confirmed offline schedule-value comparator.']
  }
}

export function distillationDtShadowPreview(): ShadowRecommendationPreviewResponse {
  return {
    ...dtShadowPreview(),
    preview_source_id: 'dt_v2_plus_distillation_shadow',
    preview_source_label: 'DT V2+ distillation shadow',
    preview_status: 'distillation_diagnostic_not_promoted',
    comparison_metrics: {
      ...dtShadowPreview().comparison_metrics,
      dt_minus_v2_plus_regret_uah: 0
    },
    boundary_labels: ['DT V2+ distillation shadow', 'Not promoted', 'Preview only', 'No market execution'],
    readiness_warnings: ['DT distillation shadow is diagnostic evidence only.']
  }
}
