// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

import {
  adaptShadowPreviewToOperatorRecommendation,
  buildOperatorHourlyRecommendationRows,
  buildShadowHourlyRecommendationRows,
  buildStrategyComparisonRows,
  previewSourceDisplayLabel,
  previewModeLabel,
  shouldLoadShadowPreview
} from './operatorShadowPreview'
import type {
  OperatorRecommendationResponse,
  ShadowRecommendationPreviewResponse
} from '../types/control-plane'

describe('operator shadow preview source switching', () => {
  it('keeps best-valid mode on the default V2+ recommendation', () => {
    const base = baseRecommendation()

    expect(shouldLoadShadowPreview('best_valid')).toBe(false)
    expect(adaptShadowPreviewToOperatorRecommendation(base, null, 'best_valid')).toBe(base)
    expect(previewModeLabel('best_valid', null)).toBe('Best valid schedule (V2+ default/fallback)')
    expect(previewModeLabel('v13_dt_lava_promoted_training', {
      ...dtShadowPreview(),
      preview_source_id: 'v13_dt_lava_promoted_training',
      preview_source_label: 'V13/DT/LAVA promoted training'
    })).toBe('V13/DT/LAVA blocked')
  })

  it('maps DT shadow rows into the same recommendation shape even when DT is worse', () => {
    const visible = adaptShadowPreviewToOperatorRecommendation(
      baseRecommendation(),
      dtShadowPreview(),
      'dt_shadow'
    )

    expect(shouldLoadShadowPreview('dt_shadow')).toBe(true)
    expect(visible?.selected_strategy_id).toBe('dt_shadow')
    expect(visible?.selection_reason).toContain('research-shadow')
    expect(visible?.market_execution_enabled).toBe(false)
    expect(visible?.proposed_bid_status).toBe('not_emitted_operator_preview')
    expect(visible?.recommendation_schedule[0]).toEqual(expect.objectContaining({
      interval_start: '2026-05-06T00:00:00Z',
      recommended_net_power_mw: 0.12,
      projected_soc_before_fraction: 0.52,
      projected_soc_after_fraction: 0.47,
      net_value_uah: 700
    }))
    expect(visible?.value_gap_series[0]).toEqual(expect.objectContaining({
      chosen_value_uah: 700,
      best_visible_value_uah: 780,
      value_gap_uah: 80,
      metric_source: 'dt_shadow_vs_strict_reference'
    }))
    expect(visible?.readiness_warnings.join(' ')).toContain('not promoted')
    expect(visible?.readiness_warnings.join(' ')).toContain('DT is worse than V2+')
    expect(visible?.bid_recommendation_preview[0]).toEqual(expect.objectContaining({
      preview_only: true,
      market_execution_enabled: false,
      market_order_payload_emitted: false,
      proposed_bid_status: 'not_emitted_operator_preview'
    }))
  })

  it('maps direct DT candidate-index shadow rows without promoting them', () => {
    const visible = adaptShadowPreviewToOperatorRecommendation(
      baseRecommendation(),
      directDtShadowPreview(),
      'dt_direct_candidate_shadow'
    )

    expect(shouldLoadShadowPreview('dt_direct_candidate_shadow')).toBe(true)
    expect(previewSourceDisplayLabel('dt_direct_candidate_shadow', 'Direct DT Shadow')).toBe('Direct DT Shadow')
    expect(visible?.selected_strategy_id).toBe('dt_direct_candidate_shadow')
    expect(visible?.selection_reason).toContain('direct_candidate_shadow_not_promoted')
    expect(visible?.market_execution_enabled).toBe(false)
    expect(visible?.policy_mode).toBe('dt_direct_candidate_shadow_preview')
    expect(visible?.policy_explanation).toContain('never emits ProposedBid')
    expect(visible?.readiness_warnings.join(' ')).toContain('V2+ remains default/fallback')
  })

  it('maps apples-to-apples DT/V2+ shadow rows without promoting them', () => {
    const visible = adaptShadowPreviewToOperatorRecommendation(
      baseRecommendation(),
      applesToApplesDtShadowPreview(),
      'dt_v2_plus_apples_to_apples_shadow'
    )

    expect(shouldLoadShadowPreview('dt_v2_plus_apples_to_apples_shadow')).toBe(true)
    expect(visible?.selected_strategy_id).toBe('dt_v2_plus_apples_to_apples_shadow')
    expect(visible?.selection_reason).toContain('apples_to_apples_not_promoted')
    expect(visible?.market_execution_enabled).toBe(false)
    expect(visible?.policy_mode).toBe('dt_v2_plus_apples_to_apples_shadow_preview')
    expect(visible?.readiness_warnings.join(' ')).toContain('DT is worse than V2+')
  })

  it('maps DT V2+ distillation shadow rows as a non-promoted diagnostic source', () => {
    const visible = adaptShadowPreviewToOperatorRecommendation(
      baseRecommendation(),
      distillationDtShadowPreview(),
      'dt_v2_plus_distillation_shadow'
    )

    expect(shouldLoadShadowPreview('dt_v2_plus_distillation_shadow')).toBe(true)
    expect(previewSourceDisplayLabel('dt_v2_plus_distillation_shadow')).toBe('DT V2+ distillation shadow')
    expect(visible?.selected_strategy_id).toBe('dt_v2_plus_distillation_shadow')
    expect(visible?.selection_reason).toContain('distillation_diagnostic_not_promoted')
    expect(visible?.market_execution_enabled).toBe(false)
    expect(visible?.policy_mode).toBe('dt_v2_plus_distillation_shadow_preview')
    expect(visible?.readiness_warnings.join(' ')).toContain('Distillation shadow mirrors the V2+ selector')
  })

  it('maps regret-aware selector shadow rows as an abstention back to V2+', () => {
    const visible = adaptShadowPreviewToOperatorRecommendation(
      baseRecommendation(),
      regretAwareSelectorPreview(),
      'regret_aware_v2_plus_selector_shadow'
    )

    expect(shouldLoadShadowPreview('regret_aware_v2_plus_selector_shadow')).toBe(true)
    expect(visible?.selected_strategy_id).toBe('regret_aware_v2_plus_selector_shadow')
    expect(visible?.selection_reason).toContain('regret_aware_abstention_not_promoted')
    expect(visible?.market_execution_enabled).toBe(false)
    expect(visible?.policy_mode).toBe('regret_aware_v2_plus_selector_shadow_preview')
    expect(visible?.readiness_warnings.join(' ')).toContain('selector abstained to V2+')
    expect(visible?.readiness_warnings.join(' ')).not.toContain('DT ties the comparator')
  })

  it('maps DT V2+ safe-switch selector rows as current non-promoted evidence', () => {
    const visible = adaptShadowPreviewToOperatorRecommendation(
      baseRecommendation(),
      safeSwitchSelectorPreview(),
      'dt_v2_plus_safe_switch_selector_shadow'
    )

    expect(shouldLoadShadowPreview('dt_v2_plus_safe_switch_selector_shadow')).toBe(true)
    expect(previewSourceDisplayLabel('dt_v2_plus_safe_switch_selector_shadow')).toBe('DT V2+ safe-switch selector')
    expect(visible?.selected_strategy_id).toBe('dt_v2_plus_safe_switch_selector_shadow')
    expect(visible?.selection_reason).toContain('safe_switch_evidence_not_promoted')
    expect(visible?.market_execution_enabled).toBe(false)
    expect(visible?.policy_mode).toBe('dt_v2_plus_safe_switch_selector_shadow_preview')
    expect(visible?.readiness_warnings.join(' ')).toContain('Safe-switch DT shadow improves V2+')
    expect(visible?.readiness_warnings.join(' ')).toContain('3 recovered safe-switch wins')
    expect(visible?.readiness_warnings.join(' ')).toContain('V2+ remains default/fallback')
  })

  it('builds hourly table rows with candidate, value, regret, and safety labels', () => {
    const rows = buildShadowHourlyRecommendationRows(dtShadowPreview(), 0.5, 60)

    expect(rows[0]).toEqual({
      timestamp: '2026-05-06T00:00:00Z',
      action: 'discharge',
      quantityLabel: '0.12 MW / 0.12 MWh (24% cap)',
      socPathLabel: '52% -> 47%',
      candidateLabel: 'dt-candidate-worse-than-v2',
      scheduleFamily: 'dt_tail_risk_aware_schedule',
      expectedValueLabel: '700 UAH',
      regretVsV2Label: '+45 UAH vs V2+',
      regretVsStrictLabel: '+80 UAH vs strict regret; 80 UAH shortfall vs strict value',
      gateStatus: 'accepted_shadow_preview / no_safety_violations_recorded'
    })
  })

  it('builds the same hourly table contract for the best-valid operator recommendation', () => {
    const rows = buildOperatorHourlyRecommendationRows(baseRecommendationWithSchedule(), 0.5)

    expect(rows).toHaveLength(2)
    expect(rows[0]).toEqual({
      timestamp: '2026-05-26T00:00:00',
      action: 'charge',
      quantityLabel: '-0.10 MW / 0.10 MWh (20% cap)',
      socPathLabel: '50% -> 70%',
      candidateLabel: 'schedule_value_learner_v2_plus',
      scheduleFamily: 'Offline V2+ schedule/value learner',
      expectedValueLabel: '19 UAH',
      regretVsV2Label: '+0 UAH vs V2+',
      regretVsStrictLabel: '0 UAH shortfall vs strict value',
      gateStatus: 'not_evaluated_preview_only / not_emitted_operator_preview'
    })
    expect(rows[1]).toEqual(expect.objectContaining({
      action: 'discharge',
      quantityLabel: '0.20 MW / 0.20 MWh (40% cap)',
      socPathLabel: '70% -> 30%',
      expectedValueLabel: '49 UAH',
      regretVsV2Label: '+0 UAH vs V2+',
      regretVsStrictLabel: '20 UAH shortfall vs strict value'
    }))
  })

  it('does not convert blocked roadmap sources into executable schedules', () => {
    const visible = adaptShadowPreviewToOperatorRecommendation(
      baseRecommendation(),
      {
        ...dtShadowPreview(),
        preview_source_id: 'v13_dt_lava_promoted_training',
        preview_source_label: 'V13/DT/LAVA blocked',
        preview_status: 'blocked_source_readiness_roadmap',
        recommendation_schedule: []
      },
      'v13_dt_lava_promoted_training'
    )

    expect(visible?.selected_strategy_id).toBe('v13_dt_lava_promoted_training')
    expect(visible?.recommendation_schedule).toEqual([])
    expect(visible?.target_delivery_window_start).toBe('2026-05-06T00:00:00Z')
    expect(visible?.target_delivery_window_end).toBe('2026-05-07T00:00:00Z')
    expect(visible?.market_execution_enabled).toBe(false)
    expect(visible?.policy_readiness).toBe('blocked_source_readiness_roadmap')
  })

  it('builds a comparison surface including direct DT, worse DT, and blocked previews', () => {
    const rows = buildStrategyComparisonRows(baseRecommendationWithSchedule(), [
      directDtShadowPreview(),
      applesToApplesDtShadowPreview(),
      regretAwareSelectorPreview(),
      safeSwitchSelectorPreview(),
      dtShadowPreview(),
      {
        ...dtShadowPreview(),
        preview_source_id: 'v13_dt_lava_promoted_training',
        preview_source_label: 'V13/DT/LAVA blocked',
        preview_status: 'blocked_source_readiness_roadmap',
        recommendation_schedule: []
      }
    ])

    expect(rows.map(row => row.sourceId)).toEqual([
      'best_valid',
      'dt_direct_candidate_shadow',
      'dt_v2_plus_apples_to_apples_shadow',
      'regret_aware_v2_plus_selector_shadow',
      'dt_v2_plus_safe_switch_selector_shadow',
      'dt_shadow',
      'v13_dt_lava_promoted_training'
    ])
    expect(rows[0]).toEqual(expect.objectContaining({
      label: 'Offline V2+ schedule/value learner',
      status: 'default_v2_plus_fallback',
      scheduleRows: 2,
      totalChargeMwh: 0.1,
      totalDischargeMwh: 0.2,
      meanRegretVsStrictUah: 10,
      marketExecutionEnabled: false
    }))
    expect(rows[1]).toEqual(expect.objectContaining({
      label: 'Direct DT Shadow',
      status: 'direct_candidate_shadow_not_promoted',
      scheduleRows: 1,
      totalDischargeMwh: 0.12,
      meanRegretVsV2Uah: 45,
      meanRegretVsStrictUah: 80,
      marketExecutionEnabled: false
    }))
    expect(rows[2]).toEqual(expect.objectContaining({
      label: 'DT vs real V2+ Shadow',
      status: 'apples_to_apples_not_promoted',
      scheduleRows: 1,
      totalDischargeMwh: 0.12,
      meanRegretVsV2Uah: 45,
      meanRegretVsStrictUah: 80,
      marketExecutionEnabled: false
    }))
    expect(rows[3]).toEqual(expect.objectContaining({
      label: 'Regret-aware V2+ selector',
      status: 'regret_aware_abstention_not_promoted',
      scheduleRows: 1,
      totalDischargeMwh: 0.12,
      meanRegretVsV2Uah: 0,
      meanRegretVsStrictUah: -136,
      marketExecutionEnabled: false
    }))
    expect(rows[4]).toEqual(expect.objectContaining({
      label: 'DT V2+ safe-switch selector',
      status: 'safe_switch_evidence_not_promoted',
      scheduleRows: 1,
      totalDischargeMwh: 0.12,
      meanRegretVsV2Uah: -7,
      marketExecutionEnabled: false
    }))
    expect(rows[5]).toEqual(expect.objectContaining({
      label: 'DT Shadow',
      status: 'research_shadow_not_promoted',
      scheduleRows: 1,
      totalDischargeMwh: 0.12,
      meanRegretVsV2Uah: 45,
      meanRegretVsStrictUah: 80,
      marketExecutionEnabled: false
    }))
    expect(rows[6]).toEqual(expect.objectContaining({
      label: 'V13/DT/LAVA blocked',
      status: 'blocked_source_readiness_roadmap',
      scheduleRows: 0,
      totalChargeMwh: 0,
      totalDischargeMwh: 0,
      isBlocked: true,
      marketExecutionEnabled: false
    }))
  })

  it('keeps a Nuxt read-model proxy for the FastAPI shadow endpoint', () => {
    const routePath = fileURLToPath(
      new URL('../../server/api/control-plane/dashboard/shadow-recommendation-preview.get.ts', import.meta.url)
    )
    const route = readFileSync(routePath, 'utf8')

    expect(route).toContain('/dashboard/shadow-recommendation-preview')
    expect(route).toContain('ShadowRecommendationPreviewResponse')
    expect(route).toContain('fetchControlPlane')
  })

  it('keeps the strategy comparison chart anchored to the best-valid recommendation in the panel', () => {
    const pagePath = fileURLToPath(new URL('../pages/operator.vue', import.meta.url))
    const panelPath = fileURLToPath(
      new URL('../components/dashboard/operator/OperatorFutureStackPanel.vue', import.meta.url)
    )
    const page = readFileSync(pagePath, 'utf8')
    const panel = readFileSync(panelPath, 'utf8')

    expect(panel).toContain('bestValidRecommendation: OperatorRecommendationResponse | null')
    expect(panel).toContain('buildStrategyComparisonRows(')
    expect(panel).toContain('props.bestValidRecommendation')
    expect(panel).toContain("'dt_v2_plus_safe_switch_selector_shadow'")
    expect(panel).toContain("return 'DT V2+ safe-switch'")
    expect(panel).toContain('interval: 0')
    expect(panel).toContain('formatter: formatStrategyAxisLabel')
    expect(page).toContain(':best-valid-recommendation="operatorRecommendation"')
  })

  it('presents one schedule source switch and keeps strict baseline as context, not a second selector', () => {
    const panelPath = fileURLToPath(
      new URL('../components/dashboard/operator/OperatorFutureStackPanel.vue', import.meta.url)
    )
    const dockPath = fileURLToPath(
      new URL('../components/dashboard/operator/OperatorScheduleDock.vue', import.meta.url)
    )
    const panel = readFileSync(panelPath, 'utf8')
    const dock = readFileSync(dockPath, 'utf8')

    expect(panel).toContain('Delivery-day schedule preview and evidence gates')
    expect(panel).toContain('<span>Schedule shown</span>')
    expect(panel).toContain('future-schedule-source-control')
    expect(panel).toContain('font-family: ui-monospace')
    expect(panel).toContain('font-size: clamp(1.25rem, 2vw, 2rem)')
    expect(panel).toContain('.future-schedule-source-control :deep(.future-strategy-select)')
    expect(panel).toContain('future-baseline-context')
    expect(panel).toContain('Strict similar-day baseline')
    expect(panel).toContain('Regret-aware selector')
    expect(panel).toContain('DT safe-switch shadow')
    expect(panel).toContain('shadowModelStoryItems')
    expect(panel).toContain('Value shortfall vs strict (UAH)')
    expect(panel).toContain('strict LP/reference value')
    expect(panel).not.toContain('V2+ schedule evidence, TFT portfolio, and strategy preview')
    expect(panel).not.toContain('Shadow regret/value gap')
    expect(panel).not.toContain('@update:model-value="updateSelectedStrategy"')
    expect(dock).toContain('aria-label="Hourly recommendation table"')
    expect(dock).toContain('Regret / value gap')
    expect(dock).toContain('hourlyRecommendationRows.length === 0')
    expect(dock).not.toContain('aria-label="Shadow hourly recommendation table"')
  })
})

function baseRecommendation(): OperatorRecommendationResponse {
  return {
    tenant_id: 'client_003_dnipro_factory',
    market_scope: 'dam_hourly_planning_preview',
    market_venue: 'DAM',
    interval_minutes: 60,
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
    policy_explanation: 'V2+ remains default/fallback.',
    policy_readiness: 'offline_strategy_promotion_ready',
    policy_forecast_context_source: 'not_applicable',
    policy_forecast_context_row_count: 0,
    policy_forecast_context_coverage_ratio: 0,
    policy_forecast_context_warning: null,
    available_strategies: [],
    forecast_model_series: [],
    value_gap_series: [],
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

function baseRecommendationWithSchedule(): OperatorRecommendationResponse {
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

function dtShadowPreview(): ShadowRecommendationPreviewResponse {
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

function directDtShadowPreview(): ShadowRecommendationPreviewResponse {
  return {
    ...dtShadowPreview(),
    preview_source_id: 'dt_direct_candidate_shadow',
    preview_source_label: 'Direct DT Shadow',
    preview_status: 'direct_candidate_shadow_not_promoted',
    boundary_labels: ['Direct DT Shadow', 'Not promoted', 'Preview only', 'No market execution'],
    readiness_warnings: ['Direct DT shadow is diagnostic evidence only.']
  }
}

function applesToApplesDtShadowPreview(): ShadowRecommendationPreviewResponse {
  return {
    ...dtShadowPreview(),
    preview_source_id: 'dt_v2_plus_apples_to_apples_shadow',
    preview_source_label: 'DT vs real V2+ Shadow',
    preview_status: 'apples_to_apples_not_promoted',
    boundary_labels: ['DT vs real V2+', 'Not promoted', 'Preview only', 'No market execution'],
    readiness_warnings: ['DT/V2+ apples-to-apples shadow is diagnostic evidence only.']
  }
}

function regretAwareSelectorPreview(): ShadowRecommendationPreviewResponse {
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

function safeSwitchSelectorPreview(): ShadowRecommendationPreviewResponse {
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
    readiness_warnings: ['Recovered 3 of 15 safe-switch opportunities; V2+ remains default/fallback.']
  }
}

function distillationDtShadowPreview(): ShadowRecommendationPreviewResponse {
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
