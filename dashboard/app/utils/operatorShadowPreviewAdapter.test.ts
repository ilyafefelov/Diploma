import { describe, expect, it } from 'vitest'

import {
  adaptShadowPreviewToOperatorRecommendation,
  previewSourceDisplayLabel,
  shouldLoadShadowPreview
} from './operatorShadowPreview'
import {
  applesToApplesDtShadowPreview,
  baseRecommendation,
  directDtShadowPreview,
  distillationDtShadowPreview,
  dtShadowPreview,
  hfLiveSafeSwitchPreview,
  hfLiveSafeSwitchValueAlignedPreview,
  regretAwareSelectorPreview,
  safeSwitchSelectorPreview
} from './test-fixtures/operatorShadowPreviewFixtures'

describe('operator shadow preview adapter', () => {
  it('does not borrow base recommendation cards while a selected shadow preview is pending', () => {
    const visible = adaptShadowPreviewToOperatorRecommendation(
      baseRecommendation(),
      null,
      'hf_live_safe_switch_value_aligned_shadow'
    )

    expect(visible).toBeNull()
  })

  it('maps DT shadow rows into the same recommendation shape even when DT is worse', () => {
    const visible = adaptShadowPreviewToOperatorRecommendation(baseRecommendation(), dtShadowPreview(), 'dt_shadow')

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

  it('maps direct and apples-to-apples DT shadows without promotion', () => {
    const direct = adaptShadowPreviewToOperatorRecommendation(baseRecommendation(), directDtShadowPreview(), 'dt_direct_candidate_shadow')
    const apples = adaptShadowPreviewToOperatorRecommendation(baseRecommendation(), applesToApplesDtShadowPreview(), 'dt_v2_plus_apples_to_apples_shadow')

    expect(previewSourceDisplayLabel('dt_direct_candidate_shadow', 'Direct DT Shadow')).toBe('Direct DT Shadow')
    expect(direct?.selected_strategy_id).toBe('dt_direct_candidate_shadow')
    expect(direct?.selection_reason).toContain('direct_candidate_shadow_not_promoted')
    expect(direct?.market_execution_enabled).toBe(false)
    expect(direct?.policy_mode).toBe('dt_direct_candidate_shadow_preview')
    expect(direct?.policy_explanation).toContain('never emits ProposedBid')
    expect(apples?.selected_strategy_id).toBe('dt_v2_plus_apples_to_apples_shadow')
    expect(apples?.selection_reason).toContain('apples_to_apples_not_promoted')
    expect(apples?.market_execution_enabled).toBe(false)
    expect(apples?.readiness_warnings.join(' ')).toContain('DT is worse than V2+')
  })

  it('maps selector and distillation shadows as non-promoted diagnostics', () => {
    const distillation = adaptShadowPreviewToOperatorRecommendation(baseRecommendation(), distillationDtShadowPreview(), 'dt_v2_plus_distillation_shadow')
    const regretAware = adaptShadowPreviewToOperatorRecommendation(baseRecommendation(), regretAwareSelectorPreview(), 'regret_aware_v2_plus_selector_shadow')
    const safeSwitch = adaptShadowPreviewToOperatorRecommendation(baseRecommendation(), safeSwitchSelectorPreview(), 'dt_v2_plus_safe_switch_selector_shadow')

    expect(previewSourceDisplayLabel('dt_v2_plus_distillation_shadow')).toBe('DT V2+ distillation shadow')
    expect(distillation?.selection_reason).toContain('distillation_diagnostic_not_promoted')
    expect(distillation?.readiness_warnings.join(' ')).toContain('Distillation shadow mirrors the V2+ selector')
    expect(regretAware?.selected_strategy_id).toBe('regret_aware_v2_plus_selector_shadow')
    expect(regretAware?.selection_reason).toContain('regret_aware_abstention_not_promoted')
    expect(regretAware?.readiness_warnings.join(' ')).toContain('selector abstained to V2+')
    expect(regretAware?.readiness_warnings.join(' ')).not.toContain('DT ties the comparator')
    expect(previewSourceDisplayLabel('dt_v2_plus_safe_switch_selector_shadow')).toBe('DT V2+ safe-switch selector')
    expect(safeSwitch?.selection_reason).toContain('safe_switch_evidence_not_promoted')
    expect(safeSwitch?.readiness_warnings.join(' ')).toContain('Safe-switch DT shadow improves V2+')
    expect(safeSwitch?.readiness_warnings.join(' ')).toContain('3 recovered safe-switch wins')
  })

  it('maps HF live safe-switch shadow with nullable actual regret as preview-only guidance', () => {
    const visible = adaptShadowPreviewToOperatorRecommendation(
      null,
      hfLiveSafeSwitchPreview(),
      'hf_live_safe_switch_shadow'
    )

    expect(previewSourceDisplayLabel('hf_live_safe_switch_shadow')).toBe('HF live safe-switch shadow')
    expect(visible?.selected_strategy_id).toBe('hf_live_safe_switch_shadow')
    expect(visible?.market_execution_enabled).toBe(false)
    expect(visible?.review_required).toBe(true)
    expect(visible?.policy_explanation).toContain('never emits ProposedBid')
    expect(visible?.readiness_warnings.join(' ')).toContain('HF live safe-switch shadow preview')
    expect(visible?.readiness_warnings.join(' ')).toContain('not promoted')
    expect(visible?.readiness_warnings.join(' ')).toContain('guard margin')
    expect(visible?.readiness_warnings.join(' ')).toContain('template value gap')
    expect(visible?.value_gap_series[0]).toEqual(expect.objectContaining({
      chosen_value_uah: 912,
      best_visible_value_uah: 912,
      value_gap_uah: 0,
      metric_source: 'hf_live_safe_switch_shadow_vs_strict_reference'
    }))
    expect(visible?.bid_recommendation_preview[0]).toEqual(expect.objectContaining({
      preview_only: true,
      market_execution_enabled: false,
      market_order_payload_emitted: false
    }))
  })

  it('maps value-aligned HF live safe-switch shadow as manual preview guidance', () => {
    const visible = adaptShadowPreviewToOperatorRecommendation(
      null,
      hfLiveSafeSwitchValueAlignedPreview(),
      'hf_live_safe_switch_value_aligned_shadow'
    )

    expect(previewSourceDisplayLabel('hf_live_safe_switch_value_aligned_shadow')).toBe('HF live safe-switch value-aligned shadow')
    expect(visible?.selected_strategy_id).toBe('hf_live_safe_switch_value_aligned_shadow')
    expect(visible?.market_execution_enabled).toBe(false)
    expect(visible?.review_required).toBe(true)
    expect(visible?.readiness_warnings.join(' ')).toContain('Value-aligned HF shadow')
    expect(visible?.readiness_warnings.join(' ')).toContain('shadow gate passed')
    expect(visible?.readiness_warnings.join(' ')).toContain('20 non-fallback days')
    expect(visible?.readiness_warnings.join(' ')).toContain('template value gap')
    expect(visible?.bid_recommendation_preview[0]).toEqual(expect.objectContaining({
      preview_only: true,
      market_execution_enabled: false,
      market_order_payload_emitted: false
    }))
  })

  it('charts HF live rows with hourly signed dispatch value instead of repeated candidate total', () => {
    const visible = adaptShadowPreviewToOperatorRecommendation(
      null,
      hfLiveSafeSwitchValueAlignedPreview(),
      'hf_live_safe_switch_value_aligned_shadow'
    )

    const firstPoint = hfLiveSafeSwitchValueAlignedPreview().recommendation_schedule[0]

    expect(firstPoint).toBeDefined()
    expect(visible?.recommendation_schedule[0]).toBeDefined()

    const visibleFirstPoint = visible?.recommendation_schedule[0]
    if (!firstPoint || !visibleFirstPoint) {
      throw new Error('HF live fixture must include a first visible schedule row')
    }
    const expectedHourlyValue = firstPoint.recommended_net_power_mw * firstPoint.forecast_price_uah_mwh

    expect(visibleFirstPoint).toEqual(expect.objectContaining({
      gross_market_value_uah: expectedHourlyValue,
      net_value_uah: expectedHourlyValue
    }))
    expect(visibleFirstPoint.net_value_uah).not.toBe(firstPoint.expected_value_uah)
    expect(visible?.economics.total_net_value_uah).toBeCloseTo(
      visible?.recommendation_schedule.reduce((total, point) => total + point.net_value_uah, 0) ?? 0
    )
  })

  it('uses the shadow delivery window date instead of a stale base recommendation target date', () => {
    const shadowPreview = {
      ...hfLiveSafeSwitchValueAlignedPreview(),
      target_delivery_window_start: '2026-06-02T00:00:00',
      target_delivery_window_end: '2026-06-03T00:00:00',
      recommendation_schedule: hfLiveSafeSwitchValueAlignedPreview().recommendation_schedule.map(point => ({
        ...point,
        interval_start: point.interval_start.replace('2026-05-06', '2026-06-02')
      }))
    }
    const visible = adaptShadowPreviewToOperatorRecommendation(
      {
        ...baseRecommendation(),
        target_delivery_date: '2026-06-01',
        target_delivery_window_start: '2026-06-01T00:00:00',
        target_delivery_window_end: '2026-06-02T00:00:00'
      },
      shadowPreview,
      'hf_live_safe_switch_value_aligned_shadow'
    )

    expect(visible?.target_delivery_date).toBe('2026-06-02')
    expect(visible?.target_delivery_window_start).toBe('2026-06-02T00:00:00')
    expect(visible?.target_delivery_window_end).toBe('2026-06-03T00:00:00')
  })

  it('uses blocked live HF shadow context instead of stale base context when rows are absent', () => {
    const visible = adaptShadowPreviewToOperatorRecommendation(
      {
        ...baseRecommendation(),
        price_context_status: 'official_published',
        target_delivery_date: '2026-06-01',
        target_delivery_window_start: '2026-06-01T00:00:00',
        target_delivery_window_end: '2026-06-02T00:00:00'
      },
      {
        ...hfLiveSafeSwitchValueAlignedPreview(),
        preview_status: 'blocked_missing_source_backed_price_context',
        target_delivery_window_start: '2026-06-04T00:00:00',
        target_delivery_window_end: '2026-06-05T00:00:00',
        recommendation_schedule: []
      },
      'hf_live_safe_switch_value_aligned_shadow'
    )

    expect(visible?.target_delivery_date).toBe('2026-06-04')
    expect(visible?.target_delivery_window_start).toBe('2026-06-04T00:00:00')
    expect(visible?.target_delivery_window_end).toBe('2026-06-05T00:00:00')
    expect(visible?.price_context_status).toBe('shadow_preview_artifact')
    expect(visible?.recommendation_schedule).toEqual([])
    expect(visible?.market_execution_enabled).toBe(false)
  })

  it('renders forecast-date HF value-aligned abstention as guarded HOLD guidance', () => {
    const forecastAbstention = hfLiveSafeSwitchValueAlignedPreview()
    const visible = adaptShadowPreviewToOperatorRecommendation(
      null,
      {
        ...forecastAbstention,
        selected_schedule_family: 'schedule_value_learner_v2_plus',
        comparison_metrics: {
          ...forecastAbstention.comparison_metrics,
          forecast_context_pre_publication: 1,
          guard_abstained_to_safe_fallback: 1,
          forecast_guard_abstained_to_safe_fallback: 1,
          threshold_guard_failed_count: 3,
          predicted_tail_guard_failed_count: 2,
          safety_guard_failed_count: 0,
          predicted_regret_delta_vs_v2_plus_uah: 0
        },
        recommendation_schedule: forecastAbstention.recommendation_schedule.map(point => ({
          ...point,
          action: 'hold',
          quantity_mw: 0,
          recommended_net_power_mw: 0,
          expected_value_uah: 0,
          schedule_family: 'schedule_value_learner_v2_plus'
        }))
      },
      'hf_live_safe_switch_value_aligned_shadow'
    )

    expect(visible?.recommendation_schedule.every(point => point.recommended_net_power_mw === 0)).toBe(true)
    expect(visible?.readiness_warnings.join(' ')).toContain('forecast-date guarded abstention')
    expect(visible?.readiness_warnings.join(' ')).toContain('3 threshold, 2 tail-risk')
    expect(visible?.market_execution_enabled).toBe(false)
    expect(visible?.bid_recommendation_preview.every(point => point.side === 'HOLD')).toBe(true)
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
})
