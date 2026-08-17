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
})
