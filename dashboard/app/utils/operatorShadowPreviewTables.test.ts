import { describe, expect, it } from 'vitest'

import {
  buildOperatorHourlyRecommendationRows,
  buildShadowHourlyRecommendationRows,
  buildStrategyComparisonRows
} from './operatorShadowPreview'
import {
  applesToApplesDtShadowPreview,
  baseRecommendationWithSchedule,
  directDtShadowPreview,
  distillationDtShadowPreview,
  dtShadowPreview,
  hfLiveSafeSwitchValueAlignedPreview,
  regretAwareSelectorPreview,
  safeSwitchSelectorPreview
} from './test-fixtures/operatorShadowPreviewFixtures'

describe('operator shadow preview table rows', () => {
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
      regretVsStrictLabel: '20 UAH shortfall vs strict value'
    }))
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
    expect(rows[0]).toEqual(expect.objectContaining({ status: 'default_v2_plus_fallback', scheduleRows: 2, meanRegretVsStrictUah: 10 }))
    expect(rows[1]).toEqual(expect.objectContaining({ label: 'Direct DT Shadow', meanRegretVsV2Uah: 45, meanRegretVsStrictUah: 80 }))
    expect(rows[3]).toEqual(expect.objectContaining({ label: 'Regret-aware V2+ selector', meanRegretVsV2Uah: 0, meanRegretVsStrictUah: -136 }))
    expect(rows[4]).toEqual(expect.objectContaining({ label: 'DT V2+ safe-switch selector', meanRegretVsV2Uah: -7 }))
    expect(rows[6]).toEqual(expect.objectContaining({ label: 'V13/DT/LAVA blocked', scheduleRows: 0, isBlocked: true, marketExecutionEnabled: false }))
  })

  it('synthesizes a V2+ comparator row from live HF metrics when no LP-backed recommendation is loaded', () => {
    const rows = buildStrategyComparisonRows(null, [
      distillationDtShadowPreview(),
      safeSwitchSelectorPreview(),
      hfLiveSafeSwitchValueAlignedPreview()
    ])

    expect(rows.map(row => row.sourceId)).toEqual([
      'best_valid',
      'dt_v2_plus_distillation_shadow',
      'dt_v2_plus_safe_switch_selector_shadow',
      'hf_live_safe_switch_value_aligned_shadow'
    ])
    expect(rows[0]).toEqual(expect.objectContaining({
      label: 'Offline V2+ schedule/value learner',
      status: 'same_window_comparator_metric_only',
      scheduleRows: 0,
      meanRegretVsV2Uah: 0,
      meanRegretVsStrictUah: 174.77,
      marketExecutionEnabled: false,
      isDefault: true,
      isPromoted: false
    }))
  })
})
