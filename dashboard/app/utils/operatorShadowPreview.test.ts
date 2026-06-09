import { describe, expect, it } from 'vitest'

import {
  adaptShadowPreviewToOperatorRecommendation,
  previewModeLabel,
  resolveShadowPreviewTargetDeliveryDate,
  shouldLoadShadowPreview
} from './operatorShadowPreview'
import {
  resolveValueAlignedHfShadowDemoScenario,
  VALUE_ALIGNED_HF_SHADOW_DEMO_SCENARIOS
} from '../lib/operator-future/operatorFuturePreviewSources'
import {
  baseRecommendation,
  dtShadowPreview
} from './test-fixtures/operatorShadowPreviewFixtures'

describe('operator shadow preview source metadata', () => {
  it('keeps best-valid mode on the default V2+ recommendation', () => {
    const base = baseRecommendation()

    expect(shouldLoadShadowPreview('best_valid')).toBe(false)
    expect(adaptShadowPreviewToOperatorRecommendation(base, null, 'best_valid')).toBe(base)
    expect(previewModeLabel('best_valid', null)).toBe('Best valid schedule (V2+ comparator/fallback)')
    expect(previewModeLabel('v13_dt_lava_promoted_training', {
      ...dtShadowPreview(),
      preview_source_id: 'v13_dt_lava_promoted_training',
      preview_source_label: 'V13/DT/LAVA promoted training'
    })).toBe('V13/DT/LAVA blocked')
    expect(shouldLoadShadowPreview('hf_live_safe_switch_shadow')).toBe(true)
    expect(shouldLoadShadowPreview('hf_live_safe_switch_value_aligned_shadow')).toBe(true)
    expect(previewModeLabel('hf_live_safe_switch_shadow', {
      ...dtShadowPreview(),
      preview_source_id: 'hf_live_safe_switch_shadow',
      preview_source_label: 'HF live safe-switch shadow'
    })).toBe('HF live safe-switch shadow')
    expect(previewModeLabel('hf_live_safe_switch_value_aligned_shadow', {
      ...dtShadowPreview(),
      preview_source_id: 'hf_live_safe_switch_value_aligned_shadow',
      preview_source_label: 'HF live safe-switch value-aligned shadow'
    })).toBe('HF live safe-switch value-aligned shadow')
  })

  it('defines four manual value-aligned HF demo scenarios', () => {
    expect(VALUE_ALIGNED_HF_SHADOW_DEMO_SCENARIOS.map(scenario => scenario.id)).toEqual([
      'official_dam_proof',
      'forecast_dam_action',
      'forecast_dam_abstention',
      'forecast_idm_abstention'
    ])
    expect(resolveValueAlignedHfShadowDemoScenario('forecast_dam_action')).toEqual({
      id: 'forecast_dam_action',
      label: 'Forecast DAM action',
      marketVenue: 'DAM',
      targetDeliveryDate: '2026-06-02',
      boundaryCopy: 'Forecast guarded action; source-backed NBEATSx/TFT context; no market execution.'
    })
  })

  it('does not substitute the old value-aligned HF proof date for normal source selection', () => {
    expect(resolveShadowPreviewTargetDeliveryDate(
      'hf_live_safe_switch_value_aligned_shadow',
      null
    )).toBeNull()
    expect(resolveShadowPreviewTargetDeliveryDate(
      'hf_live_safe_switch_value_aligned_shadow',
      '2026-06-03'
    )).toBe('2026-06-03')
  })
})
