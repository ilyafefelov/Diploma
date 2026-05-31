import { describe, expect, it } from 'vitest'

import {
  adaptShadowPreviewToOperatorRecommendation,
  previewModeLabel,
  shouldLoadShadowPreview
} from './operatorShadowPreview'
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
  })
})
