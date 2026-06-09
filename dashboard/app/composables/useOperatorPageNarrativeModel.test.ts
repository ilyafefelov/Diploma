import { ref } from 'vue'
import { describe, expect, it, vi } from 'vitest'

vi.mock('~/utils/operatorResearchMetrics', () => ({
  buildOperatorResearchMetrics: () => []
}))

import { useOperatorPageNarrativeModel } from './useOperatorPageNarrativeModel'

describe('useOperatorPageNarrativeModel', () => {
  it('keeps selected shadow source copy when live HF shadow has no schedule rows', () => {
    const model = useOperatorPageNarrativeModel({
      explanationMode: ref('mvp'),
      visibleOperatorRecommendation: ref(null),
      selectedPreviewSourceLabel: ref('HF live safe-switch value-aligned shadow'),
      isShadowPreviewMode: ref(true),
      modelRows: ref([]),
      readinessRows: ref([]),
      offlineStrategyPromotion: ref(null),
      exogenousSignals: ref(null),
      batteryState: ref(null)
    })

    expect(model.schedulePredictionHeadLabel.value).toBe(
      'Schedule source: HF live safe-switch value-aligned shadow'
    )
    expect(model.explanationModeLabel.value).toBe('HF live safe-switch value-aligned shadow')
    expect(model.scheduleMarketBoundaryLabel.value).toBe(
      'DAM/IDM hourly preview / no ProposedBid / no market submission'
    )
  })
})
