import { computed, ref } from 'vue'
import { describe, expect, it, vi } from 'vitest'

import { useSelectedOperatorPreviewState } from './useSelectedOperatorPreviewState'

const emptyStringRef = () => ref('')

describe('useSelectedOperatorPreviewState', () => {
  it('does not hold live HF shadow charts behind the generic signal loader', () => {
    const visibleRecommendation = { selected_strategy_id: 'hf_live_safe_switch_value_aligned_shadow' }
    const state = useSelectedOperatorPreviewState({
      isLiveHfShadowPreviewSelected: computed(() => true),
      isOperatorRecommendationLoading: ref(false),
      isShadowPreviewLoading: ref(false),
      isSignalPreviewLoading: ref(true),
      operatorRecommendationError: emptyStringRef(),
      baselinePreviewError: emptyStringRef(),
      shadowPreviewError: emptyStringRef(),
      error: emptyStringRef(),
      weatherError: emptyStringRef(),
      signalPreviewError: emptyStringRef(),
      shadowComparisonError: emptyStringRef(),
      operatorRecommendationLastLoadedLabel: ref('Loaded 10:00'),
      shadowPreviewLastLoadedLabel: ref('Loaded 10:01'),
      visibleOperatorRecommendation: computed(() => visibleRecommendation as never),
      hourlyRecommendationRows: computed(() => []),
      hourlyRecommendationEmptyMessage: computed(() => ''),
      shouldAutoLoadBaselinePreview: ref(true),
      clearBaselinePreviewError: vi.fn(),
      clearOperatorRecommendationError: vi.fn()
    })

    expect(state.activeMarketPreviewLoading.value).toBe(false)
    expect(state.selectedVisibleOperatorRecommendation.value).toBe(visibleRecommendation)
  })

  it('keeps non-HF operator charts behind the generic signal loader', () => {
    const state = useSelectedOperatorPreviewState({
      isLiveHfShadowPreviewSelected: computed(() => false),
      isOperatorRecommendationLoading: ref(false),
      isShadowPreviewLoading: ref(false),
      isSignalPreviewLoading: ref(true),
      operatorRecommendationError: emptyStringRef(),
      baselinePreviewError: emptyStringRef(),
      shadowPreviewError: emptyStringRef(),
      error: emptyStringRef(),
      weatherError: emptyStringRef(),
      signalPreviewError: emptyStringRef(),
      shadowComparisonError: emptyStringRef(),
      operatorRecommendationLastLoadedLabel: ref('Loaded 10:00'),
      shadowPreviewLastLoadedLabel: ref('Loaded 10:01'),
      visibleOperatorRecommendation: computed(() => null),
      hourlyRecommendationRows: computed(() => []),
      hourlyRecommendationEmptyMessage: computed(() => ''),
      shouldAutoLoadBaselinePreview: ref(true),
      clearBaselinePreviewError: vi.fn(),
      clearOperatorRecommendationError: vi.fn()
    })

    expect(state.activeMarketPreviewLoading.value).toBe(true)
  })
})
