import { ref, watch } from 'vue'

import type { ShadowRecommendationPreviewResponse } from '~/types/control-plane'
import { SHADOW_PREVIEW_SOURCE_IDS } from '~/utils/operatorShadowPreview'

export const useShadowRecommendationComparison = (
  selectedTenantId: Readonly<{ value: string }>,
  targetDeliveryWindowStart: Readonly<{ value: string | null | undefined }>
) => {
  const shadowComparisonPreviews = ref<ShadowRecommendationPreviewResponse[]>([])
  const isLoading = ref(false)
  const error = ref('')

  const clearError = (): void => {
    error.value = ''
  }

  const loadShadowComparisonPreviews = async (): Promise<void> => {
    if (!selectedTenantId.value) {
      shadowComparisonPreviews.value = []
      return
    }

    isLoading.value = true
    error.value = ''

    const queryBase = {
      tenant_id: selectedTenantId.value,
      ...(targetDeliveryWindowStart.value
        ? { target_delivery_window_start: targetDeliveryWindowStart.value }
        : {})
    }

    const results = await Promise.allSettled(
      SHADOW_PREVIEW_SOURCE_IDS.map(previewSource => $fetch<ShadowRecommendationPreviewResponse>(
        '/api/control-plane/dashboard/shadow-recommendation-preview',
        {
          query: {
            ...queryBase,
            preview_source: previewSource
          }
        }
      ))
    )

    shadowComparisonPreviews.value = results
      .filter((result): result is PromiseFulfilledResult<ShadowRecommendationPreviewResponse> => result.status === 'fulfilled')
      .map(result => result.value)

    const rejectedCount = results.filter(result => result.status === 'rejected').length
    if (rejectedCount > 0) {
      error.value = `${rejectedCount} shadow preview source${rejectedCount === 1 ? '' : 's'} failed to load.`
    }

    isLoading.value = false
  }

  watch(
    () => [
      selectedTenantId.value,
      targetDeliveryWindowStart.value
    ] as const,
    async () => {
      await loadShadowComparisonPreviews()
    }
  )

  return {
    shadowComparisonPreviews,
    isLoading,
    error,
    clearError,
    loadShadowComparisonPreviews
  }
}
