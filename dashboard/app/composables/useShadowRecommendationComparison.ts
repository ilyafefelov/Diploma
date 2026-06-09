import { ref, watch } from 'vue'

import type { ShadowRecommendationPreviewResponse } from '../types/control-plane'
import type { OperatorMarketVenue } from '../types/operator-dashboard'
import {
  comparisonPreviewSourceIdsFor,
  isLiveHfSafeSwitchPreviewSource,
  resolveShadowPreviewTargetDeliveryDate,
  type OperatorPreviewSourceId
} from '../utils/operatorShadowPreview'

const DEFAULT_MARKET_VENUE: Readonly<{ value: OperatorMarketVenue }> = { value: 'DAM' }
const DEFAULT_TARGET_DELIVERY_DATE: Readonly<{ value: string | null }> = { value: null }
const DEFAULT_PREVIEW_SOURCE: Readonly<{ value: OperatorPreviewSourceId }> = { value: 'best_valid' }

export const useShadowRecommendationComparison = (
  selectedTenantId: Readonly<{ value: string }>,
  targetDeliveryWindowStart: Readonly<{ value: string | null | undefined }>,
  selectedMarketVenue: Readonly<{ value: OperatorMarketVenue }> = DEFAULT_MARKET_VENUE,
  selectedTargetDeliveryDate: Readonly<{ value: string | null }> = DEFAULT_TARGET_DELIVERY_DATE,
  selectedPreviewSourceId: Readonly<{ value: OperatorPreviewSourceId }> = DEFAULT_PREVIEW_SOURCE
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

    const targetDeliveryDate = resolveShadowPreviewTargetDeliveryDate(
      selectedPreviewSourceId.value,
      selectedTargetDeliveryDate.value
    )
    const usesDateBasedLiveHfComparison = isLiveHfSafeSwitchPreviewSource(selectedPreviewSourceId.value)
    const queryBase = {
      tenant_id: selectedTenantId.value,
      market_venue: selectedMarketVenue.value,
      ...(targetDeliveryDate
        ? { target_delivery_date: targetDeliveryDate }
        : {}),
      ...(!usesDateBasedLiveHfComparison && targetDeliveryWindowStart.value
        ? { target_delivery_window_start: targetDeliveryWindowStart.value }
        : {})
    }

    const results = await Promise.allSettled(
      comparisonPreviewSourceIdsFor(selectedPreviewSourceId.value).map(previewSource => $fetch<ShadowRecommendationPreviewResponse>(
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
      targetDeliveryWindowStart.value,
      selectedMarketVenue.value,
      selectedTargetDeliveryDate.value,
      selectedPreviewSourceId.value
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
