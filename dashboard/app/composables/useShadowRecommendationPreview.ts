import { computed, ref, watch } from 'vue'

import type { ShadowRecommendationPreviewResponse } from '../types/control-plane'
import type { OperatorMarketVenue } from '../types/operator-dashboard'
import type { OperatorPreviewSourceId } from '../utils/operatorShadowPreview'
import { resolveShadowPreviewTargetDeliveryDate, shouldLoadShadowPreview } from '../utils/operatorShadowPreview'

const DEFAULT_MARKET_VENUE: Readonly<{ value: OperatorMarketVenue }> = { value: 'DAM' }
const DEFAULT_TARGET_DELIVERY_DATE: Readonly<{ value: string | null }> = { value: null }

export const useShadowRecommendationPreview = (
  selectedTenantId: Readonly<{ value: string }>,
  selectedPreviewSourceId: Readonly<{ value: OperatorPreviewSourceId }>,
  targetDeliveryWindowStart: Readonly<{ value: string | null | undefined }>,
  selectedMarketVenue: Readonly<{ value: OperatorMarketVenue }> = DEFAULT_MARKET_VENUE,
  selectedTargetDeliveryDate: Readonly<{ value: string | null }> = DEFAULT_TARGET_DELIVERY_DATE
) => {
  const shadowPreview = ref<ShadowRecommendationPreviewResponse | null>(null)
  const isLoading = ref(false)
  const error = ref('')
  const lastLoadedAt = ref<number | null>(null)
  let requestSequence = 0

  const clearError = (): void => {
    error.value = ''
  }

  const lastLoadedLabel = computed(() => {
    if (!lastLoadedAt.value) {
      return 'Not loaded yet'
    }

    return new Date(lastLoadedAt.value).toLocaleTimeString('en-GB', {
      hour: '2-digit',
      minute: '2-digit'
    })
  })

  const loadShadowRecommendationPreview = async (): Promise<void> => {
    const requestId = ++requestSequence

    if (!selectedTenantId.value || !shouldLoadShadowPreview(selectedPreviewSourceId.value)) {
      shadowPreview.value = null
      return
    }

    isLoading.value = true
    error.value = ''
    shadowPreview.value = null
    const targetDeliveryDate = resolveShadowPreviewTargetDeliveryDate(
      selectedPreviewSourceId.value,
      selectedTargetDeliveryDate.value
    )

    try {
      const response = await $fetch<ShadowRecommendationPreviewResponse>(
        '/api/control-plane/dashboard/shadow-recommendation-preview',
        {
          query: {
            tenant_id: selectedTenantId.value,
            preview_source: selectedPreviewSourceId.value,
            market_venue: selectedMarketVenue.value,
            ...(targetDeliveryDate
              ? { target_delivery_date: targetDeliveryDate }
              : {}),
            ...(targetDeliveryWindowStart.value
              ? { target_delivery_window_start: targetDeliveryWindowStart.value }
              : {})
          }
        }
      )
      if (requestId !== requestSequence) {
        return
      }

      shadowPreview.value = response
      lastLoadedAt.value = Date.now()
    } catch (unknownError) {
      if (requestId !== requestSequence) {
        return
      }

      shadowPreview.value = null
      error.value = unknownError instanceof Error ? unknownError.message : 'Unable to load shadow recommendation preview.'
    } finally {
      if (requestId === requestSequence) {
        isLoading.value = false
      }
    }
  }

  watch(
    () => [
      selectedTenantId.value,
      selectedPreviewSourceId.value,
      targetDeliveryWindowStart.value,
      selectedMarketVenue.value,
      selectedTargetDeliveryDate.value
    ] as const,
    async () => {
      await loadShadowRecommendationPreview()
    }
  )

  return {
    shadowPreview,
    isLoading,
    error,
    clearError,
    lastLoadedLabel,
    loadShadowRecommendationPreview
  }
}
