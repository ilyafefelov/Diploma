import { computed, ref, watch } from 'vue'

import type { ShadowRecommendationPreviewResponse } from '~/types/control-plane'
import type { OperatorPreviewSourceId } from '~/utils/operatorShadowPreview'
import { shouldLoadShadowPreview } from '~/utils/operatorShadowPreview'

export const useShadowRecommendationPreview = (
  selectedTenantId: Readonly<{ value: string }>,
  selectedPreviewSourceId: Readonly<{ value: OperatorPreviewSourceId }>,
  targetDeliveryWindowStart: Readonly<{ value: string | null | undefined }>
) => {
  const shadowPreview = ref<ShadowRecommendationPreviewResponse | null>(null)
  const isLoading = ref(false)
  const error = ref('')
  const lastLoadedAt = ref<number | null>(null)

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
    if (!selectedTenantId.value || !shouldLoadShadowPreview(selectedPreviewSourceId.value)) {
      shadowPreview.value = null
      return
    }

    isLoading.value = true
    error.value = ''

    try {
      shadowPreview.value = await $fetch<ShadowRecommendationPreviewResponse>(
        '/api/control-plane/dashboard/shadow-recommendation-preview',
        {
          query: {
            tenant_id: selectedTenantId.value,
            preview_source: selectedPreviewSourceId.value,
            ...(targetDeliveryWindowStart.value
              ? { target_delivery_window_start: targetDeliveryWindowStart.value }
              : {})
          }
        }
      )
      lastLoadedAt.value = Date.now()
    } catch (unknownError) {
      shadowPreview.value = null
      error.value = unknownError instanceof Error ? unknownError.message : 'Unable to load shadow recommendation preview.'
    } finally {
      isLoading.value = false
    }
  }

  watch(
    () => [
      selectedTenantId.value,
      selectedPreviewSourceId.value,
      targetDeliveryWindowStart.value
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
