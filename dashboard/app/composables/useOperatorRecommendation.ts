import { computed, ref, watch } from 'vue'

import type { OperatorRecommendationResponse } from '~/types/control-plane'
import type { OperatorMarketVenue } from '~/types/operator-dashboard'
import { buildOperatorPreviewQuery } from '../utils/operatorPreviewControls'

const DEFAULT_MARKET_VENUE: Readonly<{ value: OperatorMarketVenue }> = { value: 'DAM' }
const DEFAULT_TARGET_DELIVERY_DATE: Readonly<{ value: string | null }> = { value: null }

export const useOperatorRecommendation = (
  selectedTenantId: Readonly<{ value: string }>,
  selectedStrategyId: Readonly<{ value: string }>,
  selectedMarketVenue: Readonly<{ value: OperatorMarketVenue }> = DEFAULT_MARKET_VENUE,
  selectedTargetDeliveryDate: Readonly<{ value: string | null }> = DEFAULT_TARGET_DELIVERY_DATE
) => {
  const operatorRecommendation = ref<OperatorRecommendationResponse | null>(null)
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

  const loadOperatorRecommendation = async (): Promise<void> => {
    if (!selectedTenantId.value) {
      operatorRecommendation.value = null
      return
    }

    isLoading.value = true
    error.value = ''

    try {
      operatorRecommendation.value = await $fetch<OperatorRecommendationResponse>(
        '/api/control-plane/dashboard/operator-recommendation',
        {
          query: buildOperatorPreviewQuery(
            selectedTenantId.value,
            selectedMarketVenue.value,
            selectedTargetDeliveryDate.value,
            selectedStrategyId.value
          )
        }
      )
      lastLoadedAt.value = Date.now()
    } catch (unknownError) {
      operatorRecommendation.value = null
      error.value = unknownError instanceof Error ? unknownError.message : 'Unable to load operator recommendation.'
    } finally {
      isLoading.value = false
    }
  }

  watch([selectedTenantId, selectedStrategyId, selectedMarketVenue, selectedTargetDeliveryDate], async () => {
    await loadOperatorRecommendation()
  })

  return {
    operatorRecommendation,
    isLoading,
    error,
    clearError,
    lastLoadedLabel,
    loadOperatorRecommendation
  }
}
