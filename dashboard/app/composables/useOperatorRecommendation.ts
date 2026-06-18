import { computed, ref, watch } from 'vue'

import type { OperatorRecommendationResponse } from '~/types/control-plane'
import type { OperatorMarketVenue } from '~/types/operator-dashboard'
import {
  buildOperatorPreviewQuery,
  formatOperatorPreviewErrorMessage,
  isRecoverableOperatorPreviewMaterializationError
} from '../utils/operatorPreviewControls'
import { useOperatorPreviewEnsure } from './useOperatorPreviewEnsure'

const DEFAULT_MARKET_VENUE: Readonly<{ value: OperatorMarketVenue }> = { value: 'DAM' }
const DEFAULT_TARGET_DELIVERY_DATE: Readonly<{ value: string | null }> = { value: null }
const DEFAULT_AUTO_LOAD: Readonly<{ value: boolean }> = { value: true }

export const useOperatorRecommendation = (
  selectedTenantId: Readonly<{ value: string }>,
  selectedStrategyId: Readonly<{ value: string }>,
  selectedMarketVenue: Readonly<{ value: OperatorMarketVenue }> = DEFAULT_MARKET_VENUE,
  selectedTargetDeliveryDate: Readonly<{ value: string | null }> = DEFAULT_TARGET_DELIVERY_DATE,
  shouldAutoLoad: Readonly<{ value: boolean }> = DEFAULT_AUTO_LOAD
) => {
  const operatorRecommendation = ref<OperatorRecommendationResponse | null>(null)
  const isLoading = ref(false)
  const error = ref('')
  const lastLoadedAt = ref<number | null>(null)
  let requestSequence = 0
  const {
    clearOperatorPreviewEnsure,
    ensureOperatorPreview,
    isEnsuringPreview,
    operatorPreviewEnsure,
    operatorPreviewEnsureMessage
  } = useOperatorPreviewEnsure(
    selectedTenantId,
    selectedMarketVenue,
    selectedTargetDeliveryDate,
    selectedStrategyId
  )

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
    const requestId = ++requestSequence

    if (!selectedTenantId.value) {
      operatorRecommendation.value = null
      return
    }

    isLoading.value = true
    error.value = ''

    try {
      const response = await fetchOperatorRecommendation()
      if (requestId !== requestSequence) {
        return
      }

      operatorRecommendation.value = response
      clearOperatorPreviewEnsure()
      lastLoadedAt.value = Date.now()
    } catch (unknownError) {
      if (requestId !== requestSequence) {
        return
      }

      const formattedMessage = formatOperatorPreviewErrorMessage(
        unknownError,
        'Unable to load operator recommendation.'
      )
      if (
        selectedTargetDeliveryDate.value
        && isRecoverableOperatorPreviewMaterializationError(formattedMessage)
      ) {
        try {
          const ensured = await ensureOperatorPreview()
          if (requestId !== requestSequence) {
            return
          }
          if (ensured) {
            const retryResponse = await fetchOperatorRecommendation()
            if (requestId !== requestSequence) {
              return
            }
            operatorRecommendation.value = retryResponse
            operatorPreviewEnsureMessage.value = ''
            lastLoadedAt.value = Date.now()
            return
          }
          operatorRecommendation.value = null
          error.value = operatorPreviewEnsure.value?.message || formattedMessage
        } catch (ensureError) {
          if (requestId !== requestSequence) {
            return
          }
          operatorRecommendation.value = null
          error.value = formatOperatorPreviewErrorMessage(
            ensureError,
            operatorPreviewEnsure.value?.message || formattedMessage
          )
        }
      } else {
        operatorRecommendation.value = null
        error.value = formattedMessage
      }
    } finally {
      if (requestId === requestSequence) {
        isLoading.value = false
      }
    }
  }

  const fetchOperatorRecommendation = async (): Promise<OperatorRecommendationResponse> => {
    return await $fetch<OperatorRecommendationResponse>(
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
  }

  watch([selectedTenantId, selectedStrategyId, selectedMarketVenue, selectedTargetDeliveryDate, shouldAutoLoad], async () => {
    if (!shouldAutoLoad.value) {
      requestSequence += 1
      operatorRecommendation.value = null
      isLoading.value = false
      error.value = ''
      clearOperatorPreviewEnsure()
      return
    }
    await loadOperatorRecommendation()
  })

  return {
    operatorRecommendation,
    isLoading,
    error,
    clearError,
    isEnsuringPreview,
    lastLoadedLabel,
    loadOperatorRecommendation,
    operatorPreviewEnsure,
    operatorPreviewEnsureMessage
  }
}
