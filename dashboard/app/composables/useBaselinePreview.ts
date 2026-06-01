import { computed, ref, watch } from 'vue'

import type { BaselineLpPreview } from '~/types/control-plane'
import type { OperatorMarketVenue } from '~/types/operator-dashboard'
import {
  buildOperatorPreviewQuery,
  formatOperatorPreviewErrorMessage
} from '../utils/operatorPreviewControls'

const DEFAULT_MARKET_VENUE: Readonly<{ value: OperatorMarketVenue }> = { value: 'DAM' }
const DEFAULT_TARGET_DELIVERY_DATE: Readonly<{ value: string | null }> = { value: null }

export const useBaselinePreview = (
  selectedTenantId: Readonly<{ value: string }>,
  selectedMarketVenue: Readonly<{ value: OperatorMarketVenue }> = DEFAULT_MARKET_VENUE,
  selectedTargetDeliveryDate: Readonly<{ value: string | null }> = DEFAULT_TARGET_DELIVERY_DATE
) => {
  const baselinePreview = ref<BaselineLpPreview | null>(null)
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

  const loadBaselinePreview = async (): Promise<void> => {
    const requestId = ++requestSequence

    if (!selectedTenantId.value) {
      baselinePreview.value = null
      return
    }

    isLoading.value = true
    error.value = ''

    try {
      const response = await $fetch<BaselineLpPreview>('/api/control-plane/dashboard/baseline-lp-preview', {
        query: buildOperatorPreviewQuery(
          selectedTenantId.value,
          selectedMarketVenue.value,
          selectedTargetDeliveryDate.value
        )
      })
      if (requestId !== requestSequence) {
        return
      }

      baselinePreview.value = response
      lastLoadedAt.value = Date.now()
    } catch (unknownError) {
      if (requestId !== requestSequence) {
        return
      }

      baselinePreview.value = null
      error.value = formatOperatorPreviewErrorMessage(unknownError, 'Unable to load baseline LP preview.')
    } finally {
      if (requestId === requestSequence) {
        isLoading.value = false
      }
    }
  }

  watch([selectedTenantId, selectedMarketVenue, selectedTargetDeliveryDate], async () => {
    await loadBaselinePreview()
  })

  return {
    baselinePreview,
    isLoading,
    error,
    clearError,
    lastLoadedLabel,
    loadBaselinePreview
  }
}
