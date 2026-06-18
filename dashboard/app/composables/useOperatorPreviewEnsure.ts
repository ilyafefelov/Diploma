import { ref } from 'vue'

import type { OperatorPreviewEnsureResponse } from '~/types/control-plane'
import type { OperatorMarketVenue } from '~/types/operator-dashboard'
import { buildOperatorPreviewQuery } from '../utils/operatorPreviewControls'

const DEFAULT_MARKET_VENUE: Readonly<{ value: OperatorMarketVenue }> = { value: 'DAM' }
const DEFAULT_TARGET_DELIVERY_DATE: Readonly<{ value: string | null }> = { value: null }
const DEFAULT_STRATEGY_ID: Readonly<{ value: string | null }> = { value: null }

export const useOperatorPreviewEnsure = (
  selectedTenantId: Readonly<{ value: string }>,
  selectedMarketVenue: Readonly<{ value: OperatorMarketVenue }> = DEFAULT_MARKET_VENUE,
  selectedTargetDeliveryDate: Readonly<{ value: string | null }> = DEFAULT_TARGET_DELIVERY_DATE,
  selectedStrategyId: Readonly<{ value: string | null }> = DEFAULT_STRATEGY_ID
) => {
  const operatorPreviewEnsure = ref<OperatorPreviewEnsureResponse | null>(null)
  const isEnsuringPreview = ref(false)
  const operatorPreviewEnsureMessage = ref('')

  const clearOperatorPreviewEnsure = (): void => {
    operatorPreviewEnsure.value = null
    operatorPreviewEnsureMessage.value = ''
  }

  const ensureOperatorPreview = async (): Promise<boolean> => {
    if (!selectedTenantId.value || !selectedTargetDeliveryDate.value) {
      return false
    }

    isEnsuringPreview.value = true
    operatorPreviewEnsureMessage.value = 'Source-backed rows unavailable; fetching OREE rows and materializing the preview forecast store.'

    try {
      const response = await $fetch<OperatorPreviewEnsureResponse>(
        '/api/control-plane/dashboard/operator-preview/ensure',
        {
          method: 'POST',
          query: buildOperatorPreviewQuery(
            selectedTenantId.value,
            selectedMarketVenue.value,
            selectedTargetDeliveryDate.value,
            selectedStrategyId.value ?? undefined
          )
        }
      )
      operatorPreviewEnsure.value = response
      const isReady = response.status === 'ready' || response.status === 'materialized'
      operatorPreviewEnsureMessage.value = isReady
        ? `${response.message}. Refreshing preview read model.`
        : ''
      return isReady
    } finally {
      isEnsuringPreview.value = false
    }
  }

  return {
    clearOperatorPreviewEnsure,
    ensureOperatorPreview,
    isEnsuringPreview,
    operatorPreviewEnsure,
    operatorPreviewEnsureMessage
  }
}
