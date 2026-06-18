import { computed, watch, type ComputedRef, type Ref } from 'vue'

import type { OperatorRecommendationResponse } from '~/types/control-plane'
import type { ShadowHourlyRecommendationRow } from '~/utils/operatorShadowPreview'

interface SelectedOperatorPreviewStateInput {
  isLiveHfShadowPreviewSelected: ComputedRef<boolean>
  isOperatorRecommendationLoading: Ref<boolean>
  isShadowPreviewLoading: Ref<boolean>
  isSignalPreviewLoading: Ref<boolean>
  operatorRecommendationError: Ref<string>
  baselinePreviewError: Ref<string>
  shadowPreviewError: Ref<string>
  error: Ref<string>
  weatherError: Ref<string>
  signalPreviewError: Ref<string>
  shadowComparisonError: Ref<string>
  operatorRecommendationLastLoadedLabel: Ref<string>
  shadowPreviewLastLoadedLabel: Ref<string>
  visibleOperatorRecommendation: ComputedRef<OperatorRecommendationResponse | null>
  hourlyRecommendationRows: ComputedRef<ShadowHourlyRecommendationRow[]>
  hourlyRecommendationEmptyMessage: ComputedRef<string>
  shouldAutoLoadBaselinePreview: Ref<boolean>
  clearBaselinePreviewError: () => void
  clearOperatorRecommendationError: () => void
}

export const useSelectedOperatorPreviewState = ({
  isLiveHfShadowPreviewSelected,
  isOperatorRecommendationLoading,
  isShadowPreviewLoading,
  isSignalPreviewLoading,
  operatorRecommendationError,
  baselinePreviewError,
  shadowPreviewError,
  error,
  weatherError,
  signalPreviewError,
  shadowComparisonError,
  operatorRecommendationLastLoadedLabel,
  shadowPreviewLastLoadedLabel,
  visibleOperatorRecommendation,
  hourlyRecommendationRows,
  hourlyRecommendationEmptyMessage,
  shouldAutoLoadBaselinePreview,
  clearBaselinePreviewError,
  clearOperatorRecommendationError
}: SelectedOperatorPreviewStateInput) => {
  watch(isLiveHfShadowPreviewSelected, (isSelected) => {
    shouldAutoLoadBaselinePreview.value = !isSelected

    if (isSelected) {
      clearOperatorRecommendationError()
      clearBaselinePreviewError()
    }
  }, { immediate: true })

  const selectedRecommendationBlocked = computed(() => {
    if (isLiveHfShadowPreviewSelected.value) {
      return Boolean(shadowPreviewError.value)
    }

    return isOperatorRecommendationLoading.value
      || Boolean(operatorRecommendationError.value)
      || Boolean(baselinePreviewError.value)
  })
  const selectedVisibleOperatorRecommendation = computed(() => {
    if (selectedRecommendationBlocked.value) {
      return null
    }

    return visibleOperatorRecommendation.value
  })
  const selectedHourlyRecommendationRows = computed(() => {
    return selectedRecommendationBlocked.value ? [] : hourlyRecommendationRows.value
  })
  const selectedHourlyRecommendationEmptyMessage = computed(() => {
    if (selectedRecommendationBlocked.value) {
      return 'Selected DAM/IDM preview is pending or blocked; no BUY/SELL/HOLD preference is shown.'
    }

    return hourlyRecommendationEmptyMessage.value
  })
  const bestValidStrategyComparisonRecommendation = computed(() => {
    return isLiveHfShadowPreviewSelected.value ? null : selectedVisibleOperatorRecommendation.value
  })
  const activeMarketPreviewError = computed(() => {
    return isLiveHfShadowPreviewSelected.value
      ? shadowPreviewError.value
      : operatorRecommendationError.value || baselinePreviewError.value
  })
  const activeMarketPreviewLoading = computed(() => {
    if (isLiveHfShadowPreviewSelected.value) {
      return isShadowPreviewLoading.value
    }

    return isSignalPreviewLoading.value || isOperatorRecommendationLoading.value
  })
  const activeMarketPreviewLastLoadedLabel = computed(() => {
    return isLiveHfShadowPreviewSelected.value
      ? shadowPreviewLastLoadedLabel.value
      : operatorRecommendationLastLoadedLabel.value
  })
  const activeSurfaceErrorMessage = computed(() => {
    const marketPreviewError = isLiveHfShadowPreviewSelected.value
      ? shadowPreviewError.value
      : operatorRecommendationError.value || baselinePreviewError.value || shadowPreviewError.value

    return error.value
      || weatherError.value
      || signalPreviewError.value
      || marketPreviewError
      || shadowComparisonError.value
  })

  return {
    activeMarketPreviewError,
    activeMarketPreviewLastLoadedLabel,
    activeMarketPreviewLoading,
    activeSurfaceErrorMessage,
    bestValidStrategyComparisonRecommendation,
    selectedHourlyRecommendationEmptyMessage,
    selectedHourlyRecommendationRows,
    selectedVisibleOperatorRecommendation
  }
}
