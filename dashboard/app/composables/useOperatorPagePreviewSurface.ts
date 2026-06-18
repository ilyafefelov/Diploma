import { computed, type ComputedRef, type Ref } from 'vue'

import type { OperatorRecommendationResponse } from '~/types/control-plane'
import type { OperatorPreviewSourceId, ShadowHourlyRecommendationRow } from '~/utils/operatorShadowPreview'
import { isLiveHfSafeSwitchPreviewSource } from '~/utils/operatorShadowPreview'
import { useSelectedOperatorPreviewState } from './useSelectedOperatorPreviewState'

interface OperatorPagePreviewSurfaceInput {
  selectedPreviewSourceId: Ref<OperatorPreviewSourceId>
  operatorPreviewEnsureMessage: Ref<string>
  baselinePreviewEnsureMessage: Ref<string>
  isOperatorPreviewEnsuring: Ref<boolean>
  isBaselinePreviewEnsuring: Ref<boolean>
  defenseActiveErrorCount: ComputedRef<number>
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

export const useOperatorPagePreviewSurface = (input: OperatorPagePreviewSurfaceInput) => {
  const activePreviewEnsureMessage = computed(() => (
    input.operatorPreviewEnsureMessage.value || input.baselinePreviewEnsureMessage.value
  ))
  const isPreviewEnsuring = computed(() => (
    input.isOperatorPreviewEnsuring.value || input.isBaselinePreviewEnsuring.value
  ))
  const operatorReadModelErrorCount = computed(() => {
    return input.defenseActiveErrorCount.value
      + (input.operatorRecommendationError.value ? 1 : 0)
      + (input.shadowPreviewError.value ? 1 : 0)
      + (input.shadowComparisonError.value ? 1 : 0)
  })
  const isLiveHfShadowPreviewSelected = computed(() => (
    isLiveHfSafeSwitchPreviewSource(input.selectedPreviewSourceId.value)
  ))

  const selectedPreviewState = useSelectedOperatorPreviewState({
    isLiveHfShadowPreviewSelected,
    isOperatorRecommendationLoading: input.isOperatorRecommendationLoading,
    isShadowPreviewLoading: input.isShadowPreviewLoading,
    isSignalPreviewLoading: input.isSignalPreviewLoading,
    operatorRecommendationError: input.operatorRecommendationError,
    baselinePreviewError: input.baselinePreviewError,
    shadowPreviewError: input.shadowPreviewError,
    error: input.error,
    weatherError: input.weatherError,
    signalPreviewError: input.signalPreviewError,
    shadowComparisonError: input.shadowComparisonError,
    operatorRecommendationLastLoadedLabel: input.operatorRecommendationLastLoadedLabel,
    shadowPreviewLastLoadedLabel: input.shadowPreviewLastLoadedLabel,
    visibleOperatorRecommendation: input.visibleOperatorRecommendation,
    hourlyRecommendationRows: input.hourlyRecommendationRows,
    hourlyRecommendationEmptyMessage: input.hourlyRecommendationEmptyMessage,
    shouldAutoLoadBaselinePreview: input.shouldAutoLoadBaselinePreview,
    clearBaselinePreviewError: input.clearBaselinePreviewError,
    clearOperatorRecommendationError: input.clearOperatorRecommendationError
  })

  return {
    ...selectedPreviewState,
    activePreviewEnsureMessage,
    isLiveHfShadowPreviewSelected,
    isPreviewEnsuring,
    operatorReadModelErrorCount
  }
}
