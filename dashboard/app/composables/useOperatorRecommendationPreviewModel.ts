import { computed, ref, type Ref } from 'vue'

import type { BaselineLpPreview } from '~/types/control-plane'
import type { OperatorMarketVenue } from '~/types/operator-dashboard'
import {
  adaptShadowPreviewToOperatorRecommendation,
  buildOperatorHourlyRecommendationRows,
  buildShadowHourlyRecommendationRows,
  previewModeLabel,
  shouldLoadShadowPreview,
  type OperatorPreviewSourceId
} from '~/utils/operatorShadowPreview'
import { useOperatorRecommendation } from './useOperatorRecommendation'
import { useShadowRecommendationComparison } from './useShadowRecommendationComparison'
import { useShadowRecommendationPreview } from './useShadowRecommendationPreview'

interface OperatorRecommendationPreviewModelInput {
  selectedTenantId: Readonly<Ref<string>>
  selectedMarketVenue: Readonly<Ref<OperatorMarketVenue>>
  selectedTargetDeliveryDate: Readonly<Ref<string | null>>
  baselinePreview: Readonly<Ref<BaselineLpPreview | null>>
}

export const useOperatorRecommendationPreviewModel = ({
  selectedTenantId,
  selectedMarketVenue,
  selectedTargetDeliveryDate,
  baselinePreview
}: OperatorRecommendationPreviewModelInput) => {
  const selectedOperatorStrategyId = ref('schedule_value_learner_v2_plus')
  const selectedPreviewSourceId = ref<OperatorPreviewSourceId>('best_valid')

  const {
    operatorRecommendation,
    isLoading: isOperatorRecommendationLoading,
    error: operatorRecommendationError,
    clearError: clearOperatorRecommendationError,
    loadOperatorRecommendation
  } = useOperatorRecommendation(
    selectedTenantId,
    selectedOperatorStrategyId,
    selectedMarketVenue,
    selectedTargetDeliveryDate
  )

  const shadowDeliveryWindowStart = computed(() => {
    return operatorRecommendation.value?.target_delivery_window_start ?? null
  })

  const {
    shadowPreview,
    isLoading: isShadowPreviewLoading,
    error: shadowPreviewError,
    clearError: clearShadowPreviewError,
    lastLoadedLabel: shadowPreviewLastLoadedLabel,
    loadShadowRecommendationPreview
  } = useShadowRecommendationPreview(selectedTenantId, selectedPreviewSourceId, shadowDeliveryWindowStart)

  const {
    shadowComparisonPreviews,
    isLoading: isShadowComparisonLoading,
    error: shadowComparisonError,
    clearError: clearShadowComparisonError,
    loadShadowComparisonPreviews
  } = useShadowRecommendationComparison(selectedTenantId, shadowDeliveryWindowStart)

  const visibleOperatorRecommendation = computed(() => adaptShadowPreviewToOperatorRecommendation(
    operatorRecommendation.value,
    shadowPreview.value,
    selectedPreviewSourceId.value
  ))
  const selectedPreviewSourceLabel = computed(() => previewModeLabel(
    selectedPreviewSourceId.value,
    shadowPreview.value
  ))
  const hourlyRecommendationRows = computed(() => {
    const batteryCapacityMwh = baselinePreview.value?.battery_metrics.capacity_mwh ?? null
    if (selectedPreviewSourceId.value === 'best_valid') {
      return buildOperatorHourlyRecommendationRows(visibleOperatorRecommendation.value, batteryCapacityMwh)
    }

    return buildShadowHourlyRecommendationRows(
      shadowPreview.value,
      batteryCapacityMwh,
      shadowPreview.value?.interval_minutes ?? visibleOperatorRecommendation.value?.interval_minutes ?? 60
    )
  })
  const hourlyRecommendationEmptyMessage = computed(() => {
    if (selectedPreviewSourceId.value === 'v13_dt_lava_promoted_training') {
      return 'Blocked by V13 source-readiness; no promoted schedule exists; V2+ remains fallback/default.'
    }
    if (selectedPreviewSourceId.value === 'best_valid') {
      return 'Best-valid recommendation schedule is not loaded yet. Refresh the preview read model.'
    }
    return 'Selected shadow source has no hourly schedule rows. It remains roadmap evidence only.'
  })

  const loadRecommendationSurfaces = async (): Promise<void> => {
    await loadOperatorRecommendation()
    await loadShadowComparisonPreviews()
    await loadShadowRecommendationPreview()
  }

  const refreshVisibleRecommendation = async (): Promise<void> => {
    await loadOperatorRecommendation()
    await loadShadowComparisonPreviews()
    if (shouldLoadShadowPreview(selectedPreviewSourceId.value)) {
      await loadShadowRecommendationPreview()
    }
  }

  return {
    clearOperatorRecommendationError,
    clearShadowComparisonError,
    clearShadowPreviewError,
    hourlyRecommendationEmptyMessage,
    hourlyRecommendationRows,
    isOperatorRecommendationLoading,
    isShadowComparisonLoading,
    isShadowPreviewLoading,
    loadRecommendationSurfaces,
    operatorRecommendation,
    operatorRecommendationError,
    refreshVisibleRecommendation,
    selectedOperatorStrategyId,
    selectedPreviewSourceId,
    selectedPreviewSourceLabel,
    shadowComparisonError,
    shadowComparisonPreviews,
    shadowPreview,
    shadowPreviewError,
    shadowPreviewLastLoadedLabel,
    visibleOperatorRecommendation
  }
}
