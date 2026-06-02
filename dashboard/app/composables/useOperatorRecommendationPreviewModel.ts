import { computed, ref, type Ref } from 'vue'

import type { BaselineLpPreview } from '../types/control-plane'
import type { OperatorMarketVenue } from '../types/operator-dashboard'
import {
  resolveValueAlignedHfShadowDemoScenario,
  type ValueAlignedHfShadowDemoScenarioId
} from '../lib/operator-future/operatorFuturePreviewSources'
import {
  adaptShadowPreviewToOperatorRecommendation,
  buildOperatorHourlyRecommendationRows,
  buildShadowHourlyRecommendationRows,
  isLiveHfSafeSwitchPreviewSource,
  previewModeLabel,
  shouldLoadShadowPreview,
  type OperatorPreviewSourceId
} from '../utils/operatorShadowPreview'
import { useOperatorRecommendation } from './useOperatorRecommendation'
import { useShadowRecommendationComparison } from './useShadowRecommendationComparison'
import { useShadowRecommendationPreview } from './useShadowRecommendationPreview'

interface OperatorRecommendationPreviewModelInput {
  selectedTenantId: Readonly<Ref<string>>
  selectedMarketVenue: Ref<OperatorMarketVenue>
  selectedTargetDeliveryDate: Ref<string | null>
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
    lastLoadedLabel: operatorRecommendationLastLoadedLabel,
    loadOperatorRecommendation
  } = useOperatorRecommendation(
    selectedTenantId,
    selectedOperatorStrategyId,
    selectedMarketVenue,
    selectedTargetDeliveryDate,
    computed(() => !isLiveHfSafeSwitchPreviewSource(selectedPreviewSourceId.value))
  )

  const shadowDeliveryWindowStart = computed(() => {
    if (isLiveHfSafeSwitchPreviewSource(selectedPreviewSourceId.value)) {
      return null
    }
    return operatorRecommendation.value?.target_delivery_window_start ?? null
  })

  const {
    shadowPreview,
    isLoading: isShadowPreviewLoading,
    error: shadowPreviewError,
    clearError: clearShadowPreviewError,
    lastLoadedLabel: shadowPreviewLastLoadedLabel,
    loadShadowRecommendationPreview
  } = useShadowRecommendationPreview(
    selectedTenantId,
    selectedPreviewSourceId,
    shadowDeliveryWindowStart,
    selectedMarketVenue,
    selectedTargetDeliveryDate
  )

  const {
    shadowComparisonPreviews,
    isLoading: isShadowComparisonLoading,
    error: shadowComparisonError,
    clearError: clearShadowComparisonError,
    loadShadowComparisonPreviews
  } = useShadowRecommendationComparison(
    selectedTenantId,
    shadowDeliveryWindowStart,
    selectedMarketVenue,
    selectedTargetDeliveryDate,
    selectedPreviewSourceId
  )

  const visibleOperatorRecommendation = computed(() => {
    if (
      operatorRecommendationError.value
      && !isLiveHfSafeSwitchPreviewSource(selectedPreviewSourceId.value)
    ) {
      return null
    }

    return adaptShadowPreviewToOperatorRecommendation(
      operatorRecommendation.value,
      shadowPreview.value,
      selectedPreviewSourceId.value
    )
  })
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
    if (
      isLiveHfSafeSwitchPreviewSource(selectedPreviewSourceId.value)
      && shadowPreview.value?.preview_status === 'blocked_missing_source_backed_price_context'
    ) {
      return 'HF shadow is blocked for the selected delivery date because no source-backed official or forecast price rows are available. No trade preview is shown.'
    }
    return 'Selected shadow source has no hourly schedule rows. It remains roadmap evidence only.'
  })

  const loadRecommendationSurfaces = async (): Promise<void> => {
    if (isLiveHfSafeSwitchPreviewSource(selectedPreviewSourceId.value)) {
      await loadShadowRecommendationPreview()
      await loadShadowComparisonPreviews()
      return
    }
    await loadOperatorRecommendation()
    await loadShadowComparisonPreviews()
    await loadShadowRecommendationPreview()
  }

  const refreshVisibleRecommendation = async (): Promise<void> => {
    if (isLiveHfSafeSwitchPreviewSource(selectedPreviewSourceId.value)) {
      await loadShadowRecommendationPreview()
      await loadShadowComparisonPreviews()
      return
    }
    await loadOperatorRecommendation()
    await loadShadowComparisonPreviews()
    if (shouldLoadShadowPreview(selectedPreviewSourceId.value)) {
      await loadShadowRecommendationPreview()
    }
  }

  const selectValueAlignedHfShadowDemoScenario = async (
    scenarioId: ValueAlignedHfShadowDemoScenarioId
  ): Promise<void> => {
    const scenario = resolveValueAlignedHfShadowDemoScenario(scenarioId)
    selectedPreviewSourceId.value = 'hf_live_safe_switch_value_aligned_shadow'
    selectedMarketVenue.value = scenario.marketVenue
    selectedTargetDeliveryDate.value = scenario.targetDeliveryDate
    await loadShadowRecommendationPreview()
    await loadShadowComparisonPreviews()
  }

  const selectPreviewSource = async (previewSourceId: OperatorPreviewSourceId): Promise<void> => {
    selectedPreviewSourceId.value = previewSourceId

    if (isLiveHfSafeSwitchPreviewSource(previewSourceId)) {
      await loadShadowRecommendationPreview()
      await loadShadowComparisonPreviews()
      return
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
    operatorRecommendationLastLoadedLabel,
    refreshVisibleRecommendation,
    selectValueAlignedHfShadowDemoScenario,
    selectedOperatorStrategyId,
    selectPreviewSource,
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
