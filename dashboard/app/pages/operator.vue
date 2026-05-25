<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'

import {
  OperatorAlertBanner,
  OperatorBaselineConsole,
  OperatorDecisionEvidencePanel,
  OperatorFutureStackPanel,
  OperatorMarketConsole,
  OperatorMetricRibbon,
  OperatorResearchPanel,
  OperatorRightRail,
  OperatorScheduleDock,
  OperatorSidebar,
  OperatorTopBar
} from '~/components/dashboard/operator'
import { useBaselinePreview } from '~/composables/useBaselinePreview'
import { useControlPlaneRegistry } from '~/composables/useControlPlaneRegistry'
import { useOperatorDashboardViewModel } from '~/composables/useOperatorDashboardViewModel'
import { useOperatorRecommendation } from '~/composables/useOperatorRecommendation'
import { useShadowRecommendationComparison } from '~/composables/useShadowRecommendationComparison'
import { useShadowRecommendationPreview } from '~/composables/useShadowRecommendationPreview'
import { useSignalPreview } from '~/composables/useSignalPreview'
import { useWeatherControls } from '~/composables/useWeatherControls'
import { buildOperatorResearchMetrics } from '~/utils/operatorResearchMetrics'
import {
  adaptShadowPreviewToOperatorRecommendation,
  buildOperatorHourlyRecommendationRows,
  buildShadowHourlyRecommendationRows,
  previewModeLabel,
  shouldLoadShadowPreview,
  type OperatorPreviewSourceId
} from '~/utils/operatorShadowPreview'

const {
  tenants,
  selectedTenant,
  selectedTenantId,
  isLoading,
  error,
  lastLoadedAt,
  loadTenants,
  clearError,
  startAutoRefresh,
  stopAutoRefresh
} = useControlPlaneRegistry()

const {
  signalPreview,
  isLoading: isSignalPreviewLoading,
  error: signalPreviewError,
  clearError: clearSignalPreviewError,
  lastLoadedLabel: signalPreviewLastLoadedLabel,
  loadSignalPreview
} = useSignalPreview(selectedTenantId)

const {
  baselinePreview,
  isLoading: isBaselinePreviewLoading,
  error: baselinePreviewError,
  clearError: clearBaselinePreviewError,
  lastLoadedLabel: baselinePreviewLastLoadedLabel,
  loadBaselinePreview
} = useBaselinePreview(selectedTenantId)

const defense = useDefenseDashboard(selectedTenantId)

const {
  runConfig,
  materializeResult,
  operatorStatus,
  isPreparing,
  isMaterializing,
  error: weatherError,
  lastActionLabel,
  statusLabel,
  syncOperatorStatus,
  prepareRunConfig,
  materializeWeatherAssets,
  clearWeatherError
} = useWeatherControls()

const includePriceHistory = ref(true)
const explanationMode = ref<'mvp' | 'future'>('mvp')
const selectedOperatorStrategyId = ref('schedule_value_learner_v2_plus')
const selectedPreviewSourceId = ref<OperatorPreviewSourceId>('best_valid')

const {
  operatorRecommendation,
  isLoading: isOperatorRecommendationLoading,
  error: operatorRecommendationError,
  clearError: clearOperatorRecommendationError,
  loadOperatorRecommendation
} = useOperatorRecommendation(selectedTenantId, selectedOperatorStrategyId)
const shadowDeliveryWindowStart = computed(() => operatorRecommendation.value?.target_delivery_window_start ?? null)

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
const selectedPreviewSourceLabel = computed(() => previewModeLabel(selectedPreviewSourceId.value, shadowPreview.value))
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

const explanationModeLabel = computed(() => explanationMode.value === 'mvp' ? 'Selected V2+ evidence' : 'Research roadmap')

const operatorReadModelErrorCount = computed(() => {
  return defense.activeErrorCount.value
    + (operatorRecommendationError.value ? 1 : 0)
    + (shadowPreviewError.value ? 1 : 0)
    + (shadowComparisonError.value ? 1 : 0)
})
const defenseBenchmark = computed(() => defense.benchmark.value ?? null)
const defenseModelRows = computed(() => defense.modelRows.value)
const defenseSensitivity = computed(() => defense.sensitivity.value ?? null)
const defenseBatteryState = computed(() => defense.batteryState.value ?? null)
const defenseExogenousSignals = computed(() => defense.exogenousSignals.value ?? null)
const defenseFutureStack = computed(() => defense.futureStack.value ?? null)
const defenseDecisionPolicyPreview = computed(() => defense.dtPolicyPreview.value ?? null)
const defenseAcademicMvpReadiness = computed(() => defense.academicMvpReadiness.value ?? null)
const defenseGatekeeperValidationStatus = computed(() => defense.gatekeeperValidationStatus.value ?? null)
const defenseIsLoading = computed(() => defense.isLoading.value)
const defenseLastLoadedLabel = computed(() => defense.lastLoadedLabel.value)
const defenseActiveErrorCount = computed(() => defense.activeErrorCount.value)

const {
  activeAlertCount,
  activeRegistrySummary,
  batteryAssetLabel,
  batteryCapacityContextLabel,
  batterySocFormula,
  batterySocPercent,
  batterySocSourceLabel,
  batterySohFormula,
  batterySohProxyPercent,
  batterySohSourceLabel,
  batteryStatusLabel,
  batteryTelemetryIngestLabel,
  batteryTelemetryIngestTooltip,
  dispatchModeLabel,
  deliveryWindowLabel,
  gatekeeperActions,
  headlineMetrics,
  latestRecommendedPowerLabel,
  marketRegimeChips,
  moodChips,
  motiveItems,
  operatorClockLabel,
  operatorNavItems,
  registryEnvelope,
  selectedRunConfigSnippet,
  selectedTenantBadge,
  selectedTenantName,
  timelineSegments,
  weatherLocationLabel
} = useOperatorDashboardViewModel({
  tenants,
  selectedTenant,
  signalPreview,
  baselinePreview,
  operatorRecommendation: visibleOperatorRecommendation,
  batteryState: defense.batteryState,
  runConfig,
  materializeResult,
  operatorStatus,
  registryError: error,
  weatherError,
  signalPreviewError,
  baselinePreviewError,
  signalPreviewLastLoadedLabel,
  registryLastLoadedAt: lastLoadedAt,
  isMaterializing,
  readModelErrorCount: operatorReadModelErrorCount
})

const primaryBoundaryCopy = computed(() => explanationMode.value === 'mvp'
  ? 'The dashboard reads FastAPI evidence and previews the selected schedule. It does not execute trades or switch a live controller.'
  : 'Next research surfaces stay behind the same read-model boundary: TFT portfolio, market coupling, and DT/LAVA must beat V2+ before claim changes.'
)

const nextStepsItems = computed(() => explanationMode.value === 'mvp'
  ? [
      'Use V2+ as the headline offline schedule/value comparator.',
      'Compare any selected strategy against strict_similar_day and frozen V2+.',
      'Treat the lower schedule dock as a preview recommendation, not market execution.'
    ]
  : [
      'Keep the closed TFT portfolio result visible as negative evidence.',
      'Route future DT/LAVA work through candidate-value or schedule-neighbor supervision.',
      'Promote nothing unless it beats V2+ under strict LP/oracle scoring.'
    ]
)

const schedulePredictionHeadLabel = computed(() => {
  if (visibleOperatorRecommendation.value) {
    const selectedOption = visibleOperatorRecommendation.value.available_strategies.find((strategy) => {
      return strategy.strategy_id === visibleOperatorRecommendation.value?.selected_strategy_id
    })
    return `Schedule source: ${selectedOption?.label || selectedPreviewSourceLabel.value || formatStrategyId(visibleOperatorRecommendation.value.selected_strategy_id)}`
  }
  return explanationMode.value === 'mvp'
    ? 'Schedule source: strict_similar_day fallback'
    : 'Research branch: TFT/DT candidate review'
})

const scheduleMarketBoundaryLabel = computed(() => {
  if (!visibleOperatorRecommendation.value) {
    return 'DAM hourly preview / boundary loading'
  }

  return visibleOperatorRecommendation.value.market_execution_enabled
    ? 'Market execution enabled'
    : 'DAM delivery-day preview / no ProposedBid / no market submission'
})

const formatStrategyId = (strategyId: string): string => strategyId
  .split('_')
  .filter(Boolean)
  .map(part => part.length <= 3 ? part.toUpperCase() : `${part[0]?.toUpperCase() ?? ''}${part.slice(1)}`)
  .join(' ')

const operatorResearchMetrics = computed(() => buildOperatorResearchMetrics({
  modelRows: defense.modelRows.value,
  readinessRows: defense.researchReadinessRows.value,
  offlineStrategyPromotion: defense.offlineStrategyPromotion.value,
  exogenousSignals: defense.exogenousSignals.value,
  batteryState: defense.batteryState.value
}))

const refreshRegistry = async (): Promise<void> => {
  await loadTenants()
}

const handlePrepareRunConfig = async (): Promise<void> => {
  if (!selectedTenantId.value) {
    return
  }

  await prepareRunConfig(selectedTenantId.value)
}

const handleMaterializeWeather = async (): Promise<void> => {
  if (!selectedTenantId.value) {
    return
  }

  await materializeWeatherAssets(selectedTenantId.value, includePriceHistory.value)
}

const dismissSurfaceErrors = (): void => {
  clearError()
  clearWeatherError()
  clearSignalPreviewError()
  clearBaselinePreviewError()
  clearOperatorRecommendationError()
  clearShadowPreviewError()
  clearShadowComparisonError()
}

const setSelectedTenantId = (tenantId: string): void => {
  selectedTenantId.value = tenantId
}

const refreshVisibleRecommendation = async (): Promise<void> => {
  await loadOperatorRecommendation()
  await loadShadowComparisonPreviews()
  if (shouldLoadShadowPreview(selectedPreviewSourceId.value)) {
    await loadShadowRecommendationPreview()
  }
}

watch(selectedTenantId, () => {
  clearWeatherError()
  void syncOperatorStatus(selectedTenantId.value)
})

onMounted(async () => {
  if (tenants.value.length === 0) {
    await loadTenants()
  }

  await loadSignalPreview()
  await loadBaselinePreview()
  await loadOperatorRecommendation()
  await loadShadowComparisonPreviews()
  await loadShadowRecommendationPreview()
  await defense.loadDefenseDashboard()
  await syncOperatorStatus(selectedTenantId.value)
  startAutoRefresh()
})

onBeforeUnmount(() => {
  stopAutoRefresh()
})
</script>

<template>
  <main class="operator-shell">
    <div class="operator-frame">
      <OperatorTopBar
        :clock-label="operatorClockLabel"
        :is-loading="isLoading"
        :active-alert-count="activeAlertCount"
        :timezone-label="selectedTenant?.timezone || 'Timezone pending'"
        @refresh="refreshRegistry"
      />

      <OperatorMetricRibbon :metrics="headlineMetrics" />

      <OperatorAlertBanner
        v-if="error || weatherError || signalPreviewError || baselinePreviewError || operatorRecommendationError || shadowPreviewError || shadowComparisonError"
        :message="error || weatherError || signalPreviewError || baselinePreviewError || operatorRecommendationError || shadowPreviewError || shadowComparisonError"
        @dismiss="dismissSurfaceErrors"
      />

      <div class="operator-body">
        <OperatorSidebar
          :tenants="tenants"
          :selected-tenant-id="selectedTenantId"
          :nav-items="operatorNavItems"
          :active-registry-summary="activeRegistrySummary"
          :battery-asset-label="batteryAssetLabel"
          :signal-preview="signalPreview"
          :baseline-preview="baselinePreview"
          @update:selected-tenant-id="setSelectedTenantId"
        />

        <section class="operator-main-stage">
          <OperatorMarketConsole
            :tenants="tenants"
            :selected-tenant-id="selectedTenantId"
            :registry-envelope="registryEnvelope"
            :explanation-mode="explanationMode"
            :explanation-mode-label="explanationModeLabel"
            :market-regime-chips="marketRegimeChips"
            :signal-preview="signalPreview"
            :operator-recommendation="visibleOperatorRecommendation"
            :is-registry-loading="isLoading"
            :is-signal-preview-loading="isSignalPreviewLoading"
            :signal-preview-last-loaded-label="signalPreviewLastLoadedLabel"
            @update:explanation-mode="value => explanationMode = value"
          />

          <OperatorBaselineConsole
            :baseline-preview="baselinePreview"
            :operator-recommendation="visibleOperatorRecommendation"
            :selected-strategy-id="selectedOperatorStrategyId"
            :is-loading="isBaselinePreviewLoading"
            :last-loaded-label="baselinePreviewLastLoadedLabel"
            :explanation-mode="explanationMode"
          />

          <OperatorDecisionEvidencePanel
            :benchmark="defenseBenchmark"
            :model-rows="defenseModelRows"
            :sensitivity="defenseSensitivity"
            :battery-state="defenseBatteryState"
            :baseline-preview="baselinePreview"
            :operator-recommendation="visibleOperatorRecommendation"
            :exogenous-signals="defenseExogenousSignals"
            :is-loading="defenseIsLoading || isOperatorRecommendationLoading"
            :active-error-count="operatorReadModelErrorCount"
          />

          <OperatorFutureStackPanel
            :future-stack="defenseFutureStack"
            :decision-policy="defenseDecisionPolicyPreview"
            :operator-recommendation="visibleOperatorRecommendation"
            :best-valid-recommendation="operatorRecommendation"
            :shadow-preview="shadowPreview"
            :shadow-comparison-previews="shadowComparisonPreviews"
            :academic-mvp-readiness="defenseAcademicMvpReadiness"
            :selected-strategy-id="selectedOperatorStrategyId"
            :selected-preview-source-id="selectedPreviewSourceId"
            :is-loading="defenseIsLoading || isOperatorRecommendationLoading || isShadowPreviewLoading || isShadowComparisonLoading"
            :shadow-preview-last-loaded-label="shadowPreviewLastLoadedLabel"
            :active-error-count="operatorReadModelErrorCount"
            @update:selected-strategy-id="value => selectedOperatorStrategyId = value"
            @update:selected-preview-source-id="value => selectedPreviewSourceId = value"
            @refresh:shadow-preview="refreshVisibleRecommendation"
          />

          <OperatorResearchPanel
            :metrics="operatorResearchMetrics"
            :sensitivity="defenseSensitivity"
            :is-loading="defenseIsLoading"
            :last-loaded-label="defenseLastLoadedLabel"
            :active-error-count="defenseActiveErrorCount"
          />
        </section>

        <OperatorRightRail
          v-model:include-price-history="includePriceHistory"
          :mood-chips="moodChips"
          :battery-status-label="batteryStatusLabel"
          :battery-soc-percent="batterySocPercent"
          :battery-soc-source-label="batterySocSourceLabel"
          :battery-soc-formula="batterySocFormula"
          :battery-soh-proxy-percent="batterySohProxyPercent"
          :battery-soh-source-label="batterySohSourceLabel"
          :battery-soh-formula="batterySohFormula"
          :battery-telemetry-ingest-label="batteryTelemetryIngestLabel"
          :battery-telemetry-ingest-tooltip="batteryTelemetryIngestTooltip"
          :latest-recommended-power-label="latestRecommendedPowerLabel"
          :gatekeeper-actions="gatekeeperActions"
          :gatekeeper-status="defenseGatekeeperValidationStatus"
          :active-alert-count="activeAlertCount"
          :status-label="statusLabel"
          :is-preparing="isPreparing"
          :is-materializing="isMaterializing"
          :has-selected-tenant="Boolean(selectedTenantId)"
          :last-action-label="lastActionLabel"
          :weather-location-label="weatherLocationLabel"
          :motive-items="motiveItems"
          :primary-boundary-copy="primaryBoundaryCopy"
          :next-steps-items="nextStepsItems"
          :selected-run-config-snippet="selectedRunConfigSnippet"
          @prepare="handlePrepareRunConfig"
          @materialize="handleMaterializeWeather"
        />
      </div>

      <OperatorScheduleDock
        :selected-tenant-name="selectedTenantName"
        :selected-tenant-badge="selectedTenantBadge"
        :timeline-segments="timelineSegments"
        :dispatch-mode-label="dispatchModeLabel"
        :prediction-head-label="schedulePredictionHeadLabel"
        :market-boundary-label="scheduleMarketBoundaryLabel"
        :battery-capacity-context-label="batteryCapacityContextLabel"
        :delivery-window-label="deliveryWindowLabel"
        :selected-preview-source-label="selectedPreviewSourceLabel"
        :is-shadow-preview-mode="selectedPreviewSourceId !== 'best_valid'"
        :hourly-recommendation-rows="hourlyRecommendationRows"
        :hourly-empty-message="hourlyRecommendationEmptyMessage"
        :shadow-preview-last-loaded-label="shadowPreviewLastLoadedLabel"
      />
    </div>
  </main>
</template>
