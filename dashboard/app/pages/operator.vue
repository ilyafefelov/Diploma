<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'

import { useBaselinePreview } from '~/composables/useBaselinePreview'
import { useControlPlaneRegistry } from '~/composables/useControlPlaneRegistry'
import { useDefenseDashboardPanelRefs } from '~/composables/useDefenseDashboardPanelRefs'
import { useOperatorDashboardViewModel } from '~/composables/useOperatorDashboardViewModel'
import { useOperatorPageNarrativeModel } from '~/composables/useOperatorPageNarrativeModel'
import { useOperatorPagePreviewSurface } from '~/composables/useOperatorPagePreviewSurface'
import { useOperatorRecommendationPreviewModel } from '~/composables/useOperatorRecommendationPreviewModel'
import { useOperatorRootScrollRecovery } from '~/composables/useOperatorRootScrollRecovery'
import { useSignalPreview } from '~/composables/useSignalPreview'
import { useWeatherControls } from '~/composables/useWeatherControls'
import type { OperatorChartHorizon, OperatorMarketVenue } from '~/types/operator-dashboard'
import { operatorTargetDateShortcutsForPreview } from '~/utils/operatorPreviewControls'

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

const selectedMarketVenue = ref<OperatorMarketVenue>('DAM')
const selectedTargetDeliveryDate = ref<string | null>(null)
const selectedChartHorizon = ref<OperatorChartHorizon>('24h')
const shouldAutoLoadBaselinePreview = ref(true)

const {
  baselinePreview,
  isLoading: isBaselinePreviewLoading,
  error: baselinePreviewError,
  clearError: clearBaselinePreviewError,
  isEnsuringPreview: isBaselinePreviewEnsuring,
  lastLoadedLabel: baselinePreviewLastLoadedLabel,
  loadBaselinePreview,
  operatorPreviewEnsureMessage: baselinePreviewEnsureMessage
} = useBaselinePreview(
  selectedTenantId,
  selectedMarketVenue,
  selectedTargetDeliveryDate,
  shouldAutoLoadBaselinePreview
)

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
const {
  clearOperatorRecommendationError,
  clearShadowComparisonError,
  clearShadowPreviewError,
  hourlyRecommendationEmptyMessage,
  hourlyRecommendationRows,
  isOperatorRecommendationLoading,
  isOperatorPreviewEnsuring,
  isShadowComparisonLoading,
  isShadowPreviewLoading,
  operatorRecommendationLastLoadedLabel,
  loadRecommendationSurfaces,
  operatorRecommendationError,
  operatorPreviewEnsureMessage,
  refreshVisibleRecommendation,
  selectPreviewSource,
  selectValueAlignedHfShadowDemoScenario,
  selectedOperatorStrategyId,
  selectedPreviewSourceId,
  selectedPreviewSourceLabel,
  shadowComparisonError,
  shadowComparisonPreviews,
  shadowPreview,
  shadowPreviewError,
  shadowPreviewLastLoadedLabel,
  visibleOperatorRecommendation
} = useOperatorRecommendationPreviewModel({
  selectedTenantId,
  selectedMarketVenue,
  selectedTargetDeliveryDate,
  baselinePreview
})

const {
  defenseAcademicMvpReadiness,
  defenseActiveErrorCount,
  defenseBatteryState,
  defenseBenchmark,
  defenseDecisionPolicyPreview,
  defenseExogenousSignals,
  defenseFutureStack,
  defenseGatekeeperValidationStatus,
  defenseIsLoading,
  defenseLastLoadedLabel,
  defenseModelRows,
  defenseSensitivity
} = useDefenseDashboardPanelRefs(defense)

const {
  activePreviewEnsureMessage,
  activeMarketPreviewError: pError,
  activeMarketPreviewLastLoadedLabel: pLoaded,
  activeMarketPreviewLoading: pBusy,
  activeSurfaceErrorMessage,
  bestValidStrategyComparisonRecommendation,
  isLiveHfShadowPreviewSelected,
  isPreviewEnsuring,
  operatorReadModelErrorCount,
  selectedHourlyRecommendationEmptyMessage: hMsg,
  selectedHourlyRecommendationRows: hRows,
  selectedVisibleOperatorRecommendation
} = useOperatorPagePreviewSurface({
  selectedPreviewSourceId,
  operatorPreviewEnsureMessage,
  baselinePreviewEnsureMessage,
  isOperatorPreviewEnsuring,
  isBaselinePreviewEnsuring,
  defenseActiveErrorCount,
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
})
const targetDateShortcuts = computed(() => (
  operatorTargetDateShortcutsForPreview(selectedPreviewSourceId.value)
))

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
  selectedMarketVenue,
  selectedTargetDeliveryDate,
  signalPreview,
  baselinePreview,
  operatorRecommendation: selectedVisibleOperatorRecommendation,
  suppressBaselineFallback: isLiveHfShadowPreviewSelected,
  isSelectedRecommendationLoading: computed(() => (
    isLiveHfShadowPreviewSelected.value ? isShadowPreviewLoading.value : isOperatorRecommendationLoading.value
  )),
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

const {
  explanationModeLabel,
  nextStepsItems,
  operatorResearchMetrics,
  primaryBoundaryCopy,
  scheduleMarketBoundaryLabel,
  schedulePredictionHeadLabel
} = useOperatorPageNarrativeModel({
  explanationMode,
  visibleOperatorRecommendation: selectedVisibleOperatorRecommendation,
  selectedPreviewSourceLabel,
  isShadowPreviewMode: computed(() => selectedPreviewSourceId.value !== 'best_valid'),
  modelRows: defense.modelRows,
  readinessRows: defense.researchReadinessRows,
  offlineStrategyPromotion: defense.offlineStrategyPromotion,
  exogenousSignals: defense.exogenousSignals,
  batteryState: defense.batteryState
})

const handlePrepareRunConfig = async () => {
  if (!selectedTenantId.value) {
    return
  }

  await prepareRunConfig(selectedTenantId.value)
}

const handleMaterializeWeather = async () => {
  if (!selectedTenantId.value) {
    return
  }

  await materializeWeatherAssets(selectedTenantId.value, includePriceHistory.value)
}

const dismissSurfaceErrors = () => {
  clearError()
  clearWeatherError()
  clearSignalPreviewError()
  clearBaselinePreviewError()
  clearOperatorRecommendationError()
  clearShadowPreviewError()
  clearShadowComparisonError()
}

useOperatorRootScrollRecovery()

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
  await loadRecommendationSurfaces()
  await defense.loadDefenseDashboard()
  await syncOperatorStatus(selectedTenantId.value)
  startAutoRefresh()
})

onBeforeUnmount(() => {
  stopAutoRefresh()
})
</script>

<template>
  <main
    id="operator-content"
    class="operator-shell"
    tabindex="-1"
  >
    <a
      class="operator-skip-link"
      href="#operator-content"
    >
      Skip to operator dashboard
    </a>
    <div class="operator-frame">
      <OperatorTopBar
        :clock-label="operatorClockLabel"
        :is-loading="isLoading"
        :active-alert-count="activeAlertCount"
        :timezone-label="selectedTenant?.timezone || 'Timezone pending'"
        @refresh="loadTenants"
      />

      <OperatorMetricRibbon :metrics="headlineMetrics" />

      <OperatorBoundaryStrip />

      <OperatorAlertBanner
        v-if="activePreviewEnsureMessage || activeSurfaceErrorMessage"
        :title="activePreviewEnsureMessage ? 'Preparing source-backed preview' : 'Control surface issue'"
        :message="activePreviewEnsureMessage || activeSurfaceErrorMessage"
        :is-loading="isPreviewEnsuring"
        :dismissible="!activePreviewEnsureMessage"
        @dismiss="dismissSurfaceErrors"
      />

      <div class="operator-body">
        <OperatorSidebar
          v-model:selected-tenant-id="selectedTenantId"
          :tenants="tenants"
          :nav-items="operatorNavItems"
          :active-registry-summary="activeRegistrySummary"
          :battery-asset-label="batteryAssetLabel"
          :signal-preview="signalPreview"
          :baseline-preview="baselinePreview"
        />

        <section class="operator-main-stage">
          <OperatorMarketConsole
            v-model:selected-market-venue="selectedMarketVenue"
            v-model:selected-target-delivery-date="selectedTargetDeliveryDate"
            v-model:selected-chart-horizon="selectedChartHorizon"
            v-model:explanation-mode="explanationMode"
            :tenants="tenants"
            :selected-tenant-id="selectedTenantId"
            :registry-envelope="registryEnvelope"
            :explanation-mode-label="explanationModeLabel"
            :market-regime-chips="marketRegimeChips"
            :signal-preview="signalPreview"
            :operator-recommendation="selectedVisibleOperatorRecommendation"
            :market-preview-error="pError"
            :target-date-shortcuts="targetDateShortcuts"
            :is-registry-loading="isLoading"
            :is-signal-preview-loading="pBusy || isPreviewEnsuring"
            :signal-preview-last-loaded-label="pLoaded"
          />

          <OperatorBaselineConsole
            :baseline-preview="baselinePreview"
            :operator-recommendation="selectedVisibleOperatorRecommendation"
            :selected-strategy-id="selectedOperatorStrategyId"
            :selected-chart-horizon="selectedChartHorizon"
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
            :operator-recommendation="selectedVisibleOperatorRecommendation"
            :exogenous-signals="defenseExogenousSignals"
            :is-loading="defenseIsLoading || isOperatorRecommendationLoading"
            :active-error-count="operatorReadModelErrorCount"
          />

          <OperatorFutureStackPanel
            :future-stack="defenseFutureStack"
            :decision-policy="defenseDecisionPolicyPreview"
            :operator-recommendation="selectedVisibleOperatorRecommendation"
            :best-valid-recommendation="bestValidStrategyComparisonRecommendation"
            :shadow-preview="shadowPreview"
            :shadow-comparison-previews="shadowComparisonPreviews"
            :academic-mvp-readiness="defenseAcademicMvpReadiness"
            :selected-strategy-id="selectedOperatorStrategyId"
            :selected-preview-source-id="selectedPreviewSourceId"
            :selected-chart-horizon="selectedChartHorizon"
            :is-loading="defenseIsLoading || isOperatorRecommendationLoading || isShadowPreviewLoading || isShadowComparisonLoading"
            :shadow-preview-last-loaded-label="shadowPreviewLastLoadedLabel"
            :active-error-count="operatorReadModelErrorCount"
            @update:selected-strategy-id="(value: string) => selectedOperatorStrategyId = value"
            @update:selected-preview-source-id="selectPreviewSource"
            @refresh:shadow-preview="refreshVisibleRecommendation"
            @select:hf-demo-scenario="selectValueAlignedHfShadowDemoScenario"
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
        :selected-market-venue="selectedMarketVenue"
        :battery-capacity-context-label="batteryCapacityContextLabel"
        :delivery-window-label="deliveryWindowLabel"
        :selected-preview-source-label="selectedPreviewSourceLabel"
        :is-shadow-preview-mode="selectedPreviewSourceId !== 'best_valid'"
        :hourly-recommendation-rows="hRows"
        :hourly-empty-message="hMsg"
        :shadow-preview-last-loaded-label="shadowPreviewLastLoadedLabel"
      />
    </div>
  </main>
</template>
