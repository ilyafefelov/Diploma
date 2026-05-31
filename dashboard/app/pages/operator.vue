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
import { useOperatorPageNarrativeModel } from '~/composables/useOperatorPageNarrativeModel'
import { useOperatorRecommendationPreviewModel } from '~/composables/useOperatorRecommendationPreviewModel'
import { useOperatorRootScrollRecovery } from '~/composables/useOperatorRootScrollRecovery'
import { useSignalPreview } from '~/composables/useSignalPreview'
import { useWeatherControls } from '~/composables/useWeatherControls'
import type { OperatorChartHorizon, OperatorMarketVenue } from '~/types/operator-dashboard'

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

const {
  baselinePreview,
  isLoading: isBaselinePreviewLoading,
  error: baselinePreviewError,
  clearError: clearBaselinePreviewError,
  lastLoadedLabel: baselinePreviewLastLoadedLabel,
  loadBaselinePreview
} = useBaselinePreview(selectedTenantId, selectedMarketVenue, selectedTargetDeliveryDate)

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
  isShadowComparisonLoading,
  isShadowPreviewLoading,
  operatorRecommendation,
  loadRecommendationSurfaces,
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
} = useOperatorRecommendationPreviewModel({
  selectedTenantId,
  selectedMarketVenue,
  selectedTargetDeliveryDate,
  baselinePreview
})

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
  selectedMarketVenue,
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

const {
  explanationModeLabel,
  nextStepsItems,
  operatorResearchMetrics,
  primaryBoundaryCopy,
  scheduleMarketBoundaryLabel,
  schedulePredictionHeadLabel
} = useOperatorPageNarrativeModel({
  explanationMode,
  visibleOperatorRecommendation,
  selectedPreviewSourceLabel,
  modelRows: defense.modelRows,
  readinessRows: defense.researchReadinessRows,
  offlineStrategyPromotion: defense.offlineStrategyPromotion,
  exogenousSignals: defense.exogenousSignals,
  batteryState: defense.batteryState
})

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
            :operator-recommendation="operatorRecommendation"
            :market-preview-error="operatorRecommendationError || baselinePreviewError"
            :selected-market-venue="selectedMarketVenue"
            :selected-target-delivery-date="selectedTargetDeliveryDate"
            :selected-chart-horizon="selectedChartHorizon"
            :is-registry-loading="isLoading"
            :is-signal-preview-loading="isSignalPreviewLoading"
            :signal-preview-last-loaded-label="signalPreviewLastLoadedLabel"
            @update:selected-market-venue="value => selectedMarketVenue = value"
            @update:selected-target-delivery-date="value => selectedTargetDeliveryDate = value"
            @update:selected-chart-horizon="value => selectedChartHorizon = value"
            @update:explanation-mode="value => explanationMode = value"
          />

          <OperatorBaselineConsole
            :baseline-preview="baselinePreview"
            :operator-recommendation="visibleOperatorRecommendation"
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
            :selected-chart-horizon="selectedChartHorizon"
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
        :selected-market-venue="selectedMarketVenue"
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
