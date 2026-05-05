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
import { useSignalPreview } from '~/composables/useSignalPreview'
import { useWeatherControls } from '~/composables/useWeatherControls'
import { buildOperatorResearchMetrics } from '~/utils/operatorResearchMetrics'

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
const selectedOperatorStrategyId = ref('strict_similar_day')

const {
  operatorRecommendation,
  isLoading: isOperatorRecommendationLoading,
  error: operatorRecommendationError,
  clearError: clearOperatorRecommendationError,
  loadOperatorRecommendation
} = useOperatorRecommendation(selectedTenantId, selectedOperatorStrategyId)

const explanationModeLabel = computed(() => explanationMode.value === 'mvp' ? 'Current MVP logic' : 'Future production logic')

const {
  activeAlertCount,
  activeRegistrySummary,
  batterySocFormula,
  batterySocPercent,
  batterySocSourceLabel,
  batterySohFormula,
  batterySohProxyPercent,
  batterySohSourceLabel,
  batteryStatusLabel,
  dispatchModeLabel,
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
  isMaterializing
})

const primaryBoundaryCopy = computed(() => explanationMode.value === 'mvp'
  ? 'Browser requests stay same-origin and are forwarded by Nuxt to the control-plane API. That keeps the dashboard bright and simple while the data plumbing stays behind the glass.'
  : 'The future surface should still keep the browser same-origin, but the meaning of the cards changes: forecast outputs come from dedicated models and dispatch intent comes from policy logic plus deterministic validation.'
)

const nextStepsItems = computed(() => explanationMode.value === 'mvp'
  ? [
      'Materialization controls for weather and bronze assets.',
      'Feasible plan review with units, SOC guardrails, and operator-ready explanations.',
      'Dispatch and regret signatures with bright Sims-style motion cues.'
    ]
  : [
      'Forecast cards fed by NBEATSx and TFT with uncertainty bands and feature evidence.',
      'Dispatch cards fed by DT or M3DT with policy intent, counterfactual value, and safety outcomes.',
      'Benchmark views that keep the LP baseline visible as a comparison surface rather than the final decision layer.'
    ]
)

const schedulePredictionHeadLabel = computed(() => explanationMode.value === 'mvp'
  ? 'Prediction head: HourlyDamBaselineSolver -> baseline LP'
  : 'Target head: NBEATSx/TFT -> policy review'
)

const operatorResearchMetrics = computed(() => buildOperatorResearchMetrics({
  modelRows: defense.modelRows.value,
  readinessRows: defense.researchReadinessRows.value,
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
}

const setSelectedTenantId = (tenantId: string): void => {
  selectedTenantId.value = tenantId
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
        :timezone-label="selectedTenant?.timezone || 'Timezone pending'"
        @refresh="refreshRegistry"
      />

      <OperatorMetricRibbon :metrics="headlineMetrics" />

      <OperatorAlertBanner
        v-if="error || weatherError || signalPreviewError || baselinePreviewError || operatorRecommendationError"
        :message="error || weatherError || signalPreviewError || baselinePreviewError || operatorRecommendationError"
        @dismiss="dismissSurfaceErrors"
      />

      <div class="operator-body">
        <OperatorSidebar
          :tenants="tenants"
          :selected-tenant-id="selectedTenantId"
          :nav-items="operatorNavItems"
          :active-registry-summary="activeRegistrySummary"
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
            :is-registry-loading="isLoading"
            :is-signal-preview-loading="isSignalPreviewLoading"
            :signal-preview-last-loaded-label="signalPreviewLastLoadedLabel"
            @update:explanation-mode="value => explanationMode = value"
          />

          <OperatorBaselineConsole
            :baseline-preview="baselinePreview"
            :is-loading="isBaselinePreviewLoading"
            :last-loaded-label="baselinePreviewLastLoadedLabel"
            :explanation-mode="explanationMode"
          />

          <OperatorDecisionEvidencePanel
            :benchmark="defense.benchmark.value"
            :model-rows="defense.modelRows.value"
            :sensitivity="defense.sensitivity.value"
            :battery-state="defense.batteryState.value"
            :baseline-preview="baselinePreview"
            :operator-recommendation="operatorRecommendation"
            :exogenous-signals="defense.exogenousSignals.value"
            :is-loading="defense.isLoading.value || isOperatorRecommendationLoading"
          />

          <OperatorFutureStackPanel
            :future-stack="defense.futureStack.value"
            :decision-policy="defense.dtPolicyPreview.value"
            :operator-recommendation="operatorRecommendation"
            :is-loading="defense.isLoading.value || isOperatorRecommendationLoading"
          />

          <OperatorResearchPanel
            :metrics="operatorResearchMetrics"
            :sensitivity="defense.sensitivity.value"
            :is-loading="defense.isLoading.value"
            :last-loaded-label="defense.lastLoadedLabel.value"
            :active-error-count="defense.activeErrorCount.value"
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
          :latest-recommended-power-label="latestRecommendedPowerLabel"
          :gatekeeper-actions="gatekeeperActions"
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
      />
    </div>
  </main>
</template>
