<script setup lang="ts">
import { useOperatorFutureStackPanelModel } from '~/composables/useOperatorFutureStackPanelModel'
import type {
  AcademicMvpReadinessResponse,
  DecisionPolicyPreviewResponse,
  FutureStackPreviewResponse,
  OperatorRecommendationResponse,
  ShadowRecommendationPreviewResponse
} from '~/types/control-plane'
import type { OperatorChartHorizon } from '~/types/operator-dashboard'
import type { OperatorPreviewSourceId } from '~/utils/operatorShadowPreview'
import type { ValueAlignedHfShadowDemoScenarioId } from '~/utils/operatorFutureStackPresentation'
import OperatorFutureChartGrid from './OperatorFutureChartGrid.vue'
import OperatorFutureExplainerGrid from './OperatorFutureExplainerGrid.vue'
import OperatorFutureHeaderControls from './OperatorFutureHeaderControls.vue'
import OperatorFutureReadinessStrips from './OperatorFutureReadinessStrips.vue'

const props = defineProps<{
  futureStack: FutureStackPreviewResponse | null
  decisionPolicy: DecisionPolicyPreviewResponse | null
  operatorRecommendation: OperatorRecommendationResponse | null
  bestValidRecommendation: OperatorRecommendationResponse | null
  shadowPreview: ShadowRecommendationPreviewResponse | null
  shadowComparisonPreviews: ShadowRecommendationPreviewResponse[]
  academicMvpReadiness: AcademicMvpReadinessResponse | null
  selectedStrategyId: string
  selectedPreviewSourceId: OperatorPreviewSourceId
  selectedChartHorizon: OperatorChartHorizon
  isLoading: boolean
  shadowPreviewLastLoadedLabel: string
  activeErrorCount: number
}>()

const emit = defineEmits<{
  'update:selectedStrategyId': [value: string]
  'update:selectedPreviewSourceId': [value: OperatorPreviewSourceId]
  'refresh:shadowPreview': []
  'select:hf-demo-scenario': [value: ValueAlignedHfShadowDemoScenarioId]
}>()

const {
  academicMvpGatePassportItems,
  backendStatusFacts,
  decisionSourceFacts,
  forecastChartTitle,
  forecastOption,
  forecastStackDescription,
  hasOfficialPolicyRows,
  hiddenUnsafeForecastItems,
  isOfficialPolicyMode,
  isShadowRecommendationMode,
  policyChartDescription,
  policyChartSummary,
  policyChartTitle,
  policyOption,
  recommendationInputSummaryItems,
  selectedRecommendationGuideItems,
  selectedScheduleWindowLabel,
  setPolicyValueMode,
  shadowModelStoryItems,
  shadowPreviewLabel,
  statusCards,
  strategyComparisonOption,
  strategyComparisonSummary,
  strategyReadinessItems,
  v13ReadinessItems
} = useOperatorFutureStackPanelModel(props)
</script>

<template>
  <section class="surface-panel operator-future-panel">
    <OperatorFutureHeaderControls
      :selected-preview-source-id="selectedPreviewSourceId"
      :shadow-preview="shadowPreview"
      :shadow-preview-last-loaded-label="shadowPreviewLastLoadedLabel"
      :active-error-count="activeErrorCount"
      :is-loading="isLoading"
      @update:selected-preview-source-id="emit('update:selectedPreviewSourceId', $event)"
      @refresh:shadow-preview="emit('refresh:shadowPreview')"
      @select:hf-demo-scenario="emit('select:hf-demo-scenario', $event)"
    />

    <OperatorFutureReadinessStrips
      :selected-preview-source-id="selectedPreviewSourceId"
      :shadow-preview-label="shadowPreviewLabel"
      :shadow-preview-status="shadowPreview?.preview_status"
      :status-cards="statusCards"
      :strategy-readiness-items="strategyReadinessItems"
      :v13-readiness-items="v13ReadinessItems"
      :academic-mvp-gate-passport-items="academicMvpGatePassportItems"
    />

    <OperatorFutureChartGrid
      :forecast-chart-title="forecastChartTitle"
      :forecast-stack-description="forecastStackDescription"
      :recommendation-input-summary-items="recommendationInputSummaryItems"
      :hidden-unsafe-forecast-items="hiddenUnsafeForecastItems"
      :forecast-option="forecastOption"
      :policy-chart-title="policyChartTitle"
      :policy-chart-description="policyChartDescription"
      :policy-chart-summary="policyChartSummary"
      :selected-recommendation-guide-items="selectedRecommendationGuideItems"
      :policy-option="policyOption"
      :is-official-policy-mode="isOfficialPolicyMode"
      :has-official-policy-rows="hasOfficialPolicyRows"
      :is-shadow-recommendation-mode="isShadowRecommendationMode"
      :selected-schedule-window-label="selectedScheduleWindowLabel"
      :shadow-model-story-items="shadowModelStoryItems"
      :strategy-comparison-summary="strategyComparisonSummary"
      :strategy-comparison-option="strategyComparisonOption"
      @update:policy-value-mode="setPolicyValueMode"
    />

    <OperatorFutureExplainerGrid
      :decision-source-facts="decisionSourceFacts"
      :backend-status-facts="backendStatusFacts"
    />
  </section>
</template>

<style scoped>
.operator-future-panel{display:grid;gap:.85rem;padding:.8rem;min-width:0}
</style>
