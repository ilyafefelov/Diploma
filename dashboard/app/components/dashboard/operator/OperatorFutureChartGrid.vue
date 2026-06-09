<script setup lang="ts">
import type { PolicyValueMode } from '~/utils/operatorFutureStackPresentation'
import OperatorForecastStackChartCard from './future-charts/OperatorForecastStackChartCard.vue'
import OperatorPolicyValueChartCard from './future-charts/OperatorPolicyValueChartCard.vue'
import OperatorStrategyComparisonChartCard from './future-charts/OperatorStrategyComparisonChartCard.vue'
import type {
  FutureChartGuideItem,
  FutureChartHiddenForecastItem,
  FutureChartQualityItem,
  FutureChartShadowStoryItem,
  FutureChartSummaryItem
} from './future-charts/operatorFutureChartCardTypes'

defineProps<{
  forecastChartTitle: string
  forecastStackDescription: string
  recommendationInputSummaryItems: FutureChartQualityItem[]
  hiddenUnsafeForecastItems: FutureChartHiddenForecastItem[]
  forecastOption: Record<string, unknown>
  policyChartTitle: string
  policyChartDescription: string
  policyChartSummary: FutureChartSummaryItem[]
  selectedRecommendationGuideItems: FutureChartGuideItem[]
  policyOption: Record<string, unknown>
  isOfficialPolicyMode: boolean
  hasOfficialPolicyRows: boolean
  isShadowRecommendationMode: boolean
  selectedScheduleWindowLabel: string
  shadowModelStoryItems: FutureChartShadowStoryItem[]
  strategyComparisonSummary: FutureChartSummaryItem[]
  strategyComparisonOption: Record<string, unknown>
}>()

const emit = defineEmits<{
  'update:policyValueMode': [mode: PolicyValueMode]
}>()
</script>

<template>
  <div class="future-chart-grid">
    <OperatorForecastStackChartCard
      :forecast-chart-title="forecastChartTitle"
      :forecast-stack-description="forecastStackDescription"
      :recommendation-input-summary-items="recommendationInputSummaryItems"
      :hidden-unsafe-forecast-items="hiddenUnsafeForecastItems"
      :forecast-option="forecastOption"
    />

    <OperatorPolicyValueChartCard
      :policy-chart-title="policyChartTitle"
      :policy-chart-description="policyChartDescription"
      :policy-chart-summary="policyChartSummary"
      :selected-recommendation-guide-items="selectedRecommendationGuideItems"
      :policy-option="policyOption"
      :is-official-policy-mode="isOfficialPolicyMode"
      :has-official-policy-rows="hasOfficialPolicyRows"
      :is-shadow-recommendation-mode="isShadowRecommendationMode"
      @update:policy-value-mode="emit('update:policyValueMode', $event)"
    />

    <OperatorStrategyComparisonChartCard
      :selected-schedule-window-label="selectedScheduleWindowLabel"
      :shadow-model-story-items="shadowModelStoryItems"
      :strategy-comparison-summary="strategyComparisonSummary"
      :strategy-comparison-option="strategyComparisonOption"
    />
  </div>
</template>

<style src="../../../assets/css/operator-future-chart-grid.css"></style>
