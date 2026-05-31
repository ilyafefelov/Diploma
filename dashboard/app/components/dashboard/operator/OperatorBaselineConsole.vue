<script setup lang="ts">
import HudBaselinePreview from '~/components/dashboard/HudBaselinePreview.vue'
import type { BaselineLpPreview, OperatorRecommendationResponse } from '~/types/control-plane'
import type { OperatorChartHorizon, OperatorExplanationMode } from '~/types/operator-dashboard'

defineProps<{
  baselinePreview: BaselineLpPreview | null
  operatorRecommendation: OperatorRecommendationResponse | null
  selectedStrategyId: string
  selectedChartHorizon: OperatorChartHorizon
  isLoading: boolean
  lastLoadedLabel: string
  explanationMode: OperatorExplanationMode
}>()
</script>

<template>
  <ClientOnly>
    <HudBaselinePreview
      :baseline-preview="baselinePreview"
      :operator-recommendation="operatorRecommendation"
      :selected-strategy-id="selectedStrategyId"
      :selected-chart-horizon="selectedChartHorizon"
      :is-loading="isLoading"
      :last-loaded-label="lastLoadedLabel"
      :explanation-mode="explanationMode"
    />

    <template #fallback>
      <div class="chart-fallback chart-fallback-compact">
        Preparing baseline LP surface...
      </div>
    </template>
  </ClientOnly>
</template>
