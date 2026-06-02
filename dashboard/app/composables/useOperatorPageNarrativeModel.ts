import { computed, type Ref } from 'vue'

import type { OperatorRecommendationResponse } from '~/types/control-plane'
import { buildOperatorResearchMetrics } from '~/utils/operatorResearchMetrics'

export type OperatorExplanationMode = 'mvp' | 'future'

type ReadableRef<T> = Readonly<Ref<T>>
type OperatorResearchMetricsInput = Parameters<typeof buildOperatorResearchMetrics>[0]

interface OperatorPageNarrativeModelInput {
  explanationMode: ReadableRef<OperatorExplanationMode>
  visibleOperatorRecommendation: ReadableRef<OperatorRecommendationResponse | null>
  selectedPreviewSourceLabel: ReadableRef<string>
  isShadowPreviewMode?: ReadableRef<boolean>
  modelRows: ReadableRef<OperatorResearchMetricsInput['modelRows']>
  readinessRows: ReadableRef<OperatorResearchMetricsInput['readinessRows']>
  offlineStrategyPromotion: ReadableRef<OperatorResearchMetricsInput['offlineStrategyPromotion']>
  exogenousSignals: ReadableRef<OperatorResearchMetricsInput['exogenousSignals']>
  batteryState: ReadableRef<OperatorResearchMetricsInput['batteryState']>
}

export const useOperatorPageNarrativeModel = (input: OperatorPageNarrativeModelInput) => {
  const explanationModeLabel = computed(() => {
    if (input.isShadowPreviewMode?.value) {
      return input.selectedPreviewSourceLabel.value
    }

    return input.explanationMode.value === 'mvp'
      ? 'Selected V2+ evidence'
      : 'Research roadmap'
  })

  const primaryBoundaryCopy = computed(() => input.explanationMode.value === 'mvp'
    ? 'The dashboard reads FastAPI evidence and previews the selected schedule. It does not execute trades or switch a live controller.'
    : 'Next research surfaces stay behind the same read-model boundary: TFT portfolio, market coupling, and DT/LAVA must beat V2+ before claim changes.'
  )

  const nextStepsItems = computed(() => input.explanationMode.value === 'mvp'
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
    if (input.visibleOperatorRecommendation.value) {
      const selectedOption = input.visibleOperatorRecommendation.value.available_strategies.find((strategy) => {
        return strategy.strategy_id === input.visibleOperatorRecommendation.value?.selected_strategy_id
      })
      return `Schedule source: ${
        selectedOption?.label
        || input.selectedPreviewSourceLabel.value
        || formatStrategyId(input.visibleOperatorRecommendation.value.selected_strategy_id)
      }`
    }

    if (input.isShadowPreviewMode?.value) {
      return `Schedule source: ${input.selectedPreviewSourceLabel.value}`
    }

    return input.explanationMode.value === 'mvp'
      ? 'Schedule source: strict_similar_day fallback'
      : 'Research branch: TFT/DT candidate review'
  })

  const scheduleMarketBoundaryLabel = computed(() => {
    if (!input.visibleOperatorRecommendation.value) {
      if (input.isShadowPreviewMode?.value) {
        return 'DAM/IDM hourly preview / no ProposedBid / no market submission'
      }
      return 'DAM/IDM hourly preview / boundary loading'
    }

    return input.visibleOperatorRecommendation.value.market_execution_enabled
      ? 'Execution flag reported true / blocked by operator preview boundary'
      : 'DAM/IDM hourly preview / no ProposedBid / no market submission'
  })

  const operatorResearchMetrics = computed(() => buildOperatorResearchMetrics({
    modelRows: input.modelRows.value,
    readinessRows: input.readinessRows.value,
    offlineStrategyPromotion: input.offlineStrategyPromotion.value,
    exogenousSignals: input.exogenousSignals.value,
    batteryState: input.batteryState.value
  }))

  return {
    explanationModeLabel,
    nextStepsItems,
    operatorResearchMetrics,
    primaryBoundaryCopy,
    scheduleMarketBoundaryLabel,
    schedulePredictionHeadLabel
  }
}

const formatStrategyId = (strategyId: string): string => strategyId
  .split('_')
  .filter(Boolean)
  .map(part => part.length <= 3 ? part.toUpperCase() : `${part[0]?.toUpperCase() ?? ''}${part.slice(1)}`)
  .join(' ')
