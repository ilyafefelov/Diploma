import { computed } from 'vue'

import {
  buildDecisionControlRegretTimelineOption,
  buildDecisionSensitivityOption,
  buildDecisionStrategyEvidenceOption
} from '~/lib/charts/operatorDecisionEvidenceChartOptions'
import type {
  BaselineLpPreview,
  DashboardBatteryStateResponse,
  DashboardExogenousSignalsResponse,
  ForecastDispatchSensitivityResponse,
  OperatorRecommendationResponse,
  RealDataBenchmarkResponse
} from '~/types/control-plane'
import type { DefenseModelRow } from '~/utils/defenseDataset'
import {
  buildControlRegretTimeline,
  buildOperatorDecisionReadinessItems,
  buildOperatorDecisionStateCards,
  buildOperatorForecastScenarioCandidateRows,
  buildOperatorStrategyEvidenceRows,
  buildSensitivityEvidenceRows,
  type ControlRegretTimelinePoint
} from '~/utils/operatorDecisionEvidence'

export interface OperatorDecisionEvidencePanelModelInput {
  benchmark?: RealDataBenchmarkResponse | null
  modelRows: DefenseModelRow[]
  sensitivity?: ForecastDispatchSensitivityResponse | null
  batteryState?: DashboardBatteryStateResponse | null
  baselinePreview: BaselineLpPreview | null
  operatorRecommendation: OperatorRecommendationResponse | null
  exogenousSignals?: DashboardExogenousSignalsResponse | null
  isLoading: boolean
  activeErrorCount: number
}

interface DecisionChartSummaryItem {
  label: string
  value: string
  detail: string
}

export const useOperatorDecisionEvidencePanelModel = (
  input: OperatorDecisionEvidencePanelModelInput
) => {
  const strategyRows = computed(() => buildOperatorStrategyEvidenceRows(input.modelRows, input.operatorRecommendation))
  const controlTimeline = computed(() => buildControlRegretTimeline(input.benchmark ?? null, 24, input.operatorRecommendation))
  const sensitivityRows = computed(() => buildSensitivityEvidenceRows(input.sensitivity ?? null, input.operatorRecommendation))
  const forecastScenarioRows = computed(() => buildOperatorForecastScenarioCandidateRows(input.operatorRecommendation))
  const stateCards = computed(() => buildOperatorDecisionStateCards({
    operatorRecommendation: input.operatorRecommendation,
    batteryState: input.batteryState ?? null,
    baselinePreview: input.baselinePreview,
    exogenousSignals: input.exogenousSignals ?? null,
    modelRows: input.modelRows
  }))
  const readinessItems = computed(() => buildOperatorDecisionReadinessItems({
    operatorRecommendation: input.operatorRecommendation,
    batteryState: input.batteryState ?? null,
    baselinePreview: input.baselinePreview,
    exogenousSignals: input.exogenousSignals ?? null
  }))

  const readModelBadgeLabel = computed(() => {
    if (input.isLoading) {
      return 'Refreshing'
    }

    return input.activeErrorCount > 0 ? `${input.activeErrorCount} read-model gap(s)` : 'FastAPI read model'
  })

  const comparatorGuideItems = computed(() => [
    {
      label: 'X-axis',
      detail: 'each model candidate'
    },
    {
      label: 'Blue bars',
      detail: 'average lost value in UAH (lower is better)'
    },
    {
      label: 'Green line',
      detail: 'win share across anchors in % (higher is better)'
    },
    {
      label: 'Read together',
      detail: 'best model usually has low bars and high line'
    }
  ])
  const sensitivityGuideItems = computed(() => [
    {
      label: 'Strict control',
      detail: 'manual baseline regret against oracle'
    },
    {
      label: 'Selected V2+',
      detail: 'current schedule/value learner evidence'
    },
    {
      label: 'Forecast context',
      detail: 'price-model contribution to regret'
    }
  ])

  const comparatorWinNarrative = computed(() => {
    const rows = strategyRows.value
    if (rows.length === 0) {
      return null
    }

    const winner = rows[0]
    if (!winner) {
      return null
    }

    const runnerUp = rows[1]
    const control = rows.find(row => row.modelName === 'strict_similar_day')
    const winnerLabel = formatModelLabel(winner.modelName)
    const winnerWinRate = Math.round(winner.winRate * 100)
    const winnerRegret = Math.round(winner.meanRegretUah).toLocaleString('en-GB')
    const detailParts: string[] = [`Mean regret ${winnerRegret} UAH`]

    if (runnerUp) {
      const runnerUpDelta = Math.round(runnerUp.meanRegretUah - winner.meanRegretUah)
      const runnerUpLabel = formatModelLabel(runnerUp.modelName)
      if (runnerUpDelta > 0) {
        detailParts.push(`${runnerUpDelta.toLocaleString('en-GB')} UAH better than ${runnerUpLabel}`)
      } else if (runnerUpDelta < 0) {
        detailParts.push(`${Math.abs(runnerUpDelta).toLocaleString('en-GB')} UAH worse than ${runnerUpLabel}`)
      } else {
        detailParts.push(`same regret as ${runnerUpLabel}`)
      }
    }

    if (control && control.modelName !== winner.modelName) {
      const controlDelta = Math.round(control.meanRegretUah - winner.meanRegretUah)
      if (controlDelta > 0) {
        detailParts.push(`${controlDelta.toLocaleString('en-GB')} UAH better than strict control`)
      } else if (controlDelta < 0) {
        detailParts.push(`${Math.abs(controlDelta).toLocaleString('en-GB')} UAH worse than strict control`)
      } else {
        detailParts.push('same regret as strict control')
      }
    }

    return {
      headline: `${winnerLabel} wins ${winnerWinRate}% of anchors`,
      detail: `${detailParts.join('; ')}. Win rate counts how often a model is rank-1 on each anchor.`
    }
  })

  return {
    comparatorGuideItems,
    comparatorWinNarrative,
    controlTimelineSummary: computed(() => buildControlTimelineSummary(controlTimeline.value)),
    forecastScenarioRows,
    readinessItems,
    readModelBadgeLabel,
    regretTimelineOption: computed(() => buildDecisionControlRegretTimelineOption(controlTimeline.value)),
    sensitivityGuideItems,
    sensitivityOption: computed(() => buildDecisionSensitivityOption(sensitivityRows.value)),
    stateCards,
    strategyOption: computed(() => buildDecisionStrategyEvidenceOption(strategyRows.value))
  }
}

const buildControlTimelineSummary = (
  points: ControlRegretTimelinePoint[]
): DecisionChartSummaryItem[] => {
  if (points.length === 0) {
    return []
  }

  const regretValues = points.map(point => point.regretUah)
  const throughputValues = points.map(point => point.throughputMwh)
  const averageRegret = regretValues.reduce((sum, value) => sum + value, 0) / regretValues.length
  const peakRegret = Math.max(...regretValues)
  const averageThroughput = throughputValues.reduce((sum, value) => sum + value, 0) / throughputValues.length

  return [
    {
      label: 'Avg regret',
      value: `${Math.round(averageRegret).toLocaleString('en-GB')} UAH`,
      detail: 'rolling mean'
    },
    {
      label: 'Peak regret',
      value: `${Math.round(peakRegret).toLocaleString('en-GB')} UAH`,
      detail: 'worst anchor'
    },
    {
      label: 'Avg throughput',
      value: `${averageThroughput.toFixed(2)} MWh`,
      detail: 'schedule intensity'
    }
  ]
}

const formatModelLabel = (modelName: string): string => {
  if (modelName.includes(' ') || modelName.includes('/')) {
    return modelName
  }

  return modelName
    .split('_')
    .filter(Boolean)
    .map(part => part.length <= 3 ? part.toUpperCase() : `${part[0]?.toUpperCase() ?? ''}${part.slice(1)}`)
    .join(' ')
}
