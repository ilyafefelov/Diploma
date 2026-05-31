import type { OperatorRecommendationResponse } from '../../types/control-plane'
import {
  CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE,
  CURRENT_REGRET_LADDER
} from '../../utils/defenseDataset'
import type { DefenseModelRow } from '../../utils/defenseDataset'
import type {
  ControlRegretTimelinePoint,
  SensitivityEvidenceRow
} from '../../utils/operatorDecisionEvidence'

export const buildFallbackStrategyEvidenceRows = (
  operatorRecommendation?: OperatorRecommendationResponse | null
): DefenseModelRow[] => {
  const selectedStrategy = operatorRecommendation?.available_strategies.find((strategy) => {
    return strategy.strategy_id === operatorRecommendation.selected_strategy_id
  })

  return CURRENT_REGRET_LADDER.map(point => ({
    modelName: point.label,
    role: point.label === 'strict_similar_day' ? 'control' : 'forecast_candidate',
    anchorCount: 90,
    meanRegretUah: selectedStrategy?.mean_regret_uah !== null
      && selectedStrategy?.mean_regret_uah !== undefined
      && point.label === 'Calibrated V2+'
      ? selectedStrategy.mean_regret_uah
      : point.meanRegretUah,
    medianRegretUah: point.meanRegretUah,
    meanDecisionValueUah: 0,
    meanOracleValueUah: point.meanRegretUah,
    winRate: point.label === 'Calibrated V2+'
      ? selectedStrategy?.win_rate ?? 1
      : 0,
    meanThroughputMwh: operatorRecommendation?.economics?.total_throughput_mwh ?? 0
  }))
}

export const buildFallbackControlRegretTimeline = (
  operatorRecommendation?: OperatorRecommendationResponse | null
): ControlRegretTimelinePoint[] => {
  const selectedThroughput = operatorRecommendation?.economics?.total_throughput_mwh ?? 0

  return CURRENT_REGRET_LADDER.map(point => ({
    anchorLabel: point.label,
    regretUah: point.meanRegretUah,
    decisionValueUah: 0,
    oracleValueUah: point.meanRegretUah,
    throughputMwh: point.label === 'Calibrated V2+' ? selectedThroughput : 0
  }))
}

export const buildFallbackSensitivityEvidenceRows = (
  operatorRecommendation?: OperatorRecommendationResponse | null
): SensitivityEvidenceRow[] => {
  const scheduleRows = operatorRecommendation?.recommendation_schedule.length ?? 0
  const forecastContextRows = operatorRecommendation?.forecast_model_series.reduce((total, series) => {
    return total + series.points.length
  }, 0) ?? 0

  return [
    {
      bucket: 'strict control',
      rows: 90,
      meanRegretUah: CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.strictMeanRegretUah,
      meanForecastMaeUahMwh: 0,
      meanDispatchSpreadErrorUahMwh: 0
    },
    {
      bucket: 'selected V2+',
      rows: scheduleRows || 24,
      meanRegretUah: CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.meanRegretUah,
      meanForecastMaeUahMwh: 0,
      meanDispatchSpreadErrorUahMwh: 0
    },
    {
      bucket: 'forecast context',
      rows: forecastContextRows || scheduleRows || 24,
      meanRegretUah: CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.meanRegretUah,
      meanForecastMaeUahMwh: 0,
      meanDispatchSpreadErrorUahMwh: 0
    }
  ]
}
