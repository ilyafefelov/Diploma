import type {
  DecisionTransformerTrajectoryResponse,
  DflRelaxedPilotResponse,
  RealDataBenchmarkResponse,
  SimulatedLiveTradingResponse
} from '../types/control-plane'

export interface DefenseBenchmarkSummary {
  tenantId: string
  marketVenue: string
  generatedAt: string
  dataQualityTier: string
  anchorCount: number
  modelCount: number
  bestModelName: string | null
  meanRegretUah: number
  medianRegretUah: number
  sourceMode: 'fastapi_live'
}

export interface DefenseModelRow {
  modelName: string
  role: 'control' | 'forecast_candidate' | 'ensemble_gate'
  anchorCount: number
  meanRegretUah: number
  medianRegretUah: number
  meanDecisionValueUah: number
  meanOracleValueUah: number
  winRate: number
  meanThroughputMwh: number
}

export interface ResearchReadinessRow {
  label: 'DFL' | 'Decision Transformer' | 'Paper trading'
  status: string
  metric: string
  boundary: string
}

export const summarizeDefenseBenchmark = (
  response: RealDataBenchmarkResponse
): DefenseBenchmarkSummary => ({
  tenantId: response.tenant_id,
  marketVenue: response.market_venue,
  generatedAt: response.generated_at,
  dataQualityTier: response.data_quality_tier,
  anchorCount: response.anchor_count,
  modelCount: response.model_count,
  bestModelName: response.best_model_name,
  meanRegretUah: response.mean_regret_uah,
  medianRegretUah: response.median_regret_uah,
  sourceMode: 'fastapi_live'
})

export const buildDefenseModelRows = (
  benchmark: RealDataBenchmarkResponse,
  extraBenchmarks: RealDataBenchmarkResponse[] = []
): DefenseModelRow[] => {
  const allRows = [benchmark, ...extraBenchmarks].flatMap(response => response.rows)
  const modelNames = Array.from(new Set(allRows.map(row => row.forecast_model_name)))

  return modelNames
    .map((modelName) => {
      const rows = allRows.filter(row => row.forecast_model_name === modelName)
      const regrets = rows.map(row => row.regret_uah)

      return {
        modelName,
        role: modelRole(modelName),
        anchorCount: new Set(rows.map(row => row.anchor_timestamp)).size,
        meanRegretUah: mean(regrets),
        medianRegretUah: median(regrets),
        meanDecisionValueUah: mean(rows.map(row => row.decision_value_uah)),
        meanOracleValueUah: mean(rows.map(row => row.oracle_value_uah)),
        winRate: rows.length === 0
          ? 0
          : rows.filter(row => row.rank_by_regret === 1).length / rows.length,
        meanThroughputMwh: mean(rows.map(row => row.total_throughput_mwh))
      }
    })
    .sort((left, right) => modelSortRank(left.modelName) - modelSortRank(right.modelName))
}

export const buildResearchReadinessRows = (input: {
  dfl: DflRelaxedPilotResponse | null
  dt: DecisionTransformerTrajectoryResponse | null
  live: SimulatedLiveTradingResponse | null
}): ResearchReadinessRow[] => [
  {
    label: 'DFL',
    status: input.dfl && input.dfl.row_count > 0 ? 'pilot' : 'not materialized',
    metric: input.dfl ? `${formatCompactNumber(input.dfl.mean_relaxed_regret_uah)} UAH relaxed regret` : 'no rows',
    boundary: 'not full DFL'
  },
  {
    label: 'Decision Transformer',
    status: input.dt && input.dt.row_count > 0 ? 'trajectory data' : 'not materialized',
    metric: input.dt ? `${input.dt.episode_count} episodes / ${input.dt.row_count} rows` : 'no rows',
    boundary: 'not live policy'
  },
  {
    label: 'Paper trading',
    status: input.live?.simulated_only ? 'simulated only' : 'not materialized',
    metric: input.live ? `${input.live.row_count} rows` : 'no rows',
    boundary: 'not market execution'
  }
]

export const formatCompactNumber = (value: number): string => {
  if (Math.abs(value) >= 1000) {
    return Math.round(value).toLocaleString('en-GB')
  }

  return Number.isInteger(value) ? `${value}` : value.toFixed(1)
}

export const formatUah = (value: number): string => `${Math.round(value).toLocaleString('en-GB')} UAH`

export const formatPercent = (value: number): string => `${Math.round(value * 100)}%`

const modelRole = (modelName: string): DefenseModelRow['role'] => {
  if (modelName === 'strict_similar_day') {
    return 'control'
  }

  if (modelName.includes('gate') || modelName.includes('ensemble')) {
    return 'ensemble_gate'
  }

  return 'forecast_candidate'
}

const modelSortRank = (modelName: string): number => {
  if (modelName === 'strict_similar_day') {
    return 0
  }

  if (modelName.includes('tft')) {
    return 1
  }

  if (modelName.includes('nbeatsx')) {
    return 2
  }

  if (modelName.includes('gate') || modelName.includes('ensemble')) {
    return 3
  }

  return 4
}

const mean = (values: number[]): number => {
  if (values.length === 0) {
    return 0
  }

  return values.reduce((total, value) => total + value, 0) / values.length
}

const median = (values: number[]): number => {
  if (values.length === 0) {
    return 0
  }

  const sortedValues = [...values].sort((left, right) => left - right)
  const midpoint = Math.floor(sortedValues.length / 2)

  if (sortedValues.length % 2 === 1) {
    return sortedValues[midpoint] ?? 0
  }

  return ((sortedValues[midpoint - 1] ?? 0) + (sortedValues[midpoint] ?? 0)) / 2
}
