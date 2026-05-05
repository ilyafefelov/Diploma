import type {
  ForecastStrategyComparisonResponse,
  RealDataBenchmarkResponse
} from '../types/control-plane'

export type BenchmarkKind = 'forecast_comparison' | 'real_data_rolling_origin' | 'calibrated_ensemble_gate'

export interface BenchmarkRow {
  tenantId: string
  evaluationId: string
  anchorTimestamp: string
  forecastModelName: string
  strategyKind: string
  decisionValueUah: number
  oracleValueUah: number
  regretUah: number
  regretRatio: number
  totalDegradationPenaltyUah: number
  totalThroughputMwh: number
  committedAction: string
  committedPowerMw: number
  rankByRegret: number
  evaluationPayload: Record<string, unknown>
}

export interface BenchmarkDataset {
  kind: BenchmarkKind
  title: string
  tenantIds: string[]
  marketVenue: string
  generatedAt: string
  dataQualityTier: string
  anchorCount: number
  modelCount: number
  bestModelName: string | null
  meanRegretUah: number
  medianRegretUah: number
  rows: BenchmarkRow[]
}

export const normalizeForecastComparison = (
  response: ForecastStrategyComparisonResponse
): BenchmarkDataset => {
  const rows: BenchmarkRow[] = response.comparisons.map(row => ({
    tenantId: response.tenant_id,
    evaluationId: response.evaluation_id,
    anchorTimestamp: response.anchor_timestamp,
    forecastModelName: row.forecast_model_name,
    strategyKind: row.strategy_kind,
    decisionValueUah: row.decision_value_uah,
    oracleValueUah: row.oracle_value_uah,
    regretUah: row.regret_uah,
    regretRatio: row.regret_ratio,
    totalDegradationPenaltyUah: row.total_degradation_penalty_uah,
    totalThroughputMwh: row.total_throughput_mwh,
    committedAction: row.committed_action,
    committedPowerMw: row.committed_power_mw,
    rankByRegret: row.rank_by_regret,
    evaluationPayload: row.evaluation_payload
  }))

  const regrets = rows.map(row => row.regretUah)
  const bestRow = [...rows].sort((left, right) => left.rankByRegret - right.rankByRegret)[0] ?? null

  return {
    kind: 'forecast_comparison',
    title: 'Latest forecast comparison',
    tenantIds: [response.tenant_id],
    marketVenue: response.market_venue,
    generatedAt: response.generated_at,
    dataQualityTier: 'demo_grade',
    anchorCount: 1,
    modelCount: rows.length,
    bestModelName: bestRow?.forecastModelName ?? null,
    meanRegretUah: mean(regrets),
    medianRegretUah: median(regrets),
    rows
  }
}

export const normalizeRealDataBenchmark = (
  response: RealDataBenchmarkResponse,
  kind: Extract<BenchmarkKind, 'real_data_rolling_origin' | 'calibrated_ensemble_gate'> = 'real_data_rolling_origin'
): BenchmarkDataset => ({
  kind,
  title: kind === 'real_data_rolling_origin' ? 'Rolling-origin benchmark' : 'Calibrated ensemble gate',
  tenantIds: [response.tenant_id],
  marketVenue: response.market_venue,
  generatedAt: response.generated_at,
  dataQualityTier: response.data_quality_tier,
  anchorCount: response.anchor_count,
  modelCount: response.model_count,
  bestModelName: response.best_model_name,
  meanRegretUah: response.mean_regret_uah,
  medianRegretUah: response.median_regret_uah,
  rows: response.rows.map(row => ({
    tenantId: response.tenant_id,
    evaluationId: row.evaluation_id,
    anchorTimestamp: row.anchor_timestamp,
    forecastModelName: row.forecast_model_name,
    strategyKind: kind === 'real_data_rolling_origin'
      ? 'real_data_rolling_origin_benchmark'
      : 'calibrated_value_aware_ensemble_gate',
    decisionValueUah: row.decision_value_uah,
    oracleValueUah: row.oracle_value_uah,
    regretUah: row.regret_uah,
    regretRatio: row.regret_ratio,
    totalDegradationPenaltyUah: row.total_degradation_penalty_uah,
    totalThroughputMwh: row.total_throughput_mwh,
    committedAction: row.committed_action,
    committedPowerMw: row.committed_power_mw,
    rankByRegret: row.rank_by_regret,
    evaluationPayload: row.evaluation_payload
  }))
})

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
