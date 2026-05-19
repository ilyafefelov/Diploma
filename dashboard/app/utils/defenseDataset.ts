import type {
  DecisionPolicyPreviewResponse,
  DecisionTransformerTrajectoryResponse,
  DflRelaxedPilotResponse,
  DflScheduleValueProductionGateResponse,
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
  label: 'DFL' | 'Decision Transformer' | 'DT policy preview' | 'Paper trading'
  status: string
  metric: string
  boundary: string
}

export interface OfflineStrategyPromotionHeadline {
  modelName: string
  meanRegretUah: number
  strictMeanRegretUah: number
  improvementVsStrict: number
  rollingPassCount: number
  rollingWindowCount: number
  marketExecutionEnabled: boolean
  claimBoundary: string
}

export interface TftPortfolioClosureHeadline {
  label: string
  latestTenantAnchors: number
  tftBetterCandidateCount: number
  selectorFallbackCount: number
  rollingPassCount: number
  rollingWindowCount: number
  candidatePortfolioRows: number
  status: 'negative_evidence'
  interpretation: string
}

export interface DashboardExperimentCard {
  label: string
  value: string
  meta: string
  status: 'headline' | 'closed' | 'blocked' | 'next'
}

export interface DefenseRegretLadderPoint {
  label: string
  meanRegretUah: number
  note: string
  status: 'control' | 'improved' | 'headline' | 'plateau' | 'failed'
}

export interface DefensePortfolioDiagnosticPoint {
  label: string
  numerator: number
  denominator: number
  note: string
  status: 'opportunity' | 'fallback' | 'blocked'
}

export interface DefenseTftUseDecision {
  label: string
  value: string
  body: string
  status: 'useful' | 'blocked' | 'next'
}

export interface DefenseDtLavaPlanStep {
  label: string
  value: string
  body: string
  status: 'input' | 'model' | 'gate' | 'boundary'
}

export const CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE: OfflineStrategyPromotionHeadline = {
  modelName: 'Schedule/Value Learner V2+',
  meanRegretUah: 174.77,
  strictMeanRegretUah: 310.58,
  improvementVsStrict: 0.4373,
  rollingPassCount: 4,
  rollingWindowCount: 4,
  marketExecutionEnabled: false,
  claimBoundary: 'Offline Strategy Promotion evidence only'
}

export const CURRENT_TFT_PORTFOLIO_CLOSURE: TftPortfolioClosureHeadline = {
  label: 'NBEATSx+TFT candidate portfolio',
  latestTenantAnchors: 90,
  tftBetterCandidateCount: 24,
  selectorFallbackCount: 90,
  rollingPassCount: 0,
  rollingWindowCount: 4,
  candidatePortfolioRows: 120380,
  status: 'negative_evidence',
  interpretation: 'TFT offered local candidate diversity, but the prior-only selector could not exploit it robustly.'
}

export const CURRENT_DASHBOARD_EXPERIMENTS: DashboardExperimentCard[] = [
  {
    label: 'Headline',
    value: 'V2+',
    meta: '174.77 UAH mean regret / 4 of 4 rolling windows',
    status: 'headline'
  },
  {
    label: 'TFT portfolio',
    value: '0/4 rolling',
    meta: '24/90 local TFT opportunities, selector fell back on latest holdout',
    status: 'closed'
  },
  {
    label: 'Market coupling',
    value: 'blocked',
    meta: 'ENTSO-E/Poland remains governance-only, not training input',
    status: 'blocked'
  },
  {
    label: 'Next branch',
    value: 'DT/LAVA',
    meta: 'candidate/value or schedule-neighbor supervision against V2+',
    status: 'next'
  }
]

export const CURRENT_REGRET_LADDER: DefenseRegretLadderPoint[] = [
  {
    label: 'strict_similar_day',
    meanRegretUah: 310.58,
    note: 'Frozen control comparator',
    status: 'control'
  },
  {
    label: 'Frozen V2',
    meanRegretUah: 206.37,
    note: 'First schedule/value promotion evidence',
    status: 'improved'
  },
  {
    label: 'Raw V2+',
    meanRegretUah: 193.36,
    note: 'Official global-panel NBEATSx, uncalibrated source',
    status: 'improved'
  },
  {
    label: 'Calibrated V2+',
    meanRegretUah: 174.77,
    note: 'Prior-only forecast correction by horizon; current thesis headline, 4/4 rolling windows',
    status: 'headline'
  },
  {
    label: 'V3/V4/V5',
    meanRegretUah: 174.77,
    note: 'Matched V2+ through fallback; no replacement claim',
    status: 'plateau'
  },
  {
    label: 'Official bridge DFL/DT',
    meanRegretUah: 367.70,
    note: 'Negative evidence versus V2+',
    status: 'failed'
  }
]

export const CURRENT_TFT_PORTFOLIO_DIAGNOSTICS: DefensePortfolioDiagnosticPoint[] = [
  {
    label: 'TFT local opportunities',
    numerator: 24,
    denominator: 90,
    note: 'TFT had a better candidate on some latest tenant-anchors.',
    status: 'opportunity'
  },
  {
    label: 'Selector fallback',
    numerator: 90,
    denominator: 90,
    note: 'Prior-only selector stayed with V2+ on the latest holdout.',
    status: 'fallback'
  },
  {
    label: 'Rolling robustness',
    numerator: 0,
    denominator: 4,
    note: 'Portfolio did not pass the rolling strict replay gate.',
    status: 'blocked'
  }
]

export const CURRENT_TFT_USE_DECISION: DefenseTftUseDecision[] = [
  {
    label: 'What worked',
    value: '24 / 90',
    body: 'TFT produced candidate schedules that beat V2+ on some latest tenant-anchor rows after strict LP/oracle scoring.',
    status: 'useful'
  },
  {
    label: 'Why not use now',
    value: '90 / 90 fallback',
    body: 'Before realized prices were known, the prior-only selector could not identify those wins safely, so it chose V2+ for every latest-holdout row.',
    status: 'blocked'
  },
  {
    label: 'Promotion blocker',
    value: '0 / 4 rolling',
    body: 'Using the 24 post-hoc winners directly would leak final-holdout information. TFT must pass rolling robustness before it becomes a selected strategy.',
    status: 'blocked'
  },
  {
    label: 'How to make TFT usable',
    value: 'future branch',
    body: 'Train a stronger prior-only portfolio selector or DT/LAVA schedule-neighbor model that predicts when TFT risk schedules beat V2+, then re-score through the same gate.',
    status: 'next'
  }
]

export const CURRENT_DT_LAVA_NEXT_STEPS: DefenseDtLavaPlanStep[] = [
  {
    label: 'Teacher data',
    value: 'V2+ + oracle schedules',
    body: 'Train only on prior/train anchors with V2+, oracle/high-value schedules, and candidate value labels. Final holdout stays scoring-only.',
    status: 'input'
  },
  {
    label: 'Prediction target',
    value: 'schedule block / candidate',
    body: 'Predict schedule family, schedule block, or candidate index first. Do not start by emitting raw hourly BUY/SELL/HOLD commands.',
    status: 'model'
  },
  {
    label: 'LAVA-style layer',
    value: 'feasible neighbors',
    body: 'Use precomputed LP-feasible schedule neighbors and value labels, so the model learns decision quality without live solver calls inside training.',
    status: 'model'
  },
  {
    label: 'Promotion gate',
    value: 'beat V2+',
    body: 'A challenger must beat 174.77 UAH mean regret, avoid median degradation, preserve rolling robustness, and keep market_execution_enabled=false.',
    status: 'gate'
  }
]

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
  dtPolicy: DecisionPolicyPreviewResponse | null
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
    label: 'DT policy preview',
    status: input.dtPolicy && input.dtPolicy.row_count > 0 ? input.dtPolicy.policy_readiness : 'not materialized',
    metric: input.dtPolicy
      ? `${formatCompactNumber(input.dtPolicy.mean_value_gap_uah)} UAH mean value gap / ${formatPercent(input.dtPolicy.forecast_context_coverage_ratio)} forecast-conditioned`
      : 'no rows',
    boundary: input.dtPolicy?.market_execution_enabled ? 'market execution enabled' : 'preview only'
  },
  {
    label: 'Paper trading',
    status: input.live?.simulated_only ? 'simulated only' : 'not materialized',
    metric: input.live ? `${input.live.row_count} rows` : 'no rows',
    boundary: 'not market execution'
  }
]

export const summarizeScheduleValuePromotionReadModel = (
  response: DflScheduleValueProductionGateResponse | null
): string => {
  if (!response) {
    return 'backend read model pending'
  }

  return `${response.production_promote_count}/${response.row_count} offline rows passed, market execution ${response.market_execution_enabled ? 'enabled' : 'disabled'}`
}

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
