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
  status: 'headline' | 'closed' | 'shadow' | 'blocked' | 'next'
}

export interface PolandLag24ShadowChallengerHeadline {
  label: string
  latestHoldoutImprovementVsV2Plus: number
  latestHoldoutMeanRegretUah: number
  frozenV2PlusMeanRegretUah: number
  passingFeatureCount: number
  featureCount: number
  blockedFeatureCount: number
  rollingPassCount: number
  rollingWindowCount: number
  status: 'positive_not_promoted'
  interpretation: string
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

export interface DefenseTftSafeSelectionExplanation {
  label: string
  englishTitle: string
  ukrainianTitle: string
  englishBody: string
  ukrainianBody: string
  status: 'opportunity' | 'leakage' | 'diagnosis' | 'next'
}

export interface DefenseV2PlusImprovementPoint {
  label: string
  value: string
  englishBody: string
  ukrainianBody: string
  status: 'candidate_space' | 'fallback' | 'scoring' | 'boundary'
}

export interface DefenseDtLavaPlanStep {
  label: string
  value: string
  body: string
  status: 'input' | 'model' | 'gate' | 'boundary'
}

export interface DefenseBilingualExplainerSection {
  label: string
  englishTitle: string
  englishBody: string
  englishBullets: string[]
  ukrainianTitle: string
  ukrainianBody: string
  ukrainianBullets: string[]
}
