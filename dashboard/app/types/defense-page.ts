export interface DefenseMetric {
  label: string
  value: string
  note: string
  tooltipTitle: string
  tooltipBody: string
  tooltipFormula: string
}

export interface DefenseNarrativeStep {
  label: string
  text: string
}

export interface DefenseRegretLadderRow {
  label: string
  note: string
  status: string
  meanRegretUah: number
  barWidthPercent: number
}

export interface DefenseTftPortfolioRow {
  label: string
  note: string
  status: string
  numerator: number
  denominator: number
  percentLabel: string
  barWidthPercent: number
}

export interface DefenseOfflinePromotionRow {
  source_model_name: string
  latest_selected_mean_regret_uah: number
  rolling_strict_pass_window_count: number
  rolling_window_count: number
  production_promote: boolean
  promotion_blocker: string
}

export interface DefenseDtStatusRow {
  label: string
  value: string
  note: string
}

export interface DefenseDtComparisonRow {
  label: string
  note: string
  status: string
  regretBarWidthPercent: number
  meanRegretUah: number
  meanValueUah: number
}

export interface DefenseDtPassportRow {
  label: string
  value: string
  reason: string
  status: string
}

export interface DefenseFutureForecastRow {
  modelName: string
  modelFamily: string
  sourceStatus: string
  uncertaintyKind: string
  pointCount: number
  firstForecast: number | null
  lastForecast: number | null
  meanRegretUah: number | null
  winRate: number | null
}

export interface DefenseModelRow {
  modelName: string
  role: string
  anchorCount: number
  meanRegretUah: number
  medianRegretUah: number
  winRate: number
  meanThroughputMwh: number
}

export interface DefenseReadinessRow {
  label: string
  status: string
  metric: string
  boundary: string
}

export interface DefenseDtPolicySummary {
  readiness: string
  rows: number
  violations: number
  meanValueGap: number
  valueVsHold: number
  stateFeatures: string
  valueInterpretation: string
  operatorBoundary: string
  boundary: string
}

export interface DefenseErrorRow {
  key: string
  message: string
}
