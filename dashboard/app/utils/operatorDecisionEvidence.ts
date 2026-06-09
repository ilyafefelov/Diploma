import type {
  ForecastDispatchSensitivityResponse,
  OperatorRecommendationResponse,
  RealDataBenchmarkResponse
} from '../types/control-plane'
import {
  buildFallbackControlRegretTimeline,
  buildFallbackSensitivityEvidenceRows,
  buildFallbackStrategyEvidenceRows
} from '../lib/operator-decision/operatorDecisionEvidenceFallbacks'
import {
  formatAnchorLabel,
  formatCandidateStatus,
  formatSelectedStrategyStatus,
  formatUah
} from '../lib/operator-decision/operatorDecisionEvidenceFormatters'
import type { DefenseModelRow } from './defenseDataset'

export {
  buildOperatorDecisionReadinessItems,
  buildOperatorDecisionStateCards
} from '../lib/operator-decision/operatorDecisionStateReadiness'
export type {
  OperatorDecisionReadinessItem,
  OperatorDecisionStateCard
} from '../lib/operator-decision/operatorDecisionStateReadiness'

export interface OperatorStrategyEvidenceRow {
  modelName: string
  role: DefenseModelRow['role']
  meanRegretUah: number
  winRate: number
  regretDeltaVsControlUah: number
  controlComparisonLabel: string
}

export interface ControlRegretTimelinePoint {
  anchorLabel: string
  regretUah: number
  decisionValueUah: number
  oracleValueUah: number
  throughputMwh: number
}

export interface SensitivityEvidenceRow {
  bucket: string
  rows: number
  meanRegretUah: number
  meanForecastMaeUahMwh: number
  meanDispatchSpreadErrorUahMwh: number
}

export interface OperatorForecastScenarioCandidateRow {
  candidateId: string
  modelName: string
  rankLabel: string
  decisionValueLabel: string
  regretLabel: string
  throughputLabel: string
  statusLabel: string
  selectedForPreview: boolean
}

export const buildOperatorStrategyEvidenceRows = (
  modelRows: DefenseModelRow[],
  operatorRecommendation?: OperatorRecommendationResponse | null
): OperatorStrategyEvidenceRow[] => {
  const sourceRows = modelRows.length > 0
    ? modelRows
    : buildFallbackStrategyEvidenceRows(operatorRecommendation)
  const controlRegret = sourceRows.find(row => row.modelName === 'strict_similar_day')?.meanRegretUah ?? null

  return sourceRows
    .map((row) => {
      const regretDelta = controlRegret === null ? 0 : row.meanRegretUah - controlRegret

      return {
        modelName: row.modelName,
        role: row.role,
        meanRegretUah: row.meanRegretUah,
        winRate: row.winRate,
        regretDeltaVsControlUah: regretDelta,
        controlComparisonLabel: row.modelName === 'strict_similar_day'
          ? 'control'
          : `${regretDelta >= 0 ? '+' : ''}${Math.round(regretDelta).toLocaleString('en-GB')} UAH vs control`
      }
    })
    .sort((left, right) => left.meanRegretUah - right.meanRegretUah)
}

export const buildControlRegretTimeline = (
  benchmark: RealDataBenchmarkResponse | null,
  limit = 24,
  operatorRecommendation?: OperatorRecommendationResponse | null
): ControlRegretTimelinePoint[] => {
  const rows = benchmark?.rows
    .filter(row => row.forecast_model_name === 'strict_similar_day')
    .sort((left, right) => left.anchor_timestamp.localeCompare(right.anchor_timestamp))
    .slice(-limit) ?? []

  if (rows.length === 0) {
    return buildFallbackControlRegretTimeline(operatorRecommendation)
  }

  return rows.map(row => ({
    anchorLabel: formatAnchorLabel(row.anchor_timestamp),
    regretUah: row.regret_uah,
    decisionValueUah: row.decision_value_uah,
    oracleValueUah: row.oracle_value_uah,
    throughputMwh: row.total_throughput_mwh
  }))
}

export const buildSensitivityEvidenceRows = (
  sensitivity: ForecastDispatchSensitivityResponse | null,
  operatorRecommendation?: OperatorRecommendationResponse | null
): SensitivityEvidenceRow[] => {
  const rows = sensitivity?.bucket_summary.map(bucket => ({
    bucket: bucket.diagnostic_bucket,
    rows: bucket.rows,
    meanRegretUah: bucket.mean_regret_uah,
    meanForecastMaeUahMwh: bucket.mean_forecast_mae_uah_mwh,
    meanDispatchSpreadErrorUahMwh: bucket.mean_dispatch_spread_error_uah_mwh
  })) ?? []

  return rows.length > 0 ? rows : buildFallbackSensitivityEvidenceRows(operatorRecommendation)
}

export const buildOperatorForecastScenarioCandidateRows = (
  operatorRecommendation?: OperatorRecommendationResponse | null
): OperatorForecastScenarioCandidateRow[] => {
  const candidates = operatorRecommendation?.decision_advisor?.forecast_scenario_candidates ?? []

  return candidates
    .slice()
    .sort((left, right) => left.rank - right.rank)
    .map(candidate => ({
      candidateId: candidate.candidate_id,
      modelName: formatSelectedStrategyStatus(candidate.model_name),
      rankLabel: `#${candidate.rank}`,
      decisionValueLabel: formatUah(candidate.decision_value_uah),
      regretLabel: candidate.regret_to_best_uah <= 0 ? 'best' : `${formatUah(candidate.regret_to_best_uah)} regret`,
      throughputLabel: `${candidate.total_throughput_mwh.toFixed(2)} MWh`,
      statusLabel: formatCandidateStatus(candidate),
      selectedForPreview: candidate.selected_for_operator_preview
    }))
}
