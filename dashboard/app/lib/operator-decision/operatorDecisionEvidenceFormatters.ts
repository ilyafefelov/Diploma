import type { OperatorForecastScenarioCandidateResponse } from '../../types/control-plane'

const SELECTED_STRATEGY_STATUS_LABELS: Record<string, string> = {
  schedule_value_learner_v2_plus: 'Offline V2+',
  strict_similar_day: 'Strict control',
  nbeatsx_silver_v0: 'Compact NBEATSx',
  tft_silver_v0: 'Compact TFT',
  decision_transformer: 'DT preview'
}

export const formatAnchorLabel = (timestamp: string): string => new Date(timestamp).toLocaleDateString('en-GB', {
  day: '2-digit',
  month: 'short'
})

export const formatFraction = (value: number): string => `${Math.round(value * 100)}%`

export const formatUah = (value: number): string => `${Math.round(value).toLocaleString('en-GB')} UAH`

export const formatSelectedStrategyStatus = (strategyId: string): string => SELECTED_STRATEGY_STATUS_LABELS[strategyId]
  ?? strategyId
    .split('_')
    .filter(Boolean)
    .map(part => part.length <= 3 ? part.toUpperCase() : `${part[0]?.toUpperCase() ?? ''}${part.slice(1)}`)
    .join(' ')

export const formatCandidateStatus = (candidate: OperatorForecastScenarioCandidateResponse): string => {
  if (!candidate.gatekeeper_status.startsWith('passed')) {
    return 'blocked'
  }

  if (candidate.selected_for_operator_preview) {
    return 'selected preview'
  }

  return candidate.advisor_decision.replaceAll('_', ' ')
}
