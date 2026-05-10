import type {
  DecisionPolicyPreviewPointResponse,
  DecisionPolicyPreviewResponse,
  FutureForecastSeriesResponse,
  OperatorRecommendationResponse,
  RuntimeAccelerationResponse,
  OperatorStrategyOptionResponse
} from '~/types/control-plane'

const SOURCE_PRIORITY: Record<string, number> = {
  official: 0,
  calibrated: 1,
  compact: 2
}

const MODEL_PRIORITY: Record<string, number> = {
  nbeatsx: 0,
  tft: 1
}

const READINESS_STRATEGY_IDS = new Set([
  'nbeatsx_official_v0',
  'tft_official_v0',
  'decision_transformer'
])

export interface StrategyReadinessItem {
  strategyId: string
  label: string
  status: 'ready' | 'blocked'
  reason: string
}

export interface PolicyForecastContextPoint {
  label: string
  nbeatsxForecastUahMwh: number
  tftForecastUahMwh: number
  forecastUncertaintyUahMwh: number
  forecastSpreadUahMwh: number
}

type PolicyForecastContextRow = Pick<
  DecisionPolicyPreviewPointResponse,
  | 'interval_start'
  | 'state_market_price_uah_mwh'
  | 'state_nbeatsx_forecast_uah_mwh'
  | 'state_tft_forecast_uah_mwh'
  | 'state_forecast_uncertainty_uah_mwh'
  | 'state_forecast_spread_uah_mwh'
>

export const formatForecastWindowLabel = (
  forecastWindowStart: string | null | undefined,
  forecastWindowEnd: string | null | undefined
): string => {
  if (!forecastWindowStart || !forecastWindowEnd) {
    return 'forecast window pending'
  }

  return `${formatWindowTimestamp(forecastWindowStart)} -> ${formatWindowTimestamp(forecastWindowEnd)}`
}

export const sortFutureForecastSeries = (
  series: FutureForecastSeriesResponse[]
): FutureForecastSeriesResponse[] => [...series].sort((left, right) => {
  const sourceDelta = sourcePriority(left.source_status) - sourcePriority(right.source_status)
  if (sourceDelta !== 0) {
    return sourceDelta
  }

  const modelDelta = modelPriority(left.model_name) - modelPriority(right.model_name)
  if (modelDelta !== 0) {
    return modelDelta
  }

  return left.model_name.localeCompare(right.model_name)
})

export const filterOfficialPolicyValueSeries = (
  series: FutureForecastSeriesResponse[]
): FutureForecastSeriesResponse[] => sortFutureForecastSeries(series)
  .filter(candidate => candidate.source_status.toLowerCase().includes('official') && candidate.points.length > 0)

export const buildStrategySelectItems = (
  strategies: OperatorStrategyOptionResponse[]
): Array<{ label: string, value: string, disabled: boolean }> => strategies.map(strategy => ({
  label: strategy.enabled ? strategy.label : `${strategy.label} - ${strategy.reason}`,
  value: strategy.strategy_id,
  disabled: !strategy.enabled
}))

export const buildStrategyReadinessItems = (
  strategies: OperatorStrategyOptionResponse[]
): StrategyReadinessItem[] => strategies
  .filter(strategy => READINESS_STRATEGY_IDS.has(strategy.strategy_id))
  .map(strategy => ({
    strategyId: strategy.strategy_id,
    label: strategy.label,
    status: strategy.enabled ? 'ready' : 'blocked',
    reason: strategy.reason
  }))

export const buildPolicyForecastContextPoints = (
  policyRows: PolicyForecastContextRow[]
): PolicyForecastContextPoint[] => policyRows.map((row) => {
  const nbeatsxForecast = row.state_nbeatsx_forecast_uah_mwh ?? row.state_market_price_uah_mwh
  const tftForecast = row.state_tft_forecast_uah_mwh ?? nbeatsxForecast
  const forecastSpread = row.state_forecast_spread_uah_mwh ?? tftForecast - nbeatsxForecast
  return {
    label: formatWindowTimestamp(row.interval_start),
    nbeatsxForecastUahMwh: nbeatsxForecast,
    tftForecastUahMwh: tftForecast,
    forecastUncertaintyUahMwh: row.state_forecast_uncertainty_uah_mwh ?? Math.abs(forecastSpread),
    forecastSpreadUahMwh: forecastSpread
  }
})

export const formatPolicyForecastContextLabel = (
  decisionPolicy: Pick<
    DecisionPolicyPreviewResponse,
    'forecast_context_coverage_ratio' | 'forecast_context_row_count' | 'row_count'
  > | null | undefined
): string => {
  if (!decisionPolicy) {
    return 'forecast context pending'
  }

  const percentage = Math.round(decisionPolicy.forecast_context_coverage_ratio * 100)
  return `${percentage}% forecast-conditioned (${decisionPolicy.forecast_context_row_count}/${decisionPolicy.row_count} rows)`
}

export const formatOperatorPolicyForecastContextLabel = (
  operatorRecommendation: Pick<
    OperatorRecommendationResponse,
    'policy_forecast_context_coverage_ratio' | 'policy_forecast_context_row_count'
  > | null | undefined
): string => {
  if (!operatorRecommendation) {
    return 'forecast context pending'
  }

  const percentage = Math.round(operatorRecommendation.policy_forecast_context_coverage_ratio * 100)
  return `${percentage}% forecast-conditioned (${operatorRecommendation.policy_forecast_context_row_count} rows)`
}

export const formatRuntimeAccelerationLabel = (
  runtime: RuntimeAccelerationResponse | null | undefined
): string => {
  if (!runtime) {
    return 'runtime pending'
  }
  if (runtime.device_type === 'cuda') {
    return `CUDA / ${runtime.device_name}`
  }
  if (runtime.device_type === 'mps') {
    return `MPS / ${runtime.device_name}`
  }
  return `${runtime.device_name} / ${runtime.backend}`
}

export const formatForecastQualityLabel = (series: FutureForecastSeriesResponse): string => {
  if (series.out_of_dam_cap_rows > 0) {
    return `${series.out_of_dam_cap_rows} out-of-cap row${series.out_of_dam_cap_rows === 1 ? '' : 's'}`
  }

  if (series.quality_boundary === 'smoke_values_inside_dam_cap_not_value_claim') {
    return 'inside DAM cap / smoke only'
  }

  return 'inside DAM cap'
}

const sourcePriority = (sourceStatus: string): number => {
  const normalized = sourceStatus.toLowerCase()
  for (const [needle, priority] of Object.entries(SOURCE_PRIORITY)) {
    if (normalized.includes(needle)) {
      return priority
    }
  }
  return 99
}

const modelPriority = (modelName: string): number => {
  const normalized = modelName.toLowerCase()
  for (const [needle, priority] of Object.entries(MODEL_PRIORITY)) {
    if (normalized.includes(needle)) {
      return priority
    }
  }
  return 99
}

const formatWindowTimestamp = (timestamp: string): string => new Date(timestamp).toLocaleString('en-GB', {
  day: '2-digit',
  month: 'short',
  hour: '2-digit',
  minute: '2-digit',
  timeZone: 'Europe/Kyiv'
})
