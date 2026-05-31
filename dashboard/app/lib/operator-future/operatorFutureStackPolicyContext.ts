import type {
  DecisionPolicyPreviewPointResponse,
  DecisionPolicyPreviewResponse,
  FutureForecastSeriesResponse,
  OperatorRecommendationResponse,
  RuntimeAccelerationResponse
} from '~/types/control-plane'
import { formatWindowTimestamp } from './operatorFutureStackCore'

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

export const isChartSafeForecastSeries = (series: FutureForecastSeriesResponse): boolean => {
  const normalizedBoundary = series.quality_boundary.toLowerCase()

  return series.points.length > 0
    && series.out_of_dam_cap_rows === 0
    && !normalizedBoundary.includes('needs_calibration')
}

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
  if (operatorRecommendation.policy_forecast_context_row_count === 0) {
    return 'forecast context not applicable'
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
