import type {
  BaselineRecommendationPoint,
  FutureForecastSeriesResponse,
  FutureStackPreviewResponse,
  OperatorLoadForecastPointResponse,
  OperatorRecommendationResponse,
  OperatorSocProjectionPointResponse,
  OperatorStrategyOptionResponse
} from '~/types/control-plane'

const SOURCE_PRIORITY: Record<string, number> = {
  official: 0,
  calibrated: 1,
  compact: 2
}

const OFFSETLESS_ISO_LOCAL_HOUR = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?$/

const MODEL_PRIORITY: Record<string, number> = {
  nbeatsx: 0,
  tft: 1
}

const EXCLUDED_RECOMMENDATION_STRATEGIES = new Set([
  'decision_transformer',
  'nbeatsx_official_v0',
  'tft_official_v0'
])

const OFFLINE_V2_PLUS_STRATEGY_ID = 'schedule_value_learner_v2_plus'

export interface StrategyReadinessItem {
  strategyId: string
  label: string
  status: 'ready' | 'blocked'
  reason: string
}

export interface OperatorForecastChartSource {
  kind: 'operator_delivery_day' | 'future_stack_context' | 'empty'
  series: FutureForecastSeriesResponse[]
  windowStart: string | null | undefined
  windowEnd: string | null | undefined
}

export interface RecommendationInputSignalPoint {
  label: string
  forecastPriceUahMwh: number
  selectedNetPowerMw: number
  projectedSocPercent: number | null
  siteNetLoadMw: number | null
}

type OperatorForecastChartSourceInput = {
  futureStack: Pick<
    FutureStackPreviewResponse,
    'forecast_series' | 'forecast_window_start' | 'forecast_window_end'
  > | null | undefined
  operatorRecommendation: Pick<
    OperatorRecommendationResponse,
    'forecast_model_series' | 'target_delivery_window_start' | 'target_delivery_window_end'
  > | null | undefined
}

export const formatForecastWindowLabel = (
  forecastWindowStart: string | null | undefined,
  forecastWindowEnd: string | null | undefined
): string => {
  if (!forecastWindowStart || !forecastWindowEnd) {
    return 'forecast window pending'
  }

  return `${formatWindowTimestamp(forecastWindowStart)} -> ${formatWindowTimestamp(forecastWindowEnd)}`
}

export const selectOperatorForecastChartSource = (
  input: OperatorForecastChartSourceInput
): OperatorForecastChartSource => {
  const deliverySeries = input.operatorRecommendation?.forecast_model_series ?? []
  if (hasForecastRows(deliverySeries)) {
    return {
      kind: 'operator_delivery_day',
      series: deliverySeries,
      windowStart: input.operatorRecommendation?.target_delivery_window_start,
      windowEnd: input.operatorRecommendation?.target_delivery_window_end
    }
  }

  const futureStackSeries = input.futureStack?.forecast_series ?? []
  if (hasForecastRows(futureStackSeries)) {
    return {
      kind: 'future_stack_context',
      series: futureStackSeries,
      windowStart: input.futureStack?.forecast_window_start,
      windowEnd: input.futureStack?.forecast_window_end
    }
  }

  return {
    kind: 'empty',
    series: [],
    windowStart: null,
    windowEnd: null
  }
}

export const buildRecommendationInputSignalRows = (
  scheduleRows: BaselineRecommendationPoint[],
  socProjectionRows: OperatorSocProjectionPointResponse[] = [],
  loadForecastRows: OperatorLoadForecastPointResponse[] = []
): RecommendationInputSignalPoint[] => scheduleRows.map((row, index) => {
  const socProjection = socProjectionRows[index]
  const loadForecast = loadForecastRows[index]
  return {
    label: formatWindowTimestamp(row.interval_start),
    forecastPriceUahMwh: Math.round(row.forecast_price_uah_mwh),
    selectedNetPowerMw: Number(row.recommended_net_power_mw.toFixed(3)),
    projectedSocPercent: roundSocPercent(
      socProjection?.planning_soc ?? row.projected_soc_after_fraction
    ),
    siteNetLoadMw: typeof loadForecast?.net_load_mw === 'number'
      ? Number(loadForecast.net_load_mw.toFixed(3))
      : null
  }
})

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

export const buildRecommendationStrategySelectItems = (
  strategies: OperatorStrategyOptionResponse[]
): Array<{ label: string, value: string, disabled: boolean }> => strategies
  .filter((strategy) => {
    if (!strategy.enabled) {
      return false
    }

    if (EXCLUDED_RECOMMENDATION_STRATEGIES.has(strategy.strategy_id)) {
      return false
    }

    return strategy.strategy_id === 'strict_similar_day' || typeof strategy.mean_regret_uah === 'number'
  })
  .sort((left, right) => {
    if (left.strategy_id === OFFLINE_V2_PLUS_STRATEGY_ID) {
      return -1
    }

    if (right.strategy_id === OFFLINE_V2_PLUS_STRATEGY_ID) {
      return 1
    }

    if (left.strategy_id === 'strict_similar_day') {
      return -1
    }

    if (right.strategy_id === 'strict_similar_day') {
      return 1
    }

    return (left.mean_regret_uah ?? Number.POSITIVE_INFINITY) - (right.mean_regret_uah ?? Number.POSITIVE_INFINITY)
  })
  .map(strategy => ({
    label: formatRecommendationStrategyLabel(strategy),
    value: strategy.strategy_id,
    disabled: false
  }))

export const buildStrategyReadinessItems = (
  strategies: OperatorStrategyOptionResponse[]
): StrategyReadinessItem[] => strategies
  .filter(strategy => strategy.enabled)
  .filter(strategy => !EXCLUDED_RECOMMENDATION_STRATEGIES.has(strategy.strategy_id))
  .filter(strategy => strategy.strategy_id === 'strict_similar_day' || typeof strategy.mean_regret_uah === 'number')
  .map(strategy => ({
    strategyId: strategy.strategy_id,
    label: strategy.label,
    status: 'ready',
    reason: typeof strategy.mean_regret_uah === 'number'
      ? `${Math.round(strategy.mean_regret_uah).toLocaleString('en-GB')} UAH mean regret`
      : strategy.reason
  }))

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

const hasForecastRows = (series: FutureForecastSeriesResponse[]): boolean => (
  series.some(candidate => candidate.points.length > 0)
)

const formatRecommendationStrategyLabel = (strategy: OperatorStrategyOptionResponse): string => {
  if (typeof strategy.mean_regret_uah !== 'number') {
    return strategy.label
  }

  return `${strategy.label} · ${Math.round(strategy.mean_regret_uah).toLocaleString('en-GB')} UAH`
}

const roundSocPercent = (value: number | null | undefined): number | null => {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null
  }

  return Math.round(value * 100)
}

export const formatWindowTimestamp = (timestamp: string): string => {
  const offsetlessMatch = OFFSETLESS_ISO_LOCAL_HOUR.exec(timestamp)
  if (offsetlessMatch) {
    const [, year, month, day, hour, minute] = offsetlessMatch
    return new Date(Date.UTC(
      Number(year),
      Number(month) - 1,
      Number(day),
      Number(hour),
      Number(minute)
    )).toLocaleString('en-GB', {
      day: '2-digit',
      month: 'short',
      hour: '2-digit',
      minute: '2-digit',
      timeZone: 'UTC'
    })
  }

  return new Date(timestamp).toLocaleString('en-GB', {
    day: '2-digit',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
    timeZone: 'Europe/Kyiv'
  })
}
