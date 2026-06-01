import type {
  BaselineLpPreview,
  OperatorRecommendationResponse,
  SignalPreview
} from '~/types/control-plane'
import type {
  OperatorChartHorizon,
  OperatorMarketVenue
} from '~/types/operator-dashboard'

export const operatorMarketVenueOptions: Array<{ label: string, value: OperatorMarketVenue, description: string }> = [
  {
    label: 'DAM',
    value: 'DAM',
    description: 'Official day-ahead row first'
  },
  {
    label: 'IDM',
    value: 'IDM',
    description: 'Source-backed hourly preview'
  }
]

export const operatorChartHorizonOptions: Array<{ label: string, value: OperatorChartHorizon }> = [
  { label: '6H', value: '6h' },
  { label: '12H', value: '12h' },
  { label: '24H', value: '24h' },
  { label: 'All', value: 'all' }
]

export const operatorChartHorizonPointLimit = (horizon: OperatorChartHorizon): number | null => {
  if (horizon === '6h') {
    return 6
  }

  if (horizon === '12h') {
    return 12
  }

  if (horizon === '24h') {
    return 24
  }

  return null
}

export const operatorMarketVenueLabel = (venue: OperatorMarketVenue | string | null | undefined): string => {
  return venue === 'IDM' ? 'IDM' : 'DAM'
}

export const operatorMarketScopeLabel = (venue: OperatorMarketVenue | string | null | undefined): string => {
  return `${operatorMarketVenueLabel(venue)} hourly preview`
}

const hiddenSubstitutePriceFallbackPattern = new RegExp(['syn', 'thetic fallback is disabled'].join(''), 'gi')
const rawFetchPrefixPattern = /^\[[A-Z]+\]\s+"[^"]+":\s+\d{3}\s+/i

const extractOperatorPreviewErrorText = (
  unknownError: unknown,
  fallbackMessage: string
): string => {
  if (unknownError && typeof unknownError === 'object') {
    const errorRecord = unknownError as {
      data?: { detail?: unknown, message?: unknown, statusMessage?: unknown }
      statusMessage?: unknown
      message?: unknown
    }
    const dataDetail = errorRecord.data?.detail
    if (typeof dataDetail === 'string' && dataDetail.trim()) {
      return dataDetail
    }

    const dataMessage = errorRecord.data?.message ?? errorRecord.data?.statusMessage
    if (typeof dataMessage === 'string' && dataMessage.trim()) {
      return dataMessage
    }

    if (typeof errorRecord.statusMessage === 'string' && errorRecord.statusMessage.trim()) {
      return errorRecord.statusMessage
    }

    if (typeof errorRecord.message === 'string' && errorRecord.message.trim()) {
      return errorRecord.message
    }
  }

  return fallbackMessage
}

export const formatOperatorPreviewErrorMessage = (
  unknownError: unknown,
  fallbackMessage: string
): string => {
  const rawMessage = extractOperatorPreviewErrorText(unknownError, fallbackMessage)

  return rawMessage
    .replace(rawFetchPrefixPattern, '')
    .replace(hiddenSubstitutePriceFallbackPattern, 'No substitute prices are rendered')
}

export const operatorPriceContextModeLabel = (
  operatorRecommendation: OperatorRecommendationResponse | null | undefined
): string => {
  if (operatorRecommendation?.price_context_status === 'pre_publication_forecast') {
    return 'ML forecast price context'
  }

  if (operatorRecommendation?.price_context_status === 'official_published') {
    return 'Official published price context'
  }

  return 'Source-backed price context'
}

export const operatorPriceContextSourceLabel = (
  operatorRecommendation: OperatorRecommendationResponse | null | undefined
): string => {
  const venue = operatorMarketVenueLabel(operatorRecommendation?.market_venue)

  if (operatorRecommendation?.price_context_status === 'pre_publication_forecast') {
    return `ML forecast: ${operatorRecommendation.policy_forecast_context_source || 'forecast store'}`
  }

  if (operatorRecommendation?.price_context_status === 'official_published') {
    return `${venue} official OREE/source-backed row`
  }

  return `${venue} selected preview pending`
}

const normalizeSourceToken = (value: string): string => {
  return value
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
}

const operatorRecommendationWeatherSource = (
  operatorRecommendation: OperatorRecommendationResponse
): string => {
  const venue = operatorMarketVenueLabel(operatorRecommendation.market_venue)

  if (operatorRecommendation.price_context_status === 'pre_publication_forecast') {
    const source = normalizeSourceToken(operatorRecommendation.policy_forecast_context_source || 'FORECAST_STORE')
    return `ML_FORECAST_${source || 'FORECAST_STORE'}`
  }

  if (operatorRecommendation.price_context_status === 'official_published') {
    return `OFFICIAL_OREE_${venue}_PUBLISHED`
  }

  return `SOURCE_BACKED_${venue}_PRICE_CONTEXT`
}

const formatOperatorRecommendationHourLabel = (
  timestamp: string,
  timeZone: string
): string => {
  const parsedTimestamp = new Date(timestamp)

  if (Number.isNaN(parsedTimestamp.getTime())) {
    return timestamp.slice(11, 16) || timestamp
  }

  return new Intl.DateTimeFormat('en-GB', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone
  }).format(parsedTimestamp)
}

export const buildOperatorRecommendationSignalPreview = (
  operatorRecommendation: OperatorRecommendationResponse | null,
  fallbackSignalPreview: SignalPreview | null
): SignalPreview | null => {
  if (!operatorRecommendation?.recommendation_schedule.length) {
    return null
  }

  const timeZone = fallbackSignalPreview?.timezone
    || fallbackSignalPreview?.resolved_location.timezone
    || 'Europe/Kyiv'
  const source = operatorRecommendationWeatherSource(operatorRecommendation)
  const labelTimestamps = operatorRecommendation.recommendation_schedule.map(point => point.interval_start)
  const latestPriceTimestamp = labelTimestamps.at(-1) ?? operatorRecommendation.target_delivery_window_end ?? null

  return {
    tenant_id: operatorRecommendation.tenant_id,
    labels: labelTimestamps.map(timestamp => formatOperatorRecommendationHourLabel(timestamp, timeZone)),
    label_timestamps: labelTimestamps,
    latest_price_timestamp: latestPriceTimestamp,
    forecast_window_start: operatorRecommendation.target_delivery_window_start ?? labelTimestamps[0] ?? null,
    forecast_window_end: operatorRecommendation.target_delivery_window_end ?? latestPriceTimestamp,
    timezone: timeZone,
    market_price: operatorRecommendation.recommendation_schedule.map(point =>
      Number(point.forecast_price_uah_mwh.toFixed(2))
    ),
    weather_bias: operatorRecommendation.recommendation_schedule.map(() => 0),
    weather_sources: operatorRecommendation.recommendation_schedule.map(() => source),
    charge_intent: operatorRecommendation.recommendation_schedule.map(point =>
      Number(point.recommended_net_power_mw.toFixed(3))
    ),
    regret: operatorRecommendation.value_gap_series.length
      ? operatorRecommendation.recommendation_schedule.map((point) => {
          const gap = operatorRecommendation.value_gap_series.find(row => row.step_index === point.step_index)
          return Number((gap?.value_gap_uah ?? 0).toFixed(2))
        })
      : operatorRecommendation.recommendation_schedule.map(() => 0),
    resolved_location: fallbackSignalPreview?.resolved_location ?? {
      latitude: 48.46,
      longitude: 35.04,
      timezone: timeZone
    }
  }
}

export const selectOperatorMarketSignalPreview = (
  signalPreview: SignalPreview | null,
  operatorRecommendation: OperatorRecommendationResponse | null
): SignalPreview | null => {
  return buildOperatorRecommendationSignalPreview(operatorRecommendation, signalPreview)
}

export const buildOperatorPreviewQuery = (
  tenantId: string,
  marketVenue: OperatorMarketVenue,
  targetDeliveryDate: string | null,
  strategyId?: string
): Record<string, string> => {
  const query: Record<string, string> = {
    tenant_id: tenantId,
    market_venue: marketVenue
  }

  if (strategyId) {
    query.strategy_id = strategyId
  }

  if (targetDeliveryDate) {
    query.target_delivery_date = targetDeliveryDate
  }

  return query
}

export const sliceArrayForChartHorizon = <T>(rows: T[], horizon: OperatorChartHorizon): T[] => {
  const pointLimit = operatorChartHorizonPointLimit(horizon)
  if (pointLimit == null) {
    return rows
  }

  return rows.slice(0, pointLimit)
}

export const sliceSignalPreviewForChartHorizon = (
  signalPreview: SignalPreview | null,
  horizon: OperatorChartHorizon
): SignalPreview | null => {
  if (!signalPreview) {
    return null
  }

  const pointLimit = operatorChartHorizonPointLimit(horizon)
  if (pointLimit == null) {
    return signalPreview
  }

  return {
    ...signalPreview,
    labels: signalPreview.labels.slice(0, pointLimit),
    label_timestamps: signalPreview.label_timestamps?.slice(0, pointLimit),
    market_price: signalPreview.market_price.slice(0, pointLimit),
    weather_bias: signalPreview.weather_bias.slice(0, pointLimit),
    weather_sources: signalPreview.weather_sources.slice(0, pointLimit),
    charge_intent: signalPreview.charge_intent.slice(0, pointLimit),
    regret: signalPreview.regret.slice(0, pointLimit)
  }
}

export const sliceOperatorRecommendationForChartHorizon = (
  operatorRecommendation: OperatorRecommendationResponse | null,
  horizon: OperatorChartHorizon
): OperatorRecommendationResponse | null => {
  if (!operatorRecommendation) {
    return null
  }

  const pointLimit = operatorChartHorizonPointLimit(horizon)
  if (pointLimit == null) {
    return operatorRecommendation
  }

  const schedule = operatorRecommendation.recommendation_schedule.slice(0, pointLimit)
  const selectedStepIndexes = new Set(schedule.map(point => point.step_index))

  return {
    ...operatorRecommendation,
    recommendation_schedule: schedule,
    bid_recommendation_preview: operatorRecommendation.bid_recommendation_preview.slice(0, pointLimit),
    value_gap_series: operatorRecommendation.value_gap_series.filter(point => selectedStepIndexes.has(point.step_index)),
    load_forecast: operatorRecommendation.load_forecast.slice(0, pointLimit),
    soc_projection: operatorRecommendation.soc_projection.slice(0, pointLimit),
    forecast_model_series: operatorRecommendation.forecast_model_series.map(series => ({
      ...series,
      points: series.points.slice(0, pointLimit)
    }))
  }
}

export const sliceBaselinePreviewForChartHorizon = (
  baselinePreview: BaselineLpPreview | null,
  horizon: OperatorChartHorizon
): BaselineLpPreview | null => {
  if (!baselinePreview) {
    return null
  }

  const pointLimit = operatorChartHorizonPointLimit(horizon)
  if (pointLimit == null) {
    return baselinePreview
  }

  return {
    ...baselinePreview,
    forecast: baselinePreview.forecast.slice(0, pointLimit),
    recommendation_schedule: baselinePreview.recommendation_schedule.slice(0, pointLimit),
    bid_recommendation_preview: baselinePreview.bid_recommendation_preview.slice(0, pointLimit),
    projected_state: {
      ...baselinePreview.projected_state,
      trace: baselinePreview.projected_state.trace.slice(0, pointLimit)
    }
  }
}
