import type { BaselineForecastPoint, BaselineLpPreview, SignalPreview } from '~/types/control-plane'

type ControlPlaneQuery = Record<string, string | number | boolean | null | undefined>

export default defineEventHandler(async (event): Promise<SignalPreview> => {
  const runtimeConfig = useRuntimeConfig()
  const apiBase = String(runtimeConfig.apiBase || 'http://127.0.0.1:8010')
  const query = getQuery(event) as ControlPlaneQuery
  const tenantId = String(query.tenant_id || 'unknown')

  try {
    return await fetchSignalPreview(apiBase, query, 5000)
  } catch {
    return buildBaselineSignalFallback(apiBase, query, tenantId)
  }
})

const fetchSignalPreview = async (
  apiBase: string,
  query: ControlPlaneQuery,
  timeoutMs: number
): Promise<SignalPreview> => {
  const controller = new AbortController()
  const timeout = setTimeout(() => {
    controller.abort()
  }, timeoutMs)

  try {
    return await ($fetch<SignalPreview>(`${apiBase}/dashboard/signal-preview`, {
      query,
      signal: controller.signal
    }) as Promise<SignalPreview>)
  } finally {
    clearTimeout(timeout)
  }
}

const fetchBaselinePreview = async (
  apiBase: string,
  query: ControlPlaneQuery,
  timeoutMs: number
): Promise<BaselineLpPreview> => {
  const controller = new AbortController()
  const timeout = setTimeout(() => {
    controller.abort()
  }, timeoutMs)

  try {
    return await ($fetch<BaselineLpPreview>(`${apiBase}/dashboard/baseline-lp-preview`, {
      query,
      signal: controller.signal
    }) as Promise<BaselineLpPreview>)
  } finally {
    clearTimeout(timeout)
  }
}

const buildStaticSignalFallback = (tenantId: string): SignalPreview => {
  const now = new Date()
  const points = [0, 3, 6, 9, 12, 15].map((offset) => {
    const point = new Date(now)
    point.setHours(now.getHours() + offset, 0, 0, 0)
    return point.toISOString()
  })

  return {
    tenant_id: tenantId,
    labels: points.map(formatHourLabel),
    label_timestamps: points,
    latest_price_timestamp: points[0] ?? null,
    forecast_window_start: points[0] ?? null,
    forecast_window_end: points[points.length - 1] ?? null,
    timezone: 'Europe/Kyiv',
    market_price: [4100, 4300, 3900, 2300, 2500, 3800],
    weather_bias: [0, 35, 20, -10, 15, 25],
    weather_sources: points.map(() => 'DEMO_STATIC_FALLBACK'),
    charge_intent: [0.15, 0.08, 0, -0.22, -0.14, 0.12],
    regret: [460, 510, 420, 280, 310, 390],
    resolved_location: {
      latitude: 48.46,
      longitude: 35.04,
      timezone: 'Europe/Kyiv'
    }
  }
}

const buildBaselineSignalFallback = async (
  apiBase: string,
  query: ControlPlaneQuery,
  tenantId: string
): Promise<SignalPreview> => {
  let baseline: BaselineLpPreview
  try {
    baseline = await fetchBaselinePreview(apiBase, query, 4000)
  } catch {
    return buildStaticSignalFallback(tenantId)
  }
  const points = baseline.forecast
    .filter((_point: BaselineForecastPoint, index: number) => index % 3 === 0)
    .slice(0, 6)
  const lastPoint = points[points.length - 1]
  const marketPrice = points.map((point: BaselineForecastPoint) => Number(point.predicted_price_uah_mwh.toFixed(2)))
  const averagePrice = marketPrice.reduce((total, value) => total + value, 0) / Math.max(1, marketPrice.length)
  const maxDeviation = Math.max(1, ...marketPrice.map((value) => Math.abs(value - averagePrice)))
  const maxPowerMw = baseline.battery_metrics.max_power_mw

  return {
    tenant_id: tenantId,
    labels: points.map((point) => formatHourLabel(point.forecast_timestamp)),
    label_timestamps: points.map((point) => point.forecast_timestamp),
    latest_price_timestamp: lastPoint?.forecast_timestamp ?? null,
    forecast_window_start: points[0]?.forecast_timestamp ?? null,
    forecast_window_end: lastPoint?.forecast_timestamp ?? null,
    timezone: baseline.resolved_location.timezone,
    market_price: marketPrice,
    weather_bias: points.map(() => 0),
    weather_sources: points.map(() => 'BASELINE_FALLBACK'),
    charge_intent: marketPrice.map((value) =>
      Number(Math.max(-maxPowerMw, Math.min(maxPowerMw, ((value - averagePrice) / maxDeviation) * maxPowerMw)).toFixed(2))
    ),
    regret: marketPrice.map((value) => Number(Math.max(80, Math.abs(value - averagePrice) * 0.45).toFixed(2))),
    resolved_location: baseline.resolved_location
  }
}

const formatHourLabel = (timestamp: string): string => {
  const parsed = new Date(timestamp)
  if (Number.isNaN(parsed.getTime())) {
    return timestamp
  }

  return parsed.toLocaleTimeString('en-GB', {
    hour: '2-digit',
    minute: '2-digit',
    timeZone: 'Europe/Kyiv'
  })
}
