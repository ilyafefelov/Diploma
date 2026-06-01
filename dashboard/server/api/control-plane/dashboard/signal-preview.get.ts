import type { SignalPreview } from '~/types/control-plane'
import { fetchControlPlane } from '../../../utils/controlPlaneProxy'

type ControlPlaneQuery = Record<string, string | number | boolean | null | undefined>

export default defineEventHandler(async (event): Promise<SignalPreview> => {
  const query = getQuery(event) as ControlPlaneQuery
  const tenantId = String(query.tenant_id || 'unknown')

  try {
    return await fetchSignalPreview(query, 5000)
  } catch {
    return buildUnavailableSignalPreview(tenantId)
  }
})

const fetchSignalPreview = async (
  query: ControlPlaneQuery,
  timeoutMs: number
): Promise<SignalPreview> => {
  return await fetchControlPlane<SignalPreview>('/dashboard/signal-preview', { query, timeoutMs })
}

const buildUnavailableSignalPreview = (tenantId: string): SignalPreview => {
  return {
    tenant_id: tenantId,
    labels: [],
    label_timestamps: [],
    latest_price_timestamp: null,
    forecast_window_start: null,
    forecast_window_end: null,
    timezone: 'Europe/Kyiv',
    market_price: [],
    weather_bias: [],
    weather_sources: [],
    charge_intent: [],
    regret: [],
    resolved_location: {
      latitude: 48.46,
      longitude: 35.04,
      timezone: 'Europe/Kyiv'
    }
  }
}
