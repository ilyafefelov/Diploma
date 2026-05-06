import type { FutureStackPreviewResponse } from '~/types/control-plane'

type ControlPlaneQuery = Record<string, string | number | boolean | null | undefined>

export default defineEventHandler(async (event): Promise<FutureStackPreviewResponse> => {
  const runtimeConfig = useRuntimeConfig()
  const apiBase = String(runtimeConfig.apiBase || 'http://127.0.0.1:8010')
  const query = getQuery(event) as ControlPlaneQuery
  const tenantId = String(query.tenant_id || 'unknown')

  try {
    return await fetchFutureStackPreview(apiBase, query, 5000)
  } catch {
    return buildStaticFutureStackFallback(tenantId)
  }
})

const fetchFutureStackPreview = async (
  apiBase: string,
  query: ControlPlaneQuery,
  timeoutMs: number
): Promise<FutureStackPreviewResponse> => {
  const controller = new AbortController()
  const timeout = setTimeout(() => {
    controller.abort()
  }, timeoutMs)

  try {
    return await ($fetch<FutureStackPreviewResponse>(`${apiBase}/dashboard/future-stack-preview`, {
      query,
      signal: controller.signal
    }) as Promise<FutureStackPreviewResponse>)
  } finally {
    clearTimeout(timeout)
  }
}

const buildStaticFutureStackFallback = (tenantId: string): FutureStackPreviewResponse => {
  const generatedAt = new Date()
  const timestamps = [0, 1, 2, 3, 4, 5].map((offset) => {
    const timestamp = new Date(generatedAt)
    timestamp.setHours(generatedAt.getHours() + offset, 0, 0, 0)
    return timestamp.toISOString()
  })
  const basePrices = [4100, 4350, 3900, 2300, 2600, 3800]

  return {
    tenant_id: tenantId,
    generated_at: generatedAt.toISOString(),
    forecast_window_start: timestamps[0] ?? null,
    forecast_window_end: timestamps[timestamps.length - 1] ?? null,
    backend_status: {
      source: 'demo_static_fallback',
      reason: 'control_plane_future_stack_unavailable'
    },
    runtime_acceleration: {
      backend: 'local',
      device_type: 'cpu',
      device_name: 'demo runtime',
      gpu_available: false,
      cuda_version: null,
      recommended_scope: 'small smoke configs; full SOTA training after demo'
    },
    selected_forecast_model: 'nbeatsx_official_v0',
    claim_boundary: 'Demo fallback only when Postgres-backed future stack is unavailable; use materialized rows for thesis claims.',
    forecast_series: [
      {
        model_name: 'nbeatsx_official_v0',
        model_family: 'NBEATSx',
        source_status: 'demo_fallback',
        uncertainty_kind: 'point_smoke',
        mean_regret_uah: 481,
        win_rate: 0.26,
        out_of_dam_cap_rows: 0,
        quality_boundary: 'official adapter target; benchmark row fallback',
        points: timestamps.map((timestamp, index) => ({
          step_index: index,
          interval_start: timestamp,
          forecast_price_uah_mwh: basePrices[index] ?? 0,
          actual_price_uah_mwh: null,
          p10_price_uah_mwh: null,
          p50_price_uah_mwh: basePrices[index] ?? 0,
          p90_price_uah_mwh: null,
          net_power_mw: index === 3 ? -0.22 : index === 5 ? 0.12 : 0,
          value_gap_uah: index === 3 ? 120 : 80,
          price_cap_status: 'inside_dam_cap'
        }))
      },
      {
        model_name: 'tft_official_v0',
        model_family: 'TFT',
        source_status: 'demo_fallback',
        uncertainty_kind: 'quantile_smoke',
        mean_regret_uah: 551,
        win_rate: 0.26,
        out_of_dam_cap_rows: 0,
        quality_boundary: 'official adapter target; benchmark row fallback',
        points: timestamps.map((timestamp, index) => {
          const price = (basePrices[index] ?? 0) * 1.03
          return {
            step_index: index,
            interval_start: timestamp,
            forecast_price_uah_mwh: Number(price.toFixed(2)),
            actual_price_uah_mwh: null,
            p10_price_uah_mwh: Number((price * 0.92).toFixed(2)),
            p50_price_uah_mwh: Number(price.toFixed(2)),
            p90_price_uah_mwh: Number((price * 1.1).toFixed(2)),
            net_power_mw: index === 3 ? -0.18 : index === 5 ? 0.1 : 0,
            value_gap_uah: index === 3 ? 150 : 95,
            price_cap_status: 'inside_dam_cap'
          }
        })
      }
    ]
  }
}
