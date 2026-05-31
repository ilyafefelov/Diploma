export interface TenantSummary {
  tenant_id: string
  name: string | null
  type: string | null
  latitude: number
  longitude: number
  timezone: string
}

export interface SignalPreview {
  tenant_id: string
  labels: string[]
  label_timestamps?: string[]
  latest_price_timestamp?: string | null
  forecast_window_start?: string | null
  forecast_window_end?: string | null
  timezone?: string | null
  market_price: number[]
  weather_bias: number[]
  weather_sources: string[]
  charge_intent: number[]
  regret: number[]
  resolved_location: {
    latitude: number
    longitude: number
    timezone: string
  }
}

export interface BaselineForecastPoint {
  forecast_timestamp: string
  source_timestamp: string
  predicted_price_uah_mwh: number
}

export interface BaselineRecommendationPoint {
  step_index: number
  interval_start: string
  forecast_price_uah_mwh: number
  recommended_net_power_mw: number
  projected_soc_before_fraction: number
  projected_soc_after_fraction: number
  throughput_mwh: number
  degradation_penalty_uah: number
  gross_market_value_uah: number
  net_value_uah: number
}

export interface BidRecommendationPreviewPoint {
  step_index: number
  interval_start: string
  market_venue: string
  side: 'BUY' | 'SELL' | 'HOLD'
  operator_action: 'charge' | 'discharge' | 'hold'
  quantity_mw: number
  indicative_limit_price_uah_mwh: number
  preview_only: boolean
  market_execution_enabled: boolean
  market_order_payload_emitted: boolean
  proposed_bid_status: string
  read_model_boundary: string
}

export interface ProjectedBatteryTracePoint {
  step_index: number
  interval_start: string
  requested_net_power_mw: number
  feasible_net_power_mw: number
  soc_before_fraction: number
  soc_after_fraction: number
  throughput_mwh: number
  degradation_penalty_uah: number
}

export interface ProjectedBatteryStatePreview {
  tenant_id: string
  interval_minutes: number
  starting_soc_fraction: number
  battery_metrics: {
    capacity_mwh: number
    max_power_mw: number
    round_trip_efficiency: number
    degradation_cost_per_cycle_uah: number
    soc_min_fraction: number
    soc_max_fraction: number
  }
  total_throughput_mwh: number
  total_degradation_penalty_uah: number
  trace: ProjectedBatteryTracePoint[]
}

export interface BaselinePreviewEconomics {
  total_gross_market_value_uah: number
  total_degradation_penalty_uah: number
  total_net_value_uah: number
  total_throughput_mwh: number
}

export interface BaselineLpPreview {
  tenant_id: string
  market_venue: string
  market_scope: string
  interval_minutes: number
  target_delivery_date?: string | null
  anchor_timestamp: string
  forecast_generated_at: string | null
  target_delivery_window_start: string | null
  target_delivery_window_end: string | null
  market_execution_enabled: boolean
  read_model_boundary: string
  market_gate_status: string
  bid_eligibility_status: string
  proposed_bid_status: string
  starting_soc_fraction: number
  starting_soc_source?: string
  telemetry_freshness?: Record<string, unknown> | null
  battery_metrics: ProjectedBatteryStatePreview['battery_metrics']
  resolved_location: {
    latitude: number
    longitude: number
    timezone: string
  }
  forecast: BaselineForecastPoint[]
  recommendation_schedule: BaselineRecommendationPoint[]
  bid_recommendation_preview: BidRecommendationPreviewPoint[]
  projected_state: ProjectedBatteryStatePreview
  economics: BaselinePreviewEconomics
}
