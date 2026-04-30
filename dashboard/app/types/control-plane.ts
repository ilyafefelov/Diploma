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
  market_price: number[]
  weather_bias: number[]
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
  interval_minutes: number
  starting_soc_fraction: number
  battery_metrics: ProjectedBatteryStatePreview['battery_metrics']
  resolved_location: {
    latitude: number
    longitude: number
    timezone: string
  }
  forecast: BaselineForecastPoint[]
  recommendation_schedule: BaselineRecommendationPoint[]
  projected_state: ProjectedBatteryStatePreview
  economics: BaselinePreviewEconomics
}

export type OperatorFlowType = 'weather_control' | 'signal_preview' | 'baseline_lp' | 'gatekeeper' | 'dispatch_execution'

export type OperatorFlowStatus = 'idle' | 'prepared' | 'running' | 'completed' | 'failed'

export interface OperatorStatus {
  tenant_id: string
  flow_type: OperatorFlowType
  status: OperatorFlowStatus
  updated_at: string
  payload: Record<string, unknown> | null
  last_error: string | null
}