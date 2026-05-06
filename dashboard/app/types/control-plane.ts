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
  projected_state: ProjectedBatteryStatePreview
  economics: BaselinePreviewEconomics
}

export interface ForecastStrategyComparisonPointResponse {
  forecast_model_name: string
  strategy_kind: string
  decision_value_uah: number
  forecast_objective_value_uah: number
  oracle_value_uah: number
  regret_uah: number
  regret_ratio: number
  total_degradation_penalty_uah: number
  total_throughput_mwh: number
  committed_action: string
  committed_power_mw: number
  rank_by_regret: number
  evaluation_payload: Record<string, unknown>
}

export interface ForecastStrategyComparisonResponse {
  tenant_id: string
  market_venue: string
  evaluation_id: string
  anchor_timestamp: string
  generated_at: string
  horizon_hours: number
  starting_soc_fraction: number
  starting_soc_source: string
  comparisons: ForecastStrategyComparisonPointResponse[]
}

export interface RealDataBenchmarkPointResponse {
  evaluation_id: string
  anchor_timestamp: string
  forecast_model_name: string
  decision_value_uah: number
  oracle_value_uah: number
  regret_uah: number
  regret_ratio: number
  total_degradation_penalty_uah: number
  total_throughput_mwh: number
  committed_action: string
  committed_power_mw: number
  rank_by_regret: number
  evaluation_payload: Record<string, unknown>
}

export interface RealDataBenchmarkResponse {
  tenant_id: string
  market_venue: string
  generated_at: string
  data_quality_tier: string
  anchor_count: number
  model_count: number
  best_model_name: string | null
  mean_regret_uah: number
  median_regret_uah: number
  rows: RealDataBenchmarkPointResponse[]
}

export interface BatteryTelemetryObservationResponse {
  tenant_id: string
  observed_at: string
  current_soc: number
  soh: number
  power_mw: number
  temperature_c: number | null
  source: string
  source_kind: string
}

export interface BatteryStateHourlySnapshotResponse {
  tenant_id: string
  snapshot_hour: string
  observation_count: number
  soc_open: number
  soc_close: number
  soc_mean: number
  soh_close: number
  power_mw_mean: number
  throughput_mwh: number
  efc_delta: number
  telemetry_freshness: string
  first_observed_at: string
  last_observed_at: string
}

export interface DashboardBatteryStateResponse {
  tenant_id: string
  latest_telemetry: BatteryTelemetryObservationResponse | null
  hourly_snapshot: BatteryStateHourlySnapshotResponse | null
  fallback_reason: string | null
}

export interface ExogenousWeatherSignalResponse {
  timestamp: string
  fetched_at: string
  source: string
  source_kind: string
  source_url: string
  temperature: number
  cloudcover: number
  wind_speed: number
  precipitation: number
  freshness_hours: number | null
}

export interface ExogenousGridEventResponse {
  post_id: string
  post_url: string
  published_at: string
  fetched_at: string
  raw_text_summary: string
  source: string
  source_kind: string
  source_url: string
  energy_system_status: boolean
  shelling_damage: boolean
  outage_or_restriction: boolean
  consumption_change: string
  solar_shift_advice: boolean
  evening_saving_request: boolean
  affected_oblasts: string[]
  freshness_hours: number | null
}

export interface DashboardExogenousSignalsResponse {
  tenant_id: string
  resolved_location: {
    latitude: number
    longitude: number
    timezone: string
  }
  latest_weather: ExogenousWeatherSignalResponse | null
  latest_grid_event: ExogenousGridEventResponse | null
  grid_event_count_24h: number
  tenant_region_affected: boolean
  national_grid_risk_score: number
  outage_flag: boolean
  saving_request_flag: boolean
  solar_shift_hint: boolean
  event_source_freshness_hours: number | null
  source_urls: string[]
  fallback_reason: string | null
}

export interface ForecastDispatchSensitivityPointResponse {
  diagnostic_id: string
  evaluation_id: string
  anchor_timestamp: string
  forecast_model_name: string
  diagnostic_bucket: string
  regret_uah: number
  regret_ratio: number
  forecast_mae_uah_mwh: number
  forecast_rmse_uah_mwh: number
  mean_forecast_error_uah_mwh: number
  forecast_dispatch_spread_uah_mwh: number
  realized_dispatch_spread_uah_mwh: number
  dispatch_spread_error_uah_mwh: number
  total_degradation_penalty_uah: number
  total_throughput_mwh: number
  charge_energy_mwh: number
  discharge_energy_mwh: number
  committed_action: string
  committed_power_mw: number
  rank_by_regret: number
  data_quality_tier: string
}

export interface ForecastDispatchSensitivityBucketResponse {
  diagnostic_bucket: string
  rows: number
  mean_regret_uah: number
  mean_forecast_mae_uah_mwh: number
  mean_dispatch_spread_error_uah_mwh: number
}

export interface ForecastDispatchSensitivityResponse {
  tenant_id: string
  market_venue: string
  generated_at: string
  source_strategy_kind: string
  anchor_count: number
  model_count: number
  row_count: number
  bucket_summary: ForecastDispatchSensitivityBucketResponse[]
  rows: ForecastDispatchSensitivityPointResponse[]
}

export interface DflRelaxedPilotPointResponse {
  pilot_name: string
  evaluation_id: string
  anchor_timestamp: string
  forecast_model_name: string
  horizon_hours: number
  relaxed_realized_value_uah: number
  relaxed_oracle_value_uah: number
  relaxed_regret_uah: number
  first_charge_mw: number
  first_discharge_mw: number
  academic_scope: string
}

export interface DflRelaxedPilotResponse {
  tenant_id: string
  row_count: number
  mean_relaxed_regret_uah: number
  academic_scope: string
  rows: DflRelaxedPilotPointResponse[]
}

export interface DecisionTransformerTrajectoryPointResponse {
  episode_id: string
  market_venue: string
  scenario_index: number
  step_index: number
  interval_start: string
  state_soc_before: number
  state_soc_after: number
  state_soh: number
  state_market_price_uah_mwh: number
  action_charge_mw: number
  action_discharge_mw: number
  reward_uah: number
  return_to_go_uah: number
  degradation_penalty_uah: number
  baseline_value_uah: number
  oracle_value_uah: number
  regret_uah: number
  academic_scope: string
}

export interface DecisionTransformerTrajectoryResponse {
  tenant_id: string
  row_count: number
  episode_count: number
  academic_scope: string
  rows: DecisionTransformerTrajectoryPointResponse[]
}

export interface DecisionPolicyPreviewPointResponse {
  policy_run_id: string
  created_at: string
  episode_id: string
  market_venue: string
  scenario_index: number
  step_index: number
  interval_start: string
  state_market_price_uah_mwh: number
  projected_soc_before: number
  projected_soc_after: number
  raw_charge_mw: number
  raw_discharge_mw: number
  projected_charge_mw: number
  projected_discharge_mw: number
  projected_net_power_mw: number
  expected_policy_value_uah: number
  hold_value_uah: number
  value_vs_hold_uah: number
  oracle_value_uah: number
  value_gap_uah: number
  constraint_violation: boolean
  gatekeeper_status: string
  inference_latency_ms: number
  policy_mode: string
  readiness_status: string
  model_name: string
  academic_scope: string
}

export interface DecisionPolicyPreviewResponse {
  tenant_id: string
  row_count: number
  policy_run_id: string
  created_at: string
  policy_readiness: string
  live_policy_claim: boolean
  market_execution_enabled: boolean
  constraint_violation_count: number
  mean_value_gap_uah: number
  total_value_vs_hold_uah: number
  policy_state_features: string[]
  policy_value_interpretation: string
  operator_boundary: string
  academic_scope: string
  rows: DecisionPolicyPreviewPointResponse[]
}

export interface SimulatedLiveTradingPointResponse {
  episode_id: string
  interval_start: string
  step_index: number
  state_soc_before: number
  state_soc_after: number
  proposed_trade_side: string
  proposed_quantity_mw: number
  feasible_net_power_mw: number
  market_price_uah_mwh: number
  reward_uah: number
  gatekeeper_status: string
  paper_trade_provenance: string
  settlement_id: string | null
  live_mode_warning: string
}

export interface SimulatedLiveTradingResponse {
  tenant_id: string
  row_count: number
  simulated_only: boolean
  rows: SimulatedLiveTradingPointResponse[]
}

export interface OperatorStrategyOptionResponse {
  strategy_id: string
  label: string
  enabled: boolean
  reason: string
  mean_regret_uah: number | null
  win_rate: number | null
}

export interface OperatorLoadForecastPointResponse {
  timestamp: string
  load_mw: number
  pv_estimate_mw: number
  net_load_mw: number
  btm_battery_power_mw: number
  source_kind: string
  weather_source_kind: string
  reason_code: string
}

export interface OperatorSocProjectionPointResponse {
  timestamp: string
  physical_soc: number | null
  estimated_soc: number
  planning_soc: number
  soc_source: string
  confidence: string
}

export interface FutureForecastPointResponse {
  step_index: number
  interval_start: string
  forecast_price_uah_mwh: number
  actual_price_uah_mwh: number | null
  p10_price_uah_mwh: number | null
  p50_price_uah_mwh: number | null
  p90_price_uah_mwh: number | null
  net_power_mw: number | null
  value_gap_uah: number | null
  price_cap_status: string
}

export interface FutureForecastSeriesResponse {
  model_name: string
  model_family: string
  source_status: string
  uncertainty_kind: string
  mean_regret_uah: number | null
  win_rate: number | null
  out_of_dam_cap_rows: number
  quality_boundary: string
  points: FutureForecastPointResponse[]
}

export interface RuntimeAccelerationResponse {
  backend: string
  device_type: string
  device_name: string
  gpu_available: boolean
  cuda_version: string | null
  recommended_scope: string
}

export interface FutureStackPreviewResponse {
  tenant_id: string
  generated_at: string | null
  forecast_window_start: string | null
  forecast_window_end: string | null
  backend_status: Record<string, string>
  runtime_acceleration: RuntimeAccelerationResponse
  selected_forecast_model: string | null
  claim_boundary: string
  forecast_series: FutureForecastSeriesResponse[]
}

export interface OperatorValueGapPointResponse {
  step_index: number
  interval_start: string
  chosen_value_uah: number
  best_visible_value_uah: number
  value_gap_uah: number
  metric_source: string
}

export interface OperatorRecommendationResponse {
  tenant_id: string
  selected_strategy_id: string
  selection_reason: string
  forecast_source: string
  soc_source: string
  review_required: boolean
  readiness_warnings: string[]
  policy_mode: string
  selected_policy_id: string
  policy_explanation: string
  policy_readiness: string
  available_strategies: OperatorStrategyOptionResponse[]
  forecast_model_series: FutureForecastSeriesResponse[]
  value_gap_series: OperatorValueGapPointResponse[]
  load_forecast: OperatorLoadForecastPointResponse[]
  soc_projection: OperatorSocProjectionPointResponse[]
  recommendation_schedule: BaselineRecommendationPoint[]
  daily_value_uah: number
  hold_baseline_value_uah: number
  value_vs_hold_uah: number
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
