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

export interface TelemetryIngestSourceResponse {
  protocol: string
  broker_host: string
  broker_port: number
  topic: string
  source_kind: string
}

export interface DashboardBatteryStateResponse {
  tenant_id: string
  latest_telemetry: BatteryTelemetryObservationResponse | null
  hourly_snapshot: BatteryStateHourlySnapshotResponse | null
  fallback_reason: string | null
  telemetry_ingest_source?: TelemetryIngestSourceResponse | null
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

export interface DflScheduleValueProductionGatePointResponse {
  source_model_name: string
  tenant_count: number
  latest_validation_tenant_anchor_count: number
  latest_strict_mean_regret_uah: number
  latest_selected_mean_regret_uah: number
  latest_strict_median_regret_uah: number
  latest_selected_median_regret_uah: number
  latest_mean_regret_improvement_ratio_vs_strict: number
  rolling_window_count: number
  rolling_strict_pass_window_count: number
  robust_research_challenger: boolean
  production_promote: boolean
  promotion_blocker: string
  allowed_challenger: string
  fallback_strategy: string
  market_execution_enabled: boolean
  not_full_dfl: boolean
  not_market_execution: boolean
}

export interface DflScheduleValueProductionGateResponse {
  generated_at: string
  row_count: number
  production_promote_count: number
  promoted_source_model_names: string[]
  fallback_strategy: string
  market_execution_enabled: boolean
  claim_scope: string
  claim_boundary: string
  academic_scope: string
  rows: DflScheduleValueProductionGatePointResponse[]
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
  state_nbeatsx_forecast_uah_mwh: number | null
  state_tft_forecast_uah_mwh: number | null
  state_forecast_uncertainty_uah_mwh: number | null
  state_forecast_spread_uah_mwh: number | null
  projected_soc_before: number
  projected_soc_after: number
  raw_charge_mw: number
  raw_discharge_mw: number
  projected_charge_mw: number
  projected_discharge_mw: number
  projected_net_power_mw: number
  projected_action_label: string
  projection_status: string
  projection_adjustment_mw: number
  expected_policy_value_uah: number
  hold_value_uah: number
  value_vs_hold_uah: number
  oracle_value_uah: number
  value_gap_uah: number
  value_gap_ratio: number | null
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
  forecast_context_source: string
  forecast_context_row_count: number
  forecast_context_coverage_ratio: number
  forecast_context_warning: string | null
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
