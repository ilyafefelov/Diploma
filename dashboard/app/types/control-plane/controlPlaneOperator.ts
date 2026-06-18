import type {
  BaselinePreviewEconomics,
  BaselineRecommendationPoint,
  BidRecommendationPreviewPoint
} from './controlPlaneCore'

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

export interface OperatorV13ReadinessResponse {
  gate_status: string
  v13_candidate_generation_ready: boolean
  dt_lava_ready: boolean
  ready_rows: number
  readiness_rows: number
  missing_safe_switch_examples: number
  missing_required_inputs: string[]
  top_priority_blocker: string
  receipt_source_audit_probe_count: number
  receipt_source_audit_months_probed: string[]
  receipt_source_audit_candidate_found: boolean
  receipt_source_audit_csv_generated: boolean
  receipt_source_audit_all_probes_insufficient: boolean
  source_governance_status: string
  source_governance_label: string
  market_submission_receipt_gate_status: string
  scmo_credentials_required_for_diploma_mvp: boolean
  scmo_credentials_required_for_market_submission_grade_receipts: boolean
  safe_switch_target_tenant_source_count: number
  safe_switch_max_new_examples_required: number
  safe_switch_acquisition_targets: OperatorV13SafeSwitchTargetResponse[]
  market_execution_enabled: boolean
  boundary_doc: string
  source_packet_path: string | null
}

export interface OperatorV13SafeSwitchTargetResponse {
  acquisition_priority_rank: number
  tenant_id: string
  source_model_name: string
  current_prior_material_safe_switch_examples: number
  required_prior_material_safe_switch_examples: number
  target_new_prior_material_safe_switch_examples: number
  required_evidence_kind: string
  recommended_next_step: string
  target_is_precondition_only: boolean
  market_execution_enabled: boolean
}

export interface OperatorForecastScenarioCandidateResponse {
  candidate_id: string
  model_name: string
  schedule_family: string
  rank: number
  advisor_decision: string
  score_source: string
  decision_value_uah: number
  regret_to_best_uah: number
  total_throughput_mwh: number
  gatekeeper_status: string
  selected_for_operator_preview: boolean
  market_execution_enabled: boolean
  market_order_payload_emitted: boolean
}

export interface OperatorDecisionAdvisorResponse {
  advisor_source_id: string
  advisor_status: string
  candidate_decision: string
  selected_candidate_id: string | null
  selected_schedule_family: string | null
  reason: string
  evidence_layers: string[]
  comparison_metrics: Record<string, number>
  forecast_scenario_candidates?: OperatorForecastScenarioCandidateResponse[]
  market_execution_enabled: boolean
  market_order_payload_emitted: boolean
  promotion_gate_passed: boolean
  dt_lava_ready: boolean
}

export interface OperatorRecommendationResponse {
  tenant_id: string
  market_scope: string
  market_venue: string
  interval_minutes: number
  target_delivery_date?: string | null
  price_context_status?: string
  anchor_timestamp: string
  forecast_generated_at: string | null
  target_delivery_window_start: string | null
  target_delivery_window_end: string | null
  market_execution_enabled: boolean
  read_model_boundary: string
  market_gate_status: string
  bid_eligibility_status: string
  proposed_bid_status: string
  v13_readiness: OperatorV13ReadinessResponse
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
  policy_forecast_context_source: string
  policy_forecast_context_row_count: number
  policy_forecast_context_coverage_ratio: number
  policy_forecast_context_warning: string | null
  available_strategies: OperatorStrategyOptionResponse[]
  forecast_model_series: FutureForecastSeriesResponse[]
  value_gap_series: OperatorValueGapPointResponse[]
  decision_advisor?: OperatorDecisionAdvisorResponse
  load_forecast: OperatorLoadForecastPointResponse[]
  soc_projection: OperatorSocProjectionPointResponse[]
  recommendation_schedule: BaselineRecommendationPoint[]
  bid_recommendation_preview: BidRecommendationPreviewPoint[]
  daily_value_uah: number
  hold_baseline_value_uah: number
  value_vs_hold_uah: number
  economics: BaselinePreviewEconomics
}

export interface OperatorPreviewEnsureResponse {
  tenant_id: string
  market_venue: string
  target_delivery_date: string
  status: 'ready' | 'materialized' | 'blocked_source_unavailable' | 'blocked_outside_policy_horizon' | 'failed' | string
  stage: string
  message: string
  latest_observed_timestamp: string | null
  forecast_start: string | null
  forecast_horizon_end: string | null
  horizon_hours: number | null
  source_refresh_rows: number
  source_refresh_dates: string[]
  forecast_rows: number
  forecast_run_ids: Record<string, string>
  claim_boundary: string
  read_model_boundary: string
  market_execution_enabled: boolean
}

export interface ShadowPreviewSourceOptionResponse {
  preview_source_id: string
  label: string
  status: string
  is_default_strategy: boolean
  is_promoted_strategy: boolean
  market_execution_enabled: boolean
  reason: string
}

export interface ShadowRecommendationSchedulePointResponse {
  step_index: number
  interval_start: string
  action: 'charge' | 'discharge' | 'hold' | string
  quantity_mw: number
  recommended_net_power_mw: number
  forecast_price_uah_mwh: number
  soc_before_fraction: number | null
  soc_after_fraction: number | null
  selected_candidate_id: string
  schedule_family: string
  expected_value_uah: number
  regret_uah: number | null
  regret_vs_v2_plus_uah: number | null
  regret_vs_strict_uah: number | null
  value_vs_v2_plus_uah: number | null
  value_vs_strict_uah: number | null
  gate_status: string
  safety_status: string
  market_execution_enabled: boolean
  market_order_payload_emitted: boolean
  proposed_bid_status: string
}

export interface ShadowRecommendationPreviewResponse {
  tenant_id: string
  preview_source_id: string
  preview_source_label: string
  preview_status: string
  preview_only: boolean
  is_default_strategy: boolean
  is_promoted_strategy: boolean
  research_shadow_not_promotable: boolean
  default_strategy_id: string
  default_strategy_label: string
  selected_candidate_id: string | null
  selected_schedule_family: string | null
  selected_candidate_index: number | null
  market_scope: string
  market_venue: string
  interval_minutes: number
  anchor_timestamp: string | null
  target_delivery_window_start: string | null
  target_delivery_window_end: string | null
  market_execution_enabled: boolean
  proposed_bid_status: string
  market_order_payload_emitted: boolean
  promotion_gate_passed: boolean
  dt_lava_ready: boolean
  source_readiness_gate_passed: boolean
  comparison_metrics: Record<string, number>
  available_preview_sources: ShadowPreviewSourceOptionResponse[]
  recommendation_schedule: ShadowRecommendationSchedulePointResponse[]
  boundary_labels: string[]
  readiness_warnings: string[]
  artifact_paths: Record<string, string>
}

export interface AcademicMvpReadinessResponse {
  claim_scope: string
  generated_at: string | null
  academic_mvp_gate_passed: boolean
  operator_preview_gate: Record<string, unknown>
  source_governance: Record<string, unknown>
  dt_lava_prototype_gate: Record<string, unknown>
  dt_lava_teacher_contract_gate: Record<string, unknown>
  offline_challenger_gate: Record<string, unknown>
  dt_research_shadow_gate: Record<string, unknown>
  prototype_contract: Record<string, unknown>
  prototype_evidence_scorecard: Record<string, unknown>
  prototype_phase_readiness: Record<string, unknown>
  gate_passport: Record<string, unknown>
  market_submission_ready: boolean
  market_execution_gate_passed: boolean
  promotion_gate_passed: boolean
  permits_model_training: boolean
  market_execution_enabled: boolean
  no_market_execution_safety_gate_passed: boolean
  next_gate: string
  artifact_validation: Record<string, unknown>
  source_packet_path: string
  artifact_validation_packet_path: string
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

export interface GatekeeperValidationStatusResponse {
  tenant_id: string
  status: 'blocked' | 'no_validation_failures_recorded'
  validation_stage: string | null
  contract_type: string | null
  canonical_outcome: 'NO_BID' | 'HOLD' | null
  venue: string | null
  interval_start: string | null
  duration_minutes: number | null
  failure_reason: string | null
  created_at: string | null
  no_bid_semantics: string
  hold_semantics: string
  latest_failure_id: string | null
}
