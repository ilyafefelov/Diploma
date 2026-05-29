from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import cache
import json
import math
import os
from pathlib import Path
from typing import Any

import dagster as dg
from fastapi import FastAPI, HTTPException
import polars as pl
from pydantic import BaseModel

from smart_arbitrage.assets.bronze.market_weather import (
	WeatherLocation,
	build_synthetic_market_price_history,
	build_weather_forecast_window,
	build_weather_asset_run_config,
	enrich_market_price_history_with_weather,
	list_available_weather_tenants,
	resolve_tenant_registry_entry,
	resolve_weather_location_for_tenant,
)
from smart_arbitrage.assets.gold.baseline_solver import (
	DEFAULT_PRICE_COLUMN,
	DEFAULT_TIMESTAMP_COLUMN,
	LEVEL1_INTERVAL_MINUTES,
	LEVEL1_MARKET_VENUE,
	BaselineForecastPoint,
	BaselineSolveResult,
	HourlyDamBaselineSolver,
)
from smart_arbitrage.assets.mvp_demo import (
	DEMO_BATTERY_CAPEX_USD_PER_KWH,
	DEMO_BATTERY_CYCLES_PER_DAY,
	DEMO_BATTERY_LIFETIME_YEARS,
	DEMO_USD_TO_UAH_RATE,
)
from smart_arbitrage.dfl.regret_weighted import HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND
from smart_arbitrage.dfl.offline_strategy_promotion import (
	offline_strategy_promotion_academic_scope,
	summarize_offline_strategy_promotion,
)
from smart_arbitrage.dfl.schedule_value_promotion_gate import (
	DFL_SCHEDULE_VALUE_PRODUCTION_GATE_CLAIM_SCOPE,
	STRICT_DEFAULT_FALLBACK,
)
from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics
from smart_arbitrage.gatekeeper.bid_observability import (
	HOLD_SEMANTICS,
	NO_BID_SEMANTICS,
)
from smart_arbitrage.forecasting.grid_event_signals import (
	build_grid_event_signal_frame,
	is_operational_grid_event_row,
)
from smart_arbitrage.optimization.projected_battery_state import (
	ProjectedBatterySimulationResult,
	ScheduledPowerPoint,
	simulate_projected_battery_state,
)
from smart_arbitrage.resources.operator_status_store import (
	OperatorFlowStatus,
	OperatorFlowType,
	OperatorStatusRecord,
	get_operator_status_store,
	utc_now,
)
from smart_arbitrage.resources.validation_failure_store import (
	get_validation_failure_store,
)
from smart_arbitrage.resources.battery_telemetry_store import (
	BatteryStateHourlySnapshot,
	BatteryTelemetryObservation,
	get_battery_telemetry_store,
)
from smart_arbitrage.resources.dfl_training_store import get_dfl_training_store
from smart_arbitrage.resources.forecast_store import get_forecast_store
from smart_arbitrage.resources.grid_event_store import get_grid_event_store
from smart_arbitrage.resources.market_data_store import get_market_data_store
from smart_arbitrage.resources.simulated_trade_store import get_simulated_trade_store
from smart_arbitrage.resources.strategy_evaluation_store import get_strategy_evaluation_store
from smart_arbitrage.strategy.ensemble_gate import (
	CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND,
	RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND,
)
from smart_arbitrage.strategy.dispatch_sensitivity import build_forecast_dispatch_sensitivity_frame
from smart_arbitrage.tenant_load import (
	build_tenant_consumption_schedule_frame,
	build_tenant_net_load_hourly_frame,
)
from smart_arbitrage.telemetry.mqtt import battery_telemetry_topic

OFFLINE_V2_PLUS_OPERATOR_STRATEGY_ID = "schedule_value_learner_v2_plus"
OFFLINE_V2_PLUS_MEAN_REGRET_UAH = 174.77
OFFLINE_V2_PLUS_WIN_RATE = 1.0
OFFLINE_V2_PLUS_LABEL = "Offline V2+ schedule/value learner"
OFFLINE_V2_PLUS_PREVIEW_SPREAD_SCALE = 1.1
OFFLINE_V2_PLUS_PREVIEW_RANK_DELTA_UAH_MWH = 120.0
OFFLINE_V2_PLUS_PREVIEW_EXTREMA_COUNT = 3


app = FastAPI(
	title="Smart Energy Arbitrage API",
	version="0.1.0",
	description=(
		"Control-plane API for tenant selection, weather-aware Dagster run config generation, "
		"and MVP weather/market materialization flows."
	),
	openapi_tags=[
		{"name": "system", "description": "Service health and API availability checks."},
		{"name": "tenants", "description": "Tenant registry lookup for location-aware experiments."},
		{"name": "weather", "description": "Weather asset configuration and materialization endpoints."},
	],
)

WEATHER_BIAS_FEATURE_NAMES: tuple[str, ...] = (
	"cloudcover",
	"precipitation",
	"humidity_excess",
	"temperature_gap",
	"effective_solar",
	"wind_speed",
)
MIN_WEATHER_BIAS_TARGET_PEAK_UAH_MWH = 1.0
MIN_WEATHER_BIAS_TARGET_SPREAD_UAH_MWH = 1.0
MIN_WEATHER_BIAS_PREDICTION_SPREAD_UAH_MWH = 0.01
FUTURE_STACK_FORECAST_MODEL_NAMES: tuple[str, ...] = (
	"nbeatsx_official_v0",
	"tft_official_v0",
	"nbeatsx_silver_v0",
	"tft_silver_v0",
)
OFFICIAL_FORECAST_TO_LP_STRATEGY_IDS: tuple[str, ...] = (
	"nbeatsx_official_v0",
	"tft_official_v0",
)
FUTURE_STACK_DAM_PRICE_CAP_MIN_UAH_MWH = 10.0
FUTURE_STACK_DAM_PRICE_CAP_MAX_UAH_MWH = 15_000.0
OPERATOR_MARKET_SCOPE = "dam_hourly_planning_preview"
OPERATOR_READ_MODEL_BOUNDARY = "operator_preview_no_market_submission"
OPERATOR_MARKET_GATE_STATUS = "not_evaluated_preview_only"
OPERATOR_BID_ELIGIBILITY_STATUS = "not_applicable_no_proposed_bid"
OPERATOR_PROPOSED_BID_STATUS = "not_emitted_operator_preview"
V13_ACQUISITION_PACKET_JSON_ENV = "SMART_ARBITRAGE_V13_ACQUISITION_PACKET_JSON"
V13_ACQUISITION_PACKET_JSON_DEFAULT = (
	Path("data")
	/ "research_runs"
	/ "week3_dfl_ua_context_acquisition_v13"
	/ "dfl_ua_context_v13_acquisition_summary.json"
)
ACADEMIC_MVP_PACKET_JSON_ENV = "SMART_ARBITRAGE_ACADEMIC_MVP_PACKET_JSON"
ACADEMIC_MVP_PACKET_JSON_DEFAULT = (
	Path("data")
	/ "research_runs"
	/ "week3_credentialless_academic_mvp_current"
	/ "credentialless_academic_mvp_readiness_summary.json"
)
ACADEMIC_MVP_VALIDATION_JSON_ENV = "SMART_ARBITRAGE_ACADEMIC_MVP_VALIDATION_JSON"
ACADEMIC_MVP_VALIDATION_JSON_NAME = "credentialless_academic_mvp_readiness_validation.json"
ACADEMIC_MVP_VALIDATION_CLAIM_SCOPE = (
	"credentialless_academic_mvp_readiness_validation_not_market_execution"
)
DT_RESEARCH_SHADOW_SELECTED_PREVIEW_JSON_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_dt_research_shadow_current"
	/ "dt_research_shadow_selected_schedule_preview.json"
)
DT_RESEARCH_SHADOW_TEACHER_ROWS_CSV_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_v13_dt_lava_teacher_dataset_safe_switch_only"
	/ "dfl_v13_dt_lava_teacher_rows.csv"
)
DT_DIRECT_CANDIDATE_SHADOW_SELECTED_PREVIEW_JSON_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_dt_direct_candidate_shadow_current"
	/ "dt_research_shadow_selected_schedule_preview.json"
)
DT_DIRECT_CANDIDATE_SHADOW_TEACHER_ROWS_CSV_PATH = DT_RESEARCH_SHADOW_TEACHER_ROWS_CSV_PATH
DT_V2_PLUS_APPLES_TO_APPLES_SELECTED_PREVIEW_JSON_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_dt_v2_plus_apples_to_apples_current"
	/ "dt_research_shadow_selected_schedule_preview.json"
)
DT_V2_PLUS_APPLES_TO_APPLES_TEACHER_ROWS_CSV_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_dt_v2_plus_apples_to_apples_current"
	/ "dt_v2_plus_apples_to_apples_teacher_rows.csv"
)
DT_V2_PLUS_DISTILLATION_SHADOW_SELECTED_PREVIEW_JSON_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_dt_v2_plus_distillation_shadow_current"
	/ "dt_research_shadow_selected_schedule_preview.json"
)
DT_V2_PLUS_DISTILLATION_SHADOW_TEACHER_ROWS_CSV_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_dt_v2_plus_distillation_shadow_current"
	/ "dt_research_shadow_teacher_rows.csv"
)
DT_DECISION_AWARE_SHADOW_SELECTED_PREVIEW_JSON_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_dt_research_shadow_decision_aware_current"
	/ "dt_research_shadow_selected_schedule_preview.json"
)
DT_DECISION_AWARE_SHADOW_TEACHER_ROWS_CSV_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_dt_research_shadow_decision_aware_current"
	/ "dt_research_shadow_teacher_rows.csv"
)
REGRET_AWARE_V2_PLUS_SELECTOR_SELECTED_ROWS_CSV_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_regret_aware_v2_plus_selector_current"
	/ "regret_aware_v2_plus_selector_selected_rows.csv"
)
REGRET_AWARE_V2_PLUS_SELECTOR_TEACHER_ROWS_CSV_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_regret_aware_v2_plus_selector_current"
	/ "regret_aware_v2_plus_selector_teacher_rows.csv"
)
REGRET_AWARE_V2_PLUS_SELECTOR_SUMMARY_JSON_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_regret_aware_v2_plus_selector_current"
	/ "regret_aware_v2_plus_selector_summary.json"
)
DT_V2_PLUS_SAFE_SWITCH_SELECTOR_SELECTED_ROWS_CSV_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_dt_v2_plus_safe_switch_selector_current"
	/ "regret_aware_v2_plus_selector_selected_rows.csv"
)
DT_V2_PLUS_SAFE_SWITCH_SELECTOR_TEACHER_ROWS_CSV_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_dt_v2_plus_safe_switch_selector_current"
	/ "regret_aware_v2_plus_selector_teacher_rows.csv"
)
DT_V2_PLUS_SAFE_SWITCH_SELECTOR_SUMMARY_JSON_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_dt_v2_plus_safe_switch_selector_current"
	/ "regret_aware_v2_plus_selector_summary.json"
)
DT_V2_PLUS_PROMOTION_EVIDENCE_SUMMARY_JSON_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_dt_v2_plus_promotion_evidence_current"
	/ "dt_v2_plus_promotion_evidence_summary.json"
)
TFT_SHADOW_AUGMENTED_GATE_ROWS_CSV_PATH = (
	Path("data")
	/ "research_runs"
	/ "week3_tft_quantile_365_full_negative_evidence"
	/ "tft_augmented_gate_rows.csv"
)
ACADEMIC_MVP_REQUIRED_PASSPORT_GATES = frozenset(
	{
		"operator_preview_gate",
		"dam_bid_recommendation_preview_gate",
		"academic_source_governance_gate",
		"dt_lava_prototype_ci_smoke_gate",
		"lava_npz_smoke_packet_validation_gate",
		"dfl_dt_prototype_contract_gate",
		"v13_gated_teacher_contract_gate",
		"offline_challenger_non_promotion_gate",
		"dt_research_shadow_smoke_gate",
		"prototype_evidence_scorecard_gate",
		"market_execution_safety_gate",
	}
)
ACADEMIC_MVP_NON_REQUIRED_PASSPORT_GATES = frozenset(
	{
		"market_submission_receipt_gate",
		"dt_lava_training_promotion_gate",
		"market_execution_gate",
	}
)
ACADEMIC_MVP_REQUIRED_VALIDATION_GATES = frozenset(
	{
		"academic_mvp_gate",
		"operator_preview_gate",
		"dam_bid_recommendation_preview_gate",
		"academic_source_governance_gate",
		"dt_lava_prototype_ci_smoke_gate",
		"dfl_dt_prototype_contract_gate",
		"v13_gated_teacher_contract_gate",
		"offline_challenger_non_promotion_gate",
		"dt_research_shadow_gate",
		"prototype_evidence_scorecard_gate",
		"market_execution_safety_gate",
		"market_submission_receipt_gate",
		"dt_lava_training_promotion_gate",
		"market_execution_gate",
		"prototype_contract",
		"prototype_phase_readiness",
		"prototype_evidence_scorecard",
		"lava_npz_smoke_packet_validation",
		"teacher_packet_validation",
		"offline_challenger_packet_validation",
	}
)
ACADEMIC_MVP_REQUIRED_FALSE_FLAGS = {
	"market_submission_ready": False,
	"market_execution_gate_passed": False,
	"promotion_gate_passed": False,
	"permits_model_training": False,
	"market_execution_enabled": False,
	"no_market_execution_safety_gate_passed": True,
}
ACADEMIC_MVP_ALLOWED_DT_ACTION_TARGETS = frozenset(
	{
		"candidate_id",
		"candidate_index",
		"schedule_family",
		"schedule_block",
		"candidate_id_or_schedule_family",
		"candidate_id_or_schedule_block",
		"candidate_index_or_schedule_family",
		"candidate_index_or_schedule_block",
	}
)
V13_GOAL_BOUNDARY_DOC = "docs/technical/CURRENT_GOAL_BOUNDARY_V13.md"


class TenantSummaryResponse(BaseModel):
	tenant_id: str
	name: str | None
	type: str | None
	latitude: float
	longitude: float
	timezone: str


class WeatherRunConfigRequest(BaseModel):
	tenant_id: str
	location_config_path: str | None = None


class WeatherLocationResponse(BaseModel):
	latitude: float
	longitude: float
	timezone: str


class WeatherRunConfigResponse(BaseModel):
	tenant_id: str
	run_config: dict[str, Any]
	resolved_location: WeatherLocationResponse


class WeatherMaterializeRequest(BaseModel):
	tenant_id: str
	include_price_history: bool
	location_config_path: str | None = None


class WeatherMaterializeResponse(BaseModel):
	tenant_id: str
	selected_assets: list[str]
	run_config: dict[str, Any]
	resolved_location: WeatherLocationResponse
	success: bool


class DashboardSignalPreviewResponse(BaseModel):
	tenant_id: str
	labels: list[str]
	label_timestamps: list[datetime]
	latest_price_timestamp: datetime | None = None
	forecast_window_start: datetime | None = None
	forecast_window_end: datetime | None = None
	timezone: str
	market_price: list[float]
	weather_bias: list[float]
	weather_sources: list[str]
	charge_intent: list[float]
	regret: list[float]
	resolved_location: WeatherLocationResponse


class OperatorStatusResponse(BaseModel):
	tenant_id: str
	flow_type: OperatorFlowType
	status: OperatorFlowStatus
	updated_at: str
	payload: dict[str, Any] | None
	last_error: str | None


class GatekeeperValidationStatusResponse(BaseModel):
	tenant_id: str
	status: str
	validation_stage: str | None = None
	contract_type: str | None = None
	canonical_outcome: str | None = None
	venue: str | None = None
	interval_start: datetime | None = None
	duration_minutes: int | None = None
	failure_reason: str | None = None
	created_at: datetime | None = None
	no_bid_semantics: str
	hold_semantics: str
	latest_failure_id: str | None = None


class ProjectedBatterySchedulePointRequest(BaseModel):
	interval_start: datetime
	net_power_mw: float


class ProjectedBatteryStateRequest(BaseModel):
	tenant_id: str
	current_soc_fraction: float | None = None
	battery_metrics: BatteryPhysicalMetrics | None = None
	schedule: list[ProjectedBatterySchedulePointRequest] | None = None


class ProjectedBatteryTracePointResponse(BaseModel):
	step_index: int
	interval_start: datetime
	requested_net_power_mw: float
	feasible_net_power_mw: float
	soc_before_fraction: float
	soc_after_fraction: float
	throughput_mwh: float
	degradation_penalty_uah: float


class ProjectedBatteryStateResponse(BaseModel):
	tenant_id: str
	interval_minutes: int
	starting_soc_fraction: float
	battery_metrics: BatteryPhysicalMetrics
	total_throughput_mwh: float
	total_degradation_penalty_uah: float
	trace: list[ProjectedBatteryTracePointResponse]


class BaselineForecastPointResponse(BaseModel):
	forecast_timestamp: datetime
	source_timestamp: datetime
	predicted_price_uah_mwh: float


class BaselineRecommendationPointResponse(BaseModel):
	step_index: int
	interval_start: datetime
	forecast_price_uah_mwh: float
	recommended_net_power_mw: float
	projected_soc_before_fraction: float
	projected_soc_after_fraction: float
	throughput_mwh: float
	degradation_penalty_uah: float
	gross_market_value_uah: float
	net_value_uah: float


class BidRecommendationPreviewPointResponse(BaseModel):
	step_index: int
	interval_start: datetime
	market_venue: str
	side: str
	operator_action: str
	quantity_mw: float
	indicative_limit_price_uah_mwh: float
	preview_only: bool
	market_execution_enabled: bool
	market_order_payload_emitted: bool
	proposed_bid_status: str
	read_model_boundary: str


class BaselinePreviewEconomicsResponse(BaseModel):
	total_gross_market_value_uah: float
	total_degradation_penalty_uah: float
	total_net_value_uah: float
	total_throughput_mwh: float


class BaselineLpPreviewResponse(BaseModel):
	tenant_id: str
	market_venue: str
	market_scope: str
	interval_minutes: int
	anchor_timestamp: datetime
	forecast_generated_at: datetime | None
	target_delivery_window_start: datetime | None
	target_delivery_window_end: datetime | None
	market_execution_enabled: bool
	read_model_boundary: str
	market_gate_status: str
	bid_eligibility_status: str
	proposed_bid_status: str
	starting_soc_fraction: float
	starting_soc_source: str
	battery_metrics: BatteryPhysicalMetrics
	resolved_location: WeatherLocationResponse
	forecast: list[BaselineForecastPointResponse]
	recommendation_schedule: list[BaselineRecommendationPointResponse]
	bid_recommendation_preview: list[BidRecommendationPreviewPointResponse]
	projected_state: ProjectedBatteryStateResponse
	economics: BaselinePreviewEconomicsResponse
	telemetry_freshness: dict[str, Any] | None = None


class BatteryTelemetryObservationResponse(BaseModel):
	tenant_id: str
	observed_at: datetime
	current_soc: float
	soh: float
	power_mw: float
	temperature_c: float | None
	source: str
	source_kind: str


class BatteryStateHourlySnapshotResponse(BaseModel):
	tenant_id: str
	snapshot_hour: datetime
	observation_count: int
	soc_open: float
	soc_close: float
	soc_mean: float
	soh_close: float
	power_mw_mean: float
	throughput_mwh: float
	efc_delta: float
	telemetry_freshness: str
	first_observed_at: datetime
	last_observed_at: datetime


class TelemetryIngestSourceResponse(BaseModel):
	protocol: str
	broker_host: str
	broker_port: int
	topic: str
	source_kind: str


class DashboardBatteryStateResponse(BaseModel):
	tenant_id: str
	latest_telemetry: BatteryTelemetryObservationResponse | None
	hourly_snapshot: BatteryStateHourlySnapshotResponse | None
	fallback_reason: str | None
	telemetry_ingest_source: TelemetryIngestSourceResponse


class ExogenousWeatherSignalResponse(BaseModel):
	timestamp: datetime
	fetched_at: datetime
	source: str
	source_kind: str
	source_url: str
	temperature: float
	cloudcover: float
	wind_speed: float
	precipitation: float
	freshness_hours: float | None


class ExogenousGridEventResponse(BaseModel):
	post_id: str
	post_url: str
	published_at: datetime
	fetched_at: datetime
	raw_text_summary: str
	source: str
	source_kind: str
	source_url: str
	energy_system_status: bool
	shelling_damage: bool
	outage_or_restriction: bool
	consumption_change: str
	solar_shift_advice: bool
	evening_saving_request: bool
	affected_oblasts: list[str]
	freshness_hours: float | None


class DashboardExogenousSignalsResponse(BaseModel):
	tenant_id: str
	resolved_location: WeatherLocationResponse
	latest_weather: ExogenousWeatherSignalResponse | None
	latest_grid_event: ExogenousGridEventResponse | None
	grid_event_count_24h: float
	tenant_region_affected: bool
	national_grid_risk_score: float
	outage_flag: bool
	saving_request_flag: bool
	solar_shift_hint: bool
	event_source_freshness_hours: float | None
	source_urls: list[str]
	fallback_reason: str | None


class ForecastStrategyComparisonPointResponse(BaseModel):
	forecast_model_name: str
	strategy_kind: str
	decision_value_uah: float
	forecast_objective_value_uah: float
	oracle_value_uah: float
	regret_uah: float
	regret_ratio: float
	total_degradation_penalty_uah: float
	total_throughput_mwh: float
	committed_action: str
	committed_power_mw: float
	rank_by_regret: int
	evaluation_payload: dict[str, Any]


class ForecastStrategyComparisonResponse(BaseModel):
	tenant_id: str
	market_venue: str
	evaluation_id: str
	anchor_timestamp: datetime
	generated_at: datetime
	horizon_hours: int
	starting_soc_fraction: float
	starting_soc_source: str
	comparisons: list[ForecastStrategyComparisonPointResponse]


class RealDataBenchmarkPointResponse(BaseModel):
	evaluation_id: str
	anchor_timestamp: datetime
	forecast_model_name: str
	decision_value_uah: float
	oracle_value_uah: float
	regret_uah: float
	regret_ratio: float
	total_degradation_penalty_uah: float
	total_throughput_mwh: float
	committed_action: str
	committed_power_mw: float
	rank_by_regret: int
	evaluation_payload: dict[str, Any]


class RealDataBenchmarkResponse(BaseModel):
	tenant_id: str
	market_venue: str
	generated_at: datetime
	data_quality_tier: str
	anchor_count: int
	model_count: int
	best_model_name: str | None
	mean_regret_uah: float
	median_regret_uah: float
	rows: list[RealDataBenchmarkPointResponse]


class ForecastDispatchSensitivityPointResponse(BaseModel):
	diagnostic_id: str
	evaluation_id: str
	anchor_timestamp: datetime
	forecast_model_name: str
	diagnostic_bucket: str
	regret_uah: float
	regret_ratio: float
	forecast_mae_uah_mwh: float
	forecast_rmse_uah_mwh: float
	mean_forecast_error_uah_mwh: float
	forecast_dispatch_spread_uah_mwh: float
	realized_dispatch_spread_uah_mwh: float
	dispatch_spread_error_uah_mwh: float
	total_degradation_penalty_uah: float
	total_throughput_mwh: float
	charge_energy_mwh: float
	discharge_energy_mwh: float
	committed_action: str
	committed_power_mw: float
	rank_by_regret: int
	data_quality_tier: str


class ForecastDispatchSensitivityBucketResponse(BaseModel):
	diagnostic_bucket: str
	rows: int
	mean_regret_uah: float
	mean_forecast_mae_uah_mwh: float
	mean_dispatch_spread_error_uah_mwh: float


class ForecastDispatchSensitivityResponse(BaseModel):
	tenant_id: str
	market_venue: str
	generated_at: datetime
	source_strategy_kind: str
	anchor_count: int
	model_count: int
	row_count: int
	bucket_summary: list[ForecastDispatchSensitivityBucketResponse]
	rows: list[ForecastDispatchSensitivityPointResponse]


class DflRelaxedPilotPointResponse(BaseModel):
	pilot_name: str
	evaluation_id: str
	anchor_timestamp: datetime
	forecast_model_name: str
	horizon_hours: int
	relaxed_realized_value_uah: float
	relaxed_oracle_value_uah: float
	relaxed_regret_uah: float
	first_charge_mw: float
	first_discharge_mw: float
	academic_scope: str


class DflRelaxedPilotResponse(BaseModel):
	tenant_id: str
	row_count: int
	mean_relaxed_regret_uah: float
	academic_scope: str
	rows: list[DflRelaxedPilotPointResponse]


class DflScheduleValueProductionGatePointResponse(BaseModel):
	source_model_name: str
	tenant_count: int
	latest_validation_tenant_anchor_count: int
	latest_strict_mean_regret_uah: float
	latest_selected_mean_regret_uah: float
	latest_strict_median_regret_uah: float
	latest_selected_median_regret_uah: float
	latest_mean_regret_improvement_ratio_vs_strict: float
	rolling_window_count: int
	rolling_strict_pass_window_count: int
	robust_research_challenger: bool
	production_promote: bool
	promotion_blocker: str
	allowed_challenger: str
	fallback_strategy: str
	market_execution_enabled: bool
	not_full_dfl: bool
	not_market_execution: bool


class DflScheduleValueProductionGateResponse(BaseModel):
	generated_at: datetime
	row_count: int
	production_promote_count: int
	promoted_source_model_names: list[str]
	fallback_strategy: str
	market_execution_enabled: bool
	claim_scope: str
	claim_boundary: str
	academic_scope: str
	rows: list[DflScheduleValueProductionGatePointResponse]


class DecisionTransformerTrajectoryPointResponse(BaseModel):
	episode_id: str
	market_venue: str
	scenario_index: int
	step_index: int
	interval_start: datetime
	state_soc_before: float
	state_soc_after: float
	state_soh: float
	state_market_price_uah_mwh: float
	action_charge_mw: float
	action_discharge_mw: float
	reward_uah: float
	return_to_go_uah: float
	degradation_penalty_uah: float
	baseline_value_uah: float
	oracle_value_uah: float
	regret_uah: float
	academic_scope: str


class DecisionTransformerTrajectoryResponse(BaseModel):
	tenant_id: str
	row_count: int
	episode_count: int
	academic_scope: str
	rows: list[DecisionTransformerTrajectoryPointResponse]


class DecisionPolicyPreviewPointResponse(BaseModel):
	policy_run_id: str
	created_at: datetime
	episode_id: str
	market_venue: str
	scenario_index: int
	step_index: int
	interval_start: datetime
	state_market_price_uah_mwh: float
	state_nbeatsx_forecast_uah_mwh: float | None = None
	state_tft_forecast_uah_mwh: float | None = None
	state_forecast_uncertainty_uah_mwh: float | None = None
	state_forecast_spread_uah_mwh: float | None = None
	projected_soc_before: float
	projected_soc_after: float
	raw_charge_mw: float
	raw_discharge_mw: float
	projected_charge_mw: float
	projected_discharge_mw: float
	projected_net_power_mw: float
	projected_action_label: str
	projection_status: str
	projection_adjustment_mw: float
	expected_policy_value_uah: float
	hold_value_uah: float
	value_vs_hold_uah: float
	oracle_value_uah: float
	value_gap_uah: float
	value_gap_ratio: float | None
	constraint_violation: bool
	gatekeeper_status: str
	inference_latency_ms: float
	policy_mode: str
	readiness_status: str
	model_name: str
	academic_scope: str


class DecisionPolicyPreviewResponse(BaseModel):
	tenant_id: str
	row_count: int
	policy_run_id: str
	created_at: datetime
	policy_readiness: str
	live_policy_claim: bool
	market_execution_enabled: bool
	constraint_violation_count: int
	mean_value_gap_uah: float
	total_value_vs_hold_uah: float
	forecast_context_source: str
	forecast_context_row_count: int
	forecast_context_coverage_ratio: float
	forecast_context_warning: str | None = None
	policy_state_features: list[str]
	policy_value_interpretation: str
	operator_boundary: str
	academic_scope: str
	rows: list[DecisionPolicyPreviewPointResponse]


class SimulatedLiveTradingPointResponse(BaseModel):
	episode_id: str
	interval_start: datetime
	step_index: int
	state_soc_before: float
	state_soc_after: float
	proposed_trade_side: str
	proposed_quantity_mw: float
	feasible_net_power_mw: float
	market_price_uah_mwh: float
	reward_uah: float
	gatekeeper_status: str
	paper_trade_provenance: str
	settlement_id: str | None
	live_mode_warning: str


class SimulatedLiveTradingResponse(BaseModel):
	tenant_id: str
	row_count: int
	simulated_only: bool
	rows: list[SimulatedLiveTradingPointResponse]


class OperatorStrategyOptionResponse(BaseModel):
	strategy_id: str
	label: str
	enabled: bool
	reason: str
	mean_regret_uah: float | None = None
	win_rate: float | None = None


class OperatorLoadForecastPointResponse(BaseModel):
	timestamp: datetime
	load_mw: float
	pv_estimate_mw: float
	net_load_mw: float
	btm_battery_power_mw: float
	source_kind: str
	weather_source_kind: str
	reason_code: str


class OperatorSocProjectionPointResponse(BaseModel):
	timestamp: datetime
	physical_soc: float | None
	estimated_soc: float
	planning_soc: float
	soc_source: str
	confidence: str


class FutureForecastPointResponse(BaseModel):
	step_index: int
	interval_start: datetime
	forecast_price_uah_mwh: float
	actual_price_uah_mwh: float | None
	p10_price_uah_mwh: float | None
	p50_price_uah_mwh: float | None
	p90_price_uah_mwh: float | None
	net_power_mw: float | None
	value_gap_uah: float | None
	price_cap_status: str


class FutureForecastSeriesResponse(BaseModel):
	model_name: str
	model_family: str
	source_status: str
	uncertainty_kind: str
	mean_regret_uah: float | None
	win_rate: float | None
	out_of_dam_cap_rows: int
	quality_boundary: str
	points: list[FutureForecastPointResponse]


class RuntimeAccelerationResponse(BaseModel):
	backend: str
	device_type: str
	device_name: str
	gpu_available: bool
	cuda_version: str | None = None
	recommended_scope: str


class FutureStackPreviewResponse(BaseModel):
	tenant_id: str
	generated_at: datetime | None
	forecast_window_start: datetime | None
	forecast_window_end: datetime | None
	backend_status: dict[str, str]
	runtime_acceleration: RuntimeAccelerationResponse
	selected_forecast_model: str | None
	claim_boundary: str
	forecast_series: list[FutureForecastSeriesResponse]


class OperatorValueGapPointResponse(BaseModel):
	step_index: int
	interval_start: datetime
	chosen_value_uah: float
	best_visible_value_uah: float
	value_gap_uah: float
	metric_source: str


class OperatorV13SafeSwitchTargetResponse(BaseModel):
	acquisition_priority_rank: int
	tenant_id: str
	source_model_name: str
	current_prior_material_safe_switch_examples: int
	required_prior_material_safe_switch_examples: int
	target_new_prior_material_safe_switch_examples: int
	required_evidence_kind: str
	recommended_next_step: str
	target_is_precondition_only: bool
	market_execution_enabled: bool


class OperatorV13ReadinessResponse(BaseModel):
	gate_status: str
	v13_candidate_generation_ready: bool
	dt_lava_ready: bool
	ready_rows: int
	readiness_rows: int
	missing_safe_switch_examples: int
	missing_required_inputs: list[str]
	top_priority_blocker: str
	receipt_source_audit_probe_count: int
	receipt_source_audit_months_probed: list[str]
	receipt_source_audit_candidate_found: bool
	receipt_source_audit_csv_generated: bool
	receipt_source_audit_all_probes_insufficient: bool
	source_governance_status: str
	source_governance_label: str
	market_submission_receipt_gate_status: str
	scmo_credentials_required_for_diploma_mvp: bool
	scmo_credentials_required_for_market_submission_grade_receipts: bool
	safe_switch_target_tenant_source_count: int
	safe_switch_max_new_examples_required: int
	safe_switch_acquisition_targets: list[OperatorV13SafeSwitchTargetResponse]
	market_execution_enabled: bool
	boundary_doc: str
	source_packet_path: str | None


class OperatorRecommendationResponse(BaseModel):
	tenant_id: str
	market_scope: str
	market_venue: str
	interval_minutes: int
	anchor_timestamp: datetime
	forecast_generated_at: datetime | None
	target_delivery_window_start: datetime | None
	target_delivery_window_end: datetime | None
	market_execution_enabled: bool
	read_model_boundary: str
	market_gate_status: str
	bid_eligibility_status: str
	proposed_bid_status: str
	v13_readiness: OperatorV13ReadinessResponse
	selected_strategy_id: str
	selection_reason: str
	forecast_source: str
	soc_source: str
	review_required: bool
	readiness_warnings: list[str]
	policy_mode: str
	selected_policy_id: str
	policy_explanation: str
	policy_readiness: str
	policy_forecast_context_source: str
	policy_forecast_context_row_count: int
	policy_forecast_context_coverage_ratio: float
	policy_forecast_context_warning: str | None = None
	available_strategies: list[OperatorStrategyOptionResponse]
	forecast_model_series: list[FutureForecastSeriesResponse]
	value_gap_series: list[OperatorValueGapPointResponse]
	load_forecast: list[OperatorLoadForecastPointResponse]
	soc_projection: list[OperatorSocProjectionPointResponse]
	recommendation_schedule: list[BaselineRecommendationPointResponse]
	bid_recommendation_preview: list[BidRecommendationPreviewPointResponse]
	daily_value_uah: float
	hold_baseline_value_uah: float
	value_vs_hold_uah: float
	economics: BaselinePreviewEconomicsResponse


class ShadowPreviewSourceOptionResponse(BaseModel):
	preview_source_id: str
	label: str
	status: str
	is_default_strategy: bool
	is_promoted_strategy: bool
	market_execution_enabled: bool
	reason: str


class ShadowRecommendationSchedulePointResponse(BaseModel):
	step_index: int
	interval_start: datetime
	action: str
	quantity_mw: float
	recommended_net_power_mw: float
	forecast_price_uah_mwh: float
	soc_before_fraction: float | None
	soc_after_fraction: float | None
	selected_candidate_id: str
	schedule_family: str
	expected_value_uah: float
	regret_uah: float
	regret_vs_v2_plus_uah: float | None
	regret_vs_strict_uah: float | None
	value_vs_v2_plus_uah: float | None
	value_vs_strict_uah: float | None
	gate_status: str
	safety_status: str
	market_execution_enabled: bool
	market_order_payload_emitted: bool
	proposed_bid_status: str


class ShadowRecommendationPreviewResponse(BaseModel):
	tenant_id: str
	preview_source_id: str
	preview_source_label: str
	preview_status: str
	preview_only: bool
	is_default_strategy: bool
	is_promoted_strategy: bool
	research_shadow_not_promotable: bool
	default_strategy_id: str
	default_strategy_label: str
	selected_candidate_id: str | None
	selected_schedule_family: str | None
	selected_candidate_index: int | None
	market_scope: str
	market_venue: str
	interval_minutes: int
	anchor_timestamp: datetime | None
	target_delivery_window_start: datetime | None
	target_delivery_window_end: datetime | None
	market_execution_enabled: bool
	proposed_bid_status: str
	market_order_payload_emitted: bool
	promotion_gate_passed: bool
	dt_lava_ready: bool
	source_readiness_gate_passed: bool
	comparison_metrics: dict[str, float]
	available_preview_sources: list[ShadowPreviewSourceOptionResponse]
	recommendation_schedule: list[ShadowRecommendationSchedulePointResponse]
	boundary_labels: list[str]
	readiness_warnings: list[str]
	artifact_paths: dict[str, str]


class AcademicMvpReadinessResponse(BaseModel):
	claim_scope: str
	generated_at: datetime | None
	academic_mvp_gate_passed: bool
	operator_preview_gate: dict[str, Any]
	source_governance: dict[str, Any]
	dt_lava_prototype_gate: dict[str, Any]
	dt_lava_teacher_contract_gate: dict[str, Any]
	offline_challenger_gate: dict[str, Any]
	dt_research_shadow_gate: dict[str, Any]
	prototype_contract: dict[str, Any]
	prototype_evidence_scorecard: dict[str, Any]
	prototype_phase_readiness: dict[str, Any]
	gate_passport: dict[str, Any]
	market_submission_ready: bool
	market_execution_gate_passed: bool
	promotion_gate_passed: bool
	permits_model_training: bool
	market_execution_enabled: bool
	no_market_execution_safety_gate_passed: bool
	next_gate: str
	artifact_validation: dict[str, Any]
	source_packet_path: str
	artifact_validation_packet_path: str


@dataclass(frozen=True, slots=True)
class WeatherBiasCalibrationModel:
	feature_names: tuple[str, ...]
	feature_means: dict[str, float]
	feature_scales: dict[str, float]
	coefficients: dict[str, float]
	intercept_uah_mwh: float
	prediction_ceiling_uah_mwh: float

	def predict_uah_mwh(self, *, weather_row: dict[str, Any]) -> float:
		feature_values = _weather_feature_values_from_row(weather_row)
		prediction_uah_mwh = self.intercept_uah_mwh
		for feature_name in self.feature_names:
			feature_scale = self.feature_scales[feature_name]
			standardized_value = (feature_values[feature_name] - self.feature_means[feature_name]) / feature_scale
			prediction_uah_mwh += self.coefficients[feature_name] * standardized_value
		if prediction_uah_mwh > self.prediction_ceiling_uah_mwh:
			soft_ceiling_margin_uah_mwh = max(25.0, self.prediction_ceiling_uah_mwh * 0.2)
			overflow_uah_mwh = prediction_uah_mwh - self.prediction_ceiling_uah_mwh
			prediction_uah_mwh = self.prediction_ceiling_uah_mwh + soft_ceiling_margin_uah_mwh * math.log1p(
				overflow_uah_mwh / soft_ceiling_margin_uah_mwh
			)
		prediction_cap_uah_mwh = max(self.prediction_ceiling_uah_mwh, self.prediction_ceiling_uah_mwh * 2.5)
		return round(max(0.0, min(prediction_cap_uah_mwh, prediction_uah_mwh)), 2)


@dataclass(frozen=True, slots=True)
class TenantBatteryDefaults:
	metrics: BatteryPhysicalMetrics
	initial_soc_fraction: float


@dataclass(frozen=True, slots=True)
class StartingSocResolution:
	starting_soc_fraction: float
	source: str
	telemetry_freshness: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class OperatorSocResolution:
	physical_soc_fraction: float | None
	starting_soc_fraction: float
	source: str
	confidence: str
	review_required: bool
	warnings: tuple[str, ...]


@cache
def _mvp_asset_index() -> dict[str, Any]:
	from smart_arbitrage.assets.mvp_demo import MVP_DEMO_ASSETS

	return {
		asset.key.path[-1]: asset
		for asset in MVP_DEMO_ASSETS
	}


def _location_response_from_model(location: WeatherLocation) -> WeatherLocationResponse:
	return WeatherLocationResponse(
		latitude=location.latitude,
		longitude=location.longitude,
		timezone=location.timezone,
	)


def _resolve_requested_location(*, tenant_id: str, location_config_path: str | None) -> WeatherLocation:
	try:
		return resolve_weather_location_for_tenant(
			tenant_id=tenant_id,
			location_config_path=location_config_path,
		)
	except ValueError as error:
		raise HTTPException(status_code=404, detail=str(error)) from error


def _selected_weather_assets(*, include_price_history: bool) -> list[Any]:
	asset_index = _mvp_asset_index()
	selected_assets = [asset_index["weather_forecast_bronze"]]
	if include_price_history:
		selected_assets.append(asset_index["dam_price_history"])
	return selected_assets


def _build_signal_preview(*, tenant_id: str, location_config_path: str | None) -> DashboardSignalPreviewResponse:
	resolved_location = _resolve_requested_location(
		tenant_id=tenant_id,
		location_config_path=location_config_path,
	)
	battery_defaults = _resolve_tenant_battery_defaults(tenant_id=tenant_id)
	battery_metrics = battery_defaults.metrics
	starting_soc_fraction = battery_defaults.initial_soc_fraction
	price_history = _build_tenant_aware_price_history(resolved_location)
	anchor_timestamp = _resolve_baseline_anchor(price_history)
	historical_prices = _historical_prices_for_anchor(price_history, anchor_timestamp)
	weather_frame = _build_signal_preview_weather_frame(
		price_history=price_history,
		resolved_location=resolved_location,
	)
	weather_bias_model = _calibrate_weather_bias_model(
		historical_prices=historical_prices,
		weather_frame=weather_frame,
	)
	solver = HourlyDamBaselineSolver()
	solve_result = solver.solve_next_dispatch(
		historical_prices,
		battery_metrics=battery_metrics,
		current_soc_fraction=starting_soc_fraction,
		anchor_timestamp=anchor_timestamp,
	)
	forecast_points = solve_result.forecast[::3][:6] or solve_result.forecast[:6]
	labels = [point.forecast_timestamp.strftime("%H:%M") for point in forecast_points]
	label_timestamps = [point.forecast_timestamp for point in forecast_points]
	market_price = [round(point.predicted_price_uah_mwh, 2) for point in forecast_points]
	weather_rows_by_timestamp = _select_weather_rows_by_timestamp(
		forecast_points=forecast_points,
		weather_frame=weather_frame,
	)
	weather_sources = [
		str(weather_rows_by_timestamp.get(point.forecast_timestamp, {}).get("source", "SYNTHETIC"))
		for point in forecast_points
	]
	weather_bias = [
		weather_bias_model.predict_uah_mwh(
			weather_row=weather_rows_by_timestamp.get(point.forecast_timestamp, {}),
		)
		for point in forecast_points
	]
	if not _weather_bias_predictions_have_signal(weather_bias):
		fallback_weather_bias_model = _default_weather_bias_model()
		weather_bias = [
			fallback_weather_bias_model.predict_uah_mwh(
				weather_row=weather_rows_by_timestamp.get(point.forecast_timestamp, {}),
			)
			for point in forecast_points
		]
	adjusted_market_price = [
		price + weather_bias[index]
		for index, price in enumerate(market_price)
	]
	average_market_price = sum(adjusted_market_price) / len(adjusted_market_price)
	max_price_deviation = max(abs(value - average_market_price) for value in adjusted_market_price) or 1.0
	charge_intent = [
		round(
			max(
				-battery_metrics.max_power_mw,
				min(
					battery_metrics.max_power_mw,
					((value - average_market_price) / max_price_deviation) * battery_metrics.max_power_mw,
				),
			),
			2,
		)
		for value in adjusted_market_price
	]
	regret = [
		round(
			max(
				80.0,
				weather_bias[index] * 2.4 + abs(value - average_market_price) * 0.45,
			),
			2,
		)
		for index, value in enumerate(adjusted_market_price)
	]

	return DashboardSignalPreviewResponse(
		tenant_id=tenant_id,
		labels=labels,
		label_timestamps=label_timestamps,
		latest_price_timestamp=forecast_points[-1].forecast_timestamp if forecast_points else None,
		forecast_window_start=forecast_points[0].forecast_timestamp if forecast_points else None,
		forecast_window_end=forecast_points[-1].forecast_timestamp if forecast_points else None,
		timezone=resolved_location.timezone,
		market_price=market_price,
		weather_bias=weather_bias,
		weather_sources=weather_sources,
		charge_intent=charge_intent,
		regret=regret,
		resolved_location=_location_response_from_model(resolved_location),
	)


def _build_signal_preview_weather_frame(
	*,
	price_history: pl.DataFrame,
	resolved_location: WeatherLocation,
	) -> pl.DataFrame:
	window_start = price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(0)
	if not isinstance(window_start, datetime):
		raise TypeError("Price history timestamp column must contain datetime values.")
	return build_weather_forecast_window(
		start_timestamp=window_start,
		hours=price_history.height,
		weather_location=resolved_location,
	)


def _select_weather_rows_by_timestamp(
	*,
	forecast_points: list[BaselineForecastPoint],
	weather_frame: pl.DataFrame,
) -> dict[datetime, dict[str, Any]]:
	if not forecast_points:
		return {}

	selected_weather_frame = weather_frame.filter(
		pl.col(DEFAULT_TIMESTAMP_COLUMN).is_in([point.forecast_timestamp for point in forecast_points])
	).select(
		[
			DEFAULT_TIMESTAMP_COLUMN,
			"temperature",
			"wind_speed",
			"cloudcover",
			"precipitation",
			"humidity",
			"effective_solar",
			"source",
		]
	)
	return {
		row[DEFAULT_TIMESTAMP_COLUMN]: row
		for row in selected_weather_frame.iter_rows(named=True)
	}


def _calibrate_weather_bias_model(
	*,
	historical_prices: pl.DataFrame,
	weather_frame: pl.DataFrame,
) -> WeatherBiasCalibrationModel:
	training_frame = _build_weather_bias_training_frame(
		historical_prices=historical_prices,
		weather_frame=weather_frame,
	)
	training_rows = list(training_frame.iter_rows(named=True))
	if len(training_rows) < 24:
		return _default_weather_bias_model()

	targets = [float(row["weather_premium_target_uah_mwh"]) for row in training_rows]
	if not _weather_bias_targets_have_signal(targets):
		return _default_weather_bias_model()

	target_mean = sum(targets) / len(targets)
	feature_means = {
		feature_name: sum(float(row[feature_name]) for row in training_rows) / len(training_rows)
		for feature_name in WEATHER_BIAS_FEATURE_NAMES
	}
	feature_scales = {
		feature_name: max(1.0, _population_standard_deviation([float(row[feature_name]) for row in training_rows]))
		for feature_name in WEATHER_BIAS_FEATURE_NAMES
	}
	standardized_rows = [
		[
			(float(row[feature_name]) - feature_means[feature_name]) / feature_scales[feature_name]
			for feature_name in WEATHER_BIAS_FEATURE_NAMES
		]
		for row in training_rows
	]
	centered_targets = [target - target_mean for target in targets]
	coefficients = _fit_ridge_regression(
		standardized_rows=standardized_rows,
		centered_targets=centered_targets,
		feature_names=WEATHER_BIAS_FEATURE_NAMES,
	)
	prediction_ceiling_uah_mwh = max(120.0, max(targets) * 1.15)
	return WeatherBiasCalibrationModel(
		feature_names=WEATHER_BIAS_FEATURE_NAMES,
		feature_means=feature_means,
		feature_scales=feature_scales,
		coefficients=coefficients,
		intercept_uah_mwh=target_mean,
		prediction_ceiling_uah_mwh=prediction_ceiling_uah_mwh,
	)


def _build_weather_bias_training_frame(
	*,
	historical_prices: pl.DataFrame,
	weather_frame: pl.DataFrame,
) -> pl.DataFrame:
	weather_enriched_history = enrich_market_price_history_with_weather(historical_prices, weather_frame)
	hourly_baseline_by_hour = weather_enriched_history.select(
		[
			DEFAULT_TIMESTAMP_COLUMN,
			DEFAULT_PRICE_COLUMN,
			"weather_temperature",
			"weather_wind_speed",
			"weather_cloudcover",
			"weather_precipitation",
			"weather_humidity",
			"weather_effective_solar",
		]
	).with_columns(
		pl.col(DEFAULT_TIMESTAMP_COLUMN).dt.hour().alias("hour_of_day")
	).group_by("hour_of_day").agg(
		pl.col(DEFAULT_PRICE_COLUMN).mean().alias("hourly_baseline_price_uah_mwh")
	)
	return weather_enriched_history.select(
		[
			DEFAULT_TIMESTAMP_COLUMN,
			DEFAULT_PRICE_COLUMN,
			"weather_temperature",
			"weather_wind_speed",
			"weather_cloudcover",
			"weather_precipitation",
			"weather_humidity",
			"weather_effective_solar",
		]
	).with_columns(
		[
			pl.col(DEFAULT_TIMESTAMP_COLUMN).dt.hour().alias("hour_of_day"),
			pl.col("weather_cloudcover").fill_null(50.0).clip(0.0, 100.0).alias("cloudcover"),
			pl.col("weather_precipitation").fill_null(0.0).clip(0.0, 100.0).alias("precipitation"),
			(pl.col("weather_humidity").fill_null(60.0) - 65.0).clip(0.0, 100.0).alias("humidity_excess"),
			(pl.col("weather_temperature").fill_null(18.0) - 18.0).abs().alias("temperature_gap"),
			pl.col("weather_effective_solar").fill_null(0.0).clip(0.0, 1200.0).alias("effective_solar"),
			pl.col("weather_wind_speed").fill_null(5.0).clip(0.0, 50.0).alias("wind_speed"),
		]
	).join(
		hourly_baseline_by_hour,
		on="hour_of_day",
		how="left",
	).with_columns(
		(
			pl.col(DEFAULT_PRICE_COLUMN) - pl.col("hourly_baseline_price_uah_mwh")
		).clip(0.0, 1800.0).alias("weather_premium_target_uah_mwh")
	).select(
		[
			"cloudcover",
			"precipitation",
			"humidity_excess",
			"temperature_gap",
			"effective_solar",
			"wind_speed",
			"weather_premium_target_uah_mwh",
		]
	)


def _weather_feature_values_from_row(weather_row: dict[str, Any]) -> dict[str, float]:
	temperature = _coerce_weather_metric(weather_row.get("temperature"), default=18.0)
	humidity = _coerce_weather_metric(weather_row.get("humidity"), default=60.0)
	return {
		"cloudcover": _coerce_weather_metric(weather_row.get("cloudcover"), default=50.0),
		"precipitation": _coerce_weather_metric(weather_row.get("precipitation"), default=0.0),
		"humidity_excess": max(0.0, humidity - 65.0),
		"temperature_gap": abs(temperature - 18.0),
		"effective_solar": _coerce_weather_metric(weather_row.get("effective_solar"), default=0.0),
		"wind_speed": _coerce_weather_metric(weather_row.get("wind_speed"), default=5.0),
	}


def _weather_bias_targets_have_signal(targets: list[float]) -> bool:
	if not targets:
		return False
	if max(targets) < MIN_WEATHER_BIAS_TARGET_PEAK_UAH_MWH:
		return False
	return _population_standard_deviation(targets) >= MIN_WEATHER_BIAS_TARGET_SPREAD_UAH_MWH


def _weather_bias_predictions_have_signal(predictions: list[float]) -> bool:
	if not predictions:
		return False
	if max(predictions) <= 0.0:
		return False
	return _population_standard_deviation(predictions) >= MIN_WEATHER_BIAS_PREDICTION_SPREAD_UAH_MWH


def _default_weather_bias_model() -> WeatherBiasCalibrationModel:
	return WeatherBiasCalibrationModel(
		feature_names=WEATHER_BIAS_FEATURE_NAMES,
		feature_means={
			"cloudcover": 45.0,
			"precipitation": 0.0,
			"humidity_excess": 5.0,
			"temperature_gap": 8.0,
			"effective_solar": 250.0,
			"wind_speed": 5.0,
		},
		feature_scales={
			"cloudcover": 25.0,
			"precipitation": 1.0,
			"humidity_excess": 15.0,
			"temperature_gap": 10.0,
			"effective_solar": 250.0,
			"wind_speed": 8.0,
		},
		coefficients={
			"cloudcover": 55.0,
			"precipitation": 70.0,
			"humidity_excess": 28.0,
			"temperature_gap": 32.0,
			"effective_solar": -35.0,
			"wind_speed": -12.0,
		},
		intercept_uah_mwh=135.0,
		prediction_ceiling_uah_mwh=360.0,
	)


def _fit_ridge_regression(
	*,
	standardized_rows: list[list[float]],
	centered_targets: list[float],
	feature_names: tuple[str, ...],
) -> dict[str, float]:
	feature_count = len(feature_names)
	x_transpose_x = [
		[0.0 for _ in range(feature_count)]
		for _ in range(feature_count)
	]
	x_transpose_y = [0.0 for _ in range(feature_count)]
	for row_values, centered_target in zip(standardized_rows, centered_targets, strict=False):
		for left_index in range(feature_count):
			x_transpose_y[left_index] += row_values[left_index] * centered_target
			for right_index in range(feature_count):
				x_transpose_x[left_index][right_index] += row_values[left_index] * row_values[right_index]
	ridge_penalty = 0.75
	for feature_index in range(feature_count):
		x_transpose_x[feature_index][feature_index] += ridge_penalty
	coefficient_values = _solve_linear_system(
		matrix=x_transpose_x,
		vector=x_transpose_y,
	)
	return {
		feature_name: coefficient_values[index]
		for index, feature_name in enumerate(feature_names)
	}


def _solve_linear_system(*, matrix: list[list[float]], vector: list[float]) -> list[float]:
	augmented_matrix = [
		row_values[:] + [vector[row_index]]
		for row_index, row_values in enumerate(matrix)
	]
	size = len(augmented_matrix)
	for pivot_index in range(size):
		pivot_row_index = max(
			range(pivot_index, size),
			key=lambda row_index: abs(augmented_matrix[row_index][pivot_index]),
		)
		pivot_value = augmented_matrix[pivot_row_index][pivot_index]
		if abs(pivot_value) < 1e-9:
			return [0.0 for _ in range(size)]
		if pivot_row_index != pivot_index:
			augmented_matrix[pivot_index], augmented_matrix[pivot_row_index] = (
				augmented_matrix[pivot_row_index],
				augmented_matrix[pivot_index],
			)
		pivot_value = augmented_matrix[pivot_index][pivot_index]
		augmented_matrix[pivot_index] = [
			value / pivot_value
			for value in augmented_matrix[pivot_index]
		]
		for row_index in range(size):
			if row_index == pivot_index:
				continue
			factor = augmented_matrix[row_index][pivot_index]
			if abs(factor) < 1e-9:
				continue
			augmented_matrix[row_index] = [
				current_value - factor * pivot_row_value
				for current_value, pivot_row_value in zip(augmented_matrix[row_index], augmented_matrix[pivot_index], strict=False)
			]
	return [row_values[-1] for row_values in augmented_matrix]


def _population_standard_deviation(values: list[float]) -> float:
	if not values:
		return 0.0
	mean_value = sum(values) / len(values)
	variance = sum((value - mean_value) ** 2 for value in values) / len(values)
	return variance ** 0.5


def _coerce_weather_metric(value: Any, *, default: float) -> float:
	if isinstance(value, bool):
		return float(value)
	if isinstance(value, int | float):
		return float(value)
	return default


def _persist_operator_status(
	*,
	tenant_id: str,
	flow_type: OperatorFlowType,
	status: OperatorFlowStatus,
	payload: dict[str, Any] | None = None,
	last_error: str | None = None,
) -> None:
	store = get_operator_status_store()
	store.upsert_status(
		OperatorStatusRecord(
			tenant_id=tenant_id,
			flow_type=flow_type,
			status=status,
			updated_at=utc_now(),
			payload=payload,
			last_error=last_error,
		)
	)


def _resolve_tenant_battery_defaults(*, tenant_id: str) -> TenantBatteryDefaults:
	try:
		tenant_entry = resolve_tenant_registry_entry(tenant_id=tenant_id)
	except ValueError as error:
		raise HTTPException(status_code=404, detail=str(error)) from error

	try:
		energy_system = _tenant_energy_system_from_entry(tenant_entry)
		capacity_kwh = _required_positive_registry_float(
			energy_system,
			field_name="battery_capacity_kwh",
		)
		max_power_kw = _positive_registry_float(
			energy_system,
			field_name="battery_max_power_kw",
			default_value=capacity_kwh * 0.5,
		)
		round_trip_efficiency = _bounded_registry_float(
			energy_system,
			field_name="round_trip_efficiency",
			default_value=0.92,
			minimum=0.0,
			maximum=1.0,
		)
		initial_soc_fraction = _bounded_registry_float(
			energy_system,
			field_name="initial_soc_fraction",
			default_value=0.52,
			minimum=0.0,
			maximum=1.0,
		)
		soc_min_fraction = _bounded_registry_float(
			energy_system,
			field_name="soc_min_fraction",
			default_value=0.05,
			minimum=0.0,
			maximum=1.0,
		)
		soc_max_fraction = _bounded_registry_float(
			energy_system,
			field_name="soc_max_fraction",
			default_value=0.95,
			minimum=0.0,
			maximum=1.0,
		)
		degradation_cost_per_cycle_uah = _tenant_degradation_cost_per_cycle_uah(
			energy_system=energy_system,
			capacity_kwh=capacity_kwh,
		)
		metrics = BatteryPhysicalMetrics(
			capacity_mwh=capacity_kwh / 1000.0,
			max_power_mw=max_power_kw / 1000.0,
			round_trip_efficiency=round_trip_efficiency,
			degradation_cost_per_cycle_uah=degradation_cost_per_cycle_uah,
			soc_min_fraction=soc_min_fraction,
			soc_max_fraction=soc_max_fraction,
		)
	except ValueError as error:
		raise HTTPException(status_code=500, detail=f"Invalid tenant battery config for {tenant_id}: {error}") from error
	return TenantBatteryDefaults(metrics=metrics, initial_soc_fraction=initial_soc_fraction)


def _tenant_energy_system_from_entry(tenant_entry: dict[str, Any]) -> dict[str, Any]:
	energy_system = tenant_entry.get("energy_system")
	if not isinstance(energy_system, dict):
		raise ValueError("energy_system mapping is required.")
	return energy_system


def _tenant_degradation_cost_per_cycle_uah(*, energy_system: dict[str, Any], capacity_kwh: float) -> float:
	capex_usd_per_kwh = _positive_registry_float(
		energy_system,
		field_name="battery_capex_usd_per_kwh",
		default_value=DEMO_BATTERY_CAPEX_USD_PER_KWH,
	)
	lifetime_years = _positive_registry_float(
		energy_system,
		field_name="battery_lifetime_years",
		default_value=float(DEMO_BATTERY_LIFETIME_YEARS),
	)
	cycles_per_day = _positive_registry_float(
		energy_system,
		field_name="battery_cycles_per_day",
		default_value=DEMO_BATTERY_CYCLES_PER_DAY,
	)
	lifetime_cycles = lifetime_years * 365.0 * cycles_per_day
	replacement_cost_uah = capex_usd_per_kwh * capacity_kwh * DEMO_USD_TO_UAH_RATE
	return replacement_cost_uah / lifetime_cycles


def _required_positive_registry_float(mapping: dict[str, Any], *, field_name: str) -> float:
	if field_name not in mapping:
		raise ValueError(f"{field_name} is required.")
	return _positive_registry_float(mapping, field_name=field_name, default_value=0.0)


def _positive_registry_float(mapping: dict[str, Any], *, field_name: str, default_value: float) -> float:
	raw_value = mapping.get(field_name, default_value)
	value = _registry_float_value(raw_value, field_name=field_name)
	if value <= 0.0:
		raise ValueError(f"{field_name} must be positive.")
	return value


def _bounded_registry_float(
	mapping: dict[str, Any],
	*,
	field_name: str,
	default_value: float,
	minimum: float,
	maximum: float,
) -> float:
	value = _registry_float_value(mapping.get(field_name, default_value), field_name=field_name)
	if not minimum <= value <= maximum:
		raise ValueError(f"{field_name} must be between {minimum} and {maximum}.")
	return value


def _registry_float_value(raw_value: Any, *, field_name: str) -> float:
	if isinstance(raw_value, bool):
		raise ValueError(f"{field_name} must be numeric.")
	try:
		return float(raw_value)
	except (TypeError, ValueError) as error:
		raise ValueError(f"{field_name} must be numeric.") from error


def _default_projection_schedule(anchor_timestamp: datetime) -> list[ScheduledPowerPoint]:
	default_net_power_mw = [-1.2, -0.8, 0.5, 1.4, 1.8, 0.6]
	return [
		ScheduledPowerPoint(
			interval_start=anchor_timestamp + timedelta(hours=index),
			net_power_mw=net_power_mw,
		)
		for index, net_power_mw in enumerate(default_net_power_mw)
	]


def _tenant_price_bias(location: WeatherLocation) -> float:
	latitude_bias = (location.latitude - 49.0) * 28.0
	longitude_bias = (location.longitude - 31.0) * 12.0
	return latitude_bias + longitude_bias


def _build_tenant_aware_price_history(location: WeatherLocation) -> pl.DataFrame:
	price_history = build_synthetic_market_price_history(history_hours=15 * 24, forecast_hours=24)
	price_bias = _tenant_price_bias(location)
	return price_history.with_columns(
		(
			pl.col(DEFAULT_PRICE_COLUMN)
			+ pl.lit(price_bias)
			+ pl.when(pl.col(DEFAULT_TIMESTAMP_COLUMN).dt.hour().is_between(18, 21, closed="both"))
			.then(140.0)
			.when(pl.col(DEFAULT_TIMESTAMP_COLUMN).dt.hour().is_between(0, 5, closed="both"))
			.then(-90.0)
			.otherwise(0.0)
		).alias(DEFAULT_PRICE_COLUMN)
	)


def _resolve_baseline_anchor(price_history: pl.DataFrame) -> datetime:
	latest_timestamp = price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(-1)
	if not isinstance(latest_timestamp, datetime):
		raise TypeError("Price history timestamp column must contain datetime values.")
	return latest_timestamp - timedelta(hours=24)


def _operator_dam_delivery_anchor(anchor_timestamp: datetime) -> datetime:
	delivery_start = (anchor_timestamp + timedelta(days=1)).replace(
		hour=0,
		minute=0,
		second=0,
		microsecond=0,
	)
	return delivery_start - timedelta(hours=1)


def _historical_prices_for_anchor(
	price_history: pl.DataFrame,
	anchor_timestamp: datetime,
	*,
	required_through_timestamp: datetime | None = None,
) -> pl.DataFrame:
	prior_prices = price_history.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) <= anchor_timestamp)
	if prior_prices.height < 168:
		raise ValueError("At least 168 hourly DAM observations are required before the anchor timestamp.")
	filter_through_timestamp = max(
		anchor_timestamp,
		required_through_timestamp or anchor_timestamp,
	)
	historical_prices = price_history.filter(
		pl.col(DEFAULT_TIMESTAMP_COLUMN) <= filter_through_timestamp
	)
	return historical_prices


def _to_scheduled_power_points(schedule_result: BaselineSolveResult) -> list[ScheduledPowerPoint]:
	return [
		ScheduledPowerPoint(interval_start=point.interval_start, net_power_mw=point.net_power_mw)
		for point in schedule_result.schedule
	]


def _bid_preview_side_and_action(net_power_mw: float) -> tuple[str, str]:
	if net_power_mw > 1e-9:
		return "SELL", "discharge"
	if net_power_mw < -1e-9:
		return "BUY", "charge"
	return "HOLD", "hold"


def _to_bid_recommendation_preview(
	recommendation_schedule: list[BaselineRecommendationPointResponse],
) -> list[BidRecommendationPreviewPointResponse]:
	preview_points: list[BidRecommendationPreviewPointResponse] = []
	for point in recommendation_schedule:
		side, operator_action = _bid_preview_side_and_action(point.recommended_net_power_mw)
		preview_points.append(
			BidRecommendationPreviewPointResponse(
				step_index=point.step_index,
				interval_start=point.interval_start,
				market_venue=LEVEL1_MARKET_VENUE,
				side=side,
				operator_action=operator_action,
				quantity_mw=abs(point.recommended_net_power_mw),
				indicative_limit_price_uah_mwh=point.forecast_price_uah_mwh,
				preview_only=True,
				market_execution_enabled=False,
				market_order_payload_emitted=False,
				proposed_bid_status=OPERATOR_PROPOSED_BID_STATUS,
				read_model_boundary=OPERATOR_READ_MODEL_BOUNDARY,
			)
		)
	return preview_points


def _to_baseline_lp_preview_response(
	*,
	tenant_id: str,
	battery_metrics: BatteryPhysicalMetrics,
	starting_soc_fraction: float,
	starting_soc_source: str,
	telemetry_freshness: dict[str, Any] | None,
	resolved_location: WeatherLocation,
	solve_result: BaselineSolveResult,
	projected_state: ProjectedBatteryStateResponse,
	read_model_anchor_timestamp: datetime | None = None,
) -> BaselineLpPreviewResponse:
	total_gross_market_value_uah = sum(point.gross_market_value_uah for point in solve_result.schedule)
	total_degradation_penalty_uah = sum(point.degradation_penalty_uah for point in solve_result.schedule)
	total_net_value_uah = sum(point.net_objective_value_uah for point in solve_result.schedule)
	total_throughput_mwh = sum(point.throughput_mwh for point in solve_result.schedule)
	response_anchor_timestamp = read_model_anchor_timestamp or solve_result.anchor_timestamp
	target_delivery_window_start = solve_result.schedule[0].interval_start if solve_result.schedule else None
	target_delivery_window_end = (
		solve_result.schedule[-1].interval_start + timedelta(minutes=LEVEL1_INTERVAL_MINUTES)
		if solve_result.schedule
		else None
	)
	recommendation_schedule = [
		BaselineRecommendationPointResponse(
			step_index=point.step_index,
			interval_start=point.interval_start,
			forecast_price_uah_mwh=point.forecast_price_uah_mwh,
			recommended_net_power_mw=point.net_power_mw,
			projected_soc_before_fraction=point.soc_before_mwh / battery_metrics.capacity_mwh,
			projected_soc_after_fraction=point.soc_after_mwh / battery_metrics.capacity_mwh,
			throughput_mwh=point.throughput_mwh,
			degradation_penalty_uah=point.degradation_penalty_uah,
			gross_market_value_uah=point.gross_market_value_uah,
			net_value_uah=point.net_objective_value_uah,
		)
		for point in solve_result.schedule
	]
	return BaselineLpPreviewResponse(
		tenant_id=tenant_id,
		market_venue=LEVEL1_MARKET_VENUE,
		market_scope=OPERATOR_MARKET_SCOPE,
		interval_minutes=LEVEL1_INTERVAL_MINUTES,
		anchor_timestamp=response_anchor_timestamp,
		forecast_generated_at=None,
		target_delivery_window_start=target_delivery_window_start,
		target_delivery_window_end=target_delivery_window_end,
		market_execution_enabled=False,
		read_model_boundary=OPERATOR_READ_MODEL_BOUNDARY,
		market_gate_status=OPERATOR_MARKET_GATE_STATUS,
		bid_eligibility_status=OPERATOR_BID_ELIGIBILITY_STATUS,
		proposed_bid_status=OPERATOR_PROPOSED_BID_STATUS,
		starting_soc_fraction=starting_soc_fraction,
		starting_soc_source=starting_soc_source,
		battery_metrics=battery_metrics,
		resolved_location=_location_response_from_model(resolved_location),
		forecast=[
			BaselineForecastPointResponse(
				forecast_timestamp=point.forecast_timestamp,
				source_timestamp=point.source_timestamp,
				predicted_price_uah_mwh=point.predicted_price_uah_mwh,
			)
			for point in solve_result.forecast
		],
		recommendation_schedule=recommendation_schedule,
		bid_recommendation_preview=_to_bid_recommendation_preview(recommendation_schedule),
		projected_state=projected_state,
		economics=BaselinePreviewEconomicsResponse(
			total_gross_market_value_uah=total_gross_market_value_uah,
			total_degradation_penalty_uah=total_degradation_penalty_uah,
			total_net_value_uah=total_net_value_uah,
			total_throughput_mwh=total_throughput_mwh,
		),
		telemetry_freshness=telemetry_freshness,
	)


def _resolve_projection_request(
	request: ProjectedBatteryStateRequest,
) -> tuple[BatteryPhysicalMetrics, float, list[ScheduledPowerPoint]]:
	_resolve_requested_location(
		tenant_id=request.tenant_id,
		location_config_path=None,
	)
	battery_defaults = _resolve_tenant_battery_defaults(tenant_id=request.tenant_id)
	battery_metrics = request.battery_metrics or battery_defaults.metrics
	starting_soc_fraction = request.current_soc_fraction
	if starting_soc_fraction is None:
		starting_soc_fraction = battery_defaults.initial_soc_fraction
	if request.schedule is not None:
		schedule = [
			ScheduledPowerPoint(interval_start=point.interval_start, net_power_mw=point.net_power_mw)
			for point in request.schedule
		]
	else:
		anchor_timestamp = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
		schedule = _default_projection_schedule(anchor_timestamp)
	return battery_metrics, starting_soc_fraction, schedule


def _to_projected_battery_state_response(
	*,
	tenant_id: str,
	battery_metrics: BatteryPhysicalMetrics,
	simulation_result: ProjectedBatterySimulationResult,
) -> ProjectedBatteryStateResponse:
	return ProjectedBatteryStateResponse(
		tenant_id=tenant_id,
		interval_minutes=simulation_result.interval_minutes,
		starting_soc_fraction=simulation_result.starting_soc_fraction,
		battery_metrics=battery_metrics,
		total_throughput_mwh=simulation_result.total_throughput_mwh,
		total_degradation_penalty_uah=simulation_result.total_degradation_penalty_uah,
		trace=[
			ProjectedBatteryTracePointResponse(
				step_index=point.step_index,
				interval_start=point.interval_start,
				requested_net_power_mw=point.requested_net_power_mw,
				feasible_net_power_mw=point.feasible_net_power_mw,
				soc_before_fraction=point.soc_before_fraction,
				soc_after_fraction=point.soc_after_fraction,
				throughput_mwh=point.throughput_mwh,
				degradation_penalty_uah=point.degradation_penalty_uah,
			)
			for point in simulation_result.trace
		],
	)


def _to_battery_telemetry_response(observation: BatteryTelemetryObservation) -> BatteryTelemetryObservationResponse:
	return BatteryTelemetryObservationResponse(
		tenant_id=observation.tenant_id,
		observed_at=observation.observed_at,
		current_soc=observation.current_soc,
		soh=observation.soh,
		power_mw=observation.power_mw,
		temperature_c=observation.temperature_c,
		source=observation.source,
		source_kind=observation.source_kind,
	)


def _to_hourly_snapshot_response(snapshot: BatteryStateHourlySnapshot) -> BatteryStateHourlySnapshotResponse:
	return BatteryStateHourlySnapshotResponse(
		tenant_id=snapshot.tenant_id,
		snapshot_hour=snapshot.snapshot_hour,
		observation_count=snapshot.observation_count,
		soc_open=snapshot.soc_open,
		soc_close=snapshot.soc_close,
		soc_mean=snapshot.soc_mean,
		soh_close=snapshot.soh_close,
		power_mw_mean=snapshot.power_mw_mean,
		throughput_mwh=snapshot.throughput_mwh,
		efc_delta=snapshot.efc_delta,
		telemetry_freshness=snapshot.telemetry_freshness,
		first_observed_at=snapshot.first_observed_at,
		last_observed_at=snapshot.last_observed_at,
	)


def _battery_telemetry_ingest_source_response(tenant_id: str) -> TelemetryIngestSourceResponse:
	return TelemetryIngestSourceResponse(
		protocol="mqtt",
		broker_host=os.environ.get("MQTT_HOST", "localhost"),
		broker_port=_int_env("MQTT_PORT", default=1883),
		topic=battery_telemetry_topic(tenant_id),
		source_kind="configured_ingest_path_not_connectivity_probe",
	)


def _int_env(name: str, *, default: int) -> int:
	raw_value = os.environ.get(name)
	if raw_value is None:
		return default
	try:
		return int(raw_value)
	except ValueError:
		return default


def _build_exogenous_signals_response(tenant_id: str) -> DashboardExogenousSignalsResponse:
	resolved_location = _resolve_requested_location(tenant_id=tenant_id, location_config_path=None)
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	now = datetime.now(tz=UTC)
	latest_weather_row = _latest_weather_row(tenant_id=tenant_id)
	grid_event_frame = get_grid_event_store().list_grid_event_frame(source_kind="observed")
	latest_grid_event_row = _latest_grid_event_row(grid_event_frame)
	signal_timestamp = (
		_datetime_row_value(latest_grid_event_row["published_at"], field_name="published_at")
		if latest_grid_event_row is not None
		else now
	)
	event_signal = _grid_event_signal_snapshot(
		tenant_id=tenant_id,
		signal_timestamp=signal_timestamp,
		grid_event_frame=grid_event_frame,
	)
	latest_weather = (
		None
		if latest_weather_row is None
		else _to_exogenous_weather_signal_response(latest_weather_row, now=now)
	)
	latest_grid_event = (
		None
		if latest_grid_event_row is None
		else _to_exogenous_grid_event_response(latest_grid_event_row, now=now)
	)
	return DashboardExogenousSignalsResponse(
		tenant_id=tenant_id,
		resolved_location=_location_response_from_model(resolved_location),
		latest_weather=latest_weather,
		latest_grid_event=latest_grid_event,
		grid_event_count_24h=float(event_signal.get("grid_event_count_24h", 0.0)),
		tenant_region_affected=_bool_signal(event_signal.get("tenant_region_affected")),
		national_grid_risk_score=float(event_signal.get("national_grid_risk_score", 0.0)),
		outage_flag=_bool_signal(event_signal.get("outage_flag")),
		saving_request_flag=_bool_signal(event_signal.get("saving_request_flag")),
		solar_shift_hint=_bool_signal(event_signal.get("solar_shift_hint")),
		event_source_freshness_hours=_optional_signal_float(event_signal.get("event_source_freshness_hours")),
		source_urls=_exogenous_source_urls(
			latest_weather_row=latest_weather_row,
			latest_grid_event_row=latest_grid_event_row,
		),
		fallback_reason=_exogenous_fallback_reason(
			latest_weather_row=latest_weather_row,
			latest_grid_event_row=latest_grid_event_row,
		),
	)


def _latest_weather_row(*, tenant_id: str) -> dict[str, Any] | None:
	weather_frame = get_market_data_store().list_weather_observation_frame(tenant_id=tenant_id)
	if weather_frame.height == 0:
		return None
	return weather_frame.sort("timestamp").row(-1, named=True)


def _latest_grid_event_row(grid_event_frame: pl.DataFrame) -> dict[str, Any] | None:
	if grid_event_frame.height == 0:
		return None
	operational_rows = [
		row
		for row in grid_event_frame.sort(["published_at", "post_id"]).iter_rows(named=True)
		if is_operational_grid_event_row(row)
	]
	if not operational_rows:
		return None
	return operational_rows[-1]


def _grid_event_signal_snapshot(
	*,
	tenant_id: str,
	signal_timestamp: datetime,
	grid_event_frame: pl.DataFrame,
) -> dict[str, Any]:
	signal_frame = build_grid_event_signal_frame(
		price_history=pl.DataFrame({"timestamp": [signal_timestamp], "price_uah_mwh": [0.0]}),
		grid_events=grid_event_frame,
		tenant_ids=[tenant_id],
	)
	if signal_frame.height == 0:
		return {}
	return signal_frame.row(0, named=True)


def _to_exogenous_weather_signal_response(
	row: dict[str, Any],
	*,
	now: datetime,
) -> ExogenousWeatherSignalResponse:
	timestamp = _datetime_row_value(row["timestamp"], field_name="timestamp")
	fetched_at = _datetime_row_value(row["fetched_at"], field_name="fetched_at")
	return ExogenousWeatherSignalResponse(
		timestamp=timestamp,
		fetched_at=fetched_at,
		source=str(row["source"]),
		source_kind=str(row["source_kind"]),
		source_url=str(row["source_url"]),
		temperature=float(row["temperature"]),
		cloudcover=float(row["cloudcover"]),
		wind_speed=float(row["wind_speed"]),
		precipitation=float(row["precipitation"]),
		freshness_hours=_hours_between(now, fetched_at),
	)


def _to_exogenous_grid_event_response(
	row: dict[str, Any],
	*,
	now: datetime,
) -> ExogenousGridEventResponse:
	published_at = _datetime_row_value(row["published_at"], field_name="published_at")
	fetched_at = _datetime_row_value(row["fetched_at"], field_name="fetched_at")
	return ExogenousGridEventResponse(
		post_id=str(row["post_id"]),
		post_url=str(row["post_url"]),
		published_at=published_at,
		fetched_at=fetched_at,
		raw_text_summary=_short_text(str(row["raw_text"])),
		source=str(row["source"]),
		source_kind=str(row["source_kind"]),
		source_url=str(row["source_url"]),
		energy_system_status=bool(row["energy_system_status"]),
		shelling_damage=bool(row["shelling_damage"]),
		outage_or_restriction=bool(row["outage_or_restriction"]),
		consumption_change=str(row["consumption_change"]),
		solar_shift_advice=bool(row["solar_shift_advice"]),
		evening_saving_request=bool(row["evening_saving_request"]),
		affected_oblasts=_text_list(row["affected_oblasts"]),
		freshness_hours=_hours_between(now, fetched_at),
	)


def _short_text(value: str, *, limit: int = 280) -> str:
	if len(value) <= limit:
		return value
	return value[: limit - 1].rstrip() + "..."


def _text_list(value: Any) -> list[str]:
	if not isinstance(value, list):
		return []
	return [str(item) for item in value]


def _bool_signal(value: Any) -> bool:
	if isinstance(value, int | float):
		return float(value) > 0.0
	return bool(value)


def _optional_signal_float(value: Any) -> float | None:
	if isinstance(value, int | float):
		resolved_value = float(value)
		if resolved_value >= 999.0:
			return None
		return resolved_value
	return None


def _hours_between(now: datetime, earlier: datetime) -> float:
	return max(0.0, (_to_utc_datetime(now) - _to_utc_datetime(earlier)).total_seconds() / 3600.0)


def _to_utc_datetime(value: datetime) -> datetime:
	if value.tzinfo is None:
		return value.replace(tzinfo=UTC)
	return value.astimezone(UTC)


def _exogenous_source_urls(
	*,
	latest_weather_row: dict[str, Any] | None,
	latest_grid_event_row: dict[str, Any] | None,
) -> list[str]:
	source_urls = []
	if latest_weather_row is not None:
		source_urls.append(str(latest_weather_row["source_url"]))
	if latest_grid_event_row is not None:
		source_urls.append(str(latest_grid_event_row["source_url"]))
	return sorted(set(source_urls))


def _exogenous_fallback_reason(
	*,
	latest_weather_row: dict[str, Any] | None,
	latest_grid_event_row: dict[str, Any] | None,
) -> str | None:
	if latest_weather_row is None and latest_grid_event_row is None:
		return "weather_and_grid_events_unavailable"
	if latest_weather_row is None:
		return "weather_unavailable"
	if latest_grid_event_row is None:
		return "grid_events_unavailable"
	return None


def _to_forecast_strategy_comparison_response(
	*,
	tenant_id: str,
	evaluation_frame: pl.DataFrame,
) -> ForecastStrategyComparisonResponse:
	if evaluation_frame.height == 0:
		raise HTTPException(status_code=404, detail="Forecast strategy comparison not found.")
	rows = [
		row
		for row in evaluation_frame.sort(["rank_by_regret", "forecast_model_name"]).iter_rows(named=True)
	]
	first_row = rows[0]
	return ForecastStrategyComparisonResponse(
		tenant_id=tenant_id,
		market_venue=str(first_row["market_venue"]),
		evaluation_id=str(first_row["evaluation_id"]),
		anchor_timestamp=_datetime_row_value(first_row["anchor_timestamp"], field_name="anchor_timestamp"),
		generated_at=_datetime_row_value(first_row["generated_at"], field_name="generated_at"),
		horizon_hours=int(first_row["horizon_hours"]),
		starting_soc_fraction=float(first_row["starting_soc_fraction"]),
		starting_soc_source=str(first_row["starting_soc_source"]),
		comparisons=[
			ForecastStrategyComparisonPointResponse(
				forecast_model_name=str(row["forecast_model_name"]),
				strategy_kind=str(row["strategy_kind"]),
				decision_value_uah=float(row["decision_value_uah"]),
				forecast_objective_value_uah=float(row["forecast_objective_value_uah"]),
				oracle_value_uah=float(row["oracle_value_uah"]),
				regret_uah=float(row["regret_uah"]),
				regret_ratio=float(row["regret_ratio"]),
				total_degradation_penalty_uah=float(row["total_degradation_penalty_uah"]),
				total_throughput_mwh=float(row["total_throughput_mwh"]),
				committed_action=str(row["committed_action"]),
				committed_power_mw=float(row["committed_power_mw"]),
				rank_by_regret=int(row["rank_by_regret"]),
				evaluation_payload=_mapping_row_value(row["evaluation_payload"]),
			)
			for row in rows
		],
	)


def _to_real_data_benchmark_response(
	*,
	tenant_id: str,
	evaluation_frame: pl.DataFrame,
) -> RealDataBenchmarkResponse:
	if evaluation_frame.height == 0:
		raise HTTPException(status_code=404, detail="Real-data benchmark not found.")
	rows = [
		row
		for row in evaluation_frame.sort(["anchor_timestamp", "rank_by_regret", "forecast_model_name"]).iter_rows(named=True)
	]
	first_row = rows[0]
	regrets = [float(row["regret_uah"]) for row in rows]
	payloads = [_mapping_row_value(row["evaluation_payload"]) for row in rows]
	best_rows = [row for row in rows if int(row["rank_by_regret"]) == 1]
	best_model_name = None
	if best_rows:
		best_model_name = str(
			pl.DataFrame(best_rows)
			.group_by("forecast_model_name")
			.agg(pl.len().alias("wins"), pl.mean("regret_uah").alias("mean_regret_uah"))
			.sort(["wins", "mean_regret_uah"], descending=[True, False])
			.row(0, named=True)["forecast_model_name"]
		)
	return RealDataBenchmarkResponse(
		tenant_id=tenant_id,
		market_venue=str(first_row["market_venue"]),
		generated_at=_datetime_row_value(first_row["generated_at"], field_name="generated_at"),
		data_quality_tier=_benchmark_data_quality_tier(payloads),
		anchor_count=evaluation_frame.select("anchor_timestamp").n_unique(),
		model_count=evaluation_frame.select("forecast_model_name").n_unique(),
		best_model_name=best_model_name,
		mean_regret_uah=sum(regrets) / len(regrets),
		median_regret_uah=_median_float(regrets),
		rows=[
			RealDataBenchmarkPointResponse(
				evaluation_id=str(row["evaluation_id"]),
				anchor_timestamp=_datetime_row_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
				forecast_model_name=str(row["forecast_model_name"]),
				decision_value_uah=float(row["decision_value_uah"]),
				oracle_value_uah=float(row["oracle_value_uah"]),
				regret_uah=float(row["regret_uah"]),
				regret_ratio=float(row["regret_ratio"]),
				total_degradation_penalty_uah=float(row["total_degradation_penalty_uah"]),
				total_throughput_mwh=float(row["total_throughput_mwh"]),
				committed_action=str(row["committed_action"]),
				committed_power_mw=float(row["committed_power_mw"]),
				rank_by_regret=int(row["rank_by_regret"]),
				evaluation_payload=_mapping_row_value(row["evaluation_payload"]),
			)
			for row in rows
		],
	)


def _to_future_stack_preview_response(
	*,
	tenant_id: str,
	evaluation_frame: pl.DataFrame,
	forecast_observation_frame: pl.DataFrame | None = None,
) -> FutureStackPreviewResponse:
	store_frame = forecast_observation_frame if forecast_observation_frame is not None else pl.DataFrame()
	if evaluation_frame.height == 0 and store_frame.height == 0:
		raise HTTPException(status_code=404, detail="Future stack forecast rows not found.")
	model_metrics = _future_stack_model_metrics(evaluation_frame)
	store_series = _forecast_store_series(store_frame, metrics=model_metrics)
	benchmark_series: list[FutureForecastSeriesResponse] = []
	latest_anchor_frame = pl.DataFrame()
	if evaluation_frame.height:
		latest_anchor = evaluation_frame.select("anchor_timestamp").max().item()
		latest_anchor_frame = evaluation_frame.filter(pl.col("anchor_timestamp") == latest_anchor)
		benchmark_series = [
			_future_forecast_series(row=row, metrics=model_metrics)
			for row in latest_anchor_frame.sort(["forecast_model_name"]).iter_rows(named=True)
			if _is_future_stack_forecast_model(str(row["forecast_model_name"]))
		]
	series = _merge_future_forecast_series(store_series, benchmark_series)
	if not series:
		raise HTTPException(status_code=404, detail="NBEATSx/TFT future stack rows not found.")
	best_model_name = series[0].model_name if store_series else _future_stack_best_model_name(evaluation_frame)
	generated_at_value = _future_stack_generated_at(
		forecast_observation_frame=store_frame,
		latest_anchor_frame=latest_anchor_frame,
	)
	forecast_window_start, forecast_window_end = _future_stack_forecast_window(series)
	return FutureStackPreviewResponse(
		tenant_id=tenant_id,
		generated_at=_datetime_row_value(generated_at_value, field_name="generated_at"),
		forecast_window_start=forecast_window_start,
		forecast_window_end=forecast_window_end,
		backend_status=_future_stack_backend_status(),
		runtime_acceleration=_runtime_acceleration_status(),
		selected_forecast_model=best_model_name,
		claim_boundary=(
			"Operator production charts should be fed by NBEATSx/TFT forecasts with uncertainty and "
			"policy value gaps. Current official backends are used only when dependencies and smoke runs exist; "
			"compact/calibrated rows remain visible fallbacks."
		),
		forecast_series=series,
	)


def _future_forecast_series(
	*,
	row: dict[str, Any],
	metrics: dict[str, tuple[float | None, float | None]],
) -> FutureForecastSeriesResponse:
	model_name = str(row["forecast_model_name"])
	payload = _mapping_row_value(row["evaluation_payload"])
	horizon_rows = _payload_horizon_rows(payload)
	mean_regret_uah, win_rate = metrics.get(model_name, (None, None))
	points = [
		_future_forecast_point(model_name=model_name, horizon_row=horizon_row)
		for horizon_row in horizon_rows
	]
	return _future_forecast_series_response(
		model_name=model_name,
		model_family=_future_model_family(model_name),
		source_status=_future_model_source_status(model_name),
		uncertainty_kind=_future_uncertainty_kind(model_name, horizon_rows),
		mean_regret_uah=mean_regret_uah,
		win_rate=win_rate,
		points=points,
	)


def _forecast_store_series(
	forecast_observation_frame: pl.DataFrame,
	*,
	metrics: dict[str, tuple[float | None, float | None]],
) -> list[FutureForecastSeriesResponse]:
	if forecast_observation_frame.height == 0:
		return []
	series: list[FutureForecastSeriesResponse] = []
	for model_name in FUTURE_STACK_FORECAST_MODEL_NAMES:
		model_frame = (
			forecast_observation_frame
			.filter(pl.col("model_name") == model_name)
			.sort("forecast_timestamp")
		)
		if model_frame.height == 0:
			continue
		rows = list(model_frame.iter_rows(named=True))
		mean_regret_uah, win_rate = metrics.get(model_name, (None, None))
		horizon_payload_rows = [_forecast_store_horizon_payload(row) for row in rows]
		points = [
			_future_forecast_point(
				model_name=model_name,
				horizon_row=horizon_row,
			)
			for horizon_row in horizon_payload_rows
		]
		series.append(
			_future_forecast_series_response(
				model_name=model_name,
				model_family=_future_model_family(model_name),
				source_status=_future_model_source_status(model_name),
				uncertainty_kind=_future_uncertainty_kind(model_name, horizon_payload_rows),
				mean_regret_uah=mean_regret_uah,
				win_rate=win_rate,
				points=points,
			)
		)
	return series


def _future_forecast_series_response(
	*,
	model_name: str,
	model_family: str,
	source_status: str,
	uncertainty_kind: str,
	mean_regret_uah: float | None,
	win_rate: float | None,
	points: list[FutureForecastPointResponse],
) -> FutureForecastSeriesResponse:
	out_of_cap_rows = sum(
		1 for point in points if point.price_cap_status != "inside_dam_cap"
	)
	return FutureForecastSeriesResponse(
		model_name=model_name,
		model_family=model_family,
		source_status=source_status,
		uncertainty_kind=uncertainty_kind,
		mean_regret_uah=mean_regret_uah,
		win_rate=win_rate,
		out_of_dam_cap_rows=out_of_cap_rows,
		quality_boundary=_future_forecast_quality_boundary(
			source_status=source_status,
			out_of_cap_rows=out_of_cap_rows,
		),
		points=points,
	)


def _future_forecast_quality_boundary(*, source_status: str, out_of_cap_rows: int) -> str:
	if out_of_cap_rows:
		return "needs_calibration_before_value_claim"
	if source_status == "official":
		return "smoke_values_inside_dam_cap_not_value_claim"
	return "inside_dam_cap_not_value_claim"


def _forecast_store_horizon_payload(row: dict[str, Any]) -> dict[str, Any]:
	payload = _json_mapping_value(row.get("prediction_payload"))
	forecast_timestamp = row["forecast_timestamp"]
	forecast_price = _optional_float(
		payload.get("predicted_price_uah_mwh", row.get("predicted_price_uah_mwh"))
	) or 0.0
	return {
		"step_index": int(payload.get("step_index", 0)),
		"interval_start": payload.get("forecast_timestamp", forecast_timestamp),
		"forecast_price_uah_mwh": forecast_price,
		"predicted_price_uah_mwh": forecast_price,
		"predicted_price_p10_uah_mwh": payload.get("predicted_price_p10_uah_mwh"),
		"predicted_price_p50_uah_mwh": payload.get("predicted_price_p50_uah_mwh", forecast_price),
		"predicted_price_p90_uah_mwh": payload.get("predicted_price_p90_uah_mwh"),
		"net_power_mw": payload.get("net_power_mw"),
		"value_gap_uah": payload.get("value_gap_uah"),
	}


def _merge_future_forecast_series(
	primary_series: list[FutureForecastSeriesResponse],
	fallback_series: list[FutureForecastSeriesResponse],
) -> list[FutureForecastSeriesResponse]:
	merged: list[FutureForecastSeriesResponse] = []
	seen_model_names: set[str] = set()
	for series in [*primary_series, *fallback_series]:
		if series.model_name in seen_model_names:
			continue
		seen_model_names.add(series.model_name)
		merged.append(series)
	return merged


def _future_stack_forecast_window(
	series: list[FutureForecastSeriesResponse],
) -> tuple[datetime | None, datetime | None]:
	timestamps = [
		point.interval_start
		for forecast_series in series
		for point in forecast_series.points
	]
	if not timestamps:
		return None, None
	return min(timestamps), max(timestamps)


def _future_stack_generated_at(
	*,
	forecast_observation_frame: pl.DataFrame,
	latest_anchor_frame: pl.DataFrame,
) -> datetime:
	if forecast_observation_frame.height and "generated_at" in forecast_observation_frame.columns:
		value = forecast_observation_frame.select("generated_at").max().item()
		return _datetime_row_value(value, field_name="generated_at")
	if latest_anchor_frame.height:
		value = latest_anchor_frame.select("generated_at").max().item()
		return _datetime_row_value(value, field_name="generated_at")
	return datetime.now(UTC)


def _future_forecast_point(
	*,
	model_name: str,
	horizon_row: dict[str, Any],
) -> FutureForecastPointResponse:
	forecast_price = _optional_float(
		horizon_row.get("forecast_price_uah_mwh", horizon_row.get("predicted_price_uah_mwh"))
	)
	if forecast_price is None:
		forecast_price = 0.0
	p10_value = _optional_float(horizon_row.get("predicted_price_p10_uah_mwh"))
	p50_value = _optional_float(horizon_row.get("predicted_price_p50_uah_mwh")) or forecast_price
	p90_value = _optional_float(horizon_row.get("predicted_price_p90_uah_mwh"))
	if _future_model_family(model_name) == "TFT" and (p10_value is None or p90_value is None):
		band_width = max(25.0, abs(p50_value) * 0.08)
		p10_value = p50_value - band_width
		p90_value = p50_value + band_width
	return FutureForecastPointResponse(
		step_index=int(horizon_row.get("step_index", 0)),
		interval_start=_datetime_payload_value(horizon_row["interval_start"], field_name="interval_start"),
		forecast_price_uah_mwh=forecast_price,
		actual_price_uah_mwh=_optional_float(horizon_row.get("actual_price_uah_mwh")),
		p10_price_uah_mwh=p10_value,
		p50_price_uah_mwh=p50_value,
		p90_price_uah_mwh=p90_value,
		net_power_mw=_optional_float(horizon_row.get("net_power_mw")),
		value_gap_uah=_optional_float(horizon_row.get("value_gap_uah")),
		price_cap_status=_future_forecast_price_cap_status(forecast_price),
	)


def _future_forecast_price_cap_status(forecast_price_uah_mwh: float) -> str:
	if forecast_price_uah_mwh < FUTURE_STACK_DAM_PRICE_CAP_MIN_UAH_MWH:
		return "below_dam_cap"
	if forecast_price_uah_mwh > FUTURE_STACK_DAM_PRICE_CAP_MAX_UAH_MWH:
		return "above_dam_cap"
	return "inside_dam_cap"


def _future_stack_model_metrics(evaluation_frame: pl.DataFrame) -> dict[str, tuple[float | None, float | None]]:
	if evaluation_frame.height == 0:
		return {}
	anchor_count = evaluation_frame.select("anchor_timestamp").n_unique()
	summary_frame = (
		evaluation_frame
		.filter(pl.col("forecast_model_name").map_elements(_is_future_stack_forecast_model, return_dtype=pl.Boolean))
		.group_by("forecast_model_name")
		.agg(
			[
				pl.mean("regret_uah").alias("mean_regret_uah"),
				(pl.col("rank_by_regret") == 1).sum().alias("wins"),
			]
		)
	)
	return {
		str(row["forecast_model_name"]): (
			float(row["mean_regret_uah"]),
			float(row["wins"]) / anchor_count if anchor_count else None,
		)
		for row in summary_frame.iter_rows(named=True)
	}


def _future_stack_best_model_name(evaluation_frame: pl.DataFrame) -> str | None:
	if evaluation_frame.height == 0:
		return None
	summary_frame = (
		evaluation_frame
		.filter(pl.col("forecast_model_name").map_elements(_is_future_stack_forecast_model, return_dtype=pl.Boolean))
		.group_by("forecast_model_name")
		.agg(pl.mean("regret_uah").alias("mean_regret_uah"))
		.sort("mean_regret_uah")
	)
	if summary_frame.height == 0:
		return None
	return str(summary_frame.row(0, named=True)["forecast_model_name"])


def _payload_horizon_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
	horizon = payload.get("horizon")
	if not isinstance(horizon, list):
		return []
	return [row for row in horizon if isinstance(row, dict)]


def _is_future_stack_forecast_model(model_name: str) -> bool:
	lower_name = model_name.lower()
	return "nbeatsx" in lower_name or "tft" in lower_name


def _future_model_family(model_name: str) -> str:
	lower_name = model_name.lower()
	if "nbeatsx" in lower_name:
		return "NBEATSx"
	if "tft" in lower_name:
		return "TFT"
	return "forecast"


def _future_model_source_status(model_name: str) -> str:
	lower_name = model_name.lower()
	if "official" in lower_name:
		return "official"
	if "calibrated" in lower_name or "horizon_regret_weighted" in lower_name:
		return "calibrated"
	return "compact"


def _future_uncertainty_kind(model_name: str, horizon_rows: list[dict[str, Any]]) -> str:
	if any("predicted_price_p10_uah_mwh" in row and "predicted_price_p90_uah_mwh" in row for row in horizon_rows):
		return "quantile"
	if _future_model_family(model_name) == "TFT":
		return "quantile_proxy"
	if _future_model_family(model_name) == "NBEATSx":
		return "trend_exogenous_proxy"
	return "point"


def _future_stack_backend_status() -> dict[str, str]:
	return {
		"neuralforecast": _dependency_status("neuralforecast"),
		"pytorch_forecasting": _dependency_status("pytorch_forecasting"),
		"lightning": _dependency_status("lightning"),
	}


def _runtime_acceleration_status() -> RuntimeAccelerationResponse:
	try:
		import torch
	except ModuleNotFoundError:
		return RuntimeAccelerationResponse(
			backend="torch unavailable",
			device_type="cpu",
			device_name="CPU fallback",
			gpu_available=False,
			recommended_scope="install torch before official SOTA forecast/DT runs",
		)

	torch_version = str(getattr(torch, "__version__", "unknown"))
	cuda_available = bool(torch.cuda.is_available())
	if cuda_available:
		device_name = str(torch.cuda.get_device_name(0))
		cuda_version = str(getattr(torch.version, "cuda", None) or "")
		return RuntimeAccelerationResponse(
			backend=f"torch {torch_version}",
			device_type="cuda",
			device_name=device_name,
			gpu_available=True,
			cuda_version=cuda_version or None,
			recommended_scope="use GPU for official NBEATSx/TFT training and DT sweeps",
		)
	mps_backend = getattr(getattr(torch, "backends", None), "mps", None)
	mps_available = bool(mps_backend is not None and mps_backend.is_available())
	if mps_available:
		return RuntimeAccelerationResponse(
			backend=f"torch {torch_version}",
			device_type="mps",
			device_name="Apple Metal Performance Shaders",
			gpu_available=True,
			recommended_scope="use MPS for smoke-sized official forecasts; verify numerical parity on CPU",
		)
	return RuntimeAccelerationResponse(
		backend=f"torch {torch_version}",
		device_type="cpu",
		device_name="CPU only",
		gpu_available=False,
		cuda_version=str(getattr(torch.version, "cuda", None) or "") or None,
		recommended_scope="keep official NBEATSx/TFT and DT runs small; GPU will help only after CUDA torch is installed",
	)


def _dependency_status(module_name: str) -> str:
	try:
		__import__(module_name)
	except ModuleNotFoundError:
		return "dependency_missing"
	return "available"


def _to_forecast_dispatch_sensitivity_response(
	*,
	tenant_id: str,
	evaluation_frame: pl.DataFrame,
) -> ForecastDispatchSensitivityResponse:
	if evaluation_frame.height == 0:
		raise HTTPException(status_code=404, detail="Forecast-dispatch sensitivity not found.")
	sensitivity_frame = build_forecast_dispatch_sensitivity_frame(evaluation_frame)
	if sensitivity_frame.height == 0:
		raise HTTPException(status_code=404, detail="Forecast-dispatch sensitivity not found.")
	rows = [
		row
		for row in sensitivity_frame.sort(
			["anchor_timestamp", "rank_by_regret", "forecast_model_name"]
		).iter_rows(named=True)
	]
	first_row = rows[0]
	return ForecastDispatchSensitivityResponse(
		tenant_id=tenant_id,
		market_venue=str(first_row["market_venue"]),
		generated_at=_datetime_row_value(first_row["generated_at"], field_name="generated_at"),
		source_strategy_kind=str(first_row["strategy_kind"]),
		anchor_count=sensitivity_frame.select("anchor_timestamp").n_unique(),
		model_count=sensitivity_frame.select("forecast_model_name").n_unique(),
		row_count=sensitivity_frame.height,
		bucket_summary=_forecast_dispatch_sensitivity_bucket_summary(sensitivity_frame),
		rows=[
			ForecastDispatchSensitivityPointResponse(
				diagnostic_id=str(row["diagnostic_id"]),
				evaluation_id=str(row["evaluation_id"]),
				anchor_timestamp=_datetime_row_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
				forecast_model_name=str(row["forecast_model_name"]),
				diagnostic_bucket=str(row["diagnostic_bucket"]),
				regret_uah=float(row["regret_uah"]),
				regret_ratio=float(row["regret_ratio"]),
				forecast_mae_uah_mwh=float(row["forecast_mae_uah_mwh"]),
				forecast_rmse_uah_mwh=float(row["forecast_rmse_uah_mwh"]),
				mean_forecast_error_uah_mwh=float(row["mean_forecast_error_uah_mwh"]),
				forecast_dispatch_spread_uah_mwh=float(row["forecast_dispatch_spread_uah_mwh"]),
				realized_dispatch_spread_uah_mwh=float(row["realized_dispatch_spread_uah_mwh"]),
				dispatch_spread_error_uah_mwh=float(row["dispatch_spread_error_uah_mwh"]),
				total_degradation_penalty_uah=float(row["total_degradation_penalty_uah"]),
				total_throughput_mwh=float(row["total_throughput_mwh"]),
				charge_energy_mwh=float(row["charge_energy_mwh"]),
				discharge_energy_mwh=float(row["discharge_energy_mwh"]),
				committed_action=str(row["committed_action"]),
				committed_power_mw=float(row["committed_power_mw"]),
				rank_by_regret=int(row["rank_by_regret"]),
				data_quality_tier=str(row["data_quality_tier"]),
			)
			for row in rows
		],
	)


def _to_dfl_relaxed_pilot_response(
	*,
	tenant_id: str,
	relaxed_pilot_frame: pl.DataFrame,
) -> DflRelaxedPilotResponse:
	if relaxed_pilot_frame.height == 0:
		raise HTTPException(status_code=404, detail="Relaxed DFL pilot rows not found.")
	rows = [
		row
		for row in relaxed_pilot_frame.sort(["anchor_timestamp", "forecast_model_name"]).iter_rows(named=True)
	]
	regrets = [float(row["relaxed_regret_uah"]) for row in rows]
	return DflRelaxedPilotResponse(
		tenant_id=tenant_id,
		row_count=relaxed_pilot_frame.height,
		mean_relaxed_regret_uah=sum(regrets) / len(regrets),
		academic_scope=str(rows[0]["academic_scope"]),
		rows=[
			DflRelaxedPilotPointResponse(
				pilot_name=str(row["pilot_name"]),
				evaluation_id=str(row["evaluation_id"]),
				anchor_timestamp=_datetime_row_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
				forecast_model_name=str(row["forecast_model_name"]),
				horizon_hours=int(row["horizon_hours"]),
				relaxed_realized_value_uah=float(row["relaxed_realized_value_uah"]),
				relaxed_oracle_value_uah=float(row["relaxed_oracle_value_uah"]),
				relaxed_regret_uah=float(row["relaxed_regret_uah"]),
				first_charge_mw=float(row["first_charge_mw"]),
				first_discharge_mw=float(row["first_discharge_mw"]),
				academic_scope=str(row["academic_scope"]),
			)
			for row in rows
		],
	)


def _to_dfl_schedule_value_production_gate_response(
	*,
	gate_frame: pl.DataFrame,
) -> DflScheduleValueProductionGateResponse:
	if gate_frame.height == 0:
		raise HTTPException(status_code=404, detail="DFL schedule/value production gate rows not found.")
	sorted_gate_frame = gate_frame.sort("source_model_name")
	rows = [
		row
		for row in sorted_gate_frame.iter_rows(named=True)
	]
	promotion_summary = summarize_offline_strategy_promotion(sorted_gate_frame)
	first_row = rows[0]
	return DflScheduleValueProductionGateResponse(
		generated_at=_datetime_row_value(first_row["generated_at"], field_name="generated_at"),
		row_count=gate_frame.height,
		production_promote_count=int(promotion_summary["production_promote_count"]),
		promoted_source_model_names=list(promotion_summary["promoted_source_model_names"]),
		fallback_strategy=STRICT_DEFAULT_FALLBACK,
		market_execution_enabled=bool(promotion_summary["market_execution_enabled"]),
		claim_scope=DFL_SCHEDULE_VALUE_PRODUCTION_GATE_CLAIM_SCOPE,
		claim_boundary=str(promotion_summary["claim_boundary"]),
		academic_scope=offline_strategy_promotion_academic_scope(str(first_row["academic_scope"])),
		rows=[
			DflScheduleValueProductionGatePointResponse(
				source_model_name=str(row["source_model_name"]),
				tenant_count=int(row["tenant_count"]),
				latest_validation_tenant_anchor_count=int(row["latest_validation_tenant_anchor_count"]),
				latest_strict_mean_regret_uah=float(row["latest_strict_mean_regret_uah"]),
				latest_selected_mean_regret_uah=float(row["latest_selected_mean_regret_uah"]),
				latest_strict_median_regret_uah=float(row["latest_strict_median_regret_uah"]),
				latest_selected_median_regret_uah=float(row["latest_selected_median_regret_uah"]),
				latest_mean_regret_improvement_ratio_vs_strict=float(
					row["latest_mean_regret_improvement_ratio_vs_strict"]
				),
				rolling_window_count=int(row["rolling_window_count"]),
				rolling_strict_pass_window_count=int(row["rolling_strict_pass_window_count"]),
				robust_research_challenger=bool(row["robust_research_challenger"]),
				production_promote=bool(row["production_promote"]),
				promotion_blocker=str(row["promotion_blocker"]),
				allowed_challenger=str(row["allowed_challenger"]),
				fallback_strategy=str(row["fallback_strategy"]),
				market_execution_enabled=bool(row["market_execution_enabled"]),
				not_full_dfl=bool(row["not_full_dfl"]),
				not_market_execution=bool(row["not_market_execution"]),
			)
			for row in rows
		],
	)


def _to_decision_transformer_trajectory_response(
	*,
	tenant_id: str,
	trajectory_frame: pl.DataFrame,
) -> DecisionTransformerTrajectoryResponse:
	if trajectory_frame.height == 0:
		raise HTTPException(status_code=404, detail="Decision Transformer trajectory rows not found.")
	rows = [
		row
		for row in trajectory_frame.sort(["interval_start", "episode_id", "step_index"]).iter_rows(named=True)
	]
	return DecisionTransformerTrajectoryResponse(
		tenant_id=tenant_id,
		row_count=trajectory_frame.height,
		episode_count=trajectory_frame.select("episode_id").n_unique(),
		academic_scope=str(rows[0]["academic_scope"]),
		rows=[
			DecisionTransformerTrajectoryPointResponse(
				episode_id=str(row["episode_id"]),
				market_venue=str(row["market_venue"]),
				scenario_index=int(row["scenario_index"]),
				step_index=int(row["step_index"]),
				interval_start=_datetime_row_value(row["interval_start"], field_name="interval_start"),
				state_soc_before=float(row["state_soc_before"]),
				state_soc_after=float(row["state_soc_after"]),
				state_soh=float(row["state_soh"]),
				state_market_price_uah_mwh=float(row["state_market_price_uah_mwh"]),
				action_charge_mw=float(row["action_charge_mw"]),
				action_discharge_mw=float(row["action_discharge_mw"]),
				reward_uah=float(row["reward_uah"]),
				return_to_go_uah=float(row["return_to_go_uah"]),
				degradation_penalty_uah=float(row["degradation_penalty_uah"]),
				baseline_value_uah=float(row["baseline_value_uah"]),
				oracle_value_uah=float(row["oracle_value_uah"]),
				regret_uah=float(row["regret_uah"]),
				academic_scope=str(row["academic_scope"]),
			)
			for row in rows
		],
	)


def _to_decision_policy_preview_response(
	*,
	tenant_id: str,
	policy_preview_frame: pl.DataFrame,
) -> DecisionPolicyPreviewResponse:
	if policy_preview_frame.height == 0:
		raise HTTPException(status_code=404, detail="Decision Transformer policy preview rows not found.")
	rows = [
		row
		for row in policy_preview_frame.sort(["interval_start", "episode_id", "step_index"]).iter_rows(named=True)
	]
	constraint_violation_count = sum(1 for row in rows if bool(row["constraint_violation"]))
	forecast_context_summary = _policy_forecast_context_summary(rows)
	return DecisionPolicyPreviewResponse(
		tenant_id=tenant_id,
		row_count=policy_preview_frame.height,
		policy_run_id=str(rows[0]["policy_run_id"]),
		created_at=_datetime_row_value(rows[0]["created_at"], field_name="created_at"),
		policy_readiness=str(rows[0]["readiness_status"]),
		live_policy_claim=False,
		market_execution_enabled=False,
		constraint_violation_count=constraint_violation_count,
		mean_value_gap_uah=float(policy_preview_frame.select("value_gap_uah").mean().item()),
		total_value_vs_hold_uah=float(policy_preview_frame.select("value_vs_hold_uah").sum().item()),
		forecast_context_source=forecast_context_summary["source"],
		forecast_context_row_count=int(forecast_context_summary["row_count"]),
		forecast_context_coverage_ratio=float(forecast_context_summary["coverage_ratio"]),
		forecast_context_warning=forecast_context_summary["warning"],
		policy_state_features=[
			"SOC",
			"SOH",
			"market price",
			"NBEATSx forecast",
			"TFT forecast",
			"forecast uncertainty",
			"forecast spread",
			"time of day",
			"degradation penalty",
			"return target",
			"previous battery action",
		],
		policy_value_interpretation=(
			"value_gap = oracle_value_uah - expected_policy_value_uah after deterministic projection"
		),
		operator_boundary="preview_only_requires_gatekeeper_and_operator_review",
		academic_scope=str(rows[0]["academic_scope"]),
		rows=[
			DecisionPolicyPreviewPointResponse(
				policy_run_id=str(row["policy_run_id"]),
				created_at=_datetime_row_value(row["created_at"], field_name="created_at"),
				episode_id=str(row["episode_id"]),
				market_venue=str(row["market_venue"]),
				scenario_index=int(row["scenario_index"]),
				step_index=int(row["step_index"]),
				interval_start=_datetime_row_value(row["interval_start"], field_name="interval_start"),
				state_market_price_uah_mwh=float(row["state_market_price_uah_mwh"]),
				state_nbeatsx_forecast_uah_mwh=_optional_float(row.get("state_nbeatsx_forecast_uah_mwh")),
				state_tft_forecast_uah_mwh=_optional_float(row.get("state_tft_forecast_uah_mwh")),
				state_forecast_uncertainty_uah_mwh=_optional_float(row.get("state_forecast_uncertainty_uah_mwh")),
				state_forecast_spread_uah_mwh=_optional_float(row.get("state_forecast_spread_uah_mwh")),
				projected_soc_before=float(row["projected_soc_before"]),
				projected_soc_after=float(row["projected_soc_after"]),
				raw_charge_mw=float(row["raw_charge_mw"]),
				raw_discharge_mw=float(row["raw_discharge_mw"]),
				projected_charge_mw=float(row["projected_charge_mw"]),
				projected_discharge_mw=float(row["projected_discharge_mw"]),
				projected_net_power_mw=float(row["projected_net_power_mw"]),
				projected_action_label=_projected_action_label(float(row["projected_net_power_mw"])),
				projection_status=_projection_status(row),
				projection_adjustment_mw=_projection_adjustment_mw(row),
				expected_policy_value_uah=float(row["expected_policy_value_uah"]),
				hold_value_uah=float(row["hold_value_uah"]),
				value_vs_hold_uah=float(row["value_vs_hold_uah"]),
				oracle_value_uah=float(row["oracle_value_uah"]),
				value_gap_uah=float(row["value_gap_uah"]),
				value_gap_ratio=_value_gap_ratio(row),
				constraint_violation=bool(row["constraint_violation"]),
				gatekeeper_status=str(row["gatekeeper_status"]),
				inference_latency_ms=float(row["inference_latency_ms"]),
				policy_mode=str(row["policy_mode"]),
				readiness_status=str(row["readiness_status"]),
				model_name=str(row["model_name"]),
				academic_scope=str(row["academic_scope"]),
			)
			for row in rows
		],
	)


def _policy_forecast_context_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
	row_count = len(rows)
	if row_count == 0:
		return {
			"source": "missing_policy_rows",
			"row_count": 0,
			"coverage_ratio": 0.0,
			"warning": "no_policy_preview_rows",
		}
	forecast_context_row_count = sum(1 for row in rows if _row_has_policy_forecast_context(row))
	coverage_ratio = forecast_context_row_count / row_count
	if forecast_context_row_count == row_count:
		source = "nbeatsx_tft_forecast_context"
		warning = None
	elif forecast_context_row_count == 0:
		source = "market_price_fallback"
		warning = "policy_rows_use_market_price_fallback"
	else:
		source = "mixed_forecast_context"
		warning = "some_policy_rows_use_market_price_fallback"
	return {
		"source": source,
		"row_count": forecast_context_row_count,
		"coverage_ratio": coverage_ratio,
		"warning": warning,
	}


def _row_has_policy_forecast_context(row: dict[str, Any]) -> bool:
	return (
		row.get("state_nbeatsx_forecast_uah_mwh") is not None
		and row.get("state_tft_forecast_uah_mwh") is not None
	)


def _projected_action_label(projected_net_power_mw: float) -> str:
	if projected_net_power_mw > 1e-9:
		return "discharge"
	if projected_net_power_mw < -1e-9:
		return "charge"
	return "hold"


def _projection_status(row: dict[str, Any]) -> str:
	if bool(row["constraint_violation"]):
		return "blocked_by_gatekeeper"
	if _projection_adjustment_mw(row) > 1e-9:
		return "projected_by_safety_layer"
	return "accepted_without_projection"


def _projection_adjustment_mw(row: dict[str, Any]) -> float:
	return abs(float(row["raw_charge_mw"]) - float(row["projected_charge_mw"])) + abs(
		float(row["raw_discharge_mw"]) - float(row["projected_discharge_mw"])
	)


def _value_gap_ratio(row: dict[str, Any]) -> float | None:
	oracle_value = float(row["oracle_value_uah"])
	if abs(oracle_value) <= 1e-9:
		return None
	return max(0.0, float(row["value_gap_uah"]) / oracle_value)


def _to_simulated_live_trading_response(
	*,
	tenant_id: str,
	live_trading_frame: pl.DataFrame,
) -> SimulatedLiveTradingResponse:
	if live_trading_frame.height == 0:
		raise HTTPException(status_code=404, detail="Simulated live-trading rows not found.")
	rows = [
		row
		for row in live_trading_frame.sort(["interval_start", "episode_id", "step_index"]).iter_rows(named=True)
	]
	return SimulatedLiveTradingResponse(
		tenant_id=tenant_id,
		row_count=live_trading_frame.height,
		simulated_only=all(str(row["paper_trade_provenance"]) == "simulated" for row in rows),
		rows=[
			SimulatedLiveTradingPointResponse(
				episode_id=str(row["episode_id"]),
				interval_start=_datetime_row_value(row["interval_start"], field_name="interval_start"),
				step_index=int(row["step_index"]),
				state_soc_before=float(row["state_soc_before"]),
				state_soc_after=float(row["state_soc_after"]),
				proposed_trade_side=str(row["proposed_trade_side"]),
				proposed_quantity_mw=float(row["proposed_quantity_mw"]),
				feasible_net_power_mw=float(row["feasible_net_power_mw"]),
				market_price_uah_mwh=float(row["market_price_uah_mwh"]),
				reward_uah=float(row["reward_uah"]),
				gatekeeper_status=str(row["gatekeeper_status"]),
				paper_trade_provenance=str(row["paper_trade_provenance"]),
				settlement_id=None if row["settlement_id"] is None else str(row["settlement_id"]),
				live_mode_warning=str(row["live_mode_warning"]),
			)
			for row in rows
		],
	)


def _forecast_dispatch_sensitivity_bucket_summary(
	sensitivity_frame: pl.DataFrame,
) -> list[ForecastDispatchSensitivityBucketResponse]:
	summary_frame = (
		sensitivity_frame
		.group_by("diagnostic_bucket")
		.agg(
			[
				pl.len().alias("rows"),
				pl.mean("regret_uah").alias("mean_regret_uah"),
				pl.mean("forecast_mae_uah_mwh").alias("mean_forecast_mae_uah_mwh"),
				pl.mean("dispatch_spread_error_uah_mwh").alias(
					"mean_dispatch_spread_error_uah_mwh"
				),
			]
		)
		.sort("diagnostic_bucket")
	)
	return [
		ForecastDispatchSensitivityBucketResponse(
			diagnostic_bucket=str(row["diagnostic_bucket"]),
			rows=int(row["rows"]),
			mean_regret_uah=float(row["mean_regret_uah"]),
			mean_forecast_mae_uah_mwh=float(row["mean_forecast_mae_uah_mwh"]),
			mean_dispatch_spread_error_uah_mwh=float(row["mean_dispatch_spread_error_uah_mwh"]),
		)
		for row in summary_frame.iter_rows(named=True)
	]


def _benchmark_data_quality_tier(payloads: list[dict[str, Any]]) -> str:
	tiers = {str(payload.get("data_quality_tier", "demo_grade")) for payload in payloads}
	if tiers == {"thesis_grade"}:
		return "thesis_grade"
	return "demo_grade"


def _median_float(values: list[float]) -> float:
	sorted_values = sorted(values)
	midpoint = len(sorted_values) // 2
	if len(sorted_values) % 2 == 1:
		return sorted_values[midpoint]
	return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2.0


def _datetime_row_value(value: Any, *, field_name: str) -> datetime:
	if isinstance(value, datetime):
		return value
	raise ValueError(f"{field_name} must be a datetime value.")


def _datetime_payload_value(value: Any, *, field_name: str) -> datetime:
	if isinstance(value, datetime):
		return value
	if isinstance(value, str):
		return datetime.fromisoformat(value.replace("Z", "+00:00"))
	raise ValueError(f"{field_name} must be a datetime-compatible value.")


def _optional_float(value: Any) -> float | None:
	if value is None:
		return None
	return float(value)


def _mapping_row_value(value: Any) -> dict[str, Any]:
	if isinstance(value, dict):
		return value
	return {}


def _json_mapping_value(value: Any) -> dict[str, Any]:
	if isinstance(value, dict):
		return value
	if isinstance(value, str):
		try:
			decoded = json.loads(value)
		except json.JSONDecodeError:
			return {}
		if isinstance(decoded, dict):
			return decoded
	return {}


def _telemetry_freshness_payload(snapshot: BatteryStateHourlySnapshot) -> dict[str, Any]:
	return {
		"snapshot_hour": snapshot.snapshot_hour.isoformat(),
		"observation_count": snapshot.observation_count,
		"telemetry_freshness": snapshot.telemetry_freshness,
		"last_observed_at": snapshot.last_observed_at.isoformat(),
	}


def _resolve_starting_soc_for_baseline(
	*,
	tenant_id: str,
	battery_defaults: TenantBatteryDefaults,
) -> StartingSocResolution:
	latest_snapshot = get_battery_telemetry_store().get_latest_hourly_snapshot(tenant_id=tenant_id)
	if latest_snapshot is not None and latest_snapshot.telemetry_freshness == "fresh":
		return StartingSocResolution(
			starting_soc_fraction=latest_snapshot.soc_close,
			source="telemetry_hourly",
			telemetry_freshness=_telemetry_freshness_payload(latest_snapshot),
		)
	if latest_snapshot is not None:
		return StartingSocResolution(
			starting_soc_fraction=battery_defaults.initial_soc_fraction,
			source="tenant_default",
			telemetry_freshness=_telemetry_freshness_payload(latest_snapshot),
		)
	return StartingSocResolution(
		starting_soc_fraction=battery_defaults.initial_soc_fraction,
		source="tenant_default",
		telemetry_freshness=None,
	)


def _clamp_soc_fraction(value: float, battery_metrics: BatteryPhysicalMetrics) -> float:
	return max(
		battery_metrics.soc_min_fraction,
		min(battery_metrics.soc_max_fraction, value),
	)


def _resolve_operator_soc(
	*,
	tenant_id: str,
	battery_defaults: TenantBatteryDefaults,
	load_frame: pl.DataFrame,
) -> OperatorSocResolution:
	battery_metrics = battery_defaults.metrics
	telemetry_store = get_battery_telemetry_store()
	latest_telemetry = telemetry_store.get_latest_battery_telemetry(tenant_id=tenant_id)
	if latest_telemetry is not None:
		return OperatorSocResolution(
			physical_soc_fraction=latest_telemetry.current_soc,
			starting_soc_fraction=_clamp_soc_fraction(latest_telemetry.current_soc, battery_metrics),
			source="telemetry_live",
			confidence="high",
			review_required=False,
			warnings=(),
		)

	latest_snapshot = telemetry_store.get_latest_hourly_snapshot(tenant_id=tenant_id)
	if latest_snapshot is not None and latest_snapshot.telemetry_freshness == "fresh":
		return OperatorSocResolution(
			physical_soc_fraction=latest_snapshot.soc_close,
			starting_soc_fraction=_clamp_soc_fraction(latest_snapshot.soc_close, battery_metrics),
			source="hourly_snapshot",
			confidence="medium",
			review_required=False,
			warnings=(),
		)
	if latest_snapshot is not None:
		first_load_power_mw = _first_load_btm_power_mw(load_frame)
		projected_soc = latest_snapshot.soc_close - (first_load_power_mw / battery_metrics.capacity_mwh)
		return OperatorSocResolution(
			physical_soc_fraction=latest_snapshot.soc_close,
			starting_soc_fraction=_clamp_soc_fraction(projected_soc, battery_metrics),
			source="telemetry_projected",
			confidence="low",
			review_required=True,
			warnings=("Stale telemetry; SOC projected from latest hourly snapshot plus tenant load/PV schedule.",),
		)
	return OperatorSocResolution(
		physical_soc_fraction=None,
		starting_soc_fraction=battery_defaults.initial_soc_fraction,
		source="tenant_default",
		confidence="low",
		review_required=True,
		warnings=("Telemetry unavailable; using tenant default SOC.",),
	)


def _first_load_btm_power_mw(load_frame: pl.DataFrame) -> float:
	if load_frame.height == 0:
		return 0.0
	return float(load_frame.sort("timestamp").select("btm_battery_power_mw").to_series().item(0))


def _operator_load_frame(
	*,
	tenant_id: str,
	anchor_timestamp: datetime,
) -> pl.DataFrame:
	schedule_frame = build_tenant_consumption_schedule_frame()
	load_frame = build_tenant_net_load_hourly_frame(
		schedule_frame,
		anchor_timestamp=anchor_timestamp,
		horizon_hours=24,
	)
	return load_frame.filter(pl.col("tenant_id") == tenant_id)


def _operator_v13_readiness() -> OperatorV13ReadinessResponse:
	packet_path = _operator_v13_packet_path()
	if not packet_path.exists():
		return _operator_v13_readiness_fallback(
			gate_status="missing_v13_acquisition_packet",
			source_packet_path=str(packet_path),
			top_priority_blocker="v13_acquisition_packet",
		)
	try:
		packet = json.loads(packet_path.read_text(encoding="utf-8"))
	except (OSError, json.JSONDecodeError):
		return _operator_v13_readiness_fallback(
			gate_status="unreadable_v13_acquisition_packet",
			source_packet_path=str(packet_path),
			top_priority_blocker="v13_acquisition_packet",
		)
	if not isinstance(packet, dict):
		return _operator_v13_readiness_fallback(
			gate_status="invalid_v13_acquisition_packet",
			source_packet_path=str(packet_path),
			top_priority_blocker="v13_acquisition_packet",
		)

	readiness = _mapping_value(packet.get("readiness_summary"))
	safe_switch = _mapping_value(packet.get("safe_switch_deficit_summary"))
	backlog = _mapping_value(packet.get("source_acquisition_backlog_summary"))
	preflight = _mapping_value(packet.get("acquisition_input_preflight_summary"))
	receipt_audit = _mapping_value(packet.get("receipt_source_audit_summary"))
	receipt_lead_audit = _mapping_value(packet.get("receipt_source_lead_audit_summary"))
	scmo_preflight = _mapping_value(packet.get("scmo_ws_security_preflight_summary"))
	safe_switch_targets = _mapping_value(packet.get("safe_switch_acquisition_target_summary"))
	claim_boundary = _mapping_value(packet.get("claim_boundary"))
	readiness_decisions = _string_list_value(readiness.get("readiness_decisions"))
	v13_candidate_generation_ready = bool(
		packet.get(
			"v13_candidate_generation_ready",
			readiness.get("v13_candidate_generation_ready", False),
		)
	)
	market_execution_enabled = bool(claim_boundary.get("market_execution_enabled", False))
	top_priority_blocker = str(backlog.get("top_priority_blocker", "unknown"))
	source_governance = _operator_source_governance_status(
		top_priority_blocker=top_priority_blocker,
		missing_required_inputs=_string_list_value(preflight.get("missing_required_inputs")),
		receipt_audit=receipt_audit,
		receipt_lead_audit=receipt_lead_audit,
		scmo_preflight=scmo_preflight,
	)
	dt_lava_ready = (
		v13_candidate_generation_ready
		and not bool(claim_boundary.get("dt_lava_still_gated", True))
		and not market_execution_enabled
	)
	return OperatorV13ReadinessResponse(
		gate_status=_operator_v13_gate_status(
			readiness_decisions=readiness_decisions,
			v13_candidate_generation_ready=v13_candidate_generation_ready,
			dt_lava_ready=dt_lava_ready,
		),
		v13_candidate_generation_ready=v13_candidate_generation_ready,
		dt_lava_ready=dt_lava_ready,
		ready_rows=_int_value(readiness.get("ready_rows")),
		readiness_rows=_int_value(readiness.get("readiness_rows")),
		missing_safe_switch_examples=_int_value(safe_switch.get("total_missing_examples")),
		missing_required_inputs=_string_list_value(preflight.get("missing_required_inputs")),
		top_priority_blocker=top_priority_blocker,
		receipt_source_audit_probe_count=_int_value(receipt_audit.get("probe_count")),
		receipt_source_audit_months_probed=_string_list_value(
			receipt_audit.get("months_probed")
		),
		receipt_source_audit_candidate_found=bool(
			receipt_audit.get("candidate_receipt_source_found", False)
		),
		receipt_source_audit_csv_generated=bool(
			receipt_audit.get("receipt_csv_generated", False)
		),
		receipt_source_audit_all_probes_insufficient=bool(
			receipt_audit.get("all_probes_insufficient_for_v13_receipts", False)
		),
		source_governance_status=source_governance["status"],
		source_governance_label=source_governance["label"],
		market_submission_receipt_gate_status=source_governance[
			"market_submission_receipt_gate_status"
		],
		scmo_credentials_required_for_diploma_mvp=False,
		scmo_credentials_required_for_market_submission_grade_receipts=bool(
			source_governance["scmo_credentials_required_for_market_submission_grade_receipts"]
		),
		safe_switch_target_tenant_source_count=_int_value(
			safe_switch_targets.get("target_tenant_source_count")
		),
		safe_switch_max_new_examples_required=_int_value(
			safe_switch_targets.get("max_new_prior_material_safe_switch_examples_required")
		),
		safe_switch_acquisition_targets=_operator_v13_safe_switch_targets(
			safe_switch_targets.get("target_rows")
		),
		market_execution_enabled=market_execution_enabled,
		boundary_doc=V13_GOAL_BOUNDARY_DOC,
		source_packet_path=str(packet_path),
	)


def _operator_v13_packet_path() -> Path:
	raw_path = os.getenv(V13_ACQUISITION_PACKET_JSON_ENV, "").strip()
	return Path(raw_path) if raw_path else V13_ACQUISITION_PACKET_JSON_DEFAULT


def _academic_mvp_packet_path() -> Path:
	raw_path = os.getenv(ACADEMIC_MVP_PACKET_JSON_ENV, "").strip()
	return Path(raw_path) if raw_path else ACADEMIC_MVP_PACKET_JSON_DEFAULT


def _academic_mvp_validation_packet_path(*, readiness_packet_path: Path) -> Path:
	raw_path = os.getenv(ACADEMIC_MVP_VALIDATION_JSON_ENV, "").strip()
	if raw_path:
		return Path(raw_path)
	return readiness_packet_path.with_name(ACADEMIC_MVP_VALIDATION_JSON_NAME)


def _academic_mvp_passport_gate(
	*,
	gate_passport: dict[str, Any],
	gate_name: str,
) -> dict[str, Any]:
	gate = gate_passport.get(gate_name)
	if not isinstance(gate, dict):
		raise HTTPException(
			status_code=500,
			detail=f"Credentialless academic MVP readiness packet missing gate: {gate_name}",
		)
	return gate


def _validate_academic_mvp_gate_passport(packet: dict[str, Any]) -> None:
	gate_passport = packet.get("gate_passport")
	if not isinstance(gate_passport, dict):
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP readiness packet must include gate_passport.",
		)
	if packet.get("academic_mvp_gate_passed") is not True:
		return
	for flag_name, expected_value in ACADEMIC_MVP_REQUIRED_FALSE_FLAGS.items():
		if packet.get(flag_name) is not expected_value:
			raise HTTPException(
				status_code=500,
				detail=(
					"Credentialless academic MVP readiness packet has invalid "
					f"{flag_name}: expected {expected_value}."
				),
			)
	for gate_name in sorted(ACADEMIC_MVP_REQUIRED_PASSPORT_GATES):
		gate = _academic_mvp_passport_gate(gate_passport=gate_passport, gate_name=gate_name)
		if gate.get("passed") is not True:
			raise HTTPException(
				status_code=500,
				detail=(
					"Credentialless academic MVP readiness packet required gate is not "
					f"passed: {gate_name}"
				),
			)
	for gate_name in sorted(ACADEMIC_MVP_NON_REQUIRED_PASSPORT_GATES):
		gate = _academic_mvp_passport_gate(gate_passport=gate_passport, gate_name=gate_name)
		if gate.get("required_for_academic_mvp") is not False:
			raise HTTPException(
				status_code=500,
				detail=(
					"Credentialless academic MVP readiness packet future/source gate must "
					f"be non-required for the academic MVP: {gate_name}"
				),
			)
		if gate.get("passed") is True:
			raise HTTPException(
				status_code=500,
				detail=(
					"Credentialless academic MVP readiness packet future/source gate "
					f"must remain unpassed for this credentialless scope: {gate_name}"
				),
			)


def _validate_academic_mvp_prototype_contract(packet: dict[str, Any]) -> None:
	prototype_contract = packet.get("prototype_contract")
	if not isinstance(prototype_contract, dict):
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP readiness packet must include prototype_contract.",
		)
	if prototype_contract.get("claim_scope") != (
		"credentialless_dfl_dt_prototype_contract_not_market_execution"
	):
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP readiness packet has invalid prototype_contract claim_scope.",
		)
	if prototype_contract.get("product_boundary") != (
		"dam_delivery_day_operator_recommendation_preview"
	):
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP readiness packet has invalid prototype_contract product_boundary.",
		)
	if prototype_contract.get("dt_action_target_contract") not in (
		ACADEMIC_MVP_ALLOWED_DT_ACTION_TARGETS
	):
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP readiness packet has invalid DT action target contract.",
		)
	if prototype_contract.get("v2_plus_role") != "teacher_comparator_fallback":
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP readiness packet must keep V2+ as teacher/comparator/fallback.",
		)
	if prototype_contract.get("raw_hourly_action_imitation") is not False:
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP readiness packet must not use raw hourly DT action imitation.",
		)
	evaluation_contract = prototype_contract.get("evaluation_contract")
	if not isinstance(evaluation_contract, dict):
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP readiness packet must include evaluation_contract.",
		)
	for required_flag in (
		"required_controls_present",
		"behavior_cloning_control_present",
		"deterministic_safety_projection_passed",
	):
		if evaluation_contract.get(required_flag) is not True:
			raise HTTPException(
				status_code=500,
				detail=(
					"Credentialless academic MVP readiness packet evaluation "
					f"contract flag is not true: {required_flag}"
				),
			)
	if prototype_contract.get("prototype_contract_gate_passed") is not True:
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP readiness packet prototype contract gate is not passed.",
		)


def _validate_academic_mvp_dt_research_shadow_gate(packet: dict[str, Any]) -> None:
	dt_research_shadow_gate = packet.get("dt_research_shadow_gate")
	if not isinstance(dt_research_shadow_gate, dict):
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP readiness packet must include dt_research_shadow_gate.",
		)
	if dt_research_shadow_gate.get("passed_for_academic_mvp") is not True:
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP readiness packet DT research-shadow gate is not passed.",
		)
	if dt_research_shadow_gate.get("split_strategy") != "chronological_delivery_timestamp":
		raise HTTPException(
			status_code=500,
			detail=(
				"Credentialless academic MVP readiness packet DT research-shadow gate "
				"must use chronological delivery-time splits."
			),
		)
	if dt_research_shadow_gate.get("chronological_split_passed") is not True:
		raise HTTPException(
			status_code=500,
			detail=(
				"Credentialless academic MVP readiness packet DT research-shadow "
				"chronological split did not pass."
			),
		)
	if _int_value(dt_research_shadow_gate.get("research_shadow_training_rows")) <= 0:
		raise HTTPException(
			status_code=500,
			detail=(
				"Credentialless academic MVP readiness packet DT research-shadow gate "
				"must expose research_shadow_training_rows > 0."
			),
		)
	if _int_value(dt_research_shadow_gate.get("promotable_v13_permitted_training_rows")) != 0:
		raise HTTPException(
			status_code=500,
			detail=(
				"Credentialless academic MVP readiness packet DT research-shadow gate "
				"must keep promotable_v13_permitted_training_rows=0."
			),
		)
	for flag_name in (
		"publication_receipt_verified",
		"source_publication_timestamp_available",
		"market_availability_claim",
		"dt_promotion_gate_passed",
		"market_execution_enabled",
	):
		if dt_research_shadow_gate.get(flag_name) is not False:
			raise HTTPException(
				status_code=500,
				detail=(
					"Credentialless academic MVP readiness packet DT research-shadow "
					f"flag must be false: {flag_name}"
				),
			)
	if dt_research_shadow_gate.get("research_shadow_not_promotable") is not True:
		raise HTTPException(
			status_code=500,
			detail=(
				"Credentialless academic MVP readiness packet DT research-shadow gate "
				"must be marked research_shadow_not_promotable=true."
			),
		)


def _academic_mvp_artifact_validation(
	*,
	readiness_packet_path: Path,
) -> tuple[dict[str, Any], Path]:
	validation_path = _academic_mvp_validation_packet_path(
		readiness_packet_path=readiness_packet_path,
	)
	if not validation_path.exists():
		raise HTTPException(
			status_code=500,
			detail=f"Credentialless academic MVP validation artifact not found: {validation_path}",
		)
	try:
		validation = json.loads(validation_path.read_text(encoding="utf-8"))
	except (OSError, json.JSONDecodeError) as error:
		raise HTTPException(
			status_code=500,
			detail=f"Credentialless academic MVP validation artifact is unreadable: {validation_path}",
		) from error
	if not isinstance(validation, dict):
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP validation artifact must be a JSON object.",
		)
	if _json_contains_market_execution_enabled_true(validation):
		raise HTTPException(
			status_code=500,
			detail=(
				"Credentialless academic MVP validation artifact must keep "
				"market_execution_enabled=false."
			),
		)
	if validation.get("claim_scope") != ACADEMIC_MVP_VALIDATION_CLAIM_SCOPE:
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP validation artifact has invalid claim_scope.",
		)
	if validation.get("passed") is not True:
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP validation artifact did not pass.",
		)
	if validation.get("failures") != []:
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP validation artifact reports failures.",
		)
	gate_results = validation.get("gate_results")
	if not isinstance(gate_results, dict):
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP validation artifact must include gate_results.",
		)
	for gate_name in sorted(ACADEMIC_MVP_REQUIRED_VALIDATION_GATES):
		gate = gate_results.get(gate_name)
		if not isinstance(gate, dict):
			raise HTTPException(
				status_code=500,
				detail=(
					"Credentialless academic MVP validation artifact missing gate result: "
					f"{gate_name}"
				),
			)
		if gate.get("passed") is not True:
			raise HTTPException(
				status_code=500,
				detail=(
					"Credentialless academic MVP validation artifact gate did not pass: "
					f"{gate_name}"
				),
			)
		if gate.get("failures") != []:
			raise HTTPException(
				status_code=500,
				detail=(
					"Credentialless academic MVP validation artifact gate reports failures: "
					f"{gate_name}"
				),
			)
	return validation, validation_path


def _academic_mvp_readiness_response() -> AcademicMvpReadinessResponse:
	packet_path = _academic_mvp_packet_path()
	if not packet_path.exists():
		raise HTTPException(
			status_code=404,
			detail=f"Credentialless academic MVP readiness packet not found: {packet_path}",
		)
	try:
		packet = json.loads(packet_path.read_text(encoding="utf-8"))
	except (OSError, json.JSONDecodeError) as error:
		raise HTTPException(
			status_code=500,
			detail=f"Credentialless academic MVP readiness packet is unreadable: {packet_path}",
		) from error
	if not isinstance(packet, dict):
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP readiness packet must be a JSON object.",
		)
	if _json_contains_market_execution_enabled_true(packet):
		raise HTTPException(
			status_code=500,
			detail="Credentialless academic MVP readiness packet must keep market_execution_enabled=false.",
		)
	_validate_academic_mvp_gate_passport(packet)
	_validate_academic_mvp_prototype_contract(packet)
	_validate_academic_mvp_dt_research_shadow_gate(packet)
	artifact_validation, validation_path = _academic_mvp_artifact_validation(
		readiness_packet_path=packet_path,
	)
	response_payload = dict(packet)
	response_payload["artifact_validation"] = artifact_validation
	response_payload["source_packet_path"] = str(packet_path)
	response_payload["artifact_validation_packet_path"] = str(validation_path)
	return AcademicMvpReadinessResponse(**response_payload)


def _operator_v13_readiness_fallback(
	*,
	gate_status: str,
	source_packet_path: str | None,
	top_priority_blocker: str,
) -> OperatorV13ReadinessResponse:
	return OperatorV13ReadinessResponse(
		gate_status=gate_status,
		v13_candidate_generation_ready=False,
		dt_lava_ready=False,
		ready_rows=0,
		readiness_rows=0,
		missing_safe_switch_examples=0,
		missing_required_inputs=[],
		top_priority_blocker=top_priority_blocker,
		receipt_source_audit_probe_count=0,
		receipt_source_audit_months_probed=[],
		receipt_source_audit_candidate_found=False,
		receipt_source_audit_csv_generated=False,
		receipt_source_audit_all_probes_insufficient=False,
		source_governance_status="v13_source_packet_unavailable",
		source_governance_label="V13 source packet unavailable",
		market_submission_receipt_gate_status="missing_v13_packet",
		scmo_credentials_required_for_diploma_mvp=False,
		scmo_credentials_required_for_market_submission_grade_receipts=False,
		safe_switch_target_tenant_source_count=0,
		safe_switch_max_new_examples_required=0,
		safe_switch_acquisition_targets=[],
		market_execution_enabled=False,
		boundary_doc=V13_GOAL_BOUNDARY_DOC,
		source_packet_path=source_packet_path,
	)


def _operator_source_governance_status(
	*,
	top_priority_blocker: str,
	missing_required_inputs: list[str],
	receipt_audit: dict[str, Any],
	receipt_lead_audit: dict[str, Any],
	scmo_preflight: dict[str, Any],
) -> dict[str, Any]:
	explicit_receipts_blocked = (
		"explicit_dam_publication_receipts" in top_priority_blocker
		or "oree_dam_publication_receipts_csv_path" in missing_required_inputs
	)
	auth_blocked_count = _int_value(receipt_lead_audit.get("auth_blocked_count"))
	credential_material_ready = bool(
		scmo_preflight.get("credential_material_ready", False)
	)
	candidate_found = bool(receipt_audit.get("candidate_receipt_source_found", False))
	receipt_csv_generated = bool(receipt_audit.get("receipt_csv_generated", False))
	if receipt_csv_generated and not explicit_receipts_blocked:
		return {
			"status": "market_submission_receipts_ready",
			"label": "market-submission receipts ready",
			"market_submission_receipt_gate_status": "ready",
			"scmo_credentials_required_for_market_submission_grade_receipts": False,
		}
	if explicit_receipts_blocked and (auth_blocked_count > 0 or not credential_material_ready):
		return {
			"status": "receipt_gated_for_market_submission",
			"label": "receipt-gated for market submission",
			"market_submission_receipt_gate_status": "blocked_external_access",
			"scmo_credentials_required_for_market_submission_grade_receipts": True,
		}
	if explicit_receipts_blocked:
		return {
			"status": "receipt_gated_for_market_submission",
			"label": "receipt-gated for market submission",
			"market_submission_receipt_gate_status": (
				"blocked_missing_explicit_receipts"
				if candidate_found
				else "blocked_no_candidate_receipt_source"
			),
			"scmo_credentials_required_for_market_submission_grade_receipts": False,
		}
	return {
		"status": "source_governance_ready_for_preview",
		"label": "source governance ready for preview",
		"market_submission_receipt_gate_status": "not_evaluated_preview_only",
		"scmo_credentials_required_for_market_submission_grade_receipts": False,
	}


def _operator_v13_safe_switch_targets(
	value: object,
) -> list[OperatorV13SafeSwitchTargetResponse]:
	if not isinstance(value, list):
		return []
	targets: list[OperatorV13SafeSwitchTargetResponse] = []
	for row in value:
		if not isinstance(row, dict):
			continue
		targets.append(
			OperatorV13SafeSwitchTargetResponse(
				acquisition_priority_rank=_int_value(row.get("acquisition_priority_rank")),
				tenant_id=str(row.get("tenant_id", "")),
				source_model_name=str(row.get("source_model_name", "")),
				current_prior_material_safe_switch_examples=_int_value(
					row.get("current_prior_material_safe_switch_examples")
				),
				required_prior_material_safe_switch_examples=_int_value(
					row.get("required_prior_material_safe_switch_examples")
				),
				target_new_prior_material_safe_switch_examples=_int_value(
					row.get("target_new_prior_material_safe_switch_examples")
				),
				required_evidence_kind=str(row.get("required_evidence_kind", "")),
				recommended_next_step=str(row.get("recommended_next_step", "")),
				target_is_precondition_only=bool(row.get("target_is_precondition_only", False)),
				market_execution_enabled=bool(row.get("market_execution_enabled", False)),
			)
		)
	return targets


def _operator_v13_gate_status(
	*,
	readiness_decisions: list[str],
	v13_candidate_generation_ready: bool,
	dt_lava_ready: bool,
) -> str:
	if v13_candidate_generation_ready and dt_lava_ready:
		return "v13_dt_lava_ready"
	if readiness_decisions:
		return readiness_decisions[0]
	return "data_acquisition_needed"


def _mapping_value(value: object) -> dict[str, Any]:
	return value if isinstance(value, dict) else {}


def _json_contains_market_execution_enabled_true(value: object) -> bool:
	return _json_contains_truthy_flag(value, flag_name="market_execution_enabled")


def _json_contains_truthy_flag(value: object, *, flag_name: str) -> bool:
	if isinstance(value, dict):
		for key, item in value.items():
			if key == flag_name and _artifact_truthy(item):
				return True
			if _json_contains_truthy_flag(item, flag_name=flag_name):
				return True
		return False
	if isinstance(value, list | tuple):
		return any(_json_contains_truthy_flag(item, flag_name=flag_name) for item in value)
	return False


def _string_list_value(value: object) -> list[str]:
	return [str(item) for item in value] if isinstance(value, list) else []


def _int_value(value: object) -> int:
	if value is None:
		return 0
	if isinstance(value, (int, float, str)):
		return int(value)
	raise TypeError(f"Cannot convert {type(value).__name__} to int.")


def _operator_strategy_options(
	*,
	tenant_id: str,
	v13_readiness: OperatorV13ReadinessResponse,
) -> list[OperatorStrategyOptionResponse]:
	benchmark_frame = get_strategy_evaluation_store().latest_real_data_benchmark_frame(tenant_id=tenant_id)
	metrics_by_model = _operator_strategy_metrics_by_model(benchmark_frame)
	forecast_store_cap_counts = _available_forecast_store_model_cap_counts()
	policy_preview_frame = get_simulated_trade_store().latest_decision_transformer_policy_preview_frame(
		tenant_id=tenant_id,
		limit=24,
	)
	policy_ready = _decision_policy_preview_is_ready(policy_preview_frame)
	dt_enabled = policy_ready and v13_readiness.dt_lava_ready and not v13_readiness.market_execution_enabled
	options = [
		_operator_strategy_option(
			strategy_id="strict_similar_day",
			label="Strict similar-day control",
			reason="control baseline",
			metrics_by_model=metrics_by_model,
		),
		OperatorStrategyOptionResponse(
			strategy_id=OFFLINE_V2_PLUS_OPERATOR_STRATEGY_ID,
			label=OFFLINE_V2_PLUS_LABEL,
			enabled=True,
			reason=(
				"frozen Offline Strategy Promotion comparator; preview uses a deterministic "
				"V2+ read-model adapter and remains market-execution disabled"
			),
			mean_regret_uah=OFFLINE_V2_PLUS_MEAN_REGRET_UAH,
			win_rate=OFFLINE_V2_PLUS_WIN_RATE,
		),
		_operator_strategy_option(
			strategy_id="tft_silver_v0",
			label="Compact TFT",
			reason="materialized benchmark candidate",
			metrics_by_model=metrics_by_model,
		),
		_operator_strategy_option(
			strategy_id="nbeatsx_silver_v0",
			label="Compact NBEATSx",
			reason="materialized benchmark candidate",
			metrics_by_model=metrics_by_model,
		),
		_operator_forecast_store_strategy_option(
			strategy_id="nbeatsx_official_v0",
			label="Official NBEATSx",
			model_cap_counts=forecast_store_cap_counts,
		),
		_operator_forecast_store_strategy_option(
			strategy_id="tft_official_v0",
			label="Official TFT",
			model_cap_counts=forecast_store_cap_counts,
		),
		_operator_strategy_option(
			strategy_id=CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND,
			label="Calibrated value-aware gate",
			reason="materialized ensemble gate",
			metrics_by_model=metrics_by_model,
		),
		_operator_strategy_option(
			strategy_id=RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND,
			label="Risk-adjusted value gate",
			reason="materialized ensemble gate",
			metrics_by_model=metrics_by_model,
		),
		OperatorStrategyOptionResponse(
			strategy_id="decision_transformer",
			label="Decision Transformer",
			enabled=dt_enabled,
			reason=_operator_dt_option_reason(
				policy_ready=policy_ready,
				v13_readiness=v13_readiness,
			),
			mean_regret_uah=_policy_mean_value_gap(policy_preview_frame) if dt_enabled else None,
			win_rate=1.0 if dt_enabled else None,
		),
	]
	if not metrics_by_model:
		options[0] = options[0].model_copy(update={"enabled": True, "mean_regret_uah": None, "win_rate": None})
	return options


def _operator_dt_option_reason(
	*,
	policy_ready: bool,
	v13_readiness: OperatorV13ReadinessResponse,
) -> str:
	if not policy_ready:
		return "offline policy preview missing or failed safety projection"
	if not v13_readiness.dt_lava_ready or v13_readiness.market_execution_enabled:
		return (
			"blocked by V13 acquisition/source-readiness gate: "
			f"{v13_readiness.gate_status}; top blocker {v13_readiness.top_priority_blocker}; "
			f"missing safe-switch examples {v13_readiness.missing_safe_switch_examples}"
		)
	return "ready offline preview; market execution disabled"


def _available_forecast_store_model_cap_counts() -> dict[str, int]:
	forecast_frame = get_forecast_store().latest_forecast_observation_frame(
		model_names=OFFICIAL_FORECAST_TO_LP_STRATEGY_IDS,
		limit_per_model=24,
	)
	if forecast_frame.height == 0:
		return {}
	cap_counts: dict[str, int] = {}
	for model_name in OFFICIAL_FORECAST_TO_LP_STRATEGY_IDS:
		model_frame = forecast_frame.filter(pl.col("model_name") == model_name)
		if model_frame.height == 0:
			continue
		out_of_cap_rows = 0
		for row in model_frame.iter_rows(named=True):
			payload = _forecast_store_horizon_payload(row)
			status = _future_forecast_price_cap_status(float(payload["forecast_price_uah_mwh"]))
			if status != "inside_dam_cap":
				out_of_cap_rows += 1
		cap_counts[model_name] = out_of_cap_rows
	return cap_counts


def _operator_forecast_store_strategy_option(
	*,
	strategy_id: str,
	label: str,
	model_cap_counts: dict[str, int],
) -> OperatorStrategyOptionResponse:
	out_of_cap_rows = model_cap_counts.get(strategy_id)
	if out_of_cap_rows is None:
		return OperatorStrategyOptionResponse(
			strategy_id=strategy_id,
			label=label,
			enabled=False,
			reason="official forecast rows not materialized",
		)
	if out_of_cap_rows:
		return OperatorStrategyOptionResponse(
			strategy_id=strategy_id,
			label=label,
			enabled=False,
			reason=f"official forecast rows need calibration: {out_of_cap_rows} out-of-cap rows",
		)
	return OperatorStrategyOptionResponse(
		strategy_id=strategy_id,
		label=label,
		enabled=True,
		reason="materialized forecast-store rows; values inside DAM caps",
	)


def _operator_strategy_metrics_by_model(benchmark_frame: pl.DataFrame) -> dict[str, tuple[float, float]]:
	if benchmark_frame.height == 0:
		return {}
	summary_frame = (
		benchmark_frame
		.group_by("forecast_model_name")
		.agg(
			[
				pl.mean("regret_uah").alias("mean_regret_uah"),
				(pl.col("rank_by_regret") == 1).mean().alias("win_rate"),
			]
		)
	)
	return {
		str(row["forecast_model_name"]): (float(row["mean_regret_uah"]), float(row["win_rate"]))
		for row in summary_frame.iter_rows(named=True)
	}


def _operator_strategy_option(
	*,
	strategy_id: str,
	label: str,
	reason: str,
	metrics_by_model: dict[str, tuple[float, float]],
) -> OperatorStrategyOptionResponse:
	metrics = metrics_by_model.get(strategy_id)
	if metrics is None and strategy_id == "strict_similar_day":
		return OperatorStrategyOptionResponse(
			strategy_id=strategy_id,
			label=label,
			enabled=True,
			reason=reason,
		)
	if metrics is None:
		return OperatorStrategyOptionResponse(
			strategy_id=strategy_id,
			label=label,
			enabled=False,
			reason="not materialized for this tenant",
		)
	mean_regret_uah, win_rate = metrics
	return OperatorStrategyOptionResponse(
		strategy_id=strategy_id,
		label=label,
		enabled=True,
		reason=reason,
		mean_regret_uah=mean_regret_uah,
		win_rate=win_rate,
	)


def _select_operator_strategy(
	*,
	requested_strategy_id: str,
	options: list[OperatorStrategyOptionResponse],
) -> tuple[str, str, tuple[str, ...]]:
	enabled_options = {option.strategy_id: option for option in options if option.enabled}
	requested_option = enabled_options.get(requested_strategy_id)
	if requested_option is not None:
		return requested_option.strategy_id, f"manual strategy: {requested_option.label}", ()
	return (
		"strict_similar_day",
		"fallback to strict similar-day control",
		(f"Requested strategy {requested_strategy_id} is unavailable; using strict similar-day control.",),
	)


def _to_operator_load_forecast_points(load_frame: pl.DataFrame) -> list[OperatorLoadForecastPointResponse]:
	return [
		OperatorLoadForecastPointResponse(
			timestamp=_datetime_row_value(row["timestamp"], field_name="timestamp"),
			load_mw=float(row["load_mw"]),
			pv_estimate_mw=float(row["pv_estimate_mw"]),
			net_load_mw=float(row["net_load_mw"]),
			btm_battery_power_mw=float(row["btm_battery_power_mw"]),
			source_kind=str(row["source_kind"]),
			weather_source_kind=str(row["weather_source_kind"]),
			reason_code=str(row["reason_code"]),
		)
		for row in load_frame.sort("timestamp").iter_rows(named=True)
	]


def _to_operator_soc_projection_points(
	*,
	load_frame: pl.DataFrame,
	solve_result: BaselineSolveResult,
	soc_resolution: OperatorSocResolution,
	battery_metrics: BatteryPhysicalMetrics,
) -> list[OperatorSocProjectionPointResponse]:
	load_by_timestamp = {
		_datetime_row_value(row["timestamp"], field_name="timestamp"): float(row["btm_battery_power_mw"])
		for row in load_frame.iter_rows(named=True)
	}
	estimated_soc = soc_resolution.starting_soc_fraction
	points: list[OperatorSocProjectionPointResponse] = []
	for schedule_point in solve_result.schedule:
		load_soc_delta = load_by_timestamp.get(schedule_point.interval_start, 0.0) / battery_metrics.capacity_mwh
		estimated_soc = _clamp_soc_fraction(estimated_soc - load_soc_delta, battery_metrics)
		points.append(
			OperatorSocProjectionPointResponse(
				timestamp=schedule_point.interval_start,
				physical_soc=soc_resolution.physical_soc_fraction,
				estimated_soc=estimated_soc,
				planning_soc=schedule_point.soc_after_mwh / battery_metrics.capacity_mwh,
				soc_source=soc_resolution.source,
				confidence=soc_resolution.confidence,
			)
		)
	return points


def _operator_forecast_source(strategy_id: str) -> str:
	if strategy_id == "strict_similar_day":
		return "HourlyDamBaselineSolver / strict similar-day baseline"
	if strategy_id == OFFLINE_V2_PLUS_OPERATOR_STRATEGY_ID:
		return "Ukrainian-only V2+ Offline Strategy Promotion evidence with read-model preview adapter"
	if strategy_id == "nbeatsx_official_v0":
		return "official NBEATSx forecast candidate routed through Level 1 LP preview"
	if strategy_id == "tft_official_v0":
		return "official TFT forecast candidate routed through Level 1 LP preview"
	if strategy_id == "tft_silver_v0":
		return "compact TFT forecast candidate"
	if strategy_id == "nbeatsx_silver_v0":
		return "compact NBEATSx forecast candidate"
	if strategy_id in {CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND, RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND}:
		return f"{strategy_id} / pre-anchor value-aware selector"
	if strategy_id == "decision_transformer":
		return "NBEATSx/TFT forecast state plus offline Decision Transformer preview policy"
	return "strict similar-day control"


def _decision_policy_preview_is_ready(policy_preview_frame: pl.DataFrame) -> bool:
	if policy_preview_frame.height == 0:
		return False
	if "readiness_status" not in policy_preview_frame.columns or "constraint_violation" not in policy_preview_frame.columns:
		return False
	readiness_values = {str(value) for value in policy_preview_frame.select("readiness_status").to_series().to_list()}
	constraint_violation_count = int(policy_preview_frame.select("constraint_violation").sum().item())
	return readiness_values == {"ready_for_operator_preview"} and constraint_violation_count == 0


def _policy_mean_value_gap(policy_preview_frame: pl.DataFrame) -> float | None:
	if policy_preview_frame.height == 0 or "value_gap_uah" not in policy_preview_frame.columns:
		return None
	return float(policy_preview_frame.select("value_gap_uah").mean().item())


def _operator_policy_context(
	*,
	selected_strategy_id: str,
	policy_preview_frame: pl.DataFrame,
) -> dict[str, Any]:
	if selected_strategy_id == "decision_transformer" and _decision_policy_preview_is_ready(policy_preview_frame):
		first_row = policy_preview_frame.sort(["created_at", "interval_start"]).row(0, named=True)
		forecast_context_summary = _policy_forecast_context_summary(
			[
				row
				for row in policy_preview_frame.sort(["interval_start", "episode_id", "step_index"]).iter_rows(
					named=True
				)
			]
		)
		return {
			"policy_mode": "decision_transformer_preview",
			"selected_policy_id": str(first_row["policy_run_id"]),
			"policy_explanation": (
				"Offline Decision Transformer preview selected. Raw actions are projected through "
				"deterministic battery SOC/power constraints and remain market-execution disabled."
			),
			"policy_readiness": str(first_row["readiness_status"]),
			"forecast_context_source": forecast_context_summary["source"],
			"forecast_context_row_count": forecast_context_summary["row_count"],
			"forecast_context_coverage_ratio": forecast_context_summary["coverage_ratio"],
			"forecast_context_warning": forecast_context_summary["warning"],
		}
	if selected_strategy_id in OFFICIAL_FORECAST_TO_LP_STRATEGY_IDS:
		return {
			"policy_mode": "forecast_to_lp_preview",
			"selected_policy_id": selected_strategy_id,
			"policy_explanation": (
				"Official NBEATSx/TFT forecast rows are routed through the same deterministic "
				"Level 1 LP and battery projection. This is still operator preview, not market execution."
			),
			"policy_readiness": "forecast_to_lp_ready",
			**_operator_policy_context_not_applicable(),
		}
	if selected_strategy_id == OFFLINE_V2_PLUS_OPERATOR_STRATEGY_ID:
		return {
			"policy_mode": "offline_strategy_promotion_preview",
			"selected_policy_id": OFFLINE_V2_PLUS_OPERATOR_STRATEGY_ID,
			"policy_explanation": (
				"Frozen Ukrainian-only V2+ Offline Strategy Promotion evidence is selected for "
				"operator review. The visible schedule uses a deterministic V2+ read-model "
				"preview adapter over current price context, then the same battery feasibility "
				"LP; market execution stays disabled."
			),
			"policy_readiness": "offline_strategy_promotion_ready",
			**_operator_policy_context_not_applicable(),
		}
	return {
		"policy_mode": "baseline_lp_preview",
		"selected_policy_id": selected_strategy_id,
		"policy_explanation": (
			"Current operator schedule is generated by the Level 1 baseline LP preview. "
			"NBEATSx/TFT and DT surfaces are shown as forecast/policy evidence when materialized."
		),
		"policy_readiness": "lp_control_ready",
		**_operator_policy_context_not_applicable(),
	}


def _operator_policy_context_not_applicable() -> dict[str, Any]:
	return {
		"forecast_context_source": "not_applicable",
		"forecast_context_row_count": 0,
		"forecast_context_coverage_ratio": 0.0,
		"forecast_context_warning": None,
	}


def _operator_forecast_model_series(
	*,
	tenant_id: str,
	solve_result: BaselineSolveResult,
) -> list[FutureForecastSeriesResponse]:
	forecast_observation_frame = get_forecast_store().latest_forecast_observation_frame(
		model_names=FUTURE_STACK_FORECAST_MODEL_NAMES,
		limit_per_model=24,
	)
	forecast_store_series = _forecast_store_series(forecast_observation_frame, metrics={})
	if forecast_store_series:
		return forecast_store_series
	benchmark_frame = get_strategy_evaluation_store().latest_real_data_benchmark_frame(tenant_id=tenant_id)
	if benchmark_frame.height:
		metrics = _future_stack_model_metrics(benchmark_frame)
		latest_anchor = benchmark_frame.select("anchor_timestamp").max().item()
		series = [
			_future_forecast_series(row=row, metrics=metrics)
			for row in benchmark_frame
			.filter(pl.col("anchor_timestamp") == latest_anchor)
			.sort("forecast_model_name")
			.iter_rows(named=True)
			if _is_future_stack_forecast_model(str(row["forecast_model_name"]))
		]
		if series:
			return series
	return _fallback_forecast_model_series(solve_result)


def _fallback_forecast_model_series(solve_result: BaselineSolveResult) -> list[FutureForecastSeriesResponse]:
	forecast_points = list(solve_result.forecast[:24])
	nbeatsx_points = [
		FutureForecastPointResponse(
			step_index=index,
			interval_start=point.forecast_timestamp,
			forecast_price_uah_mwh=point.predicted_price_uah_mwh,
			actual_price_uah_mwh=None,
			p10_price_uah_mwh=None,
			p50_price_uah_mwh=point.predicted_price_uah_mwh,
			p90_price_uah_mwh=None,
			net_power_mw=None,
			value_gap_uah=None,
			price_cap_status=_future_forecast_price_cap_status(point.predicted_price_uah_mwh),
		)
		for index, point in enumerate(forecast_points)
	]
	tft_points = [
		FutureForecastPointResponse(
			step_index=index,
			interval_start=point.forecast_timestamp,
			forecast_price_uah_mwh=point.predicted_price_uah_mwh * 1.01,
			actual_price_uah_mwh=None,
			p10_price_uah_mwh=point.predicted_price_uah_mwh * 0.93,
			p50_price_uah_mwh=point.predicted_price_uah_mwh * 1.01,
			p90_price_uah_mwh=point.predicted_price_uah_mwh * 1.09,
			net_power_mw=None,
			value_gap_uah=None,
			price_cap_status=_future_forecast_price_cap_status(point.predicted_price_uah_mwh * 1.01),
		)
		for index, point in enumerate(forecast_points)
	]
	return [
		_future_forecast_series_response(
			model_name="nbeatsx_silver_v0",
			model_family="NBEATSx",
			source_status="compact_fallback_from_lp_preview",
			uncertainty_kind="trend_exogenous_proxy",
			mean_regret_uah=None,
			win_rate=None,
			points=nbeatsx_points,
		),
		_future_forecast_series_response(
			model_name="tft_silver_v0",
			model_family="TFT",
			source_status="compact_fallback_from_lp_preview",
			uncertainty_kind="quantile_proxy",
			mean_regret_uah=None,
			win_rate=None,
			points=tft_points,
		),
	]


def _operator_value_gap_series(baseline_preview: BaselineLpPreviewResponse) -> list[OperatorValueGapPointResponse]:
	if not baseline_preview.recommendation_schedule:
		return []
	best_visible_value_uah = max(point.net_value_uah for point in baseline_preview.recommendation_schedule)
	return [
		OperatorValueGapPointResponse(
			step_index=point.step_index,
			interval_start=point.interval_start,
			chosen_value_uah=point.net_value_uah,
			best_visible_value_uah=best_visible_value_uah,
			value_gap_uah=max(0.0, best_visible_value_uah - point.net_value_uah),
			metric_source="value_gap_visible_horizon_proxy",
		)
		for point in baseline_preview.recommendation_schedule
	]


def _operator_target_delivery_window(
	recommendation_schedule: list[BaselineRecommendationPointResponse],
) -> tuple[datetime | None, datetime | None]:
	if not recommendation_schedule:
		return None, None
	window_start = recommendation_schedule[0].interval_start
	window_end = recommendation_schedule[-1].interval_start + timedelta(minutes=LEVEL1_INTERVAL_MINUTES)
	return window_start, window_end


def _operator_shadow_preview_sources() -> list[ShadowPreviewSourceOptionResponse]:
	return [
		ShadowPreviewSourceOptionResponse(
			preview_source_id="best_valid",
			label="Best valid recommendation",
			status="default_v2_plus_fallback",
			is_default_strategy=True,
			is_promoted_strategy=True,
			market_execution_enabled=False,
			reason="Uses the existing gate-passed operator recommendation; V2+ remains default/fallback.",
		),
		ShadowPreviewSourceOptionResponse(
			preview_source_id="dt_shadow",
			label="DT Shadow",
			status="research_shadow_not_promoted",
			is_default_strategy=False,
			is_promoted_strategy=False,
			market_execution_enabled=False,
			reason="HF/local DT smoke over candidate-id or schedule-family targets; diagnostic preview only.",
		),
		ShadowPreviewSourceOptionResponse(
			preview_source_id="dt_direct_candidate_shadow",
			label="Direct DT Shadow",
			status="direct_candidate_shadow_not_promoted",
			is_default_strategy=False,
			is_promoted_strategy=False,
			market_execution_enabled=False,
			reason=(
				"Direct DT trained on V2+/strict/oracle candidate-index and schedule-family teacher targets; "
				"manual preview only."
			),
		),
		ShadowPreviewSourceOptionResponse(
			preview_source_id="dt_v2_plus_apples_to_apples_shadow",
			label="DT vs real V2+ Shadow",
			status="apples_to_apples_not_promoted",
			is_default_strategy=False,
			is_promoted_strategy=False,
			market_execution_enabled=False,
			reason=(
				"DT smoke built from the real V2+ strict-row packet; comparator-aligned "
				"research evidence only."
			),
		),
		ShadowPreviewSourceOptionResponse(
			preview_source_id="dt_v2_plus_distillation_shadow",
			label="DT V2+ distillation shadow",
			status="distillation_diagnostic_not_promoted",
			is_default_strategy=False,
			is_promoted_strategy=False,
			market_execution_enabled=False,
			reason=(
				"Rule-distillation DT smoke that mirrors V2+ selector targets; "
				"manual diagnostic preview only."
			),
		),
		ShadowPreviewSourceOptionResponse(
			preview_source_id="dt_decision_aware_shadow",
			label="DT Decision-Aware Shadow",
			status="decision_aware_diagnostic_not_promoted",
			is_default_strategy=False,
			is_promoted_strategy=False,
			market_execution_enabled=False,
			reason=(
				"Decision-aware DT objective over regret/value with conservative "
				"V2+ fallback and tail-risk guard; manual diagnostic preview only."
			),
		),
		ShadowPreviewSourceOptionResponse(
			preview_source_id="regret_aware_v2_plus_selector_shadow",
			label="Regret-aware V2+ selector",
			status="regret_aware_abstention_not_promoted",
			is_default_strategy=False,
			is_promoted_strategy=False,
			market_execution_enabled=False,
			reason=(
				"Regret-aware value-gap selector with explicit abstention back to V2+; "
				"manual diagnostic preview only."
			),
		),
		ShadowPreviewSourceOptionResponse(
			preview_source_id="dt_v2_plus_safe_switch_selector_shadow",
			label="DT V2+ safe-switch selector",
			status="safe_switch_evidence_not_promoted",
			is_default_strategy=False,
			is_promoted_strategy=False,
			market_execution_enabled=False,
			reason=(
				"Corrected residual DT/V2+ shadow recovered 3 of 15 safe-switch opportunities "
				"with 4 / 90 non-V2+ switches, zero tail-risk losses, and V2+ fallback/default preserved."
			),
		),
		ShadowPreviewSourceOptionResponse(
			preview_source_id="poland_tft_shadow",
			label="Poland/TFT Shadow",
			status="positive_not_promoted",
			is_default_strategy=False,
			is_promoted_strategy=False,
			market_execution_enabled=False,
			reason="Shadow challenger evidence; not default because rolling robustness is not sufficient.",
		),
		ShadowPreviewSourceOptionResponse(
			preview_source_id="dfl_diagnostics",
			label="DFL diagnostics",
			status="diagnostic_only",
			is_default_strategy=False,
			is_promoted_strategy=False,
			market_execution_enabled=False,
			reason="Candidate-value diagnostics for explaining regret/value behavior; not production strategy.",
		),
		ShadowPreviewSourceOptionResponse(
			preview_source_id="v13_dt_lava_promoted_training",
			label="V13/DT/LAVA blocked",
			status="blocked_source_readiness_roadmap",
			is_default_strategy=False,
			is_promoted_strategy=False,
			market_execution_enabled=False,
			reason="Blocked until V13 source-readiness and receipt gates pass.",
		),
	]


def _operator_shadow_recommendation_preview_response(
	*,
	tenant_id: str,
	preview_source: str,
	target_delivery_window_start: datetime | None = None,
) -> ShadowRecommendationPreviewResponse:
	response: ShadowRecommendationPreviewResponse
	if preview_source == "dt_shadow":
		response = _operator_dt_shadow_recommendation_preview_response(tenant_id=tenant_id)
	elif preview_source == "dt_direct_candidate_shadow":
		response = _operator_dt_shadow_recommendation_preview_response(
			tenant_id=tenant_id,
			preview_source_id=preview_source,
			label="Direct DT Shadow",
			status="direct_candidate_shadow_not_promoted",
			selected_preview_path=DT_DIRECT_CANDIDATE_SHADOW_SELECTED_PREVIEW_JSON_PATH,
			teacher_rows_path=DT_DIRECT_CANDIDATE_SHADOW_TEACHER_ROWS_CSV_PATH,
		)
	elif preview_source == "dt_v2_plus_apples_to_apples_shadow":
		response = _operator_dt_shadow_recommendation_preview_response(
			tenant_id=tenant_id,
			preview_source_id=preview_source,
			label="DT vs real V2+ Shadow",
			status="apples_to_apples_not_promoted",
			selected_preview_path=DT_V2_PLUS_APPLES_TO_APPLES_SELECTED_PREVIEW_JSON_PATH,
			teacher_rows_path=DT_V2_PLUS_APPLES_TO_APPLES_TEACHER_ROWS_CSV_PATH,
		)
	elif preview_source == "dt_v2_plus_distillation_shadow":
		response = _operator_dt_shadow_recommendation_preview_response(
			tenant_id=tenant_id,
			preview_source_id=preview_source,
			label="DT V2+ distillation shadow",
			status="distillation_diagnostic_not_promoted",
			selected_preview_path=DT_V2_PLUS_DISTILLATION_SHADOW_SELECTED_PREVIEW_JSON_PATH,
			teacher_rows_path=DT_V2_PLUS_DISTILLATION_SHADOW_TEACHER_ROWS_CSV_PATH,
		)
	elif preview_source == "dt_decision_aware_shadow":
		response = _operator_dt_shadow_recommendation_preview_response(
			tenant_id=tenant_id,
			preview_source_id=preview_source,
			label="DT Decision-Aware Shadow",
			status="decision_aware_diagnostic_not_promoted",
			selected_preview_path=DT_DECISION_AWARE_SHADOW_SELECTED_PREVIEW_JSON_PATH,
			teacher_rows_path=DT_DECISION_AWARE_SHADOW_TEACHER_ROWS_CSV_PATH,
		)
	elif preview_source == "regret_aware_v2_plus_selector_shadow":
		response = _operator_regret_aware_selector_shadow_preview_response(
			tenant_id=tenant_id,
		)
	elif preview_source == "dt_v2_plus_safe_switch_selector_shadow":
		response = _operator_regret_aware_selector_shadow_preview_response(
			tenant_id=tenant_id,
			preview_source_id=preview_source,
			label="DT V2+ safe-switch selector",
			status="safe_switch_evidence_not_promoted",
			selected_rows_path=DT_V2_PLUS_SAFE_SWITCH_SELECTOR_SELECTED_ROWS_CSV_PATH,
			teacher_rows_path=DT_V2_PLUS_SAFE_SWITCH_SELECTOR_TEACHER_ROWS_CSV_PATH,
			summary_path=DT_V2_PLUS_SAFE_SWITCH_SELECTOR_SUMMARY_JSON_PATH,
			promotion_summary_path=DT_V2_PLUS_PROMOTION_EVIDENCE_SUMMARY_JSON_PATH,
		)
	elif preview_source == "v13_dt_lava_promoted_training":
		response = _blocked_shadow_recommendation_preview_response(
			tenant_id=tenant_id,
			preview_source_id=preview_source,
			label="V13/DT/LAVA blocked",
			status="blocked_source_readiness_roadmap",
			warning=(
				"V13/DT/LAVA remains blocked by source-readiness, "
				"explicit DAM receipt, and promotion gates."
			),
		)
	elif preview_source in {"poland_tft_shadow", "dfl_diagnostics"}:
		response = _operator_artifact_shadow_recommendation_preview_response(
			tenant_id=tenant_id,
			preview_source=preview_source,
		)
	else:
		raise HTTPException(status_code=404, detail=f"Unknown shadow preview source: {preview_source}")
	return _project_shadow_preview_to_delivery_window(
		response=response,
		target_delivery_window_start=target_delivery_window_start,
	)


def _project_shadow_preview_to_delivery_window(
	*,
	response: ShadowRecommendationPreviewResponse,
	target_delivery_window_start: datetime | None,
) -> ShadowRecommendationPreviewResponse:
	if target_delivery_window_start is None or not response.recommendation_schedule:
		return response

	projected_schedule = [
		point.model_copy(
			update={
				"step_index": index,
				"interval_start": target_delivery_window_start
				+ timedelta(minutes=response.interval_minutes * index),
			}
		)
		for index, point in enumerate(response.recommendation_schedule)
	]
	target_delivery_window_end = target_delivery_window_start + timedelta(
		minutes=response.interval_minutes * len(projected_schedule)
	)
	return response.model_copy(
		update={
			"target_delivery_window_start": target_delivery_window_start,
			"target_delivery_window_end": target_delivery_window_end,
			"recommendation_schedule": projected_schedule,
			"boundary_labels": [
				*response.boundary_labels,
				"Projected onto requested delivery-day window",
			],
			"readiness_warnings": [
				*response.readiness_warnings,
				"Shadow artifact actions are timestamp-projected onto the requested DAM delivery day for preview; "
				"the artifact anchor remains unchanged and this is not market execution.",
			],
		}
	)


def _blocked_shadow_recommendation_preview_response(
	*,
	tenant_id: str,
	preview_source_id: str,
	label: str,
	status: str,
	warning: str,
) -> ShadowRecommendationPreviewResponse:
	return ShadowRecommendationPreviewResponse(
		tenant_id=tenant_id,
		preview_source_id=preview_source_id,
		preview_source_label=label,
		preview_status=status,
		preview_only=True,
		is_default_strategy=False,
		is_promoted_strategy=False,
		research_shadow_not_promotable=True,
		default_strategy_id=OFFLINE_V2_PLUS_OPERATOR_STRATEGY_ID,
		default_strategy_label=OFFLINE_V2_PLUS_LABEL,
		selected_candidate_id=None,
		selected_schedule_family=None,
		selected_candidate_index=None,
		market_scope=OPERATOR_MARKET_SCOPE,
		market_venue=LEVEL1_MARKET_VENUE,
		interval_minutes=LEVEL1_INTERVAL_MINUTES,
		anchor_timestamp=None,
		target_delivery_window_start=None,
		target_delivery_window_end=None,
		market_execution_enabled=False,
		proposed_bid_status=OPERATOR_PROPOSED_BID_STATUS,
		market_order_payload_emitted=False,
		promotion_gate_passed=False,
		dt_lava_ready=False,
		source_readiness_gate_passed=False,
		comparison_metrics={},
		available_preview_sources=_operator_shadow_preview_sources(),
		recommendation_schedule=[],
		boundary_labels=[
			"Preview only",
			"Not promoted",
			"No market execution",
			"V2+ remains default/fallback",
		],
		readiness_warnings=[warning],
		artifact_paths={},
	)


def _operator_dt_shadow_recommendation_preview_response(
	*,
	tenant_id: str,
	preview_source_id: str = "dt_shadow",
	label: str = "DT Shadow",
	status: str = "research_shadow_not_promoted",
	selected_preview_path: Path | None = None,
	teacher_rows_path: Path | None = None,
) -> ShadowRecommendationPreviewResponse:
	packet_path = selected_preview_path or DT_RESEARCH_SHADOW_SELECTED_PREVIEW_JSON_PATH
	resolved_teacher_rows_path = teacher_rows_path or DT_RESEARCH_SHADOW_TEACHER_ROWS_CSV_PATH
	packet = _read_json_object(
		packet_path,
		not_found_detail=f"{label} selected schedule preview artifact not found.",
	)
	_reject_executable_shadow_payload(packet, label=f"{label} selected schedule preview")
	rows = packet.get("preview_rows")
	if not isinstance(rows, list):
		raise HTTPException(status_code=500, detail=f"{label} preview artifact missing preview_rows.")
	preview_rows = [row for row in rows if isinstance(row, dict)]
	if not preview_rows:
		raise HTTPException(status_code=500, detail=f"{label} preview artifact has no object preview_rows.")
	try:
		preview_frame = pl.DataFrame(preview_rows)
	except (TypeError, ValueError, pl.exceptions.PolarsError) as error:
		raise HTTPException(status_code=500, detail=f"{label} preview rows are unreadable.") from error
	_reject_executable_shadow_frame(preview_frame, label=f"{label} preview rows")
	tenant_rows = [
		row for row in preview_rows if str(row.get("tenant_id", tenant_id)) == tenant_id
	]
	if not tenant_rows:
		raise HTTPException(status_code=404, detail=f"{label} preview rows not found for tenant {tenant_id}.")
	selection = max(
		tenant_rows,
		key=lambda row: _datetime_payload_value(row.get("anchor_timestamp"), field_name="anchor_timestamp"),
	)
	selected_candidate_id = str(selection.get("selected_candidate_id", "")).strip()
	if not selected_candidate_id:
		raise HTTPException(status_code=500, detail=f"{label} preview row missing selected_candidate_id.")
	candidate_row = _dt_shadow_teacher_candidate_row(
		teacher_rows_path=resolved_teacher_rows_path,
		tenant_id=tenant_id,
		selected_candidate_id=selected_candidate_id,
		label=label,
	)
	schedule = _shadow_schedule_rows_from_candidate(
		candidate_row=candidate_row,
		selection=selection,
		selected_candidate_id=selected_candidate_id,
		selected_schedule_family=str(selection.get("selected_schedule_family") or candidate_row.get("dt_schedule_family_target", "")),
	)
	target_start = schedule[0].interval_start if schedule else None
	target_end = schedule[-1].interval_start + timedelta(minutes=LEVEL1_INTERVAL_MINUTES) if schedule else None
	return ShadowRecommendationPreviewResponse(
		tenant_id=tenant_id,
		preview_source_id=preview_source_id,
		preview_source_label=label,
		preview_status=status,
		preview_only=True,
		is_default_strategy=False,
		is_promoted_strategy=False,
		research_shadow_not_promotable=True,
		default_strategy_id=OFFLINE_V2_PLUS_OPERATOR_STRATEGY_ID,
		default_strategy_label=OFFLINE_V2_PLUS_LABEL,
		selected_candidate_id=selected_candidate_id,
		selected_schedule_family=str(selection.get("selected_schedule_family") or candidate_row.get("dt_schedule_family_target", "")),
		selected_candidate_index=_optional_int(selection.get("selected_candidate_index")),
		market_scope=OPERATOR_MARKET_SCOPE,
		market_venue=LEVEL1_MARKET_VENUE,
		interval_minutes=LEVEL1_INTERVAL_MINUTES,
		anchor_timestamp=_datetime_payload_value(selection.get("anchor_timestamp"), field_name="anchor_timestamp"),
		target_delivery_window_start=target_start,
		target_delivery_window_end=target_end,
		market_execution_enabled=False,
		proposed_bid_status=OPERATOR_PROPOSED_BID_STATUS,
		market_order_payload_emitted=False,
		promotion_gate_passed=False,
		dt_lava_ready=False,
		source_readiness_gate_passed=False,
		comparison_metrics=_dt_shadow_comparison_metrics(selection=selection, packet=packet),
		available_preview_sources=_operator_shadow_preview_sources(),
		recommendation_schedule=schedule,
		boundary_labels=[
			label,
			"Not promoted",
			"Preview only",
			"No market execution",
			"V2+ remains default/fallback",
		],
		readiness_warnings=[
			f"{label} is diagnostic evidence only and is rendered even when it loses to V2+ or strict/oracle.",
			"V13 explicit DAM publication receipts remain blocked for market-submission claims.",
		],
		artifact_paths={
			"selected_preview_json": str(packet_path),
			"teacher_rows_csv": str(resolved_teacher_rows_path),
		},
	)


def _operator_regret_aware_selector_shadow_preview_response(
	*,
	tenant_id: str,
	preview_source_id: str = "regret_aware_v2_plus_selector_shadow",
	label: str = "Regret-aware V2+ selector",
	status: str = "regret_aware_abstention_not_promoted",
	selected_rows_path: Path | None = None,
	teacher_rows_path: Path | None = None,
	summary_path: Path | None = None,
	promotion_summary_path: Path | None = None,
) -> ShadowRecommendationPreviewResponse:
	resolved_selected_rows_path = selected_rows_path or REGRET_AWARE_V2_PLUS_SELECTOR_SELECTED_ROWS_CSV_PATH
	resolved_teacher_rows_path = teacher_rows_path or REGRET_AWARE_V2_PLUS_SELECTOR_TEACHER_ROWS_CSV_PATH
	resolved_summary_path = summary_path or REGRET_AWARE_V2_PLUS_SELECTOR_SUMMARY_JSON_PATH
	summary = _read_json_object(
		resolved_summary_path,
		not_found_detail=f"{label} summary artifact not found.",
	)
	_reject_executable_shadow_payload(summary, label=f"{label} artifact")
	selected_rows = _read_shadow_csv_frame(
		resolved_selected_rows_path,
		label=f"{label} selected rows",
	)
	teacher_rows = _read_shadow_csv_frame(
		resolved_teacher_rows_path,
		label=f"{label} teacher rows",
	)
	_reject_executable_shadow_frame(selected_rows, label=f"{label} selected rows")
	_reject_executable_shadow_frame(teacher_rows, label=f"{label} teacher rows")
	selection = _latest_regret_aware_selector_row(
		selected_rows,
		tenant_id=tenant_id,
		label=label,
	)
	_validate_regret_aware_v2_fallback(selection, label=label)
	selected_candidate_id = str(selection.get("selected_candidate_id", "")).strip()
	candidate_row = _regret_aware_selector_teacher_candidate_row(
		teacher_rows=teacher_rows,
		tenant_id=tenant_id,
		anchor_timestamp=_datetime_row_value(
			selection["anchor_timestamp"],
			field_name="anchor_timestamp",
		),
		selected_candidate_id=selected_candidate_id,
		label=label,
	)
	strict_row = _regret_aware_selector_reference_row(
		teacher_rows=teacher_rows,
		tenant_id=tenant_id,
		anchor_timestamp=_datetime_row_value(
			selection["anchor_timestamp"],
			field_name="anchor_timestamp",
		),
		reference_family="strict_reference",
	)
	comparison_metrics = _regret_aware_selector_comparison_metrics(summary=summary)
	if promotion_summary_path is not None:
		promotion_summary = _read_json_object(
			promotion_summary_path,
			not_found_detail=f"{label} promotion evidence summary artifact not found.",
		)
		_reject_executable_shadow_payload(
			promotion_summary,
			label=f"{label} promotion evidence artifact",
		)
		comparison_metrics.update(
			_dt_v2_plus_safe_switch_promotion_metrics(summary=promotion_summary)
		)
	shadow_selection = _regret_aware_shadow_selection(
		selection=selection,
		strict_row=strict_row,
		comparison_metrics=comparison_metrics,
	)
	selected_schedule_family = str(selection.get("selected_schedule_family") or candidate_row.get("dt_schedule_family_target", ""))
	schedule = _shadow_schedule_rows_from_candidate(
		candidate_row=candidate_row,
		selection=shadow_selection,
		selected_candidate_id=selected_candidate_id,
		selected_schedule_family=selected_schedule_family,
	)
	target_start = schedule[0].interval_start if schedule else None
	target_end = schedule[-1].interval_start + timedelta(minutes=LEVEL1_INTERVAL_MINUTES) if schedule else None
	return ShadowRecommendationPreviewResponse(
		tenant_id=tenant_id,
		preview_source_id=preview_source_id,
		preview_source_label=label,
		preview_status=status,
		preview_only=True,
		is_default_strategy=False,
		is_promoted_strategy=False,
		research_shadow_not_promotable=True,
		default_strategy_id=OFFLINE_V2_PLUS_OPERATOR_STRATEGY_ID,
		default_strategy_label=OFFLINE_V2_PLUS_LABEL,
		selected_candidate_id=selected_candidate_id,
		selected_schedule_family=selected_schedule_family,
		selected_candidate_index=_optional_int(selection.get("selected_candidate_index")),
		market_scope=OPERATOR_MARKET_SCOPE,
		market_venue=LEVEL1_MARKET_VENUE,
		interval_minutes=LEVEL1_INTERVAL_MINUTES,
		anchor_timestamp=_datetime_row_value(selection["anchor_timestamp"], field_name="anchor_timestamp"),
		target_delivery_window_start=target_start,
		target_delivery_window_end=target_end,
		market_execution_enabled=False,
		proposed_bid_status=OPERATOR_PROPOSED_BID_STATUS,
		market_order_payload_emitted=False,
		promotion_gate_passed=False,
		dt_lava_ready=False,
		source_readiness_gate_passed=False,
		comparison_metrics=comparison_metrics,
		available_preview_sources=_operator_shadow_preview_sources(),
		recommendation_schedule=schedule,
		boundary_labels=[
			label,
			"Not promoted",
			"Preview only",
			"No market execution",
			"V2+ remains default/fallback",
			"Conservative V2+ abstention/safe-switch guard",
		],
		readiness_warnings=_regret_aware_selector_readiness_warnings(
			label=label,
			promotion_summary_path=promotion_summary_path,
			comparison_metrics=comparison_metrics,
		),
		artifact_paths={
			"selected_rows_csv": str(resolved_selected_rows_path),
			"teacher_rows_csv": str(resolved_teacher_rows_path),
			"summary_json": str(resolved_summary_path),
			**(
				{"promotion_evidence_summary_json": str(promotion_summary_path)}
				if promotion_summary_path is not None
				else {}
			),
		},
	)


def _operator_artifact_shadow_recommendation_preview_response(
	*,
	tenant_id: str,
	preview_source: str,
) -> ShadowRecommendationPreviewResponse:
	label = "Poland/TFT Shadow" if preview_source == "poland_tft_shadow" else "DFL diagnostics"
	status = "positive_not_promoted" if preview_source == "poland_tft_shadow" else "diagnostic_only"
	if not TFT_SHADOW_AUGMENTED_GATE_ROWS_CSV_PATH.exists():
		return _blocked_shadow_recommendation_preview_response(
			tenant_id=tenant_id,
			preview_source_id=preview_source,
			label=label,
			status=status,
			warning=f"{label} artifact rows are not available in the local read model.",
		)
	try:
		frame = pl.read_csv(TFT_SHADOW_AUGMENTED_GATE_ROWS_CSV_PATH, try_parse_dates=True)
	except (OSError, pl.exceptions.PolarsError) as error:
		raise HTTPException(status_code=500, detail=f"{label} artifact rows are unreadable.") from error
	if frame.height == 0:
		return _blocked_shadow_recommendation_preview_response(
			tenant_id=tenant_id,
			preview_source_id=preview_source,
			label=label,
			status=status,
			warning=f"{label} artifact rows are empty.",
		)
	tenant_frame = frame.filter(pl.col("tenant_id") == tenant_id)
	if preview_source == "poland_tft_shadow" and "source_model_name" in tenant_frame.columns:
		tenant_frame = tenant_frame.filter(pl.col("source_model_name").str.contains("tft"))
	if tenant_frame.height == 0:
		return _blocked_shadow_recommendation_preview_response(
			tenant_id=tenant_id,
			preview_source_id=preview_source,
			label=label,
			status=status,
			warning=f"{label} has no schedule rows for tenant {tenant_id}.",
		)
	row = tenant_frame.sort(["anchor_timestamp", "regret_uah"]).tail(1).row(0, named=True)
	selection = {
		"anchor_timestamp": row["anchor_timestamp"],
		"selected_candidate_id": str(row.get("evaluation_id", "")),
		"selected_schedule_family": str(row.get("selection_role", preview_source)),
		"selected_candidate_index": None,
		"dt_selected_regret_uah": float(row.get("regret_uah", 0.0)),
		"dt_selected_value_uah": float(row.get("decision_value_uah", 0.0)),
		"v2_plus_regret_uah": OFFLINE_V2_PLUS_MEAN_REGRET_UAH,
		"v2_plus_value_uah": float(row.get("decision_value_uah", 0.0)),
		"strict_regret_uah": float(row.get("regret_uah", 0.0)),
		"strict_value_uah": float(row.get("oracle_value_uah", row.get("decision_value_uah", 0.0))),
	}
	schedule = _shadow_schedule_rows_from_augmented_gate_row(row=row, selection=selection)
	target_start = schedule[0].interval_start if schedule else None
	target_end = schedule[-1].interval_start + timedelta(minutes=LEVEL1_INTERVAL_MINUTES) if schedule else None
	return ShadowRecommendationPreviewResponse(
		tenant_id=tenant_id,
		preview_source_id=preview_source,
		preview_source_label=label,
		preview_status=status,
		preview_only=True,
		is_default_strategy=False,
		is_promoted_strategy=False,
		research_shadow_not_promotable=True,
		default_strategy_id=OFFLINE_V2_PLUS_OPERATOR_STRATEGY_ID,
		default_strategy_label=OFFLINE_V2_PLUS_LABEL,
		selected_candidate_id=str(selection["selected_candidate_id"]),
		selected_schedule_family=str(selection["selected_schedule_family"]),
		selected_candidate_index=None,
		market_scope=OPERATOR_MARKET_SCOPE,
		market_venue=LEVEL1_MARKET_VENUE,
		interval_minutes=LEVEL1_INTERVAL_MINUTES,
		anchor_timestamp=_datetime_row_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
		target_delivery_window_start=target_start,
		target_delivery_window_end=target_end,
		market_execution_enabled=False,
		proposed_bid_status=OPERATOR_PROPOSED_BID_STATUS,
		market_order_payload_emitted=False,
		promotion_gate_passed=False,
		dt_lava_ready=False,
		source_readiness_gate_passed=False,
		comparison_metrics=_dt_shadow_comparison_metrics(selection=selection, packet={}),
		available_preview_sources=_operator_shadow_preview_sources(),
		recommendation_schedule=schedule,
		boundary_labels=[
			label,
			status,
			"Preview only",
			"No market execution",
			"V2+ remains default/fallback",
		],
		readiness_warnings=[
			f"{label} is manually selectable diagnostic evidence and is not the production default.",
		],
		artifact_paths={"augmented_gate_rows_csv": str(TFT_SHADOW_AUGMENTED_GATE_ROWS_CSV_PATH)},
	)


def _read_shadow_csv_frame(path: Path, *, label: str) -> pl.DataFrame:
	if not path.exists():
		raise HTTPException(status_code=404, detail=f"{label} artifact not found: {path}")
	try:
		frame = pl.read_csv(path, infer_schema_length=1000, try_parse_dates=True)
	except (OSError, pl.exceptions.PolarsError) as error:
		raise HTTPException(status_code=500, detail=f"{label} artifact is unreadable: {path}") from error
	if frame.height == 0:
		raise HTTPException(status_code=404, detail=f"{label} artifact is empty.")
	return frame


def _reject_executable_shadow_payload(payload: object, *, label: str) -> None:
	if _json_contains_truthy_flag(payload, flag_name="market_execution_enabled"):
		raise HTTPException(status_code=500, detail=f"{label} must keep market_execution_enabled=false.")
	if _json_contains_truthy_flag(payload, flag_name="promotion_gate_passed") or _json_contains_truthy_flag(
		payload,
		flag_name="is_promoted_strategy",
	):
		raise HTTPException(status_code=500, detail=f"{label} must not contain promoted rows.")
	if _json_contains_truthy_flag(payload, flag_name="dt_lava_ready"):
		raise HTTPException(status_code=500, detail=f"{label} must not enable dt_lava_ready.")
	if _json_contains_truthy_flag(payload, flag_name="permits_model_training"):
		raise HTTPException(status_code=500, detail=f"{label} must not permit model training.")


def _reject_executable_shadow_frame(frame: pl.DataFrame, *, label: str) -> None:
	if _artifact_frame_column_has_true(frame, "market_execution_enabled"):
		raise HTTPException(status_code=500, detail=f"{label} must keep market_execution_enabled=false.")
	if _artifact_frame_column_has_true(frame, "market_execution_gate_passed"):
		raise HTTPException(status_code=500, detail=f"{label} must not pass market execution gates.")
	if _artifact_frame_column_has_true(frame, "promotion_gate_passed"):
		raise HTTPException(status_code=500, detail=f"{label} must not contain promoted rows.")
	if _artifact_frame_column_has_true(frame, "dt_lava_ready"):
		raise HTTPException(status_code=500, detail=f"{label} must not enable dt_lava_ready.")
	if _artifact_frame_column_has_true(frame, "permits_model_training"):
		raise HTTPException(status_code=500, detail=f"{label} must not permit model training.")


def _artifact_frame_column_has_true(frame: pl.DataFrame, column: str) -> bool:
	if column not in frame.columns:
		return False
	for value in frame[column].to_list():
		if _artifact_truthy(value):
			return True
	return False


def _artifact_truthy(value: Any) -> bool:
	if isinstance(value, bool):
		return value
	if isinstance(value, (int, float)):
		return value != 0
	if isinstance(value, str):
		return value.strip().lower() in {"1", "true", "yes"}
	return False


def _latest_regret_aware_selector_row(
	frame: pl.DataFrame,
	*,
	tenant_id: str,
	label: str,
) -> dict[str, Any]:
	if "tenant_id" not in frame.columns or "anchor_timestamp" not in frame.columns:
		raise HTTPException(status_code=500, detail=f"{label} selected rows missing tenant/anchor columns.")
	tenant_frame = frame.filter(pl.col("tenant_id").cast(pl.String) == tenant_id)
	if tenant_frame.height == 0:
		raise HTTPException(status_code=404, detail=f"{label} rows not found for tenant {tenant_id}.")
	return tenant_frame.sort("anchor_timestamp").tail(1).row(0, named=True)


def _validate_regret_aware_v2_fallback(selection: dict[str, Any], *, label: str) -> None:
	if not str(selection.get("selected_candidate_id", "")).strip():
		raise HTTPException(status_code=500, detail=f"{label} selected row missing selected_candidate_id.")
	if not str(selection.get("v2_plus_candidate_id", "")).strip():
		raise HTTPException(status_code=500, detail=f"{label} selected row missing V2+ fallback candidate.")
	if _optional_float(selection.get("v2_plus_regret_uah")) is None:
		raise HTTPException(status_code=500, detail=f"{label} selected row missing V2+ regret.")
	if _optional_float(selection.get("v2_plus_value_uah")) is None:
		raise HTTPException(status_code=500, detail=f"{label} selected row missing V2+ value.")


def _regret_aware_selector_teacher_candidate_row(
	*,
	teacher_rows: pl.DataFrame,
	tenant_id: str,
	anchor_timestamp: datetime,
	selected_candidate_id: str,
	label: str,
) -> dict[str, Any]:
	for row in teacher_rows.iter_rows(named=True):
		if str(row.get("tenant_id", "")) != tenant_id:
			continue
		if _datetime_row_value(row["anchor_timestamp"], field_name="anchor_timestamp") != anchor_timestamp:
			continue
		if not _row_candidate_id_matches(row, selected_candidate_id):
			continue
		return row
	raise HTTPException(
		status_code=404,
		detail=f"{label} selected candidate not found in teacher rows: {selected_candidate_id}",
	)


def _regret_aware_selector_reference_row(
	*,
	teacher_rows: pl.DataFrame,
	tenant_id: str,
	anchor_timestamp: datetime,
	reference_family: str,
) -> dict[str, Any] | None:
	for row in teacher_rows.iter_rows(named=True):
		if str(row.get("tenant_id", "")) != tenant_id:
			continue
		if _datetime_row_value(row["anchor_timestamp"], field_name="anchor_timestamp") != anchor_timestamp:
			continue
		families = {
			str(row.get("candidate_family", "")),
			str(row.get("dt_schedule_family_target", "")),
			str(row.get("candidate_model_name", "")),
		}
		if reference_family in families:
			return row
	return None


def _row_candidate_id_matches(row: dict[str, Any], selected_candidate_id: str) -> bool:
	for column in ("dt_candidate_id_target", "teacher_candidate_key", "candidate_model_name"):
		if str(row.get(column, "")) == selected_candidate_id:
			return True
	return False


def _regret_aware_shadow_selection(
	*,
	selection: dict[str, Any],
	strict_row: dict[str, Any] | None,
	comparison_metrics: dict[str, float],
) -> dict[str, Any]:
	strict_regret = _optional_float(strict_row.get("regret_uah")) if strict_row else None
	strict_value = _optional_float(
		strict_row.get("schedule_value_uah", strict_row.get("decision_value_uah"))
	) if strict_row else None
	return {
		"anchor_timestamp": selection["anchor_timestamp"],
		"selected_candidate_id": str(selection["selected_candidate_id"]),
		"selected_schedule_family": str(selection.get("selected_schedule_family", "")),
		"selected_candidate_index": _optional_int(selection.get("selected_candidate_index")),
		"dt_selected_regret_uah": _float_payload_value(selection.get("selected_regret_uah")),
		"dt_selected_value_uah": _float_payload_value(selection.get("selected_value_uah")),
		"v2_plus_regret_uah": _float_payload_value(selection.get("v2_plus_regret_uah")),
		"v2_plus_value_uah": _float_payload_value(selection.get("v2_plus_value_uah")),
		"strict_regret_uah": strict_regret
		if strict_regret is not None
		else comparison_metrics.get("strict_mean_regret_uah"),
		"strict_value_uah": strict_value
		if strict_value is not None
		else comparison_metrics.get("strict_mean_value_uah"),
		"abstained_to_v2_plus": _artifact_truthy(selection.get("abstained_to_v2_plus")),
		"abstention_reason": str(selection.get("abstention_reason", "")),
		"market_execution_enabled": False,
	}


def _regret_aware_selector_comparison_metrics(
	*,
	summary: dict[str, Any],
) -> dict[str, float]:
	evaluation = summary.get("evaluation")
	if not isinstance(evaluation, dict):
		raise HTTPException(status_code=500, detail="Regret-aware selector summary missing evaluation metrics.")
	control_summary = evaluation.get("control_summary")
	strict_summary = control_summary.get("strict_reference", {}) if isinstance(control_summary, dict) else {}
	selector_regret = _required_metric(evaluation, "selector_mean_regret_uah")
	selector_value = _required_metric(evaluation, "selector_mean_value_uah")
	v2_regret = _required_metric(evaluation, "v2_plus_mean_regret_uah")
	v2_value = _required_metric(evaluation, "v2_plus_mean_value_uah")
	strict_regret = _optional_metric(strict_summary, "mean_regret_uah")
	strict_value = _optional_metric(strict_summary, "mean_value_uah")
	metrics = {
		"selector_mean_regret_uah": selector_regret,
		"selector_mean_value_uah": selector_value,
		"v2_plus_mean_regret_uah": v2_regret,
		"v2_plus_mean_value_uah": v2_value,
		"selector_minus_v2_plus_mean_regret_uah": _optional_metric(
			evaluation,
			"selector_minus_v2_plus_mean_regret_uah",
		) or selector_regret - v2_regret,
		"selector_minus_v2_plus_mean_value_uah": _optional_metric(
			evaluation,
			"selector_minus_v2_plus_mean_value_uah",
		) or selector_value - v2_value,
		"non_v2_plus_switch_count": _required_metric(evaluation, "non_v2_plus_switch_count"),
		"abstention_count": _required_metric(evaluation, "abstention_count"),
		"dt_selected_mean_regret_uah": selector_regret,
		"dt_selected_mean_value_uah": selector_value,
		"dt_minus_v2_plus_regret_uah": selector_regret - v2_regret,
		"dt_minus_v2_plus_value_uah": selector_value - v2_value,
	}
	if strict_regret is not None:
		metrics["strict_mean_regret_uah"] = strict_regret
		metrics["dt_minus_strict_regret_uah"] = selector_regret - strict_regret
	if strict_value is not None:
		metrics["strict_mean_value_uah"] = strict_value
		metrics["dt_minus_strict_value_uah"] = selector_value - strict_value
	return metrics


def _dt_v2_plus_safe_switch_promotion_metrics(
	*,
	summary: dict[str, Any],
) -> dict[str, float]:
	gate = summary.get("gate")
	if not isinstance(gate, dict):
		raise HTTPException(status_code=500, detail="DT V2+ promotion evidence summary missing gate metrics.")
	metrics: dict[str, float] = {}
	for name in (
		"observed_safe_switch_opportunity_count",
		"recovered_safe_switch_opportunity_count",
		"safe_switch_win_count",
		"safe_switch_loss_count",
		"safe_switch_tie_count",
		"tail_risk_loss_count",
		"max_switch_loss_uah",
		"mean_regret_improvement_ratio_vs_v2_plus",
		"oracle_scored_final_holdout_row_count",
	):
		value = _optional_metric(gate, name)
		if value is not None:
			metrics[name] = value
	return metrics


def _regret_aware_selector_readiness_warnings(
	*,
	label: str,
	promotion_summary_path: Path | None,
	comparison_metrics: dict[str, float],
) -> list[str]:
	if promotion_summary_path is not None:
		recovered = int(comparison_metrics.get("recovered_safe_switch_opportunity_count", 0.0))
		observed = int(comparison_metrics.get("observed_safe_switch_opportunity_count", 0.0))
		switches = int(comparison_metrics.get("non_v2_plus_switch_count", 0.0))
		losses = int(comparison_metrics.get("tail_risk_loss_count", 0.0))
		improvement = comparison_metrics.get("mean_regret_improvement_ratio_vs_v2_plus", 0.0) * 100.0
		return [
			f"Recovered {recovered} of {observed} safe-switch opportunities with {label}; "
			f"{switches} / 90 non-V2+ switches and {losses} tail-risk losses.",
			f"Mean regret improved by {improvement:.2f}% versus V2+ in offline final-holdout evidence, "
			"but promotion_gate_passed=false and V2+ remains default/fallback.",
			"V13 explicit DAM publication receipts remain blocked for market-submission claims.",
		]
	return [
		"Selector abstained to V2+ on the current packet; this is diagnostic evidence, not a default switch.",
		"V13 explicit DAM publication receipts remain blocked for market-submission claims.",
	]


def _required_metric(metrics: dict[str, Any], name: str) -> float:
	value = _optional_metric(metrics, name)
	if value is None:
		raise HTTPException(status_code=500, detail=f"Regret-aware selector summary missing metric: {name}")
	return value


def _optional_metric(metrics: dict[str, Any], name: str) -> float | None:
	value = metrics.get(name)
	if isinstance(value, (int, float, str)):
		return float(value)
	return None


def _read_json_object(path: Path, *, not_found_detail: str) -> dict[str, Any]:
	if not path.exists():
		raise HTTPException(status_code=404, detail=f"{not_found_detail} Path: {path}")
	try:
		value = json.loads(path.read_text(encoding="utf-8"))
	except (OSError, json.JSONDecodeError) as error:
		raise HTTPException(status_code=500, detail=f"JSON artifact is unreadable: {path}") from error
	if not isinstance(value, dict):
		raise HTTPException(status_code=500, detail=f"JSON artifact must be an object: {path}")
	return value


def _dt_shadow_teacher_candidate_row(
	*,
	teacher_rows_path: Path,
	tenant_id: str,
	selected_candidate_id: str,
	label: str = "DT shadow",
) -> dict[str, Any]:
	if not teacher_rows_path.exists():
		raise HTTPException(status_code=404, detail=f"{label} teacher rows not found: {teacher_rows_path}")
	try:
		frame = pl.read_csv(teacher_rows_path, try_parse_dates=True)
	except (OSError, pl.exceptions.PolarsError) as error:
		raise HTTPException(status_code=500, detail=f"{label} teacher rows are unreadable: {teacher_rows_path}") from error
	if frame.height == 0:
		raise HTTPException(status_code=404, detail=f"{label} teacher rows are empty.")
	_reject_executable_shadow_frame(frame, label=f"{label} teacher rows")
	candidate_columns = [
		column
		for column in ("dt_candidate_id_target", "teacher_candidate_key", "candidate_model_name")
		if column in frame.columns
	]
	if not candidate_columns:
		raise HTTPException(status_code=500, detail=f"{label} teacher rows have no candidate id columns.")
	filter_expr = pl.col("tenant_id").cast(pl.String) == tenant_id
	candidate_expr = pl.lit(False)
	for column in candidate_columns:
		candidate_expr = candidate_expr | (pl.col(column).cast(pl.String) == selected_candidate_id)
	filtered = frame.filter(filter_expr & candidate_expr)
	if filtered.height == 0:
		raise HTTPException(
			status_code=404,
			detail=f"{label} selected candidate not found in teacher rows: {selected_candidate_id}",
		)
	return filtered.sort("anchor_timestamp").tail(1).row(0, named=True)


def _shadow_schedule_rows_from_candidate(
	*,
	candidate_row: dict[str, Any],
	selection: dict[str, Any],
	selected_candidate_id: str,
	selected_schedule_family: str,
) -> list[ShadowRecommendationSchedulePointResponse]:
	forecast_prices = _number_vector(candidate_row.get("forecast_price_uah_mwh_vector"))
	dispatch_values = _number_vector(candidate_row.get("dispatch_mw_vector"))
	soc_values = _number_vector(candidate_row.get("soc_fraction_vector"))
	horizon_rows = _candidate_horizon_rows(candidate_row)
	step_count = min(len(dispatch_values), len(forecast_prices))
	if step_count == 0 and horizon_rows:
		step_count = len(horizon_rows)
	if step_count == 0:
		raise HTTPException(status_code=500, detail="DT shadow selected candidate has no hourly schedule rows.")
	anchor_timestamp = _datetime_row_value(candidate_row["anchor_timestamp"], field_name="anchor_timestamp")
	schedule_inputs: list[dict[str, Any]] = [
		{
			"step_index": step_index,
			"interval_start": _shadow_interval_start(
				anchor_timestamp=anchor_timestamp,
				step_index=step_index,
				horizon_rows=horizon_rows,
			),
			"net_power_mw": _horizon_or_vector_value(
				horizon_rows=horizon_rows,
				step_index=step_index,
				horizon_key="net_power_mw",
				vector=dispatch_values,
			),
			"forecast_price_uah_mwh": _horizon_or_vector_value(
				horizon_rows=horizon_rows,
				step_index=step_index,
				horizon_key="forecast_price_uah_mwh",
				vector=forecast_prices,
			),
		}
		for step_index in range(step_count)
	]
	soc_pairs = _shadow_schedule_soc_pairs(
		tenant_id=str(candidate_row.get("tenant_id", "")),
		schedule_inputs=schedule_inputs,
		soc_values=soc_values,
	)
	return [
		_shadow_schedule_point(
			step_index=int(schedule_input["step_index"]),
			interval_start=schedule_input["interval_start"],
			net_power_mw=float(schedule_input["net_power_mw"]),
			forecast_price_uah_mwh=float(schedule_input["forecast_price_uah_mwh"]),
			soc_before=soc_pairs[index][0],
			soc_after=soc_pairs[index][1],
			selected_candidate_id=selected_candidate_id,
			selected_schedule_family=selected_schedule_family,
			selection=selection,
			candidate_row=candidate_row,
		)
		for index, schedule_input in enumerate(schedule_inputs)
	]


def _shadow_schedule_rows_from_augmented_gate_row(
	*,
	row: dict[str, Any],
	selection: dict[str, Any],
) -> list[ShadowRecommendationSchedulePointResponse]:
	payload = _json_mapping_value(row.get("evaluation_payload"))
	horizon = payload.get("horizon")
	if not isinstance(horizon, list) or not horizon:
		anchor_timestamp = _datetime_row_value(row["anchor_timestamp"], field_name="anchor_timestamp")
		horizon = [
			{
				"interval_start": anchor_timestamp + timedelta(hours=1),
				"net_power_mw": float(row.get("committed_power_mw", 0.0)),
				"forecast_price_uah_mwh": 0.0,
				"step_index": 0,
			}
		]
	schedule_inputs: list[dict[str, Any]] = [
		{
			"step_index": int(item.get("step_index", index)) if isinstance(item, dict) else index,
			"interval_start": _datetime_payload_value(
				item.get("interval_start") if isinstance(item, dict) else None,
				field_name="interval_start",
			),
			"net_power_mw": float(item.get("net_power_mw", 0.0)) if isinstance(item, dict) else 0.0,
			"forecast_price_uah_mwh": float(item.get("forecast_price_uah_mwh", 0.0)) if isinstance(item, dict) else 0.0,
		}
		for index, item in enumerate(horizon)
	]
	soc_pairs = _shadow_schedule_soc_pairs(
		tenant_id=str(row.get("tenant_id", "")),
		schedule_inputs=schedule_inputs,
		soc_values=[],
	)
	return [
		_shadow_schedule_point(
			step_index=int(schedule_input["step_index"]),
			interval_start=schedule_input["interval_start"],
			net_power_mw=float(schedule_input["net_power_mw"]),
			forecast_price_uah_mwh=float(schedule_input["forecast_price_uah_mwh"]),
			soc_before=soc_pairs[index][0],
			soc_after=soc_pairs[index][1],
			selected_candidate_id=str(selection["selected_candidate_id"]),
			selected_schedule_family=str(selection["selected_schedule_family"]),
			selection=selection,
			candidate_row=row,
		)
		for index, schedule_input in enumerate(schedule_inputs)
	]


def _shadow_schedule_point(
	*,
	step_index: int,
	interval_start: datetime,
	net_power_mw: float,
	forecast_price_uah_mwh: float,
	soc_before: float | None,
	soc_after: float | None,
	selected_candidate_id: str,
	selected_schedule_family: str,
	selection: dict[str, Any],
	candidate_row: dict[str, Any],
) -> ShadowRecommendationSchedulePointResponse:
	selected_regret = _float_payload_value(selection.get("dt_selected_regret_uah", candidate_row.get("regret_uah", 0.0)))
	selected_value = _float_payload_value(
		selection.get("dt_selected_value_uah", candidate_row.get("schedule_value_uah", candidate_row.get("decision_value_uah", 0.0)))
	)
	v2_regret = _optional_float(selection.get("v2_plus_regret_uah"))
	strict_regret = _optional_float(selection.get("strict_regret_uah"))
	v2_value = _optional_float(selection.get("v2_plus_value_uah"))
	strict_value = _optional_float(selection.get("strict_value_uah"))
	safety_violation_count = _int_value(candidate_row.get("safety_violation_count", 0))
	return ShadowRecommendationSchedulePointResponse(
		step_index=step_index,
		interval_start=interval_start,
		action=_shadow_action_label(net_power_mw),
		quantity_mw=abs(net_power_mw),
		recommended_net_power_mw=net_power_mw,
		forecast_price_uah_mwh=forecast_price_uah_mwh,
		soc_before_fraction=soc_before,
		soc_after_fraction=soc_after,
		selected_candidate_id=selected_candidate_id,
		schedule_family=selected_schedule_family,
		expected_value_uah=selected_value,
		regret_uah=selected_regret,
		regret_vs_v2_plus_uah=None if v2_regret is None else selected_regret - v2_regret,
		regret_vs_strict_uah=None if strict_regret is None else selected_regret - strict_regret,
		value_vs_v2_plus_uah=None if v2_value is None else selected_value - v2_value,
		value_vs_strict_uah=None if strict_value is None else selected_value - strict_value,
		gate_status="accepted_shadow_preview" if safety_violation_count == 0 else "blocked_shadow_preview",
		safety_status="no_safety_violations_recorded"
		if safety_violation_count == 0
		else f"{safety_violation_count}_safety_violation(s)",
		market_execution_enabled=False,
		market_order_payload_emitted=False,
		proposed_bid_status=OPERATOR_PROPOSED_BID_STATUS,
	)


def _dt_shadow_comparison_metrics(
	*,
	selection: dict[str, Any],
	packet: dict[str, Any],
) -> dict[str, float]:
	packet_metrics = packet.get("evaluation_metrics")
	metrics = {}
	if isinstance(packet_metrics, dict):
		metrics = {
			str(key): float(value)
			for key, value in packet_metrics.items()
			if isinstance(value, (int, float))
		}
	metric_aliases = {
		"dt_selected_mean_regret_uah": "dt_selected_regret_uah",
		"dt_selected_mean_value_uah": "dt_selected_value_uah",
		"v2_plus_mean_regret_uah": "v2_plus_regret_uah",
		"v2_plus_mean_value_uah": "v2_plus_value_uah",
		"strict_mean_regret_uah": "strict_regret_uah",
		"strict_mean_value_uah": "strict_value_uah",
		"behavior_cloning_mean_regret_uah": "behavior_cloning_regret_uah",
		"behavior_cloning_mean_value_uah": "behavior_cloning_value_uah",
	}
	for metric_name, selection_name in metric_aliases.items():
		if metric_name not in metrics and isinstance(selection.get(selection_name), (int, float)):
			metrics[metric_name] = float(selection[selection_name])
	if "dt_selected_mean_regret_uah" in metrics and "v2_plus_mean_regret_uah" in metrics:
		metrics["dt_minus_v2_plus_regret_uah"] = (
			metrics["dt_selected_mean_regret_uah"] - metrics["v2_plus_mean_regret_uah"]
		)
	if "dt_selected_mean_value_uah" in metrics and "v2_plus_mean_value_uah" in metrics:
		metrics["dt_minus_v2_plus_value_uah"] = (
			metrics["dt_selected_mean_value_uah"] - metrics["v2_plus_mean_value_uah"]
		)
	if "dt_selected_mean_regret_uah" in metrics and "strict_mean_regret_uah" in metrics:
		metrics["dt_minus_strict_regret_uah"] = (
			metrics["dt_selected_mean_regret_uah"] - metrics["strict_mean_regret_uah"]
		)
	if "dt_selected_mean_value_uah" in metrics and "strict_mean_value_uah" in metrics:
		metrics["dt_minus_strict_value_uah"] = (
			metrics["dt_selected_mean_value_uah"] - metrics["strict_mean_value_uah"]
		)
	return metrics


def _candidate_horizon_rows(candidate_row: dict[str, Any]) -> list[dict[str, Any]]:
	payload = _json_mapping_value(candidate_row.get("evaluation_payload"))
	horizon = payload.get("horizon")
	if not isinstance(horizon, list):
		return []
	return [item for item in horizon if isinstance(item, dict)]


def _shadow_interval_start(
	*,
	anchor_timestamp: datetime,
	step_index: int,
	horizon_rows: list[dict[str, Any]],
) -> datetime:
	if step_index < len(horizon_rows) and horizon_rows[step_index].get("interval_start") is not None:
		return _datetime_payload_value(horizon_rows[step_index]["interval_start"], field_name="interval_start")
	return anchor_timestamp + timedelta(minutes=LEVEL1_INTERVAL_MINUTES * (step_index + 1))


def _horizon_or_vector_value(
	*,
	horizon_rows: list[dict[str, Any]],
	step_index: int,
	horizon_key: str,
	vector: list[float],
) -> float:
	if step_index < len(horizon_rows) and horizon_rows[step_index].get(horizon_key) is not None:
		return float(horizon_rows[step_index][horizon_key])
	if step_index < len(vector):
		return float(vector[step_index])
	return 0.0


def _shadow_schedule_soc_pairs(
	*,
	tenant_id: str,
	schedule_inputs: list[dict[str, Any]],
	soc_values: list[float],
) -> list[tuple[float | None, float | None]]:
	explicit_pairs = [
		(_soc_before(soc_values, step_index), _soc_after(soc_values, step_index))
		for step_index in range(len(schedule_inputs))
	]
	if not _should_project_shadow_soc(schedule_inputs=schedule_inputs, explicit_pairs=explicit_pairs, soc_values=soc_values):
		return explicit_pairs
	if not tenant_id:
		return explicit_pairs

	starting_soc = explicit_pairs[0][0] if explicit_pairs and explicit_pairs[0][0] is not None else 0.5
	starting_soc = max(0.0, min(1.0, float(starting_soc)))
	try:
		battery_metrics = _resolve_tenant_battery_defaults(tenant_id=tenant_id).metrics
		projection = simulate_projected_battery_state(
			schedule=[
				ScheduledPowerPoint(
					interval_start=schedule_input["interval_start"],
					net_power_mw=float(schedule_input["net_power_mw"]),
				)
				for schedule_input in schedule_inputs
			],
			battery_metrics=battery_metrics,
			starting_soc_fraction=starting_soc,
			interval_minutes=LEVEL1_INTERVAL_MINUTES,
		)
	except (HTTPException, ValueError, TypeError):
		return explicit_pairs

	return [
		(trace_point.soc_before_fraction, trace_point.soc_after_fraction)
		for trace_point in projection.trace
	]


def _should_project_shadow_soc(
	*,
	schedule_inputs: list[dict[str, Any]],
	explicit_pairs: list[tuple[float | None, float | None]],
	soc_values: list[float],
) -> bool:
	non_idle_schedule = any(abs(float(schedule_input.get("net_power_mw", 0.0))) >= 0.005 for schedule_input in schedule_inputs)
	if not non_idle_schedule:
		return False
	if len(soc_values) < len(schedule_inputs) + 1:
		return True
	if any(before is None or after is None for before, after in explicit_pairs):
		return True
	complete_pairs = [
		(before, after)
		for before, after in explicit_pairs
		if before is not None and after is not None
	]
	return all(
		abs(after - before) <= 1e-6
		for before, after in complete_pairs
	)


def _number_vector(value: Any) -> list[float]:
	if isinstance(value, str):
		try:
			decoded = json.loads(value)
		except json.JSONDecodeError as error:
			raise HTTPException(status_code=500, detail="Schedule vector must be JSON encoded.") from error
		return _number_vector(decoded)
	if isinstance(value, list | tuple):
		return [float(item) for item in value]
	if isinstance(value, pl.Series):
		return [float(item) for item in value.to_list()]
	return []


def _soc_before(soc_values: list[float], step_index: int) -> float | None:
	if step_index < len(soc_values):
		return float(soc_values[step_index])
	return None


def _soc_after(soc_values: list[float], step_index: int) -> float | None:
	if step_index + 1 < len(soc_values):
		return float(soc_values[step_index + 1])
	if step_index < len(soc_values):
		return float(soc_values[step_index])
	return None


def _shadow_action_label(net_power_mw: float) -> str:
	if net_power_mw > 0.005:
		return "discharge"
	if net_power_mw < -0.005:
		return "charge"
	return "hold"


def _float_payload_value(value: Any) -> float:
	if isinstance(value, (int, float, str)):
		return float(value)
	return 0.0


def _optional_int(value: Any) -> int | None:
	if value is None:
		return None
	return int(value)


def _operator_forecast_generated_at(selected_strategy_id: str) -> datetime | None:
	if selected_strategy_id not in OFFICIAL_FORECAST_TO_LP_STRATEGY_IDS:
		return None
	forecast_frame = get_forecast_store().latest_forecast_observation_frame(
		model_names=[selected_strategy_id],
		limit_per_model=1,
	)
	if forecast_frame.height == 0 or "generated_at" not in forecast_frame.columns:
		return None
	value = forecast_frame.select("generated_at").max().item()
	return _datetime_row_value(value, field_name="generated_at")


def _build_operator_recommendation_response(
	*,
	tenant_id: str,
	strategy_id: str,
) -> OperatorRecommendationResponse:
	resolved_location = _resolve_requested_location(tenant_id=tenant_id, location_config_path=None)
	battery_defaults = _resolve_tenant_battery_defaults(tenant_id=tenant_id)
	battery_metrics = battery_defaults.metrics
	price_history = _build_tenant_aware_price_history(resolved_location)
	anchor_timestamp = _resolve_baseline_anchor(price_history)
	delivery_anchor_timestamp = _operator_dam_delivery_anchor(anchor_timestamp)
	historical_prices = _historical_prices_for_anchor(
		price_history,
		anchor_timestamp,
		required_through_timestamp=delivery_anchor_timestamp,
	)
	load_frame = _operator_load_frame(tenant_id=tenant_id, anchor_timestamp=delivery_anchor_timestamp)
	soc_resolution = _resolve_operator_soc(
		tenant_id=tenant_id,
		battery_defaults=battery_defaults,
		load_frame=load_frame,
	)
	v13_readiness = _operator_v13_readiness()
	available_strategies = _operator_strategy_options(
		tenant_id=tenant_id,
		v13_readiness=v13_readiness,
	)
	selected_strategy_id, selection_reason, selection_warnings = _select_operator_strategy(
		requested_strategy_id=strategy_id,
		options=available_strategies,
	)
	solver = HourlyDamBaselineSolver()
	try:
		baseline_solve_result = solver.solve_next_dispatch(
			historical_prices,
			battery_metrics=battery_metrics,
			current_soc_fraction=soc_resolution.starting_soc_fraction,
			anchor_timestamp=delivery_anchor_timestamp,
		)
		solve_result = _operator_solve_result_for_strategy(
			selected_strategy_id=selected_strategy_id,
			solver=solver,
			baseline_solve_result=baseline_solve_result,
			battery_metrics=battery_metrics,
			current_soc_fraction=soc_resolution.starting_soc_fraction,
			anchor_timestamp=delivery_anchor_timestamp,
		)
	except (RuntimeError, ValueError) as error:
		raise HTTPException(status_code=500, detail=str(error)) from error

	projected_simulation = simulate_projected_battery_state(
		schedule=_to_scheduled_power_points(solve_result),
		battery_metrics=battery_metrics,
		starting_soc_fraction=soc_resolution.starting_soc_fraction,
	)
	projected_state = _to_projected_battery_state_response(
		tenant_id=tenant_id,
		battery_metrics=battery_metrics,
		simulation_result=projected_simulation,
	)
	baseline_preview = _to_baseline_lp_preview_response(
		tenant_id=tenant_id,
		battery_metrics=battery_metrics,
		starting_soc_fraction=soc_resolution.starting_soc_fraction,
		starting_soc_source=soc_resolution.source,
		telemetry_freshness=None,
		resolved_location=resolved_location,
		solve_result=solve_result,
		projected_state=projected_state,
		read_model_anchor_timestamp=anchor_timestamp,
	)
	daily_value_uah = baseline_preview.economics.total_net_value_uah
	readiness_warnings = [*soc_resolution.warnings, *selection_warnings]
	policy_preview_frame = get_simulated_trade_store().latest_decision_transformer_policy_preview_frame(
		tenant_id=tenant_id,
		limit=24,
	)
	policy_context = _operator_policy_context(
		selected_strategy_id=selected_strategy_id,
		policy_preview_frame=policy_preview_frame,
	)
	target_delivery_window_start, target_delivery_window_end = _operator_target_delivery_window(
		baseline_preview.recommendation_schedule,
	)
	return OperatorRecommendationResponse(
		tenant_id=tenant_id,
		market_scope=OPERATOR_MARKET_SCOPE,
		market_venue=LEVEL1_MARKET_VENUE,
		interval_minutes=LEVEL1_INTERVAL_MINUTES,
		anchor_timestamp=anchor_timestamp,
		forecast_generated_at=_operator_forecast_generated_at(selected_strategy_id),
		target_delivery_window_start=target_delivery_window_start,
		target_delivery_window_end=target_delivery_window_end,
		market_execution_enabled=False,
		read_model_boundary=OPERATOR_READ_MODEL_BOUNDARY,
		market_gate_status=OPERATOR_MARKET_GATE_STATUS,
		bid_eligibility_status=OPERATOR_BID_ELIGIBILITY_STATUS,
		proposed_bid_status=OPERATOR_PROPOSED_BID_STATUS,
		v13_readiness=v13_readiness,
		selected_strategy_id=selected_strategy_id,
		selection_reason=selection_reason,
		forecast_source=_operator_forecast_source(selected_strategy_id),
		soc_source=soc_resolution.source,
		review_required=soc_resolution.review_required or bool(selection_warnings),
		readiness_warnings=list(readiness_warnings),
		policy_mode=policy_context["policy_mode"],
		selected_policy_id=policy_context["selected_policy_id"],
		policy_explanation=policy_context["policy_explanation"],
		policy_readiness=policy_context["policy_readiness"],
		policy_forecast_context_source=str(policy_context["forecast_context_source"]),
		policy_forecast_context_row_count=int(policy_context["forecast_context_row_count"]),
		policy_forecast_context_coverage_ratio=float(policy_context["forecast_context_coverage_ratio"]),
		policy_forecast_context_warning=policy_context["forecast_context_warning"],
		available_strategies=available_strategies,
		forecast_model_series=_operator_forecast_model_series(
			tenant_id=tenant_id,
			solve_result=solve_result,
		),
		value_gap_series=_operator_value_gap_series(baseline_preview),
		load_forecast=_to_operator_load_forecast_points(load_frame),
		soc_projection=_to_operator_soc_projection_points(
			load_frame=load_frame,
			solve_result=solve_result,
			soc_resolution=soc_resolution,
			battery_metrics=battery_metrics,
		),
		recommendation_schedule=baseline_preview.recommendation_schedule,
		bid_recommendation_preview=baseline_preview.bid_recommendation_preview,
		daily_value_uah=daily_value_uah,
		hold_baseline_value_uah=0.0,
		value_vs_hold_uah=daily_value_uah,
		economics=baseline_preview.economics,
	)


def _operator_solve_result_for_strategy(
	*,
	selected_strategy_id: str,
	solver: HourlyDamBaselineSolver,
	baseline_solve_result: BaselineSolveResult,
	battery_metrics: BatteryPhysicalMetrics,
	current_soc_fraction: float,
	anchor_timestamp: datetime,
) -> BaselineSolveResult:
	if selected_strategy_id == OFFLINE_V2_PLUS_OPERATOR_STRATEGY_ID:
		return solver.solve_dispatch_from_forecast(
			forecast=_operator_v2_plus_preview_forecast(baseline_solve_result.forecast),
			battery_metrics=battery_metrics,
			current_soc_fraction=current_soc_fraction,
			anchor_timestamp=anchor_timestamp,
			commit_reason="schedule_value_learner_v2_plus_read_model_preview",
		)

	forecast = _operator_forecast_store_forecast(
		model_name=selected_strategy_id,
		anchor_timestamp=anchor_timestamp,
	)
	if not forecast:
		return baseline_solve_result
	return solver.solve_dispatch_from_forecast(
		forecast=forecast,
		battery_metrics=battery_metrics,
		current_soc_fraction=current_soc_fraction,
		anchor_timestamp=anchor_timestamp,
		commit_reason=f"{selected_strategy_id}_forecast_to_lp_preview",
	)


def _operator_v2_plus_preview_forecast(
	baseline_forecast: list[BaselineForecastPoint],
) -> list[BaselineForecastPoint]:
	"""Build a demo/read-model V2+ preview forecast without claiming live learned dispatch."""
	if not baseline_forecast:
		return []

	prices = [point.predicted_price_uah_mwh for point in baseline_forecast]
	mean_price = sum(prices) / len(prices)
	ordered_indices = sorted(range(len(prices)), key=lambda index: prices[index])
	extrema_count = min(OFFLINE_V2_PLUS_PREVIEW_EXTREMA_COUNT, max(1, len(ordered_indices) // 4))
	low_indices = set(ordered_indices[:extrema_count])
	high_indices = set(ordered_indices[-extrema_count:])
	preview_forecast: list[BaselineForecastPoint] = []
	for index, point in enumerate(baseline_forecast):
		centered_price = point.predicted_price_uah_mwh - mean_price
		adjusted_price = mean_price + centered_price * OFFLINE_V2_PLUS_PREVIEW_SPREAD_SCALE
		if index in high_indices:
			adjusted_price += OFFLINE_V2_PLUS_PREVIEW_RANK_DELTA_UAH_MWH
		if index in low_indices:
			adjusted_price -= OFFLINE_V2_PLUS_PREVIEW_RANK_DELTA_UAH_MWH
		preview_forecast.append(
			BaselineForecastPoint(
				forecast_timestamp=point.forecast_timestamp,
				source_timestamp=point.source_timestamp,
				predicted_price_uah_mwh=max(
					FUTURE_STACK_DAM_PRICE_CAP_MIN_UAH_MWH,
					min(FUTURE_STACK_DAM_PRICE_CAP_MAX_UAH_MWH, adjusted_price),
				),
			)
		)
	return preview_forecast


def _operator_forecast_store_forecast(
	*,
	model_name: str,
	anchor_timestamp: datetime,
) -> list[BaselineForecastPoint]:
	if model_name not in OFFICIAL_FORECAST_TO_LP_STRATEGY_IDS:
		return []
	forecast_frame = get_forecast_store().latest_forecast_observation_frame(
		model_names=[model_name],
		limit_per_model=24,
	)
	if forecast_frame.height == 0:
		return []
	points: list[BaselineForecastPoint] = []
	for row in forecast_frame.sort("forecast_timestamp").iter_rows(named=True):
		payload = _forecast_store_horizon_payload(row)
		points.append(
			BaselineForecastPoint(
				forecast_timestamp=_datetime_payload_value(
					payload["interval_start"],
					field_name="forecast_timestamp",
				),
				source_timestamp=anchor_timestamp,
				predicted_price_uah_mwh=float(payload["forecast_price_uah_mwh"]),
			)
		)
	return points


def _to_operator_status_response(record: OperatorStatusRecord) -> OperatorStatusResponse:
	return OperatorStatusResponse(
		tenant_id=record.tenant_id,
		flow_type=record.flow_type,
		status=record.status,
		updated_at=record.updated_at.isoformat(),
		payload=record.payload,
		last_error=record.last_error,
	)


@app.get(
	"/health",
	tags=["system"],
	summary="Health check",
	description="Returns a minimal liveness payload for the API process.",
)
def healthcheck() -> dict[str, str]:
	return {"status": "ok"}


@app.get(
	"/tenants",
	response_model=list[TenantSummaryResponse],
	tags=["tenants"],
	summary="List weather-aware tenants",
	description=(
		"Returns the canonical tenant registry used for location-aware weather experiments. "
		"Each entry includes tenant identity plus resolved latitude, longitude, and timezone."
	),
)
def list_tenants() -> list[TenantSummaryResponse]:
	tenants = list_available_weather_tenants()
	return [TenantSummaryResponse.model_validate(tenant) for tenant in tenants if tenant.get("tenant_id")]


@app.post(
	"/weather/run-config",
	response_model=WeatherRunConfigResponse,
	tags=["weather"],
	summary="Build Dagster weather run config",
	description=(
		"Builds the Dagster run-config payload for weather_forecast_bronze from a tenant_id. "
		"Also returns the resolved location that will be used by the weather Bronze asset."
	),
)
def build_weather_run_config_endpoint(request: WeatherRunConfigRequest) -> WeatherRunConfigResponse:
	resolved_location = _resolve_requested_location(
		tenant_id=request.tenant_id,
		location_config_path=request.location_config_path,
	)
	run_config = build_weather_asset_run_config(
		tenant_id=request.tenant_id,
		location_config_path=request.location_config_path,
	)
	response = WeatherRunConfigResponse(
		tenant_id=request.tenant_id,
		run_config=run_config,
		resolved_location=_location_response_from_model(resolved_location),
	)
	_persist_operator_status(
		tenant_id=request.tenant_id,
		flow_type=OperatorFlowType.WEATHER_CONTROL,
		status=OperatorFlowStatus.PREPARED,
		payload=response.model_dump(),
	)
	return response


@app.post(
	"/weather/materialize",
	response_model=WeatherMaterializeResponse,
	tags=["weather"],
	summary="Materialize weather experiment assets",
	description=(
		"Materializes weather_forecast_bronze and optionally dam_price_history for a selected tenant. "
		"This is the API-level trigger for location-aware MVP experiment runs."
	),
)
def materialize_weather_assets(request: WeatherMaterializeRequest) -> WeatherMaterializeResponse:
	_persist_operator_status(
		tenant_id=request.tenant_id,
		flow_type=OperatorFlowType.WEATHER_CONTROL,
		status=OperatorFlowStatus.RUNNING,
		payload={
			"tenant_id": request.tenant_id,
			"include_price_history": request.include_price_history,
		},
	)
	resolved_location = _resolve_requested_location(
		tenant_id=request.tenant_id,
		location_config_path=request.location_config_path,
	)
	run_config = build_weather_asset_run_config(
		tenant_id=request.tenant_id,
		location_config_path=request.location_config_path,
	)
	selected_assets = _selected_weather_assets(include_price_history=request.include_price_history)
	result = dg.materialize(selected_assets, run_config=run_config)
	if not result.success:
		_persist_operator_status(
			tenant_id=request.tenant_id,
			flow_type=OperatorFlowType.WEATHER_CONTROL,
			status=OperatorFlowStatus.FAILED,
			payload={
				"tenant_id": request.tenant_id,
				"include_price_history": request.include_price_history,
			},
			last_error="Dagster materialization failed.",
		)
		raise HTTPException(status_code=500, detail="Dagster materialization failed.")

	response = WeatherMaterializeResponse(
		tenant_id=request.tenant_id,
		selected_assets=[asset.key.path[-1] for asset in selected_assets],
		run_config=run_config,
		resolved_location=_location_response_from_model(resolved_location),
		success=True,
	)
	_persist_operator_status(
		tenant_id=request.tenant_id,
		flow_type=OperatorFlowType.WEATHER_CONTROL,
		status=OperatorFlowStatus.COMPLETED,
		payload=response.model_dump(),
	)
	return response


@app.get(
	"/dashboard/signal-preview",
	response_model=DashboardSignalPreviewResponse,
	tags=["weather"],
	summary="Build dashboard signal preview",
	description=(
		"Builds a tenant-aware signal preview for the operator dashboard. "
		"This read model powers market pulse and dispatch preview charts."
	),
)
def dashboard_signal_preview(
	tenant_id: str,
	location_config_path: str | None = None,
) -> DashboardSignalPreviewResponse:
	response = _build_signal_preview(
		tenant_id=tenant_id,
		location_config_path=location_config_path,
	)
	_persist_operator_status(
		tenant_id=tenant_id,
		flow_type=OperatorFlowType.SIGNAL_PREVIEW,
		status=OperatorFlowStatus.COMPLETED,
		payload=response.model_dump(),
	)
	return response


@app.get(
	"/dashboard/operator-status",
	response_model=OperatorStatusResponse,
	tags=["weather"],
	summary="Get persisted operator flow status",
	description=(
		"Returns the latest persisted operator-visible state for a tenant and flow. "
		"This is the backend-owned status contract for dashboard read models."
	),
)
def get_operator_status(
	tenant_id: str,
	flow_type: OperatorFlowType,
) -> OperatorStatusResponse:
	store = get_operator_status_store()
	record = store.get_status(tenant_id=tenant_id, flow_type=flow_type)
	if record is None:
		raise HTTPException(status_code=404, detail="Operator flow status not found.")

	return _to_operator_status_response(record)


@app.get(
	"/dashboard/gatekeeper-validation-status",
	response_model=GatekeeperValidationStatusResponse,
	tags=["weather"],
	summary="Get Bid Gatekeeper validation status",
	description=(
		"Returns the latest Bid Gatekeeper validation-failure read model for the selected tenant. "
		"`NO_BID` is a market-stage fallback for failed ProposedBid validation; `HOLD` is a "
		"physical dispatch action after market-stage decisions."
	),
)
def dashboard_gatekeeper_validation_status(
	tenant_id: str,
) -> GatekeeperValidationStatusResponse:
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	latest_failure = get_validation_failure_store().latest_failure(tenant_id=tenant_id)
	if latest_failure is None:
		return GatekeeperValidationStatusResponse(
			tenant_id=tenant_id,
			status="no_validation_failures_recorded",
			no_bid_semantics=NO_BID_SEMANTICS,
			hold_semantics=HOLD_SEMANTICS,
		)

	return GatekeeperValidationStatusResponse(
		tenant_id=tenant_id,
		status="blocked",
		validation_stage=latest_failure.validation_stage.value,
		contract_type=latest_failure.contract_type,
		canonical_outcome=latest_failure.canonical_outcome,
		venue=latest_failure.venue,
		interval_start=latest_failure.interval_start,
		duration_minutes=latest_failure.duration_minutes,
		failure_reason=latest_failure.failure_reason,
		created_at=latest_failure.created_at,
		no_bid_semantics=NO_BID_SEMANTICS,
		hold_semantics=HOLD_SEMANTICS,
		latest_failure_id=latest_failure.failure_id,
	)


@app.post(
	"/dashboard/projected-battery-state",
	response_model=ProjectedBatteryStateResponse,
	tags=["weather"],
	summary="Build projected battery state preview",
	description=(
		"Simulates hourly projected SOC, throughput, and degradation-aware economics "
		"for a signed MW recommendation schedule."
	),
)
def build_projected_battery_state_preview(
	request: ProjectedBatteryStateRequest,
) -> ProjectedBatteryStateResponse:
	battery_metrics, starting_soc_fraction, schedule = _resolve_projection_request(request)
	try:
		simulation_result = simulate_projected_battery_state(
			schedule=schedule,
			battery_metrics=battery_metrics,
			starting_soc_fraction=starting_soc_fraction,
		)
	except ValueError as error:
		raise HTTPException(status_code=400, detail=str(error)) from error

	response = _to_projected_battery_state_response(
		tenant_id=request.tenant_id,
		battery_metrics=battery_metrics,
		simulation_result=simulation_result,
	)
	_persist_operator_status(
		tenant_id=request.tenant_id,
		flow_type=OperatorFlowType.BASELINE_LP,
		status=OperatorFlowStatus.COMPLETED,
		payload=response.model_dump(mode="json"),
	)
	return response


@app.get(
	"/dashboard/battery-state",
	response_model=DashboardBatteryStateResponse,
	tags=["weather"],
	summary="Get latest battery telemetry state",
	description=(
		"Returns the latest physical telemetry snapshot and the latest hourly Level 1 battery-state "
		"snapshot for the selected tenant."
	),
)
def dashboard_battery_state(tenant_id: str) -> DashboardBatteryStateResponse:
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	store = get_battery_telemetry_store()
	latest_telemetry = store.get_latest_battery_telemetry(tenant_id=tenant_id)
	latest_snapshot = store.get_latest_hourly_snapshot(tenant_id=tenant_id)
	fallback_reason = None
	if latest_telemetry is None and latest_snapshot is None:
		fallback_reason = "telemetry_unavailable"
	elif latest_snapshot is None:
		fallback_reason = "hourly_snapshot_unavailable"
	elif latest_snapshot.telemetry_freshness != "fresh":
		fallback_reason = "hourly_snapshot_stale"

	return DashboardBatteryStateResponse(
		tenant_id=tenant_id,
		latest_telemetry=None if latest_telemetry is None else _to_battery_telemetry_response(latest_telemetry),
		hourly_snapshot=None if latest_snapshot is None else _to_hourly_snapshot_response(latest_snapshot),
		fallback_reason=fallback_reason,
		telemetry_ingest_source=_battery_telemetry_ingest_source_response(tenant_id),
	)


@app.get(
	"/dashboard/exogenous-signals",
	response_model=DashboardExogenousSignalsResponse,
	tags=["weather"],
	summary="Get latest exogenous signals",
	description=(
		"Returns the latest tenant weather metadata and public Ukrenergo grid-event signal read model. "
		"These are explanatory exogenous covariates, not live trading claims."
	),
)
def dashboard_exogenous_signals(tenant_id: str) -> DashboardExogenousSignalsResponse:
	return _build_exogenous_signals_response(tenant_id)


@app.get(
	"/dashboard/forecast-strategy-comparison",
	response_model=ForecastStrategyComparisonResponse,
	tags=["weather"],
	summary="Get forecast strategy comparison",
	description=(
		"Returns the latest persisted Gold-layer comparison of strict similar-day, NBEATSx, and TFT "
		"forecast candidates after routing each forecast through the same LP and oracle-regret scoring path. "
		"This endpoint is a read model and does not return ProposedBid, ClearedTrade, or DispatchCommand contracts."
	),
)
def dashboard_forecast_strategy_comparison(
	tenant_id: str,
) -> ForecastStrategyComparisonResponse:
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	evaluation_frame = get_strategy_evaluation_store().latest_evaluation_frame(tenant_id=tenant_id)
	return _to_forecast_strategy_comparison_response(
		tenant_id=tenant_id,
		evaluation_frame=evaluation_frame,
	)


@app.get(
	"/dashboard/real-data-benchmark",
	response_model=RealDataBenchmarkResponse,
	tags=["weather"],
	summary="Get real-data benchmark",
	description=(
		"Returns the latest persisted real-data rolling-origin benchmark summary and rows "
		"for strict similar-day, NBEATSx, and TFT forecast candidates."
	),
)
def dashboard_real_data_benchmark(
	tenant_id: str,
) -> RealDataBenchmarkResponse:
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	evaluation_frame = get_strategy_evaluation_store().latest_real_data_benchmark_frame(tenant_id=tenant_id)
	return _to_real_data_benchmark_response(
		tenant_id=tenant_id,
		evaluation_frame=evaluation_frame,
	)


@app.get(
	"/dashboard/future-stack-preview",
	response_model=FutureStackPreviewResponse,
	tags=["weather"],
	summary="Get future forecast and policy stack preview",
	description=(
		"Returns NBEATSx/TFT forecast-series rows for the operator/defense future-stack graphs. "
		"Official backend status is explicit; compact/calibrated rows remain visible fallbacks."
	),
)
def dashboard_future_stack_preview(
	tenant_id: str,
) -> FutureStackPreviewResponse:
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	evaluation_frame = get_strategy_evaluation_store().latest_real_data_benchmark_frame(tenant_id=tenant_id)
	forecast_observation_frame = get_forecast_store().latest_forecast_observation_frame(
		model_names=FUTURE_STACK_FORECAST_MODEL_NAMES,
		limit_per_model=24,
	)
	return _to_future_stack_preview_response(
		tenant_id=tenant_id,
		evaluation_frame=evaluation_frame,
		forecast_observation_frame=forecast_observation_frame,
	)


@app.get(
	"/dashboard/calibrated-ensemble-benchmark",
	response_model=RealDataBenchmarkResponse,
	tags=["weather"],
	summary="Get calibrated ensemble benchmark",
	description=(
		"Returns the latest persisted calibrated value-aware ensemble gate rows. "
		"The gate chooses between strict similar-day and horizon-aware regret-weighted TFT/NBEATSx "
		"using only pre-anchor validation history."
	),
)
def dashboard_calibrated_ensemble_benchmark(
	tenant_id: str,
) -> RealDataBenchmarkResponse:
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	evaluation_frame = get_strategy_evaluation_store().latest_strategy_kind_frame(
		tenant_id=tenant_id,
		strategy_kind=CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND,
	)
	return _to_real_data_benchmark_response(
		tenant_id=tenant_id,
		evaluation_frame=evaluation_frame,
	)


@app.get(
	"/dashboard/risk-adjusted-value-gate",
	response_model=RealDataBenchmarkResponse,
	tags=["weather"],
	summary="Get risk-adjusted value gate",
	description=(
		"Returns the latest persisted risk-adjusted value gate rows. "
		"The gate chooses between strict similar-day and horizon-aware regret-weighted TFT/NBEATSx "
		"using only prior-anchor median regret, tail regret, and win rate."
	),
)
def dashboard_risk_adjusted_value_gate(
	tenant_id: str,
) -> RealDataBenchmarkResponse:
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	evaluation_frame = get_strategy_evaluation_store().latest_strategy_kind_frame(
		tenant_id=tenant_id,
		strategy_kind=RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND,
	)
	return _to_real_data_benchmark_response(
		tenant_id=tenant_id,
		evaluation_frame=evaluation_frame,
	)


@app.get(
	"/dashboard/forecast-dispatch-sensitivity",
	response_model=ForecastDispatchSensitivityResponse,
	tags=["weather"],
	summary="Get forecast-dispatch sensitivity",
	description=(
		"Returns forecast-to-dispatch diagnostic rows derived from the latest horizon-aware "
		"regret-weighted benchmark. Buckets separate low regret, forecast error, spread-objective "
		"mismatch, and LP dispatch sensitivity."
	),
)
def dashboard_forecast_dispatch_sensitivity(
	tenant_id: str,
) -> ForecastDispatchSensitivityResponse:
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	evaluation_frame = get_strategy_evaluation_store().latest_strategy_kind_frame(
		tenant_id=tenant_id,
		strategy_kind=HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
	)
	return _to_forecast_dispatch_sensitivity_response(
		tenant_id=tenant_id,
		evaluation_frame=evaluation_frame,
	)


@app.get(
	"/dashboard/dfl-relaxed-pilot",
	response_model=DflRelaxedPilotResponse,
	tags=["weather"],
	summary="Get relaxed DFL pilot",
	description=(
		"Returns persisted relaxed-LP DFL pilot rows for the selected tenant. "
		"This is a differentiable optimization research primitive, not a full DFL claim."
	),
)
def dashboard_dfl_relaxed_pilot(
	tenant_id: str,
) -> DflRelaxedPilotResponse:
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	relaxed_pilot_frame = get_dfl_training_store().latest_relaxed_pilot_frame(tenant_id=tenant_id)
	return _to_dfl_relaxed_pilot_response(
		tenant_id=tenant_id,
		relaxed_pilot_frame=relaxed_pilot_frame,
	)


@app.get(
	"/dashboard/dfl-schedule-value-production-gate",
	response_model=DflScheduleValueProductionGateResponse,
	tags=["weather"],
	summary="Get DFL schedule-value production gate",
	description=(
		"Returns the latest persisted source-specific Schedule/Value Learner V2 promotion gate. "
		"Rows are offline/read-model strategy evidence only; market execution remains disabled."
	),
)
def dashboard_dfl_schedule_value_production_gate() -> DflScheduleValueProductionGateResponse:
	gate_frame = get_dfl_training_store().latest_schedule_value_production_gate_frame()
	return _to_dfl_schedule_value_production_gate_response(gate_frame=gate_frame)


@app.get(
	"/dashboard/decision-transformer-trajectories",
	response_model=DecisionTransformerTrajectoryResponse,
	tags=["weather"],
	summary="Get Decision Transformer trajectories",
	description=(
		"Returns persisted offline Decision Transformer trajectory rows for the selected tenant. "
		"Rows are training/evaluation data only and are not live policy actions."
	),
)
def dashboard_decision_transformer_trajectories(
	tenant_id: str,
	limit: int = 200,
) -> DecisionTransformerTrajectoryResponse:
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	trajectory_frame = get_simulated_trade_store().latest_decision_transformer_trajectory_frame(
		tenant_id=tenant_id,
		limit=limit,
	)
	return _to_decision_transformer_trajectory_response(
		tenant_id=tenant_id,
		trajectory_frame=trajectory_frame,
	)


@app.get(
	"/dashboard/decision-policy-preview",
	response_model=DecisionPolicyPreviewResponse,
	tags=["weather"],
	summary="Get Decision Transformer policy preview",
	description=(
		"Returns persisted offline Decision Transformer policy-preview rows after deterministic battery "
		"projection. This can drive operator preview graphs, but it is not market execution."
	),
)
def dashboard_decision_policy_preview(
	tenant_id: str,
	limit: int = 200,
) -> DecisionPolicyPreviewResponse:
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	policy_preview_frame = get_simulated_trade_store().latest_decision_transformer_policy_preview_frame(
		tenant_id=tenant_id,
		limit=limit,
	)
	return _to_decision_policy_preview_response(
		tenant_id=tenant_id,
		policy_preview_frame=policy_preview_frame,
	)


@app.get(
	"/dashboard/simulated-live-trading",
	response_model=SimulatedLiveTradingResponse,
	tags=["weather"],
	summary="Get simulated live trading",
	description=(
		"Returns persisted simulated live-trading replay rows for the selected tenant. "
		"Rows are marked simulated and never contain real settlement identifiers."
	),
)
def dashboard_simulated_live_trading(
	tenant_id: str,
	limit: int = 200,
) -> SimulatedLiveTradingResponse:
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	live_trading_frame = get_simulated_trade_store().latest_simulated_live_trading_frame(
		tenant_id=tenant_id,
		limit=limit,
	)
	return _to_simulated_live_trading_response(
		tenant_id=tenant_id,
		live_trading_frame=live_trading_frame,
	)


@app.get(
	"/dashboard/academic-mvp-readiness",
	response_model=AcademicMvpReadinessResponse,
	tags=["weather"],
	summary="Get credentialless academic MVP readiness",
	description=(
		"Returns the materialized credentialless academic MVP readiness packet. "
		"This is a read-only thesis/demo artifact: DAM operator preview and "
		"DT/LAVA prototype gates may pass while market submission, DT training, "
		"and market execution remain disabled."
	),
)
def dashboard_academic_mvp_readiness() -> AcademicMvpReadinessResponse:
	return _academic_mvp_readiness_response()


@app.get(
	"/dashboard/operator-recommendation",
	response_model=OperatorRecommendationResponse,
	tags=["weather"],
	summary="Get operator recommendation",
	description=(
		"Returns a live operator read model that combines current or projected SOC, configured tenant "
		"load/PV schedule, available materialized strategies, and a feasible hourly recommendation."
	),
)
def dashboard_operator_recommendation(
	tenant_id: str,
	strategy_id: str = "strict_similar_day",
) -> OperatorRecommendationResponse:
	response = _build_operator_recommendation_response(
		tenant_id=tenant_id,
		strategy_id=strategy_id,
	)
	_persist_operator_status(
		tenant_id=tenant_id,
		flow_type=OperatorFlowType.BASELINE_LP,
		status=OperatorFlowStatus.COMPLETED,
		payload=response.model_dump(mode="json"),
	)
	return response


@app.get(
	"/dashboard/shadow-recommendation-preview",
	response_model=ShadowRecommendationPreviewResponse,
	tags=["weather"],
	summary="Get shadow recommendation preview",
	description=(
		"Returns a manually selected shadow/diagnostic recommendation preview for the operator dashboard. "
		"These previews are read-model evidence only: no ProposedBid, no market order payload, no DT/LAVA "
		"promotion, and market_execution_enabled remains false."
	),
)
def dashboard_shadow_recommendation_preview(
	tenant_id: str,
	preview_source: str = "dt_shadow",
	target_delivery_window_start: datetime | None = None,
) -> ShadowRecommendationPreviewResponse:
	_resolve_tenant_battery_defaults(tenant_id=tenant_id)
	return _operator_shadow_recommendation_preview_response(
		tenant_id=tenant_id,
		preview_source=preview_source,
		target_delivery_window_start=target_delivery_window_start,
	)


@app.get(
	"/dashboard/baseline-lp-preview",
	response_model=BaselineLpPreviewResponse,
	tags=["weather"],
	summary="Build baseline LP preview",
	description=(
		"Returns a tenant-aware baseline LP recommendation preview for the next DAM delivery day "
		"with hourly forecast, signed MW schedule, projected SOC trace, and UAH economics."
	),
)
def build_baseline_lp_preview(
	tenant_id: str,
) -> BaselineLpPreviewResponse:
	resolved_location = _resolve_requested_location(tenant_id=tenant_id, location_config_path=None)
	battery_defaults = _resolve_tenant_battery_defaults(tenant_id=tenant_id)
	battery_metrics = battery_defaults.metrics
	starting_soc_resolution = _resolve_starting_soc_for_baseline(
		tenant_id=tenant_id,
		battery_defaults=battery_defaults,
	)
	starting_soc_fraction = starting_soc_resolution.starting_soc_fraction
	price_history = _build_tenant_aware_price_history(resolved_location)
	anchor_timestamp = _resolve_baseline_anchor(price_history)
	delivery_anchor_timestamp = _operator_dam_delivery_anchor(anchor_timestamp)
	historical_prices = _historical_prices_for_anchor(
		price_history,
		anchor_timestamp,
		required_through_timestamp=delivery_anchor_timestamp,
	)
	solver = HourlyDamBaselineSolver()
	try:
		solve_result = solver.solve_next_dispatch(
			historical_prices,
			battery_metrics=battery_metrics,
			current_soc_fraction=starting_soc_fraction,
			anchor_timestamp=delivery_anchor_timestamp,
		)
	except (RuntimeError, ValueError) as error:
		raise HTTPException(status_code=500, detail=str(error)) from error

	projected_simulation = simulate_projected_battery_state(
		schedule=_to_scheduled_power_points(solve_result),
		battery_metrics=battery_metrics,
		starting_soc_fraction=starting_soc_fraction,
	)
	projected_state = _to_projected_battery_state_response(
		tenant_id=tenant_id,
		battery_metrics=battery_metrics,
		simulation_result=projected_simulation,
	)
	response = _to_baseline_lp_preview_response(
		tenant_id=tenant_id,
		battery_metrics=battery_metrics,
		starting_soc_fraction=starting_soc_fraction,
		starting_soc_source=starting_soc_resolution.source,
		telemetry_freshness=starting_soc_resolution.telemetry_freshness,
		resolved_location=resolved_location,
		solve_result=solve_result,
		projected_state=projected_state,
		read_model_anchor_timestamp=anchor_timestamp,
	)
	_persist_operator_status(
		tenant_id=tenant_id,
		flow_type=OperatorFlowType.BASELINE_LP,
		status=OperatorFlowStatus.COMPLETED,
		payload=response.model_dump(mode="json"),
	)
	return response
