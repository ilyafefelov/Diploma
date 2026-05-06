from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import cache
import json
import math
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
from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics
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


class BaselinePreviewEconomicsResponse(BaseModel):
	total_gross_market_value_uah: float
	total_degradation_penalty_uah: float
	total_net_value_uah: float
	total_throughput_mwh: float


class BaselineLpPreviewResponse(BaseModel):
	tenant_id: str
	market_venue: str
	interval_minutes: int
	starting_soc_fraction: float
	starting_soc_source: str
	battery_metrics: BatteryPhysicalMetrics
	resolved_location: WeatherLocationResponse
	forecast: list[BaselineForecastPointResponse]
	recommendation_schedule: list[BaselineRecommendationPointResponse]
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


class DashboardBatteryStateResponse(BaseModel):
	tenant_id: str
	latest_telemetry: BatteryTelemetryObservationResponse | None
	hourly_snapshot: BatteryStateHourlySnapshotResponse | None
	fallback_reason: str | None


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


class OperatorRecommendationResponse(BaseModel):
	tenant_id: str
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
	available_strategies: list[OperatorStrategyOptionResponse]
	forecast_model_series: list[FutureForecastSeriesResponse]
	value_gap_series: list[OperatorValueGapPointResponse]
	load_forecast: list[OperatorLoadForecastPointResponse]
	soc_projection: list[OperatorSocProjectionPointResponse]
	recommendation_schedule: list[BaselineRecommendationPointResponse]
	daily_value_uah: float
	hold_baseline_value_uah: float
	value_vs_hold_uah: float
	economics: BaselinePreviewEconomicsResponse


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


def _historical_prices_for_anchor(price_history: pl.DataFrame, anchor_timestamp: datetime) -> pl.DataFrame:
	historical_prices = price_history.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) <= anchor_timestamp)
	if historical_prices.height < 168:
		raise ValueError("At least 168 hourly DAM observations are required before the anchor timestamp.")
	return historical_prices


def _to_scheduled_power_points(schedule_result: BaselineSolveResult) -> list[ScheduledPowerPoint]:
	return [
		ScheduledPowerPoint(interval_start=point.interval_start, net_power_mw=point.net_power_mw)
		for point in schedule_result.schedule
	]


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
) -> BaselineLpPreviewResponse:
	total_gross_market_value_uah = sum(point.gross_market_value_uah for point in solve_result.schedule)
	total_degradation_penalty_uah = sum(point.degradation_penalty_uah for point in solve_result.schedule)
	total_net_value_uah = sum(point.net_objective_value_uah for point in solve_result.schedule)
	total_throughput_mwh = sum(point.throughput_mwh for point in solve_result.schedule)
	return BaselineLpPreviewResponse(
		tenant_id=tenant_id,
		market_venue=LEVEL1_MARKET_VENUE,
		interval_minutes=LEVEL1_INTERVAL_MINUTES,
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
		recommendation_schedule=[
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
		],
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


def _operator_strategy_options(*, tenant_id: str) -> list[OperatorStrategyOptionResponse]:
	benchmark_frame = get_strategy_evaluation_store().latest_real_data_benchmark_frame(tenant_id=tenant_id)
	metrics_by_model = _operator_strategy_metrics_by_model(benchmark_frame)
	forecast_store_cap_counts = _available_forecast_store_model_cap_counts()
	policy_preview_frame = get_simulated_trade_store().latest_decision_transformer_policy_preview_frame(
		tenant_id=tenant_id,
		limit=24,
	)
	policy_ready = _decision_policy_preview_is_ready(policy_preview_frame)
	options = [
		_operator_strategy_option(
			strategy_id="strict_similar_day",
			label="Strict similar-day control",
			reason="control baseline",
			metrics_by_model=metrics_by_model,
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
			enabled=policy_ready,
			reason="ready live preview; market execution disabled" if policy_ready else "offline policy preview missing or failed safety projection",
			mean_regret_uah=_policy_mean_value_gap(policy_preview_frame) if policy_ready else None,
			win_rate=1.0 if policy_ready else None,
		),
	]
	if not metrics_by_model:
		options[0] = options[0].model_copy(update={"enabled": True, "mean_regret_uah": None, "win_rate": None})
	return options


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
) -> dict[str, str]:
	if selected_strategy_id == "decision_transformer" and _decision_policy_preview_is_ready(policy_preview_frame):
		first_row = policy_preview_frame.sort(["created_at", "interval_start"]).row(0, named=True)
		return {
			"policy_mode": "decision_transformer_preview",
			"selected_policy_id": str(first_row["policy_run_id"]),
			"policy_explanation": (
				"Offline Decision Transformer preview selected. Raw actions are projected through "
				"deterministic battery SOC/power constraints and remain market-execution disabled."
			),
			"policy_readiness": str(first_row["readiness_status"]),
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
		}
	return {
		"policy_mode": "baseline_lp_preview",
		"selected_policy_id": selected_strategy_id,
		"policy_explanation": (
			"Current operator schedule is generated by the Level 1 baseline LP preview. "
			"NBEATSx/TFT and DT surfaces are shown as forecast/policy evidence when materialized."
		),
		"policy_readiness": "lp_control_ready",
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
	historical_prices = _historical_prices_for_anchor(price_history, anchor_timestamp)
	load_frame = _operator_load_frame(tenant_id=tenant_id, anchor_timestamp=anchor_timestamp)
	soc_resolution = _resolve_operator_soc(
		tenant_id=tenant_id,
		battery_defaults=battery_defaults,
		load_frame=load_frame,
	)
	available_strategies = _operator_strategy_options(tenant_id=tenant_id)
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
			anchor_timestamp=anchor_timestamp,
		)
		solve_result = _operator_solve_result_for_strategy(
			selected_strategy_id=selected_strategy_id,
			solver=solver,
			baseline_solve_result=baseline_solve_result,
			battery_metrics=battery_metrics,
			current_soc_fraction=soc_resolution.starting_soc_fraction,
			anchor_timestamp=anchor_timestamp,
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
	return OperatorRecommendationResponse(
		tenant_id=tenant_id,
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
	"/dashboard/baseline-lp-preview",
	response_model=BaselineLpPreviewResponse,
	tags=["weather"],
	summary="Build baseline LP preview",
	description=(
		"Returns a tenant-aware baseline LP recommendation preview with hourly forecast, "
		"signed MW schedule, projected SOC trace, and UAH economics."
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
	historical_prices = _historical_prices_for_anchor(price_history, anchor_timestamp)
	solver = HourlyDamBaselineSolver()
	try:
		solve_result = solver.solve_next_dispatch(
			historical_prices,
			battery_metrics=battery_metrics,
			current_soc_fraction=starting_soc_fraction,
			anchor_timestamp=anchor_timestamp,
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
	)
	_persist_operator_status(
		tenant_id=tenant_id,
		flow_type=OperatorFlowType.BASELINE_LP,
		status=OperatorFlowStatus.COMPLETED,
		payload=response.model_dump(mode="json"),
	)
	return response
