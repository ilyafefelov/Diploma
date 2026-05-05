from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import cache
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
from smart_arbitrage.resources.grid_event_store import get_grid_event_store
from smart_arbitrage.resources.market_data_store import get_market_data_store
from smart_arbitrage.resources.strategy_evaluation_store import get_strategy_evaluation_store


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


def _mapping_row_value(value: Any) -> dict[str, Any]:
	if isinstance(value, dict):
		return value
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
