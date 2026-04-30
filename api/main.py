from __future__ import annotations

from datetime import UTC, datetime, timedelta
from functools import cache
from typing import Any

import dagster as dg
from fastapi import FastAPI, HTTPException
import polars as pl
from pydantic import BaseModel

from smart_arbitrage.assets.bronze.market_weather import (
	WeatherLocation,
	build_synthetic_market_price_history,
	build_weather_asset_run_config,
	list_available_weather_tenants,
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
from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics
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
	battery_metrics: BatteryPhysicalMetrics
	resolved_location: WeatherLocationResponse
	forecast: list[BaselineForecastPointResponse]
	recommendation_schedule: list[BaselineRecommendationPointResponse]
	projected_state: ProjectedBatteryStateResponse
	economics: BaselinePreviewEconomicsResponse


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
	labels = ["06:00", "09:00", "12:00", "15:00", "18:00", "21:00"]
	latitude_bias = round((resolved_location.latitude - 45) * 2, 2)
	longitude_bias = round((resolved_location.longitude - 22) * 1.5, 2)
	market_price = [
		round(value + latitude_bias + index, 2)
		for index, value in enumerate([84.0, 96.0, 113.0, 124.0, 117.0, 101.0])
	]
	weather_bias = [
		round(value + max(0.0, longitude_bias - index), 2)
		for index, value in enumerate([3.0, 5.0, 8.0, 7.0, 5.0, 4.0])
	]
	charge_intent = [
		round(value + (latitude_bias / 3) - index, 2)
		for index, value in enumerate([28.0, 42.0, 57.0, 54.0, 39.0, 26.0])
	]
	regret = [
		round(max(3.0, value + (longitude_bias / 2) - index), 2)
		for index, value in enumerate([10.0, 9.0, 8.0, 7.0, 8.0, 9.0])
	]

	return DashboardSignalPreviewResponse(
		tenant_id=tenant_id,
		labels=labels,
		market_price=market_price,
		weather_bias=weather_bias,
		charge_intent=charge_intent,
		regret=regret,
		resolved_location=_location_response_from_model(resolved_location),
	)


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


def _default_battery_metrics() -> BatteryPhysicalMetrics:
	return BatteryPhysicalMetrics(
		capacity_mwh=10.0,
		max_power_mw=2.5,
		round_trip_efficiency=0.95,
		degradation_cost_per_cycle_uah=56.0,
		soc_min_fraction=0.05,
		soc_max_fraction=0.95,
	)


def _default_current_soc_fraction(location: WeatherLocation) -> float:
	latitude_component = (location.latitude - 45.0) / 20.0
	longitude_component = (location.longitude - 22.0) / 40.0
	return round(max(0.2, min(0.8, 0.45 + latitude_component - longitude_component)), 3)


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
	)


def _resolve_projection_request(
	request: ProjectedBatteryStateRequest,
) -> tuple[BatteryPhysicalMetrics, float, list[ScheduledPowerPoint]]:
	resolved_location = _resolve_requested_location(
		tenant_id=request.tenant_id,
		location_config_path=None,
	)
	battery_metrics = request.battery_metrics or _default_battery_metrics()
	starting_soc_fraction = request.current_soc_fraction
	if starting_soc_fraction is None:
		starting_soc_fraction = _default_current_soc_fraction(resolved_location)
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
	battery_metrics = _default_battery_metrics()
	starting_soc_fraction = _default_current_soc_fraction(resolved_location)
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
