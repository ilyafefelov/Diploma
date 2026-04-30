from __future__ import annotations

from functools import cache
from typing import Any

import dagster as dg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from smart_arbitrage.assets.bronze.market_weather import (
	WeatherLocation,
	build_weather_asset_run_config,
	list_available_weather_tenants,
	resolve_weather_location_for_tenant,
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
	return WeatherRunConfigResponse(
		tenant_id=request.tenant_id,
		run_config=run_config,
		resolved_location=_location_response_from_model(resolved_location),
	)


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
		raise HTTPException(status_code=500, detail="Dagster materialization failed.")

	return WeatherMaterializeResponse(
		tenant_id=request.tenant_id,
		selected_assets=[asset.key.path[-1] for asset in selected_assets],
		run_config=run_config,
		resolved_location=_location_response_from_model(resolved_location),
		success=True,
	)


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
	return _build_signal_preview(
		tenant_id=tenant_id,
		location_config_path=location_config_path,
	)
