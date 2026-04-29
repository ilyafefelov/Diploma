# API Endpoints

This document describes the current control-plane API exposed by [api/main.py](d:/School/GoIT/Courses/Diploma/api/main.py). In the project glossary vocabulary, these endpoints do not submit a Proposed Bid or produce a Dispatch Command directly. They serve the Tenant Registry, Weather Forecast Bronze asset configuration, and location-aware MVP materialization flow that sits upstream of the Level 1 Baseline Strategy.

## Purpose

- The API is a control surface for selecting a tenant from the canonical Tenant Registry in [simulations/tenants.yml](d:/School/GoIT/Courses/Diploma/simulations/tenants.yml).
- The API builds or executes Dagster config for [weather_forecast_bronze](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/assets/bronze/market_weather.py#L76), which then feeds weather and solar context into [dam_price_history](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/assets/mvp_demo.py#L35).
- The downstream caller chain remains: Weather Forecast Bronze asset -> Market Price History enrichment -> Baseline Forecast -> Baseline Strategy -> Oracle Benchmark / Dispatch Command validation.

## Base Service

- Module: [api/main.py](d:/School/GoIT/Courses/Diploma/api/main.py)
- Runtime: FastAPI + Uvicorn
- OpenAPI schema: `/openapi.json`
- Interactive docs: `/docs`

## Local Development Start

Windows:

```powershell
./api/start-dev.ps1
```

Override the default port if `8000` is already occupied:

```powershell
./api/start-dev.ps1 -Port 8010
```

POSIX shell:

```bash
./api/start-dev.sh
```

Override the default port in POSIX environments:

```bash
SMART_ARBITRAGE_API_PORT=8010 ./api/start-dev.sh
```

Direct Uvicorn launch:

```bash
PYTHONPATH=".:./src" .venv/Scripts/python.exe -m uvicorn api.main:app --host 127.0.0.1 --port 8000 --reload
```

The FastAPI docs will then be available at `http://127.0.0.1:8000/docs`.
The API imports both [api/main.py](d:/School/GoIT/Courses/Diploma/api/main.py) and the `src` package tree, so local launches must include both the repo root and `src` on `PYTHONPATH`.

## Endpoints

### `GET /health`

Returns a minimal liveness response for the API process.

Response example:

```json
{
  "status": "ok"
}
```

Operational notes:

- Use this for process health, not for Dagster or MLflow readiness.
- This endpoint has no request body.

### `GET /tenants`

Returns the current Tenant Registry entries available for location-aware weather experiments.

Response example:

```json
[
  {
    "tenant_id": "client_002_lviv_office",
    "name": "Lviv Business Center",
    "type": "office",
    "latitude": 49.84,
    "longitude": 24.03,
    "timezone": "Europe/Kyiv"
  }
]
```

Operational notes:

- Data is resolved from [simulations/tenants.yml](d:/School/GoIT/Courses/Diploma/simulations/tenants.yml) by default.
- The payload is intended for dashboard/UI tenant selection and experiment pickers.

### `POST /weather/run-config`

Builds the Dagster run-config payload for [weather_forecast_bronze](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/assets/bronze/market_weather.py#L76) without executing a run.

Request body:

```json
{
  "tenant_id": "client_002_lviv_office",
  "location_config_path": "simulations/tenants.yml"
}
```

Response example:

```json
{
  "tenant_id": "client_002_lviv_office",
  "run_config": {
    "ops": {
      "weather_forecast_bronze": {
        "config": {
          "tenant_id": "client_002_lviv_office",
          "location_config_path": "simulations/tenants.yml"
        }
      }
    }
  },
  "resolved_location": {
    "latitude": 49.84,
    "longitude": 24.03,
    "timezone": "Europe/Kyiv"
  }
}
```

Operational notes:

- This endpoint is useful for dashboard previews, operator confirmation flows, and future orchestration adapters.
- If `location_config_path` is omitted, the service resolves the tenant against the canonical registry path.
- If the `tenant_id` is unknown, the service returns `404`.

### `POST /weather/materialize`

Materializes weather-related MVP assets for a selected tenant.

Request body:

```json
{
  "tenant_id": "client_002_lviv_office",
  "include_price_history": true,
  "location_config_path": "simulations/tenants.yml"
}
```

Response example:

```json
{
  "tenant_id": "client_002_lviv_office",
  "selected_assets": [
    "weather_forecast_bronze",
    "dam_price_history"
  ],
  "run_config": {
    "ops": {
      "weather_forecast_bronze": {
        "config": {
          "tenant_id": "client_002_lviv_office",
          "location_config_path": "simulations/tenants.yml"
        }
      }
    }
  },
  "resolved_location": {
    "latitude": 49.84,
    "longitude": 24.03,
    "timezone": "Europe/Kyiv"
  },
  "success": true
}
```

Operational notes:

- `include_price_history=false` materializes only `weather_forecast_bronze`.
- `include_price_history=true` materializes `weather_forecast_bronze` and [dam_price_history](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/assets/mvp_demo.py#L35).
- This endpoint is the first API-level bridge between Tenant Registry selection and location-aware Bronze ingestion.
- On materialization failure, the service returns `500`.

## CLI Preset

CLI users can still launch the same weather experiment path without the API by using [simulations/run-configs/weather-location-experiment.yaml](d:/School/GoIT/Courses/Diploma/simulations/run-configs/weather-location-experiment.yaml).

Example:

```bash
dg launch --assets weather_forecast_bronze --config-file simulations/run-configs/weather-location-experiment.yaml
```

## Dashboard Integration Contract

- The dashboard should call `GET /tenants` to populate the tenant selector.
- After selection, the dashboard should call `POST /weather/run-config` for preview and confirmation.
- When the operator starts an experiment, the dashboard should call `POST /weather/materialize`.
- The returned `resolved_location` should be displayed explicitly in the UI, because it is part of the operational truth for a location-aware weather run.

## Current Scope Boundary

- These endpoints are control-plane endpoints only.
- They do not yet expose Baseline Forecast, Oracle Benchmark, Proposed Bid, Cleared Trade, or Dispatch Command resources.
- The next natural extension is a tenant-aware endpoint that materializes or returns the broader MVP slice beyond Bronze weather and price-history assets.