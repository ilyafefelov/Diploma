# API Endpoints

This document describes the current FastAPI control-plane and dashboard
read-model API exposed by `api/main.py`.

The API is a preview and evidence surface. It does not submit bids, clear
trades, emit dispatch commands, or create market order payloads. Operator-facing
routes must preserve `market_execution_enabled=false` unless a future explicit
execution gate changes the project boundary.

## Base Service

- Module: `api/main.py`
- Runtime: FastAPI + Uvicorn
- OpenAPI schema: `/openapi.json`
- Interactive docs: `/docs`
- Local default base URL: `http://127.0.0.1:8000`

## Local Start

```powershell
.\api\start-dev.ps1 -Port 8000
```

Or through the local demo helper:

```powershell
.\scripts\start-local-project.ps1 -ApiPort 8000 -DashboardPort 64163
```

If the dashboard is fresh but API routes return `404` or `502`, restart the API
from the current workspace:

```powershell
docker compose stop api
.\api\start-dev.ps1 -Port 8000
```

## Endpoint Groups

### System

| Method | Path | Purpose | Boundary |
| --- | --- | --- | --- |
| GET | `/health` | API liveness check | process health only |

### Tenant And Weather Control

| Method | Path | Purpose | Boundary |
| --- | --- | --- | --- |
| GET | `/tenants` | List tenant registry entries used by dashboard selectors | no run execution |
| POST | `/weather/run-config` | Build Dagster run config for weather source assets | prepares config only |
| POST | `/weather/materialize` | Materialize selected weather/source assets for a tenant | local MVP materialization, not market execution |
| GET | `/dashboard/signal-preview` | Build tenant-aware signal preview for dashboard context | read model |
| GET | `/dashboard/operator-status` | Return latest persisted operator flow status | read model |

### Operator Preview

| Method | Path | Purpose | Boundary |
| --- | --- | --- | --- |
| GET | `/dashboard/operator-recommendation` | Official-row-first DAM/IDM hourly recommendation preview | no `ProposedBid`, no market payload |
| GET | `/dashboard/baseline-lp-preview` | Baseline LP hourly preview with SOC/economics trace | preview only |
| GET | `/dashboard/shadow-recommendation-preview` | Manually selected DT/HF/research shadow preview | manual shadow/read model only |

Key query parameters:

- `tenant_id`: canonical tenant id, usually `client_003_dnipro_factory` for demo.
- `market_venue`: `DAM` by default; `IDM` is a separate hourly preview lane.
- `target_delivery_date`: optional `YYYY-MM-DD`; can route to published official
  OREE rows or complete forecast-store rows for unpublished horizons.
- `preview_source`: for shadow preview, for example
  `hf_live_safe_switch_value_aligned_shadow`.

Required operator-preview guarantees:

- `market_execution_enabled=false`
- `promotion_gate_passed=false` unless explicitly scoped to non-production
  shadow/demo proof
- no `ProposedBid`
- no market order payload
- no synthetic price fallback

### Battery, SOC, And Gatekeeper

| Method | Path | Purpose | Boundary |
| --- | --- | --- | --- |
| GET | `/dashboard/battery-state` | Latest telemetry/hourly battery snapshot and fallback reason | telemetry/readiness view |
| POST | `/dashboard/projected-battery-state` | Simulate projected SOC, throughput, and degradation for a preview schedule | local deterministic projection |
| GET | `/dashboard/gatekeeper-validation-status` | Latest validation failure, `NO_BID`/`HOLD` semantics, and audit fields | read model; no submission |

### Forecast And Strategy Evidence

| Method | Path | Purpose | Boundary |
| --- | --- | --- | --- |
| GET | `/dashboard/forecast-strategy-comparison` | Latest strict/NBEATSx/TFT forecast strategy comparison | offline evidence |
| GET | `/dashboard/real-data-benchmark` | Rolling-origin real-data benchmark rows | offline evidence |
| GET | `/dashboard/future-stack-preview` | Forecast/readiness series for operator and defense charts | dashboard read model |
| GET | `/dashboard/calibrated-ensemble-benchmark` | Calibrated ensemble benchmark rows | research read model |
| GET | `/dashboard/risk-adjusted-value-gate` | Risk-adjusted value-gate rows | research read model |
| GET | `/dashboard/forecast-dispatch-sensitivity` | Forecast-to-dispatch diagnostic buckets | research read model |
| GET | `/dashboard/dfl-relaxed-pilot` | Relaxed-LP DFL pilot rows | primitive only, not full DFL |
| GET | `/dashboard/dfl-schedule-value-production-gate` | Schedule/value offline promotion gate rows | offline/read-model only |
| GET | `/dashboard/decision-transformer-trajectories` | Offline DT trajectory rows | training/evaluation data only |
| GET | `/dashboard/decision-policy-preview` | DT policy-preview rows after deterministic projection | research preview only |
| GET | `/dashboard/simulated-live-trading` | Simulated replay rows | simulated only, no settlement identifiers |
| GET | `/dashboard/academic-mvp-readiness` | Credentialless academic MVP readiness packet | thesis/demo artifact |

## Example Requests

Health:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/health"
```

Default DAM preview:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/dashboard/operator-recommendation?tenant_id=client_003_dnipro_factory&market_venue=DAM"
```

IDM baseline preview:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/dashboard/baseline-lp-preview?tenant_id=client_003_dnipro_factory&market_venue=IDM"
```

HF value-aligned shadow for an unpublished horizon:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/dashboard/shadow-recommendation-preview?tenant_id=client_003_dnipro_factory&preview_source=hf_live_safe_switch_value_aligned_shadow&market_venue=DAM&target_delivery_date=2026-06-04"
```

Academic MVP readiness:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/dashboard/academic-mvp-readiness"
```

## Forecast-Store Seeding

When the dashboard needs unpublished DAM/IDM horizons, seed preview forecast
rows from source-backed OREE history:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_operator_preview_forecast_store.py --market-venue DAM --horizon-hours 72 --nbeatsx-max-steps 1 --tft-max-epochs 1
.\.venv\Scripts\python.exe scripts\materialize_operator_preview_forecast_store.py --market-venue IDM --horizon-hours 72 --nbeatsx-max-steps 1 --tft-max-epochs 1
```

These rows keep
`claim_boundary=operator_preview_forecast_rows_not_market_execution` with
`market_execution_enabled=false`.

## Verification

Focused API tests:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests\api\test_main.py -k "operator_recommendation or shadow_recommendation_preview or hf_value_aligned"
```

Full verification when runtime permits:

```powershell
.\.venv\Scripts\Activate.ps1
.\scripts\verify.ps1
```
