# API Endpoints

This document describes the current control-plane API exposed by [api/main.py](d:/School/GoIT/Courses/Diploma/api/main.py). In the project glossary vocabulary, these endpoints do not submit a Proposed Bid or produce a Dispatch Command directly. They serve the Tenant Registry, Weather Forecast Bronze asset configuration, and location-aware MVP materialization flow that sits upstream of the Level 1 Baseline Strategy.

## Purpose

- The API is a control surface for selecting a tenant from the canonical Tenant Registry in [simulations/tenants.yml](d:/School/GoIT/Courses/Diploma/simulations/tenants.yml).
- The API builds or executes Dagster config for [weather_forecast_bronze](d:/School/GoIT/Courses/Diploma/src/smart_arbitrage/assets/bronze/market_weather.py#L76), which feeds weather and solar context into dashboard read models and future forecast features.
- The baseline LP caller chain remains price-driven: Market Price History -> Baseline Forecast -> Baseline Strategy -> Oracle Benchmark / Dispatch Command validation. Weather is not a baseline LP control input until it is part of a validated weather-aware price forecast.

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

Docker Compose uses the same internal API port (`8000`) but allows host-port override:

```powershell
$env:SMART_ARBITRAGE_API_PORT = "8001"
docker compose up -d api
```

Use this when another local process already owns host port `8000`.

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

### `GET /dashboard/operator-status`

Returns the latest persisted operator-visible state for one `tenant_id + flow_type` pair.

Request query example:

```text
/dashboard/operator-status?tenant_id=client_002_lviv_office&flow_type=weather_control
```

Response example:

```json
{
  "tenant_id": "client_002_lviv_office",
  "flow_type": "weather_control",
  "status": "completed",
  "updated_at": "2026-04-30T03:58:00+00:00",
  "payload": {
    "selected_assets": ["weather_forecast_bronze", "dam_price_history"]
  },
  "last_error": null
}
```

Operational notes:

- This is the backend-owned status contract for operator-facing read models.
- The record grain is one latest state per `tenant_id + flow_type`.
- The canonical status set is `idle`, `prepared`, `running`, `completed`, `failed`.
- Current Slice 1 flow coverage is `weather_control` and `signal_preview`.

### `GET /dashboard/gatekeeper-validation-status`

Returns the latest Bid Gatekeeper validation failure for a tenant.

Request query example:

```text
/dashboard/gatekeeper-validation-status?tenant_id=client_003_dnipro_factory
```

Response shape:

- `status`: `blocked` when a validation failure exists, otherwise `no_validation_failures_recorded`.
- `validation_stage`: current canonical stages are `proposed_bid` and `dispatch_command`.
- `contract_type`: failed contract name, such as `ProposedBid` or `DispatchCommand`.
- `canonical_outcome`: `NO_BID` for market-stage bid rejection or `HOLD` for physical dispatch fallback.
- `venue`, `interval_start`, `duration_minutes`: market interval context when available.
- `failure_reason`, `created_at`, `latest_failure_id`: audit fields for robustness analysis.
- `no_bid_semantics`, `hold_semantics`: explicit UI copy anchors. `NO_BID` means the market-stage bid is not submitted; `HOLD` means zero-power physical dispatch after market-stage decisions.

Operational notes:

- The backing durable table is `validation_failures` when `SMART_ARBITRAGE_VALIDATION_FAILURE_DSN` is configured.
- This endpoint is a read model. It does not submit bids, clear trades, or dispatch equipment.

### `POST /dashboard/projected-battery-state`

Builds a constrained hourly battery-state preview from a signed MW recommendation schedule.

Request body example:

```json
{
  "tenant_id": "client_003_dnipro_factory",
  "current_soc_fraction": 0.5,
  "battery_metrics": {
    "capacity_mwh": 4.0,
    "max_power_mw": 2.0,
    "round_trip_efficiency": 0.81,
    "degradation_cost_per_cycle_uah": 6737.3,
    "soc_min_fraction": 0.25,
    "soc_max_fraction": 0.75
  },
  "schedule": [
    {"interval_start": "2026-05-01T06:00:00Z", "net_power_mw": 1.0},
    {"interval_start": "2026-05-01T07:00:00Z", "net_power_mw": -2.0},
    {"interval_start": "2026-05-01T08:00:00Z", "net_power_mw": 3.0}
  ]
}
```

Response example:

```json
{
  "tenant_id": "client_003_dnipro_factory",
  "interval_minutes": 60,
  "starting_soc_fraction": 0.5,
  "battery_metrics": {
    "capacity_mwh": 4.0,
    "max_power_mw": 2.0,
    "round_trip_efficiency": 0.81,
    "degradation_cost_per_cycle_uah": 6737.3,
    "soc_min_fraction": 0.25,
    "soc_max_fraction": 0.75
  },
  "total_throughput_mwh": 4.52,
  "total_degradation_penalty_uah": 3806.6,
  "trace": [
    {
      "step_index": 0,
      "interval_start": "2026-05-01T06:00:00Z",
      "requested_net_power_mw": 1.0,
      "feasible_net_power_mw": 0.9,
      "soc_before_fraction": 0.5,
      "soc_after_fraction": 0.25,
      "throughput_mwh": 0.9,
      "degradation_penalty_uah": 757.9
    }
  ]
}
```

Operational notes:

- This endpoint is the narrow Slice 2 simulator for projected SOC, throughput, and degradation-aware UAH penalty.
- The example above keeps the same throughput-based proxy as the operator demo materials and scales cycle cost to the illustrated `4 MWh` battery, so the implied penalty stays near `842.2 UAH/MWh throughput`.
- It accepts scenario overrides for offline/demo use, but omitted battery values now resolve from the selected tenant `energy_system` block in [simulations/tenants.yml](d:/School/GoIT/Courses/Diploma/simulations/tenants.yml), including capacity, max power, RTE, initial SOC, SOC window, and cycle-cost inputs.
- The simulator enforces hourly Level 1 granularity, `soc_min`, `soc_max`, capacity, max power, and round-trip efficiency.
- On success, the API updates the persisted `baseline_lp` flow state to `completed`.
- This remains recommendation-preview language only. It does not emit `Proposed Bid`, `Cleared Trade`, or `Dispatch Command` contracts.

### `GET /dashboard/battery-state`

Returns the latest physical battery telemetry and the latest hourly Level 1 battery-state snapshot for a tenant.

Request query example:

```text
/dashboard/battery-state?tenant_id=client_003_dnipro_factory
```

Response shape:

- `latest_telemetry`: latest 5-minute raw telemetry row, or `null` if no MQTT/API telemetry has been ingested.
- `hourly_snapshot`: latest hourly Silver snapshot with SOC open/close/mean, SOH, throughput, EFC delta, and freshness.
- `fallback_reason`: `telemetry_unavailable`, `hourly_snapshot_unavailable`, `hourly_snapshot_stale`, or `null` when data is fresh.
- `telemetry_ingest_source`: configured MQTT ingest path for this tenant. It includes `protocol`, `broker_host`, `broker_port`, and the tenant-specific topic `smart-arbitrage/{tenant_id}/battery/telemetry`.

Operational notes:

- This endpoint separates physical truth now from planning/projected state.
- It is safe for the dashboard to show `latest_telemetry` and `hourly_snapshot` as separate panels.
- If telemetry is missing or stale, optimization read models continue to fall back to tenant defaults.
- `telemetry_ingest_source` is configuration metadata, not a broker connectivity probe. Broker health still requires the Docker/MQTT smoke test.

### `GET /dashboard/exogenous-signals`

Returns the latest tenant weather metadata plus public Ukrenergo grid-event context for dashboard and model diagnostics.

Request query example:

```text
/dashboard/exogenous-signals?tenant_id=client_004_kharkiv_hospital
```

Response shape:

```json
{
  "tenant_id": "client_004_kharkiv_hospital",
  "latest_weather": {},
  "latest_grid_event": {},
  "grid_event_count_24h": 1.0,
  "tenant_region_affected": true,
  "national_grid_risk_score": 0.85,
  "source_urls": ["https://t.me/s/Ukrenergo"]
}
```

Operational notes:

- This is a read model for exogenous context only.
- Ukrenergo Telegram features are transparent rule-based covariates, not proven causal price predictors.
- If no weather or grid-event rows exist yet, the endpoint still returns safe fallback metadata.

### `GET /dashboard/baseline-lp-preview`

Builds the first tenant-aware Slice 2 baseline LP read model for operator preview.

Request query example:

```text
/dashboard/baseline-lp-preview?tenant_id=client_003_dnipro_factory
/dashboard/baseline-lp-preview?tenant_id=client_003_dnipro_factory&market_venue=IDM
```

Response shape:

- `forecast`: hourly official observed OREE DAM or IDM delivery-row prices in UAH/MWh
- `recommendation_schedule`: hourly signed MW recommendation trace with projected SOC and per-slot economics
- `bid_recommendation_preview`: non-submittable DAM/IDM BUY/SELL/HOLD preview derived from the recommendation schedule; each row keeps `preview_only=true`, `market_order_payload_emitted=false`, and `market_execution_enabled=false`
- `projected_state`: projected SOC/throughput/degradation trace derived from the feasible schedule
- `economics`: aggregated gross market value, degradation penalty, net value, and throughput in canonical UAH/MWh units
- `starting_soc_source`: `telemetry_hourly` when a fresh hourly telemetry snapshot is available, otherwise `tenant_default`
- `telemetry_freshness`: latest snapshot freshness metadata, or `null` when no telemetry snapshot exists

Operational notes:

- This is a recommendation preview only. It does not return `Proposed Bid`, `Cleared Trade`, or `Dispatch Command` contracts.
- The read model is tenant-aware through tenant-registry battery assumptions; the OREE price row itself is not tenant-biased or synthetic.
- The LP runs at Level 1 hourly DAM/IDM preview granularity and reuses the same battery constraints as the projected-state simulator. IDM remains hourly preview only, not 15-minute bid submission.
- Degradation is calculated with the same equivalent-full-cycle throughput proxy documented in [BATTERY_DEGRADATION_AND_SIMULATION.md](d:/School/GoIT/Courses/Diploma/docs/technical/BATTERY_DEGRADATION_AND_SIMULATION.md).
- On success, the API updates the persisted `baseline_lp` flow state to `completed`.

### `GET /dashboard/forecast-strategy-comparison`

Returns the latest Gold-layer comparison of strict similar-day, NBEATSx, and TFT Silver forecasts after each forecast has been routed through the same Level 1 LP and scored against realized DAM prices and an oracle LP benchmark.

Request query example:

```text
/dashboard/forecast-strategy-comparison?tenant_id=client_003_dnipro_factory
```

Response shape:

- `tenant_id`: selected tenant from the Tenant Registry.
- `generated_at`: timestamp of the latest persisted comparison batch.
- `anchor_timestamp`: strategy anchor used for the evaluated forecast horizon.
- `market_venue`: currently `DAM`.
- `strategy_kind`: currently `forecast_driven_lp`.
- `horizon_hours`: evaluated horizon length.
- `starting_soc_fraction`: physical SOC used to initialize the LP.
- `starting_soc_source`: `telemetry_hourly` when fresh Silver battery state exists, otherwise `tenant_default`.
- `comparisons`: one row per forecast candidate with decision value, forecast objective value, oracle value, regret, regret ratio, degradation penalty, throughput, first committed action/power preview, rank, and evaluation payload.

Operational notes:

- This endpoint is dashboard-ready Gold evidence, not market execution.
- It does not return `Proposed Bid`, `Cleared Trade`, settlement, or `Dispatch Command` semantics.
- The comparison is only available after Dagster materializes `forecast_strategy_comparison_frame`.
- If no rows exist for the tenant, the endpoint returns `404`.

### `GET /dashboard/real-data-benchmark`

Returns the latest persisted real-data rolling-origin benchmark for a tenant.

Request query example:

```text
/dashboard/real-data-benchmark?tenant_id=client_003_dnipro_factory
```

Response shape:

- `tenant_id`: selected tenant from the Tenant Registry.
- `market_venue`: currently `DAM`.
- `generated_at`: timestamp of the latest benchmark batch.
- `data_quality_tier`: `thesis_grade` only when benchmark rows are observed-only; otherwise `demo_grade`.
- `anchor_count`: number of rolling-origin anchors in the latest batch.
- `model_count`: number of forecast candidates compared.
- `best_model_name`: model with the most rank-1 anchor wins, tie-broken by lower mean regret.
- `mean_regret_uah` and `median_regret_uah`: batch-level regret summaries.
- `rows`: one row per anchor/model with decision value, oracle value, regret, degradation penalty, throughput, committed action preview, rank, and provenance payload.

Operational notes:

- This endpoint is a benchmark read model only. It does not emit `Proposed Bid`, `Cleared Trade`, settlement, or `Dispatch Command` contracts.
- It is populated by the Dagster asset `real_data_rolling_origin_benchmark_frame`.
- The benchmark trains/fits forecasts using rows available at or before each anchor and keeps realized future prices only for scoring and oracle comparison.
- Freshness is represented by `generated_at`; the backing Postgres store returns the latest batch for the requested `tenant_id` and `strategy_kind`.
- If no benchmark rows exist for the tenant, the endpoint returns `404`.

### `GET /dashboard/calibrated-ensemble-benchmark`

Returns the latest persisted calibrated value-aware ensemble gate rows for a tenant. The selector chooses only from `strict_similar_day`, `tft_horizon_regret_weighted_calibrated_v0`, and `nbeatsx_horizon_regret_weighted_calibrated_v0` using prior-anchor validation regret.

Request query example:

```text
/dashboard/calibrated-ensemble-benchmark?tenant_id=client_003_dnipro_factory
```

Response shape is the same as `GET /dashboard/real-data-benchmark`, but `model_count` is `1` and each row payload includes `selected_model_name`, `selection_policy`, and `prior_validation_anchor_count`.

Operational notes:

- This endpoint is a dashboard read model for research evidence, not an operational trading selector.
- Current 90-anchor result is negative: the calibrated gate improves over raw compact neural candidates but is worse than both `strict_similar_day` and horizon-aware TFT on mean regret.
- Freshness is represented by `generated_at`; older materialization batches may remain in Postgres but are not returned by this endpoint.
- If no calibrated ensemble rows exist for the tenant, the endpoint returns `404`.

### `GET /dashboard/risk-adjusted-value-gate`

Returns the latest persisted risk-adjusted value-gate rows for a tenant. The selector chooses only from `strict_similar_day`, `tft_horizon_regret_weighted_calibrated_v0`, and `nbeatsx_horizon_regret_weighted_calibrated_v0` using prior-anchor median regret, tail regret, and win rate.

Request query example:

```text
/dashboard/risk-adjusted-value-gate?tenant_id=client_003_dnipro_factory
```

Response shape is the same as `GET /dashboard/real-data-benchmark`, but `model_count` is `1` and each row payload includes `selected_model_name`, `selection_policy`, `prior_validation_anchor_count`, `risk_adjusted_score`, and candidate score diagnostics.

Operational notes:

- This endpoint is a dashboard read model for research evidence, not an operational trading selector.
- Current Dnipro 90-anchor preview result is conservative: the risk-adjusted gate mean regret is `1428.59` UAH, narrower than the calibrated ensemble at `1479.65` UAH but still worse than `strict_similar_day` at `1384.70` UAH.
- Freshness is represented by `generated_at`; older materialization batches may remain in Postgres but are not returned by this endpoint.
- If no risk-adjusted gate rows exist for the tenant, the endpoint returns `404`.

### `GET /dashboard/forecast-dispatch-sensitivity`

Returns diagnostic rows derived from the latest persisted horizon-aware regret-weighted benchmark rows for a tenant. The endpoint does not persist new data and does not run a trading selector; it rebuilds the sensitivity read model from benchmark payloads already in Postgres.

Request query example:

```text
/dashboard/forecast-dispatch-sensitivity?tenant_id=client_003_dnipro_factory
```

Response shape:

- `tenant_id`, `market_venue`, `generated_at`: source batch identity.
- `source_strategy_kind`: currently `horizon_regret_weighted_forecast_calibration_benchmark`.
- `anchor_count`, `model_count`, `row_count`: diagnostic coverage.
- `bucket_summary`: count and mean diagnostics per `diagnostic_bucket`.
- `rows`: per-anchor/model diagnostics including forecast MAE/RMSE, forecast-vs-realized dispatch spread error, regret, degradation, throughput, and committed action.

Operational notes:

- Buckets are `low_regret`, `forecast_error`, `spread_objective_mismatch`, and `lp_dispatch_sensitivity`.
- This is the API read model the dashboard can use to explain why a strategy lost value; it is not a bid, dispatch command, or promoted model.
- Freshness is represented by `generated_at`; this endpoint rebuilds diagnostics from the latest horizon-aware calibration batch for the requested tenant.
- If no horizon-aware benchmark rows exist for the tenant, the endpoint returns `404`.

### Read-model freshness and performance

The Postgres-backed research read models use
`forecast_strategy_evaluations_latest_read_idx` to retrieve the latest
`tenant_id + strategy_kind + generated_at` batch. The measured local Compose
result is documented in
[API_READ_MODEL_FRESHNESS_AND_PERFORMANCE.md](API_READ_MODEL_FRESHNESS_AND_PERFORMANCE.md).

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
- On success, the API updates the persisted `weather_control` flow state to `prepared`.

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
- During execution, the API updates persisted `weather_control` flow state through `running`, then `completed` or `failed`.

### `GET /dashboard/signal-preview`

Builds the current tenant-aware signal preview for the operator dashboard.

Response shape:

- `market_price`: visible strict-similar-day price preview in `UAH/MWh`.
- `weather_bias`: calibrated non-negative weather uplift in `UAH/MWh`.
- `weather_sources`: source label per visible point, usually `OPEN_METEO` when weather rows are available.
- `charge_intent`: simplified signed-MW visual preview derived from the weather-adjusted curve.
- `regret`: MVP opportunity-score read model for the chart, not oracle regret.

Operational notes:

- On success, the API updates the persisted `signal_preview` flow state to `completed`.
- This remains a preview/read-model endpoint and does not create `Proposed Bid`, `Cleared Trade`, or `Dispatch Command` semantics.
- The weather line is computed as `price_after_weather = market_price + weather_bias`.
- `weather_bias` is estimated from cloud cover, precipitation, humidity excess, temperature gap, effective solar, and wind speed. `effective_solar = solar_radiation * (100 - cloudcover) / 100`.
- This is an operator-facing weather-sensitivity explanation only. The baseline LP endpoint does not consume `weather_bias`; it consumes the official observed OREE DAM price row when available. Weather should enter dispatch only after it is part of a validated weather-aware forecast model and has been evaluated through rolling-origin realized-value/oracle-regret benchmarks.

### `GET /dashboard/operator-recommendation`

Returns the operator-facing recommendation read model for the selected tenant and optional strategy.

Request query example:

```text
/dashboard/operator-recommendation?tenant_id=client_003_dnipro_factory&strategy_id=risk_adjusted_value_gate_v0
/dashboard/operator-recommendation?tenant_id=client_003_dnipro_factory&market_venue=IDM&strategy_id=nbeatsx_official_idm_v0
/dashboard/operator-recommendation?tenant_id=client_003_dnipro_factory&strategy_id=nbeatsx_official_v0&target_delivery_date=2026-05-20
```

Response shape:

- `market_scope`, `market_venue`, `interval_minutes`: explicit current scope. DAM uses `dam_hourly_planning_preview`, `DAM`, `60`; IDM v1 uses `idm_hourly_planning_preview`, `IDM`, `60` as a planning/read-model lane, not a 15-minute bid lane.
- `target_delivery_date`: optional requested delivery date. When omitted, the endpoint uses the latest complete official observed OREE DAM or IDM delivery row.
- `price_context_status`: current price-source mode. `official_published` means the visible schedule is priced from the official OREE row. `pre_publication_forecast` means the requested target delivery date is not published yet and the schedule is an operator preview generated from complete NBEATSx/TFT forecast-store rows.
- `anchor_timestamp`, `forecast_generated_at`, `target_delivery_window_start`, `target_delivery_window_end`: as-of timing and target delivery window metadata for the visible preview. The schedule price source is the target complete official observed OREE DAM or IDM delivery row when available. `forecast_generated_at` may be `null` because the schedule does not need an ML forecast when the official row is already available.
- `market_execution_enabled`, `read_model_boundary`: execution boundary. Current value remains `false` with `operator_preview_no_market_submission`.
- `market_gate_status`, `bid_eligibility_status`, `proposed_bid_status`: explicit non-bid status. Current values are preview-only/not-applicable because this endpoint does not evaluate market gate closure and does not emit `ProposedBid`.
- `v13_readiness.source_governance_status`, `source_governance_label`, and `market_submission_receipt_gate_status`: source-governance status for the visible operator preview. Current value is `receipt_gated_for_market_submission` / `receipt-gated for market submission` with `blocked_external_access`; SCMO credentials are not required for the diploma MVP but remain relevant to market-submission-grade receipt proof.
- `available_strategies`: materialized strategies the operator may inspect as evidence/benchmarks; unavailable future policies stay disabled. Selecting an ML strategy does not replace the official OREE row as the operator price source.
- `selected_strategy_id`, `selected_policy_id`, `policy_mode`, `policy_readiness`: current selection and its safety/readiness boundary.
- `policy_forecast_context_source`, `policy_forecast_context_row_count`, `policy_forecast_context_coverage_ratio`, `policy_forecast_context_warning`: DT forecast-state coverage copied into the operator read model. In `pre_publication_forecast` mode this identifies the NBEATSx/TFT forecast scenario used by deterministic LP.
- `decision_advisor`: non-execution policy/advisor layer. Published DAM uses DT/V2+ safe-switch evidence when artifacts are available; IDM v1 exposes the NBEATSx/TFT/V2+/AFL/DFL/DT evidence stack and currently abstains to the deterministic LP schedule. Pre-publication DAM/IDM forecast mode builds complete NBEATSx/TFT forecast-scenario LP candidates, scores their decision value/regret through `lp_schedule_value_regret_adapter_for_v2_plus_dfl_dt_advisor`, exposes ranked `forecast_scenario_candidates` with gatekeeper status, and still abstains to the selected forecast-backed deterministic LP preview.
- `forecast_model_series`: NBEATSx/TFT forecast paths for dashboard graphs when materialized, including per-series `out_of_dam_cap_rows` and `quality_boundary` so the operator can inspect research/benchmark evidence. The endpoint no longer fabricates compact fallback forecast series from LP prices.
- `value_gap_series`: per-hour counterfactual value-gap preview for the selected schedule.
- `bid_recommendation_preview`: operator-facing DAM or IDM BUY/SELL/HOLD recommendation preview derived from the feasible schedule. It is not a `ProposedBid`, does not include market-order payload fields, and repeats the non-execution boundary per row.
- `load_forecast`, `pv_forecast`, `projected_soc`: tenant schedule, PV, and SOC context.
- `daily_value_uah`, `hold_value_uah`, `value_vs_hold_uah`: operator economics against a no-arbitrage hold baseline.

Operational notes:

- This endpoint is the main `/operator` read model. DAM remains the default hourly planning preview. IDM is available as a source-backed hourly planning/read-model lane and does not submit bids.
- This endpoint is `official OREE published row first`: it requires observed OREE rows in `market_price_observations` with `market_venue=DAM` or `market_venue=IDM`, `source_kind=observed`, and OREE source metadata for the requested delivery date. If the target date is not published yet, a complete 24-hour NBEATSx/TFT forecast-store horizon may be used as `pre_publication_forecast`; otherwise the endpoint returns a source-readiness blocker instead of falling back to synthetic/demo prices.
- IDM/ВДР follows the same source-backed rule with `market_venue=IDM`. IDM v1 is hourly preview only: NBEATSx/TFT provide scenario evidence, AFL/DFL provide decision-value evidence, and DT ranks or abstains as advisor metadata. 15-minute IDM `ProposedBid`, settlement, and live submission remain blocked.
- When `strategy_id` is `nbeatsx_official_v0` or `tft_official_v0`, materialized forecast-store rows remain visible as evidence when available, but the feasible schedule is still optimized from the official OREE DAM delivery row. Out-of-cap official forecast rows remain visible in the forecast graph with `quality_boundary=needs_calibration_before_value_claim`, but they do not replace the published DAM row in the operator schedule.
- This endpoint remains the default production-facing recommendation source. Manual DT/Poland/DFL shadow inspection is exposed through `GET /dashboard/shadow-recommendation-preview`, not by promoting those sources into the default selection path.
- `strict_similar_day` remains a backward-compatible strategy id/control comparator label, but when a complete official OREE DAM row is present the visible operator schedule uses that row directly instead of re-forecasting it.

### `GET /dashboard/shadow-recommendation-preview`

Returns a manually selected shadow/diagnostic recommendation preview for the
operator dashboard. The dashboard uses it only when the operator selects a
non-default preview source such as `dt_shadow`,
`dt_direct_candidate_shadow`, `dt_v2_plus_apples_to_apples_shadow`,
`regret_aware_v2_plus_selector_shadow`, `poland_tft_shadow`,
`dfl_diagnostics`, or `v13_dt_lava_promoted_training`.

Request query example:

```text
/dashboard/shadow-recommendation-preview?tenant_id=client_003_dnipro_factory&preview_source=dt_shadow
```

Response shape:

- `preview_source_id`, `preview_source_label`, `preview_status`: selected manual
  preview source and its boundary label. Current statuses include
  `research_shadow_not_promoted`, `direct_candidate_shadow_not_promoted`,
  `apples_to_apples_not_promoted`,
  `regret_aware_abstention_not_promoted`, `positive_not_promoted`,
  `diagnostic_only`, and `blocked_source_readiness_roadmap`.
- `default_strategy_id`: the honest default/fallback strategy that remains
  selected when the operator is not viewing a manual shadow source; current
  value is `schedule_value_learner_v2_plus`.
- `selected_candidate_id`, `selected_schedule_family`: candidate-index or
  schedule-family target selected by the shadow artifact when available.
- `recommendation_schedule`: expanded hourly schedule rows with timestamp,
  charge/discharge/hold action, signed MW, SOC before/after when available,
  candidate id, schedule family, expected value, regret/value versus V2+ and
  strict reference, and gate/safety status.
- `comparison_metrics`: regret/value metrics for DT/shadow versus V2+,
  strict/oracle reference, and behavior-cloning controls when present. The
  regret-aware selector also exposes `selector_mean_regret_uah`,
  `v2_plus_mean_regret_uah`,
  `selector_minus_v2_plus_mean_regret_uah`,
  `non_v2_plus_switch_count`, and `abstention_count`.
- `available_preview_sources`: dashboard strategy-switch options.
- `boundary_labels` and `readiness_warnings`: display labels such as
  `DT Shadow`, `Not promoted`, `Preview only`, `No market execution`, and
  `V2+ remains confirmed offline comparator/evidence`.
- `market_execution_enabled=false`, `market_order_payload_emitted=false`, and
  `proposed_bid_status=not_emitted_operator_preview`.

Operational notes:

- This endpoint is read-model only. It does not emit `ProposedBid`, market
  order payloads, settlement identifiers, or dispatch commands.
- DT Shadow is rendered even when it loses to V2+ or strict/oracle; negative
  evidence is useful diagnostic evidence and not a rendering failure.
- Direct DT Shadow is a trained HF DecisionTransformer preview over
  candidate-index/schedule-family teacher targets. It is manually selectable as
  `preview_source=dt_direct_candidate_shadow`, ties the V13 fallback row in the
  current direct packet, remains worse than strict/oracle, and stays
  non-promoted.
- DT vs real V2+ Shadow is the apples-to-apples check against the headline V2+
  strict-row packet. It is manually selectable as
  `preview_source=dt_v2_plus_apples_to_apples_shadow`, has DT selected mean
  regret `460.30` UAH versus real V2+ `174.77` UAH and strict `310.58` UAH, and
  stays non-promoted.
- Regret-aware V2+ selector is the corrected shadow model path for the current
  dashboard story. It is manually selectable as
  `preview_source=regret_aware_v2_plus_selector_shadow`, trains against the
  value-gap/regret-delta objective, reports selector mean regret `174.77` UAH
  versus V2+ `174.77` UAH, and abstains back to V2+ with `0 / 90` non-V2+
  switches and `90 / 90` abstentions. It is not a default switch, DT/LAVA
  promotion, or market-execution permission.
- `v13_dt_lava_promoted_training` intentionally returns blocked roadmap
  evidence while explicit OREE DAM/IDM source/publication evidence for preview
  and source-readiness gates are blocked.
- This endpoint does not change the default strategy selected by
  `GET /dashboard/operator-recommendation`; V2+ remains teacher, comparator,
  and fallback until a future strict LP/oracle promotion gate passes.

### `GET /dashboard/academic-mvp-readiness`

Returns the materialized credentialless academic MVP readiness packet as a
read-only dashboard/thesis artifact. This endpoint is for showing that the
DAM/IDM hourly recommendation preview and DT/LAVA prototype evidence gates have
passed without OREE or SCMO credentials.

Current expected packet state:

- `academic_mvp_gate_passed=true`.
- `operator_preview_gate.passed=true`.
- `dt_lava_prototype_gate.passed_for_academic_mvp=true`.
- `dt_lava_prototype_gate.lava_npz_smoke_validation.validation_passed=true`;
  the sibling validation artifact also requires a passed
  `lava_npz_smoke_packet_validation` gate.
- `dt_lava_teacher_contract_gate.permitted_model_training_rows=0` while the
  V13 receipt gate is blocked.
- `dt_lava_teacher_contract_gate.teacher_packet_validation.passed=true`; the
  endpoint's sibling validation artifact also requires a passed
  `teacher_packet_validation` gate.
- `offline_challenger_gate.passed_for_academic_mvp=true`, with promotion still
  blocked.
- `offline_challenger_gate.offline_challenger_packet_validation.passed=true`;
  the sibling validation artifact also requires an
  `offline_challenger_packet_validation` gate.
- `gate_passport`: single API/thesis gate map. It marks the operator preview,
  non-submittable DAM/IDM hourly preview rows, LAVA NPZ CI smoke, the separate LAVA NPZ
  packet-validation gate, V13-gated teacher contract, offline challenger
  non-promotion evidence, prototype evidence scorecard, and no-market-execution
  safety as passed. It also marks market-submission receipts as
  `blocked_external_access`, DT/LAVA training promotion as
  `blocked_until_v13_source_readiness`, and market execution as `out_of_scope`.
- `prototype_phase_readiness`: compact Phase 0-4 DFL/DT roadmap for the
  credentialless MVP. It shows V13 source readiness as
  `blocked_market_submission_receipts`, LAVA NPZ smoke as
  `passed_ci_smoke_not_promotion`, the V13-gated teacher contract as
  `passed_contract_training_rows_gated`, the offline challenger as
  `passed_non_promotion_evidence`, and full schedule-level DFL as
  `future_work_not_started`.
- `prototype_evidence_scorecard`: compact thesis/dashboard scorecard derived
  from the same packet sections. It exposes DAM/IDM hourly preview row count, LAVA NPZ
  validation status, V13-gated teacher row counts with `0` permitted model
  training rows, strict/V2+/behavior-cloning control coverage, deterministic
  safety projection, and the fixed non-execution flags. The same fields are
  also exposed as `gate_passport.prototype_evidence_scorecard_gate` so API and
  dashboard consumers can fail closed on a missing or unpassed scorecard gate.
- `dt_research_shadow_gate`: required credentialless DT prototype evidence
  for the academic MVP packet. It must use chronological delivery-time splits
  over existing candidate/value teacher rows, with
  `publication_receipt_verified=false`,
  `source_publication_timestamp_available=false`,
  `market_availability_claim=false`,
  `research_shadow_not_promotable=true`, and
  `promotable_v13_permitted_training_rows=0`; promotion remains blocked by
  missing explicit OREE DAM/IDM source/publication evidence for preview. Its `evaluation_metrics` report
  regret and value metrics for the DT shadow selection, strict LP/oracle
  reference, V2+ teacher/comparator/fallback, and behavior-cloning baseline;
  `accuracy_secondary` stays secondary evidence. Its forecast-family coverage
  fields (`forecast_context_present_families`,
  `forecast_context_missing_families`, and
  `forecast_context_coverage_status`) make the NBEATSx/TFT state-contract
  status explicit; the current refreshed packet reports
  `complete_nbeatsx_tft` by adapting TFT candidate-library rows as
  credentialless research-shadow context while keeping promotion blocked.
  The gate also exposes DT smoke backbone metadata such as `model_backbone`,
  `model_backbone_selection_reason`, and `hf_decision_transformer_available`;
  the current packet records `huggingface_decision_transformer_model` with
  `hf_decision_transformer_available=true`.
  It additionally surfaces `state_contract_passed`, `reward_contract_passed`,
  `state_context_groups`, and `return_to_go_target`, so API consumers can
  verify the DT shadow substrate without treating it as promotion-ready.
  `evaluation_packet_claim_scope` and `evaluation_packet_primary_metric`
  summarize the sidecar `dt_research_shadow_evaluation_summary.json`, whose
  metrics compare DT against strict, V2+, and behavior-cloning controls.
  `evaluation_packet_validation_passed=true` indicates the sibling
  `dt_research_shadow_evaluation_validation.json` passed its non-promotion and
  no-market-execution checks. Current materialization loads that sidecar rather
  than relying only on embedded smoke-summary metadata.
- `artifact_validation` and `artifact_validation_packet_path`: standalone
  validator evidence loaded from the sibling
  `credentialless_academic_mvp_readiness_validation.json` file, or from
  `SMART_ARBITRAGE_ACADEMIC_MVP_VALIDATION_JSON` when overridden. The endpoint
  fails closed if this artifact is missing, failed, reports validation
  failures, or contains any nested `market_execution_enabled=true`. The
  credentialless MVP packet materializer writes this sibling validation file
  in the same output directory.
- `market_submission_ready=false`, `permits_model_training=false`,
  `promotion_gate_passed=false`, and `market_execution_enabled=false`.

### `GET /dashboard/future-stack-preview`

Returns target-architecture forecast evidence for the dashboard and defense route.

Request query example:

```text
/dashboard/future-stack-preview?tenant_id=client_003_dnipro_factory
```

Response shape:

- `backend_status`: optional package availability for NeuralForecast, PyTorch Forecasting, and Lightning.
- `runtime_acceleration`: Torch backend, device type (`cpu`, `cuda`, or `mps`), device name, CUDA version when available, and recommended experiment scope.
- `selected_forecast_model`: lowest-regret forecast row available in the read model.
- `forecast_window_start`, `forecast_window_end`: exact UTC timestamps covered by the returned forecast series.
- `forecast_series`: NBEATSx/TFT paths with point forecasts, TFT-style p10/p50/p90 fields when available, per-point `price_cap_status`, and per-series `out_of_dam_cap_rows` plus `quality_boundary`.
- `claim_boundary`: text boundary that the series is evidence, not a bid.

Operational notes:

- This endpoint supports the operator forecast graph and the defense future-stack section.
- Current compact/calibrated NBEATSx/TFT rows may be displayed, but full SOTA claims require the optional official adapters and a materialized rolling-origin benchmark run.
- Official rows are prioritized in the operator graph when present. The dashboard should display the forecast window in Europe/Kyiv local time for operator readability while preserving UTC timestamps in the API payload.
- Official forecast smoke rows can be shown in the operator graph, but `quality_boundary=needs_calibration_before_value_claim` means the row must not drive a thesis value claim or a promoted live strategy until the forecast is calibrated and benchmarked through the strict LP/oracle path.
- The local smoke command `.\.venv\Scripts\python.exe scripts\run_official_forecast_smoke.py --horizon-hours 6 --nbeatsx-max-steps 1 --tft-max-epochs 1` writes report artifacts under `reports/official_forecast_smoke/`. These artifacts verify backend execution and forecast quality flags, but they are not API payloads and are not thesis-grade value results.
- Add `--persist-forecast-store` to the smoke command when Postgres and `SMART_ARBITRAGE_FORECAST_DSN` or `SMART_ARBITRAGE_MARKET_DATA_DSN` are available. That writes official smoke rows into the forecast store so `/dashboard/future-stack-preview` and `/dashboard/operator-recommendation` can show them in the NBEATSx/TFT graphs.
- If no forecast rows exist for the tenant, the endpoint returns an empty series rather than synthetic data.

### `GET /dashboard/decision-policy-preview`

Returns projected offline Decision Transformer policy-preview rows.

Response adds response-level explanation fields:

- `policy_state_features`: human-readable inputs represented in the preview state. Current DT preview includes SOC, SOH, market price, NBEATSx forecast, TFT forecast, forecast uncertainty/spread, time-of-day, degradation penalty, return target, and previous action context. The forecast fields are materialized through the Silver `decision_transformer_forecast_context_silver` bridge when NBEATSx/TFT forecast assets are available. Older rows without forecast fields remain valid and use market-price fallback context.
- `policy_value_interpretation`: how the displayed value gap is calculated.
- `operator_boundary`: explicit reminder that this is preview-only and still requires deterministic gatekeeper plus operator review.

Request query example:

```text
/dashboard/decision-policy-preview?tenant_id=client_003_dnipro_factory&limit=120
```

Response shape:

- `policy_run_id`, `created_at`, `policy_readiness`, `academic_scope`: preview batch identity and thesis boundary.
- `live_policy_claim`: always false for this slice.
- `market_execution_enabled`: false until the policy passes full offline evaluation and gatekeeper promotion.
- `constraint_violation_count`, `mean_value_gap_uah`, `total_value_vs_hold_uah`: safety and value diagnostics.
- `forecast_context_source`, `forecast_context_row_count`, `forecast_context_coverage_ratio`, `forecast_context_warning`: whether the DT preview rows are actually conditioned on both NBEATSx/TFT forecast state or using fallback market-price context.
- `rows`: interval-level DT raw action, projected feasible action, `projected_action_label`, `projection_status`, `projection_adjustment_mw`, SOC before/after, `state_nbeatsx_forecast_uah_mwh`, `state_tft_forecast_uah_mwh`, `state_forecast_uncertainty_uah_mwh`, `state_forecast_spread_uah_mwh`, expected policy value, oracle value, value gap, `value_gap_ratio`, gatekeeper status, and inference latency.

Operational notes:

- The raw DT action is never trusted directly. It is projected through the deterministic battery feasibility layer before display.
- The DT forecast context is state evidence for offline policy preview. It does not promote DT to a live policy and does not submit bids.
- This endpoint is suitable for dashboards and defense explanation, not for dispatch command execution.
- If no policy-preview rows exist, the endpoint returns `404`; the dashboard should show the DT surface as not materialized.

## CLI Preset

CLI users can still launch the same weather experiment path without the API by using [simulations/run-configs/weather-location-experiment.yaml](d:/School/GoIT/Courses/Diploma/simulations/run-configs/weather-location-experiment.yaml).

Example:

```bash
dg launch --assets weather_forecast_bronze --config-file simulations/run-configs/weather-location-experiment.yaml
```

## Dashboard Integration Contract

- The dashboard should call `GET /tenants` to populate the tenant selector.
- The dashboard should call `GET /dashboard/operator-status` when it needs the backend-owned latest visible state for a tenant and flow.
- After selection, the dashboard should call `POST /weather/run-config` for preview and confirmation.
- When the operator starts an experiment, the dashboard should call `POST /weather/materialize`.
- For Slice 2 preview work, the dashboard can call `POST /dashboard/projected-battery-state` to render feasible hourly SOC and degradation-aware economics from a signed recommendation schedule.
- For baseline recommendation preview, the dashboard can call `GET /dashboard/baseline-lp-preview` to render forecast, feasible hourly signed MW schedule, projected SOC trace, and UAH economics for the selected tenant.
- For live exogenous context, the dashboard can call `GET /dashboard/exogenous-signals` to show weather freshness and Ukrenergo grid-event risk without making a trading claim.
- For Gold strategy evidence, the dashboard can call `GET /dashboard/forecast-strategy-comparison` to compare strict similar-day, NBEATSx, and TFT by LP decision value, oracle regret, degradation penalty, throughput, and starting SOC source.
- For thesis benchmark evidence, the dashboard can call `GET /dashboard/real-data-benchmark` to compare the same forecast candidates across observed-only rolling-origin anchors and show whether the result is thesis-grade or demo-grade.
- For calibrated selector evidence, the dashboard can call `GET /dashboard/calibrated-ensemble-benchmark` to show which prior-regret gate source was selected per anchor and why this selector is not yet a dashboard default.
- For risk-adjusted selector evidence, the dashboard can call `GET /dashboard/risk-adjusted-value-gate` to show median/tail/win-rate gate decisions. Current results are diagnostic only and do not replace the strict control.
- For forecast-to-dispatch explainability, the dashboard can call `GET /dashboard/forecast-dispatch-sensitivity` to show whether high regret is mostly forecast error, spread mismatch, or LP dispatch sensitivity.
- For the live operator product surface, the dashboard should call `GET /dashboard/operator-recommendation` to render selected strategy, SOC/load/PV context, daily value against hold, forecast model series, and value-gap series.
- For manual non-default strategy inspection, the dashboard can call `GET /dashboard/shadow-recommendation-preview` and switch the same charts/tables to a DT, Poland/TFT, DFL diagnostic, or blocked V13/DT/LAVA roadmap preview. This is a diagnostic display path only; the default recommendation remains `GET /dashboard/operator-recommendation`.
- For NBEATSx/TFT forecast graphs, the dashboard can call `GET /dashboard/future-stack-preview`; this keeps forecast evidence separate from dispatch commands.
- For DT policy preview graphs, the dashboard can call `GET /dashboard/decision-policy-preview`; this remains preview-only unless the backend explicitly enables market execution after full evaluation.
- The returned `resolved_location` should be displayed explicitly in the UI, because it is part of the operational truth for a location-aware weather run.

## Current Scope Boundary

- These endpoints are control-plane endpoints only.
- They do not yet expose Baseline Forecast, Oracle Benchmark, Proposed Bid, Cleared Trade, or Dispatch Command resources.
- The projected battery state endpoint is a simulator/read model only; it does not claim market-order or dispatch semantics.
- The baseline LP preview endpoint is also a read model only; it exposes recommendation semantics, not market-order, clearing, or dispatch semantics.
- The forecast strategy comparison endpoint is Gold-layer evidence only; it compares forecast-driven LP decisions and does not generate market contracts.
- The real-data benchmark endpoint is also evidence only; it reports rolling-origin regret and data quality, not trading instructions.
- The calibrated and risk-adjusted selector endpoints are research read models only; neither is a promoted live selector.
- The forecast-dispatch sensitivity endpoint is explainability evidence only; it does not create or validate physical dispatch actions.
- The exogenous-signal endpoint is context only; it should be described as state awareness until benchmark evidence shows decision value.
- The operator recommendation endpoint is a product read model; it may show selected strategy and value previews, but it still does not emit market contracts.
- The shadow recommendation preview endpoint is a manual diagnostic read model. It may show DT-selected hourly actions as evidence, but it is not promotion, execution, or market submission.
- The future-stack endpoint is forecast evidence only. Full NeuralForecast NBEATSx / PyTorch-Forecasting TFT claims require official-adapter materialization and benchmark results.
- The DT policy-preview endpoint is an offline policy surface. It is not a live policy while `live_policy_claim=false` and `market_execution_enabled=false`.
