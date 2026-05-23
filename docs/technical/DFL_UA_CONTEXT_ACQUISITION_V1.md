# UA Context Acquisition Readiness v1

## Status

This slice is an additive **Offline Strategy Promotion** readiness gate. It does
not train another selector, start DT/LAVA, create live dispatch, or switch any
dashboard/API default. `market_execution_enabled=false` remains mandatory.

Frozen comparator remains Ukrainian-only calibrated V2+:

- mean regret: `174.77` UAH;
- median regret: `67.30` UAH;
- rolling robustness: `4 / 4`;
- strict fallback: `strict_similar_day`.

## Why This Exists

V10 showed that the current candidate space is exhausted: generated final
candidates transferred as tail-risk, not safe wins. The blocker is not missing
neural capacity. The blocker is missing prior-supported context and unstable
forecast extrema. Before V11 lower-tail-risk schedules are generated, this gate
checks whether the required Ukrainian context is actually source-backed and
available before each anchor.

## Assets

| Asset | Purpose |
|---|---|
| `dfl_ua_context_source_inventory_frame` | Lists Ukrainian source families used by the gate: OREE DAM, Open-Meteo archive, tenant load/PV proxy, Ukrenergo/grid-event history, and calendar/block context. |
| `dfl_ua_dam_publication_backfill_frame` | Requires explicit DAM publication metadata before the anchor; it does not infer publication time from observed prices. |
| `dfl_ua_weather_load_pv_proxy_backfill_frame` | Checks prior Open-Meteo weather plus tenant load/PV proxy rows. |
| `dfl_ua_grid_event_backfill_frame` | Builds a readiness-specific grid signal from the observed OREE backfill window plus `ukrenergo_grid_events_bronze`. A zero-event row counts only when the Ukrenergo source coverage window spans the anchor; otherwise the blocker is `missing_grid_event_history_source_window`. |
| `dfl_ua_calendar_block_context_backfill_frame` | Adds deterministic calendar/block context while marking DST/calendar gap hours instead of synthesizing them. |
| `dfl_ua_context_backfill_coverage_gate_frame` | Emits `v11_candidate_generation_ready`; V11 starts only when all required context families are ready. |

## Run

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,tenant_consumption_schedule_bronze,tenant_historical_net_load_silver,ukrenergo_grid_events_bronze,dfl_ua_context_source_inventory_frame,dfl_ua_dam_publication_backfill_frame,dfl_ua_weather_load_pv_proxy_backfill_frame,dfl_ua_grid_event_backfill_frame,dfl_ua_calendar_block_context_backfill_frame,dfl_ua_context_backfill_coverage_gate_frame `
  -c configs/real_data_dfl_ua_context_acquisition_v11_precondition_week3.yaml
```

If upstream V10 rows are not present in the active Dagster IO store, materialize
the V10 closure selection from `DFL_V10_TAIL_RISK_TRANSFER_AUDIT.md` first.

## Latest Materialized Status

The refreshed source-repair materialization
`03ca46ca-e2eb-43e2-ba59-c8d5659cc28a` keeps V11 blocked:

- `v11_candidate_generation_ready=false`;
- OREE DAM price-history coverage is present, but explicit publication-time
  metadata is still absent for `860 / 860` gate rows;
- Open-Meteo plus tenant load/PV proxy coverage is now complete:
  `860 / 860` ready rows;
- calendar/block context remains complete: `860 / 860` ready rows;
- Ukrenergo grid-event/no-event coverage is not yet historical enough for the
  benchmark anchors: `0 / 860` ready rows with
  `missing_grid_event_history_source_window`.

This is the intended conservative result: source coverage can improve without
unlocking V11 until every required context family is prior-available.

Export local packet:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_ua_context_backfill_readiness_packet.py `
  --source-inventory-pickle <path> `
  --dam-publication-pickle <path> `
  --weather-load-pv-pickle <path> `
  --grid-event-pickle <path> `
  --calendar-block-pickle <path> `
  --coverage-gate-pickle <path> `
  --run-slug week3_dfl_ua_context_acquisition_v1
```

## Gate

`v11_candidate_generation_ready=true` only when every required V10 backfill row
has prior-available:

- explicit OREE DAM publication metadata;
- Open-Meteo weather plus tenant load/PV proxy context;
- Ukrenergo/grid-event coverage or explicit source-backed no-event coverage;
- calendar/block context without DST/calendar synthesis.

If any family is missing, the gate emits `data_acquisition_needed`, exports a
blocked readiness packet, and V11 candidate generation remains blocked.

## Claim Boundary

No Poland/EU rows become Ukrainian target rows. Poland/TFT remain
shadow/context evidence only. This slice is not full DFL, not deployed
Decision Transformer control, not live market execution, and not a dashboard/API
default strategy switch.
