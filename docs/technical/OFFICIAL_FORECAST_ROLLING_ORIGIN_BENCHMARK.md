# Official Forecast Rolling-Origin Benchmark

This slice moves official NBEATSx/TFT adapters from single-horizon smoke evidence
to rolling-origin strict LP/oracle evidence. It is still research-only: the rows
are not full DFL, not Decision Transformer control, and not market execution.

## Purpose

The compact in-repo `nbeatsx_silver_v0` and `tft_silver_v0` candidates are useful
diagnostics, but they are not enough to judge the actual NBEATSx/TFT libraries.
The official benchmark therefore retrains the official adapters at each rolling
origin using only prior rows, masks future targets, and scores the resulting
forecast schedules through the same frozen Level 1 LP/oracle evaluator used for
`strict_similar_day`.

The question is deliberately narrow:

> Do serious but CPU-safe official NBEATSx/TFT forecasts reduce strict LP/oracle
> regret before DFL training is attempted?

## Asset

- Asset: `official_forecast_rolling_origin_benchmark_frame`
- Group: `gold_real_data_benchmark`
- Strategy kind: `official_forecast_rolling_origin_benchmark`
- Config:
  [../../configs/real_data_official_forecast_rolling_week3.yaml](../../configs/real_data_official_forecast_rolling_week3.yaml)
- Scale config:
  [../../configs/real_data_official_forecast_rolling_scale_week3.yaml](../../configs/real_data_official_forecast_rolling_scale_week3.yaml)

The asset consumes `real_data_benchmark_silver_feature_frame` and writes strategy
evaluation rows for:

- `strict_similar_day`;
- `nbeatsx_official_v0`;
- `tft_official_v0`.

Each anchor-specific official adapter receives a SOTA training frame where:

- train rows stop at or before the anchor timestamp;
- forecast rows cover the next 24 hours;
- forecast-row `y` is masked to null;
- weather features use the forecast-available path;
- observed OREE/Open-Meteo provenance is required for thesis-grade evidence.

## Default Week 3 Scope

The tracked config is intentionally CPU-safe:

- tenants: five canonical tenants;
- horizon: 24 DAM hours;
- anchors per tenant: 2 for first official rolling proof;
- NBEATSx: `max_steps=100`, fixed seed;
- TFT: `max_epochs=15`, small hidden sizes.

The scale config raises the first evidence run to four anchors per tenant while
keeping the same model settings. It is meant for runtime/robustness probing, not
as a production promotion run.

After this path materializes cleanly, the same asset can be rerun with more
anchors. The promotion authority remains the strict LP/oracle gate, not adapter
loss curves.

## Claim Boundary

This benchmark can support one of three conservative claims:

- official NBEATSx/TFT are adapter-ready but not value-improving;
- official NBEATSx/TFT improve raw neural evidence but still lose to
  `strict_similar_day`;
- one official source becomes a research challenger and must pass rolling
  robustness before any offline promotion claim.

It cannot support live execution, full DFL, or deployed Decision Transformer
claims.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,official_forecast_rolling_origin_benchmark_frame -c configs/real_data_official_forecast_rolling_week3.yaml
```

## Current Status

Implementation is additive and covered by focused tests. The first Compose-backed
run is complete.

Materialized result:

- Run `768c9796-422d-40b7-8f8d-083a861cc0e7` finished successfully.
- Scope: five canonical tenants, two latest eligible anchors per tenant,
  24-hour DAM horizon.
- Rows: 30 strict LP/oracle evaluation rows.
- Provenance: observed OREE/Open-Meteo rows through
  `real_data_benchmark_silver_feature_frame`.
- Adapter settings: NBEATSx `max_steps=100`; TFT `max_epochs=15`, hidden size
  `12`, hidden continuous size `6`.

| Model | Rows | Tenants | Anchors | Mean regret UAH | Median regret UAH | Mean decision value UAH |
|---|---:|---:|---:|---:|---:|---:|
| `strict_similar_day` | 10 | 5 | 2 | 1,587.505 | 1,477.071 | 1,646.419 |
| `nbeatsx_official_v0` | 10 | 5 | 2 | 1,782.829 | 1,398.064 | 1,451.095 |
| `tft_official_v0` | 10 | 5 | 2 | 2,055.488 | 1,755.815 | 1,178.436 |

Decision:

- The rolling official adapter path is now real and non-empty.
- A pipeline bug was found and fixed: NBEATSx previously dropped every
  minimum-history rolling train row when `lag_168_price_uah_mwh` was null, so
  it silently returned an empty readiness frame. NBEATSx now fills numeric
  feature nulls before training, matching TFT's null-handling behavior.
- This first official run does not promote either model. NBEATSx improved median
  regret versus strict on this tiny sample but had worse mean regret; TFT was
  worse on both mean and median regret.
- The next technical question is no longer whether the official rolling path can
  run. It is whether larger-anchor official training, better exogenous context,
  or a decision-loss layer can improve mean strict LP/oracle regret without
  worsening the strict baseline's stability.

Scale run:

- Run `bbbd5828-2414-42ce-b0df-ad175cbac445` finished successfully with the
  four-anchor scale config.
- Scope: five canonical tenants, four latest eligible anchors per tenant,
  24-hour DAM horizon.
- Rows: 60 strict LP/oracle evaluation rows.
- Runtime: official asset step took about 30 minutes on CPU.

| Model | Rows | Tenants | Anchors | Mean regret UAH | Median regret UAH | Mean decision value UAH |
|---|---:|---:|---:|---:|---:|---:|
| `strict_similar_day` | 20 | 5 | 4 | 1,020.821 | 771.866 | 1,851.909 |
| `nbeatsx_official_v0` | 20 | 5 | 4 | 1,508.667 | 1,277.428 | 1,364.062 |
| `tft_official_v0` | 20 | 5 | 4 | 1,535.299 | 1,065.955 | 1,337.430 |

Scale-run decision:

- Official NBEATSx/TFT are confirmed materializable, but they still lose to the
  frozen `strict_similar_day` control on both mean and median strict LP/oracle
  regret in the four-anchor sample.
- Scaling to all 104 anchors on local CPU is technically possible but expensive
  and unlikely to change the thesis direction without richer exogenous features
  or decision-focused loss. At the observed runtime, 18 anchors per tenant would
  likely take roughly two hours, and 104 anchors per tenant would be a much
  larger overnight/GPU-style run.
- The next useful slice should add market-coupling/exogenous feature governance
  and then route those features into official/DFL training, rather than only
  increasing official adapter epochs.

## Schedule/Value Promotion Follow-Up

The official rolling rows now feed an additive schedule/value promotion path:
[DFL_OFFICIAL_SCHEDULE_VALUE_PROMOTION.md](DFL_OFFICIAL_SCHEDULE_VALUE_PROMOTION.md).

That path keeps the same frozen LP/oracle evaluator and `strict_similar_day`
control, but converts official rows into the richer schedule-candidate library,
Schedule/Value Learner V2, rolling robustness, and offline promotion gate. It
does not change the official forecast benchmark itself and does not enable live
market execution.
