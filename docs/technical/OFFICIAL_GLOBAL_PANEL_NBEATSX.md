# Official Global-Panel NBEATSx

Date: 2026-05-11

This slice starts the stronger official-forecast lane without replacing the
compact in-repo candidates. The goal is to give Nixtla NBEATSx a fairer
training contract before spending CPU/GPU time on full rolling-origin
promotion evidence.

## Why This Exists

The compact `nbeatsx_silver_v0` and `tft_silver_v0` candidates are still useful
for smoke tests, deterministic evidence plumbing, and fast DFL research assets.
They are not the thesis-grade official library candidates.

The previous serious official path used `nbeatsx_official_v0` and
`tft_official_v0`, but retrained per tenant and per anchor. That is clean for
rolling-origin evaluation, but it gives the neural models little data per fit
and creates heavy CPU cost. The global-panel lane instead builds one
point-in-time panel:

- `unique_id = tenant_id:DAM`;
- `ds` = hourly timestamp;
- `y` = observed UAH/MWh price on train rows and null on forecast rows;
- known-future exogenous columns are separated from historical-observed columns;
- scaler metadata is explicit and train-only per `unique_id`.

This is closer to how NeuralForecast/NBEATSx should be used for a small panel
of related time series.

## Assets

| Asset | Layer | Purpose |
|---|---|---|
| `official_forecast_exogenous_governance_frame` | Silver | Records market-coupling source governance for official forecast training without allowing external rows into Ukrainian training. |
| `official_global_panel_training_frame` | Silver | Builds the multi-tenant point-in-time `unique_id/ds/y` panel and records scaler/exogenous metadata. |
| `nbeatsx_official_global_panel_price_forecast` | Silver | Trains one Nixtla NBEATSx model over the panel and predicts all forecast rows. |
| `nbeatsx_official_global_panel_strict_lp_benchmark_frame` | Gold | Strict-scores the global-panel NBEATSx schedule beside frozen `strict_similar_day`. |
| `nbeatsx_official_global_panel_horizon_calibration_frame` | Gold | Builds prior-only horizon-bias calibration rows from strict-scored global-panel evidence. |
| `nbeatsx_official_global_panel_calibrated_strict_lp_benchmark_frame` | Gold | Strict-scores raw and calibrated global-panel NBEATSx beside `strict_similar_day`. |
| `nbeatsx_official_global_panel_rolling_strict_lp_benchmark_frame` | Gold | Runs several point-in-time global-panel fits and strict-scores the rolling anchors. |
| `nbeatsx_official_global_panel_rolling_horizon_calibration_frame` | Gold | Builds prior-only horizon-bias calibration from the rolling strict evidence. |
| `nbeatsx_official_global_panel_rolling_calibrated_strict_lp_benchmark_frame` | Gold | Strict-scores raw and calibrated rolling global-panel NBEATSx beside `strict_similar_day`. |
| `dfl_official_global_panel_schedule_candidate_library_frame` | Gold | Converts calibrated global-panel strict rows into feasible schedule candidates for schedule/value screening. |
| `dfl_official_global_panel_schedule_candidate_library_v2_frame` | Gold | Adds deterministic blend/residual schedule families using prior anchors only. |
| `dfl_official_global_panel_schedule_value_learner_v2_frame` | Gold | Selects schedule/value profiles from prior anchors, not final-holdout outcomes. |
| `dfl_official_global_panel_schedule_value_learner_v2_strict_lp_benchmark_frame` | Gold | Strict-scores the selected global-panel schedule/value candidates. |
| `dfl_official_global_panel_schedule_value_learner_v2_robustness_frame` | Gold | Reports rolling-window screening evidence for the selected schedule/value candidates. |
| `dfl_official_global_panel_schedule_value_production_gate_frame` | Gold | Applies the full promotion rule and keeps market execution disabled. |

Tracked config:
[configs/real_data_official_global_panel_nbeatsx_week3.yaml](../../configs/real_data_official_global_panel_nbeatsx_week3.yaml).

Backfill config:
[configs/real_data_official_global_panel_nbeatsx_backfill_week3.yaml](../../configs/real_data_official_global_panel_nbeatsx_backfill_week3.yaml).

Resumable local runner:

```powershell
.\scripts\run-official-global-panel-batches.ps1 `
  -TotalAnchors 365 `
  -BatchSize 4 `
  -GeneratedAtIso 2026-05-11T20:00:00Z
```

The runner writes per-batch configs under
`.tmp_runtime/official_global_panel_batches/`, materializes
`nbeatsx_official_global_panel_rolling_strict_lp_benchmark_frame` in anchor
batches, and keeps one fixed `generated_at` so Postgres rows can be merged by
batch without losing previous work.

## Contract

The global-panel training frame must satisfy:

- all forecast-horizon `y` values are null;
- feature/scaler metadata is fitted only on train rows per `unique_id`;
- `weather_temperature` and other known future fields appear only in the
  known-future feature list;
- lagged/rolling price features appear in historical-observed feature lists;
- external market-coupling sources appear only in governance metadata until
  licensing, timezone, currency normalization, market-rule mapping, temporal
  availability, and domain-shift controls are completed;
- if an external source is marked `training_use_allowed=true` before those
  controls are ready, the official panel builder fails instead of silently
  adding the feature;
- `not_full_dfl=true` and `not_market_execution=true` remain claim boundaries.

Mutating final-holdout actual prices may change final LP/oracle scoring later,
but it must not change forecast features, scaler metadata, or selected model
configuration.

## Current Claim

Implemented now:

- additive global-panel SOTA training contract;
- additive official global-panel NBEATSx adapter surface;
- additive strict LP/oracle scoring asset for the global-panel NBEATSx output.
- additive prior-only horizon calibration and calibrated strict LP gate.
- additive rolling-window global-panel NBEATSx screening path.
- additive schedule/value screening path for official global-panel evidence.

Materialized evidence on 2026-05-11:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs `
  --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,official_forecast_exogenous_governance_frame,official_global_panel_training_frame,nbeatsx_official_global_panel_price_forecast,nbeatsx_official_global_panel_strict_lp_benchmark_frame,nbeatsx_official_global_panel_horizon_calibration_frame,nbeatsx_official_global_panel_calibrated_strict_lp_benchmark_frame `
  -c configs/real_data_official_global_panel_nbeatsx_week3.yaml
```

Result:

| Evidence item | Value |
|---|---:|
| Run status | `RUN_SUCCESS` |
| CPU NBEATSx fit time | ~37 seconds |
| Tenants | 5 |
| Strict-score anchors per tenant | 1 |
| Anchor | `2026-01-30 23:00` |
| `strict_similar_day` mean regret | 1495.71 UAH |
| `nbeatsx_official_global_panel_v1` mean regret | 1559.66 UAH |
| Calibrated NBEATSx mean regret | 1559.66 UAH |
| Tenant wins vs strict | Kyiv only |

Interpretation: the official global-panel path is now runnable and far faster
than the previous tenant/anchor retraining path, but a single-anchor result is
not promotion evidence. The calibrated candidate is currently identical to raw
NBEATSx because the prior-only calibration gate has insufficient prior
global-panel anchors.

Windowed rolling evidence on 2026-05-11:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs `
  --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,official_forecast_exogenous_governance_frame,nbeatsx_official_global_panel_rolling_strict_lp_benchmark_frame `
  -c configs/real_data_official_global_panel_nbeatsx_week3.yaml
```

Result:

| Evidence item | Value |
|---|---:|
| Run status | `RUN_SUCCESS` |
| Latest governed run id | `179771f8-5821-4e69-9642-c64c23e2fc3a` |
| Rolling windows | 4 |
| Tenants | 5 |
| Strategy rows | 40 |
| Anchor range | `2026-01-27 23:00` to `2026-01-30 23:00` |
| `strict_similar_day` mean regret | 1020.82 UAH |
| `strict_similar_day` median regret | 771.87 UAH |
| `nbeatsx_official_global_panel_v1` mean regret | 1387.85 UAH |
| `nbeatsx_official_global_panel_v1` median regret | 870.10 UAH |
| NBEATSx rank-1 tenant-anchor rows | 5 / 20 |

Window detail:

| Anchor | Strict mean regret | NBEATSx mean regret | NBEATSx rank-1 rows |
|---|---:|---:|---:|
| `2026-01-27 23:00` | 583.61 | 1226.48 | 0 / 5 |
| `2026-01-28 23:00` | 324.66 | 779.13 | 0 / 5 |
| `2026-01-29 23:00` | 1679.30 | 1986.14 | 4 / 5 |
| `2026-01-30 23:00` | 1495.71 | 1559.66 | 1 / 5 |

Interpretation: the official global-panel path is operationally viable, but
raw NBEATSx still loses the strict LP/oracle gate on both mean and median regret.
The useful signal is regime-specific: NBEATSx can win some stressed
tenant-anchor rows, but it is not yet a default or production-promotion
candidate.

Rolling calibration evidence on 2026-05-11:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs `
  --select nbeatsx_official_global_panel_rolling_horizon_calibration_frame,nbeatsx_official_global_panel_rolling_calibrated_strict_lp_benchmark_frame `
  -c configs/real_data_official_global_panel_nbeatsx_week3.yaml
```

Result:

| Evidence item | Value |
|---|---:|
| Run status | `RUN_SUCCESS` |
| Latest governed run id | `179771f8-5821-4e69-9642-c64c23e2fc3a` |
| Strategy rows | 60 |
| Tenants | 5 |
| Rolling windows | 4 |
| Anchor range | `2026-01-27 23:00` to `2026-01-30 23:00` |
| `strict_similar_day` mean regret | 1020.82 UAH |
| `strict_similar_day` median regret | 771.87 UAH |
| Raw NBEATSx mean regret | 1387.85 UAH |
| Raw NBEATSx median regret | 870.10 UAH |
| Calibrated NBEATSx mean regret | 1234.84 UAH |
| Calibrated NBEATSx median regret | 793.88 UAH |
| External feature training status | `blocked_by_governance` |

Window detail:

| Anchor | Strict mean regret | Raw NBEATSx mean regret | Calibrated NBEATSx mean regret |
|---|---:|---:|---:|
| `2026-01-27 23:00` | 583.61 | 1226.48 | 1226.48 |
| `2026-01-28 23:00` | 324.66 | 779.13 | 779.13 |
| `2026-01-29 23:00` | 1679.30 | 1986.14 | 2168.55 |
| `2026-01-30 23:00` | 1495.71 | 1559.66 | 765.20 |

Interpretation: prior-only horizon calibration improves the rolling mean regret
versus raw NBEATSx, but it still loses to `strict_similar_day` on both mean and
median regret. The improvement is concentrated in the latest window, while the
earlier calibrated windows remain weak. This supports the next direction:
regime-gated use of official NBEATSx plus UA backfill and market-coupling
exogenous features, not a default controller switch.

Official global-panel schedule/value screen on 2026-05-11:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs `
  --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,official_forecast_exogenous_governance_frame,nbeatsx_official_global_panel_rolling_strict_lp_benchmark_frame,nbeatsx_official_global_panel_rolling_horizon_calibration_frame,nbeatsx_official_global_panel_rolling_calibrated_strict_lp_benchmark_frame,dfl_official_global_panel_schedule_candidate_library_frame,dfl_official_global_panel_schedule_candidate_library_v2_frame,dfl_official_global_panel_schedule_value_learner_v2_frame,dfl_official_global_panel_schedule_value_learner_v2_strict_lp_benchmark_frame,dfl_official_global_panel_schedule_value_learner_v2_robustness_frame,dfl_official_global_panel_schedule_value_production_gate_frame `
  -c configs/real_data_official_global_panel_nbeatsx_week3.yaml
```

Result:

| Evidence item | Value |
|---|---:|
| Full materialization run id | `e072d319-fc9a-4de6-b648-264d550e93ae` |
| Gate rematerialization run id | `1bbf3da4-2678-4702-a7a2-a2f003264b88` |
| Strict/schedule-value generated at | `2026-05-11 17:16:42.416299+00` |
| Latest validation tenant-anchors per source | 5 |
| Rolling screening windows | 2 |
| `strict_similar_day` latest mean regret | 1495.71 UAH |
| Calibrated-source schedule/value latest mean regret | 598.09 UAH |
| Raw-source schedule/value latest mean regret | 1347.36 UAH |
| Calibrated-source latest improvement vs strict | 60.01% |
| Raw-source latest improvement vs strict | 9.92% |
| Rolling strict-pass windows | 1 / 2 for both sources |
| Production promotion | `false` |
| Promotion blocker | `validation_undercoverage` |
| Fallback strategy | `strict_similar_day_default_fallback` |

Interpretation: the schedule/value layer can extract useful candidate schedules
from the official global-panel evidence, especially from the calibrated source
in the latest window. This is not enough for promotion. The full promotion
threshold remains 90 latest validation tenant-anchors per source and 3 of 4
rolling strict-control passes; the current four-anchor global-panel run is only
a screening result. This fixes an important governance detail: screening
thresholds must not be reused as production-promotion thresholds.

365-anchor UA backfill batch evidence on 2026-05-11:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs `
  --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,official_forecast_exogenous_governance_frame,nbeatsx_official_global_panel_rolling_strict_lp_benchmark_frame `
  -c configs/real_data_official_global_panel_nbeatsx_backfill_week3.yaml
```

Result:

| Evidence item | Value |
|---|---:|
| Dagster run id | `b27b1bae-707c-4873-9148-a1d86a739dbd` |
| Fixed generated_at | `2026-05-11 20:00:00+00` |
| Batch anchors | 4 |
| Tenants | 5 |
| Persisted rows | 40 |
| Anchor range | `2025-04-22 23:00` to `2025-04-25 23:00` |
| `strict_similar_day` mean regret | 505.49 UAH |
| Raw global-panel NBEATSx mean regret | 910.82 UAH |
| Production promotion | `false` |
| Promotion blocker | incomplete batch coverage |

Interpretation: the source-backed 365-anchor UA panel can train and strict-score
official global-panel NBEATSx in resumable batches. The first chronological
batch does not beat `strict_similar_day`; this is expected because it is an
early-window batch, not the latest 90-anchor promotion set. The next step is to
continue batches with the same `generated_at`, then run calibration,
schedule/value, robustness, and production-gate assets only after enough
persisted anchors exist.

Not implemented yet:

- source/regime production promotion;
- completed 365-anchor official global-panel backfill. Batch execution is now
  resumable, but only the first 4-anchor batch has been materialized locally.
- Hugging Face Jobs execution. A payload builder now exists, but it only writes
  a redacted JSON/script for a future remote run and does not submit paid
  compute.
- broader UA backfill or market-coupling exogenous training features.

## Next Required Work

1. Expand official global-panel evaluation to promotion-grade coverage only
   after UA backfill or resumable batching can provide enough anchors.
2. Add UA backfill and market-coupling exogenous governance before increasing
   official model capacity.
3. Use the Hugging Face Jobs payload builder for a latest-first GPU screen only
   after the branch is pushed and artifact upload credentials are available.

The frozen `strict_similar_day` baseline remains the default fallback until the
strict LP/oracle gate passes.
