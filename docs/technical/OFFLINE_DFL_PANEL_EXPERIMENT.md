# Offline DFL Panel Experiment

Date: 2026-05-07

This slice expands the offline DFL foundation from one Dnipro tenant to the
five canonical tenants. It keeps the engineering evidence Ukrainian
OREE/Open-Meteo only and treats European datasets as future external-validation
context. The result is still a relaxed-LP research panel: not full DFL, not a
Decision Transformer, and not market execution.

## Scope

| Field | Value |
|---|---|
| Asset | `offline_dfl_panel_experiment_frame` |
| Dagster group | `gold_dfl_training` |
| Source asset | `real_data_rolling_origin_benchmark_frame` |
| Tenants | `client_001_kyiv_mall`, `client_002_lviv_office`, `client_003_dnipro_factory`, `client_004_kharkiv_hospital`, `client_005_odesa_hotel` |
| Data window | Observed OREE DAM + historical Open-Meteo, `2026-01-01` to `2026-04-30` |
| Run config | [../../configs/real_data_offline_dfl_panel_week3.yaml](../../configs/real_data_offline_dfl_panel_week3.yaml) |
| Final holdout | latest 18 anchors per tenant, 90 tenant-anchor rows per model |
| Claim scope | `offline_dfl_panel_experiment_not_full_dfl` |

## Experiment Design

The panel compares three relaxed-LP paths for each tenant/model stream:

1. raw relaxed-LP baseline with no learned horizon bias;
2. v0 horizon-bias learner trained on prior anchors;
3. v2 checkpointed horizon-bias learner, where the checkpoint is selected only
   on prior inner-validation anchors.

The final holdout is never used for learning or checkpoint selection. It is
used only to score raw relaxed regret, v0 relaxed regret, and v2 relaxed regret.
The implementation trains per tenant/model so tenant-specific starting SOC
values are not mixed into a single relaxed-LP batch.

## Materialization

Command used:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,offline_dfl_panel_experiment_frame -c configs/real_data_offline_dfl_panel_week3.yaml
```

Run outcome:

| Field | Value |
|---|---|
| Dagster run id | `5b759ed9-ae80-4c10-b049-7d39eed64d04` |
| Materialized rows | 10 |
| Dnipro evidence check | `dnipro_thesis_grade_90_anchor_evidence` passed |
| Export slug | `week3_offline_dfl_panel_v2_90` |
| Export path | `data/research_runs/week3_offline_dfl_panel_v2_90/` |

Latest source batches:

| Tenant | Rows | Anchors | Latest `generated_at` |
|---|---:|---:|---|
| `client_001_kyiv_mall` | 270 | 90 | `2026-05-07T13:01:47.913622Z` |
| `client_002_lviv_office` | 270 | 90 | `2026-05-07T13:06:56.544174Z` |
| `client_003_dnipro_factory` | 270 | 90 | `2026-05-07T13:11:49.276420Z` |
| `client_004_kharkiv_hospital` | 270 | 90 | `2026-05-07T13:16:51.077601Z` |
| `client_005_odesa_hotel` | 270 | 90 | `2026-05-07T13:21:10.424571Z` |

## Results

The development gate passed because each model has 90 final-holdout
tenant-anchors, thesis-grade observed provenance, `not_full_dfl=true`,
`not_market_execution=true`, and positive relaxed-regret improvement. This is a
development gate only. Production promotion remains blocked until a later
strict-LP/oracle promotion gate passes.

| Model | Final holdout tenant-anchors | Raw relaxed regret | v2 relaxed regret | Improvement |
|---|---:|---:|---:|---:|
| `nbeatsx_silver_v0` | 90 | 2154.92 | 2121.83 | 1.54% |
| `tft_silver_v0` | 90 | 2791.38 | 2665.30 | 4.52% |

Finding: the v2 checkpoint rule is materially better than the negative Dnipro
v0 result, but it is still relaxed-LP evidence. It does not prove full DFL,
does not beat the frozen strict-LP control, and should be framed as a readiness
step toward a stricter DFL experiment.

## API And Postgres Validation

Latest Postgres raw benchmark rows agree with the intended all-tenant panel:

| Tenant | Latest rows | Latest anchors |
|---|---:|---:|
| `client_001_kyiv_mall` | 270 | 90 |
| `client_002_lviv_office` | 270 | 90 |
| `client_003_dnipro_factory` | 270 | 90 |
| `client_004_kharkiv_hospital` | 270 | 90 |
| `client_005_odesa_hotel` | 270 | 90 |

Dnipro dashboard read models remain aligned:

| Endpoint | Generated at | Anchors | Models | Tier / rows | Summary |
|---|---|---:|---:|---|---|
| `/dashboard/real-data-benchmark` | `2026-05-07T13:11:49.276420Z` | 90 | 3 | `thesis_grade` | best model `strict_similar_day`, mean regret 1938.98 UAH |
| `/dashboard/calibrated-ensemble-benchmark` | `2026-05-07T12:03:38.940252Z` | 90 | 1 | `thesis_grade` | mean regret 1479.65 UAH |
| `/dashboard/risk-adjusted-value-gate` | `2026-05-07T12:03:38.940252Z` | 90 | 1 | `thesis_grade` | mean regret 1428.59 UAH |
| `/dashboard/forecast-dispatch-sensitivity` | `2026-05-07T12:03:38.940252Z` | 90 | 5 | 450 rows | diagnostic buckets present in the read model rows |

## European Dataset Bridge

European datasets are cited now only as future external-validation and
market-coupling sources. They are not ingested in this slice.

| Source | Future use | Current status |
|---|---|---|
| [ENTSO-E Transparency Platform](https://www.entsoe.eu/data/transparency-platform/) | Cross-border European market fundamentals and market-coupling context. | Mention only. |
| [Open Power System Data](https://open-power-system-data.org/) | Open European power-system data packages for reproducibility checks. | Mention only. |
| [OPSD time series](https://data.open-power-system-data.org/time_series/) | Future spot-price, load, wind, and solar time-series comparison. | Mention only. |
| [Nord Pool Data Portal](https://www.nordpoolgroup.com/en/services/power-market-data-services/dataportalregistration/) | Future Nordic day-ahead/intraday data reference if access is approved. | Mention only. |
| [Ember API](https://ember-energy.org/data/api) | Future high-level electricity demand, generation, emissions, and carbon-intensity context. | Mention only. |

## Verification

Commands run:

```powershell
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\dfl\test_offline_dfl_experiment.py tests\assets\test_dfl_research_assets.py
.\scripts\verify.ps1
uv run dg list defs --json
uv run dg check defs
docker compose config --quiet
git diff --check
```

Verification result: focused tests passed, full repo verification passed with
186 tests, Dagster definitions loaded, Compose config passed, and whitespace
checks passed.

## Claim Boundary

This artifact supports:

> The repo can now materialize a five-tenant, 90-anchor-per-tenant offline DFL
> v2 panel with validation-safe checkpoint selection and positive relaxed-LP
> holdout improvement.

It does not support:

- full differentiable DFL success;
- Decision Transformer deployment;
- market execution or live trading;
- replacement of `strict_similar_day` as the frozen Level 1 control comparator;
- European dataset ingestion.
