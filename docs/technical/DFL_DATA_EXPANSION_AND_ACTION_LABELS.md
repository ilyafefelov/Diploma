# UA-First DFL Data Expansion And Action Labels

Date: 2026-05-07

This slice shifts the DFL path from forecast-correction experiments toward
better in-domain evidence. The v4 action-target candidate improved the TFT
diagnostic stream but still lost decisively to `strict_similar_day`; the next
foundation is therefore coverage and action labels, not another neural model.

## Scope

| Field | Value |
|---|---|
| Coverage asset | `dfl_data_coverage_audit_frame` |
| Action-label asset | `dfl_action_label_panel_frame` |
| Dagster group | `gold_dfl_training` |
| Run config | [../../configs/real_data_dfl_data_expansion_week3.yaml](../../configs/real_data_dfl_data_expansion_week3.yaml) |
| Tenants | all five canonical tenants |
| Source models | `tft_silver_v0`, `nbeatsx_silver_v0` |
| Market | DAM |
| Currency | UAH |
| Default observed window | `2026-01-01` to `2026-04-30` |
| Benchmark cap | `max_anchors=120`, with the audit reporting the true ceiling |
| Final holdout | latest 18 anchors per tenant/model |
| Claim scope | `dfl_action_label_panel_not_full_dfl` |

The action-label panel stores one row per tenant/source-model/anchor. Each row
joins the raw forecast candidate to the frozen `strict_similar_day` control and
adds oracle LP action targets derived from actual horizon prices. It keeps
candidate vectors, strict-baseline vectors, oracle signed dispatch, oracle SOC
trajectory, target charge/discharge/hold masks, regret, throughput,
degradation proxy, provenance, and claim-boundary flags.

This is still not full DFL, not Decision Transformer control, and not market
execution.

## Materialization

Use the Compose-backed Dagster service after rebuilding backend images:

```powershell
docker compose config --quiet
docker compose up -d --build postgres mlflow dagster-webserver dagster-daemon api
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,dfl_training_example_frame,dfl_data_coverage_audit_frame,dfl_action_label_panel_frame -c configs/real_data_dfl_data_expansion_week3.yaml
```

Export/registry slug for local evidence:

```text
week3_dfl_data_expansion_ua_panel
```

## Current Evidence

Latest checked state verified on 2026-05-08.

| Evidence item | Result |
|---|---:|
| Dagster materialization/check run | `3743f42c-8cc6-4822-a3f0-7730af6af458` |
| Dagster readiness check | `dfl_action_label_panel_readiness_evidence=passed` |
| Coverage-audit tenants | 5 |
| Eligible daily anchors per tenant in feature frame | 104 |
| Target anchors per tenant | 90 |
| Tenants meeting target | 5 |
| Feature-frame source gaps | 1 price hour and 1 weather hour per tenant |
| Latest benchmark anchors per tenant/model batch | 104 |
| Latest benchmark model count | 3 |
| Persisted `dfl_action_label_vectors` rows | 1,040 |
| Train-selection action-label rows | 860 |
| Final-holdout action-label rows | 180 |
| Final-holdout window | `2026-04-12 23:00` to `2026-04-29 23:00` |
| Vector-length validation failures | 0 |
| Safety violations | 0 |

Coverage audit detail:

| Tenant | Eligible anchors | Data quality note |
|---|---:|---|
| `client_001_kyiv_mall` | 104 | target met; feature frame has 1 price/weather gap |
| `client_002_lviv_office` | 104 | target met; feature frame has 1 price/weather gap |
| `client_003_dnipro_factory` | 104 | target met; feature frame has 1 price/weather gap |
| `client_004_kharkiv_hospital` | 104 | target met; feature frame has 1 price/weather gap |
| `client_005_odesa_hotel` | 104 | target met; feature frame has 1 price/weather gap |

The action-label panel is intentionally built from the latest thesis-grade
104-anchor benchmark panel. Each tenant/model has 86 `train_selection` rows and
18 `final_holdout` rows. All persisted action-label rows are observed coverage,
`thesis_grade`, `not_full_dfl=true`, and `not_market_execution=true`.

The local research-layer export exists at:

```text
data/research_runs/week3_dfl_data_expansion_ua_panel/research_layer_manifest.json
```

That export uses the existing research-layer store path and therefore records
benchmark, calibration, selector, and `dfl_training_frame` summaries. The new
action-label vectors are validated through Postgres table
`dfl_action_label_vectors`.

API spot checks remained aligned with the existing read models:

| Endpoint | Dnipro result |
|---|---|
| `/dashboard/real-data-benchmark` | `data_quality_tier=thesis_grade`, `anchor_count=104`, `model_count=3`, `rows=312` |
| `/dashboard/calibrated-ensemble-benchmark` | `rows=90`, `selector_rows=90` |
| `/dashboard/risk-adjusted-value-gate` | `rows=90` |
| `/dashboard/forecast-dispatch-sensitivity` | `rows=450` |

Dataset-card summary: [DFL_ACTION_LABEL_DATASET_CARD.md](DFL_ACTION_LABEL_DATASET_CARD.md).

## European Dataset Bridge

European datasets are now part of the research source map but are not training
inputs for the Ukrainian DFL panel.

| Source | Status | Current use |
|---|---|---|
| [ENTSO-E Transparency Platform](https://www.entsoe.eu/data/transparency-platform/) | watch | Future external validation and market-coupling context. |
| [Open Power System Data time series](https://data.open-power-system-data.org/time_series/) | watch | Future European hourly price/load/renewables context. |
| [Ember API](https://ember-energy.org/data/api/) | watch | Future generation, demand, emissions, and carbon-intensity context. |
| [Nord Pool Data Portal](https://www.nordpoolgroup.com/en/services/power-market-data-services/dataportalregistration/) | watch/restricted | Future reference only if paid/API access is approved. |

These rows are explicitly `training_use_allowed=false` until currency,
timezone, price-cap, market-rule, licensing/API, and domain-shift normalization
questions are resolved. Current thesis-grade evidence remains Ukrainian
observed OREE DAM plus Open-Meteo.

## Claim Boundary

This artifact supports:

> The project can audit observed Ukrainian data coverage and materialize a
> strict LP/oracle action-label panel for later DFL training.

It does not support:

- full DFL success;
- Decision Transformer deployment;
- live trading or bid submission;
- European data ingestion;
- replacement of the frozen `strict_similar_day` control.
