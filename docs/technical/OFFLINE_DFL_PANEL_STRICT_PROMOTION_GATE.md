# Offline DFL Panel Strict Promotion Gate

Date: 2026-05-07

This artifact moves the all-tenant offline DFL panel from relaxed-LP
development evidence into the frozen Level 1 strict LP/oracle promotion test.
It does not add neural training, does not change API/dashboard contracts, and
does not replace `strict_similar_day` as the control comparator.

## Scope

| Field | Value |
|---|---|
| Asset | `offline_dfl_panel_strict_lp_benchmark_frame` |
| Dagster group | `gold_dfl_training` |
| Strategy kind | `offline_dfl_panel_strict_lp_benchmark` |
| Inputs | `real_data_rolling_origin_benchmark_frame`, `offline_dfl_panel_experiment_frame` |
| Run config | [../../configs/real_data_offline_dfl_panel_strict_week3.yaml](../../configs/real_data_offline_dfl_panel_strict_week3.yaml) |
| Tenants | all five canonical tenants |
| Final holdout | latest 18 anchors per tenant, 90 tenant-anchor rows per source model |
| Claim scope | `offline_dfl_panel_strict_lp_gate_not_full_dfl` |

The strict builder reconstructs forecast and actual vectors from benchmark
payloads and routes three candidates through the existing
`evaluate_forecast_candidates_against_oracle` path:

1. `strict_similar_day`;
2. the raw source model, `tft_silver_v0` or `nbeatsx_silver_v0`;
3. `offline_dfl_panel_v2_<source_model>`, where the selected checkpoint
   horizon bias is added only to the source forecast vector.

The actual vectors and final-holdout selection are never changed by the v2
checkpoint.

## Materialization

Services were rebuilt so the committed source and config were visible inside
Dagster/API containers:

```powershell
docker compose config --quiet
docker compose up -d --build postgres mlflow dagster-webserver dagster-daemon api
docker compose ps
```

The full chained materialization command was started with the Week 3 strict
config, but the CLI process exceeded the 30-minute command timeout before
writing the strict rows. The successful strict sidecar run then loaded the
already materialized all-tenant benchmark and panel outputs from Dagster
storage:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select offline_dfl_panel_strict_lp_benchmark_frame -c configs/real_data_offline_dfl_panel_strict_week3.yaml
```

| Field | Value |
|---|---|
| Dagster run id | `ebea6ab3-d295-4585-8cc2-566bb7692581` |
| Strict rows persisted | 540 |
| Tenant count | 5 |
| Source models | `tft_silver_v0`, `nbeatsx_silver_v0` |
| V2 validation tenant-anchors | 180 total, 90 per source model |
| Export slug | `week3_offline_dfl_panel_strict_gate_90` |
| Local summary | `data/research_runs/week3_offline_dfl_panel_strict_gate_90/strict_gate_summary.json` |

## Strict Gate Result

The strict promotion gate blocks production promotion. The v2 checkpointed
panel does not beat the frozen strict LP/oracle control.

| Source model | Candidate | Rows | Tenants | Final holdout timestamps | Mean regret UAH | Median regret UAH |
|---|---|---:|---:|---:|---:|---:|
| `nbeatsx_silver_v0` | `strict_similar_day` | 90 | 5 | 18 | 314.81 | 202.61 |
| `nbeatsx_silver_v0` | `nbeatsx_silver_v0` | 90 | 5 | 18 | 813.40 | 520.48 |
| `nbeatsx_silver_v0` | `offline_dfl_panel_v2_nbeatsx_silver_v0` | 90 | 5 | 18 | 816.62 | 490.54 |
| `tft_silver_v0` | `strict_similar_day` | 90 | 5 | 18 | 314.81 | 202.61 |
| `tft_silver_v0` | `tft_silver_v0` | 90 | 5 | 18 | 1003.54 | 477.99 |
| `tft_silver_v0` | `offline_dfl_panel_v2_tft_silver_v0` | 90 | 5 | 18 | 989.55 | 435.16 |

Gate interpretation:

| Source model | Improvement vs raw | Improvement vs `strict_similar_day` | Median condition | Decision |
|---|---:|---:|---|---|
| `tft_silver_v0` | 1.39% | -214.33% | worse than strict | blocked |
| `nbeatsx_silver_v0` | -0.40% | -159.40% | worse than strict | blocked |

The TFT v2 checkpoint is a small diagnostic improvement over raw TFT under
strict scoring, but it remains much worse than `strict_similar_day`. NBEATSx v2
improves median regret versus raw NBEATSx but worsens mean regret, and also
loses decisively to the strict control. This is useful evidence: the system
rejects weak DFL-style candidates instead of claiming automatic ML superiority.

## Provenance Checks

| Check | Rows passing | Total rows |
|---|---:|---:|
| `data_quality_tier=thesis_grade` | 540 | 540 |
| `observed_coverage_ratio>=1.0` | 540 | 540 |
| `not_full_dfl=true` | 540 | 540 |
| `not_market_execution=true` | 540 | 540 |

The 18 final-holdout timestamps are shared across tenants, so Postgres reports
18 distinct `anchor_timestamp` values globally. The promotion gate counts
tenant-anchor pairs, which gives the intended 90 validation tenant-anchors per
source model.

## Claim Boundary

This artifact supports:

> The repo can evaluate all-tenant offline DFL v2 horizon-bias candidates
> through the same strict LP/oracle regret protocol used by the frozen Level 1
> benchmark.

It does not support:

- full DFL success;
- Decision Transformer deployment;
- live trading or market execution;
- replacement of `strict_similar_day`;
- European dataset ingestion.

## Next Work

The next DFL slice should improve the candidate generation or learning target
before increasing claim strength. A reasonable direction is to learn from the
strict LP/oracle promotion failures: compare horizon-bias learning against
rank/order-aware price features and explicitly target high-spread dispatch
hours while preserving the no-leakage holdout rule.
