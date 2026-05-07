# Offline DFL Decision-Target v3

Date: 2026-05-07

This artifact tests a stricter follow-up to the all-tenant DFL panel: a
decision-targeted forecast correction selected by prior strict LP/oracle
regret. It does not add neural training, does not change API/dashboard
contracts, and does not replace `strict_similar_day` as the frozen Level 1
control comparator.

## Scope

| Field | Value |
|---|---|
| Parameter-selection asset | `offline_dfl_decision_target_panel_frame` |
| Strict benchmark asset | `offline_dfl_decision_target_strict_lp_benchmark_frame` |
| Dagster group | `gold_dfl_training` |
| Strategy kind | `offline_dfl_decision_target_strict_lp_benchmark` |
| Inputs | `real_data_rolling_origin_benchmark_frame`, `offline_dfl_panel_experiment_frame` |
| Run config | [../../configs/real_data_offline_dfl_decision_target_week3.yaml](../../configs/real_data_offline_dfl_decision_target_week3.yaml) |
| Tenants | all five canonical tenants |
| Final holdout | latest 18 anchors per tenant, 90 tenant-anchor rows per source model |
| Claim scope | `offline_dfl_decision_target_v3_strict_lp_gate_not_full_dfl` |

The v3 correction rule is:

```text
corrected = mean(raw)
  + spread_scale * (raw - mean(raw))
  + mean_shift_uah_mwh
  + optional_panel_v2_horizon_bias
```

The grid is intentionally small and audit-friendly:

| Parameter | Values |
|---|---|
| `spread_scale` | `0.75`, `1.0`, `1.25`, `1.5` |
| `mean_shift_uah_mwh` | `-500`, `0`, `500` |
| `include_panel_v2_bias` | `false`, `true` |

Parameters are selected only on prior/inner-validation anchors. Final-holdout
actuals are used only for final strict scoring.

## Materialization

Services were rebuilt so Dagster and API containers used the committed source:

```powershell
docker compose config --quiet
docker compose up -d --build postgres mlflow dagster-webserver dagster-daemon api
docker compose ps
```

Then the full all-tenant v3 path materialized successfully:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,offline_dfl_panel_experiment_frame,offline_dfl_decision_target_panel_frame,offline_dfl_decision_target_strict_lp_benchmark_frame -c configs/real_data_offline_dfl_decision_target_week3.yaml
```

| Field | Value |
|---|---|
| Dagster run id | `9f5962e9-fe56-4b45-bcfa-d1a233fbffdb` |
| Strict rows persisted | 720 |
| Tenant count | 5 |
| Source models | `tft_silver_v0`, `nbeatsx_silver_v0` |
| V3 validation tenant-anchors | 180 total, 90 per source model |
| Local summary | `data/research_runs/week3_offline_dfl_decision_target_v3_90/decision_target_v3_summary.json` |

API sanity check after the run: `/dashboard/real-data-benchmark?tenant_id=client_003_dnipro_factory`
returned `data_quality_tier=thesis_grade`, `anchor_count=90`, and
`model_count=3`.

## Strict Gate Result

The strict promotion gate blocks production promotion. V3 does not beat the
frozen strict LP/oracle control, and therefore remains research evidence only.

| Source model | Candidate | Rows | Tenant-anchors | Mean regret UAH | Median regret UAH |
|---|---|---:|---:|---:|---:|
| `nbeatsx_silver_v0` | `strict_similar_day` | 90 | 90 | 314.81 | 202.61 |
| `nbeatsx_silver_v0` | `nbeatsx_silver_v0` | 90 | 90 | 813.40 | 520.48 |
| `nbeatsx_silver_v0` | `offline_dfl_panel_v2_nbeatsx_silver_v0` | 90 | 90 | 816.62 | 490.54 |
| `nbeatsx_silver_v0` | `offline_dfl_decision_target_v3_nbeatsx_silver_v0` | 90 | 90 | 814.17 | 461.35 |
| `tft_silver_v0` | `strict_similar_day` | 90 | 90 | 314.81 | 202.61 |
| `tft_silver_v0` | `tft_silver_v0` | 90 | 90 | 1003.54 | 477.99 |
| `tft_silver_v0` | `offline_dfl_panel_v2_tft_silver_v0` | 90 | 90 | 989.55 | 435.16 |
| `tft_silver_v0` | `offline_dfl_decision_target_v3_tft_silver_v0` | 90 | 90 | 1015.36 | 477.99 |

Gate interpretation:

| Source model | V3 improvement vs raw | V3 improvement vs panel v2 | V3 improvement vs `strict_similar_day` | Median condition | Decision |
|---|---:|---:|---:|---|---|
| `nbeatsx_silver_v0` | -0.09% | 0.30% | -158.62% | worse than strict | blocked |
| `tft_silver_v0` | -1.18% | -2.61% | -222.53% | worse than strict | blocked |

The NBEATSx v3 candidate slightly improves over panel v2 and reduces median
regret versus raw NBEATSx, but it still loses decisively to `strict_similar_day`.
The TFT v3 candidate regresses versus both raw TFT and panel v2. This is useful
DFL-readiness evidence: affine mean/spread correction is not enough, so the
project should not claim full DFL or policy improvement.

## Selected Parameters

V3 selected different parameter combinations by tenant/source, which confirms
the selector is tenant-specific and prior-window based.

| Source model | Tenant | `spread_scale` | `mean_shift_uah_mwh` | `include_panel_v2_bias` |
|---|---|---:|---:|---|
| `nbeatsx_silver_v0` | `client_001_kyiv_mall` | 1.00 | 0 | false |
| `nbeatsx_silver_v0` | `client_002_lviv_office` | 0.75 | 0 | true |
| `nbeatsx_silver_v0` | `client_003_dnipro_factory` | 1.00 | 0 | false |
| `nbeatsx_silver_v0` | `client_004_kharkiv_hospital` | 1.00 | -500 | true |
| `nbeatsx_silver_v0` | `client_005_odesa_hotel` | 0.75 | 500 | false |
| `tft_silver_v0` | `client_001_kyiv_mall` | 1.00 | -500 | true |
| `tft_silver_v0` | `client_002_lviv_office` | 1.00 | -500 | true |
| `tft_silver_v0` | `client_003_dnipro_factory` | 1.00 | 0 | true |
| `tft_silver_v0` | `client_004_kharkiv_hospital` | 1.00 | 500 | false |
| `tft_silver_v0` | `client_005_odesa_hotel` | 1.00 | 500 | false |

## Claim Boundary

This artifact supports:

> The repo can construct and evaluate a decision-targeted offline DFL v3
> candidate under the same no-leakage, all-tenant, strict LP/oracle protocol as
> the frozen Level 1 benchmark.

It does not support:

- full DFL success;
- Decision Transformer deployment;
- live trading or market execution;
- replacement of `strict_similar_day`;
- European dataset ingestion.

## Next Work

The next DFL slice should move beyond affine forecast correction. Good targets
are action-aligned labels from strict LP dispatch, high-spread hour
classification, or pairwise/ranking losses that directly target charge and
discharge ordering. The promotion gate should remain unchanged until a candidate
beats `strict_similar_day` on the strict LP/oracle final holdout.
