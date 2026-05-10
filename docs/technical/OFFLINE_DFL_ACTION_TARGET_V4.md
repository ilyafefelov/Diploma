# Offline DFL Action-Target v4

Date: 2026-05-07

This artifact tests the next strict-gate follow-up after v3: an
action-targeted forecast correction that emphasizes raw forecast charge and
discharge ranks. It is selected only on prior strict LP/oracle regret. It does
not add neural training, does not change API/dashboard contracts, and does not
replace `strict_similar_day` as the frozen Level 1 control comparator.

## Scope

| Field | Value |
|---|---|
| Parameter-selection asset | `offline_dfl_action_target_panel_frame` |
| Strict benchmark asset | `offline_dfl_action_target_strict_lp_benchmark_frame` |
| Dagster group | `gold_dfl_training` |
| Strategy kind | `offline_dfl_action_target_strict_lp_benchmark` |
| Inputs | `real_data_rolling_origin_benchmark_frame`, `offline_dfl_panel_experiment_frame`, `offline_dfl_decision_target_panel_frame` |
| Run config | [../../configs/real_data_offline_dfl_action_target_week3.yaml](../../configs/real_data_offline_dfl_action_target_week3.yaml) |
| Tenants | all five canonical tenants |
| Final holdout | latest 18 anchors per tenant, 90 tenant-anchor rows per source model |
| Claim scope | `offline_dfl_action_target_v4_strict_lp_gate_not_full_dfl` |

The v4 correction rule starts from raw forecasts, optional panel v2 bias, and
optional v3 correction, then applies a charge/discharge rank emphasis:

```text
base = raw
base += optional_panel_v2_horizon_bias
base = optional_decision_target_v3_correction(base)

corrected[lowest raw-ranked charge hours] -= action_spread_uah_mwh / 2
corrected[highest raw-ranked discharge hours] += action_spread_uah_mwh / 2
```

The grid is intentionally small and audit-friendly:

| Parameter | Values |
|---|---|
| `charge_hour_count` | `2`, `3` |
| `discharge_hour_count` | `2`, `3` |
| `action_spread_uah_mwh` | `500`, `1000`, `1500` |
| `include_panel_v2_bias` | `false`, `true` |
| `include_decision_v3_correction` | `false`, `true` |

Parameters are selected only on prior/inner-validation anchors. Final-holdout
actuals are used only for final strict scoring.

## Materialization

Services were rebuilt so Dagster and API containers used the committed source:

```powershell
docker compose config --quiet
docker compose up -d --build postgres mlflow dagster-webserver dagster-daemon api
docker compose ps
```

Then the full all-tenant v4 path materialized successfully:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,offline_dfl_panel_experiment_frame,offline_dfl_decision_target_panel_frame,offline_dfl_action_target_panel_frame,offline_dfl_action_target_strict_lp_benchmark_frame -c configs/real_data_offline_dfl_action_target_week3.yaml
```

| Field | Value |
|---|---|
| Dagster run id | `54f1e320-b046-4aab-9d07-ff9c73714622` |
| Strict rows persisted | 900 |
| Tenant count | 5 |
| Source models | `tft_silver_v0`, `nbeatsx_silver_v0` |
| V4 validation tenant-anchors | 180 total, 90 per source model |
| Local summary | `data/research_runs/week3_offline_dfl_action_target_v4_90/action_target_v4_summary.json` |

API sanity check after the run:

| Endpoint | Tenant | Tier | Anchors | Models | Rows |
|---|---|---|---:|---:|---:|
| `/dashboard/real-data-benchmark` | `client_003_dnipro_factory` | `thesis_grade` | 90 | 3 | 270 |
| `/dashboard/calibrated-ensemble-benchmark` | `client_003_dnipro_factory` | `thesis_grade` | 90 | 1 | 90 |
| `/dashboard/risk-adjusted-value-gate` | `client_003_dnipro_factory` | `thesis_grade` | 90 | 1 | 90 |
| `/dashboard/forecast-dispatch-sensitivity` | `client_003_dnipro_factory` | diagnostic | 90 | 5 | 450 |

## Strict Gate Result

The strict promotion gate blocks production promotion. V4 improves the TFT
candidate versus raw TFT, panel v2, and decision-target v3, but it still loses
badly to the frozen `strict_similar_day` control. NBEATSx v4 regresses versus
raw, panel v2, and v3.

| Source model | Candidate | Tenant-anchors | Mean regret UAH | Median regret UAH |
|---|---|---:|---:|---:|
| `nbeatsx_silver_v0` | `strict_similar_day` | 90 | 314.81 | 202.61 |
| `nbeatsx_silver_v0` | `nbeatsx_silver_v0` | 90 | 813.40 | 520.48 |
| `nbeatsx_silver_v0` | `offline_dfl_panel_v2_nbeatsx_silver_v0` | 90 | 816.62 | 490.54 |
| `nbeatsx_silver_v0` | `offline_dfl_decision_target_v3_nbeatsx_silver_v0` | 90 | 814.17 | 461.35 |
| `nbeatsx_silver_v0` | `offline_dfl_action_target_v4_nbeatsx_silver_v0` | 90 | 851.99 | 530.35 |
| `tft_silver_v0` | `strict_similar_day` | 90 | 314.81 | 202.61 |
| `tft_silver_v0` | `tft_silver_v0` | 90 | 1003.54 | 477.99 |
| `tft_silver_v0` | `offline_dfl_panel_v2_tft_silver_v0` | 90 | 989.55 | 435.16 |
| `tft_silver_v0` | `offline_dfl_decision_target_v3_tft_silver_v0` | 90 | 1015.36 | 477.99 |
| `tft_silver_v0` | `offline_dfl_action_target_v4_tft_silver_v0` | 90 | 959.84 | 479.04 |

Gate interpretation:

| Source model | V4 improvement vs raw | V4 improvement vs panel v2 | V4 improvement vs v3 | V4 improvement vs `strict_similar_day` | Median condition | Decision |
|---|---:|---:|---:|---:|---|---|
| `nbeatsx_silver_v0` | -4.74% | -4.33% | -4.65% | -170.64% | worse than strict | blocked |
| `tft_silver_v0` | 4.35% | 3.00% | 5.47% | -204.89% | worse than strict | blocked |

## Selected Parameter Patterns

V4 selected tenant/source-specific patterns. The summary below counts final
holdout rows using each selected pattern.

| Source model | Pattern | Rows |
|---|---|---:|
| `nbeatsx_silver_v0` | `charge=2`, `discharge=2`, `spread=1500`, `panel=false`, `v3=true` | 18 |
| `nbeatsx_silver_v0` | `charge=2`, `discharge=3`, `spread=1500`, `panel=false`, `v3=true` | 18 |
| `nbeatsx_silver_v0` | `charge=2`, `discharge=3`, `spread=500`, `panel=false`, `v3=true` | 18 |
| `nbeatsx_silver_v0` | `charge=3`, `discharge=3`, `spread=500`, `panel=false`, `v3=false` | 36 |
| `tft_silver_v0` | `charge=2`, `discharge=2`, `spread=1500`, `panel=false`, `v3=true` | 36 |
| `tft_silver_v0` | `charge=2`, `discharge=3`, `spread=1500`, `panel=false`, `v3=false` | 18 |
| `tft_silver_v0` | `charge=3`, `discharge=2`, `spread=1500`, `panel=false`, `v3=true` | 18 |
| `tft_silver_v0` | `charge=3`, `discharge=3`, `spread=1500`, `panel=true`, `v3=true` | 18 |

## Claim Boundary

This artifact supports:

> The repo can construct and evaluate an action-targeted offline DFL v4
> candidate under the same no-leakage, all-tenant, strict LP/oracle protocol as
> the frozen Level 1 benchmark.

It does not support:

- full DFL success;
- Decision Transformer deployment;
- live trading or market execution;
- replacement of `strict_similar_day`;
- European dataset ingestion.

## Next Work

The next DFL slice should stop treating forecast correction as the main lever.
The evidence now points toward direct action imitation or pairwise ranking:
extract oracle/strict-control charge-discharge hour labels from LP solutions,
train or calibrate on prior-anchor action agreement, and continue using the
same strict promotion gate. A candidate should not be promoted until it beats
`strict_similar_day` on mean regret by at least 5% and does not worsen median
regret on the all-tenant final holdout.
