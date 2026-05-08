# DFL Strict-Failure Selector Robustness Gate

Date: 2026-05-08

This slice stress-tests the first source-specific strict-gate breakthrough. The
latest final holdout showed that
`dfl_strict_failure_selector_v1_tft_silver_v0` beat the frozen
`strict_similar_day` control under strict LP/oracle regret. That is promising,
but it is not enough for a thesis or engineering claim by itself.

The robustness gate replays the same prior-only selector over earlier temporal
windows and tenant slices. This remains research evidence only: not full DFL,
not Decision Transformer control, and not market execution.

## Assets

| Asset / check | Purpose |
|---|---|
| `dfl_strict_failure_selector_robustness_frame` | Replays the strict-failure selector over rolling validation windows from `dfl_schedule_candidate_library_v2_frame`. |
| `dfl_strict_failure_selector_robustness_evidence` | Dagster asset check for coverage, claim flags, and rolling-window evidence shape. |

Config:
[real_data_dfl_strict_failure_selector_robustness_week3.yaml](../../configs/real_data_dfl_strict_failure_selector_robustness_week3.yaml).

## Design

Default panel:

- five canonical Ukrainian tenants;
- source models: `tft_silver_v0`, `nbeatsx_silver_v0`;
- current verified ceiling: 104 eligible anchors per tenant/model;
- four latest-first rolling validation windows;
- 18 anchors per validation window;
- at least 30 prior anchors before each validation window.

The windows are non-overlapping and latest-first:

| Window index | Anchor index range | Prior anchor count |
|---:|---|---:|
| 1 | 86-103 | 86 |
| 2 | 68-85 | 68 |
| 3 | 50-67 | 50 |
| 4 | 32-49 | 32 |

For each window, the switch threshold is selected using anchors strictly before
the validation-window start. Validation actuals can change the score, but they
cannot change threshold selection, candidate metadata, or prior means.

## Gate Labels

| Label | Meaning |
|---|---|
| `development_pass` | Selector improves over the raw neural schedule in the window. |
| `source_specific_strict_pass` | Source-specific selector beats `strict_similar_day` by at least 5% mean regret and does not worsen median regret in the window. |
| `robust_research_challenger` | Same source passes in the latest window and at least three of four rolling windows. |
| `production_promote` | Always blocked in this slice. |

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_schedule_candidate_library_v2_frame,dfl_strict_failure_selector_robustness_frame -c configs/real_data_dfl_strict_failure_selector_robustness_week3.yaml
```

If upstream assets are stale, materialize the full strict-challenger chain first:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,offline_dfl_panel_experiment_frame,offline_dfl_decision_target_panel_frame,offline_dfl_action_target_panel_frame,dfl_trajectory_value_candidate_panel_frame,dfl_schedule_candidate_library_frame,dfl_pipeline_integrity_audit_frame,dfl_schedule_candidate_library_v2_frame,dfl_strict_failure_selector_robustness_frame -c configs/real_data_dfl_strict_failure_selector_robustness_week3.yaml
```

## Latest Materialized Evidence

Run:

- Dagster run id: `fd21fada-f453-404b-96a1-27d99b14b1a1`.
- Materialized assets: `dfl_schedule_candidate_library_v2_frame` and
  `dfl_strict_failure_selector_robustness_frame`.
- Asset check: `dfl_strict_failure_selector_robustness_evidence` passed.
- Robustness frame: 8 rows, two source models x four rolling windows.
- Coverage: 90 validation tenant-anchors per source model per window.
- Claim flags: `not_full_dfl=true`, `not_market_execution=true`.

Rolling-window result:

| Source model | Window | Validation range | Strict mean | Raw mean | Selector mean | Strict median | Selector median | Improvement vs raw | Improvement vs strict | Gate label |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---|
| `nbeatsx_silver_v0` | 1 | `2026-04-12 23:00` to `2026-04-29 23:00` | 314.81 | 813.40 | 299.73 | 202.61 | 182.76 | 63.15% | 4.79% | `development_pass` |
| `nbeatsx_silver_v0` | 2 | `2026-03-17 23:00` to `2026-04-11 23:00` | 844.04 | 996.25 | 837.74 | 484.05 | 473.73 | 15.91% | 0.75% | `development_pass` |
| `nbeatsx_silver_v0` | 3 | `2026-02-27 23:00` to `2026-03-16 23:00` | 663.52 | 1275.60 | 663.52 | 511.34 | 511.34 | 47.98% | 0.00% | `development_pass` |
| `nbeatsx_silver_v0` | 4 | `2026-02-09 23:00` to `2026-02-26 23:00` | 1019.41 | 1547.59 | 1019.41 | 732.85 | 732.85 | 34.13% | 0.00% | `development_pass` |
| `tft_silver_v0` | 1 | `2026-04-12 23:00` to `2026-04-29 23:00` | 314.81 | 1003.54 | 267.79 | 202.61 | 149.01 | 73.32% | 14.94% | `source_specific_strict_pass` |
| `tft_silver_v0` | 2 | `2026-03-17 23:00` to `2026-04-11 23:00` | 844.04 | 1083.64 | 856.91 | 484.05 | 500.72 | 20.92% | -1.53% | `development_pass` |
| `tft_silver_v0` | 3 | `2026-02-27 23:00` to `2026-03-16 23:00` | 663.52 | 1385.12 | 663.52 | 511.34 | 511.34 | 52.10% | 0.00% | `development_pass` |
| `tft_silver_v0` | 4 | `2026-02-09 23:00` to `2026-02-26 23:00` | 1019.41 | 1981.48 | 1190.82 | 732.85 | 821.92 | 39.90% | -16.81% | `development_pass` |

Interpretation:

- Development evidence is stable: every rolling window improves materially over
  the raw neural schedule.
- The strict-control breakthrough is not robust yet. TFT passes the latest
  window only, not three of four windows.
- No source receives the `robust_research_challenger` label in this run.
- Production promotion remains blocked.

## Feature-Audit Follow-Up

Follow-up run:

- Dagster run id: `b9a48061-079f-4a92-9daf-699398f67906`.
- New assets: `tenant_historical_net_load_silver`,
  `dfl_strict_failure_prior_feature_panel_frame`, and
  `dfl_strict_failure_feature_audit_frame`.
- Asset check: `dfl_strict_failure_feature_audit_evidence` passed.
- Historical load proxy: 14,395 configured rows across five tenants from
  `2026-01-01 00:00:00+00:00` to `2026-04-30 23:00:00+00:00`.
- Prior-feature panel: 720 rows.
- Feature audit: 40 rows.

The audit confirms the robustness finding. The latest window has real
strict-failure pockets, but earlier windows are mostly strict-stable or
high-spread-volatility regimes. Average selected-regret improvement over raw
neural schedules remains strong, but average improvement versus
`strict_similar_day` is not robust:

| Source model | Mean selected regret | Mean strict regret | Mean raw regret | Mean improvement vs strict | Mean improvement vs raw |
|---|---:|---:|---:|---:|---:|
| `nbeatsx_silver_v0` | 705.10 | 710.45 | 1158.21 | 1.54% | 40.02% |
| `tft_silver_v0` | 744.76 | 710.45 | 1363.44 | -1.60% | 43.07% |

Tracked note:
[DFL_STRICT_FAILURE_FEATURE_AUDIT.md](DFL_STRICT_FAILURE_FEATURE_AUDIT.md).
