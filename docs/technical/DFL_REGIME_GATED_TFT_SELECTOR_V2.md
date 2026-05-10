# DFL Regime-Gated TFT Selector V2

Date: 2026-05-10

This slice tests a stricter prior-only switch rule before any production
promotion claim. The rule defaults to the frozen `strict_similar_day` control
and allows a TFT challenger only inside audited strict-failure regimes with
enough prior-window evidence.

Claim boundary: offline/read-model strategy evidence only. This is not full DFL,
not Decision Transformer control, and not market execution.

## Assets

| Artifact | Purpose |
|---|---|
| `smart_arbitrage.dfl.coverage_repair` | Reports exact OREE/Open-Meteo gaps and whether they can be repaired from current observed feature rows. |
| `dfl_ua_coverage_repair_audit_frame` | One row per tenant/missing timestamp with gap kind, repair status, target anchor count, and claim flags. |
| `smart_arbitrage.dfl.regime_gated_tft_selector` | Prior-only regime selector and strict LP/oracle scoring helper. |
| `dfl_regime_gated_tft_selector_v2_frame` | One selected rule per source/window/regime. |
| `dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame` | Strict LP/oracle evidence rows for strict, raw, best prior non-strict reference, and the v2 selector. |
| `dfl_regime_gated_tft_selector_v2_evidence` | Dagster asset check for coverage, thesis-grade provenance, safety flags, and strict-control improvement. |
| [real_data_dfl_regime_gated_tft_selector_v2_week3.yaml](../../configs/real_data_dfl_regime_gated_tft_selector_v2_week3.yaml) | Tracked Week 3 run config. |

## Coverage Repair Result

The 180-anchor target is not currently recoverable from the observed feature
frame.

| Field | Observed value |
|---|---:|
| Tenants | 5 |
| Eligible anchors per tenant | 104 |
| Target anchors per tenant | 180 |
| Missing timestamp | `2026-03-29 23:00` |
| Gap kind | `price_and_weather_gap` |
| Repair status | `not_recoverable_from_current_feature_frame` |
| Missing price hours per tenant | 1 |
| Missing weather hours per tenant | 1 |
| Price/weather observed coverage ratio | `0.999653` |
| Data quality tier | `coverage_gap` |

This locks the current panel as a 104-anchor research ceiling until a new
source-backed OREE/Open-Meteo backfill is added. The gap is not filled with
synthetic data.

## Selector Rule

V2 is intentionally stricter than the earlier source-specific signal:

- `nbeatsx_silver_v0` is independently blocked with `source_not_tft`.
- `strict_stable_region` always defaults to `strict_similar_day`.
- TFT can switch only in `strict_failure_captured`, `high_spread_volatility`,
  `rank_instability`, `load_weather_stress`, or `tenant_specific_outlier`.
- A switch also requires enough prior windows, at least 5% prior mean-regret
  improvement versus strict, and no prior median degradation.
- Validation/final actuals affect scoring only, not rule selection.

The implementation normalizes UTC-aware and naive UTC timestamps before anchor
matching. That prevents a no-row evidence failure when the feature panel carries
timezone-aware anchors and the schedule library carries naive UTC anchors.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,tenant_consumption_schedule_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,dfl_data_coverage_audit_frame,dfl_ua_coverage_repair_audit_frame,offline_dfl_panel_experiment_frame,offline_dfl_panel_strict_lp_benchmark_frame,offline_dfl_decision_target_panel_frame,offline_dfl_decision_target_strict_lp_benchmark_frame,offline_dfl_action_target_panel_frame,offline_dfl_action_target_strict_lp_benchmark_frame,dfl_trajectory_value_candidate_panel_frame,dfl_schedule_candidate_library_frame,dfl_schedule_candidate_library_v2_frame,dfl_strict_failure_selector_robustness_frame,tenant_historical_net_load_silver,dfl_strict_failure_prior_feature_panel_frame,dfl_strict_failure_feature_audit_frame,dfl_regime_gated_tft_selector_v2_frame,dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame -c configs/real_data_dfl_regime_gated_tft_selector_v2_week3.yaml
```

Latest run:

| Field | Value |
|---|---|
| Dagster run id | `1b901874-b713-4762-9154-2e822f91be8d` |
| Run status | `SUCCESS` |
| V2 selector rows | 11 |
| V2 strict benchmark rows | 2,880 |
| Asset check | Did not pass, as expected for promotion evidence |
| Check reason | Both sources have 0.0% selector improvement versus strict because v2 defaulted to `strict_similar_day`. |

## Strict LP/Oracle Result

| Source | Role | Rows | Mean regret UAH | Median regret UAH |
|---|---:|---:|---:|---:|
| `nbeatsx_silver_v0` | strict reference | 360 | 710.445 | 442.848 |
| `nbeatsx_silver_v0` | raw reference | 360 | 1,525.526 | 1,089.276 |
| `nbeatsx_silver_v0` | best prior non-strict reference | 360 | 696.899 | 393.715 |
| `nbeatsx_silver_v0` | v2 selector | 360 | 710.445 | 442.848 |
| `tft_silver_v0` | strict reference | 360 | 710.445 | 442.848 |
| `tft_silver_v0` | raw reference | 360 | 1,800.934 | 1,460.884 |
| `tft_silver_v0` | best prior non-strict reference | 360 | 736.743 | 402.329 |
| `tft_silver_v0` | v2 selector | 360 | 710.445 | 442.848 |

Interpretation:

- V2 correctly avoids switching in `strict_stable_region`.
- TFT `high_spread_volatility` evidence exists but has only 54 tenant-anchor
  validation rows in the current panel and is blocked by prior-regime
  undercoverage.
- The best prior non-strict references remain useful diagnostic evidence, but
  they are not allowed to override strict without robust prior-window evidence.
- Production promotion remains blocked.

## Decision

No source/regime is promoted. The safe offline default remains
`strict_similar_day`.

The next route is either source-backed Ukrainian backfill beyond 104 anchors or
a better prior-only regime rule that can pass at least 3 of 4 rolling
strict-control windows without using validation actuals for selection.
