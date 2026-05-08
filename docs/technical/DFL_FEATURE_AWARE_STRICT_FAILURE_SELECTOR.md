# DFL Feature-Aware Strict-Failure Selector

Date: 2026-05-08

This slice turns the strict-failure feature audit into a prior-only selector
experiment. It does not train a neural model and does not change the existing
`dfl_strict_failure_selector_v1_*` decisions. The goal is to test whether price
regime, rank stability, spread volatility, and load/weather context can make the
strict-failure switch less blunt.

Claim boundary: research evidence only, not full DFL, not Decision Transformer
control, and not market execution.

## Assets

| Asset / check | Purpose |
|---|---|
| `dfl_feature_aware_strict_failure_selector_frame` | Selects a deterministic rule per tenant/source model from earlier rolling windows only. |
| `dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame` | Scores strict, raw, best-prior non-strict, and feature-aware selector rows on the latest final-holdout window. |
| `dfl_feature_aware_strict_failure_selector_evidence` | Dagster asset check for final-holdout coverage, provenance, claim flags, and strict/raw/selector row alignment. |

Config:
[real_data_dfl_feature_aware_strict_failure_selector_week3.yaml](../../configs/real_data_dfl_feature_aware_strict_failure_selector_week3.yaml).

## Rule Family

The selector grid is dependency-free and intentionally small:

- prior regret advantage threshold: `0`, `50`, `100`, `200`, `400` UAH;
- top/bottom rank-overlap floor: `0.0`, `0.5`, `0.75`;
- price regime policy: all regimes, low/medium spread only, or high spread only;
- spread-volatility policy: all regimes or non-volatile regimes only.

For the latest final-holdout window, the selected rule either keeps
`strict_similar_day` or switches to the best prior non-strict schedule family.
The rule is selected from windows `2`, `3`, and `4`; window `1` actuals affect
strict scoring only.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select tenant_consumption_schedule_bronze,tenant_historical_net_load_silver,dfl_schedule_candidate_library_v2_frame,dfl_strict_failure_selector_robustness_frame,dfl_strict_failure_prior_feature_panel_frame,dfl_feature_aware_strict_failure_selector_frame,dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame -c configs/real_data_dfl_feature_aware_strict_failure_selector_week3.yaml
```

## Expected Interpretation

This selector can pass a development diagnostic if it improves over raw neural
schedules. It is still blocked for production unless it beats the frozen
`strict_similar_day` control by at least 5% mean regret without worsening median
regret under the same strict LP/oracle protocol.

If it loses, that is still useful: the audit features explain why the latest
strict-failure breakthrough did not generalize.

## Latest Materialized Evidence

Run:

- Dagster run id: `1cb76f8c-e321-4178-b54a-f85cd15838b6`.
- Materialized assets: `dfl_feature_aware_strict_failure_selector_frame` and
  `dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame`.
- Asset check: `dfl_feature_aware_strict_failure_selector_evidence` passed.
- Selector rows: 10 = five tenants x two source models.
- Strict benchmark rows: 720.
- Final-holdout range: `2026-04-12 23:00` through `2026-04-29 23:00`.
- Coverage: 90 selector tenant-anchors per source model.

Strict LP/oracle result:

| Source model | Strict mean | Raw mean | Feature-aware selector mean | Feature-aware selector median | Improvement vs raw | Improvement vs strict |
|---|---:|---:|---:|---:|---:|---:|
| `nbeatsx_silver_v0` | 314.81 | 813.40 | 299.73 | 182.76 | 63.15% | 4.79% |
| `tft_silver_v0` | 314.81 | 1003.54 | 299.19 | 160.52 | 70.19% | 4.96% |

Finding:

- The feature-aware selector preserved strong development evidence versus raw
  neural schedules.
- It did not clear the conservative 5% strict-control threshold for either
  source model.
- TFT improved versus the v1 robustness failure in earlier-window logic, but
  the latest-window strict improvement is now just below the production
  threshold.
- Production promotion remains blocked.
