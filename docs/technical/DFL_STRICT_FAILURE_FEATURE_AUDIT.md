# DFL Strict-Failure Feature Audit

Date: 2026-05-08

This slice explains the strict-failure selector instead of changing it. The
prior-only selector remains unchanged; the new assets add load/weather/price
context and cluster the rolling-window outcomes so the next selector experiment
has a defensible feature map.

Claim boundary: research evidence only, not full DFL, not Decision Transformer
control, and not market execution.

## Assets

| Asset / check | Purpose |
|---|---|
| `tenant_historical_net_load_silver` | Configured tenant load/PV/net-load proxy aligned to benchmark timestamps. This is research-only configured context, not measured telemetry. |
| `dfl_strict_failure_prior_feature_panel_frame` | One row per tenant/source/window/validation anchor with prior-only selector features and validation outcomes as analysis labels. |
| `dfl_strict_failure_feature_audit_frame` | Deterministic cluster summary by tenant, source model, and rolling window. |
| `dfl_strict_failure_feature_audit_evidence` | Dagster asset check for rolling-window shape, claim flags, and audit coverage. |

Config:
[real_data_dfl_strict_failure_feature_audit_week3.yaml](../../configs/real_data_dfl_strict_failure_feature_audit_week3.yaml).

## Feature Separation

Selector-ready columns are prefixed with `selector_feature_`. They use only
anchors strictly before the validation-window start:

- prior strict/raw/non-strict regret summaries;
- prior price mean, spread, and spread volatility;
- prior top/bottom price-rank stability diagnostics;
- calendar buckets known at decision time;
- prior weather/load/load-proxy summaries.

Validation-window outcomes are prefixed with `analysis_only_`. They can explain
why the selector won or lost, but they are not selector inputs.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select tenant_consumption_schedule_bronze,tenant_historical_net_load_silver,dfl_strict_failure_selector_robustness_frame,dfl_strict_failure_prior_feature_panel_frame,dfl_strict_failure_feature_audit_frame -c configs/real_data_dfl_strict_failure_feature_audit_week3.yaml
```

Latest run:

- Dagster run id: `b9a48061-079f-4a92-9daf-699398f67906`.
- `dfl_strict_failure_selector_robustness_evidence`: passed.
- `dfl_strict_failure_feature_audit_evidence`: passed.
- Historical net-load rows: 14,395 across five tenants.
- Historical net-load range: `2026-01-01 00:00:00+00:00` to
  `2026-04-30 23:00:00+00:00`.
- Feature-panel rows: 720 = five tenants x two source models x four windows x
  18 validation anchors.
- Audit rows: 40 = five tenants x two source models x four windows.

## Audit Result

Cluster counts:

| Cluster | Rows | Meaning |
|---|---:|---|
| `strict_stable_region` | 30 | Selector usually ties strict control or only improves over raw neural schedules. |
| `high_spread_volatility` | 6 | Prior spread volatility is elevated and selector decisions need richer regime handling. |
| `strict_failure_captured` | 4 | Selector captures true strict-control failure pockets. |

Source-level audit:

| Source model | Mean selected regret | Mean strict regret | Mean raw regret | Mean improvement vs strict | Mean improvement vs raw |
|---|---:|---:|---:|---:|---:|
| `nbeatsx_silver_v0` | 705.10 | 710.45 | 1158.21 | 1.54% | 40.02% |
| `tft_silver_v0` | 744.76 | 710.45 | 1363.44 | -1.60% | 43.07% |

Window-level strict comparison:

| Source model | Window | Selected mean regret | Strict mean regret | Improvement vs strict |
|---|---:|---:|---:|---:|
| `nbeatsx_silver_v0` | 1 | 299.73 | 314.81 | 5.34% |
| `nbeatsx_silver_v0` | 2 | 837.74 | 844.04 | 0.84% |
| `nbeatsx_silver_v0` | 3 | 663.52 | 663.52 | 0.00% |
| `nbeatsx_silver_v0` | 4 | 1019.41 | 1019.41 | 0.00% |
| `tft_silver_v0` | 1 | 267.79 | 314.81 | 9.86% |
| `tft_silver_v0` | 2 | 856.91 | 844.04 | -1.17% |
| `tft_silver_v0` | 3 | 663.52 | 663.52 | 0.00% |
| `tft_silver_v0` | 4 | 1190.82 | 1019.41 | -15.10% |

## Interpretation

The selector is learning a useful raw-neural rescue behavior, not a robust
replacement for the frozen `strict_similar_day` control.

The latest window contains real strict-failure pockets for both source models,
with TFT strongest in that window. Earlier windows are mostly strict-stable or
spread-volatility regimes where the current switch rule is too blunt. The next
selector slice should use this panel to gate by price regime, rank stability,
and load/weather stress before switching away from strict control.

Production promotion remains blocked.

Follow-up: the feature-aware selector slice now consumes this panel in
`dfl_feature_aware_strict_failure_selector_frame`, selecting deterministic
price-regime/rank-stability/spread-volatility rules from earlier rolling
windows only. See
[DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR.md](DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR.md).
