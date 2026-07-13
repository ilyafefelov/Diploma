# v1.3 aligned DFL source-gate result

Status: **not eligible for training**. This is a source-readiness result, not a
negative model result and not a change to the preregistered aligned DFL protocol.

## Materialization

Dagster run `cee056c5-4e4a-421e-8f50-a1ce6ae75cfe` completed with status
`SUCCESS` on 2026-07-13. It materialized the tracked 365-day configuration
`configs/real_data_official_global_panel_tft_quantile_schedule_value_365_week3.yaml`:

| Artifact | SHA-256 | Observed contents |
| --- | --- | --- |
| `official_global_panel_training_frame` | `216c3e709e0b2f3183c910b02e5b9924cb068f9b238d79a4e13e73a1dd35f85f` | 58,190 hourly rows, 5 tenants, 2025-01-01 through 2026-04-30 |
| `tft_official_global_panel_rolling_strict_lp_benchmark_frame` | `0ac44f06e238f745c50ad01410baf399024017e4c21c5606f86fa29fd201f115` | 7,300 rolling strict-LP evaluation rows |
| `tft_official_global_panel_horizon_quantile_calibration_frame` | `a0f4021ba751d7c2722c06c9878b3859b9f31d4b9b4051ba5a1e6df9a3ac3c7a` | 5,475 horizon-calibration rows |

## Contract audit

The base panel contains the source-backed Ukrainian inputs required for a
contextual model: `y`, `lag_24_price_uah_mwh`, `weather_temperature`,
`hour_sin`, and `hour_cos`. It does **not** contain the preregistered
`poland_lag24_uah_mwh` feature or p10/p50/p90 point-in-time feature vectors.

Its explicit governance fields are:

```text
external_feature_training_status = blocked_by_governance
allowed_external_feature_columns_csv = ""
blocked_external_feature_columns_csv =
  entsoe_pl_lag24_day_ahead_price_uah_mwh,...
external_training_blockers_csv =
  licensing,timezone,currency,market_rules,temporal_availability,domain_shift
```

The rolling TFT artifact does preserve forecast values inside its evaluation
payload. That does not repair the missing governed Poland context or turn those
evaluation payloads into the preregistered joined, point-in-time feature panel.

## Decision

Do not train the aligned DFL transformer on this materialization. In particular,
do not impute Poland values, substitute an outcome-derived feature, or remove
the feature family after seeing the result. Those actions would violate
[`v1_3_aligned_dfl_preregistration.md`](v1_3_aligned_dfl_preregistration.md).

The next permitted step is source closure: resolve the listed external-source
governance conditions and materialize a versioned joined panel with availability
timestamps for all eight preregistered vectors. Until then, there is no new
aligned-DFL score and no basis to start the temporal Decision Transformer lane.

All artifacts remain offline evidence; `market_execution_enabled=false`.
