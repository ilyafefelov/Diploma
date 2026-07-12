# Random-Forest V2+ Safe-Switch Canonical Aggregate

`dt_v2_plus` is a deprecated historical artifact identifier. The estimator is
`RandomForestRegressor`, not Decision Transformer.

Source local artifacts:

- `runs/dt_v2_plus/aggregate.json`
- `runs/dt_v2_plus/canonical_seed_metrics_manifest.json`

## Result

| Field | Value |
| --- | ---: |
| Historical artifact id | `dt_v2_plus` |
| Estimator | `RandomForestRegressor` (500 trees, depth 6, leaf minimum 1) |
| Seeds | `3` |
| Baseline V2+ mean regret | `174.77` UAH |
| Mean test regret | `168.1566` UAH |
| Pass level | `secondary` |
| Primary threshold | `166.0` UAH |
| Secondary threshold | `178.26` UAH |
| Promotion gate passed | `false` |
| Market execution enabled | `false` |

## Trust Check

The manifest vector parse summary reports `720` non-empty rows and maximum
vector length `24` for each real vector column:

- `actual_price_uah_mwh_vector`
- `dispatch_mw_vector`
- `forecast_price_uah_mwh_vector`
- `soc_fraction_vector`

Interpretation: the teacher rows are exact timestamp-shifted copies of the
evaluation packet, and all four nonfallback profile-row changes occur on one
delivery date. The numerical result is an in-packet pipeline diagnostic, not an
out-of-sample estimate or transformer result. The three nominal seeds produce
the same path, so the stored zero standard deviation and p-value are not
inferential evidence. It does not support promotion or a dashboard default.

