# DT/V2+ Canonical Aggregate

Source local artifacts:

- `runs/dt_v2_plus/aggregate.json`
- `runs/dt_v2_plus/canonical_seed_metrics_manifest.json`

## Result

| Field | Value |
| --- | ---: |
| Model | `dt_v2_plus` |
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

Interpretation: the corrected DT/V2+ safe-switch signal is real enough for
secondary research evidence, but not primary promotion or dashboard default.

