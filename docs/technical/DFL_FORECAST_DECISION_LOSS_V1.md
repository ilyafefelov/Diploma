# DFL Forecast Decision Loss v1

Date: 2026-05-09

This slice is the first real DFL-shaped forecast correction path after AFL
contract hardening. It is deliberately small: a prior-only horizon-bias
correction is trained on top of compact NBEATSx/TFT forecasts with a relaxed
decision loss, then scored by the same strict LP/oracle gate as every other
candidate.

Claim boundary: this is DFL-readiness research evidence only. It is not full
DFL, not Decision Transformer control, not a promoted controller, and not market
execution. `strict_similar_day` remains the frozen Level 1 control comparator.

## Assets

| Asset | Purpose |
|---|---|
| `dfl_forecast_dfl_v1_panel_frame` | Trains/checkpoints one tiny horizon-bias correction per tenant/source model using prior/train-selection anchors only. |
| `dfl_forecast_dfl_v1_strict_lp_benchmark_frame` | Strict LP/oracle score of `strict_similar_day`, raw source forecasts, and DFL v1 corrected forecasts on the latest final holdout. |

Config:
[real_data_dfl_forecast_v1_week3.yaml](../../configs/real_data_dfl_forecast_v1_week3.yaml).

Materialization:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_forecast_dfl_v1_panel_frame,dfl_forecast_dfl_v1_strict_lp_benchmark_frame -c configs/real_data_dfl_forecast_v1_week3.yaml
```

Latest run: `5562b5f0-9f12-44de-a74c-0cb47c7d447a`.

## Loss Contract

`smart_arbitrage.dfl.decision_loss.compute_decision_loss_v1` combines:

- relaxed realized regret against an oracle relaxed value;
- AFL-style spread-shape error;
- AFL-style rank-shape error;
- a small MAE stabilizer;
- throughput/degradation regularization.

The loss trains only correction parameters from prior anchors. Final-holdout
actuals affect strict scoring only.

## Materialized Result

Panel evidence:

| Metric | Value |
|---|---:|
| Panel rows | 10 |
| Tenants | 5 |
| Source models | 2 |
| Final-holdout anchors per tenant/source | 18 |
| Strict benchmark rows | 540 |
| Selector/candidate final-holdout rows per source | 90 |
| `not_full_dfl` / `not_market_execution` | true / true |

The real-data relaxed scorer still falls back on SCS solver errors:
`fallback:score:SolverError;fallback:training_epoch:SolverError`.
Therefore all checkpoint epochs stayed at `0`, the selected correction bias was
effectively the zero correction, and strict LP scoring made DFL v1 identical to
the raw compact source forecasts.

| Model | Rows | Mean regret | Median regret | Finding |
|---|---:|---:|---:|---|
| `strict_similar_day` | 180 reference rows | 314.81 UAH | 202.61 UAH | Frozen control still wins. |
| `nbeatsx_silver_v0` | 90 | 1,121.04 UAH | 555.43 UAH | Raw compact source. |
| `dfl_forecast_dfl_v1_nbeatsx_silver_v0` | 90 | 1,121.04 UAH | 555.43 UAH | No learned improvement; zero-bias fallback. |
| `tft_silver_v0` | 90 | 1,665.41 UAH | 1,399.49 UAH | Raw compact source. |
| `dfl_forecast_dfl_v1_tft_silver_v0` | 90 | 1,665.41 UAH | 1,399.49 UAH | No learned improvement; zero-bias fallback. |

## Interpretation

This is useful negative evidence. The AFL feature contract is now cleaner and
the tiny DFL path executes end to end, but the differentiable relaxed storage
layer is not stable enough on real all-tenant batches to produce a usable DFL
forecast correction. The strict LP/oracle evaluator correctly blocks the result.

The next DFL work should harden the relaxed optimization layer before trying a
larger neural policy:

- scale/normalize price inputs for the relaxed solver;
- add bounded fallback/surrogate relaxed dispatch that remains differentiable;
- compare relaxed objective values against strict LP values on a small fixture;
- rerun DFL v1 only after relaxed solver stability is proven.

Decision Transformer remains a later offline research primitive, not the next
implementation step.
