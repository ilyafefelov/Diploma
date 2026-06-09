# DFL Forecast Decision Loss v1

Date: 2026-05-10

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

Latest run: `1fc1cc96-92b9-470c-b29a-f416a3ee3b08`.

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

The relaxed storage layer is now stabilized for this DFL v1 run:

- UAH price coefficients are scaled before the differentiable LP solve;
- bounded differentiable surrogate dispatch exists for cvxpylayer failures;
- strict-vs-relaxed fixture checks prevent treating the relaxed primitive as
  the final strict LP evaluator;
- non-finite optimizer gradients are guarded before checkpoint scoring.

Final panel status was
`cvxpylayer_scaled;training_guard:non_finite_gradient;cvxpylayer_scaled` for
all 10 tenant/source rows. Checkpoint epoch reached `4`, and there was no
catastrophic fallback score. The selected correction still produced `0.00 UAH`
mean relaxed-regret improvement versus the raw compact source forecasts.

| Model | Rows | Mean regret | Median regret | Finding |
|---|---:|---:|---:|---|
| `strict_similar_day` | 180 reference rows | 314.81 UAH | 202.61 UAH | Frozen control still wins. |
| `nbeatsx_silver_v0` | 90 | 1,121.04 UAH | 555.43 UAH | Raw compact source. |
| `dfl_forecast_dfl_v1_nbeatsx_silver_v0` | 90 | 1,121.04 UAH | 555.43 UAH | Stable run; no strict improvement over raw. |
| `tft_silver_v0` | 90 | 1,665.41 UAH | 1,399.49 UAH | Raw compact source. |
| `dfl_forecast_dfl_v1_tft_silver_v0` | 90 | 1,665.41 UAH | 1,399.49 UAH | Stable run; no strict improvement over raw. |

## Interpretation

This is useful negative evidence. The AFL feature contract is now cleaner and
the tiny DFL path now executes end to end with bounded relaxed-layer behavior.
That fixes the stability blocker, but not the modeling result: the current
horizon-bias correction does not improve relaxed final-holdout regret or strict
LP/oracle regret. The strict LP/oracle evaluator correctly blocks promotion.

The next DFL work should improve the learning target rather than add a larger
policy immediately:

- reduce reliance on horizon-only bias and train on richer schedule/value
  features;
- test smaller learning rates or closed-form/grid correction candidates against
  the same guarded relaxed layer;
- keep final promotion under strict LP/oracle scoring against
  `strict_similar_day`;
- use Decision Transformer only later as an offline research primitive.

Decision Transformer remains a later offline research primitive, not the next
implementation step.
