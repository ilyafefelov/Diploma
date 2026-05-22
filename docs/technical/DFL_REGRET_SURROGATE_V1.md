# DFL Regret-Surrogate V1

## Status

This slice adds and materializes a learning-limit audit plus an additive
Regret-Surrogate DFL v1 path after the UA-context LAVA/DT result. It does not
run another raw Decision Transformer variant. The question is narrower:

> Is there enough prior-only candidate/value signal to beat frozen calibrated
> V2+, or is the current candidate/data space already at its ceiling?

Frozen comparator remains:

- calibrated Ukrainian-only V2+ mean regret: `174.77` UAH;
- calibrated Ukrainian-only V2+ median regret: `67.30` UAH;
- rolling robustness: `4 / 4` windows;
- `market_execution_enabled=false`.

The path is offline/read-model evidence only: no live dispatch, no market
execution, no dashboard/API default switch, and no EU rows as Ukrainian target
rows.

Materialized outcome from Dagster run
`eec564f0-7437-4372-95fa-5c6e74745c18`:

- learning-limit audit rows: `1,825` tenant-anchor rows;
- latest final holdout: `90` tenant-anchor rows;
- final-holdout oracle-best upper bound: `161.38` UAH mean regret versus
  V2+ `174.77` UAH, or `7.66%` theoretical mean improvement;
- final-holdout better-candidate coverage: `8 / 90` rows;
- train/prior oracle-best upper bound: `210.47` UAH mean regret versus V2+
  `252.57` UAH, or `16.67%` theoretical mean improvement;
- Regret-Surrogate V1 selected `0 / 90` non-V2+ final rows and fell back to
  V2+ on all final rows;
- strict benchmark result: Regret-Surrogate V1 matched V2+ at `174.77` UAH
  mean regret and `67.30` UAH median regret;
- rolling robustness: `0 / 4` challenger windows and `0 / 4` diagnostic
  windows, because every rolling validation window also fell back to V2+;
- promotion status: `not_promoted`, with `market_execution_enabled=false`.

Interpretation: the candidate universe has enough oracle-switch upside to
justify better teacher/value-label work, but the current prior-only surrogate
does not learn a safe switch. The bottleneck is not "no possible better
schedule"; it is sparse and tail-risk dominated switch identification.

## Source Basis

The implementation follows the decision-focused direction from:

- ESS arbitrage should optimize downstream regret/value, not only forecast
  error: <https://arxiv.org/abs/2305.00362>.
- Perturbed/implicit DFL can connect storage optimization and learning:
  <https://arxiv.org/abs/2406.17085>.
- Predict-then-bid DFL frames the market bid as the downstream decision:
  <https://arxiv.org/abs/2505.01551>.
- TFT and NBEATSx remain forecast/feature sources, not promotion claims by
  themselves: <https://arxiv.org/abs/1912.09363> and
  <https://arxiv.org/abs/2104.05522>.

## Assets

| Asset | Purpose |
|---|---|
| `dfl_v2_plus_learning_limit_audit_frame` | Compares V2+, oracle-best available candidate, Poland/TFT shadow candidates, UA-context candidates, and LAVA candidates. It reports whether the current candidate universe has at least 5% theoretical upside versus V2+. |
| `dfl_expanded_schedule_value_teacher_label_panel_v1_frame` | Publishes prior-safe `selector_feature_*` inputs and realized `label_*` / `diagnostic_*` outcomes for candidate-value learning. Final-holdout rows are scoring-only. |
| `dfl_regret_surrogate_forecast_correction_v1_frame` | Fits a conservative prior-only regret-delta/tail-risk scorer by tenant and source model. It records runtime metadata and falls back to V2+ when no safe profile exists. |
| `dfl_regret_surrogate_candidate_value_v1_frame` | Resolves final candidate choices from the scorer and V2+ fallback keys. |
| `dfl_regret_surrogate_strict_lp_benchmark_frame` | Scores strict, V2+, and Regret-Surrogate rows under the unchanged strict LP/oracle evaluator. |
| `dfl_regret_surrogate_rolling_robustness_frame` | Replays prior-only validation windows and checks whether the challenger can become robust evidence. |

Tracked config:

`configs/real_data_dfl_regret_surrogate_v1_week3.yaml`

## Leakage Boundary

- `selector_feature_*` columns are prior inputs only.
- `label_*` and `diagnostic_*` columns hold realized regret/value outcomes.
- Final-holdout actuals can change scoring and diagnostics, but not feature
  columns, selected feature names, fitted prior profiles, or selected weights.
- No raw hourly BUY/SELL/HOLD imitation target is emitted.
- Poland/TFT can appear only as shadow/context candidate sources in this path.
  European rows never become Ukrainian target rows.

## Gate

Promotion requires all of the following:

- mean regret beats V2+ by at least `5%`;
- median regret is not worse than `67.30` UAH;
- rolling robustness passes `4 / 4` windows;
- zero safety violations;
- `market_execution_enabled=false`.

Diagnostic success is weaker and does not replace V2+: any positive mean
improvement without median harm and at least `3 / 4` diagnostic rolling windows
would justify a stronger DT/LAVA follow-up.

If the learning-limit audit says the oracle-best candidate universe cannot
beat V2+ by at least 5%, the next branch should be data acquisition/backfill or
new candidate generation, not another model over the same rows.

## Run

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_v2_plus_learning_limit_audit_frame,dfl_expanded_schedule_value_teacher_label_panel_v1_frame,dfl_regret_surrogate_forecast_correction_v1_frame,dfl_regret_surrogate_candidate_value_v1_frame,dfl_regret_surrogate_strict_lp_benchmark_frame,dfl_regret_surrogate_rolling_robustness_frame `
  -c configs/real_data_dfl_regret_surrogate_v1_week3.yaml
```

If upstream UA-context oracle-gap assets are not available in the active
Dagster IO store, materialize the UA-context safe-switch path first.

## Thesis Interpretation

Regret-Surrogate DFL v1 is the materialized evidence step after UA-context
LAVA/DT fell back to V2+. It shows that candidate-level regret labels are not
yet safely learnable from the current prior feature set. A pass would have
become a new offline challenger. The current fail is still useful: it tells the
thesis that the current feature/candidate universe has sparse upside and strong
tail risk, so DT/LAVA needs better teacher labels or better context/candidates
rather than a larger sequence model over the same supervision.
