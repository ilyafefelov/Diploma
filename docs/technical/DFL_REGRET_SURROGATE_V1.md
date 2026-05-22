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

Materialized V1 outcome from Dagster run
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

The follow-up context V2 path adds that teacher-label repair directly:

- `dfl_regret_surrogate_safe_switch_context_audit_frame` groups rare
  switch opportunities by prior context: candidate source/family, weekend,
  grid-event context, high V2+ regret, high forecast spread, and material
  schedule distance from V2+;
- `dfl_regret_surrogate_teacher_label_panel_v2_frame` adds context-support
  labels and prior-only support statistics without using final-holdout labels
  as selector features;
- `dfl_regret_surrogate_contextual_candidate_value_v2_frame` selects a
  challenger only if the same context profile has prior support, prior safe
  wins, low tail-risk probability, and expected improvement;
- `dfl_regret_surrogate_contextual_strict_lp_benchmark_frame` and
  `dfl_regret_surrogate_contextual_rolling_robustness_frame` keep the same
  strict LP/oracle evaluator and V2+ fallback.

This path is expected to be conservative. If the final safe-switch contexts
have no prior support, the correct result is a documented data/context
backfill blocker, not a forced switch.

Materialized context V2 outcome from Dagster run
`25c97839-d6b4-4eff-a5a3-843d88440a2b`:

- context audit rows: `1,825`;
- material safe-switch threshold: at least `25` UAH better than V2+;
- latest final holdout material opportunities: `5 / 90` tenant-anchor rows;
- all `5 / 90` final material opportunities were
  `context_without_prior_support`;
- contextual selector selected `0 / 90` non-V2+ final rows and fell back to
  V2+ on all latest-holdout rows;
- latest-holdout strict result matched V2+: `174.77` UAH mean regret and
  `67.30` UAH median regret;
- rolling robustness remained negative: `0 / 4` robust windows and `0 / 4`
  diagnostic windows; in older rolling windows the contextual rule attempted
  some switches and worsened mean regret by roughly `1.34%` to `2.37%`;
- promotion status: `not_promoted`, with `market_execution_enabled=false`.

This closes the immediate "why not use the 8/90 opportunities?" question. The
non-material audit counted any positive oracle switch (`8 / 90`), while context
V2 counts only material switches (`5 / 90`). Those five material rows all occur
in contexts not represented by prior train anchors, so using them would require
final-holdout hindsight. The model must either get more Ukrainian/context
history with comparable events or create safer candidate families that win in
contexts already covered by prior anchors.

Sparse Safe-Switch V6 is the next additive repair. It replaces exact
context-profile equality with nearest-prior support over prior-only
`selector_feature_*` columns. The model still abstains to V2+ unless a candidate
has nearby prior anchors, prior safe wins, predicted improvement, and low
tail-risk probability. This is intended to answer whether the sparse `5 / 90`
material opportunities are learnable without final-holdout leakage.

Materialized Sparse Safe-Switch V6 outcome:

- feature-contract audit passed;
- latest final holdout had `0 / 90` selector-safe non-reference material
  opportunities after excluding train-only oracle diagnostics and keeping
  `strict_similar_day` as a reference/control row;
- the strict control still had `4` material local wins, which explains why
  broader diagnostic audits can show money left on the table, but those rows
  are not learned challenger schedules;
- V6 selected `0 / 90` non-V2+ final rows and fell back to V2+ everywhere;
- latest-holdout strict result matched V2+: `174.77` UAH mean regret and
  `67.30` UAH median regret;
- rolling robustness remained negative: `0 / 4` promotion windows and `0 / 4`
  diagnostic windows;
- promotion status: `not_promoted`, with `market_execution_enabled=false`.

Interpretation: V6 closed the exact-profile-support question and sharpened the
blocker. The issue is now candidate/data scarcity, not a missing classifier.
Further DT/LAVA work over the same candidate universe would mostly learn to
abstain. The next useful ML branch is Ukrainian point-in-time context backfill
or new feasible candidate generation with prior-supported wins.

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
| `dfl_regret_surrogate_safe_switch_context_audit_frame` | Explains whether rare material safe-switch rows have prior support in comparable contexts. |
| `dfl_regret_surrogate_teacher_label_panel_v2_frame` | Adds context-support labels and prior-only support statistics for the next safe-switch learner. |
| `dfl_regret_surrogate_contextual_candidate_value_v2_frame` | Selects candidates only from prior-supported safe-switch contexts. |
| `dfl_regret_surrogate_contextual_strict_lp_benchmark_frame` | Scores contextual V2 against strict and frozen V2+ under the same evaluator. |
| `dfl_regret_surrogate_contextual_rolling_robustness_frame` | Replays contextual V2 over rolling prior-only windows. |
| `dfl_sparse_safe_switch_feature_contract_audit_frame` | Checks that V6 selector inputs do not include final regret labels, oracle gaps, or post-anchor actuals. |
| `dfl_sparse_safe_switch_candidate_library_v6_frame` | Builds the sparse-opportunity candidate library with V2+ fallback, shadow candidates, and train-only oracle diagnostics. |
| `dfl_sparse_safe_switch_opportunity_audit_frame` | Classifies material opportunities by nearest-prior support instead of exact context-profile equality. |
| `dfl_sparse_safe_switch_teacher_label_panel_v6_frame` | Publishes V6 teacher labels while keeping realized outcomes as `label_*` / `diagnostic_*`. |
| `dfl_sparse_safe_switch_abstention_model_v6_frame` | Selects a non-V2+ candidate only with nearest-prior support and low tail-risk. |
| `dfl_sparse_safe_switch_strict_lp_benchmark_frame` | Scores V6, frozen V2+, and strict under the unchanged LP/oracle evaluator. |
| `dfl_sparse_safe_switch_rolling_robustness_frame` | Replays V6 under prior-only rolling windows. |

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
  --select dfl_v2_plus_learning_limit_audit_frame,dfl_expanded_schedule_value_teacher_label_panel_v1_frame,dfl_regret_surrogate_forecast_correction_v1_frame,dfl_regret_surrogate_candidate_value_v1_frame,dfl_regret_surrogate_strict_lp_benchmark_frame,dfl_regret_surrogate_rolling_robustness_frame,dfl_regret_surrogate_safe_switch_context_audit_frame,dfl_regret_surrogate_teacher_label_panel_v2_frame,dfl_regret_surrogate_contextual_candidate_value_v2_frame,dfl_regret_surrogate_contextual_strict_lp_benchmark_frame,dfl_regret_surrogate_contextual_rolling_robustness_frame `
  -c configs/real_data_dfl_regret_surrogate_v1_week3.yaml
```

Sparse V6 can be materialized after the context V2 teacher panel:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_sparse_safe_switch_feature_contract_audit_frame,dfl_sparse_safe_switch_candidate_library_v6_frame,dfl_sparse_safe_switch_opportunity_audit_frame,dfl_sparse_safe_switch_teacher_label_panel_v6_frame,dfl_sparse_safe_switch_abstention_model_v6_frame,dfl_sparse_safe_switch_strict_lp_benchmark_frame,dfl_sparse_safe_switch_rolling_robustness_frame `
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
