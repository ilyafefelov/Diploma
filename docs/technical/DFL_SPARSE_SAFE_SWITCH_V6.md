# DFL Sparse Safe-Switch V6

## Status

This slice implements and materializes an additive sparse-opportunity
safe-switch path after Regret-Surrogate context V2. It does not start another
DT variant. The goal is to test whether the rare rows where a better schedule
exists can be selected with prior-neighbor support instead of exact
context-profile equality.

Frozen comparator remains:

- calibrated Ukrainian-only V2+ mean regret: `174.77` UAH;
- calibrated Ukrainian-only V2+ median regret: `67.30` UAH;
- rolling robustness: `4 / 4` windows;
- `market_execution_enabled=false`.

The path is offline/read-model evidence only. It does not enable live dispatch,
does not switch dashboard/API defaults, and does not turn EU or Poland rows into
Ukrainian target rows. Poland/TFT can appear only as shadow/context candidate
sources.

Materialized V6 outcome:

- feature-contract audit passed: no selector input used realized final regret,
  oracle gap, candidate regret delta, or post-anchor actuals;
- V6 candidate library rows: `32,780`;
- opportunity audit rows: `1,825`;
- latest final holdout: `90` tenant-anchor rows;
- selector-safe final opportunities: `0 / 90` non-reference candidates were
  both eligible and materially better than V2+;
- `strict_similar_day` control had a few local wins, including `4` material
  wins by the `25` UAH threshold, but it is kept as a reference/control row
  rather than treated as a learned policy challenger;
- V6 selected `0 / 90` non-V2+ final rows and fell back to V2+ on all final
  rows;
- latest-holdout strict result matched V2+: `174.77` UAH mean regret and
  `67.30` UAH median regret, with zero safety violations;
- rolling robustness: `0 / 4` promotion windows and `0 / 4` diagnostic
  windows, because V6 abstained to V2+ in every rolling window;
- promotion status: `not_promoted`, with `market_execution_enabled=false`.

Relevant Dagster run ids:

- strict benchmark: `89ae4c50-e5b2-4487-ad71-0f3fc266d12e`;
- rolling robustness: `e1b09d75-382d-4881-b119-a12c0ba3a4d0`.

## Why V6 Exists

The previous context V2 result found only `5 / 90` latest-holdout material
safe-switch opportunities, and all five lacked exact prior context support.
That count came from the broader diagnostic label space. V6 applies a stricter
selector-safe candidate contract: oracle-neighborhood rows are train-only, and
reference/control rows are not treated as learned challengers. Under that
stricter contract, the latest final holdout has no eligible non-reference
material candidate to switch into. V6 therefore asks a narrower question:

> Does a final candidate have nearby prior anchors with similar prior-only
> features, safe wins, and low tail-risk history?

If the answer is no, V6 abstains and returns frozen V2+. That abstention is a
valid diagnostic result, because using final-holdout outcomes to justify the
switch would be leakage.

## Asset Path

| Asset | Purpose |
|---|---|
| `dfl_sparse_safe_switch_feature_contract_audit_frame` | Proves selector inputs do not include realized regret, oracle gap, candidate regret deltas, or post-anchor actuals. |
| `dfl_sparse_safe_switch_candidate_library_v6_frame` | Reuses V2+, strict fallback, Poland/TFT/UA/LAVA shadow candidates, and train-only oracle-neighborhood diagnostics. |
| `dfl_sparse_safe_switch_opportunity_audit_frame` | Classifies each tenant-anchor as `no_material_candidate`, `material_candidate_prior_supported`, `material_candidate_no_prior_neighbor`, or `tail_risk_dominated`. |
| `dfl_sparse_safe_switch_teacher_label_panel_v6_frame` | Publishes prior-safe `selector_feature_*` columns and realized `label_*` / `diagnostic_*` outcomes for V6. |
| `dfl_sparse_safe_switch_abstention_model_v6_frame` | Fits an abstaining selector that switches only with nearest-prior support, predicted improvement, and low tail-risk probability. |
| `dfl_sparse_safe_switch_strict_lp_benchmark_frame` | Strict-scores V6, frozen V2+, and `strict_similar_day` under the unchanged LP/oracle evaluator. |
| `dfl_sparse_safe_switch_rolling_robustness_frame` | Replays prior-only rolling windows and checks whether V6 is robust enough to promote. |

Tracked config:

`configs/real_data_dfl_regret_surrogate_v1_week3.yaml`

## Feature Contract

- Inputs must use the `selector_feature_*` prefix.
- Realized regret, oracle-best regret, candidate regret delta, and tail-risk
  labels must stay under `label_*` or `diagnostic_*`.
- Final-holdout actuals can change scores and diagnostics only. They must not
  change selected feature columns, nearest-neighbor support, thresholds, or
  selected candidates.
- Oracle-neighborhood diagnostics are train-only and are never selectable on
  final holdout.

## Gate

Promotion requires all of the following:

- mean regret beats V2+ by at least `5%`;
- median regret is not worse than `67.30` UAH;
- rolling robustness passes `4 / 4` windows;
- zero safety violations;
- `market_execution_enabled=false`.

Diagnostic success is weaker and does not replace V2+: any positive mean
improvement with no median degradation and at least `3 / 4` diagnostic rolling
windows would justify a stronger DT/LAVA target. If V6 still cannot find
prior-supported material opportunities, the next branch is Ukrainian
backfill/context acquisition or better candidate generation, not another model
over the same sparse rows.

## Run

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_sparse_safe_switch_feature_contract_audit_frame,dfl_sparse_safe_switch_candidate_library_v6_frame,dfl_sparse_safe_switch_opportunity_audit_frame,dfl_sparse_safe_switch_teacher_label_panel_v6_frame,dfl_sparse_safe_switch_abstention_model_v6_frame,dfl_sparse_safe_switch_strict_lp_benchmark_frame,dfl_sparse_safe_switch_rolling_robustness_frame `
  -c configs/real_data_dfl_regret_surrogate_v1_week3.yaml
```

If upstream Regret-Surrogate context V2 assets are missing in the active
Dagster IO store, materialize the learning-limit/context path first.

## Thesis Interpretation

V6 is a stricter, more honest version of the safe-switch idea. It tries to use
nearby prior evidence instead of exact profile matching, but it still refuses
to switch when comparable history or an eligible material candidate is missing.
The materialized result matched V2+. That is not a runtime failure; it is
evidence that the current selector-safe candidate universe has no robust
final-holdout switch to learn without leakage. The next model branch should not
be another selector over this same candidate set. It should either backfill
more Ukrainian point-in-time context or generate genuinely new feasible
candidate schedules with prior-supported wins.

That follow-up is implemented as
[DFL_OPPORTUNITY_BACKFILL_V7](DFL_OPPORTUNITY_BACKFILL_V7.md). V7 first
classifies the V2+ miss classes into `backfill_needed`,
`candidate_generation_needed`, `dt_ready`, or
`stop_modeling_current_candidate_space`, then adds feasible schedule variants
and a candidate-value gate. DT/LAVA remains a later branch and resumes only if
V7 produces enough prior-supported teacher labels.
