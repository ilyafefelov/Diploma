# Candidate-Value DFL v3

## Status

This slice implements the next DFL branch after the pairwise schedule-family
DFL v2 result. It does not start another Decision Transformer. It expands the
official global-panel schedule library and trains/selects a candidate-level
value scorer over feasible LP-scored schedules.

Materialized status: `2026-05-18` Dagster run
`2dcdb48d-70b0-44f5-99b8-b8b5d4d58057` passed the label-panel, strict
benchmark, and failure-audit evidence checks. Candidate-Value DFL v3 used a
learned candidate-level value scorer, but the conservative fallback still
matched frozen V2+ on the latest strict holdout. The gate therefore remained
`diagnostic_pass_replacement_blocked`.

Frozen comparator:

- Ukrainian-only Schedule/Value Learner V2+;
- calibrated V2+ mean regret: `174.77` UAH;
- rolling robustness: `4 / 4`;
- `market_execution_enabled=false`.

## Why This Slice Exists

The V2+-teacher residual DFL / offline DT bridge failed versus V2+, and the
pairwise DFL v2 selector matched/fell back to V2+ with `0.00%` improvement. The
failure audit points to candidate-family collapse and weak trajectory objectives
rather than a need for a larger action-imitation transformer.

Candidate-Value DFL v3 attacks the remaining regret directly:

```text
V2+ candidate library
  -> expanded failure-mode candidate schedules
  -> prior/train candidate-level value scorer
  -> V2+ fallback unless prior evidence predicts improvement
  -> strict LP/oracle scoring against V2+
```

## Candidate Library V3

The new additive asset is:

- `dfl_official_global_panel_schedule_candidate_library_v3_frame`.

It starts from `dfl_official_global_panel_schedule_candidate_library_v2_plus_frame`
and adds deterministic schedule families around observed failure modes:

- `strict_neighborhood_v3`;
- `soc_terminal_target_v3`;
- `peak_trough_timing_shift_v3`;
- `uncertainty_risk_schedule_v3`;
- `degradation_price_sweep_v3`;
- `prior_best_family_template_v3`;
- `prior_oracle_residual_template_v3`;
- `oracle_neighborhood_diagnostic_v3`.

The oracle-neighborhood family is train-only diagnostic evidence. It uses
realized prices only on train/prior anchors and is never available in final
holdout selection.

The two prior-template families are the first V3 change that creates genuinely
new candidate schedules from previous anchors instead of only perturbing the
current forecast. `prior_best_family_template_v3` averages the historical
forecast-vector delta between raw forecasts and the best prior feasible
schedule. `prior_oracle_residual_template_v3` averages historical residuals
between raw forecasts and realized prices on train-selection anchors. Both
templates are applied to the current raw forecast and clipped to the DAM price
range before strict LP/oracle scoring. Final-holdout actual prices are not used
for template generation; they remain scoring labels only.

The tracked first-run config keeps the final holdout complete, but bounds
additional V3 schedule generation to the latest `10` train anchors per
tenant/source. The full V2+ library is still carried forward as the default
fallback/history; the bound only limits expensive extra LP-scored V3 variants
so the evidence run remains resumable on the local stack.

## Candidate-Value Label Panel

The new label-panel asset is:

- `dfl_official_global_panel_candidate_value_label_panel_v3_frame`.

It creates one row per candidate schedule and separates columns by contract:

- `selector_feature_*` columns are prior-safe schedule/context features such as
  prior family regret, forecast spread, forecast objective value, throughput,
  degradation penalty, SOC slack, and candidate-library version;
- `label_*` columns are realized scoring labels such as regret, decision value,
  oracle value, best-candidate flag, margin to anchor best, and margin versus
  strict control.

This panel is the correct input shape for the next objective redesign. It gives
a model stronger value labels and richer candidate schedules before any new
Decision Transformer attempt.

## Candidate-Value Scorer

The new selection asset is:

- `dfl_official_global_panel_candidate_value_dfl_v3_frame`.

The scorer chooses among full candidate schedules, not only one static family.
The current materialized scorer is `learned_linear_candidate_value_v3`: a small
ridge-style linear value model trained on `train_selection` rows from
`dfl_official_global_panel_candidate_value_label_panel_v3_frame`. The learned
target is realized candidate regret on prior anchors; final-holdout labels are
never used for weight fitting.

The learned features are:

- prior family mean regret;
- forecast spread;
- forecast objective value;
- total throughput;
- degradation penalty;
- SOC minimum slack;
- candidate-family intercepts.

The selection contract is:

1. fit the candidate-value scorer on train/prior label rows only;
2. score train and final candidate schedules using prior-safe `selector_feature_*`
   columns;
3. compute a regret-weighted pairwise ranking diagnostic on train/prior rows;
4. fall back to frozen V2+ unless train/prior mean regret improves enough.

Final-holdout actuals may affect only scoring labels, never the selected value
profile, learned feature weights, candidate generation parameters, or fallback
decision.

## Strict Gate

The strict benchmark asset is:

- `dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame`.

The asset check is:

- `dfl_official_global_panel_candidate_value_dfl_v3_evidence`.

The analysis-only failure-audit asset/check are:

- `dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_frame`;
- `dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_evidence`.

The tracked config is:

- `configs/real_data_dfl_candidate_value_dfl_v3_week3.yaml`.

V3 can replace V2+ only if it:

- improves mean regret versus V2+;
- does not worsen median regret versus V2+;
- still beats `strict_similar_day` by at least `5%`;
- keeps thesis-grade observed Ukrainian rows;
- has zero safety violations;
- keeps `market_execution_enabled=false`.

Materialized latest-holdout result:

| Source row | V3 mean regret | V2+ mean regret | Strict mean regret | Raw neural mean regret | Improvement vs V2+ | Gate |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| raw official global-panel NBEATSx | `193.36` UAH | `193.36` UAH | `310.58` UAH | `771.26` UAH | `0.00%` | blocked |
| horizon-calibrated official global-panel NBEATSx | `174.77` UAH | `174.77` UAH | `310.58` UAH | `622.25` UAH | `0.00%` | blocked |

Interpretation: the candidate-level value scorer improves strongly over raw
neural schedules, but only by selecting/falling back to the same schedules as
V2+. Therefore V2+ remains the thesis headline Offline Strategy Promotion
evidence.

The learned scorer was trained, but all 10 tenant/source model rows fell back
to V2+ under the configured `1%` prior/train improvement requirement. This is
the correct safety behavior: several pre-fallback candidate selections looked
better on the final holdout, but their train/prior evidence was weaker than
V2+.

## Failure Audit

The V3 failure audit explains why the new prior-template schedules did not beat
V2+ often enough. On the final holdout, both prior-template families were much
worse than V2+ on mean regret and won only a small minority of anchors:

| Source row | Candidate family | Mean regret | V2+ mean regret | Delta vs V2+ | Win rate vs V2+ | Diagnosis |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| calibrated NBEATSx | `prior_best_family_template_v3` | `605.71` UAH | `174.77` UAH | `+430.94` UAH | `4.44%` | `template_not_competitive_vs_v2_plus` |
| calibrated NBEATSx | `prior_oracle_residual_template_v3` | `627.08` UAH | `174.77` UAH | `+452.31` UAH | `5.56%` | `template_not_competitive_vs_v2_plus` |
| raw NBEATSx | `prior_best_family_template_v3` | `689.66` UAH | `193.36` UAH | `+496.30` UAH | `13.33%` | `template_not_competitive_vs_v2_plus` |
| raw NBEATSx | `prior_oracle_residual_template_v3` | `729.69` UAH | `193.36` UAH | `+536.33` UAH | `7.78%` | `template_not_competitive_vs_v2_plus` |

The failure mode is therefore not lack of candidate count alone. The simple
historical templates transfer average prior residuals/deltas into regimes where
V2+ already has a stronger strict/forecast blend. They occasionally help an
anchor, especially for raw NBEATSx, but not often enough to become a robust
replacement family.

A scratch, non-persisted zero-threshold fallback probe showed that the learned
scorer could improve the raw NBEATSx source from `193.36` to `185.62` UAH mean
regret by allowing one weak-prior switch. This is not promoted because it still
does not beat the calibrated V2+ headline (`174.77` UAH) and the train/prior
signal is too small for the conservative thesis gate.

## Run

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_official_global_panel_schedule_candidate_library_v3_frame,dfl_official_global_panel_candidate_value_label_panel_v3_frame,dfl_official_global_panel_candidate_value_dfl_v3_frame,dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame,dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_frame -c configs/real_data_dfl_candidate_value_dfl_v3_week3.yaml
```

Claim boundary:

- no live market execution;
- no dashboard/API default switch;
- no EU-feature training;
- `strict_similar_day` remains fallback/control;
- V2+ remains headline evidence unless V3 beats it under the unchanged gate.
