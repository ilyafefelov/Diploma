# Candidate-Value DFL v3

## Status

This slice implements the next DFL branch after the pairwise schedule-family
DFL v2 result. It does not start another Decision Transformer. It expands the
official global-panel schedule library and trains/selects a candidate-level
value scorer over feasible LP-scored schedules.

Materialized status: `2026-05-17` Dagster run
`0263a956-12e0-4b93-86b8-b10d2194317b` passed the evidence check but did not
replace V2+. Candidate-Value DFL v3 matched frozen V2+ on the latest strict
holdout, so the gate remained `diagnostic_pass_replacement_blocked`.

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
It selects a deterministic profile by train/prior objective:

- predicted value/regret score per candidate schedule;
- regret-weighted pairwise/listwise ranking loss on train/prior anchors;
- schedule features such as prior family regret, forecast spread, throughput,
  degradation penalty, SOC slack, and teacher-family value bonus;
- V2+ fallback unless prior mean regret improves over frozen V2+.

Final-holdout actuals may affect only scoring labels, never the selected value
profile, feature weights, candidate generation parameters, or fallback decision.

## Strict Gate

The strict benchmark asset is:

- `dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame`.

The asset check is:

- `dfl_official_global_panel_candidate_value_dfl_v3_evidence`.

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

| Source row | V3 mean regret | V2+ mean regret | Strict mean regret | Improvement vs V2+ | Gate |
| --- | ---: | ---: | ---: | ---: | --- |
| raw official global-panel NBEATSx | `193.36` UAH | `193.36` UAH | `310.58` UAH | `0.00%` | blocked |
| horizon-calibrated official global-panel NBEATSx | `174.77` UAH | `174.77` UAH | `310.58` UAH | `0.00%` | blocked |

Interpretation: the candidate-level value scorer improves strongly over raw
neural schedules, but only by selecting/falling back to the same schedules as
V2+. Therefore V2+ remains the thesis headline Offline Strategy Promotion
evidence.

## Run

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_official_global_panel_schedule_candidate_library_v3_frame,dfl_official_global_panel_candidate_value_dfl_v3_frame,dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame -c configs/real_data_dfl_candidate_value_dfl_v3_week3.yaml
```

To include the value-label panel for the next objective-design pass:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_official_global_panel_schedule_candidate_library_v3_frame,dfl_official_global_panel_candidate_value_label_panel_v3_frame,dfl_official_global_panel_candidate_value_dfl_v3_frame,dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame -c configs/real_data_dfl_candidate_value_dfl_v3_week3.yaml
```

Claim boundary:

- no live market execution;
- no dashboard/API default switch;
- no EU-feature training;
- `strict_similar_day` remains fallback/control;
- V2+ remains headline evidence unless V3 beats it under the unchanged gate.
