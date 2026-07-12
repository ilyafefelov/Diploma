# DFL Objective Redesign Plan

## Purpose

This note defines the next DFL/DT branch after the official V2+-teacher bridge
failed to beat Schedule/Value Learner V2+. It does not change the current thesis
headline result. The active comparator remains Ukrainian-only V2+:

- calibrated V2+ mean regret: `174.77` UAH;
- improvement vs `strict_similar_day`: `43.73%`;
- rolling robustness: `4 / 4` windows;
- `market_execution_enabled=false`.

The current objective is to redesign the learning target around why V2+ wins,
not to run a larger Decision Transformer over the same weak trajectory contract.

## Evidence Base

The official V2+-teacher bridge was materialized in Dagster run
`53efba76-38cb-4624-9cd8-e15fb8c1c7a9`; its evidence check passed, but the gate
blocked headline replacement:

| Source model | V2+ mean regret, UAH | Residual/DT mean regret, UAH | Behavior cloning mean regret, UAH |
| --- | ---: | ---: | ---: |
| `nbeatsx_official_global_panel_horizon_calibrated_v1` | 174.77 | 367.70 | 580.39 |
| `nbeatsx_official_global_panel_v1` | 193.36 | 328.51 | 675.84 |

The follow-up failure audit was materialized in Dagster run
`5ccff4bd-4628-4595-bb82-f91cb9194180`; its evidence check passed. The audit
classified 720 challenger rows:

| Failure mode | Rows | Share | Mean delta vs V2+, UAH |
| --- | ---: | ---: | ---: |
| `candidate_family_collapse` | 351 | 48.75% | 249.58 |
| `dt_imitation_weaker_than_v2_selector` | 142 | 19.72% | 518.69 |
| `weak_trajectory_objective` | 135 | 18.75% | -30.25 |
| `bad_teacher_target` | 92 | 12.78% | 123.24 |

Interpretation: the residual/DT bridge mostly failed because it collapsed onto a
small set of schedule families and imitated trajectories instead of learning a
robust value ordering among feasible schedules. Some anchors also show that V2+
itself should not be copied blindly, because `strict_similar_day` is sometimes
already stronger or near-oracle.

## Academic Direction

The redesign follows the same academic boundary already used in the thesis:
decision-focused storage learning should optimize downstream value/regret under
intertemporal SOC constraints, not only forecast error or action imitation.
Relevant local source anchors are the literature-review discussion of DFL for
energy storage arbitrage, SPO/SPO+-style decision losses, multistage storage
DFL, and perturbed/implicit storage DFL. Those sources motivate value-sensitive
losses while the project still keeps strict LP/oracle scoring as the final
evaluator.

## Candidate DFL v2 Objectives

### 1. Pairwise Schedule-Value Ranking

Train a small prior-only scorer over full candidate schedules. For each
train/prior anchor, compare pairs of feasible candidate schedules and optimize a
logistic ranking loss weighted by strict LP/oracle value difference.

Required properties:

- train only on prior/train anchors;
- input only prior-known state and candidate schedule features;
- predict a score per candidate schedule, not live market commands;
- default to V2+ unless the learned scorer predicts a clear non-degrading
  improvement;
- final holdout actuals affect scoring only.

This directly targets the audit's largest failure mode: candidate-family
collapse.

### 2. Return-Conditioned Schedule-Family Selector

If a sequence model is used, the sequence token should be schedule family,
horizon block, or dispatch-shape class rather than raw hourly imitation. The
model conditions on target return and selects among feasible schedule families.
Behavior cloning remains a required baseline.

Required properties:

- train on high-value prior/train schedules only;
- compare against filtered behavior cloning;
- block any result that improves mean regret but worsens median regret;
- keep V2+ as fallback when confidence is low.

This keeps the Decision Transformer idea, but aligns it with schedule selection
instead of copying per-hour actions.

### 3. Candidate-Value DFL Loss

Use a differentiable or solver-free surrogate only to train a value scorer over
candidate schedules. The loss combines:

- regret-weighted value ranking;
- a small forecast-shape stabilizer where useful;
- throughput/degradation regularization;
- a fallback penalty when the scorer chooses a non-V2+ candidate and V2+ wins.

Strict LP/oracle scoring remains the promotion gate. The surrogate cannot become
the evaluator.

## Gate For Any DFL v2 Candidate

A new DFL v2 candidate can replace V2+ only when all of these hold:

- mean regret improves versus V2+;
- median regret does not worsen versus V2+;
- the candidate still beats `strict_similar_day` by at least `5%` mean regret;
- rolling robustness remains `4 / 4` windows;
- thesis-grade Ukrainian observed rows are used;
- no Poland/ENTSO-E feature enters training unless the governed route later
  passes;
- zero safety violations;
- `market_execution_enabled=false`.

## Implemented DFL v2 Slice

The code slice is implemented as `dfl_schedule_value_dfl_v2`:

1. Build train/prior pairwise schedule-family value scores from the official
   V2+ candidate library.
2. Select a schedule family only when prior anchors show non-degrading
   improvement versus frozen V2+.
3. Emit a trace showing selected candidate family, pairwise value scores, V2+
   fallback decision, and final scoring labels.
4. Benchmark against `strict_similar_day`, raw source rows, and official V2+
   under the unchanged strict LP/oracle gate.

The tracked config is
`configs/real_data_dfl_schedule_value_dfl_v2_week3.yaml`, and the technical run
contract is documented in [DFL_SCHEDULE_VALUE_DFL_V2.md](DFL_SCHEDULE_VALUE_DFL_V2.md).

Materialized result:

- Dagster run id: `9af65d45-6c7d-4aec-b71b-7fb31fd2147d`;
- evidence check passed;
- local packet:
  `data/research_runs/week3_dfl_schedule_value_dfl_v2_comparison/`;
- gate decision: `diagnostic_pass_replacement_blocked`;
- calibrated DFL v2 mean regret: `174.77` UAH;
- calibrated V2+ mean regret: `174.77` UAH;
- improvement versus V2+: `0.00%`.

The result is useful negative evidence. The redesigned pairwise objective did
not beat frozen V2+, so the final thesis headline stays with V2+ while the next
DFL branch should focus on richer candidate values/families rather than another
small DT over the same trajectory objective.

## Post-Defense Time-Separated DT Check

A later corrective experiment removes the mirrored-row limitation from the DT
comparison and trains Hugging Face `DecisionTransformerModel` with nonzero
candidate actions and regret-based return-to-go values. The 36-run matrix covers
two sources, three temporally separated rolling protocols, candidate-index
cross entropy and decision-aware regret/value ranking, and three seeds.

- zero runs improve on frozen V2+;
- decision-aware DT ties V2+ in 18/18 runs and makes no non-V2+ switch;
- cross-entropy DT ties in 15/18 and is harmful in 3/18;
- harmful mean-regret deltas are +10.12, +12.28, and +21.09 UAH;
- all train/evaluation model-input-plus-target overlap counts are zero;
- `promotable_v13_permitted_training_rows=0` and
  `market_execution_enabled=false` remain unchanged.

This supports the redesign thesis: changing to a transformer does not solve the
absence of a robust point-in-time safe-switch signal. Decision-aware abstention
is safer than candidate-label imitation, but a V2+ tie is not a model gain and
is not full differentiable DFL. See
[dt_temporal_v2_plus_experiment.md](final-evidence/dt_temporal_v2_plus_experiment.md).

## Candidate-Value DFL v3 Slice

The next branch is implemented as Candidate-Value DFL v3:

1. Expand the official global-panel V2+ library with failure-mode schedule
   families: strict-neighborhood, SOC terminal-target, peak/trough timing,
   uncertainty/risk, degradation-price sweeps, and train-only
   oracle-neighborhood diagnostics, plus prior-template families that transfer
   historical forecast deltas/residuals into new feasible schedules.
2. Build a candidate-value label panel with prior-safe `selector_feature_*`
   columns and realized `label_*` value/regret columns.
3. Train a learned candidate-level value scorer on train/prior anchors using a
   ridge-style schedule-value objective and regret-weighted ranking diagnostics.
4. Fall back to frozen V2+ unless prior evidence predicts a non-degrading
   improvement.
5. Strict-score against V2+, raw official NBEATSx, and `strict_similar_day`.

Tracked config:

- `configs/real_data_dfl_candidate_value_dfl_v3_week3.yaml`.

Materialized result:

- Dagster run `2dcdb48d-70b0-44f5-99b8-b8b5d4d58057`;
- label-panel, strict-benchmark, and failure-audit evidence checks passed;
- gate decision: `diagnostic_pass_replacement_blocked`;
- calibrated Candidate-Value DFL v3 mean regret: `174.77` UAH;
- calibrated V2+ mean regret: `174.77` UAH;
- raw Candidate-Value DFL v3 mean regret: `193.36` UAH;
- raw V2+ mean regret: `193.36` UAH;
- improvement versus V2+: `0.00%`.

Interpretation: the learned candidate-level scorer worked as an evidence path,
but the conservative gate correctly kept V2+ as fallback. The failure audit
showed that the new prior-template schedules were not competitive often enough:
their final-holdout mean regret ranged from `605.71` to `729.69` UAH, and their
win rates versus V2+ ranged from `4.44%` to `13.33%`. The next redesign should
therefore improve point-in-time features or candidate generation quality rather
than repeat the same average residual-template mechanism; another tiny DT over
the same objective remains deprioritized.

Technical runbook:

- [DFL_CANDIDATE_VALUE_DFL_V3.md](DFL_CANDIDATE_VALUE_DFL_V3.md).

## Plateau-Breaker / Candidate-Value DFL v4 Slice

The next implemented branch is V4, and it starts with diagnosis rather than a
larger model. V3 matched V2+ because the non-degradation fallback selected V2+
for every tenant/source row. V4 therefore adds:

1. a V2+/V3 plateau autopsy with three deterministic causes:
   `candidate_not_better`, `candidate_available_but_not_selected`, and
   `fallback_too_conservative`;
2. a data-quality/context audit over the 365-anchor Ukrainian panel;
3. stronger feasible candidate schedules: quantile/risk, block peak,
   terminal SOC reserve, spread-volatility robust, tenant degradation/throughput
   sweep, and train-only oracle-neighborhood diagnostic schedules;
4. a candidate-level value scorer with richer schedule/regime features.

The zero-threshold raw NBEATSx diagnostic remains non-promoted: it could reduce
raw mean regret from `193.36` to `185.62` UAH, but this still does not beat the
calibrated V2+ comparator at `174.77` UAH and did not satisfy prior evidence
requirements.

Tracked config:

- `configs/real_data_dfl_candidate_value_dfl_v4_week3.yaml`.

Technical runbook:

- [DFL_PLATEAU_BREAKER_V4.md](DFL_PLATEAU_BREAKER_V4.md).

Materialized result:

- Dagster run id: `0c57f795-3b5b-4106-ad9d-0776294a1eb4`;
- candidate library rows: `71,040`;
- V4 label-panel rows: `71,040`;
- strict benchmark rows: `720`;
- label-panel and strict-benchmark checks passed;
- calibrated V4 selected V2+ at `174.77` UAH mean regret;
- raw V4 selected V2+ at `193.36` UAH mean regret;
- improvement versus V2+: `0.00%`.

Interpretation: V4 did not weaken the gate, and the gate correctly kept V2+ as
the thesis headline. The autopsy now shows that calibrated NBEATSx is mostly
`candidate_not_better` (`71 / 90` final-holdout rows), while raw NBEATSx has a
larger `fallback_too_conservative` slice (`42 / 90` rows). The data audit still
flags weather/load, calendar/event, and publication-time gaps, so the next work
should be Ukrainian data/context improvement or a teacher-trajectory DT only
after the candidate/value layer has stronger labels.
