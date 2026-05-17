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

## Candidate-Value DFL v3 Slice

The next branch is implemented as Candidate-Value DFL v3:

1. Expand the official global-panel V2+ library with failure-mode schedule
   families: strict-neighborhood, SOC terminal-target, peak/trough timing,
   uncertainty/risk, degradation-price sweeps, and train-only
   oracle-neighborhood diagnostics.
2. Train/select a candidate-level value scorer on train/prior anchors using a
   regret-weighted pairwise/listwise ranking objective.
3. Fall back to frozen V2+ unless prior evidence predicts a non-degrading
   improvement.
4. Strict-score against V2+, raw official NBEATSx, and `strict_similar_day`.

Tracked config:

- `configs/real_data_dfl_candidate_value_dfl_v3_week3.yaml`.

Materialized result:

- Dagster run `0263a956-12e0-4b93-86b8-b10d2194317b`;
- evidence check passed;
- gate decision: `diagnostic_pass_replacement_blocked`;
- calibrated Candidate-Value DFL v3 mean regret: `174.77` UAH;
- calibrated V2+ mean regret: `174.77` UAH;
- raw Candidate-Value DFL v3 mean regret: `193.36` UAH;
- raw V2+ mean regret: `193.36` UAH;
- improvement versus V2+: `0.00%`.

Interpretation: candidate-level scoring did not find a schedule/value
improvement beyond V2+. The next redesign should either broaden the
candidate-library search space in a targeted way or introduce a truly learned
value model with stronger point-in-time features; another tiny DT over the same
objective remains deprioritized.

Technical runbook:

- [DFL_CANDIDATE_VALUE_DFL_V3.md](DFL_CANDIDATE_VALUE_DFL_V3.md).
