# DFL LAVA Tail-Risk Target

This slice uses the materialized LAVA schedule-neighbor bridge as diagnostic
data. The previous bridge was useful because it failed clearly: it selected a
small number of perturbation schedules with very large regret. The next target
therefore changes the DT/LAVA supervision problem.

Instead of:

```text
predict raw hourly BUY / SELL / HOLD
```

the target becomes:

```text
choose a feasible schedule candidate / family, or fall back to V2+
```

## Frozen Comparator

| Metric | Frozen Ukrainian-only calibrated V2+ |
|---|---:|
| Mean regret | `174.77` UAH |
| Median regret | `67.30` UAH |
| Rolling robustness | `4 / 4` windows |
| Market execution | `false` |

V2+ remains the thesis headline until a challenger beats it under the same
strict LP/oracle evaluator.

## New Assets

| Asset | Purpose |
|---|---|
| `dfl_lava_tail_risk_diagnostic_frame` | Classifies LAVA candidates as V2+ default, safe neighbor, weak neighbor, oracle-only diagnostic, or tail-risk perturbation loss. |
| `dfl_lava_tail_risk_aware_target_frame` | Builds candidate-index targets from prior diagnostics and blocks families with prior tail losses. |
| `dfl_lava_tail_risk_aware_strict_lp_benchmark_frame` | Strict-scores the redesigned target against `strict_similar_day` and frozen V2+. |
| `dfl_lava_tail_risk_safe_switch_scorer_frame` | Trains a prior-profile safe-switch scorer over approved challenger sources, while requiring family-level tail-risk safety. |
| `dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame` | Strict-scores the safe-switch scorer with per-anchor V2+ fallback. |
| `dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame` | Repairs the seven null-blocked Poland context columns into explicit prior-safe selector features and keeps repair counts visible. |
| `dfl_lava_tail_risk_safe_switch_scorer_v2_frame` | Trains a small tabular rich-context safe-switch scorer over schedule, risk, and Poland lag-24 features. |
| `dfl_lava_tail_risk_safe_switch_v2_strict_lp_benchmark_frame` | Strict-scores the richer safe-switch v2 selector against frozen V2+, `strict_similar_day`, and the same LP/oracle gate. |
| `dfl_lava_tail_risk_avoidance_label_frame` | Converts the v2 feature panel into explicit `safe_switch_win`, `tail_risk_switch`, and fallback labels for the next DT/LAVA target. |
| `dfl_lava_tail_risk_avoidance_scorer_v3_frame` | Trains separate prior-only regret-delta and tail-risk scorers, then switches only when predicted improvement is strong and predicted tail-risk probability is below the configured veto. |
| `dfl_lava_tail_risk_avoidance_v3_strict_lp_benchmark_frame` | Strict-scores the v3 tail-risk avoidance selector against frozen V2+ and `strict_similar_day`. |
| `dfl_lava_schedule_neighbor_dt_training_frame` | Converts the v3 label frame into DT/LAVA-ready teacher rows over feasible schedule-neighbor candidates. |
| `dfl_lava_schedule_neighbor_dt_policy_frame` | Trains a conservative schedule-neighbor policy from those teacher rows; the label space is candidate/family/block supervision, not hourly action imitation. |
| `dfl_lava_schedule_neighbor_dt_strict_lp_benchmark_frame` | Strict-scores the schedule-neighbor DT/LAVA policy and behavior-cloning reference against frozen V2+. |

Tracked config:

- [configs/real_data_dfl_lava_tail_risk_target_week3.yaml](../../configs/real_data_dfl_lava_tail_risk_target_week3.yaml)

Core implementation:

- `src/smart_arbitrage/dfl/lava_tail_risk_target.py`

## Target Semantics

The redesigned target is intentionally not a full Decision Transformer yet.
It creates a safer training target for the later DT/LAVA branch:

- label space: `schedule_candidate_index`;
- raw hourly action imitation: `false`;
- fallback: frozen Ukrainian-only V2+;
- blocked families: any candidate family with prior tail-risk losses above the
  configured threshold, plus explicitly hard-blocked perturbation families such
  as `rank_extrema_perturbation_v2_plus`;
- allowed families: families with prior safe wins and no prior tail-risk losses;
- final holdout: scoring only, never used to decide the blocked/allowed family
  list.

This is closer to the LAVA idea in the project notes: train around feasible
neighbors of an LP solution, but use precomputed schedule candidates and strict
scores rather than calling a solver inside training.

## No-Leakage Rules

- Final-holdout actuals may change strict scores and regret labels.
- Final-holdout actuals may not change target selection rules, blocked families,
  or candidate-index supervision.
- Oracle-neighborhood rows remain train-only diagnostics.
- Poland remains an exogenous feature route only; no European row becomes a
  Ukrainian target row.
- `market_execution_enabled=false` remains fixed.

## Gate

The target can replace V2+ only if it:

- beats frozen V2+ mean regret;
- does not worsen median regret against V2+;
- still beats `strict_similar_day` by at least `5%`;
- later preserves rolling robustness;
- keeps zero market-execution claims.

If it falls back to V2+, that is still valid diagnostic evidence: it means the
safe target layer removed the known tail-risk failure but did not yet discover
enough robust upside to replace the current V2+ baseline.

## First Materialized Result

Dagster runs:

- full tail-risk target path: `2da1174c-7ada-4ebf-83c6-b6d528971802`;
- final strict benchmark refresh after calibrated-fallback fix:
  `60f19630-3469-4d07-9576-14c62c356011`.

The diagnostic found a large number of both opportunity and risk rows:

| Diagnostic class | Rows |
|---|---:|
| `safe_neighbor_candidate` | `4,358` |
| `tail_risk_perturbation_loss` | `4,351` |
| `oracle_only_train_diagnostic` | `1,735` |
| `neutral_or_weak_neighbor` | `1,616` |
| `v2_plus_default` | `1,825` |

After hard-blocking the known perturbation family and requiring no prior tail
losses, every tenant fell back to calibrated V2+. The strict benchmark result
therefore matches the frozen comparator:

| Row | Tenant-anchor rows | Mean regret, UAH | Median regret, UAH | Status |
|---|---:|---:|---:|---|
| Tail-risk-aware target | `90` | `174.77` | `67.30` | safe fallback, not promoted |
| Frozen calibrated V2+ | `90` | `174.77` | `67.30` | headline comparator |
| Frozen raw V2+ | `90` | `193.36` | `68.89` | reference |
| `strict_similar_day` | `90` | `310.58` | `198.39` | control |

This is a useful closure result: the target redesign prevents the known
tail-risk overreach, but it does not yet produce a new challenger that beats
V2+. The next DT/LAVA step should train on the diagnostic class distribution
and learn a confidence model for rare safe-switch cases, while preserving V2+
as the default action.

## Safe-Switch Scorer Result

The next ML step implemented a conservative safe-switch scorer on top of the
same diagnostic data. The important correction is that the scorer may only
switch to approved challenger sources and, by default, requires the entire
candidate family to be free of prior tail-risk losses. A narrower profile-only
screen was too permissive, so the tracked config keeps the family-level veto.

Dagster run:

- safe-switch strict benchmark: `ac432cb6-93b6-476b-a914-baca350aa14e`.

Materialized result:

| Row | Tenant-anchor rows | Mean regret, UAH | Median regret, UAH | Status |
|---|---:|---:|---:|---|
| Safe-switch scorer | `90` | `174.77` | `67.30` | full V2+ fallback, not promoted |
| Frozen calibrated V2+ | `90` | `174.77` | `67.30` | headline comparator |
| Frozen raw V2+ | `90` | `193.36` | `68.89` | reference |
| `strict_similar_day` | `90` | `310.58` | `198.39` | control |

The scorer selected no allowed risk profiles in the current evidence packet:
each tenant used `18 / 18` V2+ fallback anchors. That is the correct conservative
outcome for this slice. It shows that the current Poland/TFT candidate space
contains local wins, but not a prior-safe switch rule that can beat V2+ without
tail-risk exposure.

## Safe-Switch v2: Rich Prior Context

Safe-Switch v2 keeps the same conservative claim boundary but fixes the next
diagnostic gap: the previous selector did not consume the richer Poland feature
representation. The new feature panel attaches prior-only schedule/risk features
and all lag-24 Poland context columns to every LAVA schedule candidate. If a
Poland column is missing or non-finite for a tenant-anchor, the value is repaired
to `0.0` and the row records `selector_feature_repaired_null_count` plus the
exact repaired column names. That makes the repair explicit instead of silently
dropping the signal in a scaler or model input.

The scorer is still deliberately small: a deterministic ridge-style tabular
model predicts candidate regret delta versus frozen V2+ from prior/train
anchors only. It may switch away from V2+ only when:

- the candidate source is an approved shadow challenger;
- the candidate family is not hard-blocked by prior tail-risk evidence;
- the candidate's risk profile has enough prior safe wins and no excess prior
  tail losses;
- the predicted regret delta is better than the configured minimum improvement.

This is not a deployed DT/LAVA controller. It is a teacher-label and
safe-switch diagnostic layer. If v2 still falls back to V2+, the interpretation
is not that Poland features are useless; it means the currently available
pre-anchor features still cannot identify the rare Poland/TFT wins with enough
safety to replace V2+ under the unchanged strict LP/oracle gate.

Materialized v2 result:

- Dagster run: `5de285c0-91b6-4cba-8916-e816658a7838`;
- feature panel rows: `13,885`;
- rows with explicit Poland-context repairs: `3,810`;
- repaired selector feature values: `133,350`;
- selected Poland shadow schedules: `21 / 90`;
- V2+ fallback schedules: `69 / 90`;
- Safe-Switch v2 mean regret: `219.37` UAH;
- Safe-Switch v2 median regret: `96.02` UAH;
- frozen calibrated V2+ comparator: `174.77` UAH mean, `67.30` UAH median;
- status: failed versus V2+, diagnostic only.

The result is intentionally not promoted. The richer feature route is wired and
the selector can switch, but the switched schedules are still too tail-risky on
the final holdout. The next DT/LAVA target should therefore learn to avoid
tail-risk perturbation switches before learning any raw hourly actions.

## Tail-Risk Avoidance v3 Target

Safe-Switch v2 proved that the previous model could technically consume richer
Poland features and switch away from V2+, but it also showed the core failure:
the switch rule was still too aggressive on tail-risk schedules. Tail-Risk
Avoidance v3 changes the target before attempting a full DT. It keeps the same
candidate-index label space, but adds an explicit binary tail-risk label:

- `safe_switch_win`: candidate regret delta versus V2+ is negative;
- `tail_risk_switch`: candidate regret delta versus V2+ is at least the
  configured tail-risk threshold;
- `neutral_or_weak_switch`: candidate is neither a safe win nor a major loss;
- `v2_plus_default`: frozen fallback row;
- `oracle_only_train_diagnostic`: train-only diagnostic row, never admitted to
  final-holdout selection.

The v3 scorer trains two small prior-only ridge-style models:

1. predicted regret delta versus V2+;
2. predicted tail-risk probability.

A candidate is allowed only if it comes from the approved shadow source, is not
a hard-blocked perturbation family, has a prior-safe risk profile, predicts a
minimum regret improvement, and remains below
`max_predicted_tail_risk_probability`. Otherwise the row falls back to frozen
V2+. This is still not a live controller: it is a safer teacher-label layer for
future DT/LAVA work.

Materialized v3 result:

- Dagster run: `98040d0f-23de-435b-86f3-3599e4187fac`;
- label frame rows: `13,885`;
- `safe_switch_win` rows: `4,358`;
- `tail_risk_switch` rows: `4,351`;
- selected Poland shadow schedules: `8 / 90`;
- V2+ fallback schedules: `82 / 90`;
- selected family counts: `82` V2+ fallback, `7` strict/raw-blend schedules,
  and `1` temporal-block reconciled schedule;
- Tail-Risk Avoidance v3 mean regret: `185.65` UAH;
- Tail-Risk Avoidance v3 median regret: `76.32` UAH;
- frozen calibrated V2+ comparator: `174.77` UAH mean, `67.30` UAH median;
- status: failed versus V2+, diagnostic only.

This is better than Safe-Switch v2 (`219.37` UAH mean regret) because the new
tail-risk veto blocks most risky switches, but it still does not beat frozen
V2+. The next DT/LAVA branch should therefore use this v3 label frame as
training data for tail-risk-aware schedule-neighbor supervision, not as a
promoted policy.

## Schedule-Neighbor DT/LAVA Supervision

The next implemented layer uses the v3 label frame as teacher data for a
Decision-Transformer/LAVA bridge, but it still refuses raw hourly
BUY/SELL/HOLD imitation. The training target is:

```text
fallback_v2_plus | safe_schedule_neighbor | avoid_tail_risk_neighbor |
neutral_schedule_neighbor | oracle_neighbor_diagnostic
```

The important design point is that return-to-go and tail-risk labels are used
as train/prior teacher targets. They are not allowed to become final-holdout
selection features. The policy therefore learns a candidate-index/family
supervision problem first, and only later should a real DT sequence model learn
from this dataset.

Materialized result:

- Dagster run: `8b0d59d8-0570-4b6a-bfc4-0a26f0643c9e`;
- teacher rows: `13,885`;
- safe schedule-neighbor rows: `4,358`;
- tail-risk neighbor rows: `4,351`;
- oracle-neighbor diagnostic rows: `1,735`;
- selected Poland shadow schedules: `8 / 90`;
- V2+ fallback schedules: `82 / 90`;
- Schedule-neighbor DT/LAVA policy mean regret: `185.65` UAH;
- Schedule-neighbor DT/LAVA policy median regret: `76.32` UAH;
- behavior-cloning reference mean regret: `310.58` UAH;
- frozen calibrated V2+ comparator: `174.77` UAH mean, `67.30` UAH median;
- status: failed versus V2+, diagnostic teacher-supervision evidence only.

This confirms the core DT/LAVA lesson: the current candidate space contains
real local safe wins, but a policy that switches to them still cannot beat V2+
robustly from prior information. The next DT/LAVA step should train a sequence
model on this candidate-index dataset only after adding better teacher/value
labels or richer point-in-time context. It should not regress to raw hourly
action imitation.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_v2_plus_schedule_neighbor_teacher_label_frame,dfl_lava_schedule_neighbor_candidate_frame,dfl_lava_candidate_value_scorer_frame,dfl_lava_candidate_value_strict_lp_benchmark_frame,dfl_lava_tail_risk_diagnostic_frame,dfl_lava_tail_risk_aware_target_frame,dfl_lava_tail_risk_aware_strict_lp_benchmark_frame,dfl_lava_tail_risk_safe_switch_scorer_frame,dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame,dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame,dfl_lava_tail_risk_safe_switch_scorer_v2_frame,dfl_lava_tail_risk_safe_switch_v2_strict_lp_benchmark_frame,dfl_lava_tail_risk_avoidance_label_frame,dfl_lava_tail_risk_avoidance_scorer_v3_frame,dfl_lava_tail_risk_avoidance_v3_strict_lp_benchmark_frame,dfl_lava_schedule_neighbor_dt_training_frame,dfl_lava_schedule_neighbor_dt_policy_frame,dfl_lava_schedule_neighbor_dt_strict_lp_benchmark_frame `
  -c configs/real_data_dfl_lava_tail_risk_target_week3.yaml
```

Claim boundary: Offline Strategy Promotion/read-model evidence only, no live
dispatch, no dashboard/API default switch, and no deployed DT controller.
