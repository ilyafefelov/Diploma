# UA Context LAVA/DT Candidate-Index Policy

## Status

This slice adds an offline/read-model DT/LAVA bridge that predicts a feasible
schedule candidate or family, not raw hourly BUY/SELL/HOLD actions. It consumes
the Ukrainian context safe-switch feature panel and existing LAVA tail-risk
labels, then competes against the corrected calibrated V2+ baseline under the
unchanged strict LP/oracle gate.

Frozen comparator:

- calibrated V2+ mean regret: `174.77` UAH;
- calibrated V2+ median regret: `67.30` UAH;
- rolling robustness: `4 / 4` windows;
- `market_execution_enabled=false`.

This path is not a live controller, not full DFL, and not a dashboard/API
default switch.

## Why This Exists

Earlier compact residual DFL and offline DT attempts failed because they learned
weaker trajectory/action contracts than the V2+ schedule/value selector. The
subsequent LAVA schedule-neighbor and UA context safe-switch layers showed a
more precise problem: some non-V2+ candidates have value, but risky perturbation
families can create large regret losses, and current prior-only selectors cannot
switch safely enough.

This path therefore changes the DT/LAVA target. The model sees prior context,
candidate features, return/regret-delta labels, and tail-risk labels, then
predicts which candidate index or schedule family is safe to use. If confidence
is weak or tail risk is high, it falls back to corrected calibrated V2+.

## Assets

| Asset | Purpose |
|---|---|
| `dfl_ua_context_lava_teacher_frame` | Builds candidate-index teacher rows from UA context oracle-gap features and LAVA tail-risk labels. |
| `dfl_ua_context_lava_sequence_training_frame` | Converts teacher rows into a sequence-style training table while preserving candidate-index targets. |
| `dfl_ua_context_lava_candidate_policy_frame` | Trains a small deterministic Torch candidate policy with V2+ fallback. |
| `dfl_ua_context_lava_strict_lp_benchmark_frame` | Scores strict, V2+, behavior cloning, and UA-context LAVA policy rows under the unchanged LP/oracle evaluator. |
| `dfl_ua_context_lava_rolling_robustness_frame` | Replays prior-only rolling windows and checks whether the policy is robust enough to replace V2+. |

Tracked config:

`configs/real_data_dfl_ua_context_lava_dt_week3.yaml`

## Inputs And Leakage Boundary

Primary inputs:

- `dfl_ua_context_oracle_gap_feature_panel_frame`;
- `dfl_lava_tail_risk_avoidance_label_frame`;
- corrected calibrated V2+ strict rows as comparator/fallback.

Feature contract:

- prior-only selector columns use `selector_feature_*`;
- realized outcomes stay in `label_*` or `diagnostic_*`;
- final-holdout actuals are scoring/diagnostic only;
- no raw hourly action imitation target is emitted;
- Poland/TFT candidates may remain shadow/context candidates, but no EU rows
  become Ukrainian target rows.

## Model

The candidate policy uses a small Torch MLP with deterministic seeds. CUDA may
be used when available, with CPU fallback. The model predicts:

- expected regret delta versus corrected calibrated V2+;
- tail-risk probability;
- candidate index / schedule family / fallback decision.

The policy switches away from V2+ only when predicted improvement clears the
configured margin and predicted tail risk is below the configured threshold.
Otherwise the selected schedule is corrected calibrated V2+.

Behavior cloning is always emitted as a baseline row, so any DT/LAVA improvement
is compared against both V2+ and a weaker imitation-style baseline.

## Gate

Promotion requires:

- mean regret beats `174.77` UAH by at least `5%`;
- median regret is not worse than `67.30` UAH;
- rolling robustness passes `4 / 4` windows;
- zero safety violations;
- `market_execution_enabled=false`.

A failed challenger is still valid diagnostic evidence if the asset checks pass:
it means this candidate-index objective did not find a safe replacement for V2+
under the current Ukrainian context/candidate space.

## Materialized Latest-90 Result

The path materialized successfully in Dagster run
`e5d19967-2bce-4e0e-aceb-6fce7e8a5e9d`.

Latest-holdout strict LP/oracle result:

| Selection role | Rows | Mean regret | Median regret | Safety / execution |
|---|---:|---:|---:|---|
| `strict_reference` | `90` | `310.58` UAH | `198.39` UAH | `market_execution_enabled=false` |
| `schedule_value_learner_v2_plus_reference` | `90` | `174.77` UAH | `67.30` UAH | `market_execution_enabled=false` |
| `ua_context_lava_behavior_cloning_reference` | `90` | `576.52` UAH | `245.57` UAH | `market_execution_enabled=false` |
| `ua_context_lava_candidate_policy` | `90` | `174.77` UAH | `67.30` UAH | `market_execution_enabled=false` |

Rolling robustness result:

- `0 / 4` robust challenger windows;
- `0 / 4` diagnostic-success windows;
- all four validation windows used V2+ fallback for all `90 / 90`
  tenant-anchor rows.

Interpretation: the candidate-index DT/LAVA objective is wired correctly and
keeps the claim boundary, but it still does not beat V2+. It is useful negative
evidence: behavior cloning is much worse, while the conservative DT/LAVA policy
falls back to V2+ instead of taking unsafe candidate switches.

The next implemented slice is therefore not another DT variant. It is
[DFL_REGRET_SURROGATE_V1](DFL_REGRET_SURROGATE_V1.md): first audit whether the
candidate universe has at least 5% oracle-switch upside versus V2+, then train
a candidate-level regret-delta/tail-risk scorer only if that upper bound is
worth learning.

## Run

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_ua_context_lava_teacher_frame,dfl_ua_context_lava_sequence_training_frame,dfl_ua_context_lava_candidate_policy_frame,dfl_ua_context_lava_strict_lp_benchmark_frame,dfl_ua_context_lava_rolling_robustness_frame `
  -c configs/real_data_dfl_ua_context_lava_dt_week3.yaml
```

If upstream UA context safe-switch or LAVA tail-risk assets are missing from the
active Dagster IO store, materialize those documented upstream selections first.

## Thesis Interpretation

This is the first DT/LAVA slice that is aligned with the current evidence:
learn candidate-index or schedule-family supervision, avoid tail-risk
perturbations, and keep V2+ as fallback. It should replace V2+ only if it wins
under the same strict LP/oracle gate. Otherwise it becomes evidence that the next
improvement needs richer point-in-time context, better candidate labels, or a
stronger schedule-value objective before raw DT control is attempted.
