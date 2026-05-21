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

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_v2_plus_schedule_neighbor_teacher_label_frame,dfl_lava_schedule_neighbor_candidate_frame,dfl_lava_candidate_value_scorer_frame,dfl_lava_candidate_value_strict_lp_benchmark_frame,dfl_lava_tail_risk_diagnostic_frame,dfl_lava_tail_risk_aware_target_frame,dfl_lava_tail_risk_aware_strict_lp_benchmark_frame `
  -c configs/real_data_dfl_lava_tail_risk_target_week3.yaml
```

Claim boundary: Offline Strategy Promotion/read-model evidence only, no live
dispatch, no dashboard/API default switch, and no deployed DT controller.
