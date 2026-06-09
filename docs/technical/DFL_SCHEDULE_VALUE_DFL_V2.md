# DFL Schedule/Value DFL v2

## Status

This slice adds an official-global-panel **DFL objective redesign** experiment
anchored to the frozen Ukrainian-only Schedule/Value Learner V2+ evidence.
It is not another tiny Decision Transformer run. It tests whether a
prior-only pairwise schedule-family value objective can improve the final
strict LP/oracle regret metric.

Current comparator remains:

- calibrated V2+ mean regret: `174.77` UAH;
- improvement versus `strict_similar_day`: `43.73%`;
- rolling robustness: `4 / 4` windows;
- `market_execution_enabled=false`.

## Why This Slice Exists

The compact residual DFL/offline DT bridge and the official V2+-teacher bridge
both failed to beat V2+. The failure audit found that the challengers mostly
collapsed onto weak candidate families or imitated trajectories rather than
learning a robust ordering among feasible schedules.

The DFL v2 experiment therefore changes the objective:

```text
official V2+ candidate library
  -> train/prior pairwise family comparisons
  -> select one schedule family only if it improves prior V2+
  -> otherwise fall back to V2+
  -> strict LP/oracle final scoring
```

## Assets

The additive Dagster assets are:

- `dfl_official_global_panel_schedule_value_dfl_v2_frame`;
- `dfl_official_global_panel_schedule_value_dfl_v2_strict_lp_benchmark_frame`.

The asset check is:

- `dfl_official_global_panel_schedule_value_dfl_v2_evidence`.

The tracked config is:

- `configs/real_data_dfl_schedule_value_dfl_v2_week3.yaml`.

## Selection Contract

The selector uses only train/prior anchors to compute:

- pairwise schedule-family value score;
- pairwise win count;
- train mean regret per schedule family;
- fallback decision versus frozen V2+.

Final-holdout actuals may affect only scoring metrics, not:

- selected schedule family;
- fallback decision;
- pairwise family scores;
- candidate-generation parameters.

## Gate

A DFL v2 candidate can replace V2+ only if it:

- improves mean regret versus V2+;
- does not worsen median regret versus V2+;
- still beats `strict_similar_day` by at least `5%` mean regret;
- preserves thesis-grade coverage and zero safety violations;
- keeps `market_execution_enabled=false`.

If the gate fails, the result is still useful negative evidence: it means the
pairwise schedule-family objective did not beat the current thesis headline,
and V2+ remains the Offline Strategy Promotion result.

## Materialized Result

The slice materialized successfully on the official global-panel Ukrainian-only
evidence path.

- Dagster run id: `9af65d45-6c7d-4aec-b71b-7fb31fd2147d`;
- evidence check: `dfl_official_global_panel_schedule_value_dfl_v2_evidence`
  passed;
- local evidence packet:
  `data/research_runs/week3_dfl_schedule_value_dfl_v2_comparison/`;
- strict rows: `720`;
- learner trace rows: `10`;
- gate decision: `diagnostic_pass_replacement_blocked`.

The result is valid DFL objective evidence but not a thesis-headline
replacement. For calibrated official NBEATSx, DFL v2 matched V2+ at `174.77`
UAH mean regret and `67.30` UAH median regret, giving `0.00%` improvement
versus V2+ while still preserving the V2+ improvement versus
`strict_similar_day`. For raw official NBEATSx, DFL v2 likewise matched V2+ at
`193.36` UAH mean regret and `68.89` UAH median regret.

Interpretation: the prior-only pairwise schedule-family objective did not find
a family that could safely improve on the frozen V2+ selector. It therefore
confirms that V2+ remains the current Offline Strategy Promotion headline, while
future DFL work should target either richer candidate families or a true
candidate-value objective rather than another action-imitation DT.

## Run

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_official_global_panel_schedule_value_dfl_v2_frame,dfl_official_global_panel_schedule_value_dfl_v2_strict_lp_benchmark_frame -c configs/real_data_dfl_schedule_value_dfl_v2_week3.yaml
```

Claim boundary:

- no live market execution;
- no dashboard/API default switch;
- no EU-feature training;
- `strict_similar_day` remains fallback/control;
- V2+ remains headline evidence unless DFL v2 beats it under the unchanged gate.
