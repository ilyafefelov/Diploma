# Schedule/Value Learner V3

Schedule/Value Learner V3 is an additive experiment on top of the frozen
Schedule/Value Learner V2 evidence. It does not replace V2 and it does not
change the current Offline Strategy Promotion claim until it is materialized
and scored by the same strict LP/oracle gate.

## Purpose

V2 selected one fixed scoring profile per tenant/source from prior anchors.
V3 keeps the same feasible schedule library and strict final evaluator, but it
fits a small deterministic ridge-style value ranker on train/prior anchors.
The ranker predicts candidate schedule regret from prior-only schedule features
and chooses the lowest predicted-regret schedule per anchor.

This is still not full DFL and not Decision Transformer control. It is a
traceable schedule/value ranking experiment for offline/read-model evidence
only. `strict_similar_day` remains the frozen control and fallback.

## Inputs And Outputs

Primary official-global-panel inputs:

- `dfl_official_global_panel_schedule_candidate_library_v2_frame`;
- frozen `dfl_official_global_panel_schedule_value_learner_v2_frame`.

New additive outputs:

- `dfl_official_global_panel_schedule_value_learner_v3_frame`;
- `dfl_official_global_panel_schedule_value_learner_v3_strict_lp_benchmark_frame`.

Compact-model mirrors are also registered:

- `dfl_schedule_value_learner_v3_frame`;
- `dfl_schedule_value_learner_v3_strict_lp_benchmark_frame`.

The strict benchmark emits four aligned roles for every tenant/source/anchor:

| Role | Meaning |
|---|---|
| `strict_reference` | frozen `strict_similar_day` control |
| `raw_reference` | raw source forecast schedule |
| `schedule_value_learner_v2_reference` | frozen V2 selected schedule |
| `schedule_value_learner_v3` | V3 selected schedule |

## Feature Contract

The first V3 ranker uses only candidate-library fields available before the
final holdout is scored:

- `prior_family_mean_regret_uah`;
- `forecast_spread_uah_mwh`;
- `forecast_objective_value_uah`;
- `total_degradation_penalty_uah`;
- `total_throughput_mwh`;
- `soc_min_slack_fraction`.

Final-holdout actual prices can change final scoring, but they must not change
the fitted V3 weights, selected feature names, or training family counts.

## Gate Interpretation

V3 is promoted as stronger offline evidence only if it:

- preserves thesis-grade observed coverage and zero safety violations;
- covers five tenants and at least 90 final-holdout tenant-anchors per source;
- beats `strict_similar_day` by at least 5% mean regret;
- does not worsen median regret versus `strict_similar_day`;
- does not degrade the frozen V2 schedule/value result;
- keeps `not_market_execution=true`.

If V3 fails any of those conditions, it remains diagnostic evidence only. The
current thesis headline is V2+, while V2 remains the frozen comparison baseline.

## Run Command

After the 365-anchor official global-panel evidence packet exists locally, run:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_official_global_panel_schedule_candidate_library_frame,dfl_official_global_panel_schedule_candidate_library_v2_frame,dfl_official_global_panel_schedule_value_learner_v2_frame,dfl_official_global_panel_schedule_value_learner_v3_frame,dfl_official_global_panel_schedule_value_learner_v3_strict_lp_benchmark_frame `
  -c configs/real_data_official_global_panel_schedule_value_v3_week3.yaml
```

Record the result as V3 evidence only after the strict benchmark frame is
materialized and the asset check passes. The materialized V3 run did not replace
the stronger V2+ evidence; future selector work should compare against V2+, not
only against frozen V2.
