# DFL Opportunity Backfill V7

## Status

This slice follows Sparse Safe-Switch V6. It does not start another DT variant.
V6 proved that the current selector-safe candidate universe has no robust
final-holdout switch to learn: `0 / 90` non-V2+ rows were selected, the result
matched V2+ at `174.77` / `67.30` UAH mean/median regret, and rolling evidence
was `0 / 4`.

Frozen comparator remains:

- calibrated Ukrainian-only V2+ mean regret: `174.77` UAH;
- calibrated Ukrainian-only V2+ median regret: `67.30` UAH;
- rolling robustness: `4 / 4` windows;
- `market_execution_enabled=false`.

V7 therefore changes the question from "can another selector choose existing
rows better?" to "what prior-supported context or feasible schedules must be
added before DT/LAVA has useful teacher labels?"

## Asset Path

| Asset | Purpose |
|---|---|
| `dfl_v2_plus_opportunity_backfill_requirements_frame` | Classifies each tenant-anchor as `backfill_needed`, `candidate_generation_needed`, `dt_ready`, or `stop_modeling_current_candidate_space`. It separates missing context, candidate-family gaps, strict-control local wins, SOC pressure, spread regimes, peak/trough timing pressure, and tail-risk dominance. |
| `dfl_backfilled_context_feature_panel_v7_frame` | Adds prior-only `selector_feature_*` context readiness and missingness features. Realized outcomes stay under `label_*` or `diagnostic_*`. |
| `dfl_feasible_schedule_candidate_library_v7_frame` | Adds feasible candidate families around V2+ misses: V2+ neighborhood variants, strict-guarded rescue schedules, terminal-SOC reserve, spread-volatility robust, morning/evening block, and throughput/degradation sweep schedules. |
| `dfl_candidate_value_teacher_label_panel_v7_frame` | Attaches candidate-value labels and nearest-prior support statistics. Final holdout remains scoring/diagnostic only. |
| `dfl_candidate_value_regret_surrogate_v7_frame` | Selects a V7 candidate only when prior material examples, predicted improvement, and tail-risk checks pass; otherwise it falls back to V2+. |
| `dfl_candidate_value_v7_strict_lp_benchmark_frame` | Scores strict, V2+, and V7 under the unchanged strict LP/oracle evaluator. |
| `dfl_candidate_value_v7_rolling_robustness_frame` | Replays the V7 rule over rolling prior-only windows. |

Tracked config:

`configs/real_data_dfl_opportunity_backfill_v7_week3.yaml`

## Leakage Boundary

- `selector_feature_*` columns are prior inputs only.
- Realized regret, oracle-best regret, strict-control local wins, and candidate
  regret deltas are `label_*` or `diagnostic_*` only.
- Mutating final-holdout labels may change strict scores and diagnostics, but
  must not change generated prior features, selected feature names, nearest
  prior support, thresholds, or selected candidate keys.
- Oracle-neighborhood diagnostics remain train-only.
- Poland/TFT rows may appear as shadow/context candidates only; European rows
  never become Ukrainian target rows.

## Gate

Promotion requires all of the following:

- mean regret improves over V2+ by at least `5%`;
- median regret is not worse than `67.30` UAH;
- rolling robustness passes `4 / 4` windows;
- zero safety violations;
- `market_execution_enabled=false`.

Diagnostic success is weaker and does not replace V2+: any positive mean
improvement without median harm and at least `3 / 4` diagnostic windows would
justify a later DT/LAVA candidate-index target.

DT/LAVA is allowed to resume only if V7 produces useful teacher labels: at least
`20` prior/train material safe-switch examples, selector-safe oracle upper
bound above `5%`, and no tail-risk-heavy rolling failure.

## Run

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_v2_plus_opportunity_backfill_requirements_frame,dfl_backfilled_context_feature_panel_v7_frame,dfl_feasible_schedule_candidate_library_v7_frame,dfl_candidate_value_teacher_label_panel_v7_frame,dfl_candidate_value_regret_surrogate_v7_frame,dfl_candidate_value_v7_strict_lp_benchmark_frame,dfl_candidate_value_v7_rolling_robustness_frame `
  -c configs/real_data_dfl_opportunity_backfill_v7_week3.yaml
```

If upstream V6 assets are missing, materialize the Regret-Surrogate/V6 path
first with `configs/real_data_dfl_regret_surrogate_v1_week3.yaml`.

## Thesis Interpretation

V7 is not a new live strategy. It is a thesis-synced opportunity backfill and
candidate-value gate. A positive V7 result would mean the project has enough
prior-supported safe-switch examples to justify DT/LAVA over candidate index,
schedule family, or schedule block targets. A fallback result means the current
Ukrainian-only evidence is exhausted at the candidate/value layer and the next
branch should be data/context acquisition rather than another model over the
same rows.
