# DFL Plateau Breaker V4

## Status

This slice keeps Ukrainian-only Schedule/Value Learner V2+ as the frozen thesis
headline and adds a stricter diagnostic path for the V3 plateau. It does not
promote a new controller by default, does not use European market-coupling
features in training, and keeps `market_execution_enabled=false`.

Frozen comparator:

- calibrated V2+ mean regret: `174.77` UAH;
- raw V2+ mean regret: `193.36` UAH;
- improvement versus `strict_similar_day`: `43.73%`;
- rolling robustness: `4 / 4`;
- claim scope: Offline Strategy Promotion only.

## Why V3 Matched V2+

Candidate-Value DFL v3 did train a candidate-level value scorer, but the
configured non-degradation fallback correctly selected V2+ for every
tenant/source row. That is why V3 and V2+ reported identical latest-holdout
regret. The result is not a bug in the arithmetic: it is the safety gate doing
what it was designed to do.

The V4 slice separates three possible plateau causes:

- `candidate_not_better`: no available candidate schedule beats V2+ on the
  anchor;
- `candidate_available_but_not_selected`: a better candidate exists, but the
  scorer/rule misses it;
- `fallback_too_conservative`: relaxing the fallback threshold would improve
  final score, but prior/train evidence is too weak for thesis promotion.

The existing zero-threshold finding stays diagnostic only: raw NBEATSx could
improve from `193.36` to `185.62` UAH, but this still does not beat calibrated
V2+ at `174.77` UAH and was not backed by enough prior evidence.

## New Assets

The additive Dagster assets are:

- `dfl_official_global_panel_v2_v3_plateau_autopsy_frame`;
- `dfl_official_global_panel_plateau_data_quality_audit_frame`;
- `dfl_official_global_panel_schedule_candidate_library_v4_frame`;
- `dfl_official_global_panel_candidate_value_label_panel_v4_frame`;
- `dfl_official_global_panel_candidate_value_dfl_v4_frame`;
- `dfl_official_global_panel_candidate_value_dfl_v4_strict_lp_benchmark_frame`.

The new checks are:

- `dfl_official_global_panel_v2_v3_plateau_autopsy_evidence`;
- `dfl_official_global_panel_plateau_data_quality_audit_evidence`;
- `dfl_official_global_panel_candidate_value_label_panel_v4_evidence`;
- `dfl_official_global_panel_candidate_value_dfl_v4_evidence`.

The tracked run config is:

- `configs/real_data_dfl_candidate_value_dfl_v4_week3.yaml`.

## Data And Feature Audit

The plateau data-quality audit checks whether the remaining regret clusters
align with missing or weak context before another DT/DFL model is attempted:

- Ukrainian DAM observed-history quality;
- weather/load feature coverage;
- calendar, holiday, outage, and grid-event context;
- publication-time availability;
- alignment between regret clusters and missing context.

If the audit shows gaps, the next improvement should be data/context repair
before another model family. If the audit is clean, the next improvement should
come from stronger candidate schedules and value labels.

## Candidate Library V4

V4 expands V3 with candidate schedules that can change the LP solution more
meaningfully than average residual templates:

- `calibrated_quantile_risk_v4`;
- `block_structured_peak_schedule_v4`;
- `soc_terminal_reserve_v4`;
- `spread_volatility_robust_v4`;
- `tenant_degradation_throughput_sweep_v4`;
- `oracle_neighborhood_diagnostic_v4`.

The oracle-neighborhood family is train-only diagnostic evidence. It may explain
whether a better candidate shape exists in train/prior anchors, but it is never
available for final-holdout selection.

## Candidate-Value Scorer V4

The V4 scorer is still candidate-level, not action imitation:

```text
prior context + full schedule features + regime features
  -> learned candidate-level value/regret score
  -> choose one feasible LP-scored schedule per anchor
  -> fall back to V2+ unless prior evidence predicts improvement
  -> strict LP/oracle final scoring
```

The learned feature set extends V3 with forecast volatility, terminal SOC,
dispatch reversal count, peak/trough horizon positions, and anchor hour. Final
holdout labels never fit weights or decide the fallback rule.

## Gate

Candidate-Value DFL v4 can replace V2+ only if it:

- improves mean regret versus V2+;
- does not worsen median regret versus V2+;
- still beats `strict_similar_day` by at least `5%` mean regret;
- preserves thesis-grade Ukrainian observed coverage;
- preserves rolling robustness before any headline replacement;
- has zero safety violations;
- keeps `market_execution_enabled=false`.

If V4 fails, the failure is useful evidence: the project should either improve
point-in-time features/candidate generation or proceed to a DT teacher-trajectory
experiment only after candidate/value evidence improves.

## Materialized Result

The 2026-05-18 materialization completed successfully.

- Dagster run id: `0c57f795-3b5b-4106-ad9d-0776294a1eb4`;
- candidate library rows: `71,040`;
- V4 label-panel rows: `71,040`;
- V4 learner rows: `10` tenant/source rows;
- strict LP/oracle benchmark rows: `720`;
- V4 label-panel and strict-benchmark evidence checks passed;
- `market_execution_enabled=false`.

Strict LP/oracle latest-holdout result:

| Source row | V4 selected mean regret | V2+ mean regret | Strict mean regret | Raw neural mean regret | Improvement vs V2+ | Gate |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| raw official global-panel NBEATSx | `193.36` UAH | `193.36` UAH | `310.58` UAH | `771.26` UAH | `0.00%` | blocked |
| horizon-calibrated official global-panel NBEATSx | `174.77` UAH | `174.77` UAH | `310.58` UAH | `622.25` UAH | `0.00%` | blocked |

The V4 scorer fell back to V2+ for all `10 / 10` tenant/source rows. That is
the correct non-degradation behavior: the selected V4 schedules did not show
enough prior/train evidence to replace V2+. Before fallback, the raw NBEATSx
source had a mean regret of `190.59` UAH versus raw V2+ at `193.36` UAH, but
this was not enough to beat calibrated V2+ at `174.77` UAH and was not promoted.
For the calibrated source, the pre-fallback candidate mean regret was `187.80`
UAH, worse than calibrated V2+.

Plateau autopsy:

| Source row | `candidate_not_better` rows | `fallback_too_conservative` rows | Interpretation |
| --- | ---: | ---: | --- |
| raw official global-panel NBEATSx | `48` | `42` | Some better candidates exist on final holdout, but prior evidence does not justify replacing V2+. |
| horizon-calibrated official global-panel NBEATSx | `71` | `19` | The V2+ blend is already stronger on most anchors; V4 candidates are usually not better. |

Data-quality audit:

- Ukrainian DAM history: `ready`;
- regret-cluster alignment: `ready`;
- weather/load context: `gap_detected`;
- calendar/event context: `gap_detected`;
- publication-time availability: `gap_detected`.

Conclusion: V4 is valid diagnostic evidence, not a stronger headline result.
The plateau is now better explained: the current candidate library still does
not beat calibrated V2+ often enough, and remaining regret likely needs stronger
point-in-time context plus genuinely new candidate shapes before DT is retried.

## Run

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_official_global_panel_schedule_candidate_library_v3_frame,dfl_official_global_panel_candidate_value_label_panel_v3_frame,dfl_official_global_panel_candidate_value_dfl_v3_frame,dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame,dfl_official_global_panel_v2_v3_plateau_autopsy_frame,dfl_official_global_panel_plateau_data_quality_audit_frame,dfl_official_global_panel_schedule_candidate_library_v4_frame,dfl_official_global_panel_candidate_value_label_panel_v4_frame,dfl_official_global_panel_candidate_value_dfl_v4_frame,dfl_official_global_panel_candidate_value_dfl_v4_strict_lp_benchmark_frame -c configs/real_data_dfl_candidate_value_dfl_v4_week3.yaml
```

Claim boundary:

- no live market execution;
- no dashboard/API default switch;
- no EU-feature training;
- `strict_similar_day` remains fallback/control;
- V2+ remains headline evidence unless V4 beats it under the unchanged gate.
