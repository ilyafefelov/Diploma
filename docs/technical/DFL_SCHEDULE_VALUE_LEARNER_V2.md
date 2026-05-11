# DFL Schedule/Value Learner V2

Date: 2026-05-11

This slice adds the next Ukrainian-only DFL research challenger after the
market-coupling access gate. ENTSO-E and European/Hugging Face datasets remain
blocked for training, so the experiment uses only the current thesis-grade
OREE/Open-Meteo schedule-candidate library.

Claim boundary: this is offline DFL research evidence only. It is not full DFL,
not deployed Decision Transformer control, not a promoted controller, and not
market execution. `strict_similar_day` remains the frozen fallback unless the
strict LP/oracle promotion gates pass.

## Rationale

Earlier evidence showed that hourly action classifiers and horizon-bias
forecast correction are too weak for BESS arbitrage. The useful direction is
trajectory/value learning: choose among feasible LP-scored schedules using only
features available before the validation anchor, then score the chosen schedule
with the same strict LP/oracle regret gate.

This follows the DFL literature direction already captured in the thesis plan:
decision quality, regret, and feasible SOC path value matter more than forecast
MAE alone.

## Assets

| Asset | Purpose |
|---|---|
| `dfl_schedule_value_learner_v2_frame` | Selects a deterministic schedule-scoring profile per tenant/source model using train-selection anchors only. |
| `dfl_schedule_value_learner_v2_strict_lp_benchmark_frame` | Emits strict/raw/learner strict LP/oracle evidence rows on the final holdout. |
| `dfl_schedule_value_learner_v2_evidence` | Dagster asset check for coverage, provenance, and claim-boundary validity. |

Config:
[real_data_dfl_schedule_value_learner_v2_week3.yaml](../../configs/real_data_dfl_schedule_value_learner_v2_week3.yaml).

## Feature Contract

Selector-safe inputs are schedule features derived before final-holdout scoring:

- prior family mean regret from train-selection anchors;
- forecast spread;
- forecast objective value;
- throughput and degradation proxy from the candidate schedule;
- SOC slack from the candidate schedule;
- candidate family identity through deterministic scoring and tie-breaking.

Actual-dependent quantities such as final regret and oracle value are labels for
training/evaluation only. Final-holdout actuals may change final scoring but
must not change the selected weight profile.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_schedule_candidate_library_v2_frame,dfl_schedule_value_learner_v2_frame,dfl_schedule_value_learner_v2_strict_lp_benchmark_frame -c configs/real_data_dfl_schedule_value_learner_v2_week3.yaml
```

Latest materialization:

- run id: `cb23badd-5393-438e-9935-d0d31fd6e0e3`;
- materialized assets: `dfl_schedule_candidate_library_v2_frame`,
  `dfl_schedule_value_learner_v2_frame`,
  `dfl_schedule_value_learner_v2_strict_lp_benchmark_frame`;
- asset check: `dfl_schedule_value_learner_v2_evidence` passed;
- final-holdout window: `2026-04-12 23:00:00` through
  `2026-04-29 23:00:00`;
- coverage: five tenants, 18 validation anchors per tenant, 90 tenant-anchors
  per source model.

Latest strict LP/oracle summary:

| Source model | Role | Rows | Tenants | Anchors | Mean regret UAH | Median regret UAH |
|---|---:|---:|---:|---:|---:|---:|
| `nbeatsx_silver_v0` | raw reference | 90 | 5 | 18 | 1121.041 | 555.428 |
| `nbeatsx_silver_v0` | `strict_similar_day` reference | 90 | 5 | 18 | 314.813 | 202.606 |
| `nbeatsx_silver_v0` | schedule/value learner v2 | 90 | 5 | 18 | 258.227 | 132.616 |
| `tft_silver_v0` | raw reference | 90 | 5 | 18 | 1665.410 | 1399.493 |
| `tft_silver_v0` | `strict_similar_day` reference | 90 | 5 | 18 | 314.813 | 202.606 |
| `tft_silver_v0` | schedule/value learner v2 | 90 | 5 | 18 | 248.488 | 89.891 |

Interpretation:

- NBEATSx-source learner improves latest-holdout mean regret by approximately
  17.98% versus `strict_similar_day` and improves median regret.
- TFT-source learner improves latest-holdout mean regret by approximately
  21.07% versus `strict_similar_day` and improves median regret.
- Both source learners improve strongly versus their raw neural references.
- This is the first latest-holdout DFL-style schedule/value evidence that beats
  the frozen strict control under strict LP/oracle scoring.
- It is not enough for production/default promotion by itself. Rolling-window
  robustness over earlier temporal windows is required before the learner can
  feed any offline default-fallback gate.

Rolling robustness update:

- robustness run id: `3a5ef479-14e9-4a2b-8d31-14882cf005c7`;
- asset check: `dfl_schedule_value_learner_v2_robustness_evidence` passed;
- NBEATSx-source learner passes 4 of 4 rolling strict-control windows;
- TFT-source learner passes 3 of 4 rolling strict-control windows;
- both source learners now qualify as robust research challengers under the
  current offline evidence gate.

Promotion update:

- production gate run id: `93d0f01c-5140-4958-a64f-74067144df4f`;
- asset check: `dfl_schedule_value_production_gate_evidence` passed;
- NBEATSx-source learner: `production_promote=true` for offline/read-model
  evidence, with `market_execution_enabled=false`;
- TFT-source learner: `production_promote=true` for offline/read-model evidence,
  with `market_execution_enabled=false`;
- tracked note:
  [DFL_SCHEDULE_VALUE_PRODUCTION_GATE.md](DFL_SCHEDULE_VALUE_PRODUCTION_GATE.md).

The remaining boundary is now market-execution and product-surface semantics,
not the offline DFL evidence itself. This result still does not change
dashboard/API defaults and does not authorize live execution.

## Gate

The development gate may pass if the learner improves versus raw neural
schedules. The promotion gate is stricter:

- five canonical tenants;
- at least 90 final-holdout tenant-anchors per source model;
- thesis-grade observed rows;
- zero safety violations;
- no train/final leakage;
- mean regret improves by at least 5% versus `strict_similar_day`;
- median regret is not worse than `strict_similar_day`.

The rolling robustness gate and the explicit offline promotion/fallback gate now
pass for NBEATSx-source and TFT-source learner variants. The promotion is still
limited to offline/read-model strategy evidence; live execution and dashboard/API
default changes remain out of scope.

## Expected Interpretation

The latest materialization passes the latest-holdout strict-control signal for
both source models. The follow-up robustness gate also passes:

1. NBEATSx-source Schedule/Value Learner V2 passes 4 of 4 rolling strict-control
   windows.
2. TFT-source Schedule/Value Learner V2 passes 3 of 4 rolling strict-control
   windows.
3. The explicit offline promotion gate records `production_promote=true` for
   both source learners while keeping `market_execution_enabled=false`.
4. `strict_similar_day` remains the fallback for undercovered,
   out-of-distribution, failed-source, and live-execution contexts.
