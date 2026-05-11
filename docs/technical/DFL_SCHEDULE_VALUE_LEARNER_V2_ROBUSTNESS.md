# DFL Schedule/Value Learner V2 Rolling Robustness

Date: 2026-05-11

This slice validates whether the latest-holdout Schedule/Value Learner V2
breakthrough survives earlier temporal windows. It is a required gate before any
offline promotion claim.

Claim boundary: this remains offline DFL research evidence only. It is not full
DFL, not Decision Transformer control, not a default controller, and not market
execution. `strict_similar_day` remains the frozen fallback unless the rolling
robustness and production-promotion gates pass.

## Asset

| Asset | Purpose |
|---|---|
| `dfl_schedule_value_learner_v2_robustness_frame` | Replays Schedule/Value Learner V2 over four rolling validation windows with prior-only weight-profile selection. |
| `dfl_schedule_value_learner_v2_robustness_evidence` | Dagster asset check for rolling-window coverage and claim-boundary validity. |

Config:
[real_data_dfl_schedule_value_learner_v2_robustness_week3.yaml](../../configs/real_data_dfl_schedule_value_learner_v2_robustness_week3.yaml).

## Protocol

Default scope:

- five canonical tenants;
- Ukrainian OREE DAM and Open-Meteo/load-derived context only;
- current 104-anchor panel;
- four latest-first rolling windows;
- 18 validation anchors per tenant/window;
- at least 30 prior anchors before each validation window;
- source models: `tft_silver_v0`, `nbeatsx_silver_v0`.

The learner is replayed independently for each source/window. For each rolling
window, weight profiles are selected only from anchors before that validation
window. Validation actuals may affect strict LP/oracle scoring only; they must
not affect selected profiles or selected schedule families.

## Gate

Development evidence:

- schedule/value learner improves mean regret versus raw neural schedules in a
  rolling window.

Robust research challenger:

- latest window passes versus `strict_similar_day`;
- at least three of four rolling windows improve mean regret by at least 5%
  versus `strict_similar_day`;
- median regret is not worse than `strict_similar_day`;
- thesis-grade observed coverage, zero safety violations, no leakage, and
  `not_market_execution=true` hold for every row.

Production/default promotion remains blocked in this robustness slice itself.
The follow-up gate is
[`dfl_schedule_value_production_gate_frame`](DFL_SCHEDULE_VALUE_PRODUCTION_GATE.md),
which consumes this robustness evidence and keeps market execution disabled.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_schedule_candidate_library_v2_frame,dfl_schedule_value_learner_v2_robustness_frame -c configs/real_data_dfl_schedule_value_learner_v2_robustness_week3.yaml
```

## Materialized Result

Latest robustness materialization:

- Dagster run id: `3a5ef479-14e9-4a2b-8d31-14882cf005c7`;
- materialized assets: `dfl_schedule_candidate_library_v2_frame` and
  `dfl_schedule_value_learner_v2_robustness_frame`;
- asset check: `dfl_schedule_value_learner_v2_robustness_evidence` passed;
- coverage: five tenants, four rolling windows, 18 validation anchors per
  tenant/window, 90 validation tenant-anchors per source/window;
- claim flags: `not_full_dfl=true`, `not_market_execution=true`,
  `production_promote=false`.

Rolling strict LP/oracle summary:

| Source model | Window | Validation window | Strict mean | Learner mean | Mean improvement vs strict | Strict median | Learner median | Strict pass |
|---|---:|---|---:|---:|---:|---:|---:|---|
| `nbeatsx_silver_v0` | 1 | `2026-04-12 23:00` to `2026-04-29 23:00` | 314.813 | 258.227 | 17.97% | 202.606 | 132.616 | yes |
| `nbeatsx_silver_v0` | 2 | `2026-03-17 23:00` to `2026-04-11 23:00` | 844.037 | 700.519 | 17.00% | 484.049 | 337.228 | yes |
| `nbeatsx_silver_v0` | 3 | `2026-02-27 23:00` to `2026-03-16 23:00` | 663.517 | 491.954 | 25.86% | 511.338 | 325.295 | yes |
| `nbeatsx_silver_v0` | 4 | `2026-02-09 23:00` to `2026-02-26 23:00` | 1019.414 | 848.625 | 16.75% | 732.853 | 583.734 | yes |
| `tft_silver_v0` | 1 | `2026-04-12 23:00` to `2026-04-29 23:00` | 314.813 | 248.488 | 21.07% | 202.606 | 89.891 | yes |
| `tft_silver_v0` | 2 | `2026-03-17 23:00` to `2026-04-11 23:00` | 844.037 | 695.186 | 17.64% | 484.049 | 361.022 | yes |
| `tft_silver_v0` | 3 | `2026-02-27 23:00` to `2026-03-16 23:00` | 663.517 | 523.810 | 21.06% | 511.338 | 307.314 | yes |
| `tft_silver_v0` | 4 | `2026-02-09 23:00` to `2026-02-26 23:00` | 1019.414 | 980.206 | 3.85% | 732.853 | 751.708 | no |

Interpretation:

- NBEATSx-source Schedule/Value Learner V2 passes 4 of 4 rolling strict-control
  windows.
- TFT-source Schedule/Value Learner V2 passes 3 of 4 rolling strict-control
  windows. The oldest window fails because mean-regret improvement is below 5%
  and median regret is worse than `strict_similar_day`.
- Both sources meet the `robust_research_challenger` label under the current
  gate because the latest window passes and at least three of four rolling
  strict-control windows pass.
- This is the first robust DFL-style schedule/value evidence in the project,
  but it is still not full DFL, not Decision Transformer control, not live
  market execution, and not a dashboard/API default controller.

## Promotion Follow-Up

The follow-up promotion materialization succeeded under Dagster run
`93d0f01c-5140-4958-a64f-74067144df4f`. Asset check
`dfl_schedule_value_production_gate_evidence` passed.

| Source model | Latest mean improvement vs strict | Rolling strict passes | Production promote | Market execution |
|---|---:|---:|---|---|
| `nbeatsx_silver_v0` | 17.97% | 4 / 4 | true | false |
| `tft_silver_v0` | 21.07% | 3 / 4 | true | false |

This promotion is intentionally scoped to offline/read-model strategy evidence.
It does not change dashboard/API defaults and does not enable live market
execution. `strict_similar_day` remains the fallback for undercovered,
out-of-distribution, or failed-source regimes.
