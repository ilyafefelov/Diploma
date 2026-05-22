# DFL Ukrainian Context Candidate V8

## Status

V8 follows Opportunity Backfill + Candidate-Value V7. V7 kept the corrected
Ukrainian-only V2+ comparator honest, but did not beat it. The result means the
current selector/candidate space is still too sparse: another selector or raw
DT target would mostly learn to fall back to V2+.

Frozen comparator remains:

- calibrated Ukrainian-only V2+ mean regret: `174.77` UAH;
- calibrated Ukrainian-only V2+ median regret: `67.30` UAH;
- rolling robustness: `4 / 4` windows;
- `market_execution_enabled=false`.

V8 therefore does not train a new policy. It repairs the input side first:
attach source-backed Ukrainian context to the candidate rows and generate new
feasible schedules that can be strict-rescored later.

Materialized status:

- Dagster run id: `7f1bb67d-95c0-406b-b865-642c4a8b56c0`;
- context-backfilled rows: `43,730`;
- V8 candidate-library rows: `52,855`;
- new V8 generated candidates: `9,125`;
- rows awaiting strict rescore: `9,125`;
- `market_execution_enabled=false`.

The materialized context panel is intentionally conservative: full
`selector_feature_ua_context_ready` is `0 / 43,730` because the joined
publication/weather/load/grid readiness blockers are not all satisfied at once.
The partial means are still useful diagnostics before rescore:
publication/calendar readiness `0.305`, weather/load readiness `0.305`, and
grid-event readiness `0.022`.

Strict-rescore status:

- Dagster run id: `53fcd40c-9e7d-4173-b858-7fb0f3a00c9a`;
- strict-rescored rows: `52,855`;
- rebuilt teacher-label rows: `52,855`;
- V8 generated schedules scored: `9,125`;
- generated schedules better than V2+: `287 / 9,125`;
- final-holdout generated schedules better than V2+: `16`;
- final-holdout material safe-switch labels: `8`;
- `market_execution_enabled=false`.

The rescore shows that new candidates exist, but most generated schedules are
too aggressive. Mean generated regret is `1471.56` UAH, median generated regret
is `1177.57` UAH. The least risky V8 family is the strict-blend rescue schedule
with mean regret `339.57` UAH, but it is still not a promoted result. The next
selector must learn when these rare safe schedules are prior-supported and must
fall back to V2+ otherwise.

Selector-gate status:

- selector/strict Dagster run id: `f3521419-c8a5-4eb4-897e-71dae83d2433`;
- rolling Dagster run id: `5774b502-e67b-403f-a1e7-d72103a46b1f`;
- final V8 switches selected: `6 / 90`;
- V8 selector mean / median regret: `188.42` / `76.32` UAH;
- frozen calibrated V2+ mean / median regret: `174.77` / `67.30` UAH;
- rolling robustness: `0 / 4` promotion windows and `0 / 4` diagnostic windows;
- `market_execution_enabled=false`.

This closes V8 as negative-but-useful evidence. The stricter selector did not
leak final labels and did not over-select broadly, but even six accepted
switches worsened both mean and median regret versus V2+. The conclusion is that
the strict-rescored V8 candidates create local opportunities but not a robust
prior-only replacement for V2+.

## Asset Path

| Asset | Purpose |
|---|---|
| `dfl_ua_context_backfilled_feature_panel_v8_frame` | Merges source-backed Ukrainian prior context from the UA context safe-switch layer onto V7 schedule candidates. New inputs stay under `selector_feature_*`; realized outcomes remain `label_*` or `diagnostic_*`. |
| `dfl_ua_context_feasible_schedule_candidate_library_v8_frame` | Adds Ukrainian-context feasible schedule families around V2+ miss modes. These rows are marked `candidate_value_label_status=pending_strict_rescore` and are not promotable until the unchanged strict LP/oracle evaluator scores them. |
| `dfl_candidate_value_regret_surrogate_v8_frame` | Fits a conservative prior-only selector over strict-rescored V8 labels and keeps V2+ fallback when prior support is weak. |
| `dfl_candidate_value_v8_strict_lp_benchmark_frame` | Compares the selected V8 candidate-value row against frozen V2+ and strict control under the unchanged LP/oracle evaluator. |
| `dfl_candidate_value_v8_rolling_robustness_frame` | Replays four rolling windows with prior-only fitting before each validation window. |

Tracked config:

`configs/real_data_dfl_ua_context_candidate_v8_week3.yaml`

## Candidate Families

V8 adds schedule candidates that can actually change the BESS dispatch pattern:

- `ua_context_peak_trough_shift_v8` - moves charge/discharge toward the
  prior-known forecast trough and peak;
- `ua_context_terminal_reserve_v8` - reduces late-horizon discharge pressure so
  terminal SOC is less fragile;
- `ua_context_morning_evening_block_v8` - tests block-level morning/evening
  shifts instead of one-hour tweaks;
- `ua_context_tail_risk_clipped_v8` - halves aggressive V2+ dispatch in high
  uncertainty regimes;
- `ua_context_strict_blend_rescue_v8` - blends V2+ with strict control as a
  diagnostic rescue schedule.

These candidates are generated from prior forecast/context features and
existing feasible schedules. They are not final-holdout oracle copies.

## Leakage Boundary

- `selector_feature_*` columns are prior inputs only.
- Realized regret, oracle gaps, and candidate deltas remain `label_*` or
  `diagnostic_*`.
- V8 generated candidates start as `pending_strict_rescore`; after rescore,
  selector fitting may use train/prior labels, while final-holdout labels remain
  scoring and diagnostic only.
- Poland/TFT may remain shadow/context evidence elsewhere, but no European rows
  become Ukrainian target rows in this slice.
- No dashboard/API default switch and no live market execution claim.

## Interpretation And Next Gate

The conservative V8 selector gate is now materialized and failed the promotion
rule:

1. it did not beat V2+ mean regret;
2. it worsened median regret;
3. it produced `0 / 4` rolling promotion windows;
4. it preserved `market_execution_enabled=false`.

The next ML step should therefore not be another selector over the same rows.
Either acquire/backfill stronger prior-known Ukrainian context, or redesign the
candidate generator to avoid the high-regret V8 families before fitting a new
DT/LAVA teacher target. A future DT/LAVA run should learn candidate-index or
schedule-family selection with explicit tail-risk abstention, not raw hourly
BUY/SELL/HOLD imitation.

## Run

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_ua_context_backfilled_feature_panel_v8_frame,dfl_ua_context_feasible_schedule_candidate_library_v8_frame,dfl_ua_context_candidate_v8_strict_rescore_frame,dfl_ua_context_candidate_value_teacher_label_panel_v8_frame,dfl_candidate_value_regret_surrogate_v8_frame,dfl_candidate_value_v8_strict_lp_benchmark_frame,dfl_candidate_value_v8_rolling_robustness_frame `
  -c configs/real_data_dfl_ua_context_candidate_v8_week3.yaml
```

If Docker Desktop/WSL is unavailable, the same selection may be run from the
host `.venv` after the upstream V7 and UA-context assets are available in the
configured Dagster storage.
