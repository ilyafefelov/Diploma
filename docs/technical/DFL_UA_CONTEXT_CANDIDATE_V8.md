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

## Asset Path

| Asset | Purpose |
|---|---|
| `dfl_ua_context_backfilled_feature_panel_v8_frame` | Merges source-backed Ukrainian prior context from the UA context safe-switch layer onto V7 schedule candidates. New inputs stay under `selector_feature_*`; realized outcomes remain `label_*` or `diagnostic_*`. |
| `dfl_ua_context_feasible_schedule_candidate_library_v8_frame` | Adds Ukrainian-context feasible schedule families around V2+ miss modes. These rows are marked `candidate_value_label_status=pending_strict_rescore` and are not promotable until the unchanged strict LP/oracle evaluator scores them. |

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
- V8 generated candidates are `pending_strict_rescore`; their label fields are
  placeholders until strict LP/oracle scoring runs.
- Poland/TFT may remain shadow/context evidence elsewhere, but no European rows
  become Ukrainian target rows in this slice.
- No dashboard/API default switch and no live market execution claim.

## Next Gate

The next slice is a strict-rescore V8 gate:

1. score the V8 generated schedules through the same strict LP/oracle evaluator;
2. rebuild candidate-value labels after strict rescore;
3. train a conservative selector only if V8 creates prior-supported material
   safe-switch examples;
4. compare against frozen V2+ with the same promotion rule: mean regret at least
   `5%` better, median not worse than `67.30` UAH, `4 / 4` rolling windows, zero
   safety violations, and `market_execution_enabled=false`.

If V8 still cannot create prior-supported wins, the thesis conclusion should be
that the current Ukrainian-only evidence is candidate/data limited. DT/LAVA
then needs new teacher/value labels from data acquisition or strict-rescored
schedule neighborhoods, not another raw hourly action imitation run.

## Run

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_ua_context_backfilled_feature_panel_v8_frame,dfl_ua_context_feasible_schedule_candidate_library_v8_frame `
  -c configs/real_data_dfl_ua_context_candidate_v8_week3.yaml
```

If Docker Desktop/WSL is unavailable, the same selection may be run from the
host `.venv` after the upstream V7 and UA-context assets are available in the
configured Dagster storage.
