# DFL UA Context V12 Safe Teacher Backfill

## Purpose

V11 proved that DT/LAVA should not start yet: the lower-tail-risk candidate
universe had only `2-7` prior material safe-switch examples per tenant, below
the configured `20` examples required for candidate-index supervision. V12
therefore does two bounded things:

1. expand the Ukrainian context/source inventory without inventing unavailable
   measurements;
2. rebuild safe teacher labels and conservative low-tail candidates before any
   new DT/LAVA training is allowed.

Frozen comparator remains Ukrainian-only calibrated V2+:

- mean regret: `174.77` UAH;
- median regret: `67.30` UAH;
- rolling robustness: `4 / 4`;
- `market_execution_enabled=false`.

## Assets

The V12 path is additive in `gold_dfl_training`:

- `dfl_ua_context_source_expansion_inventory_v12_frame`
- `dfl_ua_expanded_anchor_context_panel_v12_frame`
- `dfl_ua_safe_switch_teacher_label_panel_v12_frame`
- `dfl_ua_low_tail_candidate_library_v12_frame`
- `dfl_ua_low_tail_candidate_v12_strict_rescore_frame`
- `dfl_ua_v12_dt_lava_readiness_decision_frame`

Config:

- `configs/real_data_dfl_ua_context_v12_backfill_week3.yaml`

Packet exporter:

- `scripts/materialize_ua_v12_safe_teacher_packet.py`
- default output: `data/research_runs/week3_dfl_ua_context_v12_safe_teacher_backfill/`

## Source Expansion Contract

V12 uses the existing Ukrainian-only source paths first:

- OREE DAM history and publication-rule evidence;
- Open-Meteo historical weather;
- tenant load/PV proxy features;
- Ukrenergo/grid-event archive or source-backed no-event coverage;
- calendar, holiday, and block context.

It also records hooks for new Ukrainian data acquisition:

- measured tenant load/PV telemetry imports;
- explicit DAM/IDM source/publication evidence for preview;
- richer grid/outage/event archive imports.

If these optional sources are absent, V12 marks them as
`blocked_missing_source`. It does not synthesize features or treat missing source
coverage as proof of zero risk.

## Candidate Families

V12 generated candidates are deliberately lower-tail-risk:

- `micro_v2_strict_blend`;
- `terminal_soc_preserve_clip`;
- `low_throughput_cap`;
- `peak_trough_shift_minus_1h`;
- `peak_trough_shift_plus_1h`.

The +/-1 hour peak/trough alternatives are allowed only when the prior context
has neighbor support. All generated rows start as `pending_strict_rescore` and
must pass the unchanged LP/oracle label rebuild before they can become teacher
rows.

## DT/LAVA Gate

V12 is a precondition, not a DT training run. `dt_lava_ready=true` is allowed
only if each tenant/source has at least `20` prior/train non-tail-risk material
safe-switch examples. The target label space remains candidate index / schedule
family. Raw hourly BUY/SELL/HOLD imitation is explicitly out of scope.

If V12 still cannot create enough safe labels, the correct conclusion is data
scarcity or candidate scarcity. The next branch should be real Ukrainian data
acquisition or safer candidate design, not another small selector or raw-action
Decision Transformer.

## Materialized Result

Dagster run `d1712548-a4fa-4eca-955d-183d1c4f258c` materialized the V12 path
and exported `week3_dfl_ua_context_v12_safe_teacher_backfill`.

- Source expansion inventory emitted `8` source-family rows.
- Current Ukrainian context sources were only partially covered
  (`coverage_ratio=0.5933869526362824`) for OREE DAM history, Open-Meteo,
  tenant load/PV proxy, Ukrenergo grid events, and calendar/publication rules.
- Optional acquisition hooks for measured tenant load/PV, explicit DAM/IDM
  source/publication evidence for preview, and richer grid/outage archives remained
  `blocked_missing_source`.
- V12 context rows: `1,825`; ready rows: `0`; decision:
  `blocked_missing_required_sources`.
- Candidate rows stayed at `23,499`, but V12 generated `0` new low-tail
  candidates because the source gate was not ready.
- Prior material safe-switch examples per tenant stayed at `2-7`, below the
  required `20`.
- `dt_lava_ready=false`, readiness decision:
  `blocked_insufficient_prior_safe_switch_examples`.

This is a valid blocked evidence packet. It confirms that the next improvement
is data acquisition/backfill, not DT/LAVA training on the current label space.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_ua_context_source_expansion_inventory_v12_frame,dfl_ua_expanded_anchor_context_panel_v12_frame,dfl_ua_safe_switch_teacher_label_panel_v12_frame,dfl_ua_low_tail_candidate_library_v12_frame,dfl_ua_low_tail_candidate_v12_strict_rescore_frame,dfl_ua_v12_dt_lava_readiness_decision_frame `
  -c configs/real_data_dfl_ua_context_v12_backfill_week3.yaml
```

## Claim Boundary

This slice is Offline Strategy Promotion evidence only. It does not enable live
dispatch, does not switch the dashboard/API default strategy, does not claim
market execution, and does not turn Poland/EU rows into Ukrainian target rows.
