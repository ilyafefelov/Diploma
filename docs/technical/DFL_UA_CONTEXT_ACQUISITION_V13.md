# DFL UA Context Acquisition V13

## Purpose

V13 is an acquisition/readiness gate before any new candidate schedules are
generated. It exists because V12 blocked DT/LAVA: current Ukrainian context is
only partially covered and the safe teacher-label count remains below the
configured threshold.

Frozen comparator remains Ukrainian-only calibrated V2+:

- mean regret: `174.77` UAH;
- median regret: `67.30` UAH;
- rolling robustness: `4 / 4`;
- `market_execution_enabled=false`.

## Assets

The V13 path is additive in `gold_dfl_training`:

- `dfl_ua_context_acquisition_source_evidence_v13_frame`
- `dfl_ua_context_source_inventory_v13_frame`
- `dfl_ua_context_acquisition_readiness_v13_frame`

Config:

- `configs/real_data_dfl_ua_context_v13_acquisition_week3.yaml`

Packet exporter:

- `scripts/materialize_ua_context_v13_acquisition_packet.py`
- default output: `data/research_runs/week3_dfl_ua_context_acquisition_v13/`

## What V13 Requires

V13 keeps the V12 Ukrainian-only source contract and adds explicit acquisition
requirements for the observed blockers:

- measured tenant load/PV telemetry or source-backed historical import;
- explicit row-level OREE DAM publication receipts, not only broad rules;
- richer Ukrenergo/grid/outage/no-event history;
- longer Ukrainian DAM/weather/load context for sparse safe-switch labels;
- enough prior/train non-tail-risk material safe-switch labels.

If any required source is partial or missing, the readiness frame emits
`data_acquisition_needed`. It does not create synthetic features, train a
selector, build candidates, or start DT/LAVA.

The source-evidence frame separates three cases that were previously easy to
blur:

- OREE DAM market rules are valid publication-deadline evidence, but they are
  not row-level publication receipts. V13 therefore marks this as partial until
  explicit row metadata or source logs exist.
- Open-Meteo weather and configured tenant load/PV proxy rows can satisfy the
  source-backed proxy lane, but they do not claim measured site telemetry.
- Ukrenergo/grid-event coverage may include explicit no-event windows only when
  the archive coverage window spans the anchor; missing history remains a
  blocker.

## Gate

`v13_candidate_generation_ready=true` only when:

- every required Ukrainian source family is `ready_prior_context`;
- every tenant/source has at least `20` prior/train non-tail-risk material
  safe-switch examples;
- `market_execution_enabled=false`;
- target labels remain candidate index / schedule family, not raw hourly
  BUY/SELL/HOLD actions.

If the gate stays blocked, the next action is real Ukrainian data acquisition or
source-backed import work, not another selector over the same candidate space.

## Materialization

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_ua_context_acquisition_source_evidence_v13_frame,dfl_ua_context_source_inventory_v13_frame,dfl_ua_context_acquisition_readiness_v13_frame `
  -c configs/real_data_dfl_ua_context_v13_acquisition_week3.yaml
```

Export local packet:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_ua_context_v13_acquisition_packet.py `
  --source-evidence-pickle <path> `
  --source-inventory-pickle <path> `
  --readiness-pickle <path> `
  --run-slug week3_dfl_ua_context_acquisition_v13
```

## Claim Boundary

This slice is Offline Strategy Promotion evidence only. It does not enable live
dispatch, does not switch dashboard/API defaults, does not claim market
execution, and does not turn Poland/EU rows into Ukrainian target rows.
