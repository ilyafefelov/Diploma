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

- `dfl_ua_dam_publication_receipts_overlay_frame`
- `dfl_ua_dam_publication_backfill_frame`
- `dfl_ua_context_safe_switch_examples_v13_frame`
- `dfl_ua_context_safe_switch_readiness_overlay_v13_frame`
- `dfl_ua_context_acquisition_source_evidence_v13_frame`
- `dfl_ua_context_source_inventory_v13_frame`
- `dfl_ua_context_acquisition_readiness_v13_frame`

Config:

- `configs/real_data_dfl_ua_context_v13_acquisition_week3.yaml`

Packet exporter:

- `scripts/materialize_ua_context_v13_acquisition_packet.py`
- default output: `data/research_runs/week3_dfl_ua_context_acquisition_v13/`

## DAM Receipt Overlay

`dfl_ua_dam_publication_receipts_overlay_frame` optionally left-joins explicit
OREE DAM publication receipt metadata onto observed hourly DAM rows by
`timestamp`. The tracked config keeps
`oree_dam_publication_receipts_csv_path: ""`, which preserves the current
`data_acquisition_needed` result until a source-backed CSV is provided.

Receipt CSV requirements:

- required columns: `timestamp`, `source_publication_timestamp`;
- optional columns: `source_url`, `source_title`, `receipt_id`;
- timestamps use the same local/naive Kyiv ISO datetime convention as current
  OREE rows;
- missing, null, or duplicate receipt timestamps fail explicitly.

Validate and normalize a candidate CSV before pointing the V13 config at it:

```powershell
.\.venv\Scripts\python.exe scripts\validate_oree_dam_publication_receipts.py `
  --input data\external_sources\oree\dam_publication_receipts_raw.csv `
  --output data\external_sources\oree\dam_publication_receipts_v13.csv
```

Then set `oree_dam_publication_receipts_csv_path` to the normalized CSV path.
Do not commit generated raw/normalized receipt extracts unless the source file
is intentionally curated for the thesis evidence packet.

If a candidate source is the OREE `pricectr/data_view` endpoint itself, probe it
first and treat `not_sufficient_for_v13_receipts` as valid negative evidence:

```powershell
.\.venv\Scripts\python.exe scripts\probe_oree_dam_publication_receipts.py `
  --month 04.2026 `
  --output .tmp_runtime\oree_receipt_probe\oree_dam_publication_receipt_probe_2026-04.json
```

The 2026-05-24 probe of April 2026 returned HTTP retrieval metadata and a price
table, but no row-level publication metadata and no `Last-Modified` header; it
therefore must not be converted into receipt rows.

For repeatable source discovery across several months, use the batch audit
wrapper. It can combine existing single-month probe JSON files and/or live-probe
OREE month responses, then writes a source-audit JSON. This still does not
create receipt rows:

```powershell
.\.venv\Scripts\python.exe scripts\audit_oree_dam_publication_receipt_sources.py `
  --months 01.2026,02.2026,03.2026,04.2026,05.2026 `
  --probe-output-dir .tmp_runtime\oree_receipt_probe `
  --output .tmp_runtime\oree_receipt_probe\oree_dam_publication_receipt_source_audit_2026_q1_q2_may.json
```

`all_probes_insufficient_for_v13_receipts=true` means the checked OREE source
responses are negative evidence for the receipt blocker. `candidate_receipt_source_found=true`
only identifies a candidate receipt source for manual validation; it still does
not mark V13 ready until a validated CSV is supplied through
`oree_dam_publication_receipts_csv_path`.

The 2026-05-24 batch audit of `01.2026` through `05.2026` found
`all_probes_insufficient_for_v13_receipts=true` and
`candidate_receipt_source_found=false`; see
`docs/sources/oree-dam-publication-receipt-source-audit-2026-05-24.md`.
Attach that audit to the V13 packet with `--receipt-source-audit-json`; the
exporter copies it as `dfl_ua_context_v13_receipt_source_audit.json` and adds a
`receipt_source_audit_summary` block to the packet.

Even when receipts are supplied, this overlay is source-readiness evidence only:
`market_execution_enabled=false`, no `ProposedBid`, no dashboard/API default
switch, and no DT/LAVA start.

## Safe-Switch Example Backfill Input

`dfl_ua_context_safe_switch_examples_v13_frame` optionally loads incremental
source-backed safe-switch examples for tenant/source scopes that are below the
`20` prior/train example floor. The tracked config keeps
`ua_context_safe_switch_examples_csv_path: ""`, so V13 preserves the current
V12 counts (`2-7 / 20`) until curated backfill evidence is supplied.

Safe-switch CSV requirements:

- required columns: `tenant_id`, `source_model_name`, `anchor_timestamp`,
  `split_name`, `source_evidence_timestamp`,
  `label_v13_material_safe_switch`, `label_v13_tail_risk_loss`;
- optional columns: `source_url`, `source_title`, `receipt_id`;
- `split_name` must be `train_selection`;
- every row must be a non-tail-risk material safe-switch example:
  `label_v13_material_safe_switch=true` and
  `label_v13_tail_risk_loss=false`;
- duplicate `tenant_id` / `source_model_name` / `anchor_timestamp` rows fail;
- `market_execution_enabled=true` and raw hourly action imitation rows fail.

Validate and normalize a candidate safe-switch example CSV before pointing the
V13 config at it:

```powershell
.\.venv\Scripts\python.exe scripts\validate_ua_context_safe_switch_examples_v13.py `
  --input data\external_sources\v13\safe_switch_examples_raw.csv `
  --output data\external_sources\v13\safe_switch_examples_v13.csv
```

`dfl_ua_context_safe_switch_readiness_overlay_v13_frame` adds only the count of
validated incremental examples to the V13 acquisition precondition. It keeps
`dt_lava_ready=false`, `permits_model_training=false`, and
`market_execution_enabled=false`. Passing the count floor can only allow the
next V13 candidate-generation gate if every required source family, including
explicit DAM receipts, is also ready; it is not permission to train DT/LAVA.

## Acquisition Input Preflight

Before materializing V13 with candidate acquisition CSVs, run the config-level
preflight. Empty paths are reported as `missing_config_path` blockers; configured
paths are validated with the same normalizers used by the Dagster assets:

```powershell
.\.venv\Scripts\python.exe scripts\preflight_ua_context_v13_acquisition_inputs.py `
  --config configs\real_data_dfl_ua_context_v13_acquisition_week3.yaml `
  --output .tmp_runtime\v13_acquisition_inputs_preflight.json
```

The preflight is not the full V13 gate. It only proves whether the optional DAM
receipt and safe-switch example CSV inputs are present and schema-valid. Its
output keeps `v13_candidate_generation_ready=false`, `dt_lava_ready=false`,
`permits_model_training=false`, and `market_execution_enabled=false`.
When attached to the packet exporter with
`--acquisition-input-preflight-json`, it is copied as
`dfl_ua_context_v13_acquisition_input_preflight.json` and summarized in
`acquisition_input_preflight_summary`.

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

## Safe-Switch Support Deficit

The packet exporter now includes `safe_switch_deficit_summary`, derived from
`dfl_ua_context_acquisition_readiness_v13_frame`. It lists each tenant/source
pair below `min_prior_material_safe_switch_examples_for_dt`, the current
`prior_material_safe_switch_example_count`, and the missing example count. This
is the operational target for backfilling the DT/LAVA precondition; it is not a
permission to train DT/LAVA while any pair remains below `20`.

## Safe-Switch Acquisition Targets

The packet also writes
`dfl_ua_context_v13_safe_switch_acquisition_targets.csv`, derived from the same
readiness rows. This file translates the deficit into an acquisition backlog:

- `target_new_prior_material_safe_switch_examples` is the number of new
  train/prior non-tail-risk material safe-switch examples required for the
  tenant/source pair;
- `target_total_prior_material_safe_switch_examples` remains the configured
  threshold, currently `20`;
- `required_evidence_kind` is
  `train_prior_non_tail_risk_material_safe_switch_rows`;
- `blocking_context_families` preserves the V13 blocker list for the
  tenant/source pair;
- `primary_blocking_source_family` gives the first source family to unblock
  before DT/LAVA can use the row;
- `recommended_next_step` stays at source acquisition / safe-label backfill,
  not model promotion;
- `target_is_precondition_only=true` and `market_execution_enabled=false`.

Use this file to plan the data acquisition/backfill order. It does not promote
DT/LAVA by itself: explicit DAM receipts and every other V13 source family must
also clear before candidate generation or sequence-policy work starts.

## Source Acquisition Backlog

The packet also writes
`dfl_ua_context_v13_source_acquisition_backlog.csv`. This combines the V13
source-family blockers and the tenant/source safe-switch target rows into one
operator-facing acquisition checklist:

- `backlog_item_type=source_family_blocker` rows identify required source
  families that have not reached `ready_prior_context`;
- `backlog_item_type=safe_switch_target` rows identify tenant/source pairs that
  still need prior/train non-tail-risk material safe-switch examples;
- `blocking_source_family` aligns source blockers and safe-switch targets so the
  first blocker can be acquired before DT/LAVA work is resumed;
- `required_evidence_kind` and `acceptance_evidence` describe the evidence that
  must be supplied before the row can be marked complete;
- `permits_model_training=false` and `market_execution_enabled=false` make the
  backlog an acquisition artifact only.

The current exported V13 packet has `3` blocker classes in practice: explicit
DAM publication receipts, tenant/source safe-switch example deficits, and the
aggregate V12 safe-teacher support row. The exported local packet currently
shows `77` missing safe-switch examples and still keeps V13 at
`data_acquisition_needed`.

## Materialization

This selection assumes the upstream V2+/V10/V12 evidence assets and bronze
source assets are already materialized in the target Dagster instance. For a
fresh `DAGSTER_HOME`, materialize the documented upstream evidence path first;
do not use `+dfl_ua_context_acquisition_readiness_v13_frame` as a shortcut
because it expands across the broader historical DFL promotion graph.

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_ua_dam_publication_receipts_overlay_frame,dfl_ua_dam_publication_backfill_frame,dfl_ua_context_safe_switch_examples_v13_frame,dfl_ua_context_safe_switch_readiness_overlay_v13_frame,dfl_ua_context_acquisition_source_evidence_v13_frame,dfl_ua_context_source_inventory_v13_frame,dfl_ua_context_acquisition_readiness_v13_frame `
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

Refresh an already-exported local packet from CSV artifacts, for example after
packet-summary code changes:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_ua_context_v13_acquisition_packet.py `
  --source-evidence-csv data\research_runs\week3_dfl_ua_context_acquisition_v13\dfl_ua_context_v13_source_acquisition_evidence_rows.csv `
  --source-inventory-csv data\research_runs\week3_dfl_ua_context_acquisition_v13\dfl_ua_context_v13_source_inventory_rows.csv `
  --readiness-csv data\research_runs\week3_dfl_ua_context_acquisition_v13\dfl_ua_context_v13_readiness_rows.csv `
  --receipt-source-audit-json .tmp_runtime\oree_receipt_probe\oree_dam_publication_receipt_source_audit_2026_q1_q2_may.json `
  --acquisition-input-preflight-json .tmp_runtime\v13_acquisition_inputs_preflight.json `
  --run-slug week3_dfl_ua_context_acquisition_v13
```

## Claim Boundary

This slice is Offline Strategy Promotion evidence only. It does not enable live
dispatch, does not switch dashboard/API defaults, does not claim market
execution, and does not turn Poland/EU rows into Ukrainian target rows.
