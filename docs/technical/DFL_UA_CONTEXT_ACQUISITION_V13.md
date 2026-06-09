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
- `dfl_v13_gated_dt_lava_teacher_contract_frame`

Config:

- `configs/real_data_dfl_ua_context_v13_acquisition_week3.yaml`

Packet exporter:

- `scripts/materialize_ua_context_v13_acquisition_packet.py`
- default output: `data/research_runs/week3_dfl_ua_context_acquisition_v13/`
- Phase 2 DT/LAVA teacher-dataset export:
  `scripts/materialize_v13_dt_lava_teacher_packet.py`

`dfl_v13_gated_dt_lava_teacher_contract_frame` is the handoff contract for the
later DT/LAVA lane. It consumes `dfl_ua_context_lava_sequence_training_frame`
and `dfl_ua_context_acquisition_readiness_v13_frame`, then marks each candidate
teacher row with V13 permission. The row contract is:

- DFL input: calibrated forecast context, tenant/SOC context, and feasible
  candidate schedules;
- DFL target: best candidate / schedule value / regret delta versus V2+;
- DT input: V13-passing teacher sequences with forecast, battery, tenant,
  candidate/value, and return-to-go fields;
- DT action target: candidate id or schedule family;
- V2+ role: teacher, comparator, and fallback.

When V13 is blocked, the asset preserves the target columns but sets
`permitted_model_training_row=false`, `permits_model_training=false`,
`promotion_gate_passed=false`, and `market_execution_enabled=false`.

After materializing `dfl_v13_gated_dt_lava_teacher_contract_frame`, export the
Phase 2 teacher dataset packet from the asset pickle:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_v13_dt_lava_teacher_packet.py `
  --teacher-contract-pickle .tmp_runtime\dt_lava_teacher\dfl_v13_gated_dt_lava_teacher_contract_frame.pkl `
  --output-root data\research_runs `
  --run-slug week3_v13_dt_lava_teacher_dataset
```

The packet writes `dfl_v13_dt_lava_teacher_summary.json`,
`dfl_v13_dt_lava_teacher_summary.md`, and
`dfl_v13_dt_lava_teacher_rows.csv`. It is a dataset contract only: candidate id
/ schedule-family targets, V2+ comparator/fallback role, feature-column groups,
and gate status. It does not train DT/LAVA, does not pass a promotion gate, does
not change dashboard/API defaults, and keeps `market_execution_enabled=false`.

## DAM/IDM Source Evidence Overlay

`dfl_ua_dam_publication_receipts_overlay_frame` optionally left-joins explicit
OREE DAM publication metadata onto observed hourly DAM rows by `timestamp`.
That asset remains the current DAM-side implementation hook, while the product
readiness claim is broader: explicit OREE DAM/IDM source/publication evidence
for preview. The tracked config keeps `oree_dam_publication_receipts_csv_path:
""`, which preserves the current `data_acquisition_needed` result until a
source-backed CSV is provided.

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

To audit the wider public OREE pages and exports without promoting them into
V13 readiness, run the candidate audit:

```powershell
.\.venv\Scripts\python.exe scripts\audit_oree_v13_receipt_candidates.py `
  --month 05.2026 `
  --delivery-date 2026-05-25 `
  --output-json .tmp_runtime\oree_receipt_probe\oree_v13_receipt_candidate_audit_2026-05-25.json `
  --output-csv .tmp_runtime\oree_receipt_probe\oree_v13_receipt_candidate_audit_2026-05-25.csv
```

This probes `eu_prices`, `IDM_graphs`, `pricectr`, `price_DAM_IDM_05.2026.xls`,
`indexes`, `control/results_mo/DAM`, `control/results_mo/IDM`, and the linked
PXS hdata/download endpoints. The audit classifies artifacts as `price_only`,
`observation_only`, `lead_only`, or `valid_receipt`. Only artifacts with an
explicit source-side `source_publication_timestamp`-style field plus delivery
timestamps can be `valid_receipt`; fetch `Date` headers, first-seen observations,
and XLS download timestamps remain non-receipt evidence.

If public OREE artifacts are observed but still lack row-level publication
timestamps, materialize weak policy-deadline evidence separately:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_oree_policy_publication_deadline_evidence.py `
  --candidate-audit-json .tmp_runtime\oree_receipt_probe\oree_v13_receipt_candidate_audit_2026-05-25.json `
  --output-csv .tmp_runtime\oree_receipt_probe\oree_policy_publication_deadline_evidence_2026-05-25.csv `
  --summary-json .tmp_runtime\oree_receipt_probe\oree_policy_publication_deadline_evidence_2026-05-25_summary.json
```

This computes `policy_publication_deadline_kyiv` from official OREE publication
rules and joins it with observed public OREE presence. It is governance/context
evidence only: `policy_publication_deadline_kyiv` is not
`source_publication_timestamp`, and the summary must keep
`can_satisfy_v13_explicit_receipts=false`,
`source_publication_timestamp_available=false`,
`validated_receipt_csv_ready=false`, `permits_model_training=false`, and
`market_execution_enabled=false`. If attached to the V13 packet, pass the
summary with `--policy-publication-evidence-json`; it does not unblock
`explicit_dam_publication_receipts`.

For external ecosystem leads such as alternate DAM dataset pages or download
APIs, audit the lead before attempting a receipt CSV conversion:

```powershell
.\.venv\Scripts\python.exe scripts\audit_v13_dam_receipt_source_leads.py `
  --input data\external_sources\oree\dam_receipt_source_leads.csv `
  --output .tmp_runtime\oree_receipt_probe\dam_receipt_source_lead_audit.json
```

This audit distinguishes row-level receipt candidates from acquisition leads
that are useful but insufficient. The audit treats dataset-level page metadata,
API catalog timestamps, and auth-blocked download endpoints as non-row-level DAM
publication receipts. A lead with explicit `timestamp` and
`source_publication_timestamp` columns may set
`candidate_receipt_source_found=true`, but the audit still keeps
`receipt_csv_generated=false` and `validated_receipt_csv_ready=false` until a
CSV passes `validate_oree_dam_publication_receipts.py` and is configured through
`oree_dam_publication_receipts_csv_path`. The current ecosystem lead audit is
recorded in `docs/sources/dam-receipt-source-lead-audit-2026-05-24.md`; it
keeps OREE `data_view` probes, OREE PXS DAM trading-result endpoints, Energy
Map dataset metadata, and subscription/download-limited Energy Map export paths
as acquisition leads only.

For repeatable Energy Map metadata discovery, use the metadata probe before
feeding leads into the same source-lead audit:

```powershell
.\.venv\Scripts\python.exe scripts\probe_energy_map_dam_receipt_metadata.py `
  --output-csv .tmp_runtime\oree_receipt_probe\energy_map_dam_receipt_metadata_leads_2026-05-24.csv `
  --summary-json .tmp_runtime\oree_receipt_probe\energy_map_dam_receipt_metadata_summary_2026-05-24.json
```

The 2026-05-24 live probe found `8` Energy Map DAM file-metadata leads across
the DAM trading-results and DAM indexes datasets. Those rows expose dataset
file update timestamps such as `2026-05-24T01:46:39.298Z`, but they are
classified as `file_level_publication_metadata_only`, not row-level OREE
publication receipts. The generated summary keeps `candidate_receipt_source_found=false`,
`receipt_csv_generated=false`, `validated_receipt_csv_ready=false`,
`permits_model_training=false`, and `market_execution_enabled=false`; see
`docs/sources/energy-map-dam-receipt-metadata-probe-2026-05-24.md`.

Attach a lead audit to the V13 packet with `--receipt-source-lead-audit-json`;
the exporter copies it as
`dfl_ua_context_v13_receipt_source_lead_audit.json` and adds
`receipt_source_lead_audit_summary` alongside the stricter
`receipt_source_audit_summary`.

For the official OREE PXS DAM trading-results lead, capture first-seen
observation evidence separately from publication receipts:

```powershell
.\.venv\Scripts\python.exe scripts\capture_oree_dam_publication_observations.py `
  --delivery-date 2026-05-25 `
  --output-csv .tmp_runtime\oree_receipt_probe\oree_dam_publication_observation_2026-05-25.csv `
  --summary-json .tmp_runtime\oree_receipt_probe\oree_dam_publication_observation_2026-05-25_summary.json `
  --attempt-log-json .tmp_runtime\oree_receipt_probe\oree_dam_publication_observation_2026-05-25_attempts.json `
  --max-attempts 1 `
  --sleep-seconds 0
```

The scraper can now poll the PXS results endpoint and write an attempt log even
when results are not published yet. A live 2026-05-25 capture for delivery date
2026-05-25 found `hdata_link=25.05.2026/DAM/2` on the first attempt and wrote
`24` row-level observation rows. A live first-seen poll for delivery date
2026-05-26 found no `hdata_link` yet and wrote an empty, schema-stable blocker
CSV with `source_probe_status=hdata_not_found`. Both outcomes keep
`publication_observation_status=observed_without_source_publication_timestamp`,
`can_satisfy_v13_explicit_receipts=false`,
`validated_receipt_csv_ready=false`, `permits_model_training=false`, and
`market_execution_enabled=false`; see
`docs/sources/oree-dam-publication-observation-capture-2026-05-24.md`.
Do not rename `source_observed_at_utc` to `source_publication_timestamp` or feed
this observation CSV into `oree_dam_publication_receipts_csv_path`.
The generic receipt validator now enforces that
`source_publication_timestamp < timestamp` and rejects OREE observation/download
metadata columns such as `source_observed_at_utc`, `download_http_date_utc`,
`hdata_http_date_utc`, `source_last_modified_utc`,
`workbook_summary_created_at`, and `workbook_summary_last_saved_at`.

For a deeper acquisition scrape, capture the linked PXS hdata response and XLS
download headers/hashes across one or more delivery dates:

```powershell
.\.venv\Scripts\python.exe scripts\scrape_oree_dam_download_observations.py `
  --from-date 2026-05-24 `
  --to-date 2026-05-25 `
  --output-csv .tmp_runtime\oree_receipt_probe\oree_dam_download_observations_2026-05-24_2026-05-25.csv `
  --summary-json .tmp_runtime\oree_receipt_probe\oree_dam_download_observations_2026-05-24_2026-05-25_summary.json `
  --download-dir .tmp_runtime\oree_receipt_probe\downloads
```

The live 2026-05-25 rerun for `2026-05-24` through `2026-05-25` captured `48`
hourly rows and `2` XLS downloads with SHA256 hashes, but OREE returned no
`Last-Modified` header and no explicit publication column, so
`candidate_receipt_source_found=false` and `validated_receipt_csv_ready=false`
remain correct.

A wider May scrape for `2026-05-01` through `2026-05-25` captured `600` hourly
rows and `25` XLS downloads after Windows-1251 decoding/retry handling. It also
found `last_modified_header_rows=0`, so it is acquisition evidence, not a
validated receipt CSV.

The XLS files do contain OLE SummaryInformation create/save timestamps, but a
stability probe showed those timestamps and file hashes change on repeated
downloads of the same `25.05.2026/DAM/2` file. Store that as negative receipt
evidence:

```powershell
.\.venv\Scripts\python.exe scripts\probe_oree_dam_xls_metadata_stability.py `
  --hdata-link 25.05.2026/DAM/2 `
  --samples 3 `
  --sleep-seconds 2 `
  --output .tmp_runtime\oree_receipt_probe\oree_dam_xls_metadata_stability_2026-05-25.json
```

The live probe produced `unique_sha256_count=3`,
`unique_workbook_created_at_count=3`, and
`workbook_metadata_generated_on_download=true`; these workbook metadata fields
must not be mapped to `source_publication_timestamp`.

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

Before converting a large candidate label export into a curated V13 CSV, audit
it first. The audit counts rows that look like material non-tail-risk
train-selection examples, but it still refuses to treat noncanonical V11/V12 or
generic `label_safe_switch_win` columns as ready V13 config input:

```powershell
.\.venv\Scripts\python.exe scripts\audit_ua_context_safe_switch_candidates_v13.py `
  --input data\research_runs\week3_dfl_ua_context_v12_safe_teacher_backfill\dfl_ua_v12_safe_teacher_label_rows.csv `
  --output .tmp_runtime\v13_safe_switch_candidate_audit\v12_material_label_audit.json `
  --material-label-column label_v12_material_safe_switch `
  --tail-risk-label-column label_v12_tail_risk_loss `
  --source-evidence-timestamp-column generated_at
```

The current audit of the V12 safe-teacher export found `348` weak
`label_safe_switch_win` diagnostics, but `0` canonical V12 material rows that
can be normalized directly as V13 safe-switch backfill evidence. A relaxed V11
material-label audit found only `19` investigative rows, with noncanonical label
columns and duplicate accepted-candidate anchors, so it also leaves
`normalized_safe_switch_csv_ready=false`, `permits_model_training=false`, and
`market_execution_enabled=false`.

To turn those investigative rows into an ordered manual curation queue, export a
safe-switch review backlog from the candidate label export and the V13
acquisition target CSV:

```powershell
.\.venv\Scripts\python.exe scripts\export_ua_context_v13_safe_switch_review_backlog.py `
  --candidate-rows-csv data\research_runs\week3_dfl_ua_context_v12_safe_teacher_backfill\dfl_ua_v12_safe_teacher_label_rows.csv `
  --acquisition-targets-csv data\research_runs\week3_dfl_ua_context_acquisition_v13\dfl_ua_context_v13_safe_switch_acquisition_targets.csv `
  --output-csv .tmp_runtime\v13_safe_switch_candidate_audit\v13_safe_switch_review_backlog_v11.csv `
  --summary-json .tmp_runtime\v13_safe_switch_candidate_audit\v13_safe_switch_review_backlog_v11_summary.json `
  --material-label-column label_v11_material_safe_switch `
  --tail-risk-label-column label_v11_tail_risk_loss `
  --source-evidence-timestamp-column generated_at
```

The current review backlog contains `77` rows, prioritizes
`client_004_kharkiv_hospital` with `18` review rows, and covers all `5`
tenant/source target pairs. It is still a curation queue only:
`candidate_can_satisfy_v13_without_validation=false`,
`permits_model_training=false`, and `market_execution_enabled=false`. Curated
rows must be rewritten into the canonical V13 safe-switch CSV contract and pass
`validate_ua_context_safe_switch_examples_v13.py` before they can be configured
through `ua_context_safe_switch_examples_csv_path`.

Convert the review backlog into a human-editable curation worksheet before
creating canonical rows. The worksheet deliberately clears
`source_evidence_timestamp`, `label_v13_material_safe_switch`, and
`label_v13_tail_risk_loss` so a curator must add source-backed evidence instead
of reusing weak or noncanonical diagnostic labels:

```powershell
.\.venv\Scripts\python.exe scripts\export_ua_context_v13_safe_switch_curation_worksheet.py `
  --review-backlog-csv .tmp_runtime\v13_safe_switch_candidate_audit\v13_safe_switch_review_backlog_v11.csv `
  --output-csv .tmp_runtime\v13_safe_switch_candidate_audit\v13_safe_switch_curation_worksheet_v11.csv `
  --summary-json .tmp_runtime\v13_safe_switch_candidate_audit\v13_safe_switch_curation_worksheet_v11_summary.json
```

Only rows reviewed as `approved_source_backed_v13_safe_switch`, with
`source_evidence_timestamp`, `source_url` or `source_evidence_id`,
`label_v13_material_safe_switch=true`, and
`label_v13_tail_risk_loss=false`, can be extracted into the validator contract:

```powershell
.\.venv\Scripts\python.exe scripts\extract_ua_context_v13_safe_switch_examples_from_curation.py `
  --curation-worksheet-csv data\external_sources\v13\safe_switch_curation_reviewed.csv `
  --output-csv data\external_sources\v13\safe_switch_examples_v13.csv `
  --summary-json data\external_sources\v13\safe_switch_examples_v13_summary.json
```

The extraction script writes normalized validator-compatible rows, but it still
does not configure the V13 gate, does not permit DT/LAVA training, and keeps
`market_execution_enabled=false`.

For the highest-priority deficit (`client_004_kharkiv_hospital`), attach the
scraped OREE DAM source observations to the curation rows before review:

```powershell
.\.venv\Scripts\python.exe scripts\attach_oree_observations_to_safe_switch_curation.py `
  --curation-worksheet-csv .tmp_runtime\v13_safe_switch_candidate_audit\v13_safe_switch_curation_worksheet_v11.csv `
  --oree-observations-csv .tmp_runtime\oree_receipt_probe\oree_dam_download_observations_client_004_priority_anchors.csv `
  --tenant-id client_004_kharkiv_hospital `
  --output-csv .tmp_runtime\v13_safe_switch_candidate_audit\v13_safe_switch_curation_worksheet_client_004_with_oree_observations.csv `
  --summary-json .tmp_runtime\v13_safe_switch_candidate_audit\v13_safe_switch_curation_worksheet_client_004_with_oree_observations_summary.json
```

The current attachment summary has `18` worksheet rows,
`18` DAM source-observation attachments, `0` missing observation rows, and
`0` validator-ready rows. This makes review faster but still does not approve
or backfill any safe-switch example by itself.

Attach safe-switch candidate audits to the V13 packet with repeatable
`--safe-switch-candidate-audit-json` flags. The exporter writes
`dfl_ua_context_v13_safe_switch_candidate_audits.json` and emits
`safe_switch_candidate_audit_summary`. This summary is investigative
source-discovery evidence only: it can count weak or noncanonical candidate
rows, but it does not satisfy `ua_context_safe_switch_examples_csv_path`, does
not permit DT/LAVA training, and does not change `market_execution_enabled=false`.

For the LAVA schedule-neighbor candidate-frame artifact, source-backed
safe-switch rows can be mined directly once matching OREE DAM download
observations exist:

```powershell
.\.venv\Scripts\python.exe scripts\backfill_v13_safe_switch_from_lava_candidates.py `
  --candidate-frame-pickle .tmp_runtime\dt_lava_prototype\dfl_lava_schedule_neighbor_candidate_frame.pkl `
  --oree-observations-csv .tmp_runtime\oree_receipt_probe\oree_dam_download_observations_v13_material_candidate_anchors.csv `
  --oree-observations-csv .tmp_runtime\oree_receipt_probe\oree_dam_download_observations_lava_backfill_batch1.csv `
  --acquisition-targets-csv data\research_runs\week3_dfl_ua_context_acquisition_v13\dfl_ua_context_v13_safe_switch_acquisition_targets.csv `
  --existing-safe-switch-csv .tmp_runtime\v13_safe_switch_candidate_audit\safe_switch_examples_material_candidates_v13_validated.csv `
  --output-csv .tmp_runtime\v13_safe_switch_candidate_audit\safe_switch_examples_lava_candidate_backfill_v13_batch1.csv `
  --summary-json .tmp_runtime\v13_safe_switch_candidate_audit\safe_switch_examples_lava_candidate_backfill_v13_batch1_summary.json
```

The extractor only accepts `train_selection` rows with
`label_regret_delta_vs_v2_plus_uah <= -25`, `safety_violation_count == 0`,
`eligible_for_final_selection=true`, and `market_execution_enabled=false`, and
only emits rows when an OREE observation provides `source_observed_at_utc`,
`download_url`, and `download_sha256` for the delivery date. The 2026-05-24
batch selected `58` additional rows; combined with the earlier `19` seeded
rows, the validated CSV
`data\external_sources\v13\safe_switch_examples_v13_combined_lava_backfill_validated.csv`
has `77` rows and projects every tenant/source to `20/20` prior material
safe-switch examples. This closes the safe-switch count blocker only; explicit
OREE DAM/IDM source/publication evidence for preview is still missing, so V13 remains
`data_acquisition_needed`, `permits_model_training=false`, and
`market_execution_enabled=false`.

`dfl_ua_context_safe_switch_readiness_overlay_v13_frame` adds only the count of
validated incremental examples to the V13 acquisition precondition. It keeps
`dt_lava_ready=false`, `permits_model_training=false`, and
`market_execution_enabled=false`. Passing the count floor can only allow the
next V13 candidate-generation gate if every required source family, including
explicit OREE DAM/IDM source/publication evidence for preview, is also ready;
it is not permission to train DT/LAVA.

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
The sanitized SCMO WS-Security credential preflight can also be attached with
`--scmo-ws-security-preflight-json`; it is copied as
`dfl_ua_context_v13_scmo_ws_security_preflight.json` and summarized in
`scmo_ws_security_preflight_summary`. That attachment is credential-source
blocker evidence only and does not satisfy explicit OREE DAM/IDM source/publication evidence for preview.

## What V13 Requires

V13 keeps the V12 Ukrainian-only source contract and adds explicit acquisition
requirements for the observed blockers:

- measured tenant load/PV telemetry or source-backed historical import;
- explicit OREE DAM/IDM source/publication evidence for preview, not only broad rules;
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
DT/LAVA by itself: explicit OREE DAM/IDM source/publication evidence for preview
and every other V13 source family must also clear before candidate generation or
sequence-policy work starts.
`scripts\export_ua_context_v13_safe_switch_review_backlog.py` can combine this
target file with a large candidate label export to produce an ordered review
queue, but that queue is not accepted as V13 evidence until a curator produces
canonical source-backed rows through
`scripts\export_ua_context_v13_safe_switch_curation_worksheet.py`,
`scripts\extract_ua_context_v13_safe_switch_examples_from_curation.py`, and the
validator.

## Source Acquisition Backlog

The packet also writes
`dfl_ua_context_v13_source_acquisition_backlog.csv`. This combines the V13
source-family blockers and the tenant/source safe-switch target rows into one
operator-facing acquisition checklist:

- `backlog_item_type=source_family_blocker` rows identify required source
  families that have not reached `ready_prior_context`;
- `backlog_item_type=receipt_source_lead` rows identify individual DAM receipt
  source leads from the lead audit, including `source_url`, `source_title`,
  `source_lead_id`, `source_lead_blocking_reasons`, and a lead-specific
  `recommended_next_step`;
- `backlog_item_type=safe_switch_target` rows identify tenant/source pairs that
  still need prior/train non-tail-risk material safe-switch examples;
- `blocking_source_family` aligns source blockers and safe-switch targets so the
  first blocker can be acquired before DT/LAVA work is resumed;
- `required_evidence_kind` and `acceptance_evidence` describe the evidence that
  must be supplied before the row can be marked complete;
- `permits_model_training=false` and `market_execution_enabled=false` make the
  backlog an acquisition artifact only.

The previously exported V13 packet had `4` blocker classes in practice:
explicit OREE DAM/IDM source/publication evidence for preview, receipt-source leads that still do not yield
a validated CSV, tenant/source safe-switch example deficits, and the aggregate
V12 safe-teacher support row. The current acquisition artifacts close the
safe-switch count precondition with `77` validated incremental rows, but the
config preflight still keeps V13 at `data_acquisition_needed` because
`oree_dam_publication_receipts_csv_path` is missing.

The receipt-source lead audit now includes the official SCMO XMtrade/PXS portal
as a credentialed lead. Probe it with:

```powershell
.\.venv\Scripts\python.exe scripts\probe_scmo_dam_publication_receipt_access.py `
  --probe-output-json .tmp_runtime\oree_receipt_probe\scmo_dam_publication_receipt_access_probe_2026-05-24.json `
  --lead-output-csv .tmp_runtime\oree_receipt_probe\scmo_dam_publication_receipt_source_lead_2026-05-24.csv
```

The 2026-05-24 unauthenticated probe redirected to
`login-scmo.oree.com.ua` and is classified as `auth_required_sso_login`.
This makes SCMO the likely credentialed export path for explicit OREE DAM/IDM
source/publication evidence for preview, but still leaves `candidate_receipt_source_found=false`,
`receipt_csv_generated=false`, `validated_receipt_csv_ready=false`, and
`market_execution_enabled=false` until an authenticated export with
`timestamp` and `source_publication_timestamp` is validated.

Probe the public SCMO SOAP WSDL contract to make the credentialed fetch target
precise before spending time on manual export work:

```powershell
.\.venv\Scripts\python.exe scripts\probe_scmo_dam_wsdl.py `
  --url https://scmo.oree.com.ua/interfaces/Evaluations/Service.svc?wsdl `
  --service-kind Evaluations `
  --probe-output-json .tmp_runtime\oree_receipt_probe\scmo_dam_wsdl_probe_evaluations_2026-05-25.json `
  --lead-output-csv .tmp_runtime\oree_receipt_probe\scmo_dam_wsdl_lead_evaluations_2026-05-25.csv
```

The 2026-05-25 live probe found the official `Download` operation, message
codes `807`, `810`, `831`, `934`, `941`, `951`, and `961`, and market areas
`UA_BEI` and `UA_IPS`. The WSDL also advertises WS-Security requirements such
as `AsymmetricBinding`, signed parts, `UsernameToken`, and `X509Token`. It is
still classified as
`wsdl_contract_available_download_requires_signed_or_authenticated_request`;
that is source-lead evidence only, not a validated receipt CSV.

Probe the unsigned SOAP `Download` request itself before assuming a public
download is possible:

```powershell
.\.venv\Scripts\python.exe scripts\probe_scmo_dam_soap_download.py `
  --url http://scmo.oree.com.ua/Interfaces/Evaluations/Service.svc `
  --soap-action http://sfera.sk/ws/xmtrade/isot/interfaces/evaluations/services/2009/04/01/EvaluationsContract/Download `
  --trade-day 2026-05-25 `
  --message-code 807 `
  --market-area UA_IPS `
  --probe-output-json .tmp_runtime\oree_receipt_probe\scmo_dam_soap_download_probe_2026-05-25.json `
  --lead-output-csv .tmp_runtime\oree_receipt_probe\scmo_dam_soap_download_lead_2026-05-25.csv `
  --request-output-xml .tmp_runtime\oree_receipt_probe\scmo_dam_soap_download_request_2026-05-25.xml `
  --raw-response-output .tmp_runtime\oree_receipt_probe\scmo_dam_soap_download_response_2026-05-25.xml
```

The 2026-05-25 unauthenticated SOAP attempt returned the WSDL/security contract
again, not a DAM data response. It is classified as
`wsdl_response_returned_signed_download_required`, with
`download_response_found=false`, `candidate_receipt_source_found=false`,
`validated_receipt_csv_ready=false`, and `market_execution_enabled=false`.
A follow-up sweep across all Evaluations `Download` request message codes
advertised by the WSDL (`807`, `810`, `831`, `934`, `941`, `951`, `961`) produced
the same result for each code: no `ISOTEDATA`, no candidate receipt CSV, and
`download_auth_required=true`. The sweep artifacts live under
`.tmp_runtime\oree_receipt_probe\scmo_message_code_sweep_2026-05-25\` and are
lead evidence only.

After the local credential preflight is ready, use the same probe in
`preflight-gated-mtls-username-token` mode before attempting any receipt
normalization:

```powershell
.\.venv\Scripts\python.exe scripts\probe_scmo_dam_soap_download.py `
  --credential-mode preflight-gated-mtls-username-token `
  --trade-day <YYYY-MM-DD> `
  --message-code 807 `
  --market-area UA_IPS `
  --probe-output-json .tmp_runtime\oree_receipt_probe\scmo_dam_soap_download_credentialed_probe_<date>.json `
  --lead-output-csv .tmp_runtime\oree_receipt_probe\scmo_dam_soap_download_credentialed_lead_<date>.csv `
  --request-output-xml .tmp_runtime\oree_receipt_probe\scmo_dam_soap_download_credentialed_request_sanitized_<date>.xml `
  --raw-response-output .tmp_runtime\oree_receipt_probe\scmo_dam_soap_download_credentialed_response_<date>.xml `
  --normalized-output data\external_sources\oree\dam_publication_receipts_v13.csv
```

This credentialed mode refuses to post if `credential_material_ready=false`,
writes sanitized request XML with UsernameToken secrets redacted, and records
`ws_security_signature_applied=false`. It is intentionally separated from the
stricter signed mode:

```powershell
.\.venv\Scripts\python.exe scripts\probe_scmo_dam_soap_download.py `
  --credential-mode preflight-gated-signed-ws-security `
  --trade-day <YYYY-MM-DD> `
  --message-code 807 `
  --market-area UA_IPS `
  --probe-output-json .tmp_runtime\oree_receipt_probe\scmo_dam_soap_download_signed_probe_<date>.json `
  --lead-output-csv .tmp_runtime\oree_receipt_probe\scmo_dam_soap_download_signed_lead_<date>.csv `
  --request-output-xml .tmp_runtime\oree_receipt_probe\scmo_dam_soap_download_signed_request_sanitized_<date>.xml
```

The signed mode now builds a WS-Security UsernameToken/X509/SignedParts request
locally and records `ws_security_signature_applied=true` only when that signed
request is actually posted. For signing material, it accepts either a separate
PEM pair (`SCMO_CLIENT_CERT_PEM` + `SCMO_CLIENT_KEY_PEM`) or a PKCS#12/PFX
bundle (`SCMO_CLIENT_P12` with optional `SCMO_CLIENT_P12_PASSWORD`). It still
refuses to post while `signed_download_request_ready=false`, which is the
current state on this machine because the SCMO username/password/cert material
is absent. If
`--normalized-output` is supplied to either mode, receipt writing is skipped for
blocker responses such as WSDL, faults, auth failures, network errors,
`credential_material_not_ready`, or `ws_security_signature_not_ready`. It writes
the receipt CSV only when the response contains a real candidate
`DownloadResponse`/`ISOTEDATA` that the existing normalizer validates into
`timestamp` and `source_publication_timestamp` rows. Even then it remains
source-readiness input evidence with `permits_model_training=false` and
`market_execution_enabled=false`; the V13 config/preflight and full assets still
have to pass separately.

Before attempting a signed SOAP request, preflight the required local
WS-Security material. The preflight writes only presence flags, SHA256 hashes for
certificate/key/P12 files, the credential material format, whether Python can
load the PEM pair or PKCS#12 bundle, and whether the repo can build a signed
WS-Security `SignedParts` request. It accepts optional
`SCMO_CLIENT_KEY_PASSWORD` for encrypted PEM keys and
`SCMO_CLIENT_P12_PASSWORD` for PKCS#12/PFX bundles, but only records presence.
It does not write username/password/key-password values, does not write secret values,
does not emit receipt rows, and does not permit DT/LAVA:

```powershell
.\.venv\Scripts\python.exe scripts\preflight_scmo_dam_ws_security_credentials.py `
  --output .tmp_runtime\oree_receipt_probe\scmo_ws_security_credential_preflight_latest.json
```

The current local preflight reports
`credential_material_ready=false` with missing `SCMO_USERNAME`,
`SCMO_PASSWORD`, `SCMO_CLIENT_CERT_PEM`, `SCMO_CLIENT_KEY_PEM`, and
`SCMO_CLIENT_P12`. That is a credential/acquisition blocker only:
`validated_receipt_csv_ready=false`, `permits_model_training=false`, and
`market_execution_enabled=false`.
If either a PEM pair or PKCS#12 bundle is present, `credential_material_ready=true`
is still held back until `credential_file_pair_valid=true`; this prevents
syntactically present but unusable credential material from being treated as
SCMO-ready.
The repo-side WS-Security XML signature builder is now available
(`ws_security_signature_status=xml_signature_builder_available`), so the strict
signed path is blocked by usable SCMO credential material and live SCMO
acceptance, not by missing local XML signature code.
Attach the latest JSON to V13 packet exports with
`--scmo-ws-security-preflight-json` so the credential blocker is visible next to
the receipt and acquisition-input blockers.

If an authenticated/manual SCMO export or signed SOAP `DownloadResponse` is
obtained, normalize it without renaming observation or retrieval timestamps into
receipt evidence. The normalizer accepts `csv`, generic `xml`, SCMO
`ISOTEDATA` SOAP XML, simple workbook `xlsx` exports, HTML table exports, and
one-file `zip` containers via `--input-format auto|csv|xml|xlsx|zip|html`; the output is still the canonical V13
receipt CSV:

```powershell
.\.venv\Scripts\python.exe scripts\normalize_scmo_dam_publication_receipt_export.py `
  --input data\external_sources\oree\scmo_dam_publication_export_raw.<csv|xml|xlsx|zip|html> `
  --input-format auto `
  --output data\external_sources\oree\dam_publication_receipts_v13.csv
```

The normalizer infers common delivery/publication columns such as
`delivery_hour` and `published_at`; use `--timestamp-column` and
`--source-publication-timestamp-column` only when the export uses nonstandard
headers. For authenticated workbook exports, Excel serial date/time cells are
converted to ISO datetimes before validation, so a real SCMO `.xlsx` can be
normalized without manual timestamp rewriting. For authenticated HTML table
exports, the first table must include explicit delivery and publication
timestamp headers; login/SSO HTML is not receipt evidence. For SCMO
`ISOTEDATA`, it maps
the message-level `date-time` attribute to `source_publication_timestamp`,
expands unique `Trade/Data` periods into hourly delivery timestamps, and
preserves `receipt_id` values such as
`scmo-isotedata:<message-code>:<trade-day>:<market-area>:<period>`. Ambiguous
publication timestamp columns fail explicitly. The normalizer writes the
existing V13 receipt schema and rejects `source_observed_at_utc`,
`download_http_date_utc`, `hdata_http_date_utc`, `retrieved_at`, and
`downloaded_at` as publication timestamps. It also requires
`source_publication_timestamp < timestamp`, keeps
`market_execution_enabled=false`, and does not permit DT/LAVA training until the
V13 preflight and materialized readiness gate pass with both receipt and
safe-switch CSV paths configured.

When a signed/authenticated SCMO response and the validated safe-switch CSV are
both available, the same normalizer can write the derived V13 input config and
preflight in one guarded command:

```powershell
.\.venv\Scripts\python.exe scripts\normalize_scmo_dam_publication_receipt_export.py `
  --input data\external_sources\oree\scmo_dam_publication_export_raw.xml `
  --input-format xml `
  --output data\external_sources\oree\dam_publication_receipts_v13.csv `
  --v13-base-config configs\real_data_dfl_ua_context_v13_acquisition_week3.yaml `
  --v13-safe-switch-csv data\external_sources\v13\safe_switch_examples_v13_combined_lava_backfill_validated.csv `
  --v13-output-config .tmp_runtime\v13_acquisition_inputs\v13_with_validated_external_inputs.yaml `
  --v13-preflight-output .tmp_runtime\v13_acquisition_inputs\v13_with_validated_external_inputs_preflight.json
```

This only validates and wires input files. Its summary still reports
`full_v13_gate_evaluated=false`, `v13_candidate_generation_ready=false`,
`permits_model_training=false`, and `market_execution_enabled=false`; the full
V13 assets must still be materialized and checked before any DT/LAVA training
claim.

For a direct authenticated fetch, keep the SCMO session cookie outside the repo
and pass it through the environment:

```powershell
$env:SCMO_COOKIE="<browser-session-cookie>"
.\.venv\Scripts\python.exe scripts\fetch_scmo_dam_publication_receipt_export.py `
  --url <authenticated-scmo-export-url> `
  --raw-output .tmp_runtime\oree_receipt_probe\scmo_dam_publication_export_raw.<csv|xml|xlsx|zip|html> `
  --input-format auto `
  --normalized-output data\external_sources\oree\dam_publication_receipts_v13.csv `
  --summary-json .tmp_runtime\oree_receipt_probe\scmo_dam_publication_export_fetch_summary.json `
  --v13-base-config configs\real_data_dfl_ua_context_v13_acquisition_week3.yaml `
  --v13-safe-switch-csv data\external_sources\v13\safe_switch_examples_v13_combined_lava_backfill_validated.csv `
  --v13-output-config .tmp_runtime\v13_acquisition_inputs\v13_inputs_with_scmo_receipts.yaml `
  --v13-preflight-output .tmp_runtime\v13_acquisition_inputs\v13_inputs_with_scmo_receipts_preflight.json
```

The fetcher accepts authenticated CSV/XML/XLSX/ZIP/HTML table export bodies and
rejects SSO/login HTML before writing V13 receipts. When the
optional V13 config flags are supplied, it also writes the derived run config
and preflight summary after receipt normalization; this is still input
validation only with `full_v13_gate_evaluated=false`. A live unauthenticated
2026-05-25 call to `https://scmo.oree.com.ua/` raised
`auth_required_sso_login`, which is the correct blocked result and not a receipt
artifact.

After both canonical CSVs exist, build a temporary input config from the tracked
V13 config rather than editing the tracked defaults:

```powershell
.\.venv\Scripts\python.exe scripts\build_v13_acquisition_input_config.py `
  --base-config configs\real_data_dfl_ua_context_v13_acquisition_week3.yaml `
  --dam-receipts-csv data\external_sources\oree\dam_publication_receipts_v13.csv `
  --safe-switch-csv data\external_sources\v13\safe_switch_examples_v13_combined_lava_backfill_validated.csv `
  --output-config .tmp_runtime\v13_acquisition_inputs\v13_with_validated_external_inputs.yaml `
  --preflight-output .tmp_runtime\v13_acquisition_inputs\v13_with_validated_external_inputs_preflight.json
```

The builder validates both CSVs before writing the derived config and then runs
the existing preflight. A clean preflight only proves input presence/schema; it
still reports `full_v13_gate_evaluated=false`,
`v13_candidate_generation_ready=false`, `permits_model_training=false`, and
`market_execution_enabled=false`.

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
  --receipt-source-lead-audit-json .tmp_runtime\oree_receipt_probe\dam_receipt_source_lead_audit_2026-05-24.json `
  --safe-switch-candidate-audit-json .tmp_runtime\v13_safe_switch_candidate_audit\v12_material_label_audit.json `
  --safe-switch-candidate-audit-json .tmp_runtime\v13_safe_switch_candidate_audit\v11_material_label_audit.json `
  --acquisition-input-preflight-json .tmp_runtime\v13_acquisition_inputs_preflight.json `
  --scmo-ws-security-preflight-json .tmp_runtime\oree_receipt_probe\scmo_ws_security_credential_preflight_latest.json `
  --run-slug week3_dfl_ua_context_acquisition_v13
```

## Claim Boundary

This slice is Offline Strategy Promotion evidence only. It does not enable live
dispatch, does not switch dashboard/API defaults, does not claim market
execution, and does not turn Poland/EU rows into Ukrainian target rows.
