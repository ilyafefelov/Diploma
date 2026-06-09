# V13 F3 Acquisition Sprint - 2026-05-26

Purpose: refresh the V13 acquisition state after the full-project review without
crossing the source-readiness boundary. This is evidence for the blocker state,
not candidate generation, DT/LAVA training, or market execution.

## Verdict

V13 remains blocked by explicit DAM publication receipts. The safe-switch
support lane now has local validated evidence that reaches the `20` prior/train
non-tail-risk material examples floor for all five tenant/source pairs, but that
does not unlock V13 while DAM receipts are missing.

Boundary flags remain unchanged:

- `market_execution_enabled=false`
- `dt_lava_ready=false`
- `permits_model_training=false`
- `v13_candidate_generation_ready=false`

## Safe-Switch Lane

Validated input:

- `data/external_sources/v13/safe_switch_examples_v13_combined_lava_backfill_validated.csv`
- SHA256: `BED0201CF07618C1BA1CAC6D8770C9DD0C68B636D113075C474874E13481B39B`
- Validator output: `77` rows, `5` tenant/source pairs, first anchor `2025-04-22T23:00:00`, last anchor `2026-02-10T23:00:00`

Per tenant/source incremental rows from the validated CSV:

| Tenant | Incremental validated rows |
|---|---:|
| `client_001_kyiv_mall` | 16 |
| `client_002_lviv_office` | 13 |
| `client_003_dnipro_factory` | 16 |
| `client_004_kharkiv_hospital` | 18 |
| `client_005_odesa_hotel` | 14 |

Combined with the V12 prior counts, the refreshed packet reports:

- max prior material safe-switch examples: `20 / 20`
- blocked tenant/source pairs from safe-switch deficit: `0`
- missing prior material safe-switch examples: `0`

## DAM Receipt Lane

Refreshed OREE `data_view` audit:

- artifact: `data/research_runs/week3_dfl_ua_context_acquisition_v13_2026_05_26/dfl_ua_context_v13_receipt_source_audit.json`
- SHA256: `445962D43F3F0C737AE26AC94D6E4B9F82F6507143CC4C4DE0926F8EA6D422D9`
- months probed: `01.2026`, `02.2026`, `03.2026`, `04.2026`, `05.2026`
- `candidate_receipt_source_found=false`
- `receipt_csv_generated=false`

Refreshed OREE PXS observation for delivery date `2026-05-26` found 24 hourly
observation rows, but the summary explicitly marks them
`observed_without_source_publication_timestamp` and `can_satisfy_v13_explicit_receipts=false`.
These rows are source observation evidence only and must not be converted into
`source_publication_timestamp`.

SCMO WS-Security preflight:

- artifact: `data/research_runs/week3_dfl_ua_context_acquisition_v13_2026_05_26/dfl_ua_context_v13_scmo_ws_security_preflight.json`
- SHA256: `2CE5C1FCA0DFE1283C3E59D5BF753222B48879524473E1162801CD62EB270211`
- `credential_material_ready=false`
- `signed_download_request_ready=false`
- missing: `SCMO_USERNAME`, `SCMO_PASSWORD`, `SCMO_CLIENT_CERT_PEM`, `SCMO_CLIENT_KEY_PEM`, `SCMO_CLIENT_P12`

The 2026-05-25 SCMO SOAP message-code sweep remains a row-level lead only:
all tested message codes are blocked by signed/authenticated download
requirements and no candidate receipt CSV was generated.

## Refreshed Packet

Packet:

- `data/research_runs/week3_dfl_ua_context_acquisition_v13_2026_05_26/dfl_ua_context_v13_acquisition_summary.json`
- SHA256: `FBBB3D313D7DD99BB83D47EF757B0CB60FF97C1AF81534D7BEA1832941B9D1CC`
- preflight artifact SHA256: `805C526C10DDE8A4A3A5DED3984BF729CD79DF8A2955A1F0C507BB083F748420`

Current packet status:

- safe-switch CSV input status: `validated`
- DAM receipt CSV input status: `missing_config_path`
- missing required configured inputs: `oree_dam_publication_receipts_csv_path`
- ready rows: `0`
- blocked rows: `5`
- top priority blocker: `explicit_dam_publication_receipts`

## Commands Run

```powershell
.\.venv\Scripts\python.exe scripts\validate_ua_context_safe_switch_examples_v13.py --input data\external_sources\v13\safe_switch_examples_v13_combined_lava_backfill_validated.csv --output .tmp_runtime\v13_safe_switch_examples_2026-05-26_validated.csv
.\.venv\Scripts\python.exe scripts\audit_oree_dam_publication_receipt_sources.py --months 01.2026,02.2026,03.2026,04.2026,05.2026 --probe-output-dir .tmp_runtime\oree_receipt_probe --output .tmp_runtime\oree_receipt_probe\oree_dam_publication_receipt_source_audit_2026_q1_q2_may_2026-05-26_refresh.json
.\.venv\Scripts\python.exe scripts\capture_oree_dam_publication_observations.py --delivery-date 2026-05-26 --output-csv .tmp_runtime\oree_receipt_probe\oree_dam_publication_observation_2026-05-26_refresh.csv --summary-json .tmp_runtime\oree_receipt_probe\oree_dam_publication_observation_2026-05-26_refresh_summary.json --attempt-log-json .tmp_runtime\oree_receipt_probe\oree_dam_publication_observation_2026-05-26_refresh_attempts.json --max-attempts 1 --sleep-seconds 0
.\.venv\Scripts\python.exe scripts\preflight_scmo_dam_ws_security_credentials.py --output .tmp_runtime\oree_receipt_probe\scmo_ws_security_credential_preflight_2026-05-26_refresh.json
.\.venv\Scripts\python.exe scripts\build_v13_acquisition_input_config.py --base-config configs\real_data_dfl_ua_context_v13_acquisition_week3.yaml --safe-switch-csv data\external_sources\v13\safe_switch_examples_v13_combined_lava_backfill_validated.csv --output-config .tmp_runtime\v13_acquisition_inputs\v13_with_validated_safe_switch_no_receipts_2026-05-26.yaml --preflight-output .tmp_runtime\v13_acquisition_inputs\v13_with_validated_safe_switch_no_receipts_2026-05-26_preflight.json
.\.venv\Scripts\python.exe scripts\materialize_ua_context_v13_acquisition_packet.py `
  --source-evidence-csv data\research_runs\week3_dfl_ua_context_acquisition_v13_safe_switch_only\dfl_ua_context_v13_source_acquisition_evidence_rows.csv `
  --source-inventory-csv data\research_runs\week3_dfl_ua_context_acquisition_v13_safe_switch_only\dfl_ua_context_v13_source_inventory_rows.csv `
  --readiness-csv data\research_runs\week3_dfl_ua_context_acquisition_v13_safe_switch_only\dfl_ua_context_v13_readiness_rows.csv `
  --receipt-source-audit-json .tmp_runtime\oree_receipt_probe\oree_dam_publication_receipt_source_audit_2026_q1_q2_may_2026-05-26_refresh.json `
  --receipt-source-lead-audit-json .tmp_runtime\oree_receipt_probe\scmo_message_code_sweep_2026-05-25\scmo_dam_soap_download_lead_audit_all_codes.json `
  --acquisition-input-preflight-json .tmp_runtime\v13_acquisition_inputs\v13_with_validated_safe_switch_no_receipts_2026-05-26_preflight.json `
  --scmo-ws-security-preflight-json .tmp_runtime\oree_receipt_probe\scmo_ws_security_credential_preflight_2026-05-26_refresh.json `
  --output-root data\research_runs `
  --run-slug week3_dfl_ua_context_acquisition_v13_2026_05_26 `
  --materialization-command "local V13 acquisition packet refresh with validated safe-switch input and refreshed OREE/SCMO receipt blockers; no DAM receipt CSV configured" `
  --asset-check-status blocked_v13_explicit_dam_publication_receipts
```

## Next Action

Acquire a source-backed OREE/SCMO DAM receipt export that contains both
`timestamp` and `source_publication_timestamp`. The most concrete path is to
provide SCMO WS-Security credential material, rerun the signed SOAP download
probe/fetch path, normalize the export, and validate it with
`scripts/validate_oree_dam_publication_receipts.py`.
