# DAM Receipt Source Lead Audit - 2026-05-24

Purpose: record ecosystem source leads for the V13 explicit DAM publication
receipt blocker without treating leads as validated receipt rows.

## Lead Inputs

Curated lead rows:
`docs/sources/dam-receipt-source-leads-2026-05-24.csv`.

Audit command:

```powershell
.\.venv\Scripts\python.exe scripts\audit_v13_dam_receipt_source_leads.py `
  --input docs\sources\dam-receipt-source-leads-2026-05-24.csv `
  --output .tmp_runtime\oree_receipt_probe\dam_receipt_source_lead_audit_2026-05-24.json
```

## Findings

- OREE `pricectr/data_view` remains negative for V13 receipts because the
  monthly probes found price rows and retrieval metadata, not row-level
  `source_publication_timestamp` values.
- OREE `control/results_mo/DAM` / PXS endpoints expose official row-level DAM
  trading results, including a 2026-05-24 live check for delivery date
  2026-05-25, but the public response and XLS export do not expose the original
  source publication timestamp column required by V13.
- Energy Map's DAM indexes dataset is useful for DAM history discovery and
  cross-checking, but its public page exposes dataset-level metadata, including
  page update metadata, not row-level publication receipts.
- Energy Map's current DAM trading-results dataset/API
  (`5a616fba-fbc9-4073-9532-9161592faca8`) reports dataset-level freshness
  (`2026-05-24T01:47:00`) and points back to OREE as source, but its download
  API is auth-gated and the public dataset metadata is not row-level
  publication-receipt evidence.
- The repeatable Energy Map metadata probe in
  `docs/sources/energy-map-dam-receipt-metadata-probe-2026-05-24.md` found `8`
  file-metadata leads across the DAM trading-results and DAM indexes datasets.
  These leads include file update timestamps, but they are classified as
  `file_level_publication_metadata_only` and keep
  `candidate_receipt_source_found=false`.
- The Energy Map download paths are source-acquisition leads, but the public
  page/API shows subscription/download-limit or token constraints. They are not
  usable as a V13 receipt CSV until an accessible export is validated with
  `validate_oree_dam_publication_receipts.py`.
- The SCMO XMtrade/PXS portal is now tracked as the most relevant official
  credentialed lead. A live 2026-05-24 probe of `https://scmo.oree.com.ua/`
  redirected to `https://login-scmo.oree.com.ua/login?...` and was classified
  as `auth_required_sso_login`. The public OREE manual page for XMtrade/PXS
  remains useful source-discovery evidence for the existence of the DAM
  published-information export, but without an authenticated export it is not a
  validated V13 receipt CSV.

## Current Audit Summary

- `lead_count=7`
- `candidate_receipt_source_found=false`
- `dataset_level_metadata_only_count=2`
- `auth_blocked_count=3`
- `probe_negative_count=1`
- `missing_required_receipt_column_count=1`
- `receipt_csv_generated=false`
- `validated_receipt_csv_ready=false`
- `permits_model_training=false`
- `market_execution_enabled=false`

## Source Links

- OREE data view: `https://www.oree.com.ua/index.php/pricectr/data_view`
- OREE DAM trading results: `https://www.oree.com.ua/index.php/control/results_mo/DAM`
- Energy Map DAM indexes dataset:
  `https://energy-map.info/en/datasets/c6218b35-ce7e-45c2-925e-5c8e6f5eb9fb`
- Energy Map DAM trading-results dataset:
  `https://energy-map.info/en/datasets/5a616fba-fbc9-4073-9532-9161592faca8`
- OREE DAM/IDM rules source:
  `https://www.oree.com.ua/index.php/web/13245784`
- SCMO XMtrade/PXS portal:
  `https://scmo.oree.com.ua/`
- OREE XMtrade/PXS manual:
  `https://www.oree.com.ua/index.php/web/7006?lang=english`

## Claim Boundary

This audit improves acquisition targeting only. It does not satisfy the V13
explicit DAM publication receipt source family, does not start DT/LAVA, does not
permit model training, and does not enable market execution.
