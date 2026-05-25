# Energy Map DAM Receipt Metadata Probe - 2026-05-24

Purpose: turn Energy Map DAM dataset metadata into repeatable V13 receipt source
leads without treating file-level metadata as row-level OREE publication
receipts.

## Command

```powershell
.\.venv\Scripts\python.exe scripts\probe_energy_map_dam_receipt_metadata.py `
  --output-csv .tmp_runtime\oree_receipt_probe\energy_map_dam_receipt_metadata_leads_2026-05-24.csv `
  --summary-json .tmp_runtime\oree_receipt_probe\energy_map_dam_receipt_metadata_summary_2026-05-24.json
```

Audit the generated lead CSV with:

```powershell
.\.venv\Scripts\python.exe scripts\audit_v13_dam_receipt_source_leads.py `
  --input .tmp_runtime\oree_receipt_probe\energy_map_dam_receipt_metadata_leads_2026-05-24.csv `
  --output .tmp_runtime\oree_receipt_probe\energy_map_dam_receipt_metadata_lead_audit_2026-05-24.json
```

## Findings

- Dataset metadata endpoint:
  `https://energy-map.info/apis/v1/datasets/<uuid>/?locale=en`.
- DAM trading-results dataset:
  `https://energy-map.info/en/datasets/5a616fba-fbc9-4073-9532-9161592faca8`.
- DAM indexes dataset:
  `https://energy-map.info/en/datasets/c6218b35-ce7e-45c2-925e-5c8e6f5eb9fb`.
- The probe produced `8` file-metadata lead rows across the two datasets.
- File metadata includes dataset file update timestamps, for example
  `2026-05-24T01:46:39.298Z` for the English DAM trading-results CSV.
- The file schemas include DAM timestamp/date fields but do not expose
  `source_publication_timestamp`.

## Current Summary

- `lead_rows=8`
- `dataset_count=2`
- `dataset_level_metadata_only_count=8`
- `candidate_receipt_source_found=false`
- `receipt_csv_generated=false`
- `validated_receipt_csv_ready=false`
- `permits_model_training=false`
- `market_execution_enabled=false`

## Claim Boundary

Energy Map file update timestamps are useful acquisition leads, but they are
`file_level_publication_metadata_only`. They do not satisfy the V13 explicit DAM
publication receipt blocker, do not permit DT/LAVA training, and do not enable
market execution.
