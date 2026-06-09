# OREE DAM Publication Observation Capture - 2026-05-24

Purpose: record that the official OREE PXS DAM results endpoint can be captured
as first-seen observation evidence without treating the capture timestamp as a
source-provided publication receipt.

## Capture Command

```powershell
.\.venv\Scripts\python.exe scripts\capture_oree_dam_publication_observations.py `
  --delivery-date 2026-05-25 `
  --output-csv .tmp_runtime\oree_receipt_probe\oree_dam_publication_observation_2026-05-25.csv `
  --summary-json .tmp_runtime\oree_receipt_probe\oree_dam_publication_observation_2026-05-25_summary.json
```

## Result

- `observation_rows=24`
- `first_timestamp=2026-05-25T00:00:00`
- `last_timestamp=2026-05-25T23:00:00`
- `publication_observation_status=observed_without_source_publication_timestamp`
- `can_satisfy_v13_explicit_receipts=false`
- `receipt_csv_generated=false`
- `validated_receipt_csv_ready=false`
- `permits_model_training=false`
- `market_execution_enabled=false`

## Source

- OREE DAM trading results:
  `https://www.oree.com.ua/index.php/control/results_mo/DAM`

## Claim Boundary

This capture is useful for future daily acquisition monitoring and for proving
that OREE PXS results are publicly observable. It does not provide the original
row-level `source_publication_timestamp`, does not satisfy
`oree_dam_publication_receipts_csv_path`, does not permit DT/LAVA training, and
does not enable market execution.
