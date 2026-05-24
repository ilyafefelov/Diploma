# OREE DAM Publication Receipt Source Audit - 2026-05-24

Purpose: record a repeatable multi-month source audit for the V13 requirement
for explicit row-level DAM publication receipts.

## Audit

Command:

```powershell
.\.venv\Scripts\python.exe scripts\audit_oree_dam_publication_receipt_sources.py `
  --months 01.2026,02.2026,03.2026,04.2026 `
  --probe-output-dir .tmp_runtime\oree_receipt_probe `
  --output .tmp_runtime\oree_receipt_probe\oree_dam_publication_receipt_source_audit_2026_q1_q2.json
```

Source URL probed for each month:

- `https://www.oree.com.ua/index.php/pricectr/data_view`

Observed result:

- `probe_count`: `4`
- `months_probed`: `01.2026`, `02.2026`, `03.2026`, `04.2026`
- `all_probes_insufficient_for_v13_receipts`: `true`
- `candidate_receipt_source_found`: `false`
- `candidate_receipt_months`: `[]`
- `insufficient_months`: `01.2026`, `02.2026`, `03.2026`, `04.2026`
- `receipt_csv_generated`: `false`
- `market_execution_enabled`: `false`

Interpretation: the audited OREE `data_view` responses remain negative source
evidence for the V13 receipt blocker. They provide DAM price tables and HTTP
retrieval metadata, but not row-level publication receipt metadata.

## Repo Usage

- Keep `oree_dam_publication_receipts_csv_path: ""` until a separate
  source-backed CSV with `timestamp` and `source_publication_timestamp` exists.
- Do not derive receipt rows from OREE market-rule timing, response `Date`
  headers, probe retrieval timestamps, or this audit summary.
- V13 remains an acquisition/readiness gate with no DT/LAVA start, no
  `ProposedBid`, and `market_execution_enabled=false`.
