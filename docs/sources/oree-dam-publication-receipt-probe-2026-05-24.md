# OREE DAM Publication Receipt Probe - 2026-05-24

Purpose: record whether the current OREE DAM `data_view` endpoint can satisfy
the V13 requirement for explicit row-level DAM publication receipts.

## Probe

Command:

```powershell
.\.venv\Scripts\python.exe scripts\probe_oree_dam_publication_receipts.py `
  --month 04.2026 `
  --output .tmp_runtime\oree_receipt_probe\oree_dam_publication_receipt_probe_2026-04.json
```

Source URL:

- `https://www.oree.com.ua/index.php/pricectr/data_view`

Observed result:

- `receipt_status`: `not_sufficient_for_v13_receipts`
- `row_level_publication_metadata_found`: `false`
- `http_last_modified_present`: `false`
- `http_date_header_is_retrieval_metadata_only`: `true`
- `market_execution_enabled`: `false`

Interpretation: the endpoint returned a DAM price table and HTTP retrieval
metadata, but not explicit row-level publication metadata. This is negative
source evidence for the V13 receipt blocker, not a receipt CSV.

## Repo Usage

- Keep `oree_dam_publication_receipts_csv_path: ""` until a source-backed CSV
  with `timestamp` and `source_publication_timestamp` exists.
- Do not derive receipt rows from OREE market-rule timing, response `Date`
  headers, or probe retrieval timestamps.
- V13 remains an acquisition/readiness gate with no DT/LAVA start, no
  `ProposedBid`, and `market_execution_enabled=false`.
