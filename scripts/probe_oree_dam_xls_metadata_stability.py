"""Probe whether OREE DAM XLS metadata is stable or generated at download time."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Sequence

import httpx

from smart_arbitrage.dfl.oree_dam_download_observations import (
    extract_oree_dam_xls_summary_metadata,
)

OREE_DAM_RESULTS_URL = "https://www.oree.com.ua/index.php/control/results_mo/DAM"
OREE_PXS_DOWNLOAD_URL_PREFIX = "https://www.oree.com.ua/index.php/PXS/downloadxlsx"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Download the same OREE DAM XLS several times and classify whether "
            "OLE SummaryInformation timestamps are stable source metadata or "
            "generated-on-download metadata. This never emits V13 receipt rows."
        )
    )
    parser.add_argument("--hdata-link", required=True, help="Example: 25.05.2026/DAM/2")
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--sleep-seconds", type=float, default=2.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.samples < 2:
        raise ValueError("--samples must be at least 2.")
    rows: list[dict[str, Any]] = []
    with httpx.Client(
        timeout=45.0,
        follow_redirects=True,
        headers={
            "Referer": OREE_DAM_RESULTS_URL,
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
        },
    ) as client:
        for sample_index in range(args.samples):
            response = client.get(f"{OREE_PXS_DOWNLOAD_URL_PREFIX}/{args.hdata_link}")
            response.raise_for_status()
            metadata = extract_oree_dam_xls_summary_metadata(response.content)
            rows.append(
                {
                    "sample_index": sample_index,
                    "retrieved_at_utc": datetime.now(UTC).isoformat(),
                    "http_date": response.headers.get("date"),
                    "content_type": response.headers.get("content-type"),
                    "content_disposition": response.headers.get("content-disposition"),
                    "content_length": len(response.content),
                    "sha256": hashlib.sha256(response.content).hexdigest(),
                    **metadata,
                }
            )
            if sample_index < args.samples - 1:
                time.sleep(args.sleep_seconds)
    hashes = {str(row["sha256"]) for row in rows}
    created_times = {
        str(row["workbook_summary_created_at"])
        for row in rows
        if row["workbook_summary_created_at"]
    }
    saved_times = {
        str(row["workbook_summary_last_saved_at"])
        for row in rows
        if row["workbook_summary_last_saved_at"]
    }
    generated_on_download = len(hashes) > 1 or len(created_times) > 1 or len(saved_times) > 1
    summary = {
        "claim_scope": "oree_dam_xls_metadata_stability_not_v13_receipt",
        "hdata_link": args.hdata_link,
        "sample_count": len(rows),
        "unique_sha256_count": len(hashes),
        "unique_workbook_created_at_count": len(created_times),
        "unique_workbook_last_saved_at_count": len(saved_times),
        "workbook_metadata_generated_on_download": generated_on_download,
        "can_satisfy_v13_explicit_receipts": False,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "samples": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote OREE DAM XLS metadata stability probe: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
