from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

import polars as pl

from smart_arbitrage.dfl.ua_context_acquisition_v1 import (
    normalize_dfl_ua_dam_publication_receipts_frame,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Validate source-backed OREE DAM publication receipt CSV rows for "
            "the V13 acquisition gate."
        )
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    raw_frame = pl.read_csv(args.input, try_parse_dates=True)
    normalized = normalize_dfl_ua_dam_publication_receipts_frame(raw_frame)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        normalized.write_csv(args.output)

    json.dump(
        _summary(normalized, output_path=args.output),
        sys.stdout,
        indent=2,
        sort_keys=True,
    )
    sys.stdout.write("\n")


def _summary(frame: pl.DataFrame, *, output_path: Path | None) -> dict[str, Any]:
    return {
        "claim_boundary": "v13_source_readiness_only_not_market_execution",
        "first_timestamp": _iso_at(frame, "timestamp", 0),
        "last_timestamp": _iso_at(frame, "timestamp", -1),
        "market_execution_enabled": False,
        "normalized_receipts_csv": str(output_path) if output_path is not None else None,
        "receipt_rows": frame.height,
        "required_columns": ["timestamp", "source_publication_timestamp"],
    }


def _iso_at(frame: pl.DataFrame, column_name: str, index: int) -> str | None:
    if frame.is_empty():
        return None
    value = frame[column_name].item(index)
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


if __name__ == "__main__":
    main()
