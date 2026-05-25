"""Audit DAM receipt source-discovery leads.

This does not emit receipt rows.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_receipt_lead_audit import (
    audit_dfl_ua_context_dam_receipt_source_leads_v13_frame,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit candidate DAM publication receipt source leads before V13 "
            "receipt backfill. This writes lead evidence only; it does not "
            "emit receipt rows and does not permit DT/LAVA training or market "
            "execution."
        )
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    frame = pl.read_csv(args.input, try_parse_dates=False)
    audit = audit_dfl_ua_context_dam_receipt_source_leads_v13_frame(frame)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote V13 DAM receipt source-lead audit: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
