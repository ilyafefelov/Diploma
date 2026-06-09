"""Audit candidate rows before using them as V13 safe-switch backfill evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_safe_switch_audit import (
    DEFAULT_MATERIAL_LABEL_COLUMN,
    DEFAULT_MIN_SAFE_SWITCH_EXAMPLES,
    DEFAULT_SOURCE_EVIDENCE_TIMESTAMP_COLUMN,
    DEFAULT_TAIL_RISK_LABEL_COLUMN,
    audit_dfl_ua_context_safe_switch_candidate_source_v13_frame,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit a candidate CSV before converting any rows into V13 "
            "safe-switch backfill evidence. This does not permit DT/LAVA "
            "training or market execution."
        )
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--material-label-column",
        default=DEFAULT_MATERIAL_LABEL_COLUMN,
    )
    parser.add_argument(
        "--tail-risk-label-column",
        default=DEFAULT_TAIL_RISK_LABEL_COLUMN,
    )
    parser.add_argument(
        "--source-evidence-timestamp-column",
        default=DEFAULT_SOURCE_EVIDENCE_TIMESTAMP_COLUMN,
    )
    parser.add_argument(
        "--min-prior-material-safe-switch-examples-for-dt",
        type=int,
        default=DEFAULT_MIN_SAFE_SWITCH_EXAMPLES,
    )
    args = parser.parse_args(argv)

    frame = pl.read_csv(args.input, try_parse_dates=False)
    audit = audit_dfl_ua_context_safe_switch_candidate_source_v13_frame(
        frame,
        material_label_column=args.material_label_column,
        tail_risk_label_column=args.tail_risk_label_column,
        source_evidence_timestamp_column=args.source_evidence_timestamp_column,
        min_prior_material_safe_switch_examples_for_dt=(
            args.min_prior_material_safe_switch_examples_for_dt
        ),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote V13 safe-switch candidate audit: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
