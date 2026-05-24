from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_acquisition import (
    normalize_dfl_ua_context_safe_switch_examples_v13_frame,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Validate source-backed V13 safe-switch example CSV rows for the "
            "Ukrainian context acquisition gate."
        )
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    raw_frame = pl.read_csv(args.input, try_parse_dates=True)
    normalized = normalize_dfl_ua_context_safe_switch_examples_v13_frame(raw_frame)
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
    tenant_source_count = (
        frame.select(["tenant_id", "source_model_name"]).unique().height
        if frame.height
        else 0
    )
    return {
        "claim_boundary": "v13_source_readiness_only_not_market_execution",
        "dt_lava_ready": False,
        "first_anchor_timestamp": _iso_at(frame, "anchor_timestamp", 0),
        "last_anchor_timestamp": _iso_at(frame, "anchor_timestamp", -1),
        "market_execution_enabled": False,
        "normalized_safe_switch_examples_csv": (
            str(output_path) if output_path is not None else None
        ),
        "permits_model_training": False,
        "required_columns": [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "split_name",
            "source_evidence_timestamp",
            "label_v13_material_safe_switch",
            "label_v13_tail_risk_loss",
        ],
        "safe_switch_example_rows": frame.height,
        "tenant_source_count": tenant_source_count,
    }


def _iso_at(frame: pl.DataFrame, column_name: str, index: int) -> str | None:
    if frame.is_empty():
        return None
    value = frame[column_name].item(index)
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


if __name__ == "__main__":
    main()
