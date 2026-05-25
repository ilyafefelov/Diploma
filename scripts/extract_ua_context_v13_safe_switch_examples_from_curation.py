"""Extract approved curation rows into the V13 safe-switch CSV contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_safe_switch_curation import (
    extract_dfl_ua_context_safe_switch_examples_from_curation_v13_frame,
    summarize_dfl_ua_context_safe_switch_curation_worksheet_v13_frame,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Extract approved source-backed curation rows into a normalized "
            "V13 safe-switch CSV. This does not configure the V13 gate, train "
            "DT/LAVA, or permit market execution."
        )
    )
    parser.add_argument("--curation-worksheet-csv", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    args = parser.parse_args(argv)

    worksheet = pl.read_csv(
        args.curation_worksheet_csv,
        infer_schema_length=0,
        try_parse_dates=False,
    )
    examples = extract_dfl_ua_context_safe_switch_examples_from_curation_v13_frame(
        worksheet
    )
    summary = summarize_dfl_ua_context_safe_switch_curation_worksheet_v13_frame(
        worksheet
    )
    summary.update(
        {
            "curation_worksheet_csv": str(args.curation_worksheet_csv),
            "normalized_safe_switch_examples_csv": str(args.output_csv),
            "curated_safe_switch_examples_rows": examples.height,
            "dt_lava_ready": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
        }
    )

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    examples.write_csv(args.output_csv)
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(
        json.dumps(_json_ready(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote normalized V13 safe-switch examples: {args.output_csv}")
    print(f"Wrote normalized V13 safe-switch examples summary: {args.summary_json}")
    return 0


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


if __name__ == "__main__":
    raise SystemExit(main())
