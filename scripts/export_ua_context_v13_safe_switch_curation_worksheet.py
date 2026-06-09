"""Export a source-review worksheet for V13 safe-switch curation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_safe_switch_curation import (
    build_dfl_ua_context_safe_switch_curation_worksheet_v13_frame,
    summarize_dfl_ua_context_safe_switch_curation_worksheet_v13_frame,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a non-promotional V13 safe-switch curation worksheet from "
            "the review backlog. Pending worksheet rows do not satisfy V13."
        )
    )
    parser.add_argument("--review-backlog-csv", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    args = parser.parse_args(argv)

    backlog = pl.read_csv(
        args.review_backlog_csv,
        infer_schema_length=0,
        try_parse_dates=False,
    )
    worksheet = build_dfl_ua_context_safe_switch_curation_worksheet_v13_frame(
        backlog
    )
    summary = summarize_dfl_ua_context_safe_switch_curation_worksheet_v13_frame(
        worksheet
    )
    summary.update(
        {
            "review_backlog_csv": str(args.review_backlog_csv),
            "curation_worksheet_csv": str(args.output_csv),
            "dt_lava_ready": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
        }
    )

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    worksheet.write_csv(args.output_csv)
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(
        json.dumps(_json_ready(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote V13 safe-switch curation worksheet: {args.output_csv}")
    print(f"Wrote V13 safe-switch curation worksheet summary: {args.summary_json}")
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
