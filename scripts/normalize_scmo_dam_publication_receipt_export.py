"""Normalize authenticated SCMO DAM export rows into V13 receipt CSV schema."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

import polars as pl

from smart_arbitrage.dfl.scmo_dam_receipt_export import (
    DEFAULT_SCMO_SOURCE_TITLE,
    DEFAULT_SCMO_SOURCE_URL,
    SCMO_AUTO_COLUMN,
    SCMO_DAM_RECEIPT_EXPORT_CLAIM_SCOPE,
    normalize_scmo_dam_publication_receipt_export_frame,
    read_scmo_dam_receipt_export_path,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize an authenticated/manual SCMO DAM publication export into "
            "the explicit V13 receipt CSV schema. This requires a real source "
            "publication timestamp column and refuses retrieval/observation "
            "timestamps."
        )
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--input-format",
        choices=["auto", "csv", "xml", "xlsx", "zip", "html"],
        default="auto",
        help="SCMO export format. Defaults to auto detection from file name/content.",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timestamp-column", default=SCMO_AUTO_COLUMN)
    parser.add_argument(
        "--source-publication-timestamp-column",
        default=SCMO_AUTO_COLUMN,
    )
    parser.add_argument("--receipt-id-column", default=None)
    parser.add_argument("--source-url", default=DEFAULT_SCMO_SOURCE_URL)
    parser.add_argument("--source-title", default=DEFAULT_SCMO_SOURCE_TITLE)
    parser.add_argument(
        "--v13-base-config",
        type=Path,
        default=None,
        help="Optional V13 base Dagster config to copy and wire with this receipt CSV.",
    )
    parser.add_argument(
        "--v13-safe-switch-csv",
        type=Path,
        default=None,
        help="Optional already-validated V13 safe-switch CSV for the derived config.",
    )
    parser.add_argument(
        "--v13-output-config",
        type=Path,
        default=None,
        help="Optional derived V13 config output path.",
    )
    parser.add_argument(
        "--v13-preflight-output",
        type=Path,
        default=None,
        help="Optional preflight JSON output for the derived V13 config.",
    )
    args = parser.parse_args(argv)

    raw = read_scmo_dam_receipt_export_path(
        args.input,
        input_format=args.input_format,
    )
    normalized = normalize_scmo_dam_publication_receipt_export_frame(
        raw,
        timestamp_column=args.timestamp_column,
        source_publication_timestamp_column=args.source_publication_timestamp_column,
        receipt_id_column=args.receipt_id_column,
        source_url=args.source_url,
        source_title=args.source_title,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    normalized.write_csv(args.output)
    v13_input_config_summary = _maybe_write_v13_input_config(args, args.output)
    summary = _summary(
        normalized,
        input_path=args.input,
        output_path=args.output,
        v13_input_config_summary=v13_input_config_summary,
    )
    summary_path = args.output.with_suffix(".summary.json")
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def _summary(
    frame: pl.DataFrame,
    *,
    input_path: Path,
    output_path: Path,
    v13_input_config_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = {
        "claim_scope": SCMO_DAM_RECEIPT_EXPORT_CLAIM_SCOPE,
        "claim_boundary": "v13_source_readiness_only_not_market_execution",
        "input_path": str(input_path),
        "normalized_receipts_csv": str(output_path),
        "receipt_rows": frame.height,
        "first_timestamp": _iso_at(frame, "timestamp", 0),
        "last_timestamp": _iso_at(frame, "timestamp", -1),
        "validated_receipt_csv_ready": frame.height > 0,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }
    if v13_input_config_summary is not None:
        summary["v13_input_config_summary"] = v13_input_config_summary
    return summary


def _maybe_write_v13_input_config(
    args: argparse.Namespace,
    receipt_csv_path: Path,
) -> dict[str, Any] | None:
    requested = any(
        value is not None
        for value in (
            args.v13_base_config,
            args.v13_safe_switch_csv,
            args.v13_output_config,
            args.v13_preflight_output,
        )
    )
    if not requested:
        return None
    if args.v13_base_config is None or args.v13_output_config is None:
        raise ValueError(
            "--v13-base-config and --v13-output-config are required when "
            "requesting V13 config/preflight output."
        )

    from scripts.build_v13_acquisition_input_config import (
        build_v13_acquisition_input_config,
    )

    return build_v13_acquisition_input_config(
        base_config_path=args.v13_base_config,
        dam_receipts_csv_path=receipt_csv_path,
        safe_switch_csv_path=args.v13_safe_switch_csv,
        output_config_path=args.v13_output_config,
        preflight_output_path=args.v13_preflight_output,
    )


def _iso_at(frame: pl.DataFrame, column_name: str, index: int) -> str | None:
    if frame.is_empty():
        return None
    value = frame[column_name].item(index)
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


if __name__ == "__main__":
    raise SystemExit(main())
