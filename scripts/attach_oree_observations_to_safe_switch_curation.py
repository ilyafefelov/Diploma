"""Attach OREE DAM source observations to a V13 safe-switch curation worksheet."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

import polars as pl


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Left-join OREE DAM download observations onto pending V13 "
            "safe-switch curation rows by anchor timestamp. This adds source "
            "context only; it does not approve rows, train DT/LAVA, or permit "
            "market execution."
        )
    )
    parser.add_argument("--curation-worksheet-csv", type=Path, required=True)
    parser.add_argument("--oree-observations-csv", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    parser.add_argument("--tenant-id", default="")
    args = parser.parse_args(argv)

    worksheet = pl.read_csv(args.curation_worksheet_csv, infer_schema_length=0)
    observations = pl.read_csv(args.oree_observations_csv, infer_schema_length=0)
    if args.tenant_id.strip():
        worksheet = worksheet.filter(pl.col("tenant_id") == args.tenant_id.strip())
    augmented = _attach_observations(worksheet, observations)
    summary = _summary(
        augmented,
        curation_worksheet_csv=args.curation_worksheet_csv,
        oree_observations_csv=args.oree_observations_csv,
        output_csv=args.output_csv,
    )

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    augmented.write_csv(args.output_csv)
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote OREE-augmented safe-switch worksheet: {args.output_csv}")
    print(f"Wrote OREE-augmented safe-switch worksheet summary: {args.summary_json}")
    return 0


def _attach_observations(
    worksheet: pl.DataFrame,
    observations: pl.DataFrame,
) -> pl.DataFrame:
    _require_columns(
        worksheet,
        {
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "curator_review_status",
            "permits_model_training",
            "market_execution_enabled",
        },
        frame_name="curation worksheet",
    )
    _require_columns(
        observations,
        {
            "timestamp",
            "price_uah_mwh",
            "volume_mwh",
            "download_url",
            "download_sha256",
            "source_observed_at_utc",
            "receipt_candidate_status",
            "validated_receipt_csv_ready",
            "permits_model_training",
            "market_execution_enabled",
        },
        frame_name="OREE observation frame",
    )
    _refuse_true(worksheet, "permits_model_training", frame_name="curation worksheet")
    _refuse_true(worksheet, "market_execution_enabled", frame_name="curation worksheet")
    _refuse_true(
        observations,
        "validated_receipt_csv_ready",
        frame_name="OREE observation frame",
    )
    _refuse_true(observations, "permits_model_training", frame_name="OREE observation frame")
    _refuse_true(observations, "market_execution_enabled", frame_name="OREE observation frame")

    normalized_worksheet = worksheet.with_columns(
        pl.col("anchor_timestamp")
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace_all(".000000", "", literal=True)
        .alias("_join_timestamp")
    )
    observation_slice = observations.with_columns(
        pl.col("timestamp")
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace_all(".000000", "", literal=True)
        .alias("_join_timestamp")
    ).select(
        [
            "_join_timestamp",
            pl.col("price_uah_mwh").alias("dam_source_price_uah_mwh"),
            pl.col("volume_mwh").alias("dam_source_volume_mwh"),
            pl.col("download_url").alias("dam_source_download_url"),
            pl.col("download_sha256").alias("dam_source_download_sha256"),
            pl.col("source_observed_at_utc").alias("dam_source_observed_at_utc"),
            pl.col("receipt_candidate_status").alias("dam_receipt_candidate_status"),
            _optional_string_column(observations, "workbook_summary_created_at").alias(
                "dam_workbook_summary_created_at"
            ),
            _optional_string_column(observations, "workbook_summary_last_saved_at").alias(
                "dam_workbook_summary_last_saved_at"
            ),
            _optional_string_column(
                observations,
                "workbook_summary_filetime_status",
            ).alias("dam_workbook_summary_filetime_status"),
        ]
    )
    joined = normalized_worksheet.join(observation_slice, on="_join_timestamp", how="left")
    return joined.with_columns(
        pl.col("dam_source_download_sha256")
        .is_not_null()
        .alias("dam_source_observation_attached"),
        pl.lit(False).alias("ready_for_v13_safe_switch_validator"),
        pl.lit(False).alias("dt_lava_ready"),
        pl.lit(False).alias("permits_model_training"),
        pl.lit(False).alias("market_execution_enabled"),
    ).drop("_join_timestamp")


def _summary(
    frame: pl.DataFrame,
    *,
    curation_worksheet_csv: Path,
    oree_observations_csv: Path,
    output_csv: Path,
) -> dict[str, Any]:
    attached_rows = frame.filter(pl.col("dam_source_observation_attached")).height
    return {
        "claim_scope": "v13_safe_switch_curation_oree_observation_attachment_not_training_data",
        "curation_worksheet_csv": str(curation_worksheet_csv),
        "oree_observations_csv": str(oree_observations_csv),
        "output_csv": str(output_csv),
        "worksheet_rows": frame.height,
        "dam_source_observation_attached_rows": attached_rows,
        "dam_source_observation_missing_rows": frame.height - attached_rows,
        "ready_for_v13_safe_switch_validator_rows": 0,
        "curated_safe_switch_examples_rows": 0,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }


def _require_columns(
    frame: pl.DataFrame,
    columns: set[str],
    *,
    frame_name: str,
) -> None:
    missing = columns.difference(frame.columns)
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {sorted(missing)}")


def _refuse_true(frame: pl.DataFrame, column_name: str, *, frame_name: str) -> None:
    if frame.with_columns(
        pl.col(column_name)
        .map_elements(_bool_value, return_dtype=pl.Boolean)
        .alias(column_name)
    ).filter(pl.col(column_name)).height:
        raise ValueError(f"{frame_name} contains {column_name}=true.")


def _optional_string_column(frame: pl.DataFrame, column_name: str) -> pl.Expr:
    if column_name in frame.columns:
        return pl.col(column_name).cast(pl.Utf8)
    return pl.lit("", dtype=pl.Utf8)


def _bool_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no", ""}:
        return False
    raise TypeError(f"Cannot convert {type(value).__name__} to bool.")


if __name__ == "__main__":
    raise SystemExit(main())
