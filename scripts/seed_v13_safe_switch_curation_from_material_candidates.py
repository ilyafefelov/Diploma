"""Seed V13 safe-switch curation rows from already-material source-backed candidates."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Sequence

import polars as pl

APPROVED_STATUS = "approved_source_backed_v13_safe_switch"
CURATOR_ID = "automated_material_candidate_seed_v1"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Approve only already-material, non-tail-risk V13 safe-switch "
            "curation rows that have attached OREE source observations. Weak "
            "safe-switch rows remain pending review."
        )
    )
    parser.add_argument("--input-csv", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    parser.add_argument("--tenant-id", default="")
    args = parser.parse_args(argv)

    frame = pl.read_csv(args.input_csv, infer_schema_length=0)
    if args.tenant_id.strip():
        frame = frame.filter(pl.col("tenant_id") == args.tenant_id.strip())
    seeded = _seed_material_candidates(frame)
    summary = _summary(
        seeded,
        input_csv=args.input_csv,
        output_csv=args.output_csv,
        tenant_id=args.tenant_id.strip() or None,
    )

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    seeded.write_csv(args.output_csv)
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote seeded V13 safe-switch curation worksheet: {args.output_csv}")
    print(f"Wrote seeded V13 safe-switch curation summary: {args.summary_json}")
    return 0


def _seed_material_candidates(frame: pl.DataFrame) -> pl.DataFrame:
    _require_columns(
        frame,
        {
            "material_candidate",
            "tail_risk_candidate",
            "dam_source_observation_attached",
            "candidate_source_evidence_timestamp",
            "dam_source_download_url",
            "dam_source_download_sha256",
            "anchor_timestamp",
            "permits_model_training",
            "market_execution_enabled",
        },
    )
    _refuse_true(frame, "permits_model_training")
    _refuse_true(frame, "market_execution_enabled")
    reviewed_at = datetime.now(UTC).isoformat()
    eligible = (
        _bool_expr("material_candidate")
        & (~_bool_expr("tail_risk_candidate"))
        & _bool_expr("dam_source_observation_attached")
        & pl.col("candidate_source_evidence_timestamp").is_not_null()
        & (pl.col("candidate_source_evidence_timestamp").cast(pl.Utf8).str.len_chars() > 0)
        & pl.col("dam_source_download_url").is_not_null()
        & (pl.col("dam_source_download_url").cast(pl.Utf8).str.len_chars() > 0)
        & pl.col("dam_source_download_sha256").is_not_null()
        & (pl.col("dam_source_download_sha256").cast(pl.Utf8).str.len_chars() > 0)
    )
    return frame.with_columns(
        pl.when(eligible)
        .then(pl.lit(APPROVED_STATUS))
        .otherwise(pl.col("curator_review_status"))
        .alias("curator_review_status"),
        pl.when(eligible)
        .then(pl.lit(CURATOR_ID))
        .otherwise(pl.col("curator_id"))
        .alias("curator_id"),
        pl.when(eligible)
        .then(pl.lit(reviewed_at))
        .otherwise(pl.col("curator_reviewed_at"))
        .alias("curator_reviewed_at"),
        pl.when(eligible)
        .then(pl.col("candidate_source_evidence_timestamp"))
        .otherwise(pl.col("source_evidence_timestamp"))
        .alias("source_evidence_timestamp"),
        pl.when(eligible)
        .then(pl.col("dam_source_download_url"))
        .otherwise(pl.col("source_url"))
        .alias("source_url"),
        pl.when(eligible)
        .then(pl.lit("OREE PXS DAM observation plus material safe-switch audit"))
        .otherwise(pl.col("source_title"))
        .alias("source_title"),
        pl.when(eligible)
        .then(
            pl.concat_str(
                [
                    pl.lit("oree-safe-switch:"),
                    pl.col("anchor_timestamp").cast(pl.Utf8),
                    pl.lit(":"),
                    pl.col("dam_source_download_sha256").cast(pl.Utf8).str.slice(0, 16),
                ]
            )
        )
        .otherwise(pl.col("source_evidence_id"))
        .alias("source_evidence_id"),
        pl.when(eligible)
        .then(pl.lit(True))
        .otherwise(pl.col("label_v13_material_safe_switch"))
        .alias("label_v13_material_safe_switch"),
        pl.when(eligible)
        .then(pl.lit(False))
        .otherwise(pl.col("label_v13_tail_risk_loss"))
        .alias("label_v13_tail_risk_loss"),
        pl.when(eligible)
        .then(
            pl.lit(
                "Seeded from an already-material, non-tail-risk candidate with "
                "attached OREE DAM source observation; weak rows remain pending review."
            )
        )
        .otherwise(pl.col("curator_notes"))
        .alias("curator_notes"),
        pl.when(eligible)
        .then(pl.lit(True))
        .otherwise(pl.col("ready_for_v13_safe_switch_validator"))
        .alias("ready_for_v13_safe_switch_validator"),
        pl.lit(False).alias("dt_lava_ready"),
        pl.lit(False).alias("permits_model_training"),
        pl.lit(False).alias("market_execution_enabled"),
    )


def _summary(
    frame: pl.DataFrame,
    *,
    input_csv: Path,
    output_csv: Path,
    tenant_id: str | None,
) -> dict[str, Any]:
    approved = frame.filter(pl.col("curator_review_status") == APPROVED_STATUS)
    return {
        "claim_scope": "v13_safe_switch_material_candidate_seed_not_model_training",
        "input_csv": str(input_csv),
        "output_csv": str(output_csv),
        "tenant_id": tenant_id,
        "worksheet_rows": frame.height,
        "seeded_approved_rows": approved.height,
        "pending_source_review_rows": frame.height - approved.height,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }


def _require_columns(frame: pl.DataFrame, columns: set[str]) -> None:
    missing = columns.difference(frame.columns)
    if missing:
        raise ValueError(f"input curation worksheet is missing columns: {sorted(missing)}")


def _refuse_true(frame: pl.DataFrame, column_name: str) -> None:
    if frame.with_columns(_bool_expr(column_name).alias(column_name)).filter(
        pl.col(column_name)
    ).height:
        raise ValueError(f"input curation worksheet contains {column_name}=true.")


def _bool_expr(column_name: str) -> pl.Expr:
    return pl.col(column_name).map_elements(_bool_value, return_dtype=pl.Boolean)


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
