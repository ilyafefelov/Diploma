"""Curate V13 safe-switch review rows into validator-ready evidence.

This module is an operator handoff layer. It does not promote DT/LAVA, train a
model, or create raw hourly action imitation rows.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_acquisition import (
    normalize_dfl_ua_context_safe_switch_examples_v13_frame,
)

CLAIM_SCOPE: Final[str] = "v13_safe_switch_curation_worksheet_not_training_data"
APPROVED_REVIEW_STATUS: Final[str] = "approved_source_backed_v13_safe_switch"
PENDING_REVIEW_STATUS: Final[str] = "pending_source_review"
TRAIN_SELECTION_SPLIT: Final[str] = "train_selection"

_REVIEW_BACKLOG_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "acquisition_priority_rank",
        "tenant_id",
        "source_model_name",
        "current_prior_material_safe_switch_examples",
        "required_prior_material_safe_switch_examples",
        "target_new_prior_material_safe_switch_examples",
        "review_rank_for_target",
        "anchor_timestamp",
        "split_name",
        "source_evidence_timestamp",
        "candidate_family",
        "candidate_model_name",
        "review_status",
        "required_review_action",
        "candidate_can_satisfy_v13_without_validation",
        "primary_blocking_source_family",
        "target_label_space",
        "permits_model_training",
        "market_execution_enabled",
    }
)
_WORKSHEET_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "split_name",
        "curator_review_status",
        "source_evidence_timestamp",
        "source_url",
        "source_evidence_id",
        "label_v13_material_safe_switch",
        "label_v13_tail_risk_loss",
        "candidate_can_satisfy_v13_without_validation",
        "permits_model_training",
        "market_execution_enabled",
    }
)


def build_dfl_ua_context_safe_switch_curation_worksheet_v13_frame(
    review_backlog_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build a human-review worksheet from non-promotional backlog rows."""

    if review_backlog_frame.height == 0:
        return _empty_worksheet_frame()
    _require_columns(
        review_backlog_frame,
        _REVIEW_BACKLOG_REQUIRED_COLUMNS,
        frame_name="V13 safe-switch review backlog frame",
    )
    _refuse_true(
        review_backlog_frame,
        "candidate_can_satisfy_v13_without_validation",
        frame_name="V13 safe-switch review backlog frame",
    )
    _refuse_true(
        review_backlog_frame,
        "permits_model_training",
        frame_name="V13 safe-switch review backlog frame",
    )
    _refuse_true(
        review_backlog_frame,
        "market_execution_enabled",
        frame_name="V13 safe-switch review backlog frame",
    )

    return review_backlog_frame.select(
        [
            pl.col("acquisition_priority_rank").cast(pl.Int64),
            pl.col("tenant_id").cast(pl.Utf8),
            pl.col("source_model_name").cast(pl.Utf8),
            pl.col("current_prior_material_safe_switch_examples").cast(pl.Int64),
            pl.col("required_prior_material_safe_switch_examples").cast(pl.Int64),
            pl.col("target_new_prior_material_safe_switch_examples").cast(pl.Int64),
            pl.col("review_rank_for_target").cast(pl.Int64),
            pl.col("anchor_timestamp").cast(pl.Utf8),
            pl.col("split_name").cast(pl.Utf8),
            pl.col("source_evidence_timestamp")
            .cast(pl.Utf8)
            .alias("candidate_source_evidence_timestamp"),
            _optional_string_column(review_backlog_frame, "candidate_family").alias(
                "candidate_family"
            ),
            _optional_string_column(review_backlog_frame, "candidate_model_name").alias(
                "candidate_model_name"
            ),
            _optional_string_column(review_backlog_frame, "material_label_column").alias(
                "material_label_column"
            ),
            _optional_string_column(review_backlog_frame, "tail_risk_label_column").alias(
                "tail_risk_label_column"
            ),
            _optional_string_column(
                review_backlog_frame,
                "source_evidence_timestamp_column",
            ).alias("source_evidence_timestamp_column"),
            _optional_bool_column(review_backlog_frame, "uses_canonical_v13_labels").alias(
                "uses_canonical_v13_labels"
            ),
            _optional_bool_column(review_backlog_frame, "material_candidate").alias(
                "material_candidate"
            ),
            _optional_bool_column(review_backlog_frame, "weak_safe_switch_win").alias(
                "weak_safe_switch_win"
            ),
            _optional_bool_column(review_backlog_frame, "tail_risk_candidate").alias(
                "tail_risk_candidate"
            ),
            _optional_float_column(
                review_backlog_frame,
                "label_regret_delta_vs_v2_plus_uah",
            ).alias("label_regret_delta_vs_v2_plus_uah"),
            pl.col("review_status").cast(pl.Utf8),
            pl.col("required_review_action").cast(pl.Utf8),
            pl.col("primary_blocking_source_family").cast(pl.Utf8),
            pl.col("target_label_space").cast(pl.Utf8),
            pl.lit(PENDING_REVIEW_STATUS).alias("curator_review_status"),
            pl.lit(None, dtype=pl.Utf8).alias("curator_id"),
            pl.lit(None, dtype=pl.Utf8).alias("curator_reviewed_at"),
            pl.lit(None, dtype=pl.Utf8).alias("source_evidence_timestamp"),
            pl.lit(None, dtype=pl.Utf8).alias("source_url"),
            pl.lit(None, dtype=pl.Utf8).alias("source_title"),
            pl.lit(None, dtype=pl.Utf8).alias("source_evidence_id"),
            pl.lit(None, dtype=pl.Boolean).alias("label_v13_material_safe_switch"),
            pl.lit(None, dtype=pl.Boolean).alias("label_v13_tail_risk_loss"),
            pl.lit(None, dtype=pl.Utf8).alias("curator_notes"),
            pl.lit(False).alias("ready_for_v13_safe_switch_validator"),
            pl.lit(False).alias("candidate_can_satisfy_v13_without_validation"),
            pl.lit(False).alias("dt_lava_ready"),
            pl.lit(False).alias("permits_model_training"),
            pl.lit(False).alias("market_execution_enabled"),
            pl.lit(CLAIM_SCOPE).alias("claim_scope"),
        ]
    ).sort(["acquisition_priority_rank", "review_rank_for_target"])


def summarize_dfl_ua_context_safe_switch_curation_worksheet_v13_frame(
    frame: pl.DataFrame,
) -> dict[str, Any]:
    """Summarize curation progress without promoting the rows."""

    if frame.height == 0:
        return {
            "claim_scope": CLAIM_SCOPE,
            "worksheet_rows": 0,
            "ready_for_v13_safe_switch_validator_rows": 0,
            "curated_safe_switch_examples_rows": 0,
            "target_tenant_source_count": 0,
            "dt_lava_ready": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
        }
    _validate_worksheet_frame(frame)
    scored = _scored_worksheet_frame(frame)
    ready = scored.filter(pl.col("_computed_ready_for_v13_safe_switch_validator"))
    return {
        "claim_scope": CLAIM_SCOPE,
        "worksheet_rows": frame.height,
        "ready_for_v13_safe_switch_validator_rows": ready.height,
        "curated_safe_switch_examples_rows": ready.height,
        "target_tenant_source_count": (
            frame.select(["tenant_id", "source_model_name"]).unique().height
        ),
        "approved_source_backed_rows": scored.filter(
            pl.col("curator_review_status") == APPROVED_REVIEW_STATUS
        ).height,
        "pending_source_review_rows": scored.filter(
            pl.col("curator_review_status") == PENDING_REVIEW_STATUS
        ).height,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }


def extract_dfl_ua_context_safe_switch_examples_from_curation_v13_frame(
    curation_worksheet_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Extract approved curation rows into the existing V13 validator contract."""

    if curation_worksheet_frame.height == 0:
        return normalize_dfl_ua_context_safe_switch_examples_v13_frame(pl.DataFrame())
    _validate_worksheet_frame(curation_worksheet_frame)
    scored = _scored_worksheet_frame(curation_worksheet_frame)
    approved = scored.filter(pl.col("curator_review_status") == APPROVED_REVIEW_STATUS)
    if approved.height == 0:
        return normalize_dfl_ua_context_safe_switch_examples_v13_frame(pl.DataFrame())
    invalid_approved = approved.filter(
        ~pl.col("_computed_ready_for_v13_safe_switch_validator")
    )
    if invalid_approved.height:
        raise ValueError(
            "Approved V13 safe-switch curation rows must include source evidence, "
            "train_selection split, and canonical non-tail-risk material labels."
        )

    canonical = approved.select(
        [
            pl.col("tenant_id").cast(pl.Utf8),
            pl.col("source_model_name").cast(pl.Utf8),
            pl.col("_anchor_timestamp").alias("anchor_timestamp"),
            pl.col("split_name").cast(pl.Utf8),
            pl.col("_source_evidence_timestamp").alias("source_evidence_timestamp"),
            pl.col("_label_v13_material_safe_switch").alias(
                "label_v13_material_safe_switch"
            ),
            pl.col("_label_v13_tail_risk_loss").alias("label_v13_tail_risk_loss"),
            pl.col("source_url").cast(pl.Utf8),
            pl.col("source_title").cast(pl.Utf8),
            pl.col("source_evidence_id").cast(pl.Utf8).alias("receipt_id"),
            pl.lit(False).alias("market_execution_enabled"),
        ]
    )
    return normalize_dfl_ua_context_safe_switch_examples_v13_frame(canonical)


def _validate_worksheet_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        _WORKSHEET_REQUIRED_COLUMNS,
        frame_name="V13 safe-switch curation worksheet frame",
    )
    _refuse_true(
        frame,
        "candidate_can_satisfy_v13_without_validation",
        frame_name="V13 safe-switch curation worksheet frame",
    )
    _refuse_true(
        frame,
        "permits_model_training",
        frame_name="V13 safe-switch curation worksheet frame",
    )
    _refuse_true(
        frame,
        "market_execution_enabled",
        frame_name="V13 safe-switch curation worksheet frame",
    )
    if "raw_hourly_action_imitation" in frame.columns:
        _refuse_true(
            frame,
            "raw_hourly_action_imitation",
            frame_name="V13 safe-switch curation worksheet frame",
        )


def _scored_worksheet_frame(frame: pl.DataFrame) -> pl.DataFrame:
    normalized = frame.with_columns(
        pl.col("anchor_timestamp")
        .map_elements(_datetime_value_or_none, return_dtype=pl.Datetime)
        .alias("_anchor_timestamp"),
        pl.col("source_evidence_timestamp")
        .map_elements(_datetime_value_or_none, return_dtype=pl.Datetime)
        .alias("_source_evidence_timestamp"),
        pl.col("label_v13_material_safe_switch")
        .map_elements(_bool_value_or_none, return_dtype=pl.Boolean)
        .alias("_label_v13_material_safe_switch"),
        pl.col("label_v13_tail_risk_loss")
        .map_elements(_bool_value_or_none, return_dtype=pl.Boolean)
        .alias("_label_v13_tail_risk_loss"),
        pl.col("curator_review_status").cast(pl.Utf8).alias("curator_review_status"),
        pl.col("split_name").cast(pl.Utf8).alias("split_name"),
        _string_column(frame, "source_url").alias("source_url"),
        _string_column(frame, "source_title").alias("source_title"),
        _string_column(frame, "source_evidence_id").alias("source_evidence_id"),
    )
    return normalized.with_columns(
        (
            (pl.col("curator_review_status") == APPROVED_REVIEW_STATUS)
            & (pl.col("split_name") == TRAIN_SELECTION_SPLIT)
            & pl.col("_anchor_timestamp").is_not_null()
            & pl.col("_source_evidence_timestamp").is_not_null()
            & (
                (_non_empty_string_expr("source_url"))
                | (_non_empty_string_expr("source_evidence_id"))
            )
            & pl.col("_label_v13_material_safe_switch").fill_null(False)
            & (~pl.col("_label_v13_tail_risk_loss").fill_null(True))
        ).alias("_computed_ready_for_v13_safe_switch_validator")
    )


def _empty_worksheet_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "acquisition_priority_rank": pl.Int64,
            "tenant_id": pl.Utf8,
            "source_model_name": pl.Utf8,
            "anchor_timestamp": pl.Utf8,
            "candidate_source_evidence_timestamp": pl.Utf8,
            "curator_review_status": pl.Utf8,
            "source_evidence_timestamp": pl.Utf8,
            "source_url": pl.Utf8,
            "source_title": pl.Utf8,
            "source_evidence_id": pl.Utf8,
            "label_v13_material_safe_switch": pl.Boolean,
            "label_v13_tail_risk_loss": pl.Boolean,
            "ready_for_v13_safe_switch_validator": pl.Boolean,
            "candidate_can_satisfy_v13_without_validation": pl.Boolean,
            "dt_lava_ready": pl.Boolean,
            "permits_model_training": pl.Boolean,
            "market_execution_enabled": pl.Boolean,
            "claim_scope": pl.Utf8,
        }
    )


def _require_columns(
    frame: pl.DataFrame,
    columns: frozenset[str],
    *,
    frame_name: str,
) -> None:
    missing = columns.difference(frame.columns)
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {sorted(missing)}")


def _refuse_true(frame: pl.DataFrame, column_name: str, *, frame_name: str) -> None:
    if column_name not in frame.columns or frame.is_empty():
        return
    normalized = frame.with_columns(
        pl.col(column_name)
        .map_elements(_bool_value, return_dtype=pl.Boolean)
        .alias(column_name)
    )
    if normalized.filter(pl.col(column_name)).height:
        if column_name == "market_execution_enabled":
            raise ValueError(f"{frame_name} contains market execution rows.")
        raise ValueError(f"{frame_name} contains {column_name}=true.")


def _optional_bool_column(frame: pl.DataFrame, column_name: str) -> pl.Expr:
    if column_name in frame.columns:
        return pl.col(column_name).map_elements(_bool_value, return_dtype=pl.Boolean)
    return pl.lit(False, dtype=pl.Boolean)


def _optional_float_column(frame: pl.DataFrame, column_name: str) -> pl.Expr:
    if column_name in frame.columns:
        return pl.col(column_name).cast(pl.Float64, strict=False)
    return pl.lit(None, dtype=pl.Float64)


def _optional_string_column(frame: pl.DataFrame, column_name: str) -> pl.Expr:
    if column_name in frame.columns:
        return pl.col(column_name).cast(pl.Utf8)
    return pl.lit(None, dtype=pl.Utf8)


def _string_column(frame: pl.DataFrame, column_name: str) -> pl.Expr:
    if column_name in frame.columns:
        return pl.col(column_name).fill_null("").cast(pl.Utf8).str.strip_chars()
    return pl.lit("", dtype=pl.Utf8)


def _non_empty_string_expr(column_name: str) -> pl.Expr:
    return pl.col(column_name).is_not_null() & (pl.col(column_name).str.len_chars() > 0)


def _datetime_value_or_none(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        return datetime.fromisoformat(stripped.replace("Z", "+00:00"))
    raise TypeError(f"Cannot convert {type(value).__name__} to datetime.")


def _bool_value(value: Any) -> bool:
    parsed = _bool_value_or_none(value)
    if parsed is None:
        raise TypeError(f"Cannot convert {type(value).__name__} to bool.")
    return parsed


def _bool_value_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in {0, 1}:
            return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
        if not normalized:
            return None
    raise TypeError(f"Cannot convert {type(value).__name__} to bool.")


__all__ = [
    "APPROVED_REVIEW_STATUS",
    "CLAIM_SCOPE",
    "PENDING_REVIEW_STATUS",
    "build_dfl_ua_context_safe_switch_curation_worksheet_v13_frame",
    "extract_dfl_ua_context_safe_switch_examples_from_curation_v13_frame",
    "summarize_dfl_ua_context_safe_switch_curation_worksheet_v13_frame",
]
