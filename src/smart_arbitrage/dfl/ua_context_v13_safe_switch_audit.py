"""Audit candidate safe-switch rows before V13 backfill validation.

This module is deliberately stricter than a generic safe-win counter. V13 can
only use source-backed train/prior non-tail-risk material safe-switch examples;
weaker diagnostic labels remain useful evidence, but they do not satisfy the
DT/LAVA data floor.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Final

import polars as pl

CLAIM_SCOPE: Final[str] = "v13_safe_switch_candidate_source_audit_not_market_execution"
TRAIN_SELECTION_SPLIT: Final[str] = "train_selection"
DEFAULT_MATERIAL_LABEL_COLUMN: Final[str] = "label_v13_material_safe_switch"
DEFAULT_TAIL_RISK_LABEL_COLUMN: Final[str] = "label_v13_tail_risk_loss"
DEFAULT_SOURCE_EVIDENCE_TIMESTAMP_COLUMN: Final[str] = "source_evidence_timestamp"
DEFAULT_MIN_SAFE_SWITCH_EXAMPLES: Final[int] = 20

_IDENTITY_COLUMNS: Final[tuple[str, ...]] = (
    "tenant_id",
    "source_model_name",
    "anchor_timestamp",
    "split_name",
)


def audit_dfl_ua_context_safe_switch_candidate_source_v13_frame(
    frame: pl.DataFrame,
    *,
    material_label_column: str = DEFAULT_MATERIAL_LABEL_COLUMN,
    tail_risk_label_column: str = DEFAULT_TAIL_RISK_LABEL_COLUMN,
    source_evidence_timestamp_column: str = DEFAULT_SOURCE_EVIDENCE_TIMESTAMP_COLUMN,
    min_prior_material_safe_switch_examples_for_dt: int = (
        DEFAULT_MIN_SAFE_SWITCH_EXAMPLES
    ),
) -> dict[str, Any]:
    """Summarize whether candidate rows are eligible as V13 backfill evidence."""

    blocking_reasons: list[str] = []
    fatal_reasons: list[str] = []
    missing_columns = [
        column for column in _IDENTITY_COLUMNS if column not in frame.columns
    ]
    if missing_columns:
        fatal_reasons.extend(f"missing_required_column:{column}" for column in missing_columns)
    if source_evidence_timestamp_column not in frame.columns:
        fatal_reasons.append(
            f"missing_source_evidence_timestamp_column:{source_evidence_timestamp_column}"
        )
    if material_label_column not in frame.columns:
        fatal_reasons.append(f"missing_material_label_column:{material_label_column}")
    elif material_label_column != DEFAULT_MATERIAL_LABEL_COLUMN:
        blocking_reasons.append(f"noncanonical_material_label_column:{material_label_column}")
    if tail_risk_label_column not in frame.columns:
        fatal_reasons.append(f"missing_tail_risk_label_column:{tail_risk_label_column}")
    elif tail_risk_label_column != DEFAULT_TAIL_RISK_LABEL_COLUMN:
        blocking_reasons.append(f"noncanonical_tail_risk_label_column:{tail_risk_label_column}")
    if (
        source_evidence_timestamp_column in frame.columns
        and source_evidence_timestamp_column != DEFAULT_SOURCE_EVIDENCE_TIMESTAMP_COLUMN
    ):
        blocking_reasons.append(
            f"noncanonical_source_evidence_timestamp_column:{source_evidence_timestamp_column}"
        )
    if _source_contains_true(frame, "market_execution_enabled"):
        fatal_reasons.append("source_contains_market_execution_rows")
    if _source_contains_true(frame, "raw_hourly_action_imitation"):
        fatal_reasons.append("source_contains_raw_hourly_action_imitation_rows")

    weak_safe_switch_win_rows = _true_count(frame, "label_safe_switch_win")
    blocking_reasons.extend(fatal_reasons)
    if fatal_reasons:
        return _audit_summary(
            frame=frame,
            material_label_column=material_label_column,
            tail_risk_label_column=tail_risk_label_column,
            source_evidence_timestamp_column=source_evidence_timestamp_column,
            min_prior_material_safe_switch_examples_for_dt=(
                min_prior_material_safe_switch_examples_for_dt
            ),
            blocking_reasons=blocking_reasons,
            accepted=pl.DataFrame(),
            weak_safe_switch_win_rows=weak_safe_switch_win_rows,
            tail_risk_rejected_rows=0,
            non_train_selection_rejected_rows=0,
            duplicate_accepted_rows=0,
            missing_source_evidence_timestamp_rows=0,
        )

    normalized = frame.with_columns(
        pl.col("anchor_timestamp")
        .map_elements(_datetime_value, return_dtype=pl.Datetime)
        .alias("anchor_timestamp"),
        pl.col(source_evidence_timestamp_column)
        .map_elements(_datetime_value, return_dtype=pl.Datetime)
        .alias("_v13_source_evidence_timestamp"),
        pl.col(material_label_column)
        .map_elements(_bool_value, return_dtype=pl.Boolean)
        .alias("_v13_material_label"),
        pl.col(tail_risk_label_column)
        .map_elements(_bool_value, return_dtype=pl.Boolean)
        .alias("_v13_tail_risk_label"),
    )
    non_train_selection_rejected_rows = normalized.filter(
        pl.col("split_name") != TRAIN_SELECTION_SPLIT
    ).height
    tail_risk_rejected_rows = normalized.filter(pl.col("_v13_tail_risk_label")).height
    missing_source_evidence_timestamp_rows = normalized.filter(
        pl.col("_v13_source_evidence_timestamp").is_null()
    ).height
    accepted = normalized.filter(
        (pl.col("split_name") == TRAIN_SELECTION_SPLIT)
        & pl.col("_v13_material_label")
        & (~pl.col("_v13_tail_risk_label"))
        & pl.col("_v13_source_evidence_timestamp").is_not_null()
    ).select(
        [
            pl.col("tenant_id").cast(pl.Utf8),
            pl.col("source_model_name").cast(pl.Utf8),
            pl.col("anchor_timestamp"),
            pl.col("split_name").cast(pl.Utf8),
            pl.col("_v13_source_evidence_timestamp").alias(
                "source_evidence_timestamp"
            ),
        ]
    )
    duplicate_accepted_rows = accepted.filter(
        pl.struct(["tenant_id", "source_model_name", "anchor_timestamp"])
        .is_duplicated()
    ).height
    if duplicate_accepted_rows:
        blocking_reasons.append("duplicate_accepted_candidate_rows")

    return _audit_summary(
        frame=frame,
        material_label_column=material_label_column,
        tail_risk_label_column=tail_risk_label_column,
        source_evidence_timestamp_column=source_evidence_timestamp_column,
        min_prior_material_safe_switch_examples_for_dt=(
            min_prior_material_safe_switch_examples_for_dt
        ),
        blocking_reasons=blocking_reasons,
        accepted=accepted.unique(["tenant_id", "source_model_name", "anchor_timestamp"]),
        weak_safe_switch_win_rows=weak_safe_switch_win_rows,
        tail_risk_rejected_rows=tail_risk_rejected_rows,
        non_train_selection_rejected_rows=non_train_selection_rejected_rows,
        duplicate_accepted_rows=duplicate_accepted_rows,
        missing_source_evidence_timestamp_rows=missing_source_evidence_timestamp_rows,
    )


def _audit_summary(
    *,
    frame: pl.DataFrame,
    material_label_column: str,
    tail_risk_label_column: str,
    source_evidence_timestamp_column: str,
    min_prior_material_safe_switch_examples_for_dt: int,
    blocking_reasons: list[str],
    accepted: pl.DataFrame,
    weak_safe_switch_win_rows: int,
    tail_risk_rejected_rows: int,
    non_train_selection_rejected_rows: int,
    duplicate_accepted_rows: int,
    missing_source_evidence_timestamp_rows: int,
) -> dict[str, Any]:
    accepted_rows = accepted.height
    tenant_source_counts = _tenant_source_counts(
        accepted,
        min_prior_material_safe_switch_examples_for_dt=(
            min_prior_material_safe_switch_examples_for_dt
        ),
    )
    normalized_ready = accepted_rows > 0 and not blocking_reasons
    return {
        "claim_scope": CLAIM_SCOPE,
        "source_rows": frame.height,
        "source_columns": sorted(frame.columns),
        "material_label_column": material_label_column,
        "tail_risk_label_column": tail_risk_label_column,
        "source_evidence_timestamp_column": source_evidence_timestamp_column,
        "uses_canonical_v13_labels": (
            material_label_column == DEFAULT_MATERIAL_LABEL_COLUMN
            and tail_risk_label_column == DEFAULT_TAIL_RISK_LABEL_COLUMN
            and source_evidence_timestamp_column
            == DEFAULT_SOURCE_EVIDENCE_TIMESTAMP_COLUMN
        ),
        "accepted_candidate_rows": accepted_rows,
        "rejected_candidate_rows": max(frame.height - accepted_rows, 0),
        "weak_safe_switch_win_rows": weak_safe_switch_win_rows,
        "tail_risk_rejected_rows": tail_risk_rejected_rows,
        "non_train_selection_rejected_rows": non_train_selection_rejected_rows,
        "missing_source_evidence_timestamp_rows": (
            missing_source_evidence_timestamp_rows
        ),
        "duplicate_accepted_rows": duplicate_accepted_rows,
        "tenant_source_counts": tenant_source_counts,
        "accepted_tenant_source_count": len(tenant_source_counts),
        "min_prior_material_safe_switch_examples_for_dt": (
            min_prior_material_safe_switch_examples_for_dt
        ),
        "normalized_safe_switch_csv_ready": normalized_ready,
        "blocking_reasons": blocking_reasons,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }


def _tenant_source_counts(
    accepted: pl.DataFrame,
    *,
    min_prior_material_safe_switch_examples_for_dt: int,
) -> list[dict[str, Any]]:
    if accepted.is_empty():
        return []
    counts = (
        accepted.group_by(["tenant_id", "source_model_name"])
        .agg(pl.len().alias("accepted_candidate_rows"))
        .with_columns(
            (
                pl.lit(min_prior_material_safe_switch_examples_for_dt)
                - pl.col("accepted_candidate_rows")
            )
            .clip(0)
            .alias("missing_to_floor")
        )
        .sort(["source_model_name", "tenant_id"])
    )
    return [
        {
            "tenant_id": str(row["tenant_id"]),
            "source_model_name": str(row["source_model_name"]),
            "accepted_candidate_rows": int(row["accepted_candidate_rows"]),
            "missing_to_floor": int(row["missing_to_floor"]),
        }
        for row in counts.iter_rows(named=True)
    ]


def _source_contains_true(frame: pl.DataFrame, column_name: str) -> bool:
    if frame.is_empty() or column_name not in frame.columns:
        return False
    normalized = frame.with_columns(
        pl.col(column_name)
        .map_elements(_bool_value, return_dtype=pl.Boolean)
        .alias(column_name)
    )
    return bool(normalized[column_name].any())


def _true_count(frame: pl.DataFrame, column_name: str) -> int:
    if frame.is_empty() or column_name not in frame.columns:
        return 0
    normalized = frame.with_columns(
        pl.col(column_name)
        .map_elements(_bool_value, return_dtype=pl.Boolean)
        .alias(column_name)
    )
    return int(normalized.filter(pl.col(column_name)).height)


def _datetime_value(value: Any) -> datetime | None:
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
    raise TypeError(f"Cannot convert {type(value).__name__} to bool.")


__all__ = [
    "CLAIM_SCOPE",
    "audit_dfl_ua_context_safe_switch_candidate_source_v13_frame",
]
