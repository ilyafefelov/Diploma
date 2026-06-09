"""Build a V13 safe-switch review backlog without promoting candidates."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Final

import polars as pl

CLAIM_SCOPE: Final[str] = "v13_safe_switch_review_backlog_not_training_data"
TRAIN_SELECTION_SPLIT: Final[str] = "train_selection"
DEFAULT_MATERIAL_LABEL_COLUMN: Final[str] = "label_v13_material_safe_switch"
DEFAULT_TAIL_RISK_LABEL_COLUMN: Final[str] = "label_v13_tail_risk_loss"
DEFAULT_SOURCE_EVIDENCE_TIMESTAMP_COLUMN: Final[str] = "source_evidence_timestamp"

_TARGET_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "acquisition_priority_rank",
        "tenant_id",
        "source_model_name",
        "current_prior_material_safe_switch_examples",
        "required_prior_material_safe_switch_examples",
        "target_new_prior_material_safe_switch_examples",
        "primary_blocking_source_family",
        "market_execution_enabled",
    }
)
_CANDIDATE_BASE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "split_name",
    }
)
_SUMMARY_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "acquisition_priority_rank",
        "tenant_id",
        "source_model_name",
        "candidate_can_satisfy_v13_without_validation",
        "permits_model_training",
        "market_execution_enabled",
    }
)


def build_dfl_ua_context_safe_switch_review_backlog_v13_frame(
    candidate_frame: pl.DataFrame,
    acquisition_targets_frame: pl.DataFrame,
    *,
    material_label_column: str = DEFAULT_MATERIAL_LABEL_COLUMN,
    tail_risk_label_column: str = DEFAULT_TAIL_RISK_LABEL_COLUMN,
    source_evidence_timestamp_column: str = DEFAULT_SOURCE_EVIDENCE_TIMESTAMP_COLUMN,
    max_review_rows_per_target: int | None = None,
) -> pl.DataFrame:
    """Rank candidate rows for manual V13 safe-switch evidence review.

    The output is a review backlog only. It deliberately cannot satisfy the V13
    CSV contract until a curator produces canonical rows and validates them with
    `validate_ua_context_safe_switch_examples_v13.py`.
    """

    _validate_inputs(
        candidate_frame,
        acquisition_targets_frame,
        material_label_column=material_label_column,
        tail_risk_label_column=tail_risk_label_column,
        source_evidence_timestamp_column=source_evidence_timestamp_column,
    )
    if max_review_rows_per_target is not None and max_review_rows_per_target < 0:
        raise ValueError("max_review_rows_per_target must not be negative.")

    candidate_rows = _normalized_candidate_rows(
        candidate_frame,
        material_label_column=material_label_column,
        tail_risk_label_column=tail_risk_label_column,
        source_evidence_timestamp_column=source_evidence_timestamp_column,
    )
    rows: list[dict[str, Any]] = []
    canonical_columns = (
        material_label_column == DEFAULT_MATERIAL_LABEL_COLUMN
        and tail_risk_label_column == DEFAULT_TAIL_RISK_LABEL_COLUMN
        and source_evidence_timestamp_column == DEFAULT_SOURCE_EVIDENCE_TIMESTAMP_COLUMN
    )
    for target in acquisition_targets_frame.sort("acquisition_priority_rank").iter_rows(
        named=True
    ):
        target_new = _safe_int(target["target_new_prior_material_safe_switch_examples"])
        row_limit = (
            target_new
            if max_review_rows_per_target is None
            else min(target_new, max_review_rows_per_target)
        )
        if row_limit <= 0:
            continue
        matching = [
            row
            for row in candidate_rows
            if row["tenant_id"] == str(target["tenant_id"])
            and row["source_model_name"] == str(target["source_model_name"])
        ]
        matching = sorted(
            matching,
            key=lambda row: (
                -int(bool(row["material_candidate"])),
                -int(bool(row["weak_safe_switch_win"])),
                str(row["anchor_timestamp"]),
            ),
        )
        seen_anchors: set[str] = set()
        review_rank = 0
        for candidate in matching:
            anchor_key = str(candidate["anchor_timestamp"])
            if anchor_key in seen_anchors:
                continue
            seen_anchors.add(anchor_key)
            review_rank += 1
            if review_rank > row_limit:
                break
            rows.append(
                {
                    "acquisition_priority_rank": _safe_int(
                        target["acquisition_priority_rank"]
                    ),
                    "tenant_id": str(target["tenant_id"]),
                    "source_model_name": str(target["source_model_name"]),
                    "current_prior_material_safe_switch_examples": _safe_int(
                        target["current_prior_material_safe_switch_examples"]
                    ),
                    "required_prior_material_safe_switch_examples": _safe_int(
                        target["required_prior_material_safe_switch_examples"]
                    ),
                    "target_new_prior_material_safe_switch_examples": target_new,
                    "review_rank_for_target": review_rank,
                    "anchor_timestamp": candidate["anchor_timestamp"],
                    "split_name": candidate["split_name"],
                    "source_evidence_timestamp": candidate[
                        "source_evidence_timestamp"
                    ],
                    "candidate_family": candidate["candidate_family"],
                    "candidate_model_name": candidate["candidate_model_name"],
                    "material_label_column": material_label_column,
                    "tail_risk_label_column": tail_risk_label_column,
                    "source_evidence_timestamp_column": (
                        source_evidence_timestamp_column
                    ),
                    "uses_canonical_v13_labels": canonical_columns,
                    "material_candidate": bool(candidate["material_candidate"]),
                    "weak_safe_switch_win": bool(candidate["weak_safe_switch_win"]),
                    "tail_risk_candidate": bool(candidate["tail_risk_candidate"]),
                    "label_regret_delta_vs_v2_plus_uah": candidate[
                        "label_regret_delta_vs_v2_plus_uah"
                    ],
                    "review_status": _review_status(
                        canonical_columns=canonical_columns,
                        material_candidate=bool(candidate["material_candidate"]),
                    ),
                    "required_review_action": (
                        "curate_source_backed_canonical_v13_safe_switch_row"
                    ),
                    "candidate_can_satisfy_v13_without_validation": False,
                    "can_feed_safe_switch_validator_after_review": (
                        canonical_columns
                        and bool(candidate["material_candidate"])
                        and not bool(candidate["tail_risk_candidate"])
                    ),
                    "primary_blocking_source_family": str(
                        target["primary_blocking_source_family"]
                    ),
                    "target_label_space": "v13_precondition_context_coverage",
                    "claim_scope": CLAIM_SCOPE,
                    "dt_lava_ready": False,
                    "permits_model_training": False,
                    "market_execution_enabled": False,
                }
            )
    if not rows:
        return _empty_backlog_frame()
    return pl.DataFrame(rows, infer_schema_length=None).sort(
        ["acquisition_priority_rank", "review_rank_for_target"]
    )


def summarize_dfl_ua_context_safe_switch_review_backlog_v13_frame(
    frame: pl.DataFrame,
) -> dict[str, Any]:
    """Summarize the review backlog as non-promotional acquisition evidence."""

    if frame.height == 0:
        return {
            "claim_scope": CLAIM_SCOPE,
            "review_rows": 0,
            "target_tenant_source_count": 0,
            "phase0_priority_tenant_id": None,
            "candidate_can_satisfy_v13_without_validation": False,
            "dt_lava_ready": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
        }
    _require_columns(
        frame,
        _SUMMARY_REQUIRED_COLUMNS,
        frame_name="V13 safe-switch review backlog frame",
    )
    _refuse_true(
        frame,
        "candidate_can_satisfy_v13_without_validation",
        frame_name="V13 safe-switch review backlog frame",
    )
    _refuse_true(
        frame,
        "permits_model_training",
        frame_name="V13 safe-switch review backlog frame",
    )
    _refuse_true(
        frame,
        "market_execution_enabled",
        frame_name="V13 safe-switch review backlog frame",
    )
    counts = (
        frame.group_by(["tenant_id", "source_model_name"])
        .agg(
            pl.len().alias("review_rows"),
            pl.col("target_new_prior_material_safe_switch_examples")
            .first()
            .alias("target_new_prior_material_safe_switch_examples"),
            pl.col("acquisition_priority_rank").first().alias("acquisition_priority_rank"),
        )
        .sort("acquisition_priority_rank")
    )
    first_row = frame.sort("acquisition_priority_rank").row(0, named=True)
    return {
        "claim_scope": CLAIM_SCOPE,
        "review_rows": frame.height,
        "target_tenant_source_count": counts.height,
        "phase0_priority_tenant_id": str(first_row["tenant_id"]),
        "candidate_can_satisfy_v13_without_validation": False,
        "review_rows_by_tenant_source": [
            {
                "tenant_id": str(row["tenant_id"]),
                "source_model_name": str(row["source_model_name"]),
                "review_rows": _safe_int(row["review_rows"]),
                "target_new_prior_material_safe_switch_examples": _safe_int(
                    row["target_new_prior_material_safe_switch_examples"]
                ),
            }
            for row in counts.iter_rows(named=True)
        ],
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }


def _validate_inputs(
    candidate_frame: pl.DataFrame,
    acquisition_targets_frame: pl.DataFrame,
    *,
    material_label_column: str,
    tail_risk_label_column: str,
    source_evidence_timestamp_column: str,
) -> None:
    _require_columns(
        acquisition_targets_frame,
        _TARGET_REQUIRED_COLUMNS,
        frame_name="V13 safe-switch acquisition targets frame",
    )
    _require_columns(
        candidate_frame,
        _CANDIDATE_BASE_COLUMNS
        | {material_label_column, tail_risk_label_column, source_evidence_timestamp_column},
        frame_name="V13 safe-switch candidate frame",
    )
    _refuse_true(
        acquisition_targets_frame,
        "market_execution_enabled",
        frame_name="V13 safe-switch acquisition targets frame",
    )
    _refuse_true(
        candidate_frame,
        "market_execution_enabled",
        frame_name="V13 safe-switch candidate frame",
    )
    if "raw_hourly_action_imitation" in candidate_frame.columns:
        _refuse_true(
            candidate_frame,
            "raw_hourly_action_imitation",
            frame_name="V13 safe-switch candidate frame",
        )


def _normalized_candidate_rows(
    candidate_frame: pl.DataFrame,
    *,
    material_label_column: str,
    tail_risk_label_column: str,
    source_evidence_timestamp_column: str,
) -> list[dict[str, Any]]:
    normalized = candidate_frame.with_columns(
        pl.col("anchor_timestamp")
        .map_elements(_datetime_value, return_dtype=pl.Datetime)
        .alias("anchor_timestamp"),
        pl.col(source_evidence_timestamp_column)
        .map_elements(_datetime_value, return_dtype=pl.Datetime)
        .alias("_source_evidence_timestamp"),
        pl.col(material_label_column)
        .map_elements(_bool_value, return_dtype=pl.Boolean)
        .alias("_material_candidate"),
        pl.col(tail_risk_label_column)
        .map_elements(_bool_value, return_dtype=pl.Boolean)
        .alias("_tail_risk_candidate"),
        _optional_bool_column(candidate_frame, "label_safe_switch_win").alias(
            "_weak_safe_switch_win"
        ),
        _optional_string_column(candidate_frame, "candidate_family").alias(
            "_candidate_family"
        ),
        _optional_string_column(candidate_frame, "candidate_model_name").alias(
            "_candidate_model_name"
        ),
        _optional_float_column(
            candidate_frame,
            "label_regret_delta_vs_v2_plus_uah",
        ).alias("_label_regret_delta_vs_v2_plus_uah"),
    )
    accepted = normalized.filter(
        (pl.col("split_name") == TRAIN_SELECTION_SPLIT)
        & pl.col("_source_evidence_timestamp").is_not_null()
        & (~pl.col("_tail_risk_candidate"))
        & (pl.col("_material_candidate") | pl.col("_weak_safe_switch_win"))
    ).select(
        [
            pl.col("tenant_id").cast(pl.Utf8),
            pl.col("source_model_name").cast(pl.Utf8),
            pl.col("anchor_timestamp"),
            pl.col("split_name").cast(pl.Utf8),
            pl.col("_source_evidence_timestamp").alias("source_evidence_timestamp"),
            pl.col("_candidate_family").alias("candidate_family"),
            pl.col("_candidate_model_name").alias("candidate_model_name"),
            pl.col("_material_candidate").alias("material_candidate"),
            pl.col("_weak_safe_switch_win").alias("weak_safe_switch_win"),
            pl.col("_tail_risk_candidate").alias("tail_risk_candidate"),
            pl.col("_label_regret_delta_vs_v2_plus_uah").alias(
                "label_regret_delta_vs_v2_plus_uah"
            ),
        ]
    )
    return [dict(row) for row in accepted.iter_rows(named=True)]


def _review_status(*, canonical_columns: bool, material_candidate: bool) -> str:
    if not material_candidate:
        return "requires_material_safe_switch_review"
    if not canonical_columns:
        return "requires_canonical_v13_relabel"
    return "ready_for_v13_validator_export"


def _empty_backlog_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "acquisition_priority_rank": pl.Int64,
            "tenant_id": pl.Utf8,
            "source_model_name": pl.Utf8,
            "candidate_can_satisfy_v13_without_validation": pl.Boolean,
            "permits_model_training": pl.Boolean,
            "market_execution_enabled": pl.Boolean,
        }
    )


def _require_columns(
    frame: pl.DataFrame,
    columns: frozenset[str] | set[str],
    *,
    frame_name: str,
) -> None:
    missing = set(columns).difference(frame.columns)
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
            raise ValueError(
                f"{frame_name} contains market execution rows "
                f"({column_name}=true)."
            )
        raise ValueError(f"{frame_name} contains {column_name}=true.")


def _optional_bool_column(frame: pl.DataFrame, column_name: str) -> pl.Expr:
    if column_name in frame.columns:
        return pl.col(column_name).map_elements(_bool_value, return_dtype=pl.Boolean)
    return pl.lit(False, dtype=pl.Boolean)


def _optional_string_column(frame: pl.DataFrame, column_name: str) -> pl.Expr:
    if column_name in frame.columns:
        return pl.col(column_name).cast(pl.Utf8)
    return pl.lit("", dtype=pl.Utf8)


def _optional_float_column(frame: pl.DataFrame, column_name: str) -> pl.Expr:
    if column_name in frame.columns:
        return pl.col(column_name).map_elements(_optional_float, return_dtype=pl.Float64)
    return pl.lit(None, dtype=pl.Float64)


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
        if normalized in {"false", "0", "no", ""}:
            return False
    raise TypeError(f"Cannot convert {type(value).__name__} to bool.")


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (float, int, str)):
        stripped = str(value).strip()
        if not stripped:
            return None
        return float(stripped)
    raise TypeError(f"Cannot convert {type(value).__name__} to float.")


def _safe_int(value: object) -> int:
    if isinstance(value, (int, float, str)):
        return int(value)
    raise TypeError(f"Cannot convert {type(value).__name__} to int.")


__all__ = [
    "CLAIM_SCOPE",
    "build_dfl_ua_context_safe_switch_review_backlog_v13_frame",
    "summarize_dfl_ua_context_safe_switch_review_backlog_v13_frame",
]
