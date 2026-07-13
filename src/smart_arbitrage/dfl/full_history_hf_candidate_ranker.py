"""Temporal protocol for the full-history HF value-aligned candidate ranker."""

from __future__ import annotations

from datetime import datetime
from typing import Final

import polars as pl


_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "dt_schedule_family_target",
        "regret_delta_vs_v2_plus_uah",
        "regret_uah",
        "schedule_value_uah",
    }
)


def split_full_history_candidate_frame(
    candidate_rows: pl.DataFrame,
    *,
    test_start: datetime,
    validation_anchor_count: int = 28,
    minimum_train_anchor_count: int = 293,
) -> dict[str, object]:
    """Split candidate rows into prior train/validation and future test blocks.

    The ranker may learn from realized regret labels in train and validation, but
    its test rows are strictly later. Candidate rows remain grouped by tenant,
    source, and decision anchor so all alternatives for one decision stay in
    one split.
    """

    missing = _REQUIRED_COLUMNS.difference(candidate_rows.columns)
    if missing:
        raise ValueError(f"candidate_rows is missing columns: {sorted(missing)}")
    if validation_anchor_count <= 0:
        raise ValueError("validation_anchor_count must be positive.")
    if minimum_train_anchor_count <= 0:
        raise ValueError("minimum_train_anchor_count must be positive.")

    anchors = sorted(
        value
        for value in candidate_rows.get_column("anchor_timestamp").unique().to_list()
        if isinstance(value, datetime) and value < test_start
    )
    if len(anchors) <= validation_anchor_count:
        raise ValueError(
            "Full-history ranker requires at least "
            f"{minimum_train_anchor_count} prior anchors plus a separate "
            "validation block."
        )
    validation_anchors = anchors[-validation_anchor_count:]
    validation_start = validation_anchors[0]
    train_rows = candidate_rows.filter(pl.col("anchor_timestamp") < validation_start)
    validation_rows = candidate_rows.filter(
        (pl.col("anchor_timestamp") >= validation_start)
        & (pl.col("anchor_timestamp") < test_start)
    )
    test_rows = candidate_rows.filter(pl.col("anchor_timestamp") >= test_start)
    if test_rows.is_empty():
        raise ValueError("Future test block is empty.")
    _require_full_history_per_tenant_source(
        train_rows,
        minimum_train_anchor_count=minimum_train_anchor_count,
    )
    return {
        "train_rows": train_rows,
        "validation_rows": validation_rows,
        "test_rows": test_rows,
        "train_anchor_count": train_rows.get_column("anchor_timestamp").n_unique(),
        "validation_anchor_count": validation_rows.get_column("anchor_timestamp").n_unique(),
        "test_anchor_count": test_rows.get_column("anchor_timestamp").n_unique(),
        "train_end": train_rows.get_column("anchor_timestamp").max().isoformat(),
        "validation_start": validation_start.isoformat(),
        "test_start": test_start.isoformat(),
        "claim_scope": "full_history_prior_only_hf_candidate_ranker_not_market_execution",
        "market_execution_enabled": False,
    }


def _require_full_history_per_tenant_source(
    train_rows: pl.DataFrame,
    *,
    minimum_train_anchor_count: int,
) -> None:
    counts = train_rows.group_by(["tenant_id", "source_model_name"]).agg(
        pl.col("anchor_timestamp").n_unique().alias("anchor_count")
    )
    insufficient = counts.filter(pl.col("anchor_count") < minimum_train_anchor_count)
    if not insufficient.is_empty():
        details = insufficient.sort(["tenant_id", "source_model_name"]).to_dicts()
        raise ValueError(
            "Full-history ranker requires at least "
            f"{minimum_train_anchor_count} prior anchors per tenant/source: {details}"
        )
