"""Value-aware model gate for real-data forecast strategy benchmarks."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import polars as pl

VALUE_AWARE_ENSEMBLE_MODEL_NAME = "value_aware_ensemble_v0"
VALUE_AWARE_ENSEMBLE_STRATEGY_KIND = "value_aware_ensemble_gate"
CONTROL_MODEL_NAME = "strict_similar_day"


def build_value_aware_ensemble_frame(
    evaluation_frame: pl.DataFrame,
    *,
    validation_window_anchors: int = 14,
) -> pl.DataFrame:
    """Select the model with lowest prior validation regret for each tenant anchor."""

    if validation_window_anchors <= 0:
        raise ValueError("validation_window_anchors must be positive.")
    _validate_evaluation_frame(evaluation_frame)
    selected_rows: list[dict[str, Any]] = []
    for tenant_id in sorted(
        str(value)
        for value in evaluation_frame.select("tenant_id").to_series().unique().to_list()
    ):
        tenant_frame = evaluation_frame.filter(pl.col("tenant_id") == tenant_id).sort(
            ["anchor_timestamp", "forecast_model_name"]
        )
        anchors = [
            value
            for value in tenant_frame.select("anchor_timestamp").to_series().unique().sort().to_list()
            if isinstance(value, datetime)
        ]
        for anchor_timestamp in anchors:
            selected_model_name = _selected_model_for_anchor(
                tenant_frame=tenant_frame,
                anchor_timestamp=anchor_timestamp,
                validation_window_anchors=validation_window_anchors,
            )
            matching_rows = tenant_frame.filter(
                (pl.col("anchor_timestamp") == anchor_timestamp)
                & (pl.col("forecast_model_name") == selected_model_name)
            )
            if matching_rows.height != 1:
                raise ValueError("Each tenant/anchor/model must have exactly one benchmark row.")
            selected_rows.append(_ensemble_row(matching_rows.row(0, named=True), selected_model_name=selected_model_name))
    if not selected_rows:
        return pl.DataFrame()
    return pl.DataFrame(selected_rows).sort(["tenant_id", "anchor_timestamp"])


def _selected_model_for_anchor(
    *,
    tenant_frame: pl.DataFrame,
    anchor_timestamp: datetime,
    validation_window_anchors: int,
) -> str:
    prior_frame = tenant_frame.filter(pl.col("anchor_timestamp") < anchor_timestamp)
    if prior_frame.height == 0:
        return CONTROL_MODEL_NAME
    prior_anchors = (
        prior_frame.select("anchor_timestamp")
        .unique()
        .sort("anchor_timestamp")
        .tail(validation_window_anchors)
        .to_series()
        .to_list()
    )
    validation_frame = prior_frame.filter(pl.col("anchor_timestamp").is_in(prior_anchors))
    summary = (
        validation_frame
        .group_by("forecast_model_name")
        .agg(pl.mean("regret_uah").alias("mean_regret_uah"))
        .sort(["mean_regret_uah", "forecast_model_name"])
    )
    if summary.height == 0:
        return CONTROL_MODEL_NAME
    return str(summary.row(0, named=True)["forecast_model_name"])


def _ensemble_row(row: dict[str, Any], *, selected_model_name: str) -> dict[str, Any]:
    ensemble_row = dict(row)
    original_payload = row.get("evaluation_payload")
    payload = dict(original_payload) if isinstance(original_payload, dict) else {}
    payload.update(
        {
            "selected_model_name": selected_model_name,
            "ensemble_gate": VALUE_AWARE_ENSEMBLE_MODEL_NAME,
            "selection_policy": "lowest_mean_regret_on_prior_anchors_only",
            "control_model_name": CONTROL_MODEL_NAME,
            "academic_scope": (
                "Value-aware ensemble gate selected from prior-anchor validation regret only. "
                "It reuses already evaluated forecast-to-LP candidates and does not use oracle lookahead for selection."
            ),
        }
    )
    ensemble_row["evaluation_id"] = f"{row['evaluation_id']}:ensemble"
    ensemble_row["forecast_model_name"] = VALUE_AWARE_ENSEMBLE_MODEL_NAME
    ensemble_row["strategy_kind"] = VALUE_AWARE_ENSEMBLE_STRATEGY_KIND
    ensemble_row["rank_by_regret"] = 1
    ensemble_row["evaluation_payload"] = payload
    return ensemble_row


def _validate_evaluation_frame(evaluation_frame: pl.DataFrame) -> None:
    required_columns = {
        "evaluation_id",
        "tenant_id",
        "forecast_model_name",
        "strategy_kind",
        "market_venue",
        "anchor_timestamp",
        "generated_at",
        "horizon_hours",
        "starting_soc_fraction",
        "starting_soc_source",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "regret_ratio",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "committed_action",
        "committed_power_mw",
        "rank_by_regret",
        "evaluation_payload",
    }
    missing_columns = required_columns.difference(evaluation_frame.columns)
    if missing_columns:
        raise ValueError(f"evaluation_frame is missing required columns: {sorted(missing_columns)}")
