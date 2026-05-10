"""Value-aware model gate for real-data forecast strategy benchmarks."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Final

import polars as pl

VALUE_AWARE_ENSEMBLE_MODEL_NAME: Final[str] = "value_aware_ensemble_v0"
VALUE_AWARE_ENSEMBLE_STRATEGY_KIND: Final[str] = "value_aware_ensemble_gate"
CALIBRATED_VALUE_AWARE_ENSEMBLE_MODEL_NAME: Final[str] = "calibrated_value_aware_ensemble_v0"
CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND: Final[str] = "calibrated_value_aware_ensemble_gate"
RISK_ADJUSTED_VALUE_GATE_MODEL_NAME: Final[str] = "risk_adjusted_value_gate_v0"
RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND: Final[str] = "risk_adjusted_value_gate"
CONTROL_MODEL_NAME: Final[str] = "strict_similar_day"
CALIBRATED_VALUE_AWARE_CANDIDATE_MODEL_NAMES: Final[tuple[str, ...]] = (
    CONTROL_MODEL_NAME,
    "tft_horizon_regret_weighted_calibrated_v0",
    "nbeatsx_horizon_regret_weighted_calibrated_v0",
)


def build_value_aware_ensemble_frame(
    evaluation_frame: pl.DataFrame,
    *,
    validation_window_anchors: int = 14,
) -> pl.DataFrame:
    """Select the model with lowest prior validation regret for each tenant anchor."""

    return _build_ensemble_frame(
        evaluation_frame,
        validation_window_anchors=validation_window_anchors,
        candidate_model_names=None,
        ensemble_model_name=VALUE_AWARE_ENSEMBLE_MODEL_NAME,
        ensemble_strategy_kind=VALUE_AWARE_ENSEMBLE_STRATEGY_KIND,
        evaluation_id_suffix="ensemble",
        selection_policy="lowest_mean_regret_on_prior_anchors_only",
        academic_scope=(
            "Value-aware ensemble gate selected from prior-anchor validation regret only. "
            "It reuses already evaluated forecast-to-LP candidates and does not use oracle lookahead for selection."
        ),
    )


def build_calibrated_value_aware_ensemble_frame(
    evaluation_frame: pl.DataFrame,
    *,
    validation_window_anchors: int = 14,
) -> pl.DataFrame:
    """Select strict or horizon-calibrated forecasts using prior-anchor regret only."""

    return _build_ensemble_frame(
        evaluation_frame,
        validation_window_anchors=validation_window_anchors,
        candidate_model_names=CALIBRATED_VALUE_AWARE_CANDIDATE_MODEL_NAMES,
        ensemble_model_name=CALIBRATED_VALUE_AWARE_ENSEMBLE_MODEL_NAME,
        ensemble_strategy_kind=CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND,
        evaluation_id_suffix="calibrated-ensemble",
        selection_policy="lowest_mean_regret_on_prior_anchors_only",
        academic_scope=(
            "Calibrated value-aware ensemble gate selected from strict similar-day and "
            "horizon-aware regret-weighted TFT/NBEATSx rows using prior-anchor validation regret only. "
            "Raw neural rows are ignored by this selector, and no oracle lookahead is used for selection."
        ),
    )


def build_risk_adjusted_value_gate_frame(
    evaluation_frame: pl.DataFrame,
    *,
    validation_window_anchors: int = 14,
    downside_tail_weight: float = 0.75,
    win_rate_weight: float = 50.0,
) -> pl.DataFrame:
    """Select a prior-risk-adjusted model using median regret, tail regret, and win rate."""

    if validation_window_anchors <= 0:
        raise ValueError("validation_window_anchors must be positive.")
    if downside_tail_weight < 0.0:
        raise ValueError("downside_tail_weight must be non-negative.")
    if win_rate_weight < 0.0:
        raise ValueError("win_rate_weight must be non-negative.")
    _validate_evaluation_frame(evaluation_frame)
    candidate_frame = _candidate_frame(
        evaluation_frame,
        candidate_model_names=CALIBRATED_VALUE_AWARE_CANDIDATE_MODEL_NAMES,
    )
    selected_rows: list[dict[str, Any]] = []
    for tenant_id in sorted(
        str(value)
        for value in candidate_frame.select("tenant_id").to_series().unique().to_list()
    ):
        tenant_frame = candidate_frame.filter(pl.col("tenant_id") == tenant_id).sort(
            ["anchor_timestamp", "forecast_model_name"]
        )
        anchors = [
            value
            for value in tenant_frame.select("anchor_timestamp").to_series().unique().sort().to_list()
            if isinstance(value, datetime)
        ]
        for anchor_timestamp in anchors:
            selected_model_name, selection_payload = _risk_adjusted_model_for_anchor(
                tenant_frame=tenant_frame,
                anchor_timestamp=anchor_timestamp,
                validation_window_anchors=validation_window_anchors,
                downside_tail_weight=downside_tail_weight,
                win_rate_weight=win_rate_weight,
            )
            matching_rows = tenant_frame.filter(
                (pl.col("anchor_timestamp") == anchor_timestamp)
                & (pl.col("forecast_model_name") == selected_model_name)
            )
            if matching_rows.height != 1:
                raise ValueError("Each tenant/anchor/model must have exactly one benchmark row.")
            selected_rows.append(
                _ensemble_row(
                    matching_rows.row(0, named=True),
                    selected_model_name=selected_model_name,
                    prior_validation_anchor_count=int(selection_payload["prior_validation_anchor_count"]),
                    validation_window_anchors=validation_window_anchors,
                    ensemble_model_name=RISK_ADJUSTED_VALUE_GATE_MODEL_NAME,
                    ensemble_strategy_kind=RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND,
                    evaluation_id_suffix="risk-adjusted-gate",
                    selection_policy="risk_adjusted_prior_anchor_regret_tail_and_win_rate",
                    academic_scope=(
                        "Risk-adjusted value gate selected from strict similar-day and horizon-aware "
                        "regret-weighted TFT/NBEATSx rows using prior-anchor median regret, tail regret, "
                        "and win rate only. It is a selector diagnostic, not full DFL."
                    ),
                    extra_payload=selection_payload,
                )
            )
    if not selected_rows:
        return pl.DataFrame()
    return pl.DataFrame(selected_rows).sort(["tenant_id", "anchor_timestamp"])


def _build_ensemble_frame(
    evaluation_frame: pl.DataFrame,
    *,
    validation_window_anchors: int,
    candidate_model_names: tuple[str, ...] | None,
    ensemble_model_name: str,
    ensemble_strategy_kind: str,
    evaluation_id_suffix: str,
    selection_policy: str,
    academic_scope: str,
) -> pl.DataFrame:
    if validation_window_anchors <= 0:
        raise ValueError("validation_window_anchors must be positive.")
    _validate_evaluation_frame(evaluation_frame)
    candidate_frame = _candidate_frame(
        evaluation_frame,
        candidate_model_names=candidate_model_names,
    )
    selected_rows: list[dict[str, Any]] = []
    for tenant_id in sorted(
        str(value)
        for value in candidate_frame.select("tenant_id").to_series().unique().to_list()
    ):
        tenant_frame = candidate_frame.filter(pl.col("tenant_id") == tenant_id).sort(
            ["anchor_timestamp", "forecast_model_name"]
        )
        anchors = [
            value
            for value in tenant_frame.select("anchor_timestamp").to_series().unique().sort().to_list()
            if isinstance(value, datetime)
        ]
        for anchor_timestamp in anchors:
            selected_model_name, prior_validation_anchor_count = _selected_model_for_anchor(
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
            selected_rows.append(
                _ensemble_row(
                    matching_rows.row(0, named=True),
                    selected_model_name=selected_model_name,
                    prior_validation_anchor_count=prior_validation_anchor_count,
                    validation_window_anchors=validation_window_anchors,
                    ensemble_model_name=ensemble_model_name,
                    ensemble_strategy_kind=ensemble_strategy_kind,
                    evaluation_id_suffix=evaluation_id_suffix,
                    selection_policy=selection_policy,
                    academic_scope=academic_scope,
                )
            )
    if not selected_rows:
        return pl.DataFrame()
    return pl.DataFrame(selected_rows).sort(["tenant_id", "anchor_timestamp"])


def _candidate_frame(
    evaluation_frame: pl.DataFrame,
    *,
    candidate_model_names: tuple[str, ...] | None,
) -> pl.DataFrame:
    if candidate_model_names is None:
        return evaluation_frame
    return evaluation_frame.filter(pl.col("forecast_model_name").is_in(candidate_model_names))


def _selected_model_for_anchor(
    *,
    tenant_frame: pl.DataFrame,
    anchor_timestamp: datetime,
    validation_window_anchors: int,
) -> tuple[str, int]:
    prior_frame = tenant_frame.filter(pl.col("anchor_timestamp") < anchor_timestamp)
    if prior_frame.height == 0:
        return CONTROL_MODEL_NAME, 0
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
        return CONTROL_MODEL_NAME, 0
    return str(summary.row(0, named=True)["forecast_model_name"]), len(prior_anchors)


def _risk_adjusted_model_for_anchor(
    *,
    tenant_frame: pl.DataFrame,
    anchor_timestamp: datetime,
    validation_window_anchors: int,
    downside_tail_weight: float,
    win_rate_weight: float,
) -> tuple[str, dict[str, Any]]:
    prior_frame = tenant_frame.filter(pl.col("anchor_timestamp") < anchor_timestamp)
    if prior_frame.height == 0:
        return CONTROL_MODEL_NAME, {
            "prior_validation_anchor_count": 0,
            "risk_adjusted_score": None,
            "risk_adjusted_candidate_scores": [],
            "downside_tail_weight": downside_tail_weight,
            "win_rate_weight": win_rate_weight,
        }
    prior_anchors = (
        prior_frame.select("anchor_timestamp")
        .unique()
        .sort("anchor_timestamp")
        .tail(validation_window_anchors)
        .to_series()
        .to_list()
    )
    validation_rows = list(
        prior_frame
        .filter(pl.col("anchor_timestamp").is_in(prior_anchors))
        .iter_rows(named=True)
    )
    candidate_scores = _risk_adjusted_candidate_scores(
        validation_rows=validation_rows,
        prior_anchor_count=len(prior_anchors),
        downside_tail_weight=downside_tail_weight,
        win_rate_weight=win_rate_weight,
    )
    if not candidate_scores:
        return CONTROL_MODEL_NAME, {
            "prior_validation_anchor_count": 0,
            "risk_adjusted_score": None,
            "risk_adjusted_candidate_scores": [],
            "downside_tail_weight": downside_tail_weight,
            "win_rate_weight": win_rate_weight,
        }
    selected = sorted(
        candidate_scores,
        key=lambda item: (
            float(item["risk_adjusted_score"]),
            float(item["median_regret_uah"]),
            str(item["forecast_model_name"]),
        ),
    )[0]
    return str(selected["forecast_model_name"]), {
        "prior_validation_anchor_count": len(prior_anchors),
        "risk_adjusted_score": selected["risk_adjusted_score"],
        "median_regret_uah": selected["median_regret_uah"],
        "max_regret_uah": selected["max_regret_uah"],
        "prior_win_rate": selected["prior_win_rate"],
        "downside_tail_weight": downside_tail_weight,
        "win_rate_weight": win_rate_weight,
        "risk_adjusted_candidate_scores": candidate_scores,
    }


def _risk_adjusted_candidate_scores(
    *,
    validation_rows: list[dict[str, Any]],
    prior_anchor_count: int,
    downside_tail_weight: float,
    win_rate_weight: float,
) -> list[dict[str, Any]]:
    rows_by_anchor: dict[datetime, list[dict[str, Any]]] = {}
    regrets_by_model: dict[str, list[float]] = {}
    for row in validation_rows:
        anchor_timestamp = row["anchor_timestamp"]
        if not isinstance(anchor_timestamp, datetime):
            continue
        rows_by_anchor.setdefault(anchor_timestamp, []).append(row)
        regrets_by_model.setdefault(str(row["forecast_model_name"]), []).append(float(row["regret_uah"]))

    wins_by_model = dict.fromkeys(regrets_by_model, 0)
    for anchor_rows in rows_by_anchor.values():
        minimum_regret = min(float(row["regret_uah"]) for row in anchor_rows)
        for row in anchor_rows:
            if float(row["regret_uah"]) == minimum_regret:
                wins_by_model[str(row["forecast_model_name"])] = wins_by_model.get(
                    str(row["forecast_model_name"]), 0
                ) + 1

    candidate_scores = []
    for model_name, regrets in regrets_by_model.items():
        median_regret = _median_float(regrets)
        max_regret = max(regrets)
        prior_win_rate = wins_by_model.get(model_name, 0) / prior_anchor_count
        risk_adjusted_score = median_regret + (downside_tail_weight * max_regret) - (
            win_rate_weight * prior_win_rate
        )
        candidate_scores.append(
            {
                "forecast_model_name": model_name,
                "median_regret_uah": median_regret,
                "max_regret_uah": max_regret,
                "prior_win_rate": prior_win_rate,
                "risk_adjusted_score": risk_adjusted_score,
            }
        )
    return sorted(candidate_scores, key=lambda item: str(item["forecast_model_name"]))


def _median_float(values: list[float]) -> float:
    sorted_values = sorted(values)
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2 == 1:
        return sorted_values[midpoint]
    return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2.0


def _ensemble_row(
    row: dict[str, Any],
    *,
    selected_model_name: str,
    prior_validation_anchor_count: int,
    validation_window_anchors: int,
    ensemble_model_name: str,
    ensemble_strategy_kind: str,
    evaluation_id_suffix: str,
    selection_policy: str,
    academic_scope: str,
    extra_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensemble_row = dict(row)
    original_payload = row.get("evaluation_payload")
    payload = dict(original_payload) if isinstance(original_payload, dict) else {}
    payload.update(
        {
            "selected_model_name": selected_model_name,
            "ensemble_gate": ensemble_model_name,
            "selection_policy": selection_policy,
            "control_model_name": CONTROL_MODEL_NAME,
            "prior_validation_anchor_count": prior_validation_anchor_count,
            "validation_window_anchors": validation_window_anchors,
            "academic_scope": academic_scope,
        }
    )
    if extra_payload is not None:
        payload.update(extra_payload)
    ensemble_row["evaluation_id"] = f"{row['evaluation_id']}:{evaluation_id_suffix}"
    ensemble_row["forecast_model_name"] = ensemble_model_name
    ensemble_row["strategy_kind"] = ensemble_strategy_kind
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
