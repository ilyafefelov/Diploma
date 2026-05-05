"""Small regret-weighted forecast calibration pilot.

This is intentionally not a full differentiable optimizer. It is a thesis-safe
pilot that weights forecast calibration by downstream regret so that the next
DFL iteration has a measured value-oriented starting point.
"""

from __future__ import annotations

from datetime import datetime
import math
from typing import Any, Final

import polars as pl

from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    evaluate_forecast_candidates_against_oracle,
    tenant_battery_defaults_from_registry,
)

PILOT_NAME = "regret_weighted_bias_correction_v0"
REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND: Final[str] = (
    "regret_weighted_forecast_calibration_benchmark"
)
REGRET_WEIGHTED_CALIBRATED_MODEL_NAMES: Final[dict[str, str]] = {
    "tft_silver_v0": "tft_regret_weighted_calibrated_v0",
    "nbeatsx_silver_v0": "nbeatsx_regret_weighted_calibrated_v0",
}
CONTROL_MODEL_NAME: Final[str] = "strict_similar_day"


def run_regret_weighted_dfl_pilot(
    training_frame: pl.DataFrame,
    *,
    tenant_id: str,
    forecast_model_name: str,
    validation_fraction: float = 0.2,
) -> pl.DataFrame:
    """Fit a regret-weighted mean price bias on prior rows and score a holdout slice."""

    if not 0.0 < validation_fraction < 1.0:
        raise ValueError("validation_fraction must be between 0 and 1.")
    _validate_training_frame(training_frame)
    model_frame = (
        training_frame
        .filter(
            (pl.col("tenant_id") == tenant_id)
            & (pl.col("forecast_model_name") == forecast_model_name)
        )
        .sort("anchor_timestamp")
    )
    if model_frame.height < 4:
        raise ValueError("Regret-weighted DFL pilot requires at least 4 examples.")
    validation_rows = max(1, math.ceil(model_frame.height * validation_fraction))
    train_frame = model_frame.head(model_frame.height - validation_rows)
    validation_frame = model_frame.tail(validation_rows)
    if train_frame.height == 0:
        raise ValueError("Regret-weighted DFL pilot requires at least one training row.")

    bias = _weighted_mean(
        [
            float(row["mean_actual_price_uah_mwh"]) - float(row["mean_forecast_price_uah_mwh"])
            for row in train_frame.iter_rows(named=True)
        ],
        [float(row["training_weight"]) for row in train_frame.iter_rows(named=True)],
    )
    before_errors: list[float] = []
    after_errors: list[float] = []
    weights: list[float] = []
    regrets: list[float] = []
    for row in validation_frame.iter_rows(named=True):
        actual = float(row["mean_actual_price_uah_mwh"])
        forecast = float(row["mean_forecast_price_uah_mwh"])
        weight = float(row["training_weight"])
        before_errors.append(abs(actual - forecast))
        after_errors.append(abs(actual - (forecast + bias)))
        weights.append(weight)
        regrets.append(float(row["regret_uah"]))

    before_mae = _weighted_mean(before_errors, weights)
    after_mae = _weighted_mean(after_errors, weights)
    return pl.DataFrame(
        [
            {
                "pilot_name": PILOT_NAME,
                "scope": "pilot_not_full_dfl",
                "tenant_id": tenant_id,
                "forecast_model_name": forecast_model_name,
                "train_rows": train_frame.height,
                "validation_rows": validation_frame.height,
                "regret_weighted_bias_uah_mwh": bias,
                "validation_weighted_mae_before": before_mae,
                "validation_weighted_mae_after": after_mae,
                "validation_weighted_mae_delta": before_mae - after_mae,
                "mean_validation_regret_uah": sum(regrets) / len(regrets),
                "expanded_to_all_tenants_ready": after_mae <= before_mae,
                "academic_scope": (
                    "Regret-weighted forecast calibration pilot only. "
                    "This is not a full cvxpylayers DFL training loop."
                ),
            }
        ]
    )


def build_regret_weighted_forecast_calibration_frame(
    training_frame: pl.DataFrame,
    *,
    forecast_model_names: tuple[str, ...] = tuple(REGRET_WEIGHTED_CALIBRATED_MODEL_NAMES),
    min_prior_anchors: int = 14,
    rolling_calibration_window_anchors: int = 28,
) -> pl.DataFrame:
    """Build pre-anchor regret-weighted price-bias corrections for forecast models."""

    if min_prior_anchors <= 0:
        raise ValueError("min_prior_anchors must be positive.")
    if rolling_calibration_window_anchors <= 0:
        raise ValueError("rolling_calibration_window_anchors must be positive.")
    _validate_training_frame(training_frame)
    _validate_thesis_grade(training_frame)

    rows: list[dict[str, Any]] = []
    for tenant_id in sorted(set(str(value) for value in training_frame["tenant_id"].to_list())):
        tenant_frame = training_frame.filter(pl.col("tenant_id") == tenant_id)
        for source_model_name in forecast_model_names:
            corrected_model_name = _corrected_model_name(source_model_name)
            model_frame = (
                tenant_frame
                .filter(pl.col("forecast_model_name") == source_model_name)
                .sort("anchor_timestamp")
            )
            prior_rows: list[dict[str, Any]] = []
            for row in model_frame.iter_rows(named=True):
                anchor_timestamp = row["anchor_timestamp"]
                if not isinstance(anchor_timestamp, datetime):
                    raise TypeError("anchor_timestamp must be a datetime value.")
                window_rows = prior_rows[-rolling_calibration_window_anchors:]
                if len(prior_rows) < min_prior_anchors:
                    bias = 0.0
                    status = "insufficient_prior_history"
                else:
                    bias = _weighted_mean(
                        [
                            float(prior["mean_actual_price_uah_mwh"])
                            - float(prior["mean_forecast_price_uah_mwh"])
                            for prior in window_rows
                        ],
                        [float(prior["training_weight"]) for prior in window_rows],
                    )
                    status = "calibrated"
                rows.append(
                    {
                        "tenant_id": tenant_id,
                        "anchor_timestamp": anchor_timestamp,
                        "source_forecast_model_name": source_model_name,
                        "corrected_forecast_model_name": corrected_model_name,
                        "regret_weighted_bias_uah_mwh": bias,
                        "prior_anchor_count": len(prior_rows),
                        "calibration_window_anchor_count": len(window_rows),
                        "calibration_status": status,
                        "data_quality_tier": str(row.get("data_quality_tier", "demo_grade")),
                    }
                )
                prior_rows.append(row)
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(
        ["tenant_id", "anchor_timestamp", "source_forecast_model_name"]
    )


def build_regret_weighted_forecast_strategy_benchmark_frame(
    evaluation_frame: pl.DataFrame,
    calibration_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Evaluate original and regret-weighted corrected forecasts through the strict LP."""

    _validate_evaluation_frame(evaluation_frame)
    _validate_calibration_frame(calibration_frame)
    if evaluation_frame.height == 0 or calibration_frame.height == 0:
        return pl.DataFrame()

    result_frames: list[pl.DataFrame] = []
    source_rows = {
        (
            str(row["tenant_id"]),
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
            str(row["forecast_model_name"]),
        ): row
        for row in evaluation_frame.iter_rows(named=True)
    }
    calibration_rows = {
        (
            str(row["tenant_id"]),
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
            str(row["source_forecast_model_name"]),
        ): row
        for row in calibration_frame.iter_rows(named=True)
    }
    tenant_anchors = sorted(
        {
            (tenant_id, anchor_timestamp)
            for tenant_id, anchor_timestamp, _ in calibration_rows
        },
        key=lambda item: (item[0], item[1]),
    )
    for tenant_id, anchor_timestamp in tenant_anchors:
        row_by_model = {
            model_name: source_rows[(tenant_id, anchor_timestamp, model_name)]
            for model_name in [
                CONTROL_MODEL_NAME,
                "tft_silver_v0",
                "nbeatsx_silver_v0",
            ]
            if (tenant_id, anchor_timestamp, model_name) in source_rows
        }
        missing_models = {
            CONTROL_MODEL_NAME,
            "tft_silver_v0",
            "nbeatsx_silver_v0",
        }.difference(row_by_model)
        if missing_models:
            raise ValueError(
                f"evaluation_frame is missing source rows for {tenant_id} {anchor_timestamp}: "
                f"{sorted(missing_models)}"
            )

        strict_row = row_by_model[CONTROL_MODEL_NAME]
        generated_at = _datetime_value(strict_row["generated_at"], field_name="generated_at")
        strict_payload = _mapping(strict_row["evaluation_payload"])
        _require_thesis_grade_payload(strict_payload)
        candidates = [
            ForecastCandidate(
                model_name=CONTROL_MODEL_NAME,
                forecast_frame=_forecast_frame_from_payload(
                    strict_payload,
                    price_column_name="predicted_price_uah_mwh",
                    bias_uah_mwh=0.0,
                ),
                point_prediction_column="predicted_price_uah_mwh",
            )
        ]
        calibration_payloads: dict[str, dict[str, Any]] = {}
        for source_model_name in ["tft_silver_v0", "nbeatsx_silver_v0"]:
            source_row = row_by_model[source_model_name]
            source_payload = _mapping(source_row["evaluation_payload"])
            _require_thesis_grade_payload(source_payload)
            candidates.append(
                ForecastCandidate(
                    model_name=source_model_name,
                    forecast_frame=_forecast_frame_from_payload(
                        source_payload,
                        price_column_name="predicted_price_uah_mwh",
                        bias_uah_mwh=0.0,
                    ),
                    point_prediction_column="predicted_price_uah_mwh",
                )
            )
            calibration_row = calibration_rows[(tenant_id, anchor_timestamp, source_model_name)]
            corrected_model_name = str(calibration_row["corrected_forecast_model_name"])
            bias = float(calibration_row["regret_weighted_bias_uah_mwh"])
            candidates.append(
                ForecastCandidate(
                    model_name=corrected_model_name,
                    forecast_frame=_forecast_frame_from_payload(
                        source_payload,
                        price_column_name="predicted_price_uah_mwh",
                        bias_uah_mwh=bias,
                    ),
                    point_prediction_column="predicted_price_uah_mwh",
                )
            )
            calibration_payloads[corrected_model_name] = {
                "source_forecast_model_name": source_model_name,
                "regret_weighted_bias_uah_mwh": bias,
                "prior_anchor_count": int(calibration_row["prior_anchor_count"]),
                "calibration_window_anchor_count": int(
                    calibration_row["calibration_window_anchor_count"]
                ),
                "calibration_status": str(calibration_row["calibration_status"]),
            }

        price_history = _price_history_from_payload(strict_payload)
        tenant_defaults = tenant_battery_defaults_from_registry(tenant_id)
        evaluation = evaluate_forecast_candidates_against_oracle(
            price_history=price_history,
            tenant_id=tenant_id,
            battery_metrics=tenant_defaults.metrics,
            starting_soc_fraction=float(strict_row["starting_soc_fraction"]),
            starting_soc_source=str(strict_row["starting_soc_source"]),
            anchor_timestamp=anchor_timestamp,
            candidates=candidates,
            evaluation_id=_calibration_evaluation_id(
                tenant_id=tenant_id,
                anchor_timestamp=anchor_timestamp,
            ),
            generated_at=generated_at,
        )
        result_frames.append(
            _with_calibration_metadata(
                evaluation,
                source_payload=strict_payload,
                calibration_payloads=calibration_payloads,
                capacity_mwh=tenant_defaults.metrics.capacity_mwh,
            )
        )

    return pl.concat(result_frames, how="diagonal_relaxed").sort(
        ["tenant_id", "anchor_timestamp", "rank_by_regret", "forecast_model_name"]
    )


def _validate_training_frame(training_frame: pl.DataFrame) -> None:
    required_columns = {
        "tenant_id",
        "anchor_timestamp",
        "forecast_model_name",
        "mean_forecast_price_uah_mwh",
        "mean_actual_price_uah_mwh",
        "training_weight",
        "regret_uah",
    }
    missing_columns = required_columns.difference(training_frame.columns)
    if missing_columns:
        raise ValueError(f"training_frame is missing required columns: {sorted(missing_columns)}")


def _validate_thesis_grade(training_frame: pl.DataFrame) -> None:
    if "data_quality_tier" not in training_frame.columns:
        raise ValueError("training_frame must include data_quality_tier.")
    non_thesis_rows = training_frame.filter(pl.col("data_quality_tier") != "thesis_grade")
    if non_thesis_rows.height:
        raise ValueError("Regret-weighted calibration requires thesis_grade rows only.")


def _validate_evaluation_frame(evaluation_frame: pl.DataFrame) -> None:
    required_columns = {
        "evaluation_id",
        "tenant_id",
        "forecast_model_name",
        "market_venue",
        "anchor_timestamp",
        "generated_at",
        "horizon_hours",
        "starting_soc_fraction",
        "starting_soc_source",
        "evaluation_payload",
    }
    missing_columns = required_columns.difference(evaluation_frame.columns)
    if missing_columns:
        raise ValueError(f"evaluation_frame is missing required columns: {sorted(missing_columns)}")


def _validate_calibration_frame(calibration_frame: pl.DataFrame) -> None:
    required_columns = {
        "tenant_id",
        "anchor_timestamp",
        "source_forecast_model_name",
        "corrected_forecast_model_name",
        "regret_weighted_bias_uah_mwh",
        "prior_anchor_count",
        "calibration_window_anchor_count",
        "calibration_status",
        "data_quality_tier",
    }
    missing_columns = required_columns.difference(calibration_frame.columns)
    if missing_columns:
        raise ValueError(f"calibration_frame is missing required columns: {sorted(missing_columns)}")
    non_thesis_rows = calibration_frame.filter(pl.col("data_quality_tier") != "thesis_grade")
    if non_thesis_rows.height:
        raise ValueError("calibration_frame must contain thesis_grade rows only.")


def _corrected_model_name(source_model_name: str) -> str:
    if source_model_name not in REGRET_WEIGHTED_CALIBRATED_MODEL_NAMES:
        raise ValueError(f"Unsupported forecast model for calibration: {source_model_name}")
    return REGRET_WEIGHTED_CALIBRATED_MODEL_NAMES[source_model_name]


def _mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("evaluation_payload must be a mapping.")
    return value


def _require_thesis_grade_payload(payload: dict[str, Any]) -> None:
    if str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade":
        raise ValueError("Regret-weighted calibration benchmark requires thesis_grade source rows.")


def _forecast_frame_from_payload(
    payload: dict[str, Any],
    *,
    price_column_name: str,
    bias_uah_mwh: float,
) -> pl.DataFrame:
    horizon_rows = _horizon_rows(payload)
    return pl.DataFrame(
        {
            "forecast_timestamp": [
                _datetime_value(row["interval_start"], field_name="interval_start")
                for row in horizon_rows
            ],
            "source_timestamp": [
                _datetime_value(row["interval_start"], field_name="interval_start")
                for row in horizon_rows
            ],
            price_column_name: [
                float(row["forecast_price_uah_mwh"]) + bias_uah_mwh
                for row in horizon_rows
            ],
        }
    )


def _price_history_from_payload(payload: dict[str, Any]) -> pl.DataFrame:
    horizon_rows = _horizon_rows(payload)
    return pl.DataFrame(
        {
            "timestamp": [
                _datetime_value(row["interval_start"], field_name="interval_start")
                for row in horizon_rows
            ],
            "price_uah_mwh": [
                float(row["actual_price_uah_mwh"])
                for row in horizon_rows
            ],
        }
    )


def _horizon_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list):
        raise ValueError("evaluation_payload must include a horizon list.")
    rows = [row for row in horizon if isinstance(row, dict)]
    if not rows:
        raise ValueError("evaluation_payload horizon must contain rows.")
    for row in rows:
        if "interval_start" not in row:
            raise ValueError("evaluation_payload horizon rows must include interval_start.")
        if "forecast_price_uah_mwh" not in row or "actual_price_uah_mwh" not in row:
            raise ValueError("evaluation_payload horizon rows must include forecast and actual prices.")
    return rows


def _datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed
    raise TypeError(f"{field_name} must be a datetime or ISO datetime string.")


def _calibration_evaluation_id(*, tenant_id: str, anchor_timestamp: datetime) -> str:
    return f"{tenant_id}:regret-weighted-calibration:{anchor_timestamp.strftime('%Y%m%dT%H%M')}"


def _with_calibration_metadata(
    evaluation: pl.DataFrame,
    *,
    source_payload: dict[str, Any],
    calibration_payloads: dict[str, dict[str, Any]],
    capacity_mwh: float,
) -> pl.DataFrame:
    payloads: list[dict[str, Any]] = []
    efc_values: list[float] = []
    for row in evaluation.iter_rows(named=True):
        model_name = str(row["forecast_model_name"])
        payload = dict(row["evaluation_payload"])
        total_throughput_mwh = float(row["total_throughput_mwh"])
        efc_proxy = total_throughput_mwh / (2.0 * capacity_mwh)
        payload.update(
            {
                "benchmark_kind": REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
                "academic_scope": "Regret-weighted forecast calibration benchmark; not full differentiable DFL.",
                "data_quality_tier": str(source_payload.get("data_quality_tier", "demo_grade")),
                "observed_coverage_ratio": float(source_payload.get("observed_coverage_ratio", 0.0)),
                "efc_proxy": efc_proxy,
            }
        )
        if model_name in calibration_payloads:
            payload.update(calibration_payloads[model_name])
        else:
            payload.update(
                {
                    "source_forecast_model_name": model_name,
                    "regret_weighted_bias_uah_mwh": 0.0,
                    "calibration_status": "comparator_source_row",
                }
            )
        payloads.append(payload)
        efc_values.append(efc_proxy)
    return evaluation.with_columns(
        [
            pl.lit(REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND).alias("strategy_kind"),
            pl.Series("evaluation_payload", payloads),
            pl.Series("efc_proxy", efc_values),
        ]
    )


def _weighted_mean(values: list[float], weights: list[float]) -> float:
    if len(values) != len(weights) or not values:
        raise ValueError("values and weights must have the same non-zero length.")
    denominator = sum(weights)
    if denominator <= 0.0:
        raise ValueError("weights must sum to a positive value.")
    return sum(value * weight for value, weight in zip(values, weights)) / denominator
