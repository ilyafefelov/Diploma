"""Strict LP/oracle scoring for official global-panel forecast candidates."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Final

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import (
    DEFAULT_PRICE_COLUMN,
    DEFAULT_TIMESTAMP_COLUMN,
    HourlyDamBaselineSolver,
)
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    evaluate_forecast_candidates_against_oracle,
    tenant_battery_defaults_from_registry,
)

OFFICIAL_GLOBAL_PANEL_NBEATSX_STRATEGY_KIND: Final[str] = (
    "official_global_panel_nbeatsx_strict_lp_benchmark"
)
OFFICIAL_GLOBAL_PANEL_NBEATSX_CLAIM_SCOPE: Final[str] = (
    "official_global_panel_nbeatsx_strict_lp_not_full_dfl"
)
OFFICIAL_GLOBAL_PANEL_NBEATSX_MODEL_NAME: Final[str] = "nbeatsx_official_global_panel_v1"
OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME: Final[str] = (
    "nbeatsx_official_global_panel_horizon_calibrated_v1"
)
OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATION_STRATEGY_KIND: Final[str] = (
    "official_global_panel_nbeatsx_horizon_calibrated_strict_lp_benchmark"
)
OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATION_CLAIM_SCOPE: Final[str] = (
    "official_global_panel_nbeatsx_horizon_calibrated_strict_lp_not_full_dfl"
)


def build_official_global_panel_nbeatsx_strict_lp_benchmark_frame(
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    nbeatsx_official_global_panel_price_forecast: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Strict-score global-panel NBEATSx forecasts beside frozen strict control."""

    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if nbeatsx_official_global_panel_price_forecast.is_empty():
        raise ValueError("Missing global-panel NBEATSx forecast rows.")
    resolved_generated_at = generated_at or datetime.now(UTC)
    rows: list[pl.DataFrame] = []
    for tenant_id in tenant_ids:
        tenant_forecast = _tenant_forecast(
            nbeatsx_official_global_panel_price_forecast,
            tenant_id=tenant_id,
        )
        anchor_timestamp = _anchor_from_forecast(tenant_forecast)
        price_history = _tenant_price_history(
            real_data_benchmark_silver_feature_frame,
            tenant_id=tenant_id,
        )
        defaults = tenant_battery_defaults_from_registry(tenant_id)
        strict_forecast = _strict_forecast_frame(price_history, anchor_timestamp=anchor_timestamp)
        evaluation = evaluate_forecast_candidates_against_oracle(
            price_history=price_history,
            tenant_id=tenant_id,
            battery_metrics=defaults.metrics,
            starting_soc_fraction=defaults.initial_soc_fraction,
            starting_soc_source="tenant_default",
            anchor_timestamp=anchor_timestamp,
            candidates=[
                ForecastCandidate(
                    model_name="strict_similar_day",
                    forecast_frame=strict_forecast,
                    point_prediction_column="predicted_price_uah_mwh",
                ),
                ForecastCandidate(
                    model_name=OFFICIAL_GLOBAL_PANEL_NBEATSX_MODEL_NAME,
                    forecast_frame=tenant_forecast,
                    point_prediction_column="predicted_price_uah_mwh",
                ),
            ],
            evaluation_id=f"{tenant_id}:official-global-panel-nbeatsx:{anchor_timestamp:%Y%m%dT%H%M}",
            generated_at=resolved_generated_at,
        )
        rows.append(
            _with_global_panel_metadata(
                evaluation,
                tenant_id=tenant_id,
                anchor_timestamp=anchor_timestamp,
                price_history=price_history,
            )
        )
    return pl.concat(rows, how="diagonal_relaxed").sort(
        ["tenant_id", "anchor_timestamp", "rank_by_regret", "forecast_model_name"]
    )


def build_official_global_panel_nbeatsx_horizon_calibration_frame(
    evaluation_frame: pl.DataFrame,
    *,
    min_prior_anchors: int = 14,
    rolling_calibration_window_anchors: int = 28,
) -> pl.DataFrame:
    """Build prior-only horizon biases for official global-panel NBEATSx rows."""

    if min_prior_anchors <= 0:
        raise ValueError("min_prior_anchors must be positive.")
    if rolling_calibration_window_anchors <= 0:
        raise ValueError("rolling_calibration_window_anchors must be positive.")
    _validate_evaluation_frame(evaluation_frame)

    rows: list[dict[str, Any]] = []
    tenant_ids = sorted(str(value) for value in set(evaluation_frame["tenant_id"].to_list()))
    for tenant_id in tenant_ids:
        model_frame = (
            evaluation_frame
            .filter(
                (pl.col("tenant_id") == tenant_id)
                & (pl.col("forecast_model_name") == OFFICIAL_GLOBAL_PANEL_NBEATSX_MODEL_NAME)
            )
            .sort("anchor_timestamp")
        )
        prior_rows: list[dict[str, Any]] = []
        for row in model_frame.iter_rows(named=True):
            anchor_timestamp = _datetime_value(
                row["anchor_timestamp"],
                field_name="anchor_timestamp",
            )
            payload = _payload(row["evaluation_payload"])
            _require_thesis_grade_payload(payload)
            horizon_count = len(_horizon_rows(payload))
            window_rows = prior_rows[-rolling_calibration_window_anchors:]
            if len(prior_rows) < min_prior_anchors:
                horizon_biases = [0.0 for _ in range(horizon_count)]
                status = "insufficient_prior_history"
            else:
                horizon_biases = _weighted_horizon_biases(
                    window_rows,
                    horizon_count=horizon_count,
                )
                status = "calibrated"
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "anchor_timestamp": anchor_timestamp,
                    "source_forecast_model_name": OFFICIAL_GLOBAL_PANEL_NBEATSX_MODEL_NAME,
                    "corrected_forecast_model_name": (
                        OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME
                    ),
                    "horizon_biases_uah_mwh": horizon_biases,
                    "mean_horizon_bias_uah_mwh": _mean_float(horizon_biases),
                    "max_abs_horizon_bias_uah_mwh": max(
                        (abs(value) for value in horizon_biases),
                        default=0.0,
                    ),
                    "prior_anchor_count": len(prior_rows),
                    "calibration_window_anchor_count": len(window_rows),
                    "calibration_status": status,
                    "data_quality_tier": str(payload.get("data_quality_tier", "demo_grade")),
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
            prior_rows.append(row)
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(["tenant_id", "anchor_timestamp"])


def build_official_global_panel_nbeatsx_horizon_calibrated_strict_lp_benchmark_frame(
    evaluation_frame: pl.DataFrame,
    calibration_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict-score raw and prior-only calibrated official global-panel NBEATSx."""

    _validate_evaluation_frame(evaluation_frame)
    _validate_calibration_frame(calibration_frame)
    if evaluation_frame.is_empty() or calibration_frame.is_empty():
        return pl.DataFrame()

    source_rows = {
        (
            str(row["tenant_id"]),
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
            str(row["forecast_model_name"]),
        ): row
        for row in evaluation_frame.iter_rows(named=True)
    }
    result_frames: list[pl.DataFrame] = []
    for calibration_row in calibration_frame.sort(["tenant_id", "anchor_timestamp"]).iter_rows(named=True):
        tenant_id = str(calibration_row["tenant_id"])
        anchor_timestamp = _datetime_value(
            calibration_row["anchor_timestamp"],
            field_name="anchor_timestamp",
        )
        strict_key = (tenant_id, anchor_timestamp, "strict_similar_day")
        source_key = (tenant_id, anchor_timestamp, OFFICIAL_GLOBAL_PANEL_NBEATSX_MODEL_NAME)
        if strict_key not in source_rows:
            raise ValueError(
                f"evaluation_frame is missing strict_similar_day for {tenant_id} {anchor_timestamp}."
            )
        if source_key not in source_rows:
            raise ValueError(
                "evaluation_frame is missing nbeatsx_official_global_panel_v1 "
                f"for {tenant_id} {anchor_timestamp}."
            )
        strict_row = source_rows[strict_key]
        source_row = source_rows[source_key]
        strict_payload = _payload(strict_row["evaluation_payload"])
        source_payload = _payload(source_row["evaluation_payload"])
        _require_thesis_grade_payload(strict_payload)
        _require_thesis_grade_payload(source_payload)
        horizon_biases = _float_list(calibration_row["horizon_biases_uah_mwh"])
        candidates = [
            ForecastCandidate(
                model_name="strict_similar_day",
                forecast_frame=_forecast_frame_from_payload(
                    strict_payload,
                    price_column_name="predicted_price_uah_mwh",
                    horizon_biases=None,
                ),
                point_prediction_column="predicted_price_uah_mwh",
            ),
            ForecastCandidate(
                model_name=OFFICIAL_GLOBAL_PANEL_NBEATSX_MODEL_NAME,
                forecast_frame=_forecast_frame_from_payload(
                    source_payload,
                    price_column_name="predicted_price_uah_mwh",
                    horizon_biases=None,
                ),
                point_prediction_column="predicted_price_uah_mwh",
            ),
            ForecastCandidate(
                model_name=OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME,
                forecast_frame=_forecast_frame_from_payload(
                    source_payload,
                    price_column_name="predicted_price_uah_mwh",
                    horizon_biases=horizon_biases,
                ),
                point_prediction_column="predicted_price_uah_mwh",
            ),
        ]
        tenant_defaults = tenant_battery_defaults_from_registry(tenant_id)
        evaluation = evaluate_forecast_candidates_against_oracle(
            price_history=_price_history_from_payload(source_payload),
            tenant_id=tenant_id,
            battery_metrics=tenant_defaults.metrics,
            starting_soc_fraction=float(strict_row["starting_soc_fraction"]),
            starting_soc_source=str(strict_row["starting_soc_source"]),
            anchor_timestamp=anchor_timestamp,
            candidates=candidates,
            evaluation_id=(
                f"{tenant_id}:official-global-panel-nbeatsx-calibrated:"
                f"{anchor_timestamp:%Y%m%dT%H%M}"
            ),
            generated_at=_datetime_value(strict_row["generated_at"], field_name="generated_at"),
        )
        result_frames.append(
            _with_calibration_metadata(
                evaluation,
                source_payload=source_payload,
                calibration_row=calibration_row,
            )
        )
    return pl.concat(result_frames, how="diagonal_relaxed").sort(
        ["tenant_id", "anchor_timestamp", "rank_by_regret", "forecast_model_name"]
    )


def _tenant_forecast(forecast_frame: pl.DataFrame, *, tenant_id: str) -> pl.DataFrame:
    required_columns = {"unique_id", "forecast_timestamp", "predicted_price_uah_mwh"}
    missing_columns = required_columns.difference(forecast_frame.columns)
    if missing_columns:
        raise ValueError(f"global-panel NBEATSx forecast is missing columns: {sorted(missing_columns)}")
    unique_id = f"{tenant_id}:DAM"
    tenant_forecast = (
        forecast_frame
        .filter(pl.col("unique_id") == unique_id)
        .select(["forecast_timestamp", "predicted_price_uah_mwh"])
        .sort("forecast_timestamp")
    )
    if tenant_forecast.is_empty():
        raise ValueError(f"Missing global-panel NBEATSx forecast rows for tenant_id={tenant_id}.")
    return tenant_forecast


def _tenant_price_history(silver_frame: pl.DataFrame, *, tenant_id: str) -> pl.DataFrame:
    required_columns = {"tenant_id", DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN}
    missing_columns = required_columns.difference(silver_frame.columns)
    if missing_columns:
        raise ValueError(f"real_data_benchmark_silver_feature_frame is missing columns: {sorted(missing_columns)}")
    tenant_frame = (
        silver_frame
        .filter(pl.col("tenant_id") == tenant_id)
        .drop("tenant_id")
        .drop_nulls(subset=[DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN])
        .unique(subset=[DEFAULT_TIMESTAMP_COLUMN], keep="last")
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )
    if tenant_frame.is_empty():
        raise ValueError(f"Missing Silver benchmark rows for tenant_id={tenant_id}.")
    if "source_kind" in tenant_frame.columns and tenant_frame.filter(pl.col("source_kind") != "observed").height:
        raise ValueError("official global-panel strict benchmark requires observed source rows.")
    return tenant_frame


def _anchor_from_forecast(forecast_frame: pl.DataFrame) -> datetime:
    first_timestamp = forecast_frame.select("forecast_timestamp").to_series().item(0)
    if not isinstance(first_timestamp, datetime):
        raise TypeError("forecast_timestamp column must contain datetime values.")
    return first_timestamp - timedelta(hours=1)


def _strict_forecast_frame(price_history: pl.DataFrame, *, anchor_timestamp: datetime) -> pl.DataFrame:
    historical_prices = price_history.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) <= anchor_timestamp)
    forecast = HourlyDamBaselineSolver().build_forecast(
        historical_prices,
        anchor_timestamp=anchor_timestamp,
    )
    return pl.DataFrame(
        {
            "forecast_timestamp": [point.forecast_timestamp for point in forecast],
            "source_timestamp": [point.source_timestamp for point in forecast],
            "predicted_price_uah_mwh": [point.predicted_price_uah_mwh for point in forecast],
        }
    )


def _with_global_panel_metadata(
    evaluation: pl.DataFrame,
    *,
    tenant_id: str,
    anchor_timestamp: datetime,
    price_history: pl.DataFrame,
) -> pl.DataFrame:
    payloads: list[dict[str, Any]] = []
    data_quality_tier = _data_quality_tier(price_history)
    observed_coverage_ratio = _observed_coverage_ratio(price_history)
    for row in evaluation.iter_rows(named=True):
        payload = dict(row["evaluation_payload"])
        payload.update(
            {
                "claim_scope": OFFICIAL_GLOBAL_PANEL_NBEATSX_CLAIM_SCOPE,
                "benchmark_kind": OFFICIAL_GLOBAL_PANEL_NBEATSX_STRATEGY_KIND,
                "data_quality_tier": data_quality_tier,
                "observed_coverage_ratio": observed_coverage_ratio,
                "tenant_id": tenant_id,
                "anchor_timestamp": anchor_timestamp.isoformat(),
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
        payloads.append(payload)
    return evaluation.with_columns(
        [
            pl.lit(OFFICIAL_GLOBAL_PANEL_NBEATSX_STRATEGY_KIND).alias("strategy_kind"),
            pl.Series("evaluation_payload", payloads),
        ]
    )


def _data_quality_tier(price_history: pl.DataFrame) -> str:
    if "source_kind" not in price_history.columns:
        return "demo_grade"
    source_kinds = {str(value) for value in price_history["source_kind"].to_list()}
    return "thesis_grade" if source_kinds == {"observed"} else "demo_grade"


def _observed_coverage_ratio(price_history: pl.DataFrame) -> float:
    if price_history.is_empty() or "source_kind" not in price_history.columns:
        return 0.0
    return price_history.filter(pl.col("source_kind") == "observed").height / price_history.height


def _validate_evaluation_frame(evaluation_frame: pl.DataFrame) -> None:
    required_columns = {
        "evaluation_id",
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "generated_at",
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
        "horizon_biases_uah_mwh",
        "mean_horizon_bias_uah_mwh",
        "max_abs_horizon_bias_uah_mwh",
        "prior_anchor_count",
        "calibration_window_anchor_count",
        "calibration_status",
        "data_quality_tier",
        "not_full_dfl",
        "not_market_execution",
    }
    missing_columns = required_columns.difference(calibration_frame.columns)
    if missing_columns:
        raise ValueError(f"calibration_frame is missing required columns: {sorted(missing_columns)}")
    non_thesis_rows = calibration_frame.filter(pl.col("data_quality_tier") != "thesis_grade")
    if non_thesis_rows.height:
        raise ValueError("official global-panel calibration requires thesis_grade rows only.")
    if calibration_frame.filter(~pl.col("not_full_dfl") | ~pl.col("not_market_execution")).height:
        raise ValueError("official global-panel calibration must remain research-only.")


def _payload(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("evaluation_payload must be a mapping.")
    return value


def _require_thesis_grade_payload(payload: dict[str, Any]) -> None:
    if str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade":
        raise ValueError("official global-panel calibration requires thesis_grade source rows.")
    if payload.get("not_full_dfl") is not True or payload.get("not_market_execution") is not True:
        raise ValueError("official global-panel calibration requires research-only claim flags.")


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


def _forecast_frame_from_payload(
    payload: dict[str, Any],
    *,
    price_column_name: str,
    horizon_biases: list[float] | None,
) -> pl.DataFrame:
    horizon_rows = _horizon_rows(payload)
    if horizon_biases is not None and len(horizon_biases) != len(horizon_rows):
        raise ValueError("horizon_biases_uah_mwh must match forecast horizon length.")
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
                float(row["forecast_price_uah_mwh"])
                + (horizon_biases[index] if horizon_biases is not None else 0.0)
                for index, row in enumerate(horizon_rows)
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
            "price_uah_mwh": [float(row["actual_price_uah_mwh"]) for row in horizon_rows],
        }
    )


def _with_calibration_metadata(
    evaluation: pl.DataFrame,
    *,
    source_payload: dict[str, Any],
    calibration_row: dict[str, Any],
) -> pl.DataFrame:
    payloads: list[dict[str, Any]] = []
    horizon_biases = _float_list(calibration_row["horizon_biases_uah_mwh"])
    for row in evaluation.iter_rows(named=True):
        model_name = str(row["forecast_model_name"])
        payload = dict(row["evaluation_payload"])
        payload.update(
            {
                "claim_scope": OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATION_CLAIM_SCOPE,
                "benchmark_kind": OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATION_STRATEGY_KIND,
                "data_quality_tier": str(source_payload.get("data_quality_tier", "demo_grade")),
                "observed_coverage_ratio": float(source_payload.get("observed_coverage_ratio", 0.0)),
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
        if model_name == OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME:
            payload.update(
                {
                    "source_forecast_model_name": OFFICIAL_GLOBAL_PANEL_NBEATSX_MODEL_NAME,
                    "horizon_biases_uah_mwh": horizon_biases,
                    "mean_horizon_bias_uah_mwh": float(
                        calibration_row["mean_horizon_bias_uah_mwh"]
                    ),
                    "max_abs_horizon_bias_uah_mwh": float(
                        calibration_row["max_abs_horizon_bias_uah_mwh"]
                    ),
                    "prior_anchor_count": int(calibration_row["prior_anchor_count"]),
                    "calibration_window_anchor_count": int(
                        calibration_row["calibration_window_anchor_count"]
                    ),
                    "calibration_status": str(calibration_row["calibration_status"]),
                }
            )
        else:
            payload.update(
                {
                    "source_forecast_model_name": model_name,
                    "horizon_biases_uah_mwh": [],
                    "mean_horizon_bias_uah_mwh": 0.0,
                    "max_abs_horizon_bias_uah_mwh": 0.0,
                    "prior_anchor_count": 0,
                    "calibration_window_anchor_count": 0,
                    "calibration_status": "comparator_source_row",
                }
            )
        payloads.append(payload)
    return evaluation.with_columns(
        [
            pl.lit(OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATION_STRATEGY_KIND).alias(
                "strategy_kind"
            ),
            pl.Series("evaluation_payload", payloads),
        ]
    )


def _weighted_horizon_biases(
    prior_rows: list[dict[str, Any]],
    *,
    horizon_count: int,
) -> list[float]:
    step_values: list[list[float]] = [[] for _ in range(horizon_count)]
    step_weights: list[list[float]] = [[] for _ in range(horizon_count)]
    for prior_row in prior_rows:
        payload = _payload(prior_row["evaluation_payload"])
        weight = 1.0 + max(0.0, float(prior_row.get("regret_ratio", 0.0)))
        for default_step_index, horizon_row in enumerate(_horizon_rows(payload)):
            step_index = int(horizon_row.get("step_index", default_step_index))
            if 0 <= step_index < horizon_count:
                step_values[step_index].append(
                    float(horizon_row["actual_price_uah_mwh"])
                    - float(horizon_row["forecast_price_uah_mwh"])
                )
                step_weights[step_index].append(weight)
    return [
        _weighted_mean(values, weights) if values else 0.0
        for values, weights in zip(step_values, step_weights)
    ]


def _weighted_mean(values: list[float], weights: list[float]) -> float:
    if len(values) != len(weights) or not values:
        raise ValueError("values and weights must have the same non-zero length.")
    denominator = sum(weights)
    if denominator <= 0.0:
        raise ValueError("weights must sum to a positive value.")
    return sum(value * weight for value, weight in zip(values, weights)) / denominator


def _mean_float(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _float_list(value: Any) -> list[float]:
    if not isinstance(value, list):
        raise ValueError("horizon_biases_uah_mwh must be a list.")
    return [float(item) for item in value]


def _datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed
    raise TypeError(f"{field_name} must be a datetime or ISO datetime string.")
