"""Forecast-pipeline truth audit before larger DFL experiments."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from math import isfinite
from statistics import mean
from typing import Any, Final

import polars as pl

from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_FORECAST_PIPELINE_TRUTH_AUDIT_CLAIM_SCOPE: Final[str] = (
    "dfl_forecast_pipeline_truth_audit_not_full_dfl"
)
DFL_FORECAST_PIPELINE_TRUTH_AUDIT_ACADEMIC_SCOPE: Final[str] = (
    "Forecast-pipeline truth audit for rolling-origin forecast vectors, timestamp "
    "alignment, source provenance, and unit sanity before full DFL/DT experiments. "
    "This is research-only diagnostics, not full DFL and not market execution."
)

REQUIRED_BENCHMARK_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "horizon_hours",
        "evaluation_payload",
    }
)
REQUIRED_TRUTH_AUDIT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "forecast_model_name",
        "tenant_count",
        "anchor_count",
        "row_count",
        "horizon_hours_min",
        "horizon_hours_max",
        "data_quality_tiers",
        "observed_coverage_min",
        "non_thesis_grade_rows",
        "non_observed_rows",
        "unit_sanity_failure_count",
        "vector_round_trip_failure_count",
        "horizon_order_failure_count",
        "leaky_horizon_row_count",
        "horizon_gap_failure_count",
        "zero_shift_best_anchor_count",
        "shifted_better_anchor_count",
        "best_shift_offset_counts",
        "perfect_forecast_anchor_count",
        "perfect_forecast_sanity_passed",
        "blocking_failure_count",
        "diagnostic_warning_count",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_forecast_pipeline_truth_audit_frame(
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    *,
    price_floor_uah_mwh: float = 0.0,
    price_cap_uah_mwh: float = 16_000.0,
    horizon_shift_offsets: tuple[int, ...] = (-2, -1, 0, 1, 2),
    vector_tolerance: float = 1e-9,
) -> pl.DataFrame:
    """Summarize forecast-vector alignment and provenance diagnostics by model."""

    _require_columns(
        real_data_rolling_origin_benchmark_frame,
        REQUIRED_BENCHMARK_COLUMNS,
        frame_name="real_data_rolling_origin_benchmark_frame",
    )
    _validate_config(
        price_floor_uah_mwh=price_floor_uah_mwh,
        price_cap_uah_mwh=price_cap_uah_mwh,
        horizon_shift_offsets=horizon_shift_offsets,
        vector_tolerance=vector_tolerance,
    )

    diagnostics = [
        _row_diagnostics(
            row,
            price_floor_uah_mwh=price_floor_uah_mwh,
            price_cap_uah_mwh=price_cap_uah_mwh,
            horizon_shift_offsets=horizon_shift_offsets,
            vector_tolerance=vector_tolerance,
        )
        for row in real_data_rolling_origin_benchmark_frame.iter_rows(named=True)
    ]
    if not diagnostics:
        return pl.DataFrame(schema={column: pl.Null for column in REQUIRED_TRUTH_AUDIT_COLUMNS})

    rows = [
        _summary_row(
            forecast_model_name,
            model_diagnostics,
            vector_tolerance=vector_tolerance,
        )
        for forecast_model_name, model_diagnostics in sorted(_group_by_model(diagnostics).items())
    ]
    return pl.DataFrame(rows).sort("forecast_model_name")


def validate_forecast_pipeline_truth_audit_evidence(
    audit_frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate that forecast-pipeline truth diagnostics have no blocking failures."""

    missing_columns = sorted(REQUIRED_TRUTH_AUDIT_COLUMNS.difference(audit_frame.columns))
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"forecast pipeline truth audit is missing columns: {missing_columns}",
            {"row_count": audit_frame.height},
        )
    if audit_frame.height == 0:
        return EvidenceCheckOutcome(False, "forecast pipeline truth audit has no rows", {"row_count": 0})

    rows = list(audit_frame.iter_rows(named=True))
    blocking_failures = sum(int(row["blocking_failure_count"]) for row in rows)
    claim_flag_failures = [
        row
        for row in rows
        if str(row["claim_scope"]) != DFL_FORECAST_PIPELINE_TRUTH_AUDIT_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
    ]
    non_thesis_rows = sum(int(row["non_thesis_grade_rows"]) for row in rows)
    non_observed_rows = sum(int(row["non_observed_rows"]) for row in rows)
    unit_failures = sum(int(row["unit_sanity_failure_count"]) for row in rows)
    horizon_failures = sum(
        int(row["horizon_order_failure_count"])
        + int(row["leaky_horizon_row_count"])
        + int(row["horizon_gap_failure_count"])
        for row in rows
    )
    round_trip_failures = sum(int(row["vector_round_trip_failure_count"]) for row in rows)
    warning_count = sum(int(row["diagnostic_warning_count"]) for row in rows)
    failures: list[str] = []
    if blocking_failures:
        failures.append("forecast pipeline truth audit has blocking failures")
    if claim_flag_failures:
        failures.append("forecast pipeline truth audit claim flags must remain research-only")

    return EvidenceCheckOutcome(
        not failures,
        "Forecast pipeline truth audit passed." if not failures else "; ".join(failures),
        {
            "row_count": audit_frame.height,
            "forecast_model_names": sorted(str(row["forecast_model_name"]) for row in rows),
            "blocking_failure_count": blocking_failures,
            "diagnostic_warning_count": warning_count,
            "non_thesis_grade_rows": non_thesis_rows,
            "non_observed_rows": non_observed_rows,
            "unit_sanity_failure_count": unit_failures,
            "horizon_failure_count": horizon_failures,
            "vector_round_trip_failure_count": round_trip_failures,
            "claim_flag_failure_rows": len(claim_flag_failures),
        },
    )


def _validate_config(
    *,
    price_floor_uah_mwh: float,
    price_cap_uah_mwh: float,
    horizon_shift_offsets: tuple[int, ...],
    vector_tolerance: float,
) -> None:
    if not isfinite(price_floor_uah_mwh):
        raise ValueError("price_floor_uah_mwh must be finite.")
    if not isfinite(price_cap_uah_mwh) or price_cap_uah_mwh <= price_floor_uah_mwh:
        raise ValueError("price_cap_uah_mwh must be finite and above the floor.")
    if not horizon_shift_offsets:
        raise ValueError("horizon_shift_offsets must contain at least one offset.")
    if 0 not in horizon_shift_offsets:
        raise ValueError("horizon_shift_offsets must include zero.")
    if vector_tolerance < 0:
        raise ValueError("vector_tolerance must not be negative.")


def _row_diagnostics(
    row: dict[str, Any],
    *,
    price_floor_uah_mwh: float,
    price_cap_uah_mwh: float,
    horizon_shift_offsets: tuple[int, ...],
    vector_tolerance: float,
) -> dict[str, Any]:
    payload = _payload(row)
    horizon = _horizon(payload)
    forecast_prices = _price_vector(horizon, "forecast_price_uah_mwh")
    actual_prices = _price_vector(horizon, "actual_price_uah_mwh")
    interval_starts = [_datetime_value(point["interval_start"], field_name="interval_start") for point in horizon]
    anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    best_shift = _best_shift_offset(
        forecast_prices,
        actual_prices,
        horizon_shift_offsets=horizon_shift_offsets,
    )
    zero_shift_mae = _shift_mae(forecast_prices, actual_prices, shift_offset=0)
    best_shift_mae = _shift_mae(forecast_prices, actual_prices, shift_offset=best_shift)
    perfect_forecast_gap = _max_abs_gap(forecast_prices, actual_prices)
    data_quality_tier = str(payload.get("data_quality_tier", "demo_grade"))
    observed_coverage_ratio = float(payload.get("observed_coverage_ratio", 0.0))
    return {
        "tenant_id": str(row["tenant_id"]),
        "forecast_model_name": str(row["forecast_model_name"]),
        "anchor_timestamp": anchor_timestamp,
        "horizon_hours": int(row["horizon_hours"]),
        "data_quality_tier": data_quality_tier,
        "observed_coverage_ratio": observed_coverage_ratio,
        "unit_sanity_failed": _unit_sanity_failed(
            forecast_prices,
            actual_prices,
            price_floor_uah_mwh=price_floor_uah_mwh,
            price_cap_uah_mwh=price_cap_uah_mwh,
        ),
        "vector_round_trip_failed": not _vector_round_trips(forecast_prices, actual_prices),
        "horizon_order_failed": not _horizon_order_is_valid(horizon, interval_starts),
        "leaky_horizon_row_count": _leaky_horizon_row_count(anchor_timestamp, interval_starts),
        "horizon_gap_failed": _horizon_gap_failed(interval_starts),
        "zero_shift_best": best_shift == 0,
        "shifted_better": best_shift != 0 and best_shift_mae + vector_tolerance < zero_shift_mae,
        "best_shift_offset": best_shift,
        "perfect_forecast_gap_uah_mwh": perfect_forecast_gap,
        "perfect_forecast": perfect_forecast_gap <= vector_tolerance,
    }


def _summary_row(
    forecast_model_name: str,
    diagnostics: list[dict[str, Any]],
    *,
    vector_tolerance: float,
) -> dict[str, Any]:
    data_quality_tiers = sorted({str(row["data_quality_tier"]) for row in diagnostics})
    observed_coverage_min = min(float(row["observed_coverage_ratio"]) for row in diagnostics)
    non_thesis_grade_rows = sum(1 for row in diagnostics if str(row["data_quality_tier"]) != "thesis_grade")
    non_observed_rows = sum(1 for row in diagnostics if float(row["observed_coverage_ratio"]) < 1.0)
    unit_failures = sum(1 for row in diagnostics if bool(row["unit_sanity_failed"]))
    round_trip_failures = sum(1 for row in diagnostics if bool(row["vector_round_trip_failed"]))
    horizon_order_failures = sum(1 for row in diagnostics if bool(row["horizon_order_failed"]))
    leaky_horizon_rows = sum(int(row["leaky_horizon_row_count"]) for row in diagnostics)
    horizon_gap_failures = sum(1 for row in diagnostics if bool(row["horizon_gap_failed"]))
    shifted_better = sum(1 for row in diagnostics if bool(row["shifted_better"]))
    zero_shift_best = sum(1 for row in diagnostics if bool(row["zero_shift_best"]))
    perfect_count = sum(1 for row in diagnostics if bool(row["perfect_forecast"]))
    perfect_max_gap = max(
        (float(row["perfect_forecast_gap_uah_mwh"]) for row in diagnostics if bool(row["perfect_forecast"])),
        default=0.0,
    )
    blocking_failure_count = (
        non_thesis_grade_rows
        + non_observed_rows
        + unit_failures
        + round_trip_failures
        + horizon_order_failures
        + leaky_horizon_rows
        + horizon_gap_failures
    )
    return {
        "forecast_model_name": forecast_model_name,
        "tenant_count": len({str(row["tenant_id"]) for row in diagnostics}),
        "anchor_count": len({row["anchor_timestamp"] for row in diagnostics}),
        "row_count": len(diagnostics),
        "horizon_hours_min": min(int(row["horizon_hours"]) for row in diagnostics),
        "horizon_hours_max": max(int(row["horizon_hours"]) for row in diagnostics),
        "data_quality_tiers": data_quality_tiers,
        "observed_coverage_min": observed_coverage_min,
        "non_thesis_grade_rows": non_thesis_grade_rows,
        "non_observed_rows": non_observed_rows,
        "unit_sanity_failure_count": unit_failures,
        "vector_round_trip_failure_count": round_trip_failures,
        "horizon_order_failure_count": horizon_order_failures,
        "leaky_horizon_row_count": leaky_horizon_rows,
        "horizon_gap_failure_count": horizon_gap_failures,
        "zero_shift_best_anchor_count": zero_shift_best,
        "shifted_better_anchor_count": shifted_better,
        "best_shift_offset_counts": _best_shift_offset_counts(diagnostics),
        "perfect_forecast_anchor_count": perfect_count,
        "perfect_forecast_max_abs_gap_uah_mwh": perfect_max_gap,
        "perfect_forecast_sanity_passed": perfect_max_gap <= vector_tolerance,
        "blocking_failure_count": blocking_failure_count,
        "diagnostic_warning_count": shifted_better,
        "claim_scope": DFL_FORECAST_PIPELINE_TRUTH_AUDIT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload")
    if not isinstance(payload, dict):
        raise ValueError("evaluation_payload must be a dictionary.")
    return payload


def _horizon(payload: dict[str, Any]) -> list[dict[str, Any]]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list) or not horizon:
        raise ValueError("evaluation_payload.horizon must contain at least one horizon point.")
    if any(not isinstance(point, dict) for point in horizon):
        raise ValueError("evaluation_payload.horizon must contain dictionary points.")
    return horizon


def _price_vector(horizon: list[dict[str, Any]], field_name: str) -> list[float]:
    prices: list[float] = []
    for point in sorted(horizon, key=lambda item: int(item.get("step_index", 0))):
        try:
            prices.append(float(point[field_name]))
        except KeyError as exc:
            raise ValueError(f"horizon point is missing {field_name}.") from exc
    return prices


def _datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError(f"{field_name} must be a datetime or ISO datetime string.")
    if parsed.tzinfo is not None:
        return parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


def _unit_sanity_failed(
    forecast_prices: list[float],
    actual_prices: list[float],
    *,
    price_floor_uah_mwh: float,
    price_cap_uah_mwh: float,
) -> bool:
    for price in [*forecast_prices, *actual_prices]:
        if not isfinite(price):
            return True
        if price < price_floor_uah_mwh or price > price_cap_uah_mwh:
            return True
    return False


def _vector_round_trips(forecast_prices: list[float], actual_prices: list[float]) -> bool:
    try:
        encoded = json.dumps(
            {"forecast": forecast_prices, "actual": actual_prices},
            allow_nan=False,
            separators=(",", ":"),
        )
    except ValueError:
        return False
    decoded = json.loads(encoded)
    return _vectors_match(forecast_prices, decoded["forecast"]) and _vectors_match(
        actual_prices,
        decoded["actual"],
    )


def _horizon_order_is_valid(horizon: list[dict[str, Any]], interval_starts: list[datetime]) -> bool:
    step_indices = [int(point.get("step_index", index)) for index, point in enumerate(horizon)]
    return step_indices == list(range(len(step_indices))) and interval_starts == sorted(interval_starts)


def _leaky_horizon_row_count(anchor_timestamp: datetime, interval_starts: list[datetime]) -> int:
    return sum(1 for timestamp in interval_starts if timestamp <= anchor_timestamp)


def _horizon_gap_failed(interval_starts: list[datetime]) -> bool:
    if len(interval_starts) < 2:
        return False
    return any(
        current - previous != timedelta(hours=1)
        for previous, current in zip(interval_starts, interval_starts[1:])
    )


def _best_shift_offset(
    forecast_prices: list[float],
    actual_prices: list[float],
    *,
    horizon_shift_offsets: tuple[int, ...],
) -> int:
    return min(
        horizon_shift_offsets,
        key=lambda offset: (_shift_mae(forecast_prices, actual_prices, shift_offset=offset), abs(offset), offset),
    )


def _shift_mae(forecast_prices: list[float], actual_prices: list[float], *, shift_offset: int) -> float:
    errors: list[float] = []
    for index, forecast_price in enumerate(forecast_prices):
        actual_index = index + shift_offset
        if 0 <= actual_index < len(actual_prices):
            errors.append(abs(forecast_price - actual_prices[actual_index]))
    return mean(errors) if errors else float("inf")


def _max_abs_gap(forecast_prices: list[float], actual_prices: list[float]) -> float:
    return max(
        (
            abs(forecast_price - actual_price)
            for forecast_price, actual_price in zip(forecast_prices, actual_prices, strict=True)
        ),
        default=0.0,
    )


def _vectors_match(left: list[float], right: list[float]) -> bool:
    if len(left) != len(right):
        return False
    return all(left_value == right_value for left_value, right_value in zip(left, right, strict=True))


def _group_by_model(diagnostics: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in diagnostics:
        groups.setdefault(str(row["forecast_model_name"]), []).append(row)
    return groups


def _best_shift_offset_counts(diagnostics: list[dict[str, Any]]) -> list[dict[str, int]]:
    offsets = sorted({int(row["best_shift_offset"]) for row in diagnostics})
    return [
        {
            "shift_offset_hours": offset,
            "anchor_count": sum(1 for row in diagnostics if int(row["best_shift_offset"]) == offset),
        }
        for offset in offsets
    ]


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"{frame_name} is missing required columns: {missing_columns}")
