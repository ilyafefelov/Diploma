"""Build vector-rich DFL training examples from strategy evaluations."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Final, Literal

import polars as pl

from smart_arbitrage.dfl.schemas import DFLTrainingExampleV2

CONTROL_MODEL_NAME: Final[Literal["strict_similar_day"]] = "strict_similar_day"
CLAIM_SCOPE: Final[Literal["dfl_training_examples_not_full_dfl"]] = "dfl_training_examples_not_full_dfl"
REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "evaluation_id",
        "tenant_id",
        "forecast_model_name",
        "strategy_kind",
        "market_venue",
        "anchor_timestamp",
        "generated_at",
        "horizon_hours",
        "decision_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "evaluation_payload",
    }
)


def build_dfl_training_example_frame(
    evaluation_frame: pl.DataFrame,
    *,
    require_thesis_grade: bool = True,
) -> pl.DataFrame:
    """Create vector-rich DFL examples while preserving the existing summary frame."""

    _validate_evaluation_frame(evaluation_frame)
    if evaluation_frame.height == 0:
        return pl.DataFrame()
    strict_rows = _strict_rows_by_anchor(evaluation_frame)
    rows: list[dict[str, Any]] = []
    for row in evaluation_frame.sort(["tenant_id", "anchor_timestamp", "forecast_model_name"]).iter_rows(named=True):
        payload = _payload(row)
        if require_thesis_grade and _data_quality_tier(payload) != "thesis_grade":
            raise ValueError("DFL training examples require thesis_grade rows.")
        key = _anchor_key(row)
        if key not in strict_rows:
            raise ValueError("Each tenant/anchor must include a strict_similar_day control row.")
        strict_row = strict_rows[key]
        strict_payload = _payload(strict_row)
        if require_thesis_grade and _data_quality_tier(strict_payload) != "thesis_grade":
            raise ValueError("DFL training examples require a thesis_grade strict_similar_day row.")
        example = _example_from_rows(row=row, payload=payload, strict_row=strict_row, strict_payload=strict_payload)
        rows.append(example.model_dump(mode="python"))
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


def _validate_evaluation_frame(evaluation_frame: pl.DataFrame) -> None:
    missing_columns = REQUIRED_COLUMNS.difference(evaluation_frame.columns)
    if missing_columns:
        raise ValueError(f"evaluation_frame is missing required columns: {sorted(missing_columns)}")


def _strict_rows_by_anchor(evaluation_frame: pl.DataFrame) -> dict[tuple[str, datetime], dict[str, Any]]:
    rows: dict[tuple[str, datetime], dict[str, Any]] = {}
    for row in evaluation_frame.filter(pl.col("forecast_model_name") == CONTROL_MODEL_NAME).iter_rows(named=True):
        rows[_anchor_key(row)] = row
    return rows


def _example_from_rows(
    *,
    row: dict[str, Any],
    payload: dict[str, Any],
    strict_row: dict[str, Any],
    strict_payload: dict[str, Any],
) -> DFLTrainingExampleV2:
    horizon_rows = _horizon_rows(payload, expected_horizon_hours=int(row["horizon_hours"]))
    strict_horizon_rows = _horizon_rows(strict_payload, expected_horizon_hours=int(strict_row["horizon_hours"]))
    interval_starts = [_datetime_value(item["interval_start"], field_name="interval_start") for item in horizon_rows]
    return DFLTrainingExampleV2(
        training_example_id=_training_example_id(row),
        evaluation_id=str(row["evaluation_id"]),
        baseline_evaluation_id=str(strict_row["evaluation_id"]),
        tenant_id=str(row["tenant_id"]),
        anchor_timestamp=_datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        horizon_start=interval_starts[0],
        horizon_end=interval_starts[-1],
        horizon_hours=int(row["horizon_hours"]),
        market_venue="DAM",
        currency="UAH",
        forecast_model_name=str(row["forecast_model_name"]),
        strategy_kind=str(row["strategy_kind"]),
        baseline_strategy_name=CONTROL_MODEL_NAME,
        baseline_forecast_model_name=CONTROL_MODEL_NAME,
        forecast_price_vector_uah_mwh=_float_vector(horizon_rows, "forecast_price_uah_mwh"),
        actual_price_vector_uah_mwh=_float_vector(horizon_rows, "actual_price_uah_mwh"),
        candidate_dispatch_vector_mw=_float_vector(horizon_rows, "net_power_mw"),
        baseline_dispatch_vector_mw=_float_vector(strict_horizon_rows, "net_power_mw"),
        candidate_degradation_penalty_vector_uah=_float_vector(horizon_rows, "degradation_penalty_uah"),
        baseline_degradation_penalty_vector_uah=_float_vector(strict_horizon_rows, "degradation_penalty_uah"),
        candidate_net_value_uah=float(row["decision_value_uah"]),
        baseline_net_value_uah=float(strict_row["decision_value_uah"]),
        oracle_net_value_uah=float(row["oracle_value_uah"]),
        candidate_regret_uah=float(row["regret_uah"]),
        baseline_regret_uah=float(strict_row["regret_uah"]),
        regret_delta_vs_baseline_uah=float(row["regret_uah"]) - float(strict_row["regret_uah"]),
        total_throughput_mwh=float(row["total_throughput_mwh"]),
        total_degradation_penalty_uah=float(row["total_degradation_penalty_uah"]),
        candidate_feasible=_is_feasible(payload),
        baseline_feasible=_is_feasible(strict_payload),
        safety_violation_count=_safety_violation_count(payload),
        data_quality_tier="thesis_grade",
        observed_coverage_ratio=float(payload.get("observed_coverage_ratio", 0.0)),
        claim_scope=CLAIM_SCOPE,
        not_full_dfl=True,
        not_market_execution=True,
        generated_at=_datetime_value(row["generated_at"], field_name="generated_at"),
    )


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row["evaluation_payload"]
    if not isinstance(payload, dict):
        raise ValueError("evaluation_payload must be a mapping.")
    return payload


def _data_quality_tier(payload: dict[str, Any]) -> str:
    return str(payload.get("data_quality_tier", "demo_grade"))


def _horizon_rows(payload: dict[str, Any], *, expected_horizon_hours: int) -> list[dict[str, Any]]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list):
        raise ValueError("evaluation_payload must include a horizon list.")
    rows = [item for item in horizon if isinstance(item, dict)]
    if len(rows) != expected_horizon_hours:
        raise ValueError("evaluation_payload horizon length must match horizon_hours.")
    required_keys = {
        "interval_start",
        "forecast_price_uah_mwh",
        "actual_price_uah_mwh",
        "net_power_mw",
        "degradation_penalty_uah",
    }
    for item in rows:
        missing_keys = required_keys.difference(item)
        if missing_keys:
            raise ValueError(f"evaluation_payload horizon row is missing keys: {sorted(missing_keys)}")
    return rows


def _float_vector(rows: list[dict[str, Any]], key: str) -> list[float]:
    return [float(row[key]) for row in rows]


def _anchor_key(row: dict[str, Any]) -> tuple[str, datetime]:
    return str(row["tenant_id"]), _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")


def _datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"{field_name} must be a datetime value.")


def _training_example_id(row: dict[str, Any]) -> str:
    anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    return f"{row['tenant_id']}:{row['forecast_model_name']}:{anchor_timestamp.strftime('%Y%m%dT%H%M')}:v2"


def _is_feasible(payload: dict[str, Any]) -> bool:
    if "feasible" in payload:
        return bool(payload["feasible"])
    if "candidate_feasible" in payload:
        return bool(payload["candidate_feasible"])
    return _safety_violation_count(payload) == 0


def _safety_violation_count(payload: dict[str, Any]) -> int:
    if "safety_violation_count" in payload:
        return int(payload["safety_violation_count"])
    violations = payload.get("safety_violations")
    if isinstance(violations, list):
        return len(violations)
    return 0
