"""Conservative promotion gates for DFL and forecast candidates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl

DNIPRO_TENANT_ID: Final[str] = "client_003_dnipro_factory"
CONTROL_MODEL_NAME: Final[str] = "strict_similar_day"
DEFAULT_MIN_ANCHOR_COUNT: Final[int] = 90
DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO: Final[float] = 0.05
REQUIRED_STRATEGY_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "generated_at",
        "regret_uah",
        "evaluation_payload",
    }
)
REQUIRED_OFFLINE_DFL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "validation_anchor_count",
        "baseline_validation_relaxed_regret_uah",
        "dfl_validation_relaxed_regret_uah",
        "improved_over_baseline",
        "data_quality_tier",
        "claim_scope",
        "not_market_execution",
    }
)


@dataclass(frozen=True)
class PromotionGateResult:
    passed: bool
    decision: str
    description: str
    metrics: dict[str, Any]


def evaluate_strategy_promotion_gate(
    evaluation_frame: pl.DataFrame,
    *,
    candidate_model_name: str,
    tenant_id: str = DNIPRO_TENANT_ID,
    control_model_name: str = CONTROL_MODEL_NAME,
    min_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> PromotionGateResult:
    """Evaluate whether a strategy candidate can beat the frozen control."""

    failures = _missing_column_failures(evaluation_frame, REQUIRED_STRATEGY_COLUMNS)
    if failures:
        return _result(failures=failures, metrics={})
    tenant_rows = [row for row in evaluation_frame.iter_rows(named=True) if row["tenant_id"] == tenant_id]
    if not tenant_rows:
        return _result(failures=[f"tenant_id={tenant_id} has no rows"], metrics={"tenant_id": tenant_id})
    latest_generated_at = max(_datetime_value(row["generated_at"], field_name="generated_at") for row in tenant_rows)
    latest_rows = [
        row
        for row in tenant_rows
        if _datetime_value(row["generated_at"], field_name="generated_at") == latest_generated_at
    ]
    candidate_rows = [row for row in latest_rows if row["forecast_model_name"] == candidate_model_name]
    control_rows = [row for row in latest_rows if row["forecast_model_name"] == control_model_name]
    failures.extend(
        _coverage_failures(
            candidate_rows=candidate_rows,
            control_rows=control_rows,
            candidate_model_name=candidate_model_name,
            control_model_name=control_model_name,
            min_anchor_count=min_anchor_count,
        )
    )
    failures.extend(_provenance_failures([*candidate_rows, *control_rows]))

    candidate_regrets = [float(row["regret_uah"]) for row in candidate_rows]
    control_regrets = [float(row["regret_uah"]) for row in control_rows]
    candidate_mean = mean(candidate_regrets) if candidate_regrets else 0.0
    control_mean = mean(control_regrets) if control_regrets else 0.0
    candidate_median = median(candidate_regrets) if candidate_regrets else 0.0
    control_median = median(control_regrets) if control_regrets else 0.0
    improvement_ratio = (control_mean - candidate_mean) / abs(control_mean) if abs(control_mean) > 1e-9 else 0.0
    if candidate_rows and control_rows and improvement_ratio < min_mean_regret_improvement_ratio:
        failures.append(
            "mean regret improvement must be at least "
            f"{min_mean_regret_improvement_ratio:.1%}; observed {improvement_ratio:.1%}"
        )
    if candidate_rows and control_rows and candidate_median > control_median:
        failures.append(
            f"median regret must not be worse than {control_model_name}; "
            f"observed candidate={candidate_median:.2f}, control={control_median:.2f}"
        )

    return _result(
        failures=failures,
        metrics={
            "tenant_id": tenant_id,
            "candidate_model_name": candidate_model_name,
            "control_model_name": control_model_name,
            "latest_generated_at": latest_generated_at.isoformat(),
            "candidate_anchor_count": len(_anchor_set(candidate_rows)),
            "control_anchor_count": len(_anchor_set(control_rows)),
            "candidate_mean_regret_uah": candidate_mean,
            "control_mean_regret_uah": control_mean,
            "candidate_median_regret_uah": candidate_median,
            "control_median_regret_uah": control_median,
            "mean_regret_improvement_ratio": improvement_ratio,
        },
    )


def evaluate_offline_dfl_promotion_gate(
    offline_experiment_frame: pl.DataFrame,
    *,
    tenant_id: str = DNIPRO_TENANT_ID,
    min_validation_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> PromotionGateResult:
    """Block weak offline relaxed-LP experiments from being promoted as DFL wins."""

    failures = _missing_column_failures(offline_experiment_frame, REQUIRED_OFFLINE_DFL_COLUMNS)
    if failures:
        return _result(failures=failures, metrics={})
    rows = [row for row in offline_experiment_frame.iter_rows(named=True) if row["tenant_id"] == tenant_id]
    if not rows:
        return _result(failures=[f"tenant_id={tenant_id} has no offline DFL rows"], metrics={"tenant_id": tenant_id})

    validation_counts = [int(row["validation_anchor_count"]) for row in rows]
    baseline_regrets = [float(row["baseline_validation_relaxed_regret_uah"]) for row in rows]
    dfl_regrets = [float(row["dfl_validation_relaxed_regret_uah"]) for row in rows]
    baseline_mean = mean(baseline_regrets)
    dfl_mean = mean(dfl_regrets)
    improvement_ratio = (baseline_mean - dfl_mean) / abs(baseline_mean) if abs(baseline_mean) > 1e-9 else 0.0

    if min(validation_counts) < min_validation_anchor_count:
        failures.append(
            f"validation_anchor_count must be at least {min_validation_anchor_count}; "
            f"observed {min(validation_counts)}"
        )
    if any(str(row["data_quality_tier"]) != "thesis_grade" for row in rows):
        failures.append("offline DFL promotion requires thesis_grade rows")
    if any(not bool(row["not_market_execution"]) for row in rows):
        failures.append("offline DFL promotion requires not_market_execution rows")
    if any(not bool(row["improved_over_baseline"]) for row in rows):
        failures.append("offline DFL candidate does not improve over its raw relaxed-LP baseline")
    if improvement_ratio < min_mean_regret_improvement_ratio:
        failures.append(
            "mean regret improvement must be at least "
            f"{min_mean_regret_improvement_ratio:.1%}; observed {improvement_ratio:.1%}"
        )

    return _result(
        failures=failures,
        metrics={
            "tenant_id": tenant_id,
            "row_count": len(rows),
            "min_validation_anchor_count": min(validation_counts),
            "baseline_mean_relaxed_regret_uah": baseline_mean,
            "dfl_mean_relaxed_regret_uah": dfl_mean,
            "mean_regret_improvement_ratio": improvement_ratio,
        },
    )


def _coverage_failures(
    *,
    candidate_rows: list[dict[str, Any]],
    control_rows: list[dict[str, Any]],
    candidate_model_name: str,
    control_model_name: str,
    min_anchor_count: int,
) -> list[str]:
    failures: list[str] = []
    candidate_anchors = _anchor_set(candidate_rows)
    control_anchors = _anchor_set(control_rows)
    if len(candidate_anchors) < min_anchor_count:
        failures.append(
            f"{candidate_model_name} anchor_count must be at least {min_anchor_count}; observed {len(candidate_anchors)}"
        )
    if len(control_anchors) < min_anchor_count:
        failures.append(
            f"{control_model_name} anchor_count must be at least {min_anchor_count}; observed {len(control_anchors)}"
        )
    if candidate_anchors != control_anchors:
        failures.append("candidate and strict control must cover the same latest-batch anchors")
    return failures


def _provenance_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    payloads = [_payload(row) for row in rows]
    if any(str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade" for payload in payloads):
        failures.append("promotion requires thesis_grade evidence")
    if any(float(payload.get("observed_coverage_ratio", 0.0)) < 1.0 for payload in payloads):
        failures.append("promotion requires observed coverage ratio of 1.0")
    safety_violation_count = sum(_safety_violation_count(payload) for payload in payloads)
    if safety_violation_count:
        failures.append(f"promotion requires zero safety violations; observed {safety_violation_count}")
    if any(not bool(payload.get("not_market_execution", True)) for payload in payloads):
        failures.append("promotion evidence must remain not_market_execution")
    return failures


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing_columns = sorted(required_columns.difference(frame.columns))
    return [f"frame is missing required columns: {missing_columns}"] if missing_columns else []


def _result(*, failures: list[str], metrics: dict[str, Any]) -> PromotionGateResult:
    return PromotionGateResult(
        passed=not failures,
        decision="promote" if not failures else "block",
        description="Promotion gate passed." if not failures else "; ".join(failures),
        metrics=metrics,
    )


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row["evaluation_payload"]
    if not isinstance(payload, dict):
        return {}
    return payload


def _anchor_set(rows: list[dict[str, Any]]) -> set[datetime]:
    return {_datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp") for row in rows}


def _datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"{field_name} must be a datetime value.")


def _safety_violation_count(payload: dict[str, Any]) -> int:
    if "safety_violation_count" in payload:
        return int(payload["safety_violation_count"])
    violations = payload.get("safety_violations")
    if isinstance(violations, list):
        return len(violations)
    return 0
