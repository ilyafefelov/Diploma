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
REQUIRED_OFFLINE_DFL_PANEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "final_validation_anchor_count",
        "baseline_final_holdout_relaxed_regret_uah",
        "v2_final_holdout_relaxed_regret_uah",
        "v2_improved_over_baseline",
        "data_quality_tier",
        "observed_coverage_ratio",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_OFFLINE_DFL_PANEL_STRICT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "generated_at",
        "regret_uah",
        "evaluation_payload",
    }
)
REQUIRED_OFFLINE_DFL_DECISION_TARGET_COLUMNS: Final[frozenset[str]] = REQUIRED_OFFLINE_DFL_PANEL_STRICT_COLUMNS
REQUIRED_OFFLINE_DFL_ACTION_TARGET_COLUMNS: Final[frozenset[str]] = REQUIRED_OFFLINE_DFL_PANEL_STRICT_COLUMNS


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


def evaluate_offline_dfl_panel_development_gate(
    offline_panel_frame: pl.DataFrame,
    *,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> PromotionGateResult:
    """Evaluate whether an all-tenant offline DFL panel is ready for deeper experiments."""

    failures = _missing_column_failures(offline_panel_frame, REQUIRED_OFFLINE_DFL_PANEL_COLUMNS)
    if failures:
        return _result(failures=failures, metrics={})
    rows = list(offline_panel_frame.iter_rows(named=True))
    if not rows:
        return _result(failures=["offline DFL panel has no rows"], metrics={})

    if any(str(row["data_quality_tier"]) != "thesis_grade" for row in rows):
        failures.append("offline DFL panel development gate requires thesis_grade rows")
    observed_coverage_min = min(float(row["observed_coverage_ratio"]) for row in rows)
    if observed_coverage_min < 1.0:
        failures.append("offline DFL panel development gate requires observed coverage ratio of 1.0")
    if any(not bool(row["not_full_dfl"]) for row in rows):
        failures.append("offline DFL panel must remain not_full_dfl")
    if any(not bool(row["not_market_execution"]) for row in rows):
        failures.append("offline DFL panel must remain not_market_execution")

    model_summaries = _offline_panel_model_summaries(rows)
    passing_models = [
        summary
        for summary in model_summaries
        if summary["validation_tenant_anchor_count"] >= min_validation_tenant_anchor_count
        and summary["mean_relaxed_regret_improvement_ratio"] > 0.0
    ]
    max_validation_count = max(
        int(summary["validation_tenant_anchor_count"]) for summary in model_summaries
    )
    if max_validation_count < min_validation_tenant_anchor_count:
        failures.append(
            "validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {max_validation_count}"
        )
    if not passing_models:
        failures.append("v2 relaxed regret must improve over the raw relaxed-LP baseline")

    best_summary = max(
        model_summaries,
        key=lambda summary: float(summary["mean_relaxed_regret_improvement_ratio"]),
    )
    return _result(
        failures=failures,
        metrics={
            "tenant_count": len({str(row["tenant_id"]) for row in rows}),
            "model_count": len(model_summaries),
            "validation_tenant_anchor_count": max_validation_count,
            "best_forecast_model_name": best_summary["forecast_model_name"],
            "baseline_mean_relaxed_regret_uah": best_summary["baseline_mean_relaxed_regret_uah"],
            "v2_mean_relaxed_regret_uah": best_summary["v2_mean_relaxed_regret_uah"],
            "mean_relaxed_regret_improvement_ratio": best_summary["mean_relaxed_regret_improvement_ratio"],
            "model_summaries": model_summaries,
        },
    )


def evaluate_offline_dfl_panel_strict_promotion_gate(
    strict_panel_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    control_model_name: str = CONTROL_MODEL_NAME,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> PromotionGateResult:
    """Evaluate strict LP/oracle promotion readiness for offline DFL panel candidates."""

    failures = _missing_column_failures(strict_panel_frame, REQUIRED_OFFLINE_DFL_PANEL_STRICT_COLUMNS)
    if failures:
        return _result(failures=failures, metrics={})
    rows = list(strict_panel_frame.iter_rows(named=True))
    if not rows:
        return _result(failures=["offline DFL panel strict benchmark has no rows"], metrics={})

    source_names = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
    if not source_names:
        return _result(
            failures=["offline DFL panel strict benchmark rows must identify source_forecast_model_name"],
            metrics={},
        )

    model_summaries: list[dict[str, Any]] = []
    structural_failures: list[str] = []
    for source_model_name in source_names:
        source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
        if not source_rows:
            structural_failures.append(f"{source_model_name} has no strict panel rows")
            continue
        summary, summary_failures = _offline_panel_strict_model_summary(
            source_rows,
            source_model_name=source_model_name,
            control_model_name=control_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
        )
        model_summaries.append(summary)
        structural_failures.extend(summary_failures)

    if not model_summaries:
        return _result(failures=structural_failures, metrics={})
    passing_models = [summary for summary in model_summaries if bool(summary["passed"])]
    failures.extend(structural_failures)
    if not passing_models:
        failures.append(
            f"no v2 source model beats {control_model_name} by "
            f"{min_mean_regret_improvement_ratio:.1%} with stable median regret"
        )
    best_summary = max(
        model_summaries,
        key=lambda summary: float(summary["mean_regret_improvement_ratio_vs_strict"]),
    )
    return _result(
        failures=failures,
        metrics={
            "best_source_model_name": best_summary["source_model_name"],
            "tenant_count": best_summary["tenant_count"],
            "validation_tenant_anchor_count": best_summary["validation_tenant_anchor_count"],
            "strict_mean_regret_uah": best_summary["strict_mean_regret_uah"],
            "raw_mean_regret_uah": best_summary["raw_mean_regret_uah"],
            "v2_mean_regret_uah": best_summary["v2_mean_regret_uah"],
            "strict_median_regret_uah": best_summary["strict_median_regret_uah"],
            "v2_median_regret_uah": best_summary["v2_median_regret_uah"],
            "mean_regret_improvement_ratio_vs_strict": best_summary[
                "mean_regret_improvement_ratio_vs_strict"
            ],
            "mean_regret_improvement_ratio_vs_raw": best_summary["mean_regret_improvement_ratio_vs_raw"],
            "passing_source_model_names": [str(summary["source_model_name"]) for summary in passing_models],
            "model_summaries": model_summaries,
        },
    )


def evaluate_offline_dfl_decision_target_promotion_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    control_model_name: str = CONTROL_MODEL_NAME,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> PromotionGateResult:
    """Evaluate strict LP/oracle promotion readiness for decision-target v3 candidates."""

    failures = _missing_column_failures(strict_frame, REQUIRED_OFFLINE_DFL_DECISION_TARGET_COLUMNS)
    if failures:
        return _result(failures=failures, metrics={})
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return _result(failures=["offline DFL decision-target strict benchmark has no rows"], metrics={})

    source_names = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
    if not source_names:
        return _result(
            failures=["offline DFL decision-target rows must identify source_forecast_model_name"],
            metrics={},
        )

    model_summaries: list[dict[str, Any]] = []
    structural_failures: list[str] = []
    for source_model_name in source_names:
        source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
        if not source_rows:
            structural_failures.append(f"{source_model_name} has no decision-target strict rows")
            continue
        summary, summary_failures = _offline_decision_target_strict_model_summary(
            source_rows,
            source_model_name=source_model_name,
            control_model_name=control_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
        )
        model_summaries.append(summary)
        structural_failures.extend(summary_failures)

    if not model_summaries:
        return _result(failures=structural_failures, metrics={})
    passing_models = [summary for summary in model_summaries if bool(summary["passed"])]
    failures.extend(structural_failures)
    if not passing_models:
        failures.append(
            f"no decision-target v3 source model beats {control_model_name} by "
            f"{min_mean_regret_improvement_ratio:.1%} with stable median regret"
        )
    best_summary = max(
        model_summaries,
        key=lambda summary: float(summary["mean_regret_improvement_ratio_vs_strict"]),
    )
    return _result(
        failures=failures,
        metrics={
            "best_source_model_name": best_summary["source_model_name"],
            "tenant_count": best_summary["tenant_count"],
            "validation_tenant_anchor_count": best_summary["validation_tenant_anchor_count"],
            "strict_mean_regret_uah": best_summary["strict_mean_regret_uah"],
            "raw_mean_regret_uah": best_summary["raw_mean_regret_uah"],
            "v2_mean_regret_uah": best_summary["v2_mean_regret_uah"],
            "v3_mean_regret_uah": best_summary["v3_mean_regret_uah"],
            "strict_median_regret_uah": best_summary["strict_median_regret_uah"],
            "v3_median_regret_uah": best_summary["v3_median_regret_uah"],
            "mean_regret_improvement_ratio_vs_strict": best_summary[
                "mean_regret_improvement_ratio_vs_strict"
            ],
            "mean_regret_improvement_ratio_vs_raw": best_summary["mean_regret_improvement_ratio_vs_raw"],
            "mean_regret_improvement_ratio_vs_panel_v2": best_summary[
                "mean_regret_improvement_ratio_vs_panel_v2"
            ],
            "passing_source_model_names": [str(summary["source_model_name"]) for summary in passing_models],
            "model_summaries": model_summaries,
        },
    )


def evaluate_offline_dfl_action_target_promotion_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    control_model_name: str = CONTROL_MODEL_NAME,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> PromotionGateResult:
    """Evaluate strict LP/oracle promotion readiness for action-target v4 candidates."""

    failures = _missing_column_failures(strict_frame, REQUIRED_OFFLINE_DFL_ACTION_TARGET_COLUMNS)
    if failures:
        return _result(failures=failures, metrics={})
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return _result(failures=["offline DFL action-target strict benchmark has no rows"], metrics={})

    source_names = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
    if not source_names:
        return _result(
            failures=["offline DFL action-target rows must identify source_forecast_model_name"],
            metrics={},
        )

    model_summaries: list[dict[str, Any]] = []
    structural_failures: list[str] = []
    for source_model_name in source_names:
        source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
        if not source_rows:
            structural_failures.append(f"{source_model_name} has no action-target strict rows")
            continue
        summary, summary_failures = _offline_action_target_strict_model_summary(
            source_rows,
            source_model_name=source_model_name,
            control_model_name=control_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
        )
        model_summaries.append(summary)
        structural_failures.extend(summary_failures)

    if not model_summaries:
        return _result(failures=structural_failures, metrics={})
    passing_models = [summary for summary in model_summaries if bool(summary["passed"])]
    failures.extend(structural_failures)
    if not passing_models:
        failures.append(
            f"no action-target v4 source model beats {control_model_name} by "
            f"{min_mean_regret_improvement_ratio:.1%} with stable median regret"
        )
    best_summary = max(
        model_summaries,
        key=lambda summary: float(summary["mean_regret_improvement_ratio_vs_strict"]),
    )
    return _result(
        failures=failures,
        metrics={
            "best_source_model_name": best_summary["source_model_name"],
            "tenant_count": best_summary["tenant_count"],
            "validation_tenant_anchor_count": best_summary["validation_tenant_anchor_count"],
            "strict_mean_regret_uah": best_summary["strict_mean_regret_uah"],
            "raw_mean_regret_uah": best_summary["raw_mean_regret_uah"],
            "v2_mean_regret_uah": best_summary["v2_mean_regret_uah"],
            "v3_mean_regret_uah": best_summary["v3_mean_regret_uah"],
            "v4_mean_regret_uah": best_summary["v4_mean_regret_uah"],
            "strict_median_regret_uah": best_summary["strict_median_regret_uah"],
            "v4_median_regret_uah": best_summary["v4_median_regret_uah"],
            "mean_regret_improvement_ratio_vs_strict": best_summary[
                "mean_regret_improvement_ratio_vs_strict"
            ],
            "mean_regret_improvement_ratio_vs_raw": best_summary["mean_regret_improvement_ratio_vs_raw"],
            "mean_regret_improvement_ratio_vs_panel_v2": best_summary[
                "mean_regret_improvement_ratio_vs_panel_v2"
            ],
            "mean_regret_improvement_ratio_vs_decision_v3": best_summary[
                "mean_regret_improvement_ratio_vs_decision_v3"
            ],
            "passing_source_model_names": [str(summary["source_model_name"]) for summary in passing_models],
            "model_summaries": model_summaries,
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


def _offline_panel_model_summaries(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    model_names = sorted({str(row["forecast_model_name"]) for row in rows})
    for model_name in model_names:
        model_rows = [row for row in rows if str(row["forecast_model_name"]) == model_name]
        validation_count = sum(int(row["final_validation_anchor_count"]) for row in model_rows)
        baseline_mean = _weighted_mean(
            [
                (
                    float(row["baseline_final_holdout_relaxed_regret_uah"]),
                    int(row["final_validation_anchor_count"]),
                )
                for row in model_rows
            ]
        )
        v2_mean = _weighted_mean(
            [
                (
                    float(row["v2_final_holdout_relaxed_regret_uah"]),
                    int(row["final_validation_anchor_count"]),
                )
                for row in model_rows
            ]
        )
        improvement_ratio = (baseline_mean - v2_mean) / abs(baseline_mean) if abs(baseline_mean) > 1e-9 else 0.0
        summaries.append(
            {
                "forecast_model_name": model_name,
                "tenant_count": len({str(row["tenant_id"]) for row in model_rows}),
                "validation_tenant_anchor_count": validation_count,
                "baseline_mean_relaxed_regret_uah": baseline_mean,
                "v2_mean_relaxed_regret_uah": v2_mean,
                "mean_relaxed_regret_improvement_ratio": improvement_ratio,
            }
        )
    return summaries


def _offline_panel_strict_model_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    control_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    v2_model_name = f"offline_dfl_panel_v2_{source_model_name}"
    control_rows = [row for row in rows if str(row["forecast_model_name"]) == control_model_name]
    raw_rows = [row for row in rows if str(row["forecast_model_name"]) == source_model_name]
    v2_rows = [row for row in rows if str(row["forecast_model_name"]) == v2_model_name]
    control_anchors = _tenant_anchor_set(control_rows)
    raw_anchors = _tenant_anchor_set(raw_rows)
    v2_anchors = _tenant_anchor_set(v2_rows)
    anchor_sets_match = control_anchors == raw_anchors == v2_anchors
    validation_count = len(v2_anchors)
    tenant_count = len({tenant_id for tenant_id, _ in v2_anchors})

    if tenant_count < min_tenant_count:
        failures.append(f"{source_model_name} tenant_count must be at least {min_tenant_count}; observed {tenant_count}")
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {validation_count}"
        )
    if not anchor_sets_match:
        failures.append(f"{source_model_name} strict/raw/v2 rows must cover matching tenant-anchor sets")
    failures.extend(_offline_panel_strict_provenance_failures([*control_rows, *raw_rows, *v2_rows]))

    strict_regrets = [float(row["regret_uah"]) for row in control_rows]
    raw_regrets = [float(row["regret_uah"]) for row in raw_rows]
    v2_regrets = [float(row["regret_uah"]) for row in v2_rows]
    strict_mean = mean(strict_regrets) if strict_regrets else 0.0
    raw_mean = mean(raw_regrets) if raw_regrets else 0.0
    v2_mean = mean(v2_regrets) if v2_regrets else 0.0
    strict_median = median(strict_regrets) if strict_regrets else 0.0
    raw_median = median(raw_regrets) if raw_regrets else 0.0
    v2_median = median(v2_regrets) if v2_regrets else 0.0
    improvement_vs_strict = (strict_mean - v2_mean) / abs(strict_mean) if abs(strict_mean) > 1e-9 else 0.0
    improvement_vs_raw = (raw_mean - v2_mean) / abs(raw_mean) if abs(raw_mean) > 1e-9 else 0.0
    if v2_rows and control_rows and improvement_vs_strict < min_mean_regret_improvement_ratio:
        failures.append(
            f"{source_model_name} mean regret improvement vs {control_model_name} must be at least "
            f"{min_mean_regret_improvement_ratio:.1%}; observed {improvement_vs_strict:.1%}"
        )
    if v2_rows and control_rows and v2_median > strict_median:
        failures.append(
            f"{source_model_name} median regret must not be worse than {control_model_name}; "
            f"observed v2={v2_median:.2f}, strict={strict_median:.2f}"
        )
    summary = {
        "source_model_name": source_model_name,
        "v2_model_name": v2_model_name,
        "tenant_count": tenant_count,
        "validation_tenant_anchor_count": validation_count,
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "v2_mean_regret_uah": v2_mean,
        "strict_median_regret_uah": strict_median,
        "raw_median_regret_uah": raw_median,
        "v2_median_regret_uah": v2_median,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "passed": not failures,
        "failures": failures,
    }
    return summary, failures


def _offline_decision_target_strict_model_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    control_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    v2_model_name = f"offline_dfl_panel_v2_{source_model_name}"
    v3_model_name = f"offline_dfl_decision_target_v3_{source_model_name}"
    control_rows = [row for row in rows if str(row["forecast_model_name"]) == control_model_name]
    raw_rows = [row for row in rows if str(row["forecast_model_name"]) == source_model_name]
    v2_rows = [row for row in rows if str(row["forecast_model_name"]) == v2_model_name]
    v3_rows = [row for row in rows if str(row["forecast_model_name"]) == v3_model_name]
    control_anchors = _tenant_anchor_set(control_rows)
    raw_anchors = _tenant_anchor_set(raw_rows)
    v2_anchors = _tenant_anchor_set(v2_rows)
    v3_anchors = _tenant_anchor_set(v3_rows)
    anchor_sets_match = control_anchors == raw_anchors == v2_anchors == v3_anchors
    validation_count = len(v3_anchors)
    tenant_count = len({tenant_id for tenant_id, _ in v3_anchors})

    if tenant_count < min_tenant_count:
        failures.append(f"{source_model_name} tenant_count must be at least {min_tenant_count}; observed {tenant_count}")
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {validation_count}"
        )
    if not anchor_sets_match:
        failures.append(f"{source_model_name} strict/raw/v2/v3 rows must cover matching tenant-anchor sets")
    failures.extend(_offline_decision_target_provenance_failures([*control_rows, *raw_rows, *v2_rows, *v3_rows]))

    strict_regrets = [float(row["regret_uah"]) for row in control_rows]
    raw_regrets = [float(row["regret_uah"]) for row in raw_rows]
    v2_regrets = [float(row["regret_uah"]) for row in v2_rows]
    v3_regrets = [float(row["regret_uah"]) for row in v3_rows]
    strict_mean = mean(strict_regrets) if strict_regrets else 0.0
    raw_mean = mean(raw_regrets) if raw_regrets else 0.0
    v2_mean = mean(v2_regrets) if v2_regrets else 0.0
    v3_mean = mean(v3_regrets) if v3_regrets else 0.0
    strict_median = median(strict_regrets) if strict_regrets else 0.0
    raw_median = median(raw_regrets) if raw_regrets else 0.0
    v2_median = median(v2_regrets) if v2_regrets else 0.0
    v3_median = median(v3_regrets) if v3_regrets else 0.0
    improvement_vs_strict = (strict_mean - v3_mean) / abs(strict_mean) if abs(strict_mean) > 1e-9 else 0.0
    improvement_vs_raw = (raw_mean - v3_mean) / abs(raw_mean) if abs(raw_mean) > 1e-9 else 0.0
    improvement_vs_v2 = (v2_mean - v3_mean) / abs(v2_mean) if abs(v2_mean) > 1e-9 else 0.0
    if v3_rows and control_rows and improvement_vs_strict < min_mean_regret_improvement_ratio:
        failures.append(
            f"{source_model_name} mean regret improvement vs {control_model_name} must be at least "
            f"{min_mean_regret_improvement_ratio:.1%}; observed {improvement_vs_strict:.1%}"
        )
    if v3_rows and control_rows and v3_median > strict_median:
        failures.append(
            f"{source_model_name} median regret must not be worse than {control_model_name}; "
            f"observed v3={v3_median:.2f}, strict={strict_median:.2f}"
        )
    summary = {
        "source_model_name": source_model_name,
        "v2_model_name": v2_model_name,
        "v3_model_name": v3_model_name,
        "tenant_count": tenant_count,
        "validation_tenant_anchor_count": validation_count,
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "v2_mean_regret_uah": v2_mean,
        "v3_mean_regret_uah": v3_mean,
        "strict_median_regret_uah": strict_median,
        "raw_median_regret_uah": raw_median,
        "v2_median_regret_uah": v2_median,
        "v3_median_regret_uah": v3_median,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "mean_regret_improvement_ratio_vs_panel_v2": improvement_vs_v2,
        "passed": not failures,
        "failures": failures,
    }
    return summary, failures


def _offline_action_target_strict_model_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    control_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    v2_model_name = f"offline_dfl_panel_v2_{source_model_name}"
    v3_model_name = f"offline_dfl_decision_target_v3_{source_model_name}"
    v4_model_name = f"offline_dfl_action_target_v4_{source_model_name}"
    control_rows = [row for row in rows if str(row["forecast_model_name"]) == control_model_name]
    raw_rows = [row for row in rows if str(row["forecast_model_name"]) == source_model_name]
    v2_rows = [row for row in rows if str(row["forecast_model_name"]) == v2_model_name]
    v3_rows = [row for row in rows if str(row["forecast_model_name"]) == v3_model_name]
    v4_rows = [row for row in rows if str(row["forecast_model_name"]) == v4_model_name]
    control_anchors = _tenant_anchor_set(control_rows)
    raw_anchors = _tenant_anchor_set(raw_rows)
    v2_anchors = _tenant_anchor_set(v2_rows)
    v3_anchors = _tenant_anchor_set(v3_rows)
    v4_anchors = _tenant_anchor_set(v4_rows)
    anchor_sets_match = control_anchors == raw_anchors == v2_anchors == v3_anchors == v4_anchors
    validation_count = len(v4_anchors)
    tenant_count = len({tenant_id for tenant_id, _ in v4_anchors})

    if tenant_count < min_tenant_count:
        failures.append(f"{source_model_name} tenant_count must be at least {min_tenant_count}; observed {tenant_count}")
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {validation_count}"
        )
    if not anchor_sets_match:
        failures.append(f"{source_model_name} strict/raw/v2/v3/v4 rows must cover matching tenant-anchor sets")
    failures.extend(_offline_action_target_provenance_failures([*control_rows, *raw_rows, *v2_rows, *v3_rows, *v4_rows]))

    strict_regrets = [float(row["regret_uah"]) for row in control_rows]
    raw_regrets = [float(row["regret_uah"]) for row in raw_rows]
    v2_regrets = [float(row["regret_uah"]) for row in v2_rows]
    v3_regrets = [float(row["regret_uah"]) for row in v3_rows]
    v4_regrets = [float(row["regret_uah"]) for row in v4_rows]
    strict_mean = mean(strict_regrets) if strict_regrets else 0.0
    raw_mean = mean(raw_regrets) if raw_regrets else 0.0
    v2_mean = mean(v2_regrets) if v2_regrets else 0.0
    v3_mean = mean(v3_regrets) if v3_regrets else 0.0
    v4_mean = mean(v4_regrets) if v4_regrets else 0.0
    strict_median = median(strict_regrets) if strict_regrets else 0.0
    raw_median = median(raw_regrets) if raw_regrets else 0.0
    v2_median = median(v2_regrets) if v2_regrets else 0.0
    v3_median = median(v3_regrets) if v3_regrets else 0.0
    v4_median = median(v4_regrets) if v4_regrets else 0.0
    improvement_vs_strict = (strict_mean - v4_mean) / abs(strict_mean) if abs(strict_mean) > 1e-9 else 0.0
    improvement_vs_raw = (raw_mean - v4_mean) / abs(raw_mean) if abs(raw_mean) > 1e-9 else 0.0
    improvement_vs_v2 = (v2_mean - v4_mean) / abs(v2_mean) if abs(v2_mean) > 1e-9 else 0.0
    improvement_vs_v3 = (v3_mean - v4_mean) / abs(v3_mean) if abs(v3_mean) > 1e-9 else 0.0
    if v4_rows and control_rows and improvement_vs_strict < min_mean_regret_improvement_ratio:
        failures.append(
            f"{source_model_name} mean regret improvement vs {control_model_name} must be at least "
            f"{min_mean_regret_improvement_ratio:.1%}; observed {improvement_vs_strict:.1%}"
        )
    if v4_rows and control_rows and v4_median > strict_median:
        failures.append(
            f"{source_model_name} median regret must not be worse than {control_model_name}; "
            f"observed v4={v4_median:.2f}, strict={strict_median:.2f}"
        )
    summary = {
        "source_model_name": source_model_name,
        "v2_model_name": v2_model_name,
        "v3_model_name": v3_model_name,
        "v4_model_name": v4_model_name,
        "tenant_count": tenant_count,
        "validation_tenant_anchor_count": validation_count,
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "v2_mean_regret_uah": v2_mean,
        "v3_mean_regret_uah": v3_mean,
        "v4_mean_regret_uah": v4_mean,
        "strict_median_regret_uah": strict_median,
        "raw_median_regret_uah": raw_median,
        "v2_median_regret_uah": v2_median,
        "v3_median_regret_uah": v3_median,
        "v4_median_regret_uah": v4_median,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "mean_regret_improvement_ratio_vs_panel_v2": improvement_vs_v2,
        "mean_regret_improvement_ratio_vs_decision_v3": improvement_vs_v3,
        "passed": not failures,
        "failures": failures,
    }
    return summary, failures


def _offline_panel_strict_provenance_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    payloads = [_payload(row) for row in rows]
    if any(str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade" for payload in payloads):
        failures.append("offline DFL panel strict gate requires thesis_grade rows")
    if any(float(payload.get("observed_coverage_ratio", 0.0)) < 1.0 for payload in payloads):
        failures.append("offline DFL panel strict gate requires observed coverage ratio of 1.0")
    safety_violation_count = sum(_safety_violation_count(payload) for payload in payloads)
    if safety_violation_count:
        failures.append(f"offline DFL panel strict gate requires zero safety violations; observed {safety_violation_count}")
    if any(not bool(payload.get("not_full_dfl", False)) for payload in payloads):
        failures.append("offline DFL panel strict gate requires not_full_dfl rows")
    if any(not bool(payload.get("not_market_execution", False)) for payload in payloads):
        failures.append("offline DFL panel strict gate requires not_market_execution rows")
    return failures


def _offline_decision_target_provenance_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    payloads = [_payload(row) for row in rows]
    if any(str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade" for payload in payloads):
        failures.append("offline DFL decision-target strict gate requires thesis_grade rows")
    if any(float(payload.get("observed_coverage_ratio", 0.0)) < 1.0 for payload in payloads):
        failures.append("offline DFL decision-target strict gate requires observed coverage ratio of 1.0")
    safety_violation_count = sum(_safety_violation_count(payload) for payload in payloads)
    if safety_violation_count:
        failures.append(
            f"offline DFL decision-target strict gate requires zero safety violations; observed {safety_violation_count}"
        )
    if any(not bool(payload.get("not_full_dfl", False)) for payload in payloads):
        failures.append("offline DFL decision-target strict gate requires not_full_dfl rows")
    if any(not bool(payload.get("not_market_execution", False)) for payload in payloads):
        failures.append("offline DFL decision-target strict gate requires not_market_execution rows")
    return failures


def _offline_action_target_provenance_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    payloads = [_payload(row) for row in rows]
    if any(str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade" for payload in payloads):
        failures.append("offline DFL action-target strict gate requires thesis_grade rows")
    if any(float(payload.get("observed_coverage_ratio", 0.0)) < 1.0 for payload in payloads):
        failures.append("offline DFL action-target strict gate requires observed coverage ratio of 1.0")
    safety_violation_count = sum(_safety_violation_count(payload) for payload in payloads)
    if safety_violation_count:
        failures.append(
            f"offline DFL action-target strict gate requires zero safety violations; observed {safety_violation_count}"
        )
    if any(not bool(payload.get("not_full_dfl", False)) for payload in payloads):
        failures.append("offline DFL action-target strict gate requires not_full_dfl rows")
    if any(not bool(payload.get("not_market_execution", False)) for payload in payloads):
        failures.append("offline DFL action-target strict gate requires not_market_execution rows")
    return failures


def _tenant_anchor_set(rows: list[dict[str, Any]]) -> set[tuple[str, datetime]]:
    return {
        (str(row["tenant_id"]), _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"))
        for row in rows
    }


def _weighted_mean(values: list[tuple[float, int]]) -> float:
    total_weight = sum(weight for _, weight in values)
    if total_weight <= 0:
        return 0.0
    return sum(value * weight for value, weight in values) / total_weight


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


def _source_model_name(row: dict[str, Any]) -> str:
    payload = _payload(row)
    source_model_name = payload.get("source_forecast_model_name")
    return str(source_model_name) if source_model_name else ""


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
