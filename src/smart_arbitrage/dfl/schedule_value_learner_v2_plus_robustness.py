"""Rolling robustness evidence for Schedule/Value Learner V2+."""

from __future__ import annotations

from datetime import datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus import (
    V2_PLUS_CANDIDATE_FAMILIES,
    build_dfl_schedule_value_learner_v2_plus_frame,
    build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_value_learner_v2_plus_robustness_not_full_dfl"
)
DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_ACADEMIC_SCOPE: Final[str] = (
    "Rolling-window prior-only robustness gate for Schedule/Value Learner V2+. "
    "It compares V2+ against strict_similar_day and frozen V2. It is not full "
    "DFL, not Decision Transformer control, and not market execution."
)
DEFAULT_ROLLING_VALIDATION_WINDOW_COUNT: Final[int] = 4
DEFAULT_VALIDATION_ANCHOR_COUNT: Final[int] = 18
DEFAULT_MIN_PRIOR_ANCHORS_BEFORE_WINDOW: Final[int] = 30
DEFAULT_MIN_ROBUST_PASSING_WINDOWS: Final[int] = 3

REQUIRED_ROBUSTNESS_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source_model_name",
        "window_index",
        "tenant_count",
        "validation_anchor_count_per_tenant",
        "validation_tenant_anchor_count",
        "minimum_prior_anchor_count_before_window",
        "strict_mean_regret_uah",
        "raw_mean_regret_uah",
        "v2_mean_regret_uah",
        "selected_mean_regret_uah",
        "strict_median_regret_uah",
        "v2_median_regret_uah",
        "selected_median_regret_uah",
        "development_passed",
        "source_specific_strict_passed",
        "v2_non_degradation_passed",
        "v2_plus_window_passed",
        "robust_research_challenger",
        "production_promote",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_dfl_schedule_value_learner_v2_plus_robustness_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    validation_window_count: int = DEFAULT_ROLLING_VALIDATION_WINDOW_COUNT,
    validation_anchor_count: int = DEFAULT_VALIDATION_ANCHOR_COUNT,
    min_prior_anchors_before_window: int = DEFAULT_MIN_PRIOR_ANCHORS_BEFORE_WINDOW,
    min_robust_passing_windows: int = DEFAULT_MIN_ROBUST_PASSING_WINDOWS,
    min_validation_tenant_anchor_count_per_source_model: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    min_prior_mean_improvement_ratio_vs_v2: float = 0.01,
) -> pl.DataFrame:
    """Replay V2+ over rolling windows using only anchors before each window."""

    _validate_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        validation_window_count=validation_window_count,
        validation_anchor_count=validation_anchor_count,
        min_prior_anchors_before_window=min_prior_anchors_before_window,
        min_robust_passing_windows=min_robust_passing_windows,
    )
    v2._validate_library_frame(schedule_candidate_library_frame)

    rows: list[dict[str, Any]] = []
    for source_model_name in forecast_model_names:
        source_rows: list[dict[str, Any]] = []
        windows = _rolling_windows(
            schedule_candidate_library_frame,
            tenant_ids=tenant_ids,
            source_model_name=source_model_name,
            validation_window_count=validation_window_count,
            validation_anchor_count=validation_anchor_count,
            min_prior_anchors_before_window=min_prior_anchors_before_window,
        )
        for window in windows:
            source_rows.append(
                _window_summary_row(
                    schedule_candidate_library_frame,
                    tenant_ids=tenant_ids,
                    source_model_name=source_model_name,
                    window=window,
                    validation_anchor_count=validation_anchor_count,
                    min_validation_tenant_anchor_count=(
                        min_validation_tenant_anchor_count_per_source_model
                    ),
                    min_mean_regret_improvement_ratio=(
                        min_mean_regret_improvement_ratio
                    ),
                    min_prior_mean_improvement_ratio_vs_v2=(
                        min_prior_mean_improvement_ratio_vs_v2
                    ),
                )
            )
        passing_count = sum(1 for row in source_rows if bool(row["v2_plus_window_passed"]))
        latest_passed = any(
            bool(row["v2_plus_window_passed"])
            for row in source_rows
            if int(row["window_index"]) == 1
        )
        robust = latest_passed and passing_count >= min_robust_passing_windows
        for row in source_rows:
            row["passing_window_count_for_source"] = passing_count
            row["robust_research_challenger"] = robust
            row["gate_label"] = _gate_label(row, robust=robust)
            row["production_promote"] = False
        rows.extend(source_rows)
    return pl.DataFrame(rows)


def build_dfl_schedule_value_learner_v2_plus_rolling_strict_rows_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    validation_window_count: int = DEFAULT_ROLLING_VALIDATION_WINDOW_COUNT,
    validation_anchor_count: int = DEFAULT_VALIDATION_ANCHOR_COUNT,
    min_prior_anchors_before_window: int = DEFAULT_MIN_PRIOR_ANCHORS_BEFORE_WINDOW,
    min_prior_mean_improvement_ratio_vs_v2: float = 0.01,
) -> pl.DataFrame:
    """Materialize candidate-level strict rows for distinct rolling windows."""

    _validate_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        validation_window_count=validation_window_count,
        validation_anchor_count=validation_anchor_count,
        min_prior_anchors_before_window=min_prior_anchors_before_window,
        min_robust_passing_windows=1,
    )
    v2._validate_library_frame(schedule_candidate_library_frame)
    frames: list[pl.DataFrame] = []
    for source_model_name in forecast_model_names:
        windows = _rolling_windows(
            schedule_candidate_library_frame,
            tenant_ids=tenant_ids,
            source_model_name=source_model_name,
            validation_window_count=validation_window_count,
            validation_anchor_count=validation_anchor_count,
            min_prior_anchors_before_window=min_prior_anchors_before_window,
        )
        for window in windows:
            frames.append(
                _rolling_window_strict_frame(
                    schedule_candidate_library_frame,
                    tenant_ids=tenant_ids,
                    source_model_name=source_model_name,
                    window=window,
                    validation_anchor_count=validation_anchor_count,
                    min_prior_mean_improvement_ratio_vs_v2=(
                        min_prior_mean_improvement_ratio_vs_v2
                    ),
                ).with_columns(
                    pl.lit(int(window["window_index"])).alias(
                        "evaluation_window_index"
                    ),
                    pl.lit("final_holdout").alias("split_name"),
                )
            )
    return pl.concat(frames, how="diagonal_relaxed").sort(
        [
            "source_model_name",
            "evaluation_window_index",
            "tenant_id",
            "anchor_timestamp",
            "selection_role",
        ]
    )


def validate_dfl_schedule_value_learner_v2_plus_robustness_evidence(
    robustness_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_window_count: int = DEFAULT_ROLLING_VALIDATION_WINDOW_COUNT,
) -> EvidenceCheckOutcome:
    """Validate V2+ rolling robustness coverage and claim boundaries."""

    failures = _missing_column_failures(robustness_frame, REQUIRED_ROBUSTNESS_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(
            False, "; ".join(failures), {"row_count": robustness_frame.height}
        )
    rows = list(robustness_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(
            False, "schedule/value learner v2+ robustness evidence has no rows", {"row_count": 0}
        )

    source_names = source_model_names or tuple(
        _ordered_unique(str(row["source_model_name"]) for row in rows)
    )
    summaries: list[dict[str, Any]] = []
    for source_model_name in source_names:
        source_rows = [
            row for row in rows if str(row["source_model_name"]) == source_model_name
        ]
        source_failures: list[str] = []
        window_indices = sorted({int(row["window_index"]) for row in source_rows})
        if len(window_indices) < min_window_count:
            source_failures.append(
                f"{source_model_name} window_count must be at least {min_window_count}; "
                f"observed {len(window_indices)}"
            )
        if window_indices[:min_window_count] != list(range(1, min_window_count + 1)):
            source_failures.append(
                f"{source_model_name} must include latest-first windows 1..{min_window_count}"
            )
        for row in source_rows:
            if int(row["tenant_count"]) < min_tenant_count:
                source_failures.append(
                    f"{source_model_name} tenant_count must be at least "
                    f"{min_tenant_count}; observed {row['tenant_count']}"
                )
                break
            if int(row["validation_tenant_anchor_count"]) < min_validation_tenant_anchor_count:
                source_failures.append(
                    f"{source_model_name} validation tenant-anchor count must be at least "
                    f"{min_validation_tenant_anchor_count}; observed "
                    f"{row['validation_tenant_anchor_count']}"
                )
                break
        source_failures.extend(_claim_failures(source_rows))
        failures.extend(source_failures)
        summaries.append(_source_summary(source_model_name, source_rows, source_failures))

    robust_source_names = [
        str(summary["source_model_name"])
        for summary in summaries
        if bool(summary["robust_research_challenger"])
    ]
    metadata = {
        "row_count": robustness_frame.height,
        "source_model_count": len(source_names),
        "source_model_names": list(source_names),
        "robust_source_model_names": robust_source_names,
        "model_summaries": summaries,
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Schedule/value learner V2+ robustness evidence has valid rolling-window coverage."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def evaluate_dfl_schedule_value_learner_v2_plus_robustness_gate(
    robustness_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
) -> PromotionGateResult:
    """Report V2+ robust-headline status while market execution stays disabled."""

    evidence = validate_dfl_schedule_value_learner_v2_plus_robustness_evidence(
        robustness_frame,
        source_model_names=source_model_names,
    )
    if not evidence.passed:
        return PromotionGateResult(False, "blocked", evidence.description, evidence.metadata)

    summaries = list(evidence.metadata["model_summaries"])
    robust_sources = [
        str(summary["source_model_name"])
        for summary in summaries
        if bool(summary["robust_research_challenger"])
    ]
    development_sources = [
        str(summary["source_model_name"])
        for summary in summaries
        if int(summary["development_window_count"]) > 0
    ]
    metrics = {
        **evidence.metadata,
        "development_source_model_names": development_sources,
        "production_gate_passed": False,
        "market_execution_enabled": False,
    }
    if robust_sources:
        return PromotionGateResult(
            False,
            "v2_plus_robust_research_challenger",
            "Schedule/value learner V2+ is robust versus strict and frozen V2; "
            "market execution remains disabled.",
            metrics,
        )
    if development_sources:
        return PromotionGateResult(
            False,
            "diagnostic_pass_not_robust",
            "Schedule/value learner V2+ improves over raw in some windows but is "
            f"not robust versus {CONTROL_MODEL_NAME} and frozen V2.",
            metrics,
        )
    return PromotionGateResult(
        False,
        "blocked",
        "Schedule/value learner V2+ did not produce rolling-window development improvement.",
        metrics,
    )


def _window_summary_row(
    frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_name: str,
    window: dict[str, Any],
    validation_anchor_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
    min_prior_mean_improvement_ratio_vs_v2: float,
) -> dict[str, Any]:
    strict_frame, v2_plus_learner_frame = _rolling_window_frames(
        frame,
        tenant_ids=tenant_ids,
        source_model_name=source_model_name,
        window=window,
        validation_anchor_count=validation_anchor_count,
        min_prior_mean_improvement_ratio_vs_v2=(
            min_prior_mean_improvement_ratio_vs_v2
        ),
    )
    strict_rows = _role_rows(strict_frame, source_model_name, "strict_reference")
    raw_rows = _role_rows(strict_frame, source_model_name, "raw_reference")
    v2_rows = _role_rows(
        strict_frame, source_model_name, "schedule_value_learner_v2_reference"
    )
    selected_rows = _role_rows(
        strict_frame, source_model_name, "schedule_value_learner_v2_plus"
    )
    strict_mean = _mean_regret(strict_rows)
    raw_mean = _mean_regret(raw_rows)
    v2_mean = _mean_regret(v2_rows)
    selected_mean = _mean_regret(selected_rows)
    strict_median = _median_regret(strict_rows)
    v2_median = _median_regret(v2_rows)
    selected_median = _median_regret(selected_rows)
    improvement_vs_raw = _improvement_ratio(raw_mean, selected_mean)
    improvement_vs_strict = _improvement_ratio(strict_mean, selected_mean)
    improvement_vs_v2 = _improvement_ratio(v2_mean, selected_mean)
    validation_tenant_anchor_count = len(_tenant_anchor_set(selected_rows))
    development_passed = (
        validation_tenant_anchor_count >= min_validation_tenant_anchor_count
        and improvement_vs_raw > 0.0
    )
    strict_passed = (
        validation_tenant_anchor_count >= min_validation_tenant_anchor_count
        and improvement_vs_strict >= min_mean_regret_improvement_ratio
        and selected_median <= strict_median
    )
    v2_passed = improvement_vs_v2 > 0.0 and selected_median <= v2_median
    v2_plus_window_passed = strict_passed and v2_passed
    return {
        "source_model_name": source_model_name,
        "window_index": int(window["window_index"]),
        "validation_start_anchor_timestamp": window["validation_start_anchor_timestamp"],
        "validation_end_anchor_timestamp": window["validation_end_anchor_timestamp"],
        "tenant_count": len(tenant_ids),
        "validation_anchor_count_per_tenant": validation_anchor_count,
        "validation_tenant_anchor_count": validation_tenant_anchor_count,
        "minimum_prior_anchor_count_before_window": int(
            window["minimum_prior_anchor_count_before_window"]
        ),
        "fallback_to_v2_by_tenant": {
            str(row["tenant_id"]): bool(row["fallback_to_v2"])
            for row in v2_plus_learner_frame.iter_rows(named=True)
        },
        "selected_family_counts": _selected_family_counts(selected_rows),
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "v2_mean_regret_uah": v2_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "v2_median_regret_uah": v2_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "mean_regret_improvement_ratio_vs_v2": improvement_vs_v2,
        "development_passed": development_passed,
        "source_specific_strict_passed": strict_passed,
        "v2_non_degradation_passed": v2_passed,
        "v2_plus_window_passed": v2_plus_window_passed,
        "passing_window_count_for_source": 0,
        "robust_research_challenger": False,
        "production_promote": False,
        "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_CLAIM_SCOPE,
        "academic_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _rolling_window_strict_frame(
    frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_name: str,
    window: dict[str, Any],
    validation_anchor_count: int,
    min_prior_mean_improvement_ratio_vs_v2: float,
) -> pl.DataFrame:
    strict_frame, _ = _rolling_window_frames(
        frame,
        tenant_ids=tenant_ids,
        source_model_name=source_model_name,
        window=window,
        validation_anchor_count=validation_anchor_count,
        min_prior_mean_improvement_ratio_vs_v2=(
            min_prior_mean_improvement_ratio_vs_v2
        ),
    )
    return strict_frame


def _rolling_window_frames(
    frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_name: str,
    window: dict[str, Any],
    validation_anchor_count: int,
    min_prior_mean_improvement_ratio_vs_v2: float,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    validation_start = v2._datetime_value(
        window["validation_start_anchor_timestamp"],
        field_name="validation_start_anchor_timestamp",
    )
    rolling_frame = _rolling_split_frame(
        frame,
        tenant_ids=tenant_ids,
        source_model_name=source_model_name,
        validation_anchors=set(window["validation_anchors"]),
        validation_start=validation_start,
    )
    v2_learner_frame = v2.build_dfl_schedule_value_learner_v2_frame(
        _base_library_frame(rolling_frame),
        tenant_ids=tenant_ids,
        forecast_model_names=(source_model_name,),
        final_validation_anchor_count_per_tenant=validation_anchor_count,
    )
    v2_plus_learner_frame = build_dfl_schedule_value_learner_v2_plus_frame(
        rolling_frame,
        v2_learner_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=(source_model_name,),
        final_validation_anchor_count_per_tenant=validation_anchor_count,
        min_prior_mean_improvement_ratio_vs_v2=min_prior_mean_improvement_ratio_vs_v2,
    )
    return (
        build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
            rolling_frame,
            v2_plus_learner_frame,
            v2_learner_frame,
        ),
        v2_plus_learner_frame,
    )


def _base_library_frame(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.filter(~pl.col("candidate_family").is_in(sorted(V2_PLUS_CANDIDATE_FAMILIES)))


def _rolling_split_frame(
    frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_name: str,
    validation_anchors: set[datetime],
    validation_start: datetime,
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    tenant_set = set(tenant_ids)
    for row in frame.iter_rows(named=True):
        if str(row["tenant_id"]) not in tenant_set:
            continue
        if str(row["source_model_name"]) != source_model_name:
            continue
        anchor = v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        if anchor in validation_anchors:
            new_row = dict(row)
            new_row["split_name"] = "final_holdout"
            rows.append(new_row)
        elif anchor < validation_start:
            new_row = dict(row)
            new_row["split_name"] = "train_selection"
            rows.append(new_row)
    return pl.DataFrame(rows)


def _rolling_windows(
    frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_name: str,
    validation_window_count: int,
    validation_anchor_count: int,
    min_prior_anchors_before_window: int,
) -> list[dict[str, Any]]:
    common_anchors: list[datetime] | None = None
    for tenant_id in tenant_ids:
        rows = [
            row
            for row in frame.iter_rows(named=True)
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
        ]
        anchors = sorted(_anchor_set(rows))
        required_anchor_count = (
            validation_window_count * validation_anchor_count
            + min_prior_anchors_before_window
        )
        if len(anchors) < required_anchor_count:
            raise ValueError(
                "schedule/value learner V2+ robustness requires at least "
                f"{required_anchor_count} anchors for {tenant_id}/{source_model_name}; "
                f"observed {len(anchors)}"
            )
        if common_anchors is None:
            common_anchors = anchors
        elif anchors != common_anchors:
            raise ValueError(f"coverage mismatch across tenants for {source_model_name}")
    if common_anchors is None:
        raise ValueError(f"coverage missing for {source_model_name}")

    windows: list[dict[str, Any]] = []
    for offset in range(validation_window_count):
        end = len(common_anchors) - (offset * validation_anchor_count)
        start = end - validation_anchor_count
        if start < min_prior_anchors_before_window:
            raise ValueError(
                "rolling validation window does not have enough prior anchors before validation start"
            )
        validation_anchors = common_anchors[start:end]
        windows.append(
            {
                "window_index": offset + 1,
                "validation_anchors": validation_anchors,
                "validation_start_anchor_timestamp": validation_anchors[0],
                "validation_end_anchor_timestamp": validation_anchors[-1],
                "minimum_prior_anchor_count_before_window": start,
            }
        )
    return windows


def _validate_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    validation_window_count: int,
    validation_anchor_count: int,
    min_prior_anchors_before_window: int,
    min_robust_passing_windows: int,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one source model.")
    if validation_window_count < 1:
        raise ValueError("validation_window_count must be at least 1.")
    if validation_anchor_count < 1:
        raise ValueError("validation_anchor_count must be at least 1.")
    if min_prior_anchors_before_window < 1:
        raise ValueError("min_prior_anchors_before_window must be at least 1.")
    if min_robust_passing_windows < 1:
        raise ValueError("min_robust_passing_windows must be at least 1.")


def _role_rows(frame: pl.DataFrame, source_model_name: str, role: str) -> list[dict[str, Any]]:
    return [
        row
        for row in frame.iter_rows(named=True)
        if str(row.get("source_model_name", "")) == source_model_name
        and str(row.get("selection_role", "")) == role
    ]


def _claim_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    for row in rows:
        if str(row["claim_scope"]) != DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_CLAIM_SCOPE:
            failures.append("schedule/value learner V2+ robustness rows must use the robustness claim_scope")
            break
        if not bool(row["not_full_dfl"]):
            failures.append("schedule/value learner V2+ robustness rows must remain not_full_dfl")
            break
        if not bool(row["not_market_execution"]):
            failures.append(
                "schedule/value learner V2+ robustness rows must remain not_market_execution"
            )
            break
        if bool(row["production_promote"]):
            failures.append("schedule/value learner V2+ robustness must not set production_promote=true")
            break
    return failures


def _source_summary(
    source_model_name: str,
    source_rows: list[dict[str, Any]],
    failures: list[str],
) -> dict[str, Any]:
    if not source_rows:
        return {
            "source_model_name": source_model_name,
            "window_count": 0,
            "development_window_count": 0,
            "strict_pass_window_count": 0,
            "v2_plus_window_count": 0,
            "robust_research_challenger": False,
            "failures": failures,
        }
    return {
        "source_model_name": source_model_name,
        "window_count": len({int(row["window_index"]) for row in source_rows}),
        "development_window_count": sum(
            1 for row in source_rows if bool(row["development_passed"])
        ),
        "strict_pass_window_count": sum(
            1 for row in source_rows if bool(row["source_specific_strict_passed"])
        ),
        "v2_plus_window_count": sum(
            1 for row in source_rows if bool(row["v2_plus_window_passed"])
        ),
        "robust_research_challenger": any(
            bool(row["robust_research_challenger"]) for row in source_rows
        ),
        "latest_window_passed": any(
            bool(row["v2_plus_window_passed"])
            for row in source_rows
            if int(row["window_index"]) == 1
        ),
        "best_mean_regret_improvement_ratio_vs_strict": max(
            float(row["mean_regret_improvement_ratio_vs_strict"]) for row in source_rows
        ),
        "best_mean_regret_improvement_ratio_vs_v2": max(
            float(row["mean_regret_improvement_ratio_vs_v2"]) for row in source_rows
        ),
        "failures": failures,
    }


def _gate_label(row: dict[str, Any], *, robust: bool) -> str:
    if robust:
        return "robust_research_challenger"
    if bool(row["v2_plus_window_passed"]):
        return "v2_plus_window_pass"
    if bool(row["source_specific_strict_passed"]):
        return "strict_pass_v2_degraded"
    if bool(row["development_passed"]):
        return "development_pass"
    return "blocked"


def _selected_family_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        payload = row.get("evaluation_payload", {})
        family = ""
        if isinstance(payload, dict):
            family = str(payload.get("selector_row_candidate_family", ""))
        counts[family] = counts.get(family, 0) + 1
    return dict(sorted(counts.items()))


def _anchor_set(rows: Any) -> set[datetime]:
    return {
        v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        for row in rows
    }


def _tenant_anchor_set(rows: list[dict[str, Any]]) -> set[tuple[str, datetime]]:
    return {
        (
            str(row["tenant_id"]),
            v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        )
        for row in rows
    }


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    regrets = [float(row["regret_uah"]) for row in rows]
    return mean(regrets) if regrets else 0.0


def _median_regret(rows: list[dict[str, Any]]) -> float:
    regrets = [float(row["regret_uah"]) for row in rows]
    return median(regrets) if regrets else 0.0


def _improvement_ratio(control_value: float, candidate_value: float) -> float:
    return (
        (control_value - candidate_value) / abs(control_value)
        if abs(control_value) > 1e-9
        else 0.0
    )


def _missing_column_failures(
    frame: pl.DataFrame, required_columns: frozenset[str]
) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"missing required columns: {missing}"] if missing else []


def _ordered_unique(values: Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


__all__ = [
    "DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_CLAIM_SCOPE",
    "build_dfl_schedule_value_learner_v2_plus_robustness_frame",
    "build_dfl_schedule_value_learner_v2_plus_rolling_strict_rows_frame",
    "evaluate_dfl_schedule_value_learner_v2_plus_robustness_gate",
    "validate_dfl_schedule_value_learner_v2_plus_robustness_evidence",
]
