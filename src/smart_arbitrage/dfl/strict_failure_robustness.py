"""Rolling-window robustness gate for the strict-failure selector."""

from __future__ import annotations

from datetime import datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.dfl.strict_failure_selector import (
    CANDIDATE_FAMILY_RAW,
    CANDIDATE_FAMILY_STRICT,
    DEFAULT_SWITCH_THRESHOLD_GRID_UAH,
    REFERENCE_FAMILY_ORDER,
    REQUIRED_LIBRARY_COLUMNS,
    strict_failure_selector_model_name,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS_CLAIM_SCOPE: Final[str] = (
    "dfl_strict_failure_selector_robustness_not_full_dfl"
)
DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS_ACADEMIC_SCOPE: Final[str] = (
    "Rolling-window prior-only robustness gate for the strict-failure selector. "
    "It is not full DFL, not Decision Transformer control, and not market execution."
)
DEFAULT_ROLLING_VALIDATION_WINDOW_COUNT: Final[int] = 4
DEFAULT_VALIDATION_ANCHOR_COUNT: Final[int] = 18
DEFAULT_MIN_PRIOR_ANCHORS_BEFORE_WINDOW: Final[int] = 30
DEFAULT_MIN_ROBUST_PASSING_WINDOWS: Final[int] = 3

REQUIRED_ROBUSTNESS_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source_model_name",
        "selector_model_name",
        "window_index",
        "tenant_count",
        "validation_anchor_count_per_tenant",
        "validation_tenant_anchor_count",
        "minimum_prior_anchor_count_before_window",
        "strict_mean_regret_uah",
        "raw_mean_regret_uah",
        "selected_mean_regret_uah",
        "strict_median_regret_uah",
        "selected_median_regret_uah",
        "development_passed",
        "source_specific_strict_passed",
        "robust_research_challenger",
        "production_promote",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_dfl_strict_failure_selector_robustness_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    validation_window_count: int = DEFAULT_ROLLING_VALIDATION_WINDOW_COUNT,
    validation_anchor_count: int = DEFAULT_VALIDATION_ANCHOR_COUNT,
    min_prior_anchors_before_window: int = DEFAULT_MIN_PRIOR_ANCHORS_BEFORE_WINDOW,
    min_prior_anchor_count: int = 3,
    switch_threshold_grid_uah: tuple[float, ...] = DEFAULT_SWITCH_THRESHOLD_GRID_UAH,
    min_robust_passing_windows: int = DEFAULT_MIN_ROBUST_PASSING_WINDOWS,
    min_validation_tenant_anchor_count_per_source_model: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> pl.DataFrame:
    """Replay the strict-failure selector over prior-only rolling windows."""

    _validate_library_frame(schedule_candidate_library_frame)
    _validate_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        validation_window_count=validation_window_count,
        validation_anchor_count=validation_anchor_count,
        min_prior_anchors_before_window=min_prior_anchors_before_window,
        min_prior_anchor_count=min_prior_anchor_count,
        switch_threshold_grid_uah=switch_threshold_grid_uah,
        min_robust_passing_windows=min_robust_passing_windows,
    )

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
                    min_prior_anchor_count=min_prior_anchor_count,
                    switch_threshold_grid_uah=switch_threshold_grid_uah,
                    min_validation_tenant_anchor_count=(
                        min_validation_tenant_anchor_count_per_source_model
                    ),
                    min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
                )
            )
        passing_count = sum(
            1 for row in source_rows if bool(row["source_specific_strict_passed"])
        )
        latest_passed = any(
            bool(row["source_specific_strict_passed"])
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
    return pl.DataFrame(rows).sort(["source_model_name", "window_index"])


def validate_dfl_strict_failure_selector_robustness_evidence(
    robustness_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_window_count: int = DEFAULT_ROLLING_VALIDATION_WINDOW_COUNT,
) -> EvidenceCheckOutcome:
    """Validate structural evidence for the rolling-window robustness gate."""

    failures = _missing_column_failures(robustness_frame, REQUIRED_ROBUSTNESS_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": robustness_frame.height})
    rows = list(robustness_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "strict-failure robustness evidence has no rows", {"row_count": 0})

    source_names = source_model_names or tuple(sorted({str(row["source_model_name"]) for row in rows}))
    summaries: list[dict[str, Any]] = []
    for source_model_name in source_names:
        source_rows = [row for row in rows if str(row["source_model_name"]) == source_model_name]
        window_indices = sorted({int(row["window_index"]) for row in source_rows})
        source_failures: list[str] = []
        if len(window_indices) < min_window_count:
            source_failures.append(
                f"{source_model_name} window_count must be at least {min_window_count}; "
                f"observed {len(window_indices)}"
            )
        if window_indices[:min_window_count] != list(range(1, min_window_count + 1)):
            source_failures.append(f"{source_model_name} must include latest-first windows 1..{min_window_count}")
        for row in source_rows:
            if int(row["tenant_count"]) < min_tenant_count:
                source_failures.append(
                    f"{source_model_name} tenant_count must be at least {min_tenant_count}; "
                    f"observed {row['tenant_count']}"
                )
                break
            if int(row["validation_tenant_anchor_count"]) < min_validation_tenant_anchor_count:
                source_failures.append(
                    f"{source_model_name} validation tenant-anchor count must be at least "
                    f"{min_validation_tenant_anchor_count}; observed {row['validation_tenant_anchor_count']}"
                )
                break
        source_failures.extend(_robustness_claim_failures(source_rows))
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
            "Strict-failure selector robustness evidence has valid rolling-window coverage."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def evaluate_dfl_strict_failure_selector_robustness_gate(
    robustness_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
) -> PromotionGateResult:
    """Report robustness status while keeping production promotion blocked."""

    evidence = validate_dfl_strict_failure_selector_robustness_evidence(
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
    }
    if robust_sources:
        return PromotionGateResult(
            False,
            "robust_research_challenger_production_blocked",
            "At least one source-specific selector is a robust research challenger, "
            "but production promotion remains blocked in this slice.",
            metrics,
        )
    if development_sources:
        return PromotionGateResult(
            False,
            "diagnostic_pass_production_blocked",
            "Selector improves over raw neural schedules in some windows but is not robust versus "
            f"{CONTROL_MODEL_NAME}.",
            metrics,
        )
    return PromotionGateResult(
        False,
        "blocked",
        "Selector did not produce rolling-window development improvement.",
        metrics,
    )


def _window_summary_row(
    frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_name: str,
    window: dict[str, Any],
    min_prior_anchor_count: int,
    switch_threshold_grid_uah: tuple[float, ...],
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
) -> dict[str, Any]:
    strict_rows: list[dict[str, Any]] = []
    raw_rows: list[dict[str, Any]] = []
    selected_rows: list[dict[str, Any]] = []
    selected_thresholds_by_tenant: dict[str, float] = {}
    tenant_summaries: list[dict[str, Any]] = []
    validation_anchors = list(window["validation_anchors"])
    prior_cutoff = _datetime_value(
        window["validation_start_anchor_timestamp"],
        field_name="validation_start_anchor_timestamp",
    )

    for tenant_id in tenant_ids:
        tenant_rows = _library_rows(frame, tenant_id=tenant_id, source_model_name=source_model_name)
        train_decisions = _training_decisions_before(
            tenant_rows,
            prior_cutoff=prior_cutoff,
            min_prior_anchor_count=min_prior_anchor_count,
        )
        if not train_decisions:
            raise ValueError(f"missing prior decisions before validation window for {tenant_id}/{source_model_name}")
        threshold = _select_switch_threshold(train_decisions, switch_threshold_grid_uah)
        selected_thresholds_by_tenant[tenant_id] = threshold

        validation_decisions = [
            _decision_for_anchor(
                tenant_rows,
                anchor_timestamp=anchor,
                prior_cutoff=prior_cutoff,
                min_prior_anchor_count=min_prior_anchor_count,
            )
            for anchor in validation_anchors
        ]
        if any(decision is None for decision in validation_decisions):
            raise ValueError(f"missing validation decision rows for {tenant_id}/{source_model_name}")
        concrete_decisions = [decision for decision in validation_decisions if decision is not None]
        tenant_strict_rows = [decision["strict_row"] for decision in concrete_decisions]
        tenant_raw_rows = [decision["raw_row"] for decision in concrete_decisions]
        tenant_selected_rows = _selected_rows_by_threshold(concrete_decisions, switch_threshold_uah=threshold)
        strict_rows.extend(tenant_strict_rows)
        raw_rows.extend(tenant_raw_rows)
        selected_rows.extend(tenant_selected_rows)
        tenant_summaries.append(
            _tenant_summary(
                tenant_id=tenant_id,
                strict_rows=tenant_strict_rows,
                raw_rows=tenant_raw_rows,
                selected_rows=tenant_selected_rows,
                selected_threshold_uah=threshold,
                min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
            )
        )

    validation_tenant_anchor_count = len(selected_rows)
    strict_mean = _mean_regret(strict_rows)
    raw_mean = _mean_regret(raw_rows)
    selected_mean = _mean_regret(selected_rows)
    strict_median = _median_regret(strict_rows)
    selected_median = _median_regret(selected_rows)
    improvement_vs_raw = _improvement_ratio(raw_mean, selected_mean)
    improvement_vs_strict = _improvement_ratio(strict_mean, selected_mean)
    development_passed = validation_tenant_anchor_count >= min_validation_tenant_anchor_count and improvement_vs_raw > 0.0
    source_specific_strict_passed = (
        validation_tenant_anchor_count >= min_validation_tenant_anchor_count
        and improvement_vs_strict >= min_mean_regret_improvement_ratio
        and selected_median <= strict_median
    )
    return {
        "source_model_name": source_model_name,
        "selector_model_name": strict_failure_selector_model_name(source_model_name),
        "window_index": int(window["window_index"]),
        "validation_start_anchor_timestamp": window["validation_start_anchor_timestamp"],
        "validation_end_anchor_timestamp": window["validation_end_anchor_timestamp"],
        "tenant_count": len(tenant_ids),
        "validation_anchor_count_per_tenant": len(validation_anchors),
        "validation_tenant_anchor_count": validation_tenant_anchor_count,
        "minimum_prior_anchor_count_before_window": int(
            window["minimum_prior_anchor_count_before_window"]
        ),
        "selected_thresholds_by_tenant": selected_thresholds_by_tenant,
        "selected_family_counts": _family_counts(selected_rows),
        "tenant_summaries": tenant_summaries,
        "tenant_strict_pass_count": sum(
            1 for summary in tenant_summaries if bool(summary["source_specific_strict_passed"])
        ),
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "development_passed": development_passed,
        "source_specific_strict_passed": source_specific_strict_passed,
        "passing_window_count_for_source": 0,
        "robust_research_challenger": False,
        "production_promote": False,
        "claim_scope": DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS_CLAIM_SCOPE,
        "academic_scope": DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


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
        tenant_rows = _library_rows(frame, tenant_id=tenant_id, source_model_name=source_model_name)
        anchors = sorted(_anchor_set(tenant_rows))
        required_anchor_count = (
            validation_window_count * validation_anchor_count + min_prior_anchors_before_window
        )
        if len(anchors) < required_anchor_count:
            raise ValueError(
                "rolling validation requires at least "
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


def _training_decisions_before(
    rows: list[dict[str, Any]],
    *,
    prior_cutoff: datetime,
    min_prior_anchor_count: int,
) -> list[dict[str, Any]]:
    decisions: list[dict[str, Any]] = []
    for anchor_timestamp in sorted(anchor for anchor in _anchor_set(rows) if anchor < prior_cutoff):
        decision = _decision_for_anchor(
            rows,
            anchor_timestamp=anchor_timestamp,
            prior_cutoff=anchor_timestamp,
            min_prior_anchor_count=min_prior_anchor_count,
        )
        if decision is not None:
            decisions.append(decision)
    return decisions


def _decision_for_anchor(
    rows: list[dict[str, Any]],
    *,
    anchor_timestamp: datetime,
    prior_cutoff: datetime,
    min_prior_anchor_count: int,
) -> dict[str, Any] | None:
    anchor_rows = [
        row
        for row in rows
        if _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp") == anchor_timestamp
    ]
    strict_row = _single_family_row(anchor_rows, CANDIDATE_FAMILY_STRICT)
    raw_row = _single_family_row(anchor_rows, CANDIDATE_FAMILY_RAW)
    strict_prior_mean = _prior_mean_regret(
        rows,
        strict_row,
        prior_cutoff=prior_cutoff,
        min_prior_anchor_count=min_prior_anchor_count,
    )
    if strict_prior_mean is None:
        return None

    non_strict_candidates: list[dict[str, Any]] = []
    for row in anchor_rows:
        if str(row["candidate_family"]) == CANDIDATE_FAMILY_STRICT:
            continue
        prior_mean = _prior_mean_regret(
            rows,
            row,
            prior_cutoff=prior_cutoff,
            min_prior_anchor_count=min_prior_anchor_count,
        )
        if prior_mean is None:
            continue
        candidate = dict(row)
        candidate["selector_prior_mean_regret_uah"] = prior_mean
        non_strict_candidates.append(candidate)
    if not non_strict_candidates:
        return None
    best_non_strict = min(
        non_strict_candidates,
        key=lambda row: (
            float(row["selector_prior_mean_regret_uah"]),
            _family_sort_index(str(row["candidate_family"])),
            str(row["candidate_model_name"]),
        ),
    )
    return {
        "anchor_timestamp": anchor_timestamp,
        "strict_row": dict(strict_row),
        "raw_row": dict(raw_row),
        "best_prior_non_strict_row": best_non_strict,
        "strict_prior_mean_regret_uah": strict_prior_mean,
        "best_non_strict_prior_mean_regret_uah": float(
            best_non_strict["selector_prior_mean_regret_uah"]
        ),
        "prior_advantage_uah": strict_prior_mean
        - float(best_non_strict["selector_prior_mean_regret_uah"]),
    }


def _select_switch_threshold(
    train_decisions: list[dict[str, Any]],
    switch_threshold_grid_uah: tuple[float, ...],
) -> float:
    return min(
        switch_threshold_grid_uah,
        key=lambda threshold: (
            _mean_regret(_selected_rows_by_threshold(train_decisions, switch_threshold_uah=threshold)),
            threshold,
        ),
    )


def _selected_rows_by_threshold(
    decisions: list[dict[str, Any]],
    *,
    switch_threshold_uah: float,
) -> list[dict[str, Any]]:
    return [_selected_row(decision, switch_threshold_uah=switch_threshold_uah) for decision in decisions]


def _selected_row(decision: dict[str, Any], *, switch_threshold_uah: float) -> dict[str, Any]:
    if float(decision["prior_advantage_uah"]) >= switch_threshold_uah:
        return dict(decision["best_prior_non_strict_row"])
    return dict(decision["strict_row"])


def _tenant_summary(
    *,
    tenant_id: str,
    strict_rows: list[dict[str, Any]],
    raw_rows: list[dict[str, Any]],
    selected_rows: list[dict[str, Any]],
    selected_threshold_uah: float,
    min_mean_regret_improvement_ratio: float,
) -> dict[str, Any]:
    strict_mean = _mean_regret(strict_rows)
    raw_mean = _mean_regret(raw_rows)
    selected_mean = _mean_regret(selected_rows)
    strict_median = _median_regret(strict_rows)
    selected_median = _median_regret(selected_rows)
    return {
        "tenant_id": tenant_id,
        "selected_threshold_uah": selected_threshold_uah,
        "validation_anchor_count": len(selected_rows),
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_raw": _improvement_ratio(raw_mean, selected_mean),
        "mean_regret_improvement_ratio_vs_strict": _improvement_ratio(strict_mean, selected_mean),
        "source_specific_strict_passed": (
            _improvement_ratio(strict_mean, selected_mean) >= min_mean_regret_improvement_ratio
            and selected_median <= strict_median
        ),
    }


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
            "robust_research_challenger": False,
            "failures": failures,
        }
    return {
        "source_model_name": source_model_name,
        "window_count": len({int(row["window_index"]) for row in source_rows}),
        "development_window_count": sum(1 for row in source_rows if bool(row["development_passed"])),
        "strict_pass_window_count": sum(
            1 for row in source_rows if bool(row["source_specific_strict_passed"])
        ),
        "robust_research_challenger": any(
            bool(row["robust_research_challenger"]) for row in source_rows
        ),
        "latest_window_passed": any(
            bool(row["source_specific_strict_passed"])
            for row in source_rows
            if int(row["window_index"]) == 1
        ),
        "best_mean_regret_improvement_ratio_vs_strict": max(
            float(row["mean_regret_improvement_ratio_vs_strict"]) for row in source_rows
        ),
        "failures": failures,
    }


def _gate_label(row: dict[str, Any], *, robust: bool) -> str:
    if robust:
        return "robust_research_challenger"
    if bool(row["source_specific_strict_passed"]):
        return "source_specific_strict_pass"
    if bool(row["development_passed"]):
        return "development_pass"
    return "blocked"


def _validate_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    validation_window_count: int,
    validation_anchor_count: int,
    min_prior_anchors_before_window: int,
    min_prior_anchor_count: int,
    switch_threshold_grid_uah: tuple[float, ...],
    min_robust_passing_windows: int,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if validation_window_count < 1:
        raise ValueError("validation_window_count must be at least 1.")
    if validation_anchor_count < 1:
        raise ValueError("validation_anchor_count must be at least 1.")
    if min_prior_anchors_before_window < 1:
        raise ValueError("min_prior_anchors_before_window must be at least 1.")
    if min_prior_anchor_count < 1:
        raise ValueError("min_prior_anchor_count must be at least 1.")
    if not switch_threshold_grid_uah:
        raise ValueError("switch_threshold_grid_uah must contain at least one threshold.")
    if min_robust_passing_windows < 1:
        raise ValueError("min_robust_passing_windows must be at least 1.")


def _validate_library_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_LIBRARY_COLUMNS, frame_name="dfl_schedule_candidate_library_v2_frame")
    for row in frame.iter_rows(named=True):
        horizon_hours = int(row["horizon_hours"])
        for column in (
            "forecast_price_uah_mwh_vector",
            "actual_price_uah_mwh_vector",
            "dispatch_mw_vector",
            "soc_fraction_vector",
        ):
            if len(_float_list(row[column], field_name=column)) != horizon_hours:
                raise ValueError(f"vector length must match horizon_hours for {column}")
        if str(row["data_quality_tier"]) != "thesis_grade":
            raise ValueError("strict-failure robustness requires thesis_grade rows")
        if float(row["observed_coverage_ratio"]) < 1.0:
            raise ValueError("strict-failure robustness requires observed coverage ratio of 1.0")
        if int(row["safety_violation_count"]):
            raise ValueError("strict-failure robustness requires zero safety violations")
        if not bool(row["not_full_dfl"]):
            raise ValueError("strict-failure robustness requires not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError("strict-failure robustness requires not_market_execution=true")
    split_by_anchor: dict[tuple[str, str, datetime], set[str]] = {}
    for row in frame.iter_rows(named=True):
        key = (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        )
        split_by_anchor.setdefault(key, set()).add(str(row["split_name"]))
    if any(len(splits) > 1 for splits in split_by_anchor.values()):
        raise ValueError("train/final overlap is not allowed in strict-failure robustness evidence")


def _robustness_claim_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    for row in rows:
        if str(row["claim_scope"]) != DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS_CLAIM_SCOPE:
            failures.append("strict-failure robustness rows must use the robustness claim_scope")
            break
        if not bool(row["not_full_dfl"]):
            failures.append("strict-failure robustness rows must remain not_full_dfl")
            break
        if not bool(row["not_market_execution"]):
            failures.append("strict-failure robustness rows must remain not_market_execution")
            break
        if bool(row["production_promote"]):
            failures.append("strict-failure robustness must not set production_promote=true")
            break
    return failures


def _library_rows(frame: pl.DataFrame, *, tenant_id: str, source_model_name: str) -> list[dict[str, Any]]:
    rows = [
        row
        for row in frame.iter_rows(named=True)
        if str(row["tenant_id"]) == tenant_id and str(row["source_model_name"]) == source_model_name
    ]
    if not rows:
        raise ValueError(f"coverage missing schedule candidate rows for {tenant_id}/{source_model_name}")
    return rows


def _single_family_row(rows: list[dict[str, Any]], candidate_family: str) -> dict[str, Any]:
    matches = [row for row in rows if str(row["candidate_family"]) == candidate_family]
    if not matches:
        raise ValueError(f"missing {candidate_family} row")
    return matches[0]


def _prior_mean_regret(
    rows: list[dict[str, Any]],
    row: dict[str, Any],
    *,
    prior_cutoff: datetime,
    min_prior_anchor_count: int,
) -> float | None:
    prior_rows = [
        candidate
        for candidate in rows
        if str(candidate["candidate_family"]) == str(row["candidate_family"])
        and str(candidate["candidate_model_name"]) == str(row["candidate_model_name"])
        and _datetime_value(candidate["anchor_timestamp"], field_name="anchor_timestamp") < prior_cutoff
    ]
    if len(_anchor_set(prior_rows)) < min_prior_anchor_count:
        return None
    return _mean_regret(prior_rows)


def _anchor_set(rows: Any) -> set[datetime]:
    return {
        _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        for row in rows
    }


def _family_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        family = str(row["candidate_family"])
        counts[family] = counts.get(family, 0) + 1
    return dict(sorted(counts.items()))


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    regrets = [float(row["regret_uah"]) for row in rows]
    return mean(regrets) if regrets else 0.0


def _median_regret(rows: list[dict[str, Any]]) -> float:
    regrets = [float(row["regret_uah"]) for row in rows]
    return median(regrets) if regrets else 0.0


def _improvement_ratio(baseline: float, candidate: float) -> float:
    return (baseline - candidate) / abs(baseline) if abs(baseline) > 1e-9 else 0.0


def _family_sort_index(candidate_family: str) -> int:
    if candidate_family in REFERENCE_FAMILY_ORDER:
        return REFERENCE_FAMILY_ORDER.index(candidate_family)
    return len(REFERENCE_FAMILY_ORDER)


def _float_list(value: object, *, field_name: str) -> list[float]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return [float(item) for item in value]


def _datetime_value(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise ValueError(f"{field_name} must be a datetime")


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"missing required columns: {missing}"] if missing else []


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    failures = _missing_column_failures(frame, required_columns)
    if failures:
        raise ValueError(f"{frame_name} " + "; ".join(failures))
