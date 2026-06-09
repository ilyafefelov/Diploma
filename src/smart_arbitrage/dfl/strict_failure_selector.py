"""Prior-only selector for strict-control failure opportunities."""

from __future__ import annotations

from datetime import UTC, datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_STRICT_FAILURE_SELECTOR_CLAIM_SCOPE: Final[str] = (
    "dfl_strict_failure_selector_v1_not_full_dfl"
)
DFL_STRICT_FAILURE_SELECTOR_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_strict_failure_selector_v1_strict_lp_gate_not_full_dfl"
)
DFL_STRICT_FAILURE_SELECTOR_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_strict_failure_selector_strict_lp_benchmark"
)
DFL_STRICT_FAILURE_SELECTOR_PREFIX: Final[str] = "dfl_strict_failure_selector_v1_"
DFL_STRICT_FAILURE_SELECTOR_ACADEMIC_SCOPE: Final[str] = (
    "Prior-only selector that learns when to distrust strict_similar_day. "
    "It is not full DFL, not Decision Transformer control, and not market execution."
)

CANDIDATE_FAMILY_STRICT: Final[str] = "strict_control"
CANDIDATE_FAMILY_RAW: Final[str] = "raw_source"
DEFAULT_SWITCH_THRESHOLD_GRID_UAH: Final[tuple[float, ...]] = (
    0.0,
    50.0,
    100.0,
    200.0,
    400.0,
)
REFERENCE_FAMILY_ORDER: Final[tuple[str, ...]] = (
    CANDIDATE_FAMILY_RAW,
    "strict_raw_blend_v2",
    "strict_prior_residual_v2",
    "forecast_perturbation",
    "panel_v2",
    "decision_target_v3",
    "action_target_v4",
)
REQUIRED_LIBRARY_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "candidate_family",
        "candidate_model_name",
        "anchor_timestamp",
        "split_name",
        "horizon_hours",
        "forecast_price_uah_mwh_vector",
        "actual_price_uah_mwh_vector",
        "dispatch_mw_vector",
        "soc_fraction_vector",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "regret_ratio",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "forecast_spread_uah_mwh",
        "safety_violation_count",
        "data_quality_tier",
        "observed_coverage_ratio",
        "not_full_dfl",
        "not_market_execution",
        "evaluation_payload",
    }
)
REQUIRED_AUTOPSY_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "strict_regret_uah",
        "best_non_strict_regret_uah",
        "strict_gap_to_best_non_strict_uah",
        "recommended_next_action",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_SELECTOR_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "selector_model_name",
        "selected_switch_threshold_uah",
        "train_selection_anchor_count",
        "final_holdout_anchor_count",
        "final_holdout_tenant_anchor_count",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_EVALUATION_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "evaluation_id",
        "tenant_id",
        "source_model_name",
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
)


def strict_failure_selector_model_name(source_model_name: str) -> str:
    """Return the v1 strict-failure selector model name."""

    return f"{DFL_STRICT_FAILURE_SELECTOR_PREFIX}{source_model_name}"


def build_dfl_strict_failure_selector_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    dfl_strict_baseline_autopsy_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    switch_threshold_grid_uah: tuple[float, ...] = DEFAULT_SWITCH_THRESHOLD_GRID_UAH,
    min_prior_anchor_count: int = 1,
    min_final_holdout_tenant_anchor_count_per_source_model: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> pl.DataFrame:
    """Fit a threshold selector from train-selection anchors only."""

    _validate_library_frame(schedule_candidate_library_frame)
    _validate_autopsy_frame(dfl_strict_baseline_autopsy_frame)
    _validate_selector_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        switch_threshold_grid_uah=switch_threshold_grid_uah,
        min_prior_anchor_count=min_prior_anchor_count,
    )

    autopsy_opportunity_count = _strict_failure_opportunity_count(dfl_strict_baseline_autopsy_frame)
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = _library_rows(
                schedule_candidate_library_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
            )
            train_decisions = _anchor_decisions(
                source_rows,
                split_name="train_selection",
                min_prior_anchor_count=min_prior_anchor_count,
            )
            final_decisions = _anchor_decisions(
                source_rows,
                split_name="final_holdout",
                min_prior_anchor_count=min_prior_anchor_count,
            )
            final_tenant_anchor_count = len(final_decisions) * len(tenant_ids)
            if final_tenant_anchor_count < min_final_holdout_tenant_anchor_count_per_source_model:
                raise ValueError(
                    "final-holdout tenant-anchor count must be at least "
                    f"{min_final_holdout_tenant_anchor_count_per_source_model}; "
                    f"observed {final_tenant_anchor_count}"
                )
            if not train_decisions:
                raise ValueError(f"missing train-selection prior decisions for {tenant_id}/{source_model_name}")
            threshold = _select_switch_threshold(train_decisions, switch_threshold_grid_uah)
            selected_train_rows = _selected_rows_by_threshold(train_decisions, switch_threshold_uah=threshold)
            selected_final_rows = _selected_rows_by_threshold(final_decisions, switch_threshold_uah=threshold)
            rows.append(
                _selector_row(
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                    selected_switch_threshold_uah=threshold,
                    train_decisions=train_decisions,
                    final_decisions=final_decisions,
                    selected_train_rows=selected_train_rows,
                    selected_final_rows=selected_final_rows,
                    tenant_count=len(tenant_ids),
                    min_final_holdout_tenant_anchor_count_per_source_model=(
                        min_final_holdout_tenant_anchor_count_per_source_model
                    ),
                    strict_failure_opportunity_count=autopsy_opportunity_count,
                )
            )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id"])


def build_dfl_strict_failure_selector_strict_lp_benchmark_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    dfl_strict_failure_selector_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict/raw/best-prior/selector rows for the strict-failure selector gate."""

    _validate_library_frame(schedule_candidate_library_frame)
    _require_columns(
        dfl_strict_failure_selector_frame,
        REQUIRED_SELECTOR_COLUMNS,
        frame_name="dfl_strict_failure_selector_frame",
    )
    resolved_generated_at = generated_at or datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    for selector_row in dfl_strict_failure_selector_frame.iter_rows(named=True):
        tenant_id = str(selector_row["tenant_id"])
        source_model_name = str(selector_row["source_model_name"])
        threshold = float(selector_row["selected_switch_threshold_uah"])
        source_rows = _library_rows(
            schedule_candidate_library_frame,
            tenant_id=tenant_id,
            source_model_name=source_model_name,
        )
        decisions = _anchor_decisions(
            source_rows,
            split_name="final_holdout",
            min_prior_anchor_count=int(selector_row.get("min_prior_anchor_count", 1)),
        )
        for decision in decisions:
            strict_row = decision["strict_row"]
            raw_row = _single_anchor_family_row(
                source_rows,
                anchor_timestamp=decision["anchor_timestamp"],
                candidate_family=CANDIDATE_FAMILY_RAW,
            )
            best_prior_non_strict_row = decision["best_prior_non_strict_row"]
            selected_row = _selected_row(decision, switch_threshold_uah=threshold)
            rows.append(
                _strict_benchmark_row(
                    strict_row,
                    source_model_name=source_model_name,
                    selector_row=selector_row,
                    role="strict_reference",
                    generated_at=resolved_generated_at,
                )
            )
            rows.append(
                _strict_benchmark_row(
                    raw_row,
                    source_model_name=source_model_name,
                    selector_row=selector_row,
                    role="raw_reference",
                    generated_at=resolved_generated_at,
                )
            )
            rows.append(
                _strict_benchmark_row(
                    best_prior_non_strict_row,
                    source_model_name=source_model_name,
                    selector_row=selector_row,
                    role="best_prior_non_strict_reference",
                    generated_at=resolved_generated_at,
                )
            )
            rows.append(
                _strict_benchmark_row(
                    selected_row,
                    source_model_name=source_model_name,
                    selector_row=selector_row,
                    role="selector",
                    generated_at=resolved_generated_at,
                )
            )
    return pl.DataFrame(rows).sort(["tenant_id", "source_model_name", "anchor_timestamp", "forecast_model_name"])


def validate_dfl_strict_failure_selector_evidence(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    control_model_name: str = CONTROL_MODEL_NAME,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> EvidenceCheckOutcome:
    """Validate coverage and provenance for strict-failure selector evidence."""

    failures = _missing_column_failures(strict_frame, REQUIRED_EVALUATION_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": strict_frame.height})

    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "strict-failure selector evidence has no rows", {"row_count": 0})

    source_names = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
    summaries: list[dict[str, Any]] = []
    for source_model_name in source_names:
        summary, summary_failures = _selector_gate_summary(
            rows,
            source_model_name=source_model_name,
            control_model_name=control_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
            include_promotion_failures=False,
        )
        summaries.append(summary)
        failures.extend(summary_failures)

    metadata = {
        "row_count": strict_frame.height,
        "source_model_count": len(source_names),
        "source_model_names": list(source_names),
        "model_summaries": summaries,
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Strict-failure selector evidence has valid coverage and provenance."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def evaluate_dfl_strict_failure_selector_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    control_model_name: str = CONTROL_MODEL_NAME,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> PromotionGateResult:
    """Evaluate whether the prior-only strict-failure selector beats strict control."""

    failures = _missing_column_failures(strict_frame, REQUIRED_EVALUATION_COLUMNS)
    if failures:
        return PromotionGateResult(False, "blocked", "; ".join(failures), {})
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return PromotionGateResult(False, "blocked", "strict-failure selector strict frame has no rows", {})

    source_names = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
    summaries: list[dict[str, Any]] = []
    all_failures: list[str] = []
    for source_model_name in source_names:
        summary, summary_failures = _selector_gate_summary(
            rows,
            source_model_name=source_model_name,
            control_model_name=control_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
            include_promotion_failures=True,
        )
        summaries.append(summary)
        all_failures.extend(summary_failures)

    production_passing = [summary for summary in summaries if summary["production_gate_passed"]]
    development_passing = [summary for summary in summaries if summary["development_gate_passed"]]
    best = max(summaries, key=lambda summary: float(summary["mean_regret_improvement_ratio_vs_strict"]))
    metrics = {
        "best_source_model_name": best["source_model_name"],
        "tenant_count": best["tenant_count"],
        "validation_tenant_anchor_count": best["validation_tenant_anchor_count"],
        "strict_mean_regret_uah": best["strict_mean_regret_uah"],
        "raw_mean_regret_uah": best["raw_mean_regret_uah"],
        "selected_mean_regret_uah": best["selected_mean_regret_uah"],
        "strict_median_regret_uah": best["strict_median_regret_uah"],
        "selected_median_regret_uah": best["selected_median_regret_uah"],
        "mean_regret_improvement_ratio_vs_strict": best["mean_regret_improvement_ratio_vs_strict"],
        "mean_regret_improvement_ratio_vs_raw": best["mean_regret_improvement_ratio_vs_raw"],
        "development_gate_passed": bool(development_passing),
        "production_gate_passed": bool(production_passing),
        "passing_source_model_names": [str(summary["source_model_name"]) for summary in production_passing],
        "model_summaries": summaries,
    }
    if production_passing and not all_failures:
        return PromotionGateResult(
            True,
            "promote",
            "strict-failure selector passes strict LP/oracle gate",
            metrics,
        )
    if development_passing:
        return PromotionGateResult(
            False,
            "diagnostic_pass_production_blocked",
            "strict-failure selector improves over raw neural schedules but remains blocked versus "
            f"{control_model_name}: " + "; ".join(all_failures),
            metrics,
        )
    description = "; ".join(all_failures) if all_failures else "strict-failure selector has no development improvement"
    return PromotionGateResult(False, "blocked", description, metrics)


def _validate_selector_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    switch_threshold_grid_uah: tuple[float, ...],
    min_prior_anchor_count: int,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if not switch_threshold_grid_uah:
        raise ValueError("switch_threshold_grid_uah must contain at least one threshold.")
    if min_prior_anchor_count < 1:
        raise ValueError("min_prior_anchor_count must be at least 1.")


def _validate_library_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_LIBRARY_COLUMNS, frame_name="schedule_candidate_library_frame")
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
            raise ValueError("strict-failure selector requires thesis_grade rows")
        if float(row["observed_coverage_ratio"]) < 1.0:
            raise ValueError("strict-failure selector requires observed coverage ratio of 1.0")
        if int(row["safety_violation_count"]):
            raise ValueError("strict-failure selector requires zero safety violations")
        if not bool(row["not_full_dfl"]):
            raise ValueError("strict-failure selector requires not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError("strict-failure selector requires not_market_execution=true")
    split_by_anchor: dict[tuple[str, str, datetime], set[str]] = {}
    for row in frame.iter_rows(named=True):
        key = (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        )
        split_by_anchor.setdefault(key, set()).add(str(row["split_name"]))
    if any(len(splits) > 1 for splits in split_by_anchor.values()):
        raise ValueError("train/final overlap is not allowed in strict-failure selector evidence")


def _validate_autopsy_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_AUTOPSY_COLUMNS, frame_name="dfl_strict_baseline_autopsy_frame")
    for row in frame.iter_rows(named=True):
        if not bool(row["not_full_dfl"]):
            raise ValueError("strict-failure autopsy rows must remain not_full_dfl")
        if not bool(row["not_market_execution"]):
            raise ValueError("strict-failure autopsy rows must remain not_market_execution")


def _selector_row(
    *,
    tenant_id: str,
    source_model_name: str,
    selected_switch_threshold_uah: float,
    train_decisions: list[dict[str, Any]],
    final_decisions: list[dict[str, Any]],
    selected_train_rows: list[dict[str, Any]],
    selected_final_rows: list[dict[str, Any]],
    tenant_count: int,
    min_final_holdout_tenant_anchor_count_per_source_model: int,
    strict_failure_opportunity_count: int,
) -> dict[str, Any]:
    train_strict_rows = [decision["strict_row"] for decision in train_decisions]
    final_strict_rows = [decision["strict_row"] for decision in final_decisions]
    train_best_rows = [decision["best_prior_non_strict_row"] for decision in train_decisions]
    final_best_rows = [decision["best_prior_non_strict_row"] for decision in final_decisions]
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "selector_model_name": strict_failure_selector_model_name(source_model_name),
        "selected_switch_threshold_uah": selected_switch_threshold_uah,
        "min_prior_anchor_count": min(
            int(decision["strict_prior_anchor_count"]) for decision in [*train_decisions, *final_decisions]
        ),
        "train_selection_anchor_count": len(train_decisions),
        "final_holdout_anchor_count": len(final_decisions),
        "final_holdout_tenant_anchor_count": len(final_decisions) * tenant_count,
        "min_final_holdout_tenant_anchor_count_per_source_model": (
            min_final_holdout_tenant_anchor_count_per_source_model
        ),
        "train_switch_count": _switch_count(train_decisions, selected_train_rows),
        "final_switch_count": _switch_count(final_decisions, selected_final_rows),
        "train_strict_mean_regret_uah": _mean_regret(train_strict_rows),
        "train_best_prior_non_strict_mean_regret_uah": _mean_regret(train_best_rows),
        "selected_train_mean_regret_uah": _mean_regret(selected_train_rows),
        "selected_train_median_regret_uah": _median_regret(selected_train_rows),
        "final_strict_mean_regret_uah": _mean_regret(final_strict_rows),
        "final_best_prior_non_strict_mean_regret_uah": _mean_regret(final_best_rows),
        "selected_final_mean_regret_uah": _mean_regret(selected_final_rows),
        "selected_final_median_regret_uah": _median_regret(selected_final_rows),
        "selected_final_family_counts": _family_counts(selected_final_rows),
        "strict_failure_opportunity_count": strict_failure_opportunity_count,
        "claim_scope": DFL_STRICT_FAILURE_SELECTOR_CLAIM_SCOPE,
        "academic_scope": DFL_STRICT_FAILURE_SELECTOR_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _anchor_decisions(
    source_rows: list[dict[str, Any]],
    *,
    split_name: str,
    min_prior_anchor_count: int,
) -> list[dict[str, Any]]:
    decisions: list[dict[str, Any]] = []
    for anchor_timestamp in sorted(_anchor_set(row for row in source_rows if str(row["split_name"]) == split_name)):
        anchor_rows = [
            row
            for row in source_rows
            if str(row["split_name"]) == split_name
            and _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp") == anchor_timestamp
        ]
        strict_row = _single_family_row(anchor_rows, CANDIDATE_FAMILY_STRICT)
        strict_prior = _prior_mean_regret(
            source_rows,
            strict_row,
            anchor_timestamp=anchor_timestamp,
            min_prior_anchor_count=min_prior_anchor_count,
        )
        if strict_prior is None:
            continue
        non_strict_candidates: list[dict[str, Any]] = []
        for row in anchor_rows:
            if str(row["candidate_family"]) == CANDIDATE_FAMILY_STRICT:
                continue
            prior_mean = _prior_mean_regret(
                source_rows,
                row,
                anchor_timestamp=anchor_timestamp,
                min_prior_anchor_count=min_prior_anchor_count,
            )
            if prior_mean is None:
                continue
            candidate = dict(row)
            candidate["selector_prior_mean_regret_uah"] = prior_mean
            non_strict_candidates.append(candidate)
        if not non_strict_candidates:
            continue
        best_non_strict = min(
            non_strict_candidates,
            key=lambda row: (
                float(row["selector_prior_mean_regret_uah"]),
                _family_sort_index(str(row["candidate_family"])),
                str(row["candidate_model_name"]),
            ),
        )
        decisions.append(
            {
                "anchor_timestamp": anchor_timestamp,
                "strict_row": strict_row,
                "best_prior_non_strict_row": best_non_strict,
                "strict_prior_mean_regret_uah": strict_prior,
                "best_non_strict_prior_mean_regret_uah": float(
                    best_non_strict["selector_prior_mean_regret_uah"]
                ),
                "prior_advantage_uah": strict_prior
                - float(best_non_strict["selector_prior_mean_regret_uah"]),
                "strict_prior_anchor_count": _prior_anchor_count(source_rows, strict_row, anchor_timestamp),
            }
        )
    return decisions


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


def _strict_benchmark_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    selector_row: dict[str, Any],
    role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(_payload(row))
    candidate_family = str(row["candidate_family"])
    selector_model_name = strict_failure_selector_model_name(source_model_name)
    forecast_model_name = selector_model_name if role == "selector" else str(row["candidate_model_name"])
    anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    payload.update(
        {
            "strict_gate_kind": "dfl_strict_failure_selector_strict_lp",
            "source_forecast_model_name": source_model_name,
            "selector_model_name": selector_model_name,
            "selector_selected_switch_threshold_uah": float(selector_row["selected_switch_threshold_uah"]),
            "selector_row_candidate_family": candidate_family,
            "selector_row_candidate_model_name": str(row["candidate_model_name"]),
            "selector_row_role": role,
            "claim_scope": DFL_STRICT_FAILURE_SELECTOR_STRICT_CLAIM_SCOPE,
            "academic_scope": DFL_STRICT_FAILURE_SELECTOR_ACADEMIC_SCOPE,
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": int(row["safety_violation_count"]),
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:strict-failure-selector:{source_model_name}:"
            f"{role}:{candidate_family}:{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": DFL_STRICT_FAILURE_SELECTOR_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": _first_or_default(row["soc_fraction_vector"], default=0.5),
        "starting_soc_source": "schedule_candidate_library_v2",
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": _committed_action(row),
        "committed_power_mw": abs(_first_or_default(row["dispatch_mw_vector"], default=0.0)),
        "rank_by_regret": 1,
        "evaluation_payload": payload,
    }


def _selector_gate_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    control_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
    include_promotion_failures: bool,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    selected_model_name = strict_failure_selector_model_name(source_model_name)
    source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
    strict_rows = [row for row in source_rows if row["forecast_model_name"] == control_model_name]
    raw_rows = [row for row in source_rows if row["forecast_model_name"] == source_model_name]
    selected_rows = [row for row in source_rows if row["forecast_model_name"] == selected_model_name]
    strict_anchors = _tenant_anchor_set(strict_rows)
    raw_anchors = _tenant_anchor_set(raw_rows)
    selected_anchors = _tenant_anchor_set(selected_rows)
    if strict_anchors != raw_anchors or strict_anchors != selected_anchors:
        failures.append(f"{source_model_name} strict/raw/selector rows must cover matching tenant-anchor sets")
    tenant_count = len({tenant_id for tenant_id, _ in selected_anchors})
    validation_count = len(selected_anchors)
    if tenant_count < min_tenant_count:
        failures.append(f"{source_model_name} tenant_count must be at least {min_tenant_count}; observed {tenant_count}")
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {validation_count}"
        )
    failures.extend(_provenance_failures([*strict_rows, *raw_rows, *selected_rows]))

    strict_mean = _mean_regret(strict_rows)
    raw_mean = _mean_regret(raw_rows)
    selected_mean = _mean_regret(selected_rows)
    strict_median = _median_regret(strict_rows)
    selected_median = _median_regret(selected_rows)
    improvement_vs_raw = _improvement_ratio(raw_mean, selected_mean)
    improvement_vs_strict = _improvement_ratio(strict_mean, selected_mean)
    development_passed = validation_count >= min_validation_tenant_anchor_count and improvement_vs_raw > 0.0
    production_passed = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_strict >= min_mean_regret_improvement_ratio
        and selected_median <= strict_median
        and not failures
    )
    if include_promotion_failures:
        if selected_rows and strict_rows and improvement_vs_strict < min_mean_regret_improvement_ratio:
            failures.append(
                f"{source_model_name} mean regret improvement vs {control_model_name} must be at least "
                f"{min_mean_regret_improvement_ratio:.1%}; observed {improvement_vs_strict:.1%}"
            )
        if selected_rows and strict_rows and selected_median > strict_median:
            failures.append(
                f"{source_model_name} median regret must not be worse than {control_model_name}; "
                f"observed selector={selected_median:.2f}, strict={strict_median:.2f}"
            )
    return {
        "source_model_name": source_model_name,
        "selector_model_name": selected_model_name,
        "tenant_count": tenant_count,
        "validation_tenant_anchor_count": validation_count,
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "development_gate_passed": development_passed,
        "production_gate_passed": production_passed,
        "failures": failures,
    }, failures


def _strict_failure_opportunity_count(frame: pl.DataFrame) -> int:
    return frame.filter(pl.col("recommended_next_action") == "train_selector_to_detect_strict_failure").height


def _library_rows(frame: pl.DataFrame, *, tenant_id: str, source_model_name: str) -> list[dict[str, Any]]:
    rows = [
        row
        for row in frame.iter_rows(named=True)
        if row["tenant_id"] == tenant_id and row["source_model_name"] == source_model_name
    ]
    if not rows:
        raise ValueError(f"missing schedule candidate rows for {tenant_id}/{source_model_name}")
    return rows


def _single_anchor_family_row(
    rows: list[dict[str, Any]],
    *,
    anchor_timestamp: datetime,
    candidate_family: str,
) -> dict[str, Any]:
    return _single_family_row(
        [
            row
            for row in rows
            if _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp") == anchor_timestamp
        ],
        candidate_family,
    )


def _single_family_row(rows: list[dict[str, Any]], candidate_family: str) -> dict[str, Any]:
    matches = [row for row in rows if row["candidate_family"] == candidate_family]
    if not matches:
        raise ValueError(f"missing {candidate_family} row")
    return matches[0]


def _prior_mean_regret(
    rows: list[dict[str, Any]],
    row: dict[str, Any],
    *,
    anchor_timestamp: datetime,
    min_prior_anchor_count: int,
) -> float | None:
    prior_rows = [
        candidate
        for candidate in rows
        if str(candidate["split_name"]) == "train_selection"
        and str(candidate["candidate_family"]) == str(row["candidate_family"])
        and str(candidate["candidate_model_name"]) == str(row["candidate_model_name"])
        and _datetime_value(candidate["anchor_timestamp"], field_name="anchor_timestamp") < anchor_timestamp
    ]
    if len(_anchor_set(prior_rows)) < min_prior_anchor_count:
        return None
    return _mean_regret(prior_rows)


def _prior_anchor_count(rows: list[dict[str, Any]], row: dict[str, Any], anchor_timestamp: datetime) -> int:
    prior_rows = [
        candidate
        for candidate in rows
        if str(candidate["split_name"]) == "train_selection"
        and str(candidate["candidate_family"]) == str(row["candidate_family"])
        and str(candidate["candidate_model_name"]) == str(row["candidate_model_name"])
        and _datetime_value(candidate["anchor_timestamp"], field_name="anchor_timestamp") < anchor_timestamp
    ]
    return len(_anchor_set(prior_rows))


def _anchor_set(rows: Any) -> set[datetime]:
    return {
        _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        for row in rows
    }


def _tenant_anchor_set(rows: list[dict[str, Any]]) -> set[tuple[str, datetime]]:
    return {
        (str(row["tenant_id"]), _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"))
        for row in rows
    }


def _source_model_name(row: dict[str, Any]) -> str:
    if "source_model_name" in row and row["source_model_name"]:
        return str(row["source_model_name"])
    payload = _payload(row)
    return str(payload.get("source_forecast_model_name", ""))


def _provenance_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    for row in rows:
        payload = _payload(row)
        if str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade":
            failures.append("strict-failure selector evidence requires thesis_grade rows")
            break
        if float(payload.get("observed_coverage_ratio", 0.0)) < 1.0:
            failures.append("strict-failure selector evidence requires observed coverage ratio of 1.0")
            break
        if int(payload.get("safety_violation_count", 0)):
            failures.append("strict-failure selector evidence requires zero safety violations")
            break
        if payload.get("not_full_dfl") is False:
            failures.append("strict-failure selector evidence must remain not_full_dfl")
            break
        if payload.get("not_market_execution") is False:
            failures.append("strict-failure selector evidence must remain not_market_execution")
            break
    return failures


def _switch_count(decisions: list[dict[str, Any]], selected_rows: list[dict[str, Any]]) -> int:
    return sum(
        1
        for decision, selected_row in zip(decisions, selected_rows, strict=True)
        if str(selected_row["candidate_family"]) != str(decision["strict_row"]["candidate_family"])
    )


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


def _committed_action(row: dict[str, Any]) -> str:
    committed_power = _first_or_default(row["dispatch_mw_vector"], default=0.0)
    if committed_power > 0.0:
        return "DISCHARGE"
    if committed_power < 0.0:
        return "CHARGE"
    return "HOLD"


def _first_or_default(value: object, *, default: float) -> float:
    values = _float_list(value, field_name="vector")
    return values[0] if values else default


def _family_sort_index(candidate_family: str) -> int:
    if candidate_family in REFERENCE_FAMILY_ORDER:
        return REFERENCE_FAMILY_ORDER.index(candidate_family)
    return len(REFERENCE_FAMILY_ORDER)


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload", {})
    return payload if isinstance(payload, dict) else {}


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
