"""Governed market-coupling ablation evidence for Schedule/Value Learner V2+."""

from __future__ import annotations

from statistics import mean, median
from typing import Final

import polars as pl

from smart_arbitrage.dfl.schedule_value_learner_v2_plus import (
    evaluate_dfl_schedule_value_learner_v2_plus_gate,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus_robustness import (
    validate_dfl_schedule_value_learner_v2_plus_robustness_evidence,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome
from smart_arbitrage.forecasting.market_coupling_features import (
    REQUIRED_MARKET_COUPLING_FEATURE_ROUTE_COLUMNS,
    validate_market_coupling_feature_route_evidence,
)

DFL_MARKET_COUPLING_V2_PLUS_ABLATION_CLAIM_SCOPE: Final[str] = (
    "dfl_market_coupling_v2_plus_ablation_not_market_execution"
)
DFL_MARKET_COUPLING_V2_PLUS_ABLATION_ACADEMIC_SCOPE: Final[str] = (
    "Governed market-coupling ablation comparing Ukrainian-only Schedule/Value "
    "Learner V2+ against Ukrainian plus approved point-in-time neighbor-market "
    "exogenous features. It is Offline Strategy Promotion evidence only and not "
    "market execution."
)
REQUIRED_MARKET_COUPLING_ABLATION_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source_model_name",
        "ablation_status",
        "approved_external_feature_columns_csv",
        "blocked_external_feature_columns_csv",
        "did_train_market_coupled_variant",
        "baseline_mean_regret_uah",
        "baseline_median_regret_uah",
        "market_coupled_mean_regret_uah",
        "market_coupled_median_regret_uah",
        "mean_regret_improvement_ratio_vs_ukrainian_v2_plus",
        "baseline_rolling_pass_windows",
        "market_coupled_rolling_pass_windows",
        "rolling_robustness_preserved",
        "ablation_passed",
        "ablation_blocker",
        "data_quality_tier",
        "observed_coverage_ratio",
        "safety_violation_count",
        "production_promote",
        "market_execution_enabled",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_dfl_market_coupling_v2_plus_ablation_frame(
    ukrainian_only_strict_frame: pl.DataFrame,
    ukrainian_only_robustness_frame: pl.DataFrame,
    official_forecast_exogenous_feature_route_frame: pl.DataFrame,
    *,
    market_coupled_strict_frame: pl.DataFrame | None = None,
    market_coupled_robustness_frame: pl.DataFrame | None = None,
    source_model_names: tuple[str, ...],
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = 90,
    min_window_count: int = 4,
) -> pl.DataFrame:
    """Build the ablation summary while keeping unapproved EU features blocked."""

    if not source_model_names:
        raise ValueError("source_model_names must contain at least one source.")
    _validate_route_or_raise(official_forecast_exogenous_feature_route_frame)
    baseline_gate = evaluate_dfl_schedule_value_learner_v2_plus_gate(
        ukrainian_only_strict_frame,
        source_model_names=source_model_names,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
    )
    if not baseline_gate.metrics:
        raise ValueError(f"Ukrainian-only V2+ strict evidence invalid: {baseline_gate.description}")
    _validate_robustness_or_raise(
        ukrainian_only_robustness_frame,
        source_model_names=source_model_names,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
        min_window_count=min_window_count,
        label="Ukrainian-only V2+",
    )

    approved_columns = _approved_feature_columns(
        official_forecast_exogenous_feature_route_frame
    )
    blocked_columns = _blocked_feature_columns(
        official_forecast_exogenous_feature_route_frame
    )
    route_blockers = _route_blockers(official_forecast_exogenous_feature_route_frame)
    rows: list[dict[str, object]] = []
    for source_model_name in source_model_names:
        baseline_summary = _strict_summary(
            ukrainian_only_strict_frame,
            source_model_name=source_model_name,
            selected_role="schedule_value_learner_v2_plus",
        )
        baseline_rolling_pass_windows = _rolling_pass_windows(
            ukrainian_only_robustness_frame,
            source_model_name=source_model_name,
        )
        if not approved_columns:
            rows.append(
                _ablation_row(
                    source_model_name=source_model_name,
                    ablation_status="blocked_by_governance",
                    approved_columns=approved_columns,
                    blocked_columns=blocked_columns,
                    did_train_market_coupled_variant=False,
                    baseline_summary=baseline_summary,
                    market_coupled_summary=None,
                    baseline_rolling_pass_windows=baseline_rolling_pass_windows,
                    market_coupled_rolling_pass_windows=0,
                    ablation_blocker="no_approved_external_features",
                    route_blockers=route_blockers,
                )
            )
            continue
        if market_coupled_strict_frame is None or market_coupled_robustness_frame is None:
            rows.append(
                _ablation_row(
                    source_model_name=source_model_name,
                    ablation_status="approved_route_pending_materialization",
                    approved_columns=approved_columns,
                    blocked_columns=blocked_columns,
                    did_train_market_coupled_variant=False,
                    baseline_summary=baseline_summary,
                    market_coupled_summary=None,
                    baseline_rolling_pass_windows=baseline_rolling_pass_windows,
                    market_coupled_rolling_pass_windows=0,
                    ablation_blocker="missing_market_coupled_v2_plus_evidence",
                    route_blockers=route_blockers,
                )
            )
            continue
        coupled_gate = evaluate_dfl_schedule_value_learner_v2_plus_gate(
            market_coupled_strict_frame,
            source_model_names=source_model_names,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
        )
        if not coupled_gate.metrics:
            raise ValueError(f"market-coupled V2+ strict evidence invalid: {coupled_gate.description}")
        _validate_robustness_or_raise(
            market_coupled_robustness_frame,
            source_model_names=source_model_names,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_window_count=min_window_count,
            label="market-coupled V2+",
        )
        market_coupled_summary = _strict_summary(
            market_coupled_strict_frame,
            source_model_name=source_model_name,
            selected_role="schedule_value_learner_v2_plus",
        )
        market_coupled_rolling_pass_windows = _rolling_pass_windows(
            market_coupled_robustness_frame,
            source_model_name=source_model_name,
        )
        rows.append(
            _ablation_row(
                source_model_name=source_model_name,
                ablation_status="comparison_complete",
                approved_columns=approved_columns,
                blocked_columns=blocked_columns,
                did_train_market_coupled_variant=True,
                baseline_summary=baseline_summary,
                market_coupled_summary=market_coupled_summary,
                baseline_rolling_pass_windows=baseline_rolling_pass_windows,
                market_coupled_rolling_pass_windows=market_coupled_rolling_pass_windows,
                ablation_blocker="",
                route_blockers=route_blockers,
            )
        )
    return pl.DataFrame(rows).sort("source_model_name")


def validate_dfl_market_coupling_v2_plus_ablation_evidence(
    frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
) -> EvidenceCheckOutcome:
    """Validate market-coupling ablation evidence and claim boundaries."""

    failures = _missing_column_failures(frame, REQUIRED_MARKET_COUPLING_ABLATION_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": frame.height})
    rows = list(frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(
            False,
            "market-coupling V2+ ablation has no rows",
            {"row_count": 0},
        )
    expected_sources = source_model_names or tuple(
        sorted({str(row["source_model_name"]) for row in rows})
    )
    observed_sources = {str(row["source_model_name"]) for row in rows}
    missing_sources = sorted(set(expected_sources).difference(observed_sources))
    if missing_sources:
        failures.append(f"missing source_model_name rows: {missing_sources}")

    bad_claim_rows = [
        row
        for row in rows
        if str(row["claim_scope"]) != DFL_MARKET_COUPLING_V2_PLUS_ABLATION_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
        or bool(row["market_execution_enabled"])
        or bool(row["production_promote"])
    ]
    if bad_claim_rows:
        failures.append("market-coupling ablation rows must keep research-only claim flags")
    if any(str(row["data_quality_tier"]) != "thesis_grade" for row in rows):
        failures.append("market-coupling ablation requires thesis_grade rows")
    if any(float(row["observed_coverage_ratio"]) < 1.0 for row in rows):
        failures.append("market-coupling ablation requires observed coverage")
    if any(int(row["safety_violation_count"]) != 0 for row in rows):
        failures.append("market-coupling ablation requires zero safety violations")
    for row in rows:
        status = str(row["ablation_status"])
        if status not in {
            "blocked_by_governance",
            "approved_route_pending_materialization",
            "comparison_complete",
        }:
            failures.append(f"invalid ablation_status={status}")
            break
        if status == "blocked_by_governance" and bool(row["did_train_market_coupled_variant"]):
            failures.append("blocked governance rows must not train the market-coupled variant")
        if bool(row["ablation_passed"]):
            if status != "comparison_complete":
                failures.append("passing ablation rows must be comparison_complete")
            if not str(row["approved_external_feature_columns_csv"]).strip():
                failures.append("passing ablation rows require approved external features")
            if float(row["mean_regret_improvement_ratio_vs_ukrainian_v2_plus"]) <= 0.0:
                failures.append("passing ablation rows must improve mean regret over Ukrainian-only V2+")
            if float(row["market_coupled_median_regret_uah"]) > float(row["baseline_median_regret_uah"]):
                failures.append("passing ablation rows must not degrade median regret")
            if not bool(row["rolling_robustness_preserved"]):
                failures.append("passing ablation rows must preserve rolling robustness")

    metadata = {
        "row_count": len(rows),
        "source_model_count": len(observed_sources),
        "passed_rows": len([row for row in rows if bool(row["ablation_passed"])]),
        "blocked_rows": len(
            [row for row in rows if str(row["ablation_status"]) != "comparison_complete"]
        ),
        "approved_feature_columns": sorted(
            {
                value
                for row in rows
                for value in str(row["approved_external_feature_columns_csv"]).split(",")
                if value
            }
        ),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Market-coupling V2+ ablation evidence preserves governance and claim boundaries."
            if not failures
            else "; ".join(dict.fromkeys(failures))
        ),
        metadata=metadata,
    )


def _validate_route_or_raise(route_frame: pl.DataFrame) -> None:
    failures = _missing_column_failures(route_frame, REQUIRED_MARKET_COUPLING_FEATURE_ROUTE_COLUMNS)
    if failures:
        raise ValueError("; ".join(failures))
    outcome = validate_market_coupling_feature_route_evidence(route_frame)
    if not outcome.passed:
        raise ValueError(f"official feature route invalid: {outcome.description}")


def _validate_robustness_or_raise(
    frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...],
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_window_count: int,
    label: str,
) -> None:
    outcome = validate_dfl_schedule_value_learner_v2_plus_robustness_evidence(
        frame,
        source_model_names=source_model_names,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
        min_window_count=min_window_count,
    )
    if not outcome.passed:
        raise ValueError(f"{label} rolling robustness evidence invalid: {outcome.description}")


def _ablation_row(
    *,
    source_model_name: str,
    ablation_status: str,
    approved_columns: tuple[str, ...],
    blocked_columns: tuple[str, ...],
    did_train_market_coupled_variant: bool,
    baseline_summary: dict[str, float],
    market_coupled_summary: dict[str, float] | None,
    baseline_rolling_pass_windows: int,
    market_coupled_rolling_pass_windows: int,
    ablation_blocker: str,
    route_blockers: str,
) -> dict[str, object]:
    coupled_mean = (
        market_coupled_summary["mean_regret_uah"] if market_coupled_summary else 0.0
    )
    coupled_median = (
        market_coupled_summary["median_regret_uah"] if market_coupled_summary else 0.0
    )
    improvement = (
        _improvement_ratio(baseline_summary["mean_regret_uah"], coupled_mean)
        if market_coupled_summary
        else 0.0
    )
    rolling_preserved = (
        market_coupled_rolling_pass_windows >= baseline_rolling_pass_windows
        and market_coupled_rolling_pass_windows >= 3
    )
    blockers = _ablation_blockers(
        ablation_status=ablation_status,
        ablation_blocker=ablation_blocker,
        improvement=improvement,
        baseline_median=baseline_summary["median_regret_uah"],
        coupled_median=coupled_median,
        rolling_preserved=rolling_preserved,
    )
    return {
        "source_model_name": source_model_name,
        "ablation_status": ablation_status,
        "approved_external_feature_columns_csv": ",".join(approved_columns),
        "blocked_external_feature_columns_csv": ",".join(blocked_columns),
        "external_training_blockers_csv": route_blockers,
        "did_train_market_coupled_variant": did_train_market_coupled_variant,
        "baseline_mean_regret_uah": baseline_summary["mean_regret_uah"],
        "baseline_median_regret_uah": baseline_summary["median_regret_uah"],
        "market_coupled_mean_regret_uah": coupled_mean,
        "market_coupled_median_regret_uah": coupled_median,
        "mean_regret_improvement_ratio_vs_ukrainian_v2_plus": improvement,
        "baseline_rolling_pass_windows": baseline_rolling_pass_windows,
        "market_coupled_rolling_pass_windows": market_coupled_rolling_pass_windows,
        "rolling_robustness_preserved": rolling_preserved,
        "ablation_passed": not blockers and did_train_market_coupled_variant,
        "ablation_blocker": ",".join(blockers),
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": 0,
        "production_promote": False,
        "market_execution_enabled": False,
        "claim_scope": DFL_MARKET_COUPLING_V2_PLUS_ABLATION_CLAIM_SCOPE,
        "academic_scope": DFL_MARKET_COUPLING_V2_PLUS_ABLATION_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _ablation_blockers(
    *,
    ablation_status: str,
    ablation_blocker: str,
    improvement: float,
    baseline_median: float,
    coupled_median: float,
    rolling_preserved: bool,
) -> list[str]:
    if ablation_blocker:
        return [ablation_blocker]
    blockers: list[str] = []
    if ablation_status != "comparison_complete":
        blockers.append(ablation_status)
    if improvement <= 0.0:
        blockers.append("mean_not_improved")
    if coupled_median > baseline_median:
        blockers.append("median_degraded")
    if not rolling_preserved:
        blockers.append("rolling_robustness_not_preserved")
    return blockers


def _strict_summary(
    strict_frame: pl.DataFrame,
    *,
    source_model_name: str,
    selected_role: str,
) -> dict[str, float]:
    rows = [
        row
        for row in strict_frame.iter_rows(named=True)
        if str(row["source_model_name"]) == source_model_name
        and str(row["selection_role"]) == selected_role
    ]
    if not rows:
        raise ValueError(f"missing {selected_role} rows for {source_model_name}")
    regrets = [float(row["regret_uah"]) for row in rows]
    return {
        "mean_regret_uah": round(mean(regrets), 2),
        "median_regret_uah": round(median(regrets), 2),
    }


def _rolling_pass_windows(robustness_frame: pl.DataFrame, *, source_model_name: str) -> int:
    return len(
        [
            row
            for row in robustness_frame.iter_rows(named=True)
            if str(row["source_model_name"]) == source_model_name
            and bool(row["v2_plus_window_passed"])
        ]
    )


def _approved_feature_columns(route_frame: pl.DataFrame) -> tuple[str, ...]:
    return tuple(
        sorted(
            str(row["approved_feature_column"])
            for row in route_frame.iter_rows(named=True)
            if bool(row["approved_for_official_training"])
            or bool(row.get("approved_for_experimental_ablation", False))
        )
    )


def _blocked_feature_columns(route_frame: pl.DataFrame) -> tuple[str, ...]:
    approved = set(_approved_feature_columns(route_frame))
    return tuple(
        sorted(
            str(row["approved_feature_column"])
            for row in route_frame.iter_rows(named=True)
            if str(row["approved_feature_column"]).strip()
            and str(row["approved_feature_column"]) not in approved
        )
    )


def _route_blockers(route_frame: pl.DataFrame) -> str:
    blockers = sorted(
        {
            blocker
            for row in route_frame.iter_rows(named=True)
            for blocker in str(row["training_blockers_csv"]).split(",")
            if blocker.strip()
        }
    )
    return ",".join(blockers)


def _improvement_ratio(baseline: float, candidate: float) -> float:
    if abs(baseline) < 1e-9:
        return 0.0
    return round((baseline - candidate) / abs(baseline), 10)


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"missing required columns: {missing}"] if missing else []


__all__ = [
    "DFL_MARKET_COUPLING_V2_PLUS_ABLATION_CLAIM_SCOPE",
    "build_dfl_market_coupling_v2_plus_ablation_frame",
    "validate_dfl_market_coupling_v2_plus_ablation_evidence",
]
