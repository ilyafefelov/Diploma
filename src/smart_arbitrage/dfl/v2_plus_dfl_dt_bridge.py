"""V2+-anchored comparison gate for residual DFL and offline DT challengers."""

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

DFL_V2_PLUS_DFL_DT_BRIDGE_CLAIM_SCOPE: Final[str] = (
    "dfl_v2_plus_dfl_dt_bridge_not_full_dfl"
)
DFL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark"
)
DFL_V2_PLUS_DFL_DT_BRIDGE_ACADEMIC_SCOPE: Final[str] = (
    "V2+-anchored strict LP/oracle comparison for residual DFL and tiny "
    "offline Decision Transformer challengers. V2+ remains the comparator and "
    "fallback. This is Offline Strategy Promotion evidence only: not full DFL, "
    "not deployed Decision Transformer control, and not market execution."
)

V2_PLUS_HEADLINE_BASELINE_METRICS: Final[dict[str, object]] = {
    "calibrated_v2_plus_mean_regret_uah": 174.77,
    "improvement_vs_strict_similar_day_ratio": 0.4373,
    "rolling_robustness_pass_windows": 4,
    "rolling_robustness_total_windows": 4,
    "market_execution_enabled": False,
}

ROLE_MAP: Final[dict[str, tuple[str, str]]] = {
    "strict_reference": ("strict_reference", CONTROL_MODEL_NAME),
    "schedule_value_learner_v2_plus": (
        "schedule_value_learner_v2_plus_reference",
        "dfl_schedule_value_learner_v2_plus",
    ),
    "residual_reference": ("residual_dfl_reference", "dfl_residual_schedule_value_v1"),
    "offline_dt_reference": ("offline_dt_reference", "dfl_offline_dt_candidate_v1"),
    "filtered_behavior_cloning_reference": (
        "filtered_behavior_cloning_reference",
        "filtered_behavior_cloning_v1",
    ),
    "fallback_strategy": ("residual_dt_fallback_reference", "dfl_residual_dt_fallback_v1"),
}
REQUIRED_BRIDGE_ROLES: Final[tuple[str, ...]] = (
    "strict_reference",
    "schedule_value_learner_v2_plus_reference",
    "residual_dfl_reference",
    "offline_dt_reference",
    "filtered_behavior_cloning_reference",
    "residual_dt_fallback_reference",
)
CHALLENGER_ROLES: Final[tuple[str, ...]] = (
    "residual_dfl_reference",
    "offline_dt_reference",
    "residual_dt_fallback_reference",
)
REQUIRED_INPUT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "forecast_model_name",
        "strategy_kind",
        "anchor_timestamp",
        "generated_at",
        "regret_uah",
        "decision_value_uah",
        "selection_role",
        "data_quality_tier",
        "observed_coverage_ratio",
        "safety_violation_count",
        "not_full_dfl",
        "not_market_execution",
        "evaluation_payload",
    }
)
REQUIRED_BRIDGE_COLUMNS: Final[frozenset[str]] = frozenset(
    REQUIRED_INPUT_COLUMNS
    | frozenset(
        {
            "selected_strategy_source",
            "claim_scope",
        }
    )
)


def build_dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame(
    residual_dt_fallback_strict_frame: pl.DataFrame,
    schedule_value_v2_plus_strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    generated_at: datetime | None = None,
    strategy_kind: str = DFL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND,
    claim_scope: str = DFL_V2_PLUS_DFL_DT_BRIDGE_CLAIM_SCOPE,
    academic_scope: str = DFL_V2_PLUS_DFL_DT_BRIDGE_ACADEMIC_SCOPE,
    baseline_metrics: dict[str, object] | None = None,
) -> pl.DataFrame:
    """Normalize residual/DT and V2+ strict rows into one V2+-anchored comparison."""

    _validate_input_frame(
        residual_dt_fallback_strict_frame,
        frame_name="dfl_residual_dt_fallback_strict_lp_benchmark_frame",
    )
    _validate_input_frame(
        schedule_value_v2_plus_strict_frame,
        frame_name="dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    )
    source_names = source_model_names or tuple(
        sorted(
            {
                *_source_names(residual_dt_fallback_strict_frame),
                *_source_names(schedule_value_v2_plus_strict_frame),
            }
        )
    )
    resolved_generated_at = generated_at or _latest_generated_at(
        residual_dt_fallback_strict_frame,
        schedule_value_v2_plus_strict_frame,
    )
    residual_rows = list(residual_dt_fallback_strict_frame.iter_rows(named=True))
    v2_plus_rows = list(schedule_value_v2_plus_strict_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for source_model_name in source_names:
        scoped_residual_rows = [
            row for row in residual_rows if _source_model_name(row) == source_model_name
        ]
        scoped_v2_rows = [
            row for row in v2_plus_rows if _source_model_name(row) == source_model_name
        ]
        _validate_source_alignment(
            scoped_residual_rows,
            scoped_v2_rows,
            source_model_name=source_model_name,
        )
        for row in scoped_v2_rows:
            original_role = str(row["selection_role"])
            if original_role in {"strict_reference", "schedule_value_learner_v2_plus"}:
                role, strategy_source = ROLE_MAP[original_role]
                output_rows.append(
                    _bridge_row(
                        row,
                        selection_role=role,
                        selected_strategy_source=strategy_source,
                        original_selection_role=original_role,
                        generated_at=resolved_generated_at,
                        strategy_kind=strategy_kind,
                        claim_scope=claim_scope,
                        academic_scope=academic_scope,
                        baseline_metrics=baseline_metrics,
                    )
                )
        for row in scoped_residual_rows:
            original_role = str(row["selection_role"])
            if original_role in {
                "residual_reference",
                "offline_dt_reference",
                "filtered_behavior_cloning_reference",
                "fallback_strategy",
            }:
                role, strategy_source = ROLE_MAP[original_role]
                output_rows.append(
                    _bridge_row(
                        row,
                        selection_role=role,
                        selected_strategy_source=strategy_source,
                        original_selection_role=original_role,
                        generated_at=resolved_generated_at,
                        strategy_kind=strategy_kind,
                        claim_scope=claim_scope,
                        academic_scope=academic_scope,
                        baseline_metrics=baseline_metrics,
                    )
                )
    if not output_rows:
        return pl.DataFrame()
    return pl.DataFrame(output_rows).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp", "selection_role"]
    )


def validate_dfl_v2_plus_dfl_dt_bridge_evidence(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> EvidenceCheckOutcome:
    """Validate bridge evidence structure without forcing a challenger win."""

    failures, summaries = _structure_failures(
        strict_frame,
        source_model_names=source_model_names,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
    )
    return EvidenceCheckOutcome(
        not failures,
        "V2+-anchored DFL/DT bridge evidence is structurally valid."
        if not failures
        else "; ".join(failures),
        {
            "row_count": strict_frame.height,
            "source_model_count": len(summaries),
            "source_model_names": [summary["source_model_name"] for summary in summaries],
            "model_summaries": summaries,
            "market_execution_enabled": False,
            "v2_plus_headline_baseline": dict(V2_PLUS_HEADLINE_BASELINE_METRICS),
        },
    )


def evaluate_dfl_v2_plus_dfl_dt_bridge_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.0,
    min_mean_regret_improvement_ratio_vs_strict: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
) -> PromotionGateResult:
    """Require residual/DT challengers to beat V2+ before any bridge pass."""

    structural_failures, summaries = _structure_failures(
        strict_frame,
        source_model_names=source_model_names,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
    )
    if not summaries:
        return PromotionGateResult(
            False,
            "blocked",
            "; ".join(structural_failures)
            if structural_failures
            else "V2+-anchored bridge has no summaries",
            {"market_execution_enabled": False},
        )
    challenger_summaries: list[dict[str, Any]] = []
    passing_challengers: list[dict[str, Any]] = []
    gate_failures = list(structural_failures)
    for summary in summaries:
        for challenger in summary["challenger_summaries"]:
            challenger_summaries.append(challenger)
            if _challenger_passes(
                challenger,
                min_mean_regret_improvement_ratio_vs_v2_plus=(
                    min_mean_regret_improvement_ratio_vs_v2_plus
                ),
                min_mean_regret_improvement_ratio_vs_strict=(
                    min_mean_regret_improvement_ratio_vs_strict
                ),
            ):
                passing_challengers.append(challenger)
    if not passing_challengers:
        gate_failures.append(
            "no residual/DT challenger beats V2+ mean regret without median degradation "
            f"and still clears {CONTROL_MODEL_NAME}"
        )
    best_passing = (
        max(
            passing_challengers,
            key=lambda challenger: float(
                challenger["mean_regret_improvement_ratio_vs_v2_plus"]
            ),
        )
        if passing_challengers
        else None
    )
    best_observed = max(
        challenger_summaries,
        key=lambda challenger: float(
            challenger["mean_regret_improvement_ratio_vs_v2_plus"]
        ),
    )
    metrics = {
        "best_source_model_name": (
            best_passing["source_model_name"] if best_passing is not None else None
        ),
        "best_challenger_role": (
            best_passing["selection_role"] if best_passing is not None else None
        ),
        "best_mean_regret_uah": (
            best_passing["mean_regret_uah"] if best_passing is not None else None
        ),
        "best_mean_regret_improvement_ratio_vs_v2_plus": (
            best_passing["mean_regret_improvement_ratio_vs_v2_plus"]
            if best_passing is not None
            else 0.0
        ),
        "best_mean_regret_improvement_ratio_vs_strict": (
            best_passing["mean_regret_improvement_ratio_vs_strict"]
            if best_passing is not None
            else 0.0
        ),
        "best_observed_challenger_role": best_observed["selection_role"],
        "best_observed_source_model_name": best_observed["source_model_name"],
        "best_observed_mean_regret_improvement_ratio_vs_v2_plus": best_observed[
            "mean_regret_improvement_ratio_vs_v2_plus"
        ],
        "validation_tenant_anchor_count": max(
            int(summary["validation_tenant_anchor_count"]) for summary in summaries
        ),
        "passing_challenger_count": len(passing_challengers),
        "market_execution_enabled": False,
        "production_promote": False,
        "offline_strategy_challenger_passed": bool(passing_challengers)
        and not structural_failures,
        "model_summaries": summaries,
    }
    if passing_challengers and not structural_failures:
        return PromotionGateResult(
            True,
            "offline_strategy_challenger",
            "residual/DT bridge challenger beats V2+ under strict LP/oracle evidence",
            metrics,
        )
    return PromotionGateResult(False, "blocked", "; ".join(gate_failures), metrics)


def _validate_input_frame(frame: pl.DataFrame, *, frame_name: str) -> None:
    missing = sorted(REQUIRED_INPUT_COLUMNS.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {missing}")
    for row in frame.iter_rows(named=True):
        _validate_claim_row(row, frame_name=frame_name)


def _validate_claim_row(row: dict[str, Any], *, frame_name: str) -> None:
    if str(row["data_quality_tier"]) != "thesis_grade":
        raise ValueError(f"{frame_name} requires thesis_grade rows")
    if float(row["observed_coverage_ratio"]) < 1.0:
        raise ValueError(f"{frame_name} requires observed coverage ratio of 1.0")
    if int(row["safety_violation_count"]):
        raise ValueError(f"{frame_name} requires zero safety violations")
    if row.get("not_full_dfl") is not True:
        raise ValueError(f"{frame_name} requires not_full_dfl=true")
    if row.get("not_market_execution") is not True:
        raise ValueError(f"{frame_name} requires not_market_execution=true")
    payload = _payload(row)
    if payload.get("market_execution_enabled") is True:
        raise ValueError(f"{frame_name} requires market_execution_enabled=false")


def _validate_source_alignment(
    residual_rows: list[dict[str, Any]],
    v2_rows: list[dict[str, Any]],
    *,
    source_model_name: str,
) -> None:
    if not residual_rows:
        raise ValueError(f"{source_model_name} has no residual/DT fallback rows")
    if not v2_rows:
        raise ValueError(f"{source_model_name} has no V2+ strict rows")
    v2_plus_keys = _role_anchor_keys(v2_rows, "schedule_value_learner_v2_plus")
    if not v2_plus_keys:
        raise ValueError(f"{source_model_name} is missing V2+ comparator rows")
    required_input_roles = {
        "strict_reference": v2_rows,
        "residual_reference": residual_rows,
        "offline_dt_reference": residual_rows,
        "filtered_behavior_cloning_reference": residual_rows,
        "fallback_strategy": residual_rows,
    }
    for role, rows in required_input_roles.items():
        role_keys = _role_anchor_keys(rows, role)
        if role_keys != v2_plus_keys:
            raise ValueError(
                f"{source_model_name} {role} coverage must match V2+ anchors; "
                f"observed {len(role_keys)} versus {len(v2_plus_keys)}"
            )


def _bridge_row(
    row: dict[str, Any],
    *,
    selection_role: str,
    selected_strategy_source: str,
    original_selection_role: str,
    generated_at: datetime,
    strategy_kind: str,
    claim_scope: str,
    academic_scope: str,
    baseline_metrics: dict[str, object] | None,
) -> dict[str, Any]:
    anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    payload = _payload(row)
    source_strategy_kind = str(row["strategy_kind"])
    resolved_baseline_metrics = baseline_metrics or V2_PLUS_HEADLINE_BASELINE_METRICS
    payload.update(
        {
            "source_strategy_kind": source_strategy_kind,
            "source_selection_role": original_selection_role,
            "bridge_selection_role": selection_role,
            "selected_strategy_source": selected_strategy_source,
            "claim_scope": claim_scope,
            "academic_scope": academic_scope,
            "v2_plus_headline_baseline": dict(resolved_baseline_metrics),
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    copied = dict(row)
    copied.update(
        {
            "evaluation_id": (
                f"{strategy_kind}:"
                f"{row['tenant_id']}:{row['source_model_name']}:{selection_role}:"
                f"{anchor_timestamp:%Y%m%dT%H%M}"
            ),
            "strategy_kind": strategy_kind,
            "anchor_timestamp": anchor_timestamp,
            "generated_at": generated_at,
            "selection_role": selection_role,
            "selected_strategy_source": selected_strategy_source,
            "claim_scope": claim_scope,
            "not_full_dfl": True,
            "not_market_execution": True,
            "evaluation_payload": payload,
        }
    )
    return copied


def _structure_failures(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
) -> tuple[list[str], list[dict[str, Any]]]:
    missing_columns = sorted(REQUIRED_BRIDGE_COLUMNS.difference(strict_frame.columns))
    if missing_columns:
        return [
            f"V2+-anchored bridge frame is missing required columns: {missing_columns}"
        ], []
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return ["V2+-anchored bridge frame has no rows"], []
    failures: list[str] = []
    for row in rows:
        try:
            _validate_claim_row(row, frame_name="dfl_v2_plus_dfl_dt_bridge")
        except ValueError as exc:
            failures.append(str(exc))
    source_names = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
    summaries = [
        _source_summary(
            rows,
            source_model_name=source_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
        )
        for source_model_name in source_names
    ]
    for summary in summaries:
        failures.extend(str(failure) for failure in summary["structural_failures"])
    return sorted(set(failures)), summaries


def _source_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
) -> dict[str, Any]:
    source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
    structural_failures: list[str] = []
    role_rows = {
        role: [row for row in source_rows if str(row["selection_role"]) == role]
        for role in REQUIRED_BRIDGE_ROLES
    }
    for role, scoped_rows in role_rows.items():
        if not scoped_rows:
            structural_failures.append(f"{source_model_name} is missing {role} rows")
    reference_keys = _anchor_keys(role_rows["schedule_value_learner_v2_plus_reference"])
    for role, scoped_rows in role_rows.items():
        role_keys = _anchor_keys(scoped_rows)
        if reference_keys and role_keys != reference_keys:
            structural_failures.append(
                f"{source_model_name} {role} coverage does not match V2+ comparator"
            )
    tenant_count = len({str(row["tenant_id"]) for row in source_rows})
    validation_count = len(reference_keys)
    if tenant_count < min_tenant_count:
        structural_failures.append(
            f"{source_model_name} tenant count must be at least {min_tenant_count}; "
            f"observed {tenant_count}"
        )
    if validation_count < min_validation_tenant_anchor_count:
        structural_failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {validation_count}"
        )
    strict_stats = _role_stats(role_rows["strict_reference"])
    v2_plus_stats = _role_stats(role_rows["schedule_value_learner_v2_plus_reference"])
    bc_stats = _role_stats(role_rows["filtered_behavior_cloning_reference"])
    challenger_summaries = [
        _challenger_summary(
            role_rows[role],
            source_model_name=source_model_name,
            role=role,
            strict_stats=strict_stats,
            v2_plus_stats=v2_plus_stats,
            behavior_cloning_stats=bc_stats,
        )
        for role in CHALLENGER_ROLES
    ]
    return {
        "source_model_name": source_model_name,
        "tenant_count": tenant_count,
        "validation_tenant_anchor_count": validation_count,
        "strict_mean_regret_uah": strict_stats["mean_regret_uah"],
        "strict_median_regret_uah": strict_stats["median_regret_uah"],
        "v2_plus_mean_regret_uah": v2_plus_stats["mean_regret_uah"],
        "v2_plus_median_regret_uah": v2_plus_stats["median_regret_uah"],
        "behavior_cloning_mean_regret_uah": bc_stats["mean_regret_uah"],
        "behavior_cloning_median_regret_uah": bc_stats["median_regret_uah"],
        "challenger_summaries": challenger_summaries,
        "structural_failures": structural_failures,
    }


def _challenger_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    role: str,
    strict_stats: dict[str, float],
    v2_plus_stats: dict[str, float],
    behavior_cloning_stats: dict[str, float],
) -> dict[str, Any]:
    stats = _role_stats(rows)
    improvement_vs_v2_plus = _improvement_ratio(
        v2_plus_stats["mean_regret_uah"],
        stats["mean_regret_uah"],
    )
    improvement_vs_strict = _improvement_ratio(
        strict_stats["mean_regret_uah"],
        stats["mean_regret_uah"],
    )
    median_not_worse_vs_v2_plus = (
        stats["median_regret_uah"] <= v2_plus_stats["median_regret_uah"]
    )
    median_not_worse_vs_strict = (
        stats["median_regret_uah"] <= strict_stats["median_regret_uah"]
    )
    beats_behavior_cloning = (
        role != "offline_dt_reference"
        or stats["mean_regret_uah"] <= behavior_cloning_stats["mean_regret_uah"]
    )
    return {
        "source_model_name": source_model_name,
        "selection_role": role,
        "validation_tenant_anchor_count": len(_anchor_keys(rows)),
        "mean_regret_uah": stats["mean_regret_uah"],
        "median_regret_uah": stats["median_regret_uah"],
        "mean_regret_improvement_ratio_vs_v2_plus": improvement_vs_v2_plus,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "median_not_worse_vs_v2_plus": median_not_worse_vs_v2_plus,
        "median_not_worse_vs_strict": median_not_worse_vs_strict,
        "beats_behavior_cloning": beats_behavior_cloning,
    }


def _challenger_passes(
    challenger: dict[str, Any],
    *,
    min_mean_regret_improvement_ratio_vs_v2_plus: float,
    min_mean_regret_improvement_ratio_vs_strict: float,
) -> bool:
    return (
        float(challenger["mean_regret_improvement_ratio_vs_v2_plus"])
        > min_mean_regret_improvement_ratio_vs_v2_plus
        and float(challenger["mean_regret_improvement_ratio_vs_strict"])
        >= min_mean_regret_improvement_ratio_vs_strict
        and bool(challenger["median_not_worse_vs_v2_plus"])
        and bool(challenger["median_not_worse_vs_strict"])
        and bool(challenger["beats_behavior_cloning"])
    )


def _role_stats(rows: list[dict[str, Any]]) -> dict[str, float]:
    regrets = [float(row["regret_uah"]) for row in rows]
    return {
        "mean_regret_uah": mean(regrets) if regrets else 0.0,
        "median_regret_uah": median(regrets) if regrets else 0.0,
    }


def _role_anchor_keys(rows: list[dict[str, Any]], role: str) -> set[tuple[str, datetime]]:
    return _anchor_keys([row for row in rows if str(row["selection_role"]) == role])


def _anchor_keys(rows: list[dict[str, Any]]) -> set[tuple[str, datetime]]:
    return {
        (
            str(row["tenant_id"]),
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        )
        for row in rows
    }


def _source_names(frame: pl.DataFrame) -> set[str]:
    if "source_model_name" not in frame.columns:
        return set()
    return {str(value) for value in frame["source_model_name"].unique().to_list()}


def _source_model_name(row: dict[str, Any]) -> str:
    return str(row.get("source_model_name") or _payload(row).get("source_forecast_model_name"))


def _latest_generated_at(*frames: pl.DataFrame) -> datetime:
    generated_values: list[datetime] = []
    for frame in frames:
        if "generated_at" not in frame.columns:
            continue
        generated_values.extend(
            _datetime_value(value, field_name="generated_at")
            for value in frame["generated_at"].to_list()
        )
    return max(generated_values) if generated_values else datetime.now(tz=UTC)


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload")
    return dict(payload) if isinstance(payload, dict) else {}


def _datetime_value(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"{field_name} must be datetime or ISO string; got {type(value).__name__}")


def _improvement_ratio(control_mean: float, candidate_mean: float) -> float:
    if abs(control_mean) <= 1e-9:
        return 0.0
    return (control_mean - candidate_mean) / abs(control_mean)


__all__ = [
    "DFL_V2_PLUS_DFL_DT_BRIDGE_CLAIM_SCOPE",
    "DFL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND",
    "V2_PLUS_HEADLINE_BASELINE_METRICS",
    "build_dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame",
    "evaluate_dfl_v2_plus_dfl_dt_bridge_gate",
    "validate_dfl_v2_plus_dfl_dt_bridge_evidence",
]
