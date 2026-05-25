"""V13-gated offline DT/LAVA challenger promotion checks.

This module does not train DT/LAVA and does not enable market execution. It
only combines a V13-ready teacher dataset packet with the existing V2+-anchored
strict LP/oracle bridge gate.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final, cast

import polars as pl

from smart_arbitrage.dfl.promotion_gate import PromotionGateResult
from smart_arbitrage.dfl.v2_plus_dfl_dt_bridge import (
    evaluate_dfl_v2_plus_dfl_dt_bridge_gate,
    validate_dfl_v2_plus_dfl_dt_bridge_evidence,
)

V13_DT_LAVA_OFFLINE_CHALLENGER_CLAIM_SCOPE: Final[str] = (
    "v13_dt_lava_offline_challenger_not_deployed_not_market_execution"
)

_V13_TEACHER_DATASET_PHASE: Final[str] = (
    "phase_2_v13_gated_dt_lava_teacher_dataset"
)
_REQUIRED_CONTROL_ROLES: Final[frozenset[str]] = frozenset(
    {
        "strict_reference",
        "schedule_value_learner_v2_plus_reference",
        "filtered_behavior_cloning_reference",
    }
)


def evaluate_v13_dt_lava_offline_challenger_gate(
    *,
    teacher_packet: Mapping[str, Any],
    bridge_strict_frame: pl.DataFrame,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = 90,
) -> PromotionGateResult:
    """Evaluate DT/LAVA as an offline challenger behind V13 source readiness."""

    failures: list[str] = []
    teacher_status = _teacher_packet_status(teacher_packet)
    failures.extend(cast(list[str], teacher_status["failures"]))

    role_status = _bridge_role_status(bridge_strict_frame)
    failures.extend(cast(list[str], role_status["failures"]))
    failures.extend(_bridge_market_execution_failures(bridge_strict_frame))
    safety_status = _deterministic_safety_projection_status(bridge_strict_frame)
    failures.extend(cast(list[str], safety_status["failures"]))

    bridge_evidence = validate_dfl_v2_plus_dfl_dt_bridge_evidence(
        bridge_strict_frame,
        source_model_names=source_model_names,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
    )
    bridge_gate = evaluate_dfl_v2_plus_dfl_dt_bridge_gate(
        bridge_strict_frame,
        source_model_names=source_model_names,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
    )
    if not bridge_evidence.passed:
        failures.append(f"bridge evidence invalid: {bridge_evidence.description}")
    if not bridge_gate.passed:
        failures.append(f"bridge gate blocked: {bridge_gate.description}")

    metrics = {
        "claim_scope": V13_DT_LAVA_OFFLINE_CHALLENGER_CLAIM_SCOPE,
        "teacher_dataset_ready": teacher_status["teacher_dataset_ready"],
        "v13_training_permission_gate_passed": teacher_status[
            "v13_training_permission_gate_passed"
        ],
        "teacher_permitted_model_training_rows": teacher_status[
            "teacher_permitted_model_training_rows"
        ],
        "teacher_final_holdout_scoring_rows": teacher_status[
            "teacher_final_holdout_scoring_rows"
        ],
        "safe_switch_coverage_gate_passed": teacher_status[
            "safe_switch_coverage_gate_passed"
        ],
        "safe_switch_covered_tenant_source_count": teacher_status[
            "safe_switch_covered_tenant_source_count"
        ],
        "safe_switch_required_tenant_source_count": teacher_status[
            "safe_switch_required_tenant_source_count"
        ],
        "teacher_phase": teacher_status["teacher_phase"],
        "bridge_evidence_passed": bridge_evidence.passed,
        "bridge_gate_passed": bridge_gate.passed,
        "deterministic_safety_projection_passed": safety_status[
            "deterministic_safety_projection_passed"
        ],
        "deterministic_safety_projection_expected_row_count": safety_status[
            "deterministic_safety_projection_expected_row_count"
        ],
        "deterministic_safety_projection_row_count": safety_status[
            "deterministic_safety_projection_row_count"
        ],
        "deterministic_safety_projection_failed_row_count": safety_status[
            "deterministic_safety_projection_failed_row_count"
        ],
        "deterministic_safety_projection_missing_row_count": safety_status[
            "deterministic_safety_projection_missing_row_count"
        ],
        "deterministic_safety_projection_coverage_ratio": safety_status[
            "deterministic_safety_projection_coverage_ratio"
        ],
        "bridge_source_model_names": bridge_evidence.metadata.get(
            "source_model_names",
            [],
        ),
        "best_challenger_role": bridge_gate.metrics.get("best_challenger_role"),
        "best_source_model_name": bridge_gate.metrics.get("best_source_model_name"),
        "best_mean_regret_improvement_ratio_vs_v2_plus": bridge_gate.metrics.get(
            "best_mean_regret_improvement_ratio_vs_v2_plus",
            0.0,
        ),
        "best_mean_regret_improvement_ratio_vs_strict": bridge_gate.metrics.get(
            "best_mean_regret_improvement_ratio_vs_strict",
            0.0,
        ),
        "validation_tenant_anchor_count": bridge_gate.metrics.get(
            "validation_tenant_anchor_count",
            0,
        ),
        "required_control_roles_present": role_status[
            "required_control_roles_present"
        ],
        "behavior_cloning_control_present": role_status[
            "behavior_cloning_control_present"
        ],
        "bridge_selection_roles": role_status["bridge_selection_roles"],
        "control_comparison_summary": _control_comparison_summary(
            bridge_gate_metrics=bridge_gate.metrics,
            role_status=role_status,
        ),
        "permits_model_training": teacher_status["teacher_dataset_ready"],
        "offline_dt_lava_challenger_gate_passed": False,
        "production_promote": False,
        "not_full_dfl": True,
        "not_deployed_decision_transformer_control": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "market_execution_gate_passed": False,
        "no_dashboard_api_default_switch": True,
    }
    if failures:
        return PromotionGateResult(
            False,
            "blocked",
            "; ".join(sorted(set(failures))),
            metrics,
        )
    metrics["offline_dt_lava_challenger_gate_passed"] = True
    return PromotionGateResult(
        True,
        "offline_dt_lava_challenger",
        (
            "V13-ready teacher dataset and V2+-anchored strict LP/oracle bridge "
            "support an offline DT/LAVA challenger claim only"
        ),
        metrics,
    )


def _teacher_packet_status(packet: Mapping[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    phase = str(packet.get("phase") or "")
    claim_boundary = _mapping(packet.get("claim_boundary"))
    dataset_summary = _mapping(packet.get("dataset_summary"))
    gate_passport = _mapping(packet.get("gate_passport"))
    v13_gate = _mapping(gate_passport.get("v13_training_permission_gate"))
    market_gate = _mapping(gate_passport.get("market_execution_gate"))

    if phase != _V13_TEACHER_DATASET_PHASE:
        failures.append(
            "V13 teacher packet phase must be "
            f"{_V13_TEACHER_DATASET_PHASE}; observed {phase or '<missing>'}"
        )
    if claim_boundary.get("not_market_execution") is not True:
        failures.append("V13 teacher packet must keep not_market_execution=true")
    if claim_boundary.get("not_deployed_decision_transformer_control") is not True:
        failures.append(
            "V13 teacher packet must keep "
            "not_deployed_decision_transformer_control=true"
        )
    if _is_true(claim_boundary.get("market_execution_enabled")):
        failures.append("V13 teacher packet claim boundary enables market execution")
    if _is_true(dataset_summary.get("market_execution_enabled")):
        failures.append("V13 teacher dataset summary enables market execution")
    if _is_true(market_gate.get("passed")):
        failures.append("V13 teacher packet market execution gate must remain out of scope")
    if _is_true(market_gate.get("market_execution_enabled")):
        failures.append("V13 teacher packet market execution gate enables execution")

    dataset_ready = _is_true(dataset_summary.get("dt_lava_training_dataset_ready"))
    v13_permission_passed = _is_true(
        dataset_summary.get("v13_training_permission_gate_passed")
    ) and _is_true(v13_gate.get("passed"))
    permitted_rows = _int_value(dataset_summary.get("permitted_model_training_rows"))
    final_holdout_rows = _int_value(dataset_summary.get("final_holdout_scoring_rows"))
    safe_switch_coverage_passed = _is_true(
        dataset_summary.get("safe_switch_coverage_gate_passed")
    )
    safe_switch_covered_count = _int_value(
        dataset_summary.get("safe_switch_covered_tenant_source_count")
    )
    safe_switch_required_count = _int_value(
        dataset_summary.get("safe_switch_required_tenant_source_count")
    )
    if not dataset_ready:
        failures.append("V13 teacher dataset is not ready for DT/LAVA training")
    if not v13_permission_passed:
        failures.append("V13 training permission gate has not passed")
    if permitted_rows <= 0:
        failures.append("V13 teacher packet has no permitted model-training rows")
    if final_holdout_rows <= 0:
        failures.append("V13 teacher packet has no final-holdout scoring rows")
    if not safe_switch_coverage_passed:
        failures.append("V13 teacher packet safe-switch coverage gate has not passed")

    return {
        "teacher_dataset_ready": (
            dataset_ready
            and v13_permission_passed
            and final_holdout_rows > 0
            and safe_switch_coverage_passed
        ),
        "v13_training_permission_gate_passed": v13_permission_passed,
        "teacher_permitted_model_training_rows": permitted_rows,
        "teacher_final_holdout_scoring_rows": final_holdout_rows,
        "safe_switch_coverage_gate_passed": safe_switch_coverage_passed,
        "safe_switch_covered_tenant_source_count": safe_switch_covered_count,
        "safe_switch_required_tenant_source_count": safe_switch_required_count,
        "teacher_phase": phase,
        "failures": failures,
    }


def _bridge_role_status(frame: pl.DataFrame) -> dict[str, Any]:
    roles = _unique_strings(frame, "selection_role")
    missing_roles = sorted(_REQUIRED_CONTROL_ROLES.difference(roles))
    failures = [
        f"V13 DT/LAVA challenger gate requires control role {role}"
        for role in missing_roles
    ]
    return {
        "bridge_selection_roles": sorted(roles),
        "required_control_roles_present": not missing_roles,
        "behavior_cloning_control_present": (
            "filtered_behavior_cloning_reference" in roles
        ),
        "failures": failures,
    }


def _bridge_market_execution_failures(frame: pl.DataFrame) -> list[str]:
    failures: list[str] = []
    if _true_count(frame, "market_execution_enabled"):
        failures.append("bridge frame must keep market_execution_enabled=false")
    if _true_count(frame, "market_execution_gate_passed"):
        failures.append("bridge frame must keep market_execution_gate_passed=false")
    if "evaluation_payload" in frame.columns:
        for payload in frame["evaluation_payload"].to_list():
            if isinstance(payload, Mapping) and _is_true(
                payload.get("market_execution_enabled")
            ):
                failures.append(
                    "bridge evaluation payload must keep market_execution_enabled=false"
                )
                break
    return failures


def _control_comparison_summary(
    *,
    bridge_gate_metrics: Mapping[str, Any],
    role_status: Mapping[str, Any],
) -> dict[str, Any]:
    model_summaries = [
        _source_control_summary(summary)
        for summary in _sequence(bridge_gate_metrics.get("model_summaries"))
    ]
    validation_counts = [
        int(summary["validation_tenant_anchor_count"])
        for summary in model_summaries
    ]
    return {
        "claim_scope": "strict_lp_oracle_control_comparison_not_market_execution",
        "required_control_roles": sorted(_REQUIRED_CONTROL_ROLES),
        "required_control_roles_present": bool(
            role_status.get("required_control_roles_present", False)
        ),
        "behavior_cloning_control_present": bool(
            role_status.get("behavior_cloning_control_present", False)
        ),
        "source_model_count": len(model_summaries),
        "validation_tenant_anchor_count": max(validation_counts, default=0),
        "best_observed_challenger_role": bridge_gate_metrics.get(
            "best_observed_challenger_role"
        ),
        "best_observed_source_model_name": bridge_gate_metrics.get(
            "best_observed_source_model_name"
        ),
        "best_observed_mean_regret_improvement_ratio_vs_v2_plus": (
            bridge_gate_metrics.get(
                "best_observed_mean_regret_improvement_ratio_vs_v2_plus",
                0.0,
            )
        ),
        "source_model_summaries": model_summaries,
        "market_execution_enabled": False,
    }


def _source_control_summary(summary: Any) -> dict[str, Any]:
    value = _mapping(summary)
    return {
        "source_model_name": str(value.get("source_model_name", "")),
        "tenant_count": _int_value(value.get("tenant_count")),
        "validation_tenant_anchor_count": _int_value(
            value.get("validation_tenant_anchor_count")
        ),
        "strict_mean_regret_uah": _float_value(value.get("strict_mean_regret_uah")),
        "strict_median_regret_uah": _float_value(value.get("strict_median_regret_uah")),
        "v2_plus_mean_regret_uah": _float_value(value.get("v2_plus_mean_regret_uah")),
        "v2_plus_median_regret_uah": _float_value(value.get("v2_plus_median_regret_uah")),
        "behavior_cloning_mean_regret_uah": _float_value(
            value.get("behavior_cloning_mean_regret_uah")
        ),
        "behavior_cloning_median_regret_uah": _float_value(
            value.get("behavior_cloning_median_regret_uah")
        ),
        "challenger_summaries": [
            _challenger_control_summary(challenger)
            for challenger in _sequence(value.get("challenger_summaries"))
        ],
        "market_execution_enabled": False,
    }


def _challenger_control_summary(summary: Any) -> dict[str, Any]:
    value = _mapping(summary)
    return {
        "source_model_name": str(value.get("source_model_name", "")),
        "selection_role": str(value.get("selection_role", "")),
        "validation_tenant_anchor_count": _int_value(
            value.get("validation_tenant_anchor_count")
        ),
        "mean_regret_uah": _float_value(value.get("mean_regret_uah")),
        "median_regret_uah": _float_value(value.get("median_regret_uah")),
        "mean_regret_improvement_ratio_vs_v2_plus": _float_value(
            value.get("mean_regret_improvement_ratio_vs_v2_plus")
        ),
        "mean_regret_improvement_ratio_vs_strict": _float_value(
            value.get("mean_regret_improvement_ratio_vs_strict")
        ),
        "median_not_worse_vs_v2_plus": _is_true(
            value.get("median_not_worse_vs_v2_plus")
        ),
        "median_not_worse_vs_strict": _is_true(
            value.get("median_not_worse_vs_strict")
        ),
        "beats_behavior_cloning": _is_true(value.get("beats_behavior_cloning")),
        "market_execution_enabled": False,
    }


def _deterministic_safety_projection_status(frame: pl.DataFrame) -> dict[str, Any]:
    failures: list[str] = []
    row_count = frame.height
    if row_count < 1:
        return {
            "deterministic_safety_projection_passed": False,
            "deterministic_safety_projection_expected_row_count": 0,
            "deterministic_safety_projection_row_count": 0,
            "deterministic_safety_projection_failed_row_count": 0,
            "failures": ["bridge frame has no deterministic safety projection rows"],
            "deterministic_safety_projection_missing_row_count": 0,
            "deterministic_safety_projection_coverage_ratio": 0.0,
        }

    projected_rows = 0
    failed_rows = 0
    missing_rows = 0
    has_projection_column = "deterministic_safety_projection_passed" in frame.columns
    for row in frame.iter_rows(named=True):
        projected = (
            _is_true(row.get("deterministic_safety_projection_passed"))
            if has_projection_column
            else _is_true(
                _mapping(row.get("evaluation_payload")).get(
                    "deterministic_safety_projection_passed"
                )
            )
        )
        if projected:
            projected_rows += 1
        else:
            failed_rows += 1
            if not has_projection_column and not _is_true(
                _mapping(row.get("evaluation_payload")).get(
                    "deterministic_safety_projection_passed"
                )
            ):
                missing_rows += 1
    if failed_rows:
        failures.append(
            "bridge frame deterministic safety projection must pass for every row"
        )
    return {
        "deterministic_safety_projection_passed": failed_rows == 0,
        "deterministic_safety_projection_expected_row_count": row_count,
        "deterministic_safety_projection_row_count": projected_rows,
        "deterministic_safety_projection_failed_row_count": failed_rows,
        "deterministic_safety_projection_missing_row_count": missing_rows,
        "deterministic_safety_projection_coverage_ratio": projected_rows / row_count,
        "failures": failures,
    }


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _unique_strings(frame: pl.DataFrame, column_name: str) -> set[str]:
    if column_name not in frame.columns:
        return set()
    return {str(value) for value in frame[column_name].unique().to_list()}


def _true_count(frame: pl.DataFrame, column_name: str) -> int:
    if column_name not in frame.columns:
        return 0
    return sum(1 for value in frame[column_name].to_list() if _is_true(value))


def _is_true(value: object) -> bool:
    return value is True


def _int_value(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if not isinstance(value, str):
        return 0
    try:
        return int(value)
    except ValueError:
        return 0


def _float_value(value: object) -> float:
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, int | float):
        return float(value)
    if not isinstance(value, str):
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


__all__ = [
    "V13_DT_LAVA_OFFLINE_CHALLENGER_CLAIM_SCOPE",
    "evaluate_v13_dt_lava_offline_challenger_gate",
]
