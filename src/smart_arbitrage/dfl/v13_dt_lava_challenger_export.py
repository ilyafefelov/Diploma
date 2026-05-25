"""Evidence export for V13-gated DT/LAVA offline challenger packets."""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.v13_dt_lava_challenger_gate import (
    evaluate_v13_dt_lava_offline_challenger_gate,
)

V13_DT_LAVA_CHALLENGER_JSON_ARTIFACT_NAME: Final[str] = (
    "dfl_v13_dt_lava_offline_challenger_summary.json"
)
V13_DT_LAVA_CHALLENGER_MARKDOWN_ARTIFACT_NAME: Final[str] = (
    "dfl_v13_dt_lava_offline_challenger_summary.md"
)
V13_DT_LAVA_CHALLENGER_METRICS_ARTIFACT_NAME: Final[str] = (
    "dfl_v13_dt_lava_offline_challenger_metrics.json"
)
V13_DT_LAVA_CHALLENGER_VALIDATION_ARTIFACT_NAME: Final[str] = (
    "dfl_v13_dt_lava_offline_challenger_validation.json"
)


def build_v13_dt_lava_offline_challenger_packet(
    *,
    run_slug: str,
    teacher_packet: dict[str, Any],
    bridge_strict_frame: pl.DataFrame,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = 90,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
    asset_check_status: str | None = None,
) -> dict[str, Any]:
    """Build a repeatable Phase 3 offline challenger evidence packet."""

    gate = evaluate_v13_dt_lava_offline_challenger_gate(
        teacher_packet=teacher_packet,
        bridge_strict_frame=bridge_strict_frame,
        source_model_names=source_model_names,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
    )
    metrics = dict(gate.metrics)
    return {
        "run_slug": run_slug,
        "phase": "phase_3_v13_gated_dt_lava_offline_challenger_gate",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "asset_check_status": asset_check_status,
        "claim_boundary": {
            "offline_challenger_only": True,
            "requires_v13_source_readiness": True,
            "not_full_dfl": True,
            "not_deployed_decision_transformer_control": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "market_execution_gate_passed": False,
            "no_dashboard_api_default_switch": True,
            "comparison_gate": "strict_lp_oracle_regret_value_vs_v2_plus_and_controls",
            "required_controls": [
                "strict_reference",
                "schedule_value_learner_v2_plus_reference",
                "filtered_behavior_cloning_reference",
            ],
        },
        "teacher_packet_summary": _teacher_packet_summary(teacher_packet),
        "bridge_input_summary": _bridge_input_summary(bridge_strict_frame),
        "gate": {
            "passed": gate.passed,
            "decision": gate.decision,
            "description": gate.description,
            "metrics": metrics,
        },
        "promotion_gate": {
            "offline_dt_lava_challenger_gate_passed": bool(
                metrics["offline_dt_lava_challenger_gate_passed"]
            ),
            "market_execution_gate_passed": False,
            "production_promote": False,
            "permits_model_training": bool(metrics["permits_model_training"]),
            "market_execution_enabled": False,
        },
        "attached_artifacts": {
            "summary_json": V13_DT_LAVA_CHALLENGER_JSON_ARTIFACT_NAME,
            "summary_markdown": V13_DT_LAVA_CHALLENGER_MARKDOWN_ARTIFACT_NAME,
            "metrics_json": V13_DT_LAVA_CHALLENGER_METRICS_ARTIFACT_NAME,
            "validation_json": V13_DT_LAVA_CHALLENGER_VALIDATION_ARTIFACT_NAME,
        },
    }


def validate_v13_dt_lava_offline_challenger_packet(
    packet: dict[str, Any],
) -> dict[str, Any]:
    """Validate Phase 3 offline challenger evidence without promoting execution."""

    failures: list[str] = []
    gate_results: dict[str, dict[str, Any]] = {}
    if _contains_market_execution_enabled_true(packet):
        failures.append("nested_market_execution_enabled_true")

    _add_validation_gate(
        gate_results=gate_results,
        failures=failures,
        gate_name="packet_contract",
        gate_failures=_packet_contract_failures(packet),
    )
    _add_validation_gate(
        gate_results=gate_results,
        failures=failures,
        gate_name="strict_control_comparison",
        gate_failures=_strict_control_comparison_failures(packet),
    )
    _add_validation_gate(
        gate_results=gate_results,
        failures=failures,
        gate_name="deterministic_safety_projection",
        gate_failures=_deterministic_safety_projection_failures(packet),
    )
    _add_validation_gate(
        gate_results=gate_results,
        failures=failures,
        gate_name="non_promotion_execution_boundary",
        gate_failures=_non_promotion_execution_boundary_failures(packet),
    )
    _add_validation_gate(
        gate_results=gate_results,
        failures=failures,
        gate_name="no_market_execution",
        gate_failures=(
            ["nested_market_execution_enabled_true"]
            if _contains_market_execution_enabled_true(packet)
            else []
        ),
    )
    return {
        "claim_scope": (
            "v13_dt_lava_offline_challenger_packet_validation_not_market_execution"
        ),
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "passed": not failures,
        "failures": failures,
        "gate_results": gate_results,
        "market_execution_enabled": False,
    }


def write_v13_dt_lava_offline_challenger_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
) -> Path:
    """Write local JSON, Markdown, and metrics artifacts for a challenger gate."""

    export_dir = output_root / str(packet["run_slug"])
    export_dir.mkdir(parents=True, exist_ok=True)
    (export_dir / V13_DT_LAVA_CHALLENGER_JSON_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / V13_DT_LAVA_CHALLENGER_METRICS_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet["gate"]["metrics"]), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / V13_DT_LAVA_CHALLENGER_MARKDOWN_ARTIFACT_NAME).write_text(
        _packet_markdown(packet),
        encoding="utf-8",
    )
    (export_dir / V13_DT_LAVA_CHALLENGER_VALIDATION_ARTIFACT_NAME).write_text(
        json.dumps(
            validate_v13_dt_lava_offline_challenger_packet(packet),
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return export_dir


def _teacher_packet_summary(packet: dict[str, Any]) -> dict[str, Any]:
    dataset = _mapping(packet.get("dataset_summary"))
    gate_passport = _mapping(packet.get("gate_passport"))
    v13_gate = _mapping(gate_passport.get("v13_training_permission_gate"))
    market_gate = _mapping(gate_passport.get("market_execution_gate"))
    return {
        "phase": str(packet.get("phase") or ""),
        "v13_training_permission_gate_passed": bool(
            dataset.get("v13_training_permission_gate_passed", False)
        ),
        "v13_training_permission_gate_status": str(v13_gate.get("status") or ""),
        "dt_lava_training_dataset_ready": bool(
            dataset.get("dt_lava_training_dataset_ready", False)
        ),
        "permitted_model_training_rows": _int_value(
            dataset.get("permitted_model_training_rows")
        ),
        "final_holdout_scoring_rows": _int_value(
            dataset.get("final_holdout_scoring_rows")
        ),
        "safe_switch_coverage_gate_passed": bool(
            dataset.get("safe_switch_coverage_gate_passed", False)
        ),
        "market_execution_gate_status": str(market_gate.get("status") or ""),
        "market_execution_enabled": False,
    }


def _bridge_input_summary(frame: pl.DataFrame) -> dict[str, Any]:
    return {
        "row_count": frame.height,
        "tenant_count": frame["tenant_id"].n_unique()
        if "tenant_id" in frame.columns and frame.height
        else 0,
        "source_model_count": frame["source_model_name"].n_unique()
        if "source_model_name" in frame.columns and frame.height
        else 0,
        "selection_roles": sorted(
            str(value)
            for value in (
                frame["selection_role"].unique().to_list()
                if "selection_role" in frame.columns
                else []
            )
        ),
        "market_execution_enabled": False,
    }


def _packet_markdown(packet: dict[str, Any]) -> str:
    gate = packet["gate"]
    metrics = gate["metrics"]
    teacher = packet["teacher_packet_summary"]
    control_summary = _mapping(metrics.get("control_comparison_summary"))
    lines = [
        "# V13 DT/LAVA Offline Challenger Packet",
        "",
        f"Run slug: `{packet['run_slug']}`",
        f"Asset check status: `{packet.get('asset_check_status')}`",
        "",
        "## Claim Boundary",
        "",
        "This packet is an offline challenger gate only. It is not a deployed "
        "Decision Transformer controller, not full differentiable DFL, not market "
        "execution, and `market_execution_enabled=false`.",
        "",
        "## Gate",
        "",
        f"- Gate decision: `{gate['decision']}`",
        f"- Gate passed: `{gate['passed']}`",
        f"- Description: {gate['description']}",
        f"- V13 training permission: `{teacher['v13_training_permission_gate_status']}`",
        f"- Teacher dataset ready: `{metrics['teacher_dataset_ready']}`",
        f"- Bridge gate passed: `{metrics['bridge_gate_passed']}`",
        f"- Safe-switch coverage gate passed: `{metrics['safe_switch_coverage_gate_passed']}`",
        f"- Deterministic safety projection passed: `{metrics['deterministic_safety_projection_passed']}`",
        f"- Control comparison anchors: `{control_summary.get('validation_tenant_anchor_count')}`",
        f"- Control source models: `{control_summary.get('source_model_count')}`",
        f"- Best challenger role: `{metrics.get('best_challenger_role')}`",
        f"- Market execution gate passed: `{packet['promotion_gate']['market_execution_gate_passed']}`",
        "",
        "## Artifacts",
        "",
        f"- `{V13_DT_LAVA_CHALLENGER_JSON_ARTIFACT_NAME}`",
        f"- `{V13_DT_LAVA_CHALLENGER_MARKDOWN_ARTIFACT_NAME}`",
        f"- `{V13_DT_LAVA_CHALLENGER_METRICS_ARTIFACT_NAME}`",
        "",
    ]
    return "\n".join(lines)


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _packet_contract_failures(packet: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    claim_boundary = _mapping(packet.get("claim_boundary"))
    if packet.get("phase") != "phase_3_v13_gated_dt_lava_offline_challenger_gate":
        failures.append("invalid_phase")
    for flag_name in (
        "offline_challenger_only",
        "requires_v13_source_readiness",
        "not_full_dfl",
        "not_deployed_decision_transformer_control",
        "not_market_execution",
        "no_dashboard_api_default_switch",
    ):
        if claim_boundary.get(flag_name) is not True:
            failures.append(f"claim_boundary_flag_not_true:{flag_name}")
    for flag_name in ("market_execution_enabled", "market_execution_gate_passed"):
        if claim_boundary.get(flag_name) is not False:
            failures.append(f"claim_boundary_flag_not_false:{flag_name}")
    if not _mapping(packet.get("teacher_packet_summary")):
        failures.append("teacher_packet_summary_missing")
    if not _mapping(packet.get("bridge_input_summary")):
        failures.append("bridge_input_summary_missing")
    if not _mapping(packet.get("gate")):
        failures.append("gate_missing")
    return failures


def _strict_control_comparison_failures(packet: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    metrics = _gate_metrics(packet)
    control_summary = _mapping(metrics.get("control_comparison_summary"))
    if metrics.get("required_control_roles_present") is not True:
        failures.append("required_control_roles_missing")
    if metrics.get("behavior_cloning_control_present") is not True:
        failures.append("behavior_cloning_control_missing")
    if control_summary.get("required_control_roles_present") is not True:
        failures.append("control_summary_required_roles_missing")
    if control_summary.get("behavior_cloning_control_present") is not True:
        failures.append("control_summary_behavior_cloning_missing")
    if _int_value(control_summary.get("validation_tenant_anchor_count")) <= 0:
        failures.append("control_summary_validation_anchors_missing")
    required_controls = {
        "strict_reference",
        "schedule_value_learner_v2_plus_reference",
        "filtered_behavior_cloning_reference",
    }
    observed_controls = {
        str(role)
        for role in control_summary.get("required_control_roles", [])
        if str(role).strip()
    }
    missing_controls = sorted(required_controls - observed_controls)
    failures.extend(f"control_summary_missing_role:{role}" for role in missing_controls)
    return failures


def _deterministic_safety_projection_failures(packet: dict[str, Any]) -> list[str]:
    metrics = _gate_metrics(packet)
    failures: list[str] = []
    if metrics.get("deterministic_safety_projection_passed") is not True:
        failures.append("deterministic_safety_projection_not_passed")
    if _int_value(metrics.get("deterministic_safety_projection_failed_row_count")) != 0:
        failures.append("deterministic_safety_projection_failed_rows")
    if _int_value(metrics.get("deterministic_safety_projection_missing_row_count")) != 0:
        failures.append("deterministic_safety_projection_missing_rows")
    expected_rows = _int_value(
        metrics.get("deterministic_safety_projection_expected_row_count")
    )
    observed_rows = _int_value(metrics.get("deterministic_safety_projection_row_count"))
    if expected_rows <= 0:
        failures.append("deterministic_safety_projection_expected_rows_missing")
    if observed_rows != expected_rows:
        failures.append("deterministic_safety_projection_row_count_mismatch")
    return failures


def _non_promotion_execution_boundary_failures(packet: dict[str, Any]) -> list[str]:
    metrics = _gate_metrics(packet)
    promotion_gate = _mapping(packet.get("promotion_gate"))
    failures: list[str] = []
    if promotion_gate.get("market_execution_gate_passed") is not False:
        failures.append("market_execution_gate_passed_true")
    if promotion_gate.get("production_promote") is not False:
        failures.append("production_promote_true")
    if promotion_gate.get("market_execution_enabled") is not False:
        failures.append("promotion_gate_market_execution_enabled_true")
    if metrics.get("market_execution_gate_passed") is not False:
        failures.append("metrics_market_execution_gate_passed_true")
    if metrics.get("production_promote") is not False:
        failures.append("metrics_production_promote_true")
    if metrics.get("not_full_dfl") is not True:
        failures.append("metrics_not_full_dfl_not_true")
    if metrics.get("not_deployed_decision_transformer_control") is not True:
        failures.append("metrics_not_deployed_dt_control_not_true")
    if metrics.get("not_market_execution") is not True:
        failures.append("metrics_not_market_execution_not_true")
    if metrics.get("no_dashboard_api_default_switch") is not True:
        failures.append("metrics_no_dashboard_api_default_switch_not_true")
    return failures


def _gate_metrics(packet: dict[str, Any]) -> dict[str, Any]:
    return _mapping(_mapping(packet.get("gate")).get("metrics"))


def _add_validation_gate(
    *,
    gate_results: dict[str, dict[str, Any]],
    failures: list[str],
    gate_name: str,
    gate_failures: list[str],
) -> None:
    gate_results[gate_name] = {
        "passed": not gate_failures,
        "failures": gate_failures,
        "market_execution_enabled": False,
    }
    failures.extend(gate_failures)


def _contains_market_execution_enabled_true(value: object) -> bool:
    if isinstance(value, dict):
        return any(
            (key == "market_execution_enabled" and bool(item))
            or _contains_market_execution_enabled_true(item)
            for key, item in value.items()
        )
    if isinstance(value, list | tuple):
        return any(_contains_market_execution_enabled_true(item) for item in value)
    return False


def _int_value(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    return value


__all__ = [
    "V13_DT_LAVA_CHALLENGER_JSON_ARTIFACT_NAME",
    "V13_DT_LAVA_CHALLENGER_MARKDOWN_ARTIFACT_NAME",
    "V13_DT_LAVA_CHALLENGER_METRICS_ARTIFACT_NAME",
    "V13_DT_LAVA_CHALLENGER_VALIDATION_ARTIFACT_NAME",
    "build_v13_dt_lava_offline_challenger_packet",
    "validate_v13_dt_lava_offline_challenger_packet",
    "write_v13_dt_lava_offline_challenger_packet",
]
