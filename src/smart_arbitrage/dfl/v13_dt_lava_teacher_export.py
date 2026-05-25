"""Evidence export for V13-gated DT/LAVA teacher-row datasets.

The exported packet is a Phase 2 handoff artifact: it documents candidate-index
or schedule-family supervision rows for a later offline DT/LAVA challenger. It
does not train a model, promote a policy, or enable market execution.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import csv
import json
from pathlib import Path
from typing import Any, Final

import polars as pl

V13_DT_LAVA_TEACHER_JSON_ARTIFACT_NAME: Final[str] = (
    "dfl_v13_dt_lava_teacher_summary.json"
)
V13_DT_LAVA_TEACHER_MARKDOWN_ARTIFACT_NAME: Final[str] = (
    "dfl_v13_dt_lava_teacher_summary.md"
)
V13_DT_LAVA_TEACHER_ROWS_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_v13_dt_lava_teacher_rows.csv"
)
V13_DT_LAVA_TEACHER_VALIDATION_JSON_ARTIFACT_NAME: Final[str] = (
    "dfl_v13_dt_lava_teacher_validation.json"
)

_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "split_name",
        "dt_candidate_id_target",
        "dt_candidate_index_target",
        "dt_schedule_family_target",
        "return_to_go_regret_target_uah",
        "regret_delta_vs_v2_plus_uah",
        "schedule_value_uah",
        "dfl_input_contract",
        "dfl_target_contract",
        "dt_input_contract",
        "dt_action_target_contract",
        "v2_plus_role",
        "v13_training_permission_gate_passed",
        "permitted_model_training_row",
        "permits_model_training",
        "training_blocker",
        "promotion_gate_passed",
        "market_execution_gate_passed",
        "raw_hourly_action_imitation",
        "not_full_dfl",
        "not_deployed_dt_control",
        "not_market_execution",
        "market_execution_enabled",
    }
)
_REQUIRED_FEATURE_GROUP_COLUMNS: Final[dict[str, frozenset[str]]] = {
    "forecast_context": frozenset({"forecast_price_uah_mwh_vector"}),
    "battery_soc_context": frozenset({"soc_fraction_vector"}),
    "candidate_schedule_context": frozenset({"dispatch_mw_vector"}),
    "value_return_targets": frozenset(
        {
            "return_to_go_regret_target_uah",
            "regret_delta_vs_v2_plus_uah",
            "schedule_value_uah",
        }
    ),
}
_DFL_INPUT_GROUPS: Final[tuple[str, ...]] = (
    "forecast_context",
    "battery_soc_context",
    "tenant_context",
    "candidate_schedule_context",
)
_DFL_TARGET_GROUPS: Final[tuple[str, ...]] = ("value_return_targets",)
_DT_INPUT_GROUPS: Final[tuple[str, ...]] = (
    "identity_context",
    "forecast_context",
    "battery_soc_context",
    "tenant_context",
    "candidate_schedule_context",
    "value_return_targets",
    "gate_context",
)


def build_dfl_v13_dt_lava_teacher_packet(
    *,
    run_slug: str,
    teacher_contract_frame: pl.DataFrame,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
    asset_check_status: str | None = None,
) -> dict[str, Any]:
    """Build a V13-gated candidate-index teacher dataset packet."""

    _validate_teacher_contract_frame(teacher_contract_frame)
    dataset_summary = _dataset_summary(teacher_contract_frame)
    feature_contract = _feature_contract(teacher_contract_frame)
    teacher_contract_summary = _teacher_contract_summary(
        dataset_summary=dataset_summary,
        feature_contract=feature_contract,
    )
    return {
        "run_slug": run_slug,
        "phase": "phase_2_v13_gated_dt_lava_teacher_dataset",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "asset_check_status": asset_check_status,
        "claim_boundary": {
            "offline_dt_lava_dataset_only": True,
            "not_full_dfl": True,
            "not_deployed_decision_transformer_control": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "no_raw_hourly_action_imitation": True,
            "no_dashboard_api_default_switch": True,
            "target_label_space": "candidate_index_or_schedule_family",
            "comparator": "Schedule/Value Learner V2+",
            "fallback": "Schedule/Value Learner V2+ or strict_similar_day",
        },
        "feature_contract": feature_contract,
        "teacher_contract_summary": teacher_contract_summary,
        "dataset_summary": dataset_summary,
        "gate_passport": _gate_passport(dataset_summary),
        "attached_artifacts": {
            "summary_json": V13_DT_LAVA_TEACHER_JSON_ARTIFACT_NAME,
            "summary_markdown": V13_DT_LAVA_TEACHER_MARKDOWN_ARTIFACT_NAME,
            "teacher_rows_csv": V13_DT_LAVA_TEACHER_ROWS_CSV_ARTIFACT_NAME,
            "validation_json": V13_DT_LAVA_TEACHER_VALIDATION_JSON_ARTIFACT_NAME,
        },
    }


def validate_dfl_v13_dt_lava_teacher_packet(packet: dict[str, Any]) -> dict[str, Any]:
    """Validate a V13 DT/LAVA teacher packet without promoting training."""

    failures: list[str] = []
    gate_results: dict[str, dict[str, Any]] = {}
    if _contains_market_execution_enabled_true(packet):
        failures.append("nested_market_execution_enabled_true")

    _add_validation_gate(
        gate_results=gate_results,
        failures=failures,
        gate_name="candidate_schedule_teacher_contract",
        gate_failures=_candidate_schedule_teacher_contract_failures(packet),
    )
    _add_validation_gate(
        gate_results=gate_results,
        failures=failures,
        gate_name="training_permission_consistency",
        gate_failures=_training_permission_consistency_failures(packet),
    )
    _add_validation_gate(
        gate_results=gate_results,
        failures=failures,
        gate_name="promotion_execution_blocked",
        gate_failures=_promotion_execution_blocked_failures(packet),
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
        "claim_scope": "v13_dt_lava_teacher_packet_validation_not_market_execution",
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "passed": not failures,
        "failures": failures,
        "gate_results": gate_results,
        "market_execution_enabled": False,
    }


def write_dfl_v13_dt_lava_teacher_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
    teacher_contract_frame: pl.DataFrame,
) -> Path:
    """Write local JSON, Markdown, and CSV teacher-dataset artifacts."""

    _validate_teacher_contract_frame(teacher_contract_frame)
    export_dir = output_root / str(packet["run_slug"])
    export_dir.mkdir(parents=True, exist_ok=True)
    _write_rows_csv(
        export_dir / V13_DT_LAVA_TEACHER_ROWS_CSV_ARTIFACT_NAME,
        _frame_rows(teacher_contract_frame),
    )
    (export_dir / V13_DT_LAVA_TEACHER_JSON_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / V13_DT_LAVA_TEACHER_MARKDOWN_ARTIFACT_NAME).write_text(
        _packet_markdown(packet),
        encoding="utf-8",
    )
    (export_dir / V13_DT_LAVA_TEACHER_VALIDATION_JSON_ARTIFACT_NAME).write_text(
        json.dumps(
            validate_dfl_v13_dt_lava_teacher_packet(packet),
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return export_dir


def _validate_teacher_contract_frame(frame: pl.DataFrame) -> None:
    missing = sorted(_REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"V13 DT/LAVA teacher contract missing columns: {missing}")
    for group_name, required_columns in _REQUIRED_FEATURE_GROUP_COLUMNS.items():
        group_missing = sorted(required_columns - set(frame.columns))
        if group_missing:
            raise ValueError(
                "V13 DT/LAVA teacher contract missing "
                f"{group_name} columns: {group_missing}"
            )
    _refuse_true(frame, "market_execution_enabled")
    _refuse_true(frame, "market_execution_gate_passed")
    _refuse_true(frame, "promotion_gate_passed")
    _refuse_true(frame, "raw_hourly_action_imitation")
    _require_true(frame, "not_full_dfl")
    _require_true(frame, "not_deployed_dt_control")
    _require_true(frame, "not_market_execution")


def _dataset_summary(frame: pl.DataFrame) -> dict[str, Any]:
    train_rows = frame.filter(pl.col("split_name") == "train_selection")
    permitted_rows = frame.filter(pl.col("permitted_model_training_row"))
    final_holdout_rows = frame.filter(pl.col("training_blocker") == "final_holdout_scoring_only")
    blocker_counts = _training_blocker_counts(frame)
    safe_switch_summary = _safe_switch_coverage_summary(frame, train_rows)
    v13_permission_passed = bool(
        train_rows.height > 0
        and permitted_rows.height == train_rows.height
        and set(blocker_counts).issubset({"none", "final_holdout_scoring_only"})
    )
    return {
        "rows": frame.height,
        "tenant_count": frame["tenant_id"].n_unique(),
        "source_model_count": frame["source_model_name"].n_unique(),
        "train_selection_rows": train_rows.height,
        "permitted_model_training_rows": permitted_rows.height,
        "blocked_model_training_rows": frame.height - permitted_rows.height,
        "final_holdout_scoring_rows": final_holdout_rows.height,
        **safe_switch_summary,
        "training_blocker_counts": blocker_counts,
        "v13_training_permission_gate_passed": v13_permission_passed,
        "dt_lava_training_dataset_ready": v13_permission_passed,
        "promotion_gate_passed": False,
        "market_execution_gate_passed": False,
        "market_execution_enabled": False,
    }


def _feature_contract(frame: pl.DataFrame) -> dict[str, Any]:
    return {
        "dfl_input_contract": _single_contract_value(frame, "dfl_input_contract"),
        "dfl_target_contract": _single_contract_value(frame, "dfl_target_contract"),
        "dt_input_contract": _single_contract_value(frame, "dt_input_contract"),
        "dt_action_target_contract": _single_contract_value(
            frame, "dt_action_target_contract"
        ),
        "v2_plus_role": _single_contract_value(frame, "v2_plus_role"),
        "architecture_recommendation": {
            "dfl_input": (
                "calibrated_nbeatsx_tft_forecasts_plus_tenant_soc_context_plus_"
                "feasible_candidate_schedules"
            ),
            "dfl_target": "best_candidate_schedule_value_regret_delta_vs_v2_plus",
            "dt_input": (
                "v13_passing_teacher_sequences_with_forecast_battery_tenant_"
                "candidate_value_return_to_go_regret"
            ),
            "dt_action_target": "candidate_id_or_schedule_family",
            "v2_plus_role": "teacher_comparator_fallback",
        },
        "feature_column_groups": {
            "identity_context": _present(
                frame,
                [
                    "tenant_id",
                    "source_model_name",
                    "anchor_timestamp",
                    "split_name",
                ],
            ),
            "forecast_context": _columns_containing(
                frame,
                ("forecast_", "forecast"),
            ),
            "battery_soc_context": _columns_containing(frame, ("soc", "battery")),
            "tenant_context": _present(
                frame,
                [
                    "tenant_id",
                    "source_model_name",
                    "v13_readiness_decision",
                    "v13_blocking_context_families",
                ],
            ),
            "candidate_schedule_context": _present(
                frame,
                [
                    "dt_candidate_id_target",
                    "dt_candidate_index_target",
                    "dt_schedule_family_target",
                    "dispatch_mw_vector",
                ],
            ),
            "value_return_targets": _present(
                frame,
                [
                    "return_to_go_regret_target_uah",
                    "regret_delta_vs_v2_plus_uah",
                    "schedule_value_uah",
                ],
            ),
            "gate_context": _present(
                frame,
                [
                    "v13_training_permission_gate_passed",
                    "permitted_model_training_row",
                    "training_blocker",
                    "market_execution_enabled",
                ],
            ),
        },
    }


def _teacher_contract_summary(
    *,
    dataset_summary: dict[str, Any],
    feature_contract: dict[str, Any],
) -> dict[str, Any]:
    feature_column_groups = feature_contract["feature_column_groups"]
    v13_training_permission_passed = bool(
        dataset_summary["v13_training_permission_gate_passed"]
    )
    permitted_rows = int(dataset_summary["permitted_model_training_rows"])
    train_selection_rows = int(dataset_summary["train_selection_rows"])
    training_permission_status = (
        "ready_for_offline_training_benchmark"
        if v13_training_permission_passed and permitted_rows > 0
        else "blocked_until_v13_source_readiness"
    )
    return {
        "claim_scope": "candidate_schedule_teacher_contract_not_market_execution",
        "dfl_input_groups": list(_DFL_INPUT_GROUPS),
        "dfl_target_groups": list(_DFL_TARGET_GROUPS),
        "dt_input_groups": list(_DT_INPUT_GROUPS),
        "required_dfl_input_groups_present": _feature_groups_present(
            feature_column_groups,
            _DFL_INPUT_GROUPS,
        ),
        "required_dfl_target_groups_present": _feature_groups_present(
            feature_column_groups,
            _DFL_TARGET_GROUPS,
        ),
        "required_dt_input_groups_present": _feature_groups_present(
            feature_column_groups,
            _DT_INPUT_GROUPS,
        ),
        "target_label_space": "candidate_index_or_schedule_family",
        "dt_action_target_contract": feature_contract["dt_action_target_contract"],
        "v2_plus_role": feature_contract["v2_plus_role"],
        "training_permission_status": training_permission_status,
        "train_selection_rows": train_selection_rows,
        "permitted_model_training_rows": permitted_rows,
        "training_rows_blocked_by_v13_source_readiness": bool(
            train_selection_rows > 0
            and permitted_rows == 0
            and not v13_training_permission_passed
        ),
        "raw_hourly_action_imitation": False,
        "not_full_dfl": True,
        "not_deployed_dt_control": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _feature_groups_present(
    feature_column_groups: dict[str, list[str]],
    group_names: tuple[str, ...],
) -> bool:
    return all(bool(feature_column_groups.get(group_name)) for group_name in group_names)


def _candidate_schedule_teacher_contract_failures(packet: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if packet.get("phase") != "phase_2_v13_gated_dt_lava_teacher_dataset":
        failures.append("invalid_phase")
    claim_boundary = _mapping(packet.get("claim_boundary"))
    feature_contract = _mapping(packet.get("feature_contract"))
    teacher_contract = _mapping(packet.get("teacher_contract_summary"))
    if claim_boundary.get("target_label_space") != "candidate_index_or_schedule_family":
        failures.append("invalid_target_label_space")
    if claim_boundary.get("no_raw_hourly_action_imitation") is not True:
        failures.append("raw_hourly_action_boundary_missing")
    if feature_contract.get("dt_action_target_contract") != (
        "candidate_id_or_schedule_family"
    ):
        failures.append("invalid_dt_action_target_contract")
    if feature_contract.get("v2_plus_role") != "teacher_comparator_fallback":
        failures.append("invalid_v2_plus_role")
    for flag_name in (
        "required_dfl_input_groups_present",
        "required_dfl_target_groups_present",
        "required_dt_input_groups_present",
    ):
        if teacher_contract.get(flag_name) is not True:
            failures.append(f"teacher_contract_flag_not_true:{flag_name}")
    if teacher_contract.get("raw_hourly_action_imitation") is not False:
        failures.append("teacher_contract_raw_hourly_action_imitation")
    return failures


def _training_permission_consistency_failures(packet: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    dataset_summary = _mapping(packet.get("dataset_summary"))
    teacher_contract = _mapping(packet.get("teacher_contract_summary"))
    gate_passport = _mapping(packet.get("gate_passport"))
    v13_gate = _mapping(gate_passport.get("v13_training_permission_gate"))
    permission_passed = bool(dataset_summary.get("v13_training_permission_gate_passed"))
    permitted_rows = _int_value(dataset_summary.get("permitted_model_training_rows"))
    train_rows = _int_value(dataset_summary.get("train_selection_rows"))
    if v13_gate.get("passed") is not permission_passed:
        failures.append("v13_gate_passport_mismatch")
    if permission_passed and permitted_rows <= 0:
        failures.append("v13_permission_passed_without_permitted_rows")
    if not permission_passed and permitted_rows != 0:
        failures.append("v13_permission_blocked_with_permitted_rows")
    if not permission_passed and teacher_contract.get("training_permission_status") != (
        "blocked_until_v13_source_readiness"
    ):
        failures.append("blocked_training_permission_status_mismatch")
    if permission_passed and train_rows > 0 and permitted_rows > train_rows:
        failures.append("permitted_rows_exceed_train_rows")
    return failures


def _promotion_execution_blocked_failures(packet: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    dataset_summary = _mapping(packet.get("dataset_summary"))
    gate_passport = _mapping(packet.get("gate_passport"))
    claim_boundary = _mapping(packet.get("claim_boundary"))
    promotion_gate = _mapping(gate_passport.get("dt_lava_training_promotion_gate"))
    market_gate = _mapping(gate_passport.get("market_execution_gate"))
    if dataset_summary.get("promotion_gate_passed") is not False:
        failures.append("promotion_gate_passed_not_false")
    if dataset_summary.get("market_execution_gate_passed") is not False:
        failures.append("market_execution_gate_passed_not_false")
    if promotion_gate.get("passed") is not False:
        failures.append("dt_lava_training_promotion_gate_passed")
    if market_gate.get("passed") is not False:
        failures.append("market_execution_gate_passed")
    if claim_boundary.get("not_deployed_decision_transformer_control") is not True:
        failures.append("deployed_dt_boundary_missing")
    if claim_boundary.get("not_full_dfl") is not True:
        failures.append("not_full_dfl_boundary_missing")
    return failures


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


def _gate_passport(dataset_summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    v13_passed = bool(dataset_summary["v13_training_permission_gate_passed"])
    return {
        "teacher_dataset_contract_gate": {
            "passed": True,
            "status": "passed",
            "claim_scope": "candidate_index_or_schedule_family_dataset_contract",
            "market_execution_enabled": False,
        },
        "v13_training_permission_gate": {
            "passed": v13_passed,
            "status": "passed" if v13_passed else "blocked",
            "claim_scope": "source_readiness_for_dt_lava_training",
            "market_execution_enabled": False,
        },
        "safe_switch_coverage_gate": {
            "passed": bool(dataset_summary["safe_switch_coverage_gate_passed"]),
            "status": "passed"
            if bool(dataset_summary["safe_switch_coverage_gate_passed"])
            else "blocked",
            "claim_scope": "tenant_source_safe_switch_floor_for_dt_lava_training",
            "market_execution_enabled": False,
        },
        "dt_lava_training_promotion_gate": {
            "passed": False,
            "status": "not_run",
            "claim_scope": "future_offline_dt_lava_strict_lp_promotion",
            "market_execution_enabled": False,
        },
        "market_execution_gate": {
            "passed": False,
            "status": "out_of_scope",
            "claim_scope": "future_market_execution_contract",
            "market_execution_enabled": False,
        },
    }


def _packet_markdown(packet: dict[str, Any]) -> str:
    summary = packet["dataset_summary"]
    gates = packet["gate_passport"]
    architecture = packet["feature_contract"]["architecture_recommendation"]
    teacher_contract = packet["teacher_contract_summary"]
    lines = [
        "# V13 DT/LAVA Teacher Dataset Packet",
        "",
        f"Run slug: `{packet['run_slug']}`",
        f"Dagster run: `{packet.get('dagster_run_id')}`",
        f"Asset check status: `{packet.get('asset_check_status')}`",
        "",
        "## Claim Boundary",
        "",
        "This packet exports candidate id / schedule-family targets for an "
        "offline DT/LAVA challenger. It is not a deployed Decision Transformer "
        "controller, not full differentiable DFL, not market execution, and "
        "`market_execution_enabled=false`.",
        "",
        "## Dataset",
        "",
        f"- Rows: `{summary['rows']}`",
        f"- Permitted model-training rows: `{summary['permitted_model_training_rows']}`",
        f"- Final-holdout scoring rows: `{summary['final_holdout_scoring_rows']}`",
        f"- V13 training permission: `{gates['v13_training_permission_gate']['status']}`",
        f"- DT/LAVA training promotion gate: `{gates['dt_lava_training_promotion_gate']['status']}`",
        f"- Market execution gate: `{gates['market_execution_gate']['status']}`",
        "",
        "## Architecture Contract",
        "",
        f"- DFL input: `{architecture['dfl_input']}`",
        f"- DFL target: `{architecture['dfl_target']}`",
        f"- DT input: `{architecture['dt_input']}`",
        f"- DT action target: `{architecture['dt_action_target']}`",
        f"- V2+ role: `{architecture['v2_plus_role']}`",
        "",
        "## Teacher Contract Summary",
        "",
        f"- Required DFL input groups present: `{teacher_contract['required_dfl_input_groups_present']}`",
        f"- Required DFL target groups present: `{teacher_contract['required_dfl_target_groups_present']}`",
        f"- Required DT input groups present: `{teacher_contract['required_dt_input_groups_present']}`",
        f"- Training permission status: `{teacher_contract['training_permission_status']}`",
        f"- Raw hourly action imitation: `{teacher_contract['raw_hourly_action_imitation']}`",
        "",
        "## Artifacts",
        "",
        f"- `{V13_DT_LAVA_TEACHER_JSON_ARTIFACT_NAME}`",
        f"- `{V13_DT_LAVA_TEACHER_MARKDOWN_ARTIFACT_NAME}`",
        f"- `{V13_DT_LAVA_TEACHER_ROWS_CSV_ARTIFACT_NAME}`",
        "",
    ]
    return "\n".join(lines)


def _training_blocker_counts(frame: pl.DataFrame) -> dict[str, int]:
    rows = (
        frame.group_by("training_blocker")
        .agg(pl.len().alias("row_count"))
        .iter_rows(named=True)
    )
    return {str(row["training_blocker"]): int(row["row_count"]) for row in rows}


def _safe_switch_coverage_summary(
    frame: pl.DataFrame,
    permitted_rows: pl.DataFrame,
) -> dict[str, Any]:
    required_columns = {
        "tenant_id",
        "source_model_name",
        "v13_prior_material_safe_switch_example_count",
        "v13_min_prior_material_safe_switch_examples_for_dt",
    }
    if not required_columns.issubset(set(frame.columns)):
        return {
            "safe_switch_covered_tenant_source_count": 0,
            "safe_switch_required_tenant_source_count": 0,
            "safe_switch_min_prior_material_examples_required": 0,
            "safe_switch_min_observed_prior_material_examples": 0,
            "safe_switch_coverage_gate_passed": False,
        }
    required_pairs = {
        (str(row["tenant_id"]), str(row["source_model_name"]))
        for row in permitted_rows.iter_rows(named=True)
    }
    covered_pairs: set[tuple[str, str]] = set()
    min_required = 0
    min_observed: int | None = None
    for row in permitted_rows.iter_rows(named=True):
        observed = _safe_int(row["v13_prior_material_safe_switch_example_count"])
        required = _safe_int(row["v13_min_prior_material_safe_switch_examples_for_dt"])
        min_required = max(min_required, required)
        min_observed = observed if min_observed is None else min(min_observed, observed)
        if observed >= required and required > 0:
            covered_pairs.add((str(row["tenant_id"]), str(row["source_model_name"])))
    return {
        "safe_switch_covered_tenant_source_count": len(covered_pairs),
        "safe_switch_required_tenant_source_count": len(required_pairs),
        "safe_switch_min_prior_material_examples_required": min_required,
        "safe_switch_min_observed_prior_material_examples": min_observed or 0,
        "safe_switch_coverage_gate_passed": bool(
            required_pairs and covered_pairs == required_pairs
        ),
    }


def _single_contract_value(frame: pl.DataFrame, column_name: str) -> str:
    values = sorted({str(value) for value in frame[column_name].unique().to_list()})
    if len(values) != 1:
        raise ValueError(f"V13 DT/LAVA teacher contract requires one {column_name}.")
    return values[0]


def _safe_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return 0
    return 0


def _int_value(value: Any) -> int:
    return _safe_int(value)


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _contains_market_execution_enabled_true(value: Any) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "market_execution_enabled" and bool(item):
                return True
            if _contains_market_execution_enabled_true(item):
                return True
        return False
    if isinstance(value, list | tuple):
        return any(_contains_market_execution_enabled_true(item) for item in value)
    return False


def _columns_containing(frame: pl.DataFrame, needles: tuple[str, ...]) -> list[str]:
    return [
        column
        for column in frame.columns
        if any(needle in column.lower() for needle in needles)
    ]


def _present(frame: pl.DataFrame, columns: list[str]) -> list[str]:
    return [column for column in columns if column in frame.columns]


def _refuse_true(frame: pl.DataFrame, column_name: str) -> None:
    if any(bool(value) for value in frame[column_name].to_list()):
        raise ValueError(f"V13 DT/LAVA teacher export refuses {column_name}=true.")


def _require_true(frame: pl.DataFrame, column_name: str) -> None:
    if not all(bool(value) for value in frame[column_name].to_list()):
        raise ValueError(f"V13 DT/LAVA teacher export requires {column_name}=true.")


def _write_rows_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in rows[0]})


def _frame_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return [_jsonable(row) for row in frame.iter_rows(named=True)]


def _csv_value(value: Any) -> Any:
    value = _jsonable(value)
    if isinstance(value, dict | list):
        return json.dumps(value, sort_keys=True)
    return value


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
    "V13_DT_LAVA_TEACHER_JSON_ARTIFACT_NAME",
    "V13_DT_LAVA_TEACHER_MARKDOWN_ARTIFACT_NAME",
    "V13_DT_LAVA_TEACHER_ROWS_CSV_ARTIFACT_NAME",
    "V13_DT_LAVA_TEACHER_VALIDATION_JSON_ARTIFACT_NAME",
    "build_dfl_v13_dt_lava_teacher_packet",
    "validate_dfl_v13_dt_lava_teacher_packet",
    "write_dfl_v13_dt_lava_teacher_packet",
]
