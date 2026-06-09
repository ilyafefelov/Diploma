"""V13-gated DT/LAVA teacher-row contract.

This module is a narrow bridge between the existing UA-context LAVA sequence
rows and a later offline DT/LAVA trainer. It does not train a model. It
materializes the row contract that makes the target academically honest:

* DFL labels are candidate value and regret deltas versus V2+;
* DT labels are candidate id / schedule-family targets, not raw hourly actions;
* V2+ remains the teacher/comparator/fallback;
* V13 source readiness decides whether any row may be used for model training.
"""

from __future__ import annotations

from typing import Any, Final

import polars as pl

V13_DT_LAVA_TEACHER_CONTRACT_CLAIM_SCOPE: Final[str] = (
    "v13_gated_dt_lava_teacher_contract_not_training_until_source_ready"
)

DFL_INPUT_CONTRACT: Final[str] = (
    "calibrated_forecasts_tenant_soc_context_feasible_candidate_schedules"
)
DFL_TARGET_CONTRACT: Final[str] = (
    "best_candidate_schedule_value_regret_delta_vs_v2_plus"
)
DT_INPUT_CONTRACT: Final[str] = (
    "v13_teacher_sequence_forecast_battery_tenant_candidate_value_return_to_go"
)
DT_ACTION_TARGET_CONTRACT: Final[str] = "candidate_id_or_schedule_family"
V2_PLUS_ROLE: Final[str] = "teacher_comparator_fallback"

_REQUIRED_SEQUENCE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "split_name",
        "is_training_row",
        "teacher_candidate_key",
        "dt_candidate_index_target",
        "label_regret_delta_vs_v2_plus_uah",
        "decision_value_uah",
        "forecast_price_uah_mwh_vector",
        "dispatch_mw_vector",
        "soc_fraction_vector",
        "raw_hourly_action_imitation",
        "market_execution_enabled",
    }
)
_REQUIRED_READINESS_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "v13_candidate_generation_ready",
        "readiness_decision",
        "prior_material_safe_switch_example_count",
        "min_prior_material_safe_switch_examples_for_dt",
        "blocking_context_families",
        "market_execution_enabled",
    }
)


def build_dfl_v13_gated_dt_lava_teacher_contract_frame(
    ua_context_lava_sequence_training_frame: pl.DataFrame,
    ua_context_acquisition_readiness_v13_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Overlay V13 source-readiness permission onto DT/LAVA teacher rows."""

    _require_columns(
        ua_context_lava_sequence_training_frame,
        _REQUIRED_SEQUENCE_COLUMNS,
        frame_name="ua_context_lava_sequence_training_frame",
    )
    _require_columns(
        ua_context_acquisition_readiness_v13_frame,
        _REQUIRED_READINESS_COLUMNS,
        frame_name="ua_context_acquisition_readiness_v13_frame",
    )
    _refuse_execution_claims(
        ua_context_lava_sequence_training_frame,
        frame_name="ua_context_lava_sequence_training_frame",
    )
    _refuse_execution_claims(
        ua_context_acquisition_readiness_v13_frame,
        frame_name="ua_context_acquisition_readiness_v13_frame",
    )
    _refuse_raw_hourly_action_imitation(ua_context_lava_sequence_training_frame)

    readiness_by_key = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in ua_context_acquisition_readiness_v13_frame.iter_rows(named=True)
    }
    output_rows: list[dict[str, Any]] = []
    for row in ua_context_lava_sequence_training_frame.iter_rows(named=True):
        readiness = readiness_by_key.get(
            (str(row["tenant_id"]), str(row["source_model_name"]))
        )
        gate_passed = _v13_training_permission_gate_passed(readiness)
        is_training_row = bool(row["is_training_row"])
        split_name = str(row["split_name"])
        permitted_training_row = gate_passed and is_training_row
        training_blocker = _training_blocker(
            readiness=readiness,
            gate_passed=gate_passed,
            is_training_row=is_training_row,
            split_name=split_name,
        )
        copied = dict(row)
        copied.update(
            {
                "dt_candidate_id_target": str(row["teacher_candidate_key"]),
                "dt_candidate_index_target": int(row["dt_candidate_index_target"]),
                "dt_schedule_family_target": _schedule_family_target(row),
                "return_to_go_regret_target_uah": _float_value(
                    row.get("dt_return_to_go_uah"),
                    fallback=_float_value(row.get("teacher_return_to_go_delta_uah")),
                ),
                "regret_delta_vs_v2_plus_uah": _float_value(
                    row["label_regret_delta_vs_v2_plus_uah"]
                ),
                "schedule_value_uah": _float_value(row["decision_value_uah"]),
                "dfl_input_contract": DFL_INPUT_CONTRACT,
                "dfl_target_contract": DFL_TARGET_CONTRACT,
                "dt_input_contract": DT_INPUT_CONTRACT,
                "dt_action_target_contract": DT_ACTION_TARGET_CONTRACT,
                "v2_plus_role": V2_PLUS_ROLE,
                "v13_training_permission_gate_passed": gate_passed,
                "v13_readiness_decision": _readiness_text(
                    readiness,
                    "readiness_decision",
                    default="missing_v13_readiness_row",
                ),
                "v13_blocking_context_families": _readiness_text(
                    readiness,
                    "blocking_context_families",
                    default="missing_v13_readiness_row",
                ),
                "v13_prior_material_safe_switch_example_count": _readiness_int(
                    readiness,
                    "prior_material_safe_switch_example_count",
                ),
                "v13_min_prior_material_safe_switch_examples_for_dt": _readiness_int(
                    readiness,
                    "min_prior_material_safe_switch_examples_for_dt",
                ),
                "permitted_model_training_row": permitted_training_row,
                "permits_model_training": permitted_training_row,
                "training_blocker": training_blocker,
                "promotion_gate_passed": False,
                "market_execution_gate_passed": False,
                "raw_hourly_action_imitation": False,
                "claim_scope": V13_DT_LAVA_TEACHER_CONTRACT_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_deployed_dt_control": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        output_rows.append(copied)

    if not output_rows:
        return _empty_contract_frame()
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "dt_candidate_index_target",
        ]
    )


def _v13_training_permission_gate_passed(readiness: dict[str, Any] | None) -> bool:
    if readiness is None:
        return False
    return bool(readiness["v13_candidate_generation_ready"]) and str(
        readiness["readiness_decision"]
    ) == "v13_candidate_generation_ready"


def _training_blocker(
    *,
    readiness: dict[str, Any] | None,
    gate_passed: bool,
    is_training_row: bool,
    split_name: str,
) -> str:
    if readiness is None:
        return "missing_v13_readiness_row"
    if not gate_passed:
        return "v13_training_permission_gate_blocked"
    if not is_training_row and split_name == "final_holdout":
        return "final_holdout_scoring_only"
    if not is_training_row:
        return "not_marked_training_row"
    return "none"


def _schedule_family_target(row: dict[str, Any]) -> str:
    if row.get("dt_candidate_family_target"):
        return str(row["dt_candidate_family_target"])
    if row.get("teacher_target_family"):
        return str(row["teacher_target_family"])
    if row.get("candidate_family"):
        return str(row["candidate_family"])
    return "unknown_schedule_family"


def _readiness_text(
    readiness: dict[str, Any] | None,
    column: str,
    *,
    default: str,
) -> str:
    if readiness is None:
        return default
    return str(readiness.get(column, default))


def _readiness_int(readiness: dict[str, Any] | None, column: str) -> int:
    if readiness is None:
        return 0
    value = readiness.get(column, 0)
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _float_value(value: Any, *, fallback: float = 0.0) -> float:
    if value is None:
        return fallback
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _require_columns(
    frame: pl.DataFrame,
    required: frozenset[str],
    *,
    frame_name: str,
) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} missing required columns: {missing}")


def _refuse_execution_claims(frame: pl.DataFrame, *, frame_name: str) -> None:
    if "market_execution_enabled" not in frame.columns:
        return
    if any(bool(value) for value in frame["market_execution_enabled"].to_list()):
        raise ValueError(f"{frame_name} refuses market execution claims.")


def _refuse_raw_hourly_action_imitation(frame: pl.DataFrame) -> None:
    if any(bool(value) for value in frame["raw_hourly_action_imitation"].to_list()):
        raise ValueError(
            "V13 DT/LAVA teacher contract refuses raw hourly action imitation."
        )


def _empty_contract_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "tenant_id": pl.Utf8,
            "source_model_name": pl.Utf8,
            "anchor_timestamp": pl.Datetime,
            "dt_candidate_id_target": pl.Utf8,
            "dt_candidate_index_target": pl.Int64,
            "dt_schedule_family_target": pl.Utf8,
            "return_to_go_regret_target_uah": pl.Float64,
            "regret_delta_vs_v2_plus_uah": pl.Float64,
            "schedule_value_uah": pl.Float64,
            "dfl_input_contract": pl.Utf8,
            "dfl_target_contract": pl.Utf8,
            "dt_input_contract": pl.Utf8,
            "dt_action_target_contract": pl.Utf8,
            "v2_plus_role": pl.Utf8,
            "v13_training_permission_gate_passed": pl.Boolean,
            "permitted_model_training_row": pl.Boolean,
            "permits_model_training": pl.Boolean,
            "training_blocker": pl.Utf8,
            "promotion_gate_passed": pl.Boolean,
            "market_execution_gate_passed": pl.Boolean,
            "raw_hourly_action_imitation": pl.Boolean,
            "claim_scope": pl.Utf8,
            "not_full_dfl": pl.Boolean,
            "not_deployed_dt_control": pl.Boolean,
            "not_market_execution": pl.Boolean,
            "market_execution_enabled": pl.Boolean,
        }
    )


__all__ = [
    "V13_DT_LAVA_TEACHER_CONTRACT_CLAIM_SCOPE",
    "build_dfl_v13_gated_dt_lava_teacher_contract_frame",
]
