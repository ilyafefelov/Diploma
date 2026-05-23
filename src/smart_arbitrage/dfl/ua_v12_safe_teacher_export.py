"""Local evidence export for V12 UA safe teacher-label backfill packets."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.v2_plus_dfl_dt_bridge import V2_PLUS_HEADLINE_BASELINE_METRICS

UA_V12_SAFE_TEACHER_JSON_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_v12_safe_teacher_summary.json"
)
UA_V12_SAFE_TEACHER_MARKDOWN_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_v12_safe_teacher_summary.md"
)
UA_V12_SOURCE_INVENTORY_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_v12_source_inventory_rows.csv"
)
UA_V12_CONTEXT_PANEL_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_v12_expanded_context_rows.csv"
)
UA_V12_TEACHER_LABEL_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_v12_safe_teacher_label_rows.csv"
)
UA_V12_CANDIDATE_LIBRARY_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_v12_low_tail_candidate_rows.csv"
)
UA_V12_STRICT_RESCORE_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_v12_low_tail_strict_rescore_rows.csv"
)
UA_V12_READINESS_DECISION_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_v12_readiness_decision_rows.csv"
)


def build_dfl_ua_v12_safe_teacher_backfill_packet(
    *,
    run_slug: str,
    source_inventory_frame: pl.DataFrame,
    expanded_context_panel_frame: pl.DataFrame,
    safe_teacher_label_panel_frame: pl.DataFrame,
    low_tail_candidate_library_frame: pl.DataFrame,
    low_tail_strict_rescore_frame: pl.DataFrame,
    readiness_decision_frame: pl.DataFrame,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
    asset_check_status: str | None = None,
) -> dict[str, Any]:
    """Build a V12 readiness packet from materialized V12 frames."""

    _validate_packet_inputs(
        source_inventory_frame=source_inventory_frame,
        expanded_context_panel_frame=expanded_context_panel_frame,
        safe_teacher_label_panel_frame=safe_teacher_label_panel_frame,
        low_tail_candidate_library_frame=low_tail_candidate_library_frame,
        low_tail_strict_rescore_frame=low_tail_strict_rescore_frame,
        readiness_decision_frame=readiness_decision_frame,
    )
    readiness_summary = _readiness_summary(readiness_decision_frame)
    return {
        "run_slug": run_slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "asset_check_status": asset_check_status,
        "dt_lava_ready": readiness_summary["dt_lava_ready"],
        "claim_boundary": {
            "offline_strategy_promotion_only": True,
            "not_full_dfl": True,
            "not_deployed_decision_transformer_control": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "no_european_training_rows": True,
            "no_dashboard_api_default_switch": True,
            "dt_target_if_ready": "candidate_index_or_schedule_family",
            "no_raw_hourly_action_imitation": True,
            "comparator": "Schedule/Value Learner V2+",
        },
        "headline_baseline": {
            **dict(V2_PLUS_HEADLINE_BASELINE_METRICS),
            "calibrated_v2_plus_median_regret_uah": 67.30,
        },
        "source_inventory_summary": _source_inventory_summary(source_inventory_frame),
        "context_summary": _context_summary(expanded_context_panel_frame),
        "teacher_summary": _teacher_summary(safe_teacher_label_panel_frame),
        "candidate_summary": _candidate_summary(
            low_tail_candidate_library_frame,
            low_tail_strict_rescore_frame,
        ),
        "readiness_summary": readiness_summary,
        "attached_artifacts": {
            "summary_json": UA_V12_SAFE_TEACHER_JSON_ARTIFACT_NAME,
            "summary_markdown": UA_V12_SAFE_TEACHER_MARKDOWN_ARTIFACT_NAME,
            "source_inventory_csv": UA_V12_SOURCE_INVENTORY_CSV_ARTIFACT_NAME,
            "context_panel_csv": UA_V12_CONTEXT_PANEL_CSV_ARTIFACT_NAME,
            "teacher_label_csv": UA_V12_TEACHER_LABEL_CSV_ARTIFACT_NAME,
            "candidate_library_csv": UA_V12_CANDIDATE_LIBRARY_CSV_ARTIFACT_NAME,
            "strict_rescore_csv": UA_V12_STRICT_RESCORE_CSV_ARTIFACT_NAME,
            "readiness_decision_csv": UA_V12_READINESS_DECISION_CSV_ARTIFACT_NAME,
        },
    }


def write_dfl_ua_v12_safe_teacher_backfill_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
    source_inventory_frame: pl.DataFrame,
    expanded_context_panel_frame: pl.DataFrame,
    safe_teacher_label_panel_frame: pl.DataFrame,
    low_tail_candidate_library_frame: pl.DataFrame,
    low_tail_strict_rescore_frame: pl.DataFrame,
    readiness_decision_frame: pl.DataFrame,
) -> Path:
    """Write local JSON, Markdown, and CSV V12 readiness artifacts."""

    export_dir = output_root / str(packet["run_slug"])
    export_dir.mkdir(parents=True, exist_ok=True)
    _write_csv_safe(
        source_inventory_frame,
        export_dir / UA_V12_SOURCE_INVENTORY_CSV_ARTIFACT_NAME
    )
    _write_csv_safe(
        expanded_context_panel_frame,
        export_dir / UA_V12_CONTEXT_PANEL_CSV_ARTIFACT_NAME
    )
    _write_csv_safe(
        safe_teacher_label_panel_frame,
        export_dir / UA_V12_TEACHER_LABEL_CSV_ARTIFACT_NAME
    )
    _write_csv_safe(
        low_tail_candidate_library_frame,
        export_dir / UA_V12_CANDIDATE_LIBRARY_CSV_ARTIFACT_NAME
    )
    _write_csv_safe(
        low_tail_strict_rescore_frame,
        export_dir / UA_V12_STRICT_RESCORE_CSV_ARTIFACT_NAME
    )
    _write_csv_safe(
        readiness_decision_frame,
        export_dir / UA_V12_READINESS_DECISION_CSV_ARTIFACT_NAME
    )
    (export_dir / UA_V12_SAFE_TEACHER_JSON_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / UA_V12_SAFE_TEACHER_MARKDOWN_ARTIFACT_NAME).write_text(
        _packet_markdown(packet),
        encoding="utf-8",
    )
    return export_dir


def _validate_packet_inputs(
    *,
    source_inventory_frame: pl.DataFrame,
    expanded_context_panel_frame: pl.DataFrame,
    safe_teacher_label_panel_frame: pl.DataFrame,
    low_tail_candidate_library_frame: pl.DataFrame,
    low_tail_strict_rescore_frame: pl.DataFrame,
    readiness_decision_frame: pl.DataFrame,
) -> None:
    _require_columns(
        source_inventory_frame,
        {
            "source_family",
            "source_status",
            "coverage_ratio",
            "market_execution_enabled",
        },
        frame_name="V12 source inventory frame",
    )
    _require_columns(
        expanded_context_panel_frame,
        {
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "v12_existing_source_context_ready",
            "v12_context_expansion_decision",
            "market_execution_enabled",
        },
        frame_name="V12 expanded context frame",
    )
    _require_columns(
        safe_teacher_label_panel_frame,
        {
            "tenant_id",
            "source_model_name",
            "split_name",
            "candidate_source",
            "label_v12_material_safe_switch",
            "label_v12_tail_risk_loss",
            "market_execution_enabled",
        },
        frame_name="V12 teacher label frame",
    )
    _require_columns(
        low_tail_candidate_library_frame,
        {"candidate_source", "candidate_value_label_status", "market_execution_enabled"},
        frame_name="V12 candidate library frame",
    )
    _require_columns(
        low_tail_strict_rescore_frame,
        {"candidate_source", "candidate_value_label_status", "market_execution_enabled"},
        frame_name="V12 strict rescore frame",
    )
    _require_columns(
        readiness_decision_frame,
        {
            "tenant_id",
            "source_model_name",
            "prior_material_safe_switch_example_count",
            "min_prior_material_safe_switch_examples_for_dt",
            "dt_lava_ready",
            "readiness_decision",
            "target_label_space",
            "raw_hourly_action_imitation",
            "market_execution_enabled",
        },
        frame_name="V12 readiness decision frame",
    )
    frames = {
        "source inventory": source_inventory_frame,
        "expanded context": expanded_context_panel_frame,
        "teacher labels": safe_teacher_label_panel_frame,
        "candidate library": low_tail_candidate_library_frame,
        "strict rescore": low_tail_strict_rescore_frame,
        "readiness decision": readiness_decision_frame,
    }
    for name, frame in frames.items():
        if frame.select(pl.col("market_execution_enabled").any()).item():
            raise ValueError(f"V12 packet refuses {name} market execution rows.")
    if readiness_decision_frame.select(
        pl.col("raw_hourly_action_imitation").any()
    ).item():
        raise ValueError("V12 packet refuses raw hourly action imitation.")


def _source_inventory_summary(frame: pl.DataFrame) -> list[dict[str, Any]]:
    columns = [
        "source_family",
        "source_status",
        "coverage_ratio",
        "required_for_v12_candidate_generation",
        "optional_source_hook",
    ]
    return _frame_rows(frame.select([column for column in columns if column in frame.columns]))


def _context_summary(frame: pl.DataFrame) -> dict[str, Any]:
    ready_rows = frame.filter(pl.col("v12_existing_source_context_ready")).height
    return {
        "context_rows": frame.height,
        "ready_rows": ready_rows,
        "blocked_rows": frame.height - ready_rows,
        "context_decisions": sorted(
            str(value) for value in frame["v12_context_expansion_decision"].unique()
        ),
    }


def _teacher_summary(frame: pl.DataFrame) -> dict[str, Any]:
    return {
        "teacher_rows": frame.height,
        "material_safe_switch_rows": frame.filter(
            pl.col("label_v12_material_safe_switch")
        ).height,
        "tail_risk_rows": frame.filter(pl.col("label_v12_tail_risk_loss")).height,
        "train_prior_material_safe_switch_rows": frame.filter(
            (pl.col("split_name").is_in(["train", "prior"]))
            & pl.col("label_v12_material_safe_switch")
            & ~pl.col("label_v12_tail_risk_loss")
        ).height,
    }


def _candidate_summary(
    candidate_library_frame: pl.DataFrame,
    strict_rescore_frame: pl.DataFrame,
) -> dict[str, Any]:
    generated_filter = pl.col("candidate_source") == "ua_low_tail_v12_generated_candidate"
    generated = candidate_library_frame.filter(generated_filter)
    rescored_generated = strict_rescore_frame.filter(generated_filter)
    return {
        "candidate_rows": candidate_library_frame.height,
        "generated_candidate_rows": generated.height,
        "strict_rescored_generated_rows": rescored_generated.filter(
            pl.col("candidate_value_label_status") == "strict_rescored_v12_candidate"
        ).height
        if rescored_generated.height
        else 0,
        "generated_material_safe_switch_rows": rescored_generated.filter(
            pl.col("label_v12_material_safe_switch")
        ).height
        if "label_v12_material_safe_switch" in rescored_generated.columns
        else 0,
        "generated_tail_risk_rows": rescored_generated.filter(
            pl.col("label_v12_tail_risk_loss")
        ).height
        if "label_v12_tail_risk_loss" in rescored_generated.columns
        else 0,
    }


def _readiness_summary(frame: pl.DataFrame) -> dict[str, Any]:
    ready_rows = frame.filter(pl.col("dt_lava_ready")).height
    return {
        "readiness_rows": frame.height,
        "dt_lava_ready_rows": ready_rows,
        "blocked_rows": frame.height - ready_rows,
        "dt_lava_ready": frame.height > 0 and ready_rows == frame.height,
        "min_safe_examples_required": _safe_int(
            frame["min_prior_material_safe_switch_examples_for_dt"].max()
        )
        if frame.height
        else 20,
        "max_prior_material_safe_switch_examples": _safe_int(
            frame["prior_material_safe_switch_example_count"].max()
        )
        if frame.height
        else 0,
        "readiness_decisions": sorted(
            str(value) for value in frame["readiness_decision"].unique()
        ),
    }


def _packet_markdown(packet: dict[str, Any]) -> str:
    readiness = packet["readiness_summary"]
    status = "DT/LAVA Ready" if packet["dt_lava_ready"] else "DT/LAVA Blocked"
    return "\n".join(
        [
            "# V12 UA Safe Teacher-Label Backfill Packet",
            "",
            f"Run slug: `{packet['run_slug']}`",
            f"Dagster run: `{packet.get('dagster_run_id')}`",
            f"Asset check status: `{packet.get('asset_check_status')}`",
            "",
            "## Claim Boundary",
            "",
            "This packet is Offline Strategy Promotion evidence only. It is not "
            "live dispatch, not a dashboard/API default switch, and not market "
            "execution. `market_execution_enabled=false`.",
            "",
            f"## {status}",
            "",
            (
                "- Frozen comparator: calibrated Ukrainian-only V2+ mean regret "
                f"`{packet['headline_baseline']['calibrated_v2_plus_mean_regret_uah']}` "
                "UAH, median `67.30` UAH, rolling `4 / 4`."
            ),
            (
                "- DT/LAVA starts only after V12 has enough prior/train "
                "non-tail-risk safe-switch labels."
            ),
            f"- Readiness rows: `{readiness['readiness_rows']}`.",
            f"- Ready rows: `{readiness['dt_lava_ready_rows']}`.",
            f"- Blocked rows: `{readiness['blocked_rows']}`.",
            (
                "- Max prior material safe-switch examples: "
                f"`{readiness['max_prior_material_safe_switch_examples']}` / "
                f"`{readiness['min_safe_examples_required']}` required."
            ),
            (
                "- Readiness decisions: "
                f"`{', '.join(readiness['readiness_decisions'])}`."
            ),
            "",
        ]
    )


def _require_columns(
    frame: pl.DataFrame,
    required_columns: set[str],
    *,
    frame_name: str,
) -> None:
    missing_columns = required_columns.difference(frame.columns)
    if missing_columns:
        raise ValueError(
            f"{frame_name} is missing required columns: {sorted(missing_columns)}"
        )


def _write_csv_safe(frame: pl.DataFrame, path: Path) -> None:
    _csv_safe_frame(frame).write_csv(path)


def _csv_safe_frame(frame: pl.DataFrame) -> pl.DataFrame:
    expressions: list[pl.Expr] = []
    for column_name, dtype in zip(frame.columns, frame.dtypes, strict=True):
        if str(dtype).startswith(("List", "Array", "Struct")):
            expressions.append(
                pl.col(column_name)
                .map_elements(_json_string, return_dtype=pl.String)
                .alias(column_name)
            )
        else:
            expressions.append(pl.col(column_name))
    return frame.select(expressions)


def _json_string(value: object) -> str | None:
    if value is None:
        return None
    return json.dumps(_jsonable(value), sort_keys=True)


def _safe_int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float, str)):
        return int(value)
    raise TypeError(f"Cannot convert {type(value).__name__} to int.")


def _frame_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return list(frame.iter_rows(named=True))


def _jsonable(value: Any) -> Any:
    if isinstance(value, pl.Series):
        return [_jsonable(item) for item in value.to_list()]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


__all__ = [
    "UA_V12_CANDIDATE_LIBRARY_CSV_ARTIFACT_NAME",
    "UA_V12_CONTEXT_PANEL_CSV_ARTIFACT_NAME",
    "UA_V12_READINESS_DECISION_CSV_ARTIFACT_NAME",
    "UA_V12_SAFE_TEACHER_JSON_ARTIFACT_NAME",
    "UA_V12_SAFE_TEACHER_MARKDOWN_ARTIFACT_NAME",
    "UA_V12_SOURCE_INVENTORY_CSV_ARTIFACT_NAME",
    "UA_V12_STRICT_RESCORE_CSV_ARTIFACT_NAME",
    "UA_V12_TEACHER_LABEL_CSV_ARTIFACT_NAME",
    "build_dfl_ua_v12_safe_teacher_backfill_packet",
    "write_dfl_ua_v12_safe_teacher_backfill_packet",
]
