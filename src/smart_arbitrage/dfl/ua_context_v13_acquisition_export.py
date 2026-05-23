"""Local evidence export for V13 Ukrainian context acquisition readiness."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.v2_plus_dfl_dt_bridge import V2_PLUS_HEADLINE_BASELINE_METRICS

UA_CONTEXT_V13_JSON_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_acquisition_summary.json"
)
UA_CONTEXT_V13_MARKDOWN_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_acquisition_summary.md"
)
UA_CONTEXT_V13_SOURCE_INVENTORY_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_source_inventory_rows.csv"
)
UA_CONTEXT_V13_SOURCE_EVIDENCE_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_source_acquisition_evidence_rows.csv"
)
UA_CONTEXT_V13_READINESS_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_readiness_rows.csv"
)


def build_dfl_ua_context_v13_acquisition_packet(
    *,
    run_slug: str,
    source_inventory_frame: pl.DataFrame,
    readiness_frame: pl.DataFrame,
    acquisition_source_evidence_frame: pl.DataFrame | None = None,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
    asset_check_status: str | None = None,
) -> dict[str, Any]:
    """Build a V13 source-acquisition packet from materialized frames."""

    _validate_packet_inputs(
        source_inventory_frame=source_inventory_frame,
        readiness_frame=readiness_frame,
        acquisition_source_evidence_frame=acquisition_source_evidence_frame,
    )
    readiness_summary = _readiness_summary(readiness_frame)
    return {
        "run_slug": run_slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "asset_check_status": asset_check_status,
        "v13_candidate_generation_ready": readiness_summary[
            "v13_candidate_generation_ready"
        ],
        "claim_boundary": {
            "offline_strategy_promotion_only": True,
            "not_full_dfl": True,
            "not_deployed_decision_transformer_control": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "no_european_training_rows": True,
            "no_dashboard_api_default_switch": True,
            "v13_stops_before_candidate_generation": True,
            "dt_lava_still_gated": True,
        },
        "headline_baseline": {
            **dict(V2_PLUS_HEADLINE_BASELINE_METRICS),
            "calibrated_v2_plus_median_regret_uah": 67.30,
        },
        "source_inventory_summary": _source_inventory_summary(source_inventory_frame),
        "acquisition_source_evidence_summary": _source_inventory_summary(
            acquisition_source_evidence_frame
        )
        if acquisition_source_evidence_frame is not None
        else None,
        "readiness_summary": readiness_summary,
        "attached_artifacts": {
            "summary_json": UA_CONTEXT_V13_JSON_ARTIFACT_NAME,
            "summary_markdown": UA_CONTEXT_V13_MARKDOWN_ARTIFACT_NAME,
            "source_evidence_csv": UA_CONTEXT_V13_SOURCE_EVIDENCE_CSV_ARTIFACT_NAME,
            "source_inventory_csv": UA_CONTEXT_V13_SOURCE_INVENTORY_CSV_ARTIFACT_NAME,
            "readiness_csv": UA_CONTEXT_V13_READINESS_CSV_ARTIFACT_NAME,
        },
    }


def write_dfl_ua_context_v13_acquisition_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
    source_inventory_frame: pl.DataFrame,
    readiness_frame: pl.DataFrame,
    acquisition_source_evidence_frame: pl.DataFrame | None = None,
) -> Path:
    """Write local JSON, Markdown, and CSV V13 acquisition artifacts."""

    export_dir = output_root / str(packet["run_slug"])
    export_dir.mkdir(parents=True, exist_ok=True)
    if acquisition_source_evidence_frame is not None:
        _write_csv_safe(
            acquisition_source_evidence_frame,
            export_dir / UA_CONTEXT_V13_SOURCE_EVIDENCE_CSV_ARTIFACT_NAME,
        )
    _write_csv_safe(
        source_inventory_frame,
        export_dir / UA_CONTEXT_V13_SOURCE_INVENTORY_CSV_ARTIFACT_NAME,
    )
    _write_csv_safe(
        readiness_frame,
        export_dir / UA_CONTEXT_V13_READINESS_CSV_ARTIFACT_NAME,
    )
    (export_dir / UA_CONTEXT_V13_JSON_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / UA_CONTEXT_V13_MARKDOWN_ARTIFACT_NAME).write_text(
        _packet_markdown(packet),
        encoding="utf-8",
    )
    return export_dir


def _validate_packet_inputs(
    *,
    source_inventory_frame: pl.DataFrame,
    readiness_frame: pl.DataFrame,
    acquisition_source_evidence_frame: pl.DataFrame | None,
) -> None:
    _require_columns(
        source_inventory_frame,
        {
            "source_family",
            "source_status",
            "coverage_ratio",
            "required_for_v13_candidate_generation",
            "market_execution_enabled",
        },
        frame_name="V13 source inventory frame",
    )
    _require_columns(
        readiness_frame,
        {
            "tenant_id",
            "source_model_name",
            "v13_candidate_generation_ready",
            "readiness_decision",
            "blocking_context_families",
            "prior_material_safe_switch_example_count",
            "min_prior_material_safe_switch_examples_for_dt",
            "dt_lava_ready",
            "target_label_space",
            "raw_hourly_action_imitation",
            "market_execution_enabled",
        },
        frame_name="V13 readiness frame",
    )
    if acquisition_source_evidence_frame is not None:
        _require_columns(
            acquisition_source_evidence_frame,
            {
                "source_family",
                "source_status",
                "coverage_ratio",
                "required_for_v13_candidate_generation",
                "market_execution_enabled",
            },
            frame_name="V13 acquisition source evidence frame",
        )
    for name, frame in {
        "source inventory": source_inventory_frame,
        "readiness": readiness_frame,
        **(
            {"acquisition source evidence": acquisition_source_evidence_frame}
            if acquisition_source_evidence_frame is not None
            else {}
        ),
    }.items():
        if frame.select(pl.col("market_execution_enabled").any()).item():
            raise ValueError(f"V13 packet refuses {name} market execution rows.")
    if readiness_frame.select(pl.col("raw_hourly_action_imitation").any()).item():
        raise ValueError("V13 packet refuses raw hourly action imitation.")


def _source_inventory_summary(frame: pl.DataFrame) -> dict[str, Any]:
    required = frame.filter(pl.col("required_for_v13_candidate_generation"))
    blocked = required.filter(pl.col("source_status") != "ready_prior_context")
    return {
        "source_family_count": frame.height,
        "required_source_family_count": required.height,
        "blocked_required_source_family_count": blocked.height,
        "blocked_required_sources": sorted(blocked["source_family"].to_list())
        if blocked.height
        else [],
        "source_statuses": sorted(
            str(value) for value in frame["source_status"].unique()
        ),
    }


def _readiness_summary(frame: pl.DataFrame) -> dict[str, Any]:
    ready_rows = frame.filter(pl.col("v13_candidate_generation_ready")).height
    return {
        "readiness_rows": frame.height,
        "ready_rows": ready_rows,
        "blocked_rows": frame.height - ready_rows,
        "v13_candidate_generation_ready": frame.height > 0
        and ready_rows == frame.height,
        "readiness_decisions": sorted(
            str(value) for value in frame["readiness_decision"].unique()
        ),
        "max_prior_material_safe_switch_examples": _safe_int(
            frame["prior_material_safe_switch_example_count"].max()
        )
        if frame.height
        else 0,
        "min_safe_examples_required": _safe_int(
            frame["min_prior_material_safe_switch_examples_for_dt"].max()
        )
        if frame.height
        else 20,
    }


def _packet_markdown(packet: dict[str, Any]) -> str:
    readiness = packet["readiness_summary"]
    status = (
        "V13 Candidate Generation Ready"
        if packet["v13_candidate_generation_ready"]
        else "Data Acquisition Needed"
    )
    return "\n".join(
        [
            "# V13 Ukrainian Context Acquisition Packet",
            "",
            f"Run slug: `{packet['run_slug']}`",
            f"Dagster run: `{packet.get('dagster_run_id')}`",
            f"Asset check status: `{packet.get('asset_check_status')}`",
            "",
            "## Claim Boundary",
            "",
            "This packet is Offline Strategy Promotion evidence only. It is not "
            "candidate generation, not DT/LAVA training, not live dispatch, and "
            "not market execution. `market_execution_enabled=false`.",
            "",
            f"## {status}",
            "",
            (
                "- Frozen comparator: calibrated Ukrainian-only V2+ mean regret "
                f"`{packet['headline_baseline']['calibrated_v2_plus_mean_regret_uah']}` "
                "UAH, median `67.30` UAH, rolling `4 / 4`."
            ),
            f"- Readiness rows: `{readiness['readiness_rows']}`.",
            f"- Ready rows: `{readiness['ready_rows']}`.",
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
    "UA_CONTEXT_V13_JSON_ARTIFACT_NAME",
    "UA_CONTEXT_V13_MARKDOWN_ARTIFACT_NAME",
    "UA_CONTEXT_V13_READINESS_CSV_ARTIFACT_NAME",
    "UA_CONTEXT_V13_SOURCE_EVIDENCE_CSV_ARTIFACT_NAME",
    "UA_CONTEXT_V13_SOURCE_INVENTORY_CSV_ARTIFACT_NAME",
    "build_dfl_ua_context_v13_acquisition_packet",
    "write_dfl_ua_context_v13_acquisition_packet",
]
