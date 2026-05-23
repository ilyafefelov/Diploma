"""Local evidence export for UA context acquisition readiness packets."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.v2_plus_dfl_dt_bridge import V2_PLUS_HEADLINE_BASELINE_METRICS

UA_CONTEXT_READINESS_JSON_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_backfill_readiness_summary.json"
)
UA_CONTEXT_READINESS_MARKDOWN_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_backfill_readiness_summary.md"
)
UA_CONTEXT_SOURCE_INVENTORY_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_source_inventory_rows.csv"
)
UA_CONTEXT_FAMILY_COVERAGE_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_family_coverage_rows.csv"
)
UA_CONTEXT_V11_GATE_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v11_gate_decision_rows.csv"
)


def build_dfl_ua_context_backfill_readiness_packet(
    *,
    run_slug: str,
    source_inventory_frame: pl.DataFrame,
    dam_publication_frame: pl.DataFrame,
    weather_load_pv_frame: pl.DataFrame,
    grid_event_frame: pl.DataFrame,
    calendar_block_frame: pl.DataFrame,
    coverage_gate_frame: pl.DataFrame,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
    asset_check_status: str | None = None,
) -> dict[str, Any]:
    """Build a V11-precondition readiness packet from materialized frames."""

    _validate_packet_inputs(source_inventory_frame, coverage_gate_frame)
    readiness_summary = _readiness_summary(coverage_gate_frame)
    return {
        "run_slug": run_slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "asset_check_status": asset_check_status,
        "v11_candidate_generation_ready": readiness_summary[
            "v11_candidate_generation_ready"
        ],
        "claim_boundary": {
            "offline_strategy_promotion_only": True,
            "not_full_dfl": True,
            "not_deployed_decision_transformer_control": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "no_european_training_rows": True,
            "no_dashboard_api_default_switch": True,
            "comparator": "Schedule/Value Learner V2+",
        },
        "headline_baseline": {
            **dict(V2_PLUS_HEADLINE_BASELINE_METRICS),
            "calibrated_v2_plus_median_regret_uah": 67.30,
        },
        "readiness_summary": readiness_summary,
        "source_inventory_summary": _source_inventory_summary(source_inventory_frame),
        "family_coverage_summary": _family_coverage_summary(
            dam_publication_frame=dam_publication_frame,
            weather_load_pv_frame=weather_load_pv_frame,
            grid_event_frame=grid_event_frame,
            calendar_block_frame=calendar_block_frame,
        ),
        "attached_artifacts": {
            "summary_json": UA_CONTEXT_READINESS_JSON_ARTIFACT_NAME,
            "summary_markdown": UA_CONTEXT_READINESS_MARKDOWN_ARTIFACT_NAME,
            "source_inventory_csv": UA_CONTEXT_SOURCE_INVENTORY_CSV_ARTIFACT_NAME,
            "family_coverage_csv": UA_CONTEXT_FAMILY_COVERAGE_CSV_ARTIFACT_NAME,
            "v11_gate_csv": UA_CONTEXT_V11_GATE_CSV_ARTIFACT_NAME,
        },
    }


def write_dfl_ua_context_backfill_readiness_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
    source_inventory_frame: pl.DataFrame,
    dam_publication_frame: pl.DataFrame,
    weather_load_pv_frame: pl.DataFrame,
    grid_event_frame: pl.DataFrame,
    calendar_block_frame: pl.DataFrame,
    coverage_gate_frame: pl.DataFrame,
) -> Path:
    """Write local JSON, Markdown, and CSV readiness artifacts."""

    export_dir = output_root / str(packet["run_slug"])
    export_dir.mkdir(parents=True, exist_ok=True)
    source_inventory_frame.write_csv(
        export_dir / UA_CONTEXT_SOURCE_INVENTORY_CSV_ARTIFACT_NAME
    )
    pl.concat(
        [
            _coverage_export_rows("dam_publication", dam_publication_frame),
            _coverage_export_rows("weather_load_pv", weather_load_pv_frame),
            _coverage_export_rows("grid_event", grid_event_frame),
            _coverage_export_rows("calendar_block", calendar_block_frame),
        ],
        how="diagonal_relaxed",
    ).write_csv(export_dir / UA_CONTEXT_FAMILY_COVERAGE_CSV_ARTIFACT_NAME)
    coverage_gate_frame.write_csv(export_dir / UA_CONTEXT_V11_GATE_CSV_ARTIFACT_NAME)
    (export_dir / UA_CONTEXT_READINESS_JSON_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / UA_CONTEXT_READINESS_MARKDOWN_ARTIFACT_NAME).write_text(
        _readiness_markdown(packet),
        encoding="utf-8",
    )
    return export_dir


def _validate_packet_inputs(
    source_inventory_frame: pl.DataFrame,
    coverage_gate_frame: pl.DataFrame,
) -> None:
    _require_columns(
        source_inventory_frame,
        {
            "source_family",
            "source_rows",
            "required_anchor_rows",
            "market_execution_enabled",
        },
        frame_name="UA context source inventory frame",
    )
    _require_columns(
        coverage_gate_frame,
        {
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "v11_candidate_generation_ready",
            "context_backfill_gate_decision",
            "blocking_context_families",
            "market_execution_enabled",
        },
        frame_name="UA context coverage gate frame",
    )
    if source_inventory_frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("UA context readiness packet refuses source inventory market execution rows.")
    if coverage_gate_frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("UA context readiness packet refuses gate market execution rows.")


def _readiness_summary(gate_frame: pl.DataFrame) -> dict[str, Any]:
    ready_rows = gate_frame.filter(pl.col("v11_candidate_generation_ready")).height
    blocked_rows = gate_frame.height - ready_rows
    decisions = sorted(
        str(value) for value in gate_frame["context_backfill_gate_decision"].unique()
    )
    return {
        "gate_rows": gate_frame.height,
        "v11_ready_rows": ready_rows,
        "blocked_rows": blocked_rows,
        "v11_candidate_generation_ready": gate_frame.height > 0 and blocked_rows == 0,
        "context_backfill_gate_decisions": decisions,
        "blocking_context_families": sorted(
            {
                family
                for value in gate_frame["blocking_context_families"].to_list()
                for family in str(value).split(",")
                if family and family != "none"
            }
        ),
    }


def _source_inventory_summary(source_inventory_frame: pl.DataFrame) -> list[dict[str, Any]]:
    return _frame_rows(
        source_inventory_frame.select(
            [
                "source_family",
                "source_rows",
                "required_anchor_rows",
                "publication_metadata_supported",
                "prior_timestamp_supported",
            ]
        )
    )


def _family_coverage_summary(
    *,
    dam_publication_frame: pl.DataFrame,
    weather_load_pv_frame: pl.DataFrame,
    grid_event_frame: pl.DataFrame,
    calendar_block_frame: pl.DataFrame,
) -> list[dict[str, Any]]:
    return [
        _coverage_summary_row("dam_publication", dam_publication_frame),
        _coverage_summary_row("weather_load_pv", weather_load_pv_frame),
        _coverage_summary_row("grid_event", grid_event_frame),
        _coverage_summary_row("calendar_block", calendar_block_frame),
    ]


def _coverage_summary_row(family: str, frame: pl.DataFrame) -> dict[str, Any]:
    return {
        "context_family": family,
        "rows": frame.height,
        "ready_rows": frame.filter(pl.col("prior_available")).height
        if frame.height and "prior_available" in frame.columns
        else 0,
    }


def _coverage_export_rows(family: str, frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height == 0:
        return pl.DataFrame({"context_family": [family]})
    return frame.with_columns(pl.lit(family).alias("context_family"))


def _readiness_markdown(packet: dict[str, Any]) -> str:
    summary = packet["readiness_summary"]
    status = (
        "V11 Candidate Generation Ready"
        if packet["v11_candidate_generation_ready"]
        else "V11 Candidate Generation Blocked"
    )
    return "\n".join(
        [
            "# UA Context Acquisition Readiness Packet",
            "",
            f"Run slug: `{packet['run_slug']}`",
            f"Dagster run: `{packet.get('dagster_run_id')}`",
            f"Asset check status: `{packet.get('asset_check_status')}`",
            "",
            "## Claim Boundary",
            "",
            "This packet is Offline Strategy Promotion evidence only. It is not "
            "live market execution, not deployed DT/LAVA, and not a dashboard/API "
            "default switch. `market_execution_enabled=false`.",
            "",
            f"## {status}",
            "",
            (
                "- Frozen comparator: calibrated Ukrainian-only V2+ mean regret "
                f"`{packet['headline_baseline']['calibrated_v2_plus_mean_regret_uah']}` "
                "UAH, median `67.30` UAH, rolling `4 / 4`."
            ),
            f"- Gate rows: `{summary['gate_rows']}`.",
            f"- V11 ready rows: `{summary['v11_ready_rows']}`.",
            f"- Blocked rows: `{summary['blocked_rows']}`.",
            (
                "- Gate decisions: "
                f"`{', '.join(summary['context_backfill_gate_decisions'])}`."
            ),
            (
                "- Blocking context families: "
                f"`{', '.join(summary['blocking_context_families']) or 'none'}`."
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
        raise ValueError(f"{frame_name} is missing required columns: {sorted(missing_columns)}")


def _frame_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return list(frame.iter_rows(named=True))


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


__all__ = [
    "UA_CONTEXT_FAMILY_COVERAGE_CSV_ARTIFACT_NAME",
    "UA_CONTEXT_READINESS_JSON_ARTIFACT_NAME",
    "UA_CONTEXT_READINESS_MARKDOWN_ARTIFACT_NAME",
    "UA_CONTEXT_SOURCE_INVENTORY_CSV_ARTIFACT_NAME",
    "UA_CONTEXT_V11_GATE_CSV_ARTIFACT_NAME",
    "build_dfl_ua_context_backfill_readiness_packet",
    "write_dfl_ua_context_backfill_readiness_packet",
]
