"""Local evidence export for TFT quantile screen packets."""

from __future__ import annotations

from datetime import date, datetime, timezone
import csv
import json
from pathlib import Path
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.tft_quantile_schedule_value import (
    FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    TFT_QUANTILE_SOURCE_MODELS,
    evaluate_dfl_tft_augmented_v2_plus_gate,
)

TFT_SCREEN_JSON_ARTIFACT_NAME: Final[str] = "dfl_tft_quantile_screen_summary.json"
TFT_SCREEN_MARKDOWN_ARTIFACT_NAME: Final[str] = "dfl_tft_quantile_screen_summary.md"
TFT_RAW_ROWS_CSV_ARTIFACT_NAME: Final[str] = "tft_raw_strict_rows.csv"
TFT_CANDIDATES_CSV_ARTIFACT_NAME: Final[str] = "tft_candidate_library_rows.csv"
TFT_AUGMENTED_ROWS_CSV_ARTIFACT_NAME: Final[str] = "tft_augmented_gate_rows.csv"


def build_dfl_tft_quantile_screen_packet(
    *,
    run_slug: str,
    raw_strict_frame: pl.DataFrame,
    candidate_library_frame: pl.DataFrame,
    augmented_gate_frame: pl.DataFrame,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
    asset_check_status: str | None = None,
) -> dict[str, Any]:
    """Build a TFT screen packet without requiring promotion-gate success."""

    _validate_frame(
        raw_strict_frame,
        required_columns={
            "tenant_id",
            "forecast_model_name",
            "anchor_timestamp",
            "regret_uah",
            "evaluation_payload",
        },
        frame_name="raw TFT strict frame",
    )
    _validate_frame(
        candidate_library_frame,
        required_columns={
            "tenant_id",
            "source_model_name",
            "candidate_family",
            "split_name",
            "anchor_timestamp",
            "regret_uah",
            "evaluation_payload",
        },
        frame_name="TFT candidate library frame",
    )
    _validate_frame(
        augmented_gate_frame,
        required_columns={
            "tenant_id",
            "source_model_name",
            "selection_role",
            "anchor_timestamp",
            "regret_uah",
            "evaluation_payload",
        },
        frame_name="TFT augmented gate frame",
    )
    if _market_execution_enabled(
        raw_strict_frame, candidate_library_frame, augmented_gate_frame
    ):
        raise ValueError("TFT screen packet requires market execution disabled.")
    gate = evaluate_dfl_tft_augmented_v2_plus_gate(
        augmented_gate_frame,
        baseline_source_model_name=FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
        tft_source_model_names=TFT_QUANTILE_SOURCE_MODELS,
    )
    return {
        "run_slug": run_slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "asset_check_status": asset_check_status,
        "claim_boundary": {
            "offline_strategy_promotion_only": True,
            "not_full_dfl": True,
            "not_deployed_decision_transformer_control": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "strict_fallback": "strict_similar_day",
            "comparator": "Ukrainian-only Schedule/Value Learner V2+",
            "no_european_training_rows": True,
        },
        "gate": {
            "passed": gate.passed,
            "decision": gate.decision,
            "description": gate.description,
            "metrics": gate.metrics,
        },
        "gate_blockers": _gate_blockers(augmented_gate_frame),
        "raw_strict_summary": _raw_strict_summary(raw_strict_frame),
        "candidate_library_summary": _candidate_library_summary(
            candidate_library_frame
        ),
        "augmented_gate_summary": _augmented_gate_summary(augmented_gate_frame),
        "attached_artifacts": {
            "summary_json": TFT_SCREEN_JSON_ARTIFACT_NAME,
            "summary_markdown": TFT_SCREEN_MARKDOWN_ARTIFACT_NAME,
            "raw_strict_rows_csv": TFT_RAW_ROWS_CSV_ARTIFACT_NAME,
            "candidate_library_rows_csv": TFT_CANDIDATES_CSV_ARTIFACT_NAME,
            "augmented_gate_rows_csv": TFT_AUGMENTED_ROWS_CSV_ARTIFACT_NAME,
        },
    }


def write_dfl_tft_quantile_screen_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
    raw_strict_frame: pl.DataFrame,
    candidate_library_frame: pl.DataFrame,
    augmented_gate_frame: pl.DataFrame,
) -> Path:
    """Write JSON, Markdown, and CSV artifacts for the TFT screen packet."""

    export_dir = output_root / str(packet["run_slug"])
    export_dir.mkdir(parents=True, exist_ok=True)
    _write_rows_csv(
        export_dir / TFT_RAW_ROWS_CSV_ARTIFACT_NAME,
        _frame_rows(raw_strict_frame),
    )
    _write_rows_csv(
        export_dir / TFT_CANDIDATES_CSV_ARTIFACT_NAME,
        _frame_rows(candidate_library_frame),
    )
    _write_rows_csv(
        export_dir / TFT_AUGMENTED_ROWS_CSV_ARTIFACT_NAME,
        _frame_rows(augmented_gate_frame),
    )
    (export_dir / TFT_SCREEN_JSON_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / TFT_SCREEN_MARKDOWN_ARTIFACT_NAME).write_text(
        _packet_markdown(packet),
        encoding="utf-8",
    )
    return export_dir


def _raw_strict_summary(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return _frame_rows(
        frame.group_by("forecast_model_name")
        .agg(
            [
                pl.len().alias("row_count"),
                pl.col("tenant_id").n_unique().alias("tenant_count"),
                pl.col("anchor_timestamp").n_unique().alias("anchor_count"),
                pl.col("regret_uah").mean().alias("mean_regret_uah"),
                pl.col("regret_uah").median().alias("median_regret_uah"),
            ]
        )
        .sort("forecast_model_name")
    )


def _candidate_library_summary(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return _frame_rows(
        frame.group_by(["source_model_name", "split_name", "candidate_family"])
        .agg(
            [
                pl.len().alias("row_count"),
                pl.col("tenant_id").n_unique().alias("tenant_count"),
                pl.col("anchor_timestamp").n_unique().alias("anchor_count"),
                pl.col("regret_uah").mean().alias("mean_regret_uah"),
                pl.col("regret_uah").median().alias("median_regret_uah"),
            ]
        )
        .sort(["source_model_name", "split_name", "candidate_family"])
    )


def _augmented_gate_summary(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return _frame_rows(
        frame.group_by(["source_model_name", "selection_role"])
        .agg(
            [
                pl.len().alias("row_count"),
                pl.col("tenant_id").n_unique().alias("tenant_count"),
                pl.col("anchor_timestamp").n_unique().alias("anchor_count"),
                pl.col("regret_uah").mean().alias("mean_regret_uah"),
                pl.col("regret_uah").median().alias("median_regret_uah"),
            ]
        )
        .sort(["source_model_name", "selection_role"])
    )


def _packet_markdown(packet: dict[str, Any]) -> str:
    gate = packet["gate"]
    lines = [
        "# TFT Quantile Screen Evidence Packet",
        "",
        f"Run slug: `{packet['run_slug']}`",
        f"Dagster run: `{packet.get('dagster_run_id')}`",
        f"Asset check status: `{packet.get('asset_check_status')}`",
        "",
        "## Claim Boundary",
        "",
        "This packet is Offline Strategy Promotion evidence only. It is not live "
        "market execution, not a deployed Decision Transformer controller, and "
        "not a claim that TFT has replaced the Ukrainian-only V2+ baseline.",
        "",
        "## Gate Status",
        "",
        f"- Gate decision: `{gate['decision']}`",
        f"- Gate passed: `{gate['passed']}`",
        f"- Gate description: {gate['description']}",
        f"- Gate blockers: `{', '.join(packet['gate_blockers']) or 'none'}`",
        "",
        "## Raw Strict Summary",
        "",
        "| Forecast model | Rows | Anchors | Mean regret UAH | Median regret UAH |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in packet["raw_strict_summary"]:
        lines.append(
            "| {model} | {rows} | {anchors} | {mean:.2f} | {median:.2f} |".format(
                model=row["forecast_model_name"],
                rows=row["row_count"],
                anchors=row["anchor_count"],
                mean=row["mean_regret_uah"],
                median=row["median_regret_uah"],
            )
        )
    lines.extend(
        [
            "",
            "## Attached Artifacts",
            "",
            f"- `{TFT_SCREEN_JSON_ARTIFACT_NAME}`",
            f"- `{TFT_SCREEN_MARKDOWN_ARTIFACT_NAME}`",
            f"- `{TFT_RAW_ROWS_CSV_ARTIFACT_NAME}`",
            f"- `{TFT_CANDIDATES_CSV_ARTIFACT_NAME}`",
            f"- `{TFT_AUGMENTED_ROWS_CSV_ARTIFACT_NAME}`",
            "",
        ]
    )
    return "\n".join(lines)


def _validate_frame(
    frame: pl.DataFrame,
    *,
    required_columns: set[str],
    frame_name: str,
) -> None:
    missing = sorted(required_columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {missing}")
    if frame.is_empty():
        raise ValueError(f"{frame_name} must not be empty.")


def _gate_blockers(frame: pl.DataFrame) -> list[str]:
    blockers: set[str] = set()
    if "tft_gate_blocker" in frame.columns:
        blockers.update(
            str(value)
            for value in frame["tft_gate_blocker"].unique().to_list()
            if value is not None and str(value)
        )
    for payload in frame["evaluation_payload"].to_list():
        if isinstance(payload, dict):
            blocker = payload.get("tft_gate_blocker")
            if blocker is not None and str(blocker):
                blockers.add(str(blocker))
    return sorted(blockers)


def _market_execution_enabled(*frames: pl.DataFrame) -> bool:
    for frame in frames:
        if "market_execution_enabled" in frame.columns and any(
            bool(value) for value in frame["market_execution_enabled"].to_list()
        ):
            return True
        if "evaluation_payload" in frame.columns:
            for payload in frame["evaluation_payload"].to_list():
                if isinstance(payload, dict) and payload.get("market_execution_enabled") is True:
                    return True
    return False


def _frame_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return [_jsonable(row) for row in frame.iter_rows(named=True)]


def _write_rows_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=sorted(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in rows[0]})


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime | date):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def _csv_value(value: Any) -> Any:
    value = _jsonable(value)
    if isinstance(value, dict | list):
        return json.dumps(value, sort_keys=True)
    return value


__all__ = [
    "build_dfl_tft_quantile_screen_packet",
    "write_dfl_tft_quantile_screen_packet",
]
