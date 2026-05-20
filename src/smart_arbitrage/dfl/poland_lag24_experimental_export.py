"""Evidence export for Poland lag-24 experimental schedule/value screens."""

from __future__ import annotations

import csv
from datetime import date, datetime, timezone
import json
from pathlib import Path
from typing import Any, Final

import polars as pl

SUMMARY_JSON_ARTIFACT_NAME: Final[str] = (
    "poland_lag24_experimental_schedule_value_summary.json"
)
SUMMARY_MARKDOWN_ARTIFACT_NAME: Final[str] = (
    "poland_lag24_experimental_schedule_value_summary.md"
)
COMPARISON_CSV_ARTIFACT_NAME: Final[str] = (
    "poland_lag24_experimental_schedule_value_comparison.csv"
)
RAW_STRICT_CSV_ARTIFACT_NAME: Final[str] = (
    "poland_lag24_experimental_raw_strict_rows.csv"
)

FROZEN_COMPARATOR_GROUP: Final[str] = "frozen_ukrainian_v2_plus"
EXPERIMENTAL_GROUP: Final[str] = "poland_lag24_experimental"
FROZEN_V2_PLUS_BASELINE: Final[dict[str, Any]] = {
    "name": "ukrainian_only_schedule_value_learner_v2_plus",
    "mean_regret_uah": 174.768398,
    "median_regret_uah": 67.300273,
    "rolling_robustness_windows": "4/4",
    "strict_similar_day_mean_regret_uah": 310.582808,
    "market_execution_enabled": False,
}

REQUIRED_COMPARISON_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "comparison_group",
        "forecast_model_name",
        "row_count",
        "tenant_count",
        "anchor_count",
        "mean_regret_uah",
        "median_regret_uah",
        "best_frozen_v2_plus_model_name",
        "best_frozen_v2_plus_mean_regret_uah",
        "mean_regret_delta_vs_best_frozen_v2_plus_uah",
        "mean_regret_improvement_ratio_vs_best_frozen_v2_plus",
        "market_execution_enabled",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_poland_lag24_experimental_schedule_value_packet(
    *,
    run_slug: str,
    comparison_frame: pl.DataFrame,
    raw_strict_frame: pl.DataFrame | None = None,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
) -> dict[str, Any]:
    """Build a local evidence packet for the Poland lag-24 experimental path."""

    comparison_rows = _validated_comparison_rows(comparison_frame)
    raw_summary = _raw_strict_summary(raw_strict_frame) if raw_strict_frame is not None else []
    frozen_rows = [
        row for row in comparison_rows if row["comparison_group"] == FROZEN_COMPARATOR_GROUP
    ]
    experimental_rows = [
        row for row in comparison_rows if row["comparison_group"] == EXPERIMENTAL_GROUP
    ]
    if not frozen_rows:
        raise ValueError("comparison packet requires frozen Ukrainian-only V2+ rows")
    if not experimental_rows:
        raise ValueError("comparison packet requires Poland lag-24 experimental rows")
    best_frozen = min(frozen_rows, key=lambda row: float(row["mean_regret_uah"]))
    best_experimental = min(
        experimental_rows,
        key=lambda row: float(row["mean_regret_uah"]),
    )
    best_frozen_mean = float(best_frozen["mean_regret_uah"])
    best_experimental_mean = float(best_experimental["mean_regret_uah"])
    mean_delta = best_experimental_mean - best_frozen_mean
    improvement_ratio = (
        0.0 if best_frozen_mean == 0.0 else (best_frozen_mean - best_experimental_mean) / best_frozen_mean
    )
    promotes = improvement_ratio > 0.0
    blocker = None if promotes else "mean_not_improved_vs_frozen_v2_plus"
    return {
        "run_slug": run_slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "baseline_comparator": FROZEN_V2_PLUS_BASELINE,
        "claim_boundary": {
            "offline_strategy_promotion_only": True,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "no_dashboard_or_api_default_switch": True,
            "strict_fallback": "strict_similar_day",
            "external_feature_role": (
                "point_in_time_poland_lag24_exogenous_columns_only"
            ),
            "no_european_training_rows": True,
        },
        "summary": {
            "comparison_row_count": len(comparison_rows),
            "experimental_row_count": len(experimental_rows),
            "best_frozen_v2_plus_model_name": best_frozen["forecast_model_name"],
            "best_frozen_v2_plus_mean_regret_uah": best_frozen_mean,
            "best_frozen_v2_plus_median_regret_uah": float(
                best_frozen["median_regret_uah"]
            ),
            "best_experimental_model_name": best_experimental["forecast_model_name"],
            "best_experimental_mean_regret_uah": best_experimental_mean,
            "best_experimental_median_regret_uah": float(
                best_experimental["median_regret_uah"]
            ),
            "mean_regret_delta_vs_frozen_v2_plus_uah": mean_delta,
            "mean_regret_improvement_ratio_vs_frozen_v2_plus": improvement_ratio,
            "raw_strict_summary": raw_summary,
        },
        "gate": {
            "promotes_over_frozen_v2_plus": promotes,
            "blocker": blocker,
            "negative_evidence": not promotes,
        },
        "comparison_rows": comparison_rows,
        "attached_artifacts": {
            "summary_json": SUMMARY_JSON_ARTIFACT_NAME,
            "summary_markdown": SUMMARY_MARKDOWN_ARTIFACT_NAME,
            "comparison_csv": COMPARISON_CSV_ARTIFACT_NAME,
            "raw_strict_csv": RAW_STRICT_CSV_ARTIFACT_NAME
            if raw_strict_frame is not None
            else None,
        },
    }


def write_poland_lag24_experimental_schedule_value_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
    comparison_frame: pl.DataFrame,
    raw_strict_frame: pl.DataFrame | None = None,
) -> Path:
    """Write JSON, Markdown, and CSV artifacts for local evidence review."""

    export_dir = output_root / str(packet["run_slug"])
    export_dir.mkdir(parents=True, exist_ok=True)
    _write_rows_csv(
        export_dir / COMPARISON_CSV_ARTIFACT_NAME,
        _frame_rows(comparison_frame),
    )
    if raw_strict_frame is not None:
        _write_rows_csv(
            export_dir / RAW_STRICT_CSV_ARTIFACT_NAME,
            _frame_rows(raw_strict_frame),
        )
    (export_dir / SUMMARY_JSON_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / SUMMARY_MARKDOWN_ARTIFACT_NAME).write_text(
        _packet_markdown(packet),
        encoding="utf-8",
    )
    return export_dir


def _validated_comparison_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    if frame.is_empty():
        raise ValueError("comparison packet requires non-empty rows")
    missing_columns = sorted(REQUIRED_COMPARISON_COLUMNS.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"comparison frame is missing columns: {missing_columns}")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("comparison packet refuses market execution claims")
    if not frame.select(pl.col("not_market_execution").all()).item():
        raise ValueError("comparison packet refuses non-research rows")
    if not frame.select(pl.col("not_full_dfl").all()).item():
        raise ValueError("comparison packet refuses full-DFL overclaims")
    return _frame_rows(frame)


def _raw_strict_summary(frame: pl.DataFrame) -> list[dict[str, Any]]:
    required_columns = {"forecast_model_name", "regret_uah"}
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"raw strict frame is missing columns: {missing_columns}")
    if frame.is_empty():
        return []
    summary = frame.group_by("forecast_model_name").agg(
        [
            pl.len().alias("row_count"),
            pl.mean("regret_uah").alias("mean_regret_uah"),
            pl.median("regret_uah").alias("median_regret_uah"),
        ]
    )
    return _frame_rows(summary.sort("mean_regret_uah"))


def _packet_markdown(packet: dict[str, Any]) -> str:
    summary = packet["summary"]
    gate = packet["gate"]
    baseline = packet["baseline_comparator"]
    result_label = (
        "promotion evidence"
        if gate["promotes_over_frozen_v2_plus"]
        else "near-miss negative evidence"
    )
    lines = [
        "# Poland Lag-24 Experimental Schedule/Value Evidence Packet",
        "",
        f"Run slug: `{packet['run_slug']}`",
        f"Dagster run: `{packet.get('dagster_run_id')}`",
        "",
        "## Claim Boundary",
        "",
        "This packet is Offline Strategy Promotion evidence only: "
        "`market_execution_enabled=false`, no dashboard/API default switch, "
        "no live dispatch, and no European rows in Ukrainian training.",
        "",
        "## Result",
        "",
        f"This is **{result_label}**.",
        (
            "- Frozen Ukrainian-only V2+ mean regret: "
            f"{summary['best_frozen_v2_plus_mean_regret_uah']:.2f} UAH"
        ),
        (
            "- Best Poland lag-24 experimental mean regret: "
            f"{summary['best_experimental_mean_regret_uah']:.2f} UAH"
        ),
        (
            "- Mean regret delta versus frozen V2+: "
            f"{summary['mean_regret_delta_vs_frozen_v2_plus_uah']:.2f} UAH"
        ),
        (
            "- Improvement ratio versus frozen V2+: "
            f"{summary['mean_regret_improvement_ratio_vs_frozen_v2_plus']:.2%}"
        ),
        f"- Gate blocker: `{gate['blocker']}`",
        "",
        "## Frozen Baseline",
        "",
        f"- Baseline: `{baseline['name']}`",
        f"- Mean regret: {baseline['mean_regret_uah']:.2f} UAH",
        f"- Median regret: {baseline['median_regret_uah']:.2f} UAH",
        f"- Rolling robustness: `{baseline['rolling_robustness_windows']}`",
        "",
        "## Comparison Rows",
        "",
        "| Group | Forecast model | Rows | Mean regret | Median regret |",
        "|---|---|---:|---:|---:|",
    ]
    for row in packet["comparison_rows"]:
        lines.append(
            "| "
            f"`{row['comparison_group']}` | "
            f"`{row['forecast_model_name']}` | "
            f"{row['row_count']} | "
            f"{float(row['mean_regret_uah']):.2f} | "
            f"{float(row['median_regret_uah']):.2f} |"
        )
    raw_summary = summary["raw_strict_summary"]
    if raw_summary:
        lines.extend(
            [
                "",
                "## Raw Strict Forecast Rows",
                "",
                "| Forecast model | Rows | Mean regret | Median regret |",
                "|---|---:|---:|---:|",
            ]
        )
        for row in raw_summary:
            lines.append(
                "| "
                f"`{row['forecast_model_name']}` | "
                f"{row['row_count']} | "
                f"{float(row['mean_regret_uah']):.2f} | "
                f"{float(row['median_regret_uah']):.2f} |"
            )
    lines.append("")
    return "\n".join(lines)


def _frame_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return [_jsonable(row) for row in frame.iter_rows(named=True)]


def _write_rows_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=sorted(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(inner_value) for key, inner_value in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


__all__ = [
    "build_poland_lag24_experimental_schedule_value_packet",
    "write_poland_lag24_experimental_schedule_value_packet",
]
