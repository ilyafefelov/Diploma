"""Local evidence export for governed market-coupling ablation packets."""

from __future__ import annotations

from datetime import date, datetime, timezone
import csv
import json
from pathlib import Path
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.market_coupling_ablation import (
    validate_dfl_market_coupling_v2_plus_ablation_evidence,
)

ABLATION_JSON_ARTIFACT_NAME: Final[str] = (
    "dfl_market_coupling_v2_plus_ablation_summary.json"
)
ABLATION_MARKDOWN_ARTIFACT_NAME: Final[str] = (
    "dfl_market_coupling_v2_plus_ablation_summary.md"
)
ABLATION_ROWS_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_market_coupling_v2_plus_ablation_rows.csv"
)
BASELINE_COMPARATOR: Final[dict[str, object]] = {
    "name": "ukrainian_only_schedule_value_learner_v2_plus",
    "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
    "calibrated_v2_plus_mean_regret_uah": 174.77,
    "strict_similar_day_mean_regret_uah": 310.58,
    "frozen_v2_mean_regret_uah": 206.37,
    "improvement_ratio_vs_strict": 0.4373,
    "improvement_ratio_vs_frozen_v2": 0.1531,
    "rolling_robustness_windows": "4/4",
}


def build_dfl_market_coupling_v2_plus_ablation_packet(
    *,
    run_slug: str,
    ablation_frame: pl.DataFrame,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
) -> dict[str, Any]:
    """Build an export packet only after ablation evidence validates."""

    evidence = validate_dfl_market_coupling_v2_plus_ablation_evidence(ablation_frame)
    if not evidence.passed:
        raise ValueError(
            "market-coupling ablation evidence check failed; refusing export: "
            f"{evidence.description}"
        )
    rows = _frame_rows(ablation_frame)
    return {
        "run_slug": run_slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "baseline_comparator": BASELINE_COMPARATOR,
        "claim_boundary": {
            "offline_strategy_promotion_only": True,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "strict_fallback": "strict_similar_day",
            "no_european_training_rows": True,
            "neighbor_data_role": "governed_exogenous_feature_candidates_only",
        },
        "evidence_check": {
            "passed": evidence.passed,
            "description": evidence.description,
            "metadata": evidence.metadata,
        },
        "ablation_summary": _ablation_summary(rows),
        "ablation_rows": rows,
        "attached_artifacts": {
            "summary_json": ABLATION_JSON_ARTIFACT_NAME,
            "summary_markdown": ABLATION_MARKDOWN_ARTIFACT_NAME,
            "ablation_rows_csv": ABLATION_ROWS_CSV_ARTIFACT_NAME,
        },
    }


def write_dfl_market_coupling_v2_plus_ablation_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
    ablation_frame: pl.DataFrame,
) -> Path:
    """Write local JSON, Markdown, and CSV artifacts for the ablation packet."""

    export_dir = output_root / str(packet["run_slug"])
    export_dir.mkdir(parents=True, exist_ok=True)
    _write_rows_csv(
        export_dir / ABLATION_ROWS_CSV_ARTIFACT_NAME,
        _frame_rows(ablation_frame),
    )
    (export_dir / ABLATION_JSON_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / ABLATION_MARKDOWN_ARTIFACT_NAME).write_text(
        _ablation_markdown(packet),
        encoding="utf-8",
    )
    return export_dir


def _ablation_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    approved_feature_columns: set[str] = set()
    blocked_feature_columns: set[str] = set()
    training_blockers: set[str] = set()
    trained_count = 0
    passed_count = 0
    for row in rows:
        status = str(row["ablation_status"])
        status_counts[status] = status_counts.get(status, 0) + 1
        trained_count += int(bool(row["did_train_market_coupled_variant"]))
        passed_count += int(bool(row["ablation_passed"]))
        approved_feature_columns.update(
            _csv_values(str(row["approved_external_feature_columns_csv"]))
        )
        blocked_feature_columns.update(
            _csv_values(str(row["blocked_external_feature_columns_csv"]))
        )
        training_blockers.update(_csv_values(str(row["external_training_blockers_csv"])))
    return {
        "row_count": len(rows),
        "source_model_count": len({str(row["source_model_name"]) for row in rows}),
        "status_counts": dict(sorted(status_counts.items())),
        "approved_feature_columns": sorted(approved_feature_columns),
        "blocked_feature_columns": sorted(blocked_feature_columns),
        "training_blockers": sorted(training_blockers),
        "trained_market_coupled_variant_count": trained_count,
        "ablation_passed_count": passed_count,
        "all_rows_market_execution_disabled": all(
            not bool(row["market_execution_enabled"]) for row in rows
        ),
    }


def _ablation_markdown(packet: dict[str, Any]) -> str:
    summary = packet["ablation_summary"]
    baseline = packet["baseline_comparator"]
    lines = [
        "# DFL Market-Coupling Ablation V1 Evidence Packet",
        "",
        f"Run slug: `{packet['run_slug']}`",
        f"Dagster run: `{packet.get('dagster_run_id')}`",
        "",
        "## Claim Boundary",
        "",
        "This packet is Offline Strategy Promotion evidence only. It is not live "
        "market execution, not a deployed Decision Transformer controller, and "
        "not a claim that European rows entered Ukrainian training.",
        "",
        "## Frozen Baseline Comparator",
        "",
        f"- Baseline: `{baseline['name']}`",
        (
            "- Calibrated V2+ mean regret: "
            f"{baseline['calibrated_v2_plus_mean_regret_uah']:.2f} UAH"
        ),
        (
            "- Strict similar-day mean regret: "
            f"{baseline['strict_similar_day_mean_regret_uah']:.2f} UAH"
        ),
        (
            "- Improvement versus strict: "
            f"{baseline['improvement_ratio_vs_strict']:.2%}"
        ),
        f"- Rolling robustness: `{baseline['rolling_robustness_windows']}`",
        "",
        "## Ablation Status",
        "",
        f"- Status counts: `{summary['status_counts']}`",
        (
            "- Approved feature columns: "
            f"`{', '.join(summary['approved_feature_columns'])}`"
            if summary["approved_feature_columns"]
            else "- Approved feature columns: none"
        ),
        (
            "- Blocked feature columns: "
            f"`{', '.join(summary['blocked_feature_columns'])}`"
            if summary["blocked_feature_columns"]
            else "- Blocked feature columns: none"
        ),
        (
            "- Training blockers: "
            f"`{', '.join(summary['training_blockers'])}`"
            if summary["training_blockers"]
            else "- Training blockers: none"
        ),
        (
            "- Market-coupled training runs: "
            f"{summary['trained_market_coupled_variant_count']}"
        ),
        f"- Passing ablation rows: {summary['ablation_passed_count']}",
        "",
    ]
    if summary["trained_market_coupled_variant_count"] == 0:
        lines.extend(
            [
                "No market-coupled training run was executed because governance "
                "did not approve an external feature route.",
                "",
            ]
        )
    lines.extend(
        [
            "## Source Rows",
            "",
            "| Source | Status | Trained B | Passed | Blocker |",
            "|---|---|---:|---:|---|",
        ]
    )
    for row in packet["ablation_rows"]:
        lines.append(
            "| "
            f"`{row['source_model_name']}` | "
            f"`{row['ablation_status']}` | "
            f"{row['did_train_market_coupled_variant']} | "
            f"{row['ablation_passed']} | "
            f"`{row['ablation_blocker']}` |"
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


def _csv_values(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


__all__ = [
    "build_dfl_market_coupling_v2_plus_ablation_packet",
    "write_dfl_market_coupling_v2_plus_ablation_packet",
]
