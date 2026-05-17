"""Local evidence export for V2+-anchored residual DFL/offline DT bridge packets."""

from __future__ import annotations

from datetime import date, datetime, timezone
import csv
import json
from pathlib import Path
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.v2_plus_dfl_dt_bridge import (
    V2_PLUS_HEADLINE_BASELINE_METRICS,
    evaluate_dfl_v2_plus_dfl_dt_bridge_gate,
    validate_dfl_v2_plus_dfl_dt_bridge_evidence,
)

BRIDGE_JSON_ARTIFACT_NAME: Final[str] = "dfl_v2_plus_dfl_dt_bridge_summary.json"
BRIDGE_MARKDOWN_ARTIFACT_NAME: Final[str] = "dfl_v2_plus_dfl_dt_bridge_summary.md"
BRIDGE_ROWS_CSV_ARTIFACT_NAME: Final[str] = "dfl_v2_plus_dfl_dt_bridge_rows.csv"
BRIDGE_ROLE_SUMMARY_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_v2_plus_dfl_dt_bridge_role_summary.csv"
)


def build_dfl_v2_plus_dfl_dt_bridge_packet(
    *,
    run_slug: str,
    strict_frame: pl.DataFrame,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
    asset_check_status: str | None = None,
) -> dict[str, Any]:
    """Build a compact bridge packet after structural evidence validation.

    A blocked gate is exportable here: this packet is also used to preserve
    negative evidence that the compact residual/DT challengers did not beat V2+.
    """

    evidence = validate_dfl_v2_plus_dfl_dt_bridge_evidence(strict_frame)
    if not evidence.passed:
        raise ValueError(
            "V2+-anchored bridge evidence check failed; refusing export: "
            f"{evidence.description}"
        )
    gate = evaluate_dfl_v2_plus_dfl_dt_bridge_gate(strict_frame)
    bridge_scope = _bridge_scope(strict_frame)
    return {
        "run_slug": run_slug,
        "bridge_scope": bridge_scope,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "asset_check_status": asset_check_status,
        "negative_evidence": not gate.passed,
        "claim_boundary": {
            "offline_strategy_promotion_only": True,
            "not_full_dfl": True,
            "not_deployed_decision_transformer_control": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "strict_fallback": "strict_similar_day",
            "comparator": "Schedule/Value Learner V2+",
            "no_european_training_rows": True,
        },
        "headline_baseline": dict(V2_PLUS_HEADLINE_BASELINE_METRICS),
        "evidence_check": {
            "passed": evidence.passed,
            "description": evidence.description,
            "metadata": evidence.metadata,
        },
        "gate": {
            "passed": gate.passed,
            "decision": gate.decision,
            "description": gate.description,
            "metrics": gate.metrics,
        },
        "role_summary": _role_summary(strict_frame),
        "attached_artifacts": {
            "summary_json": BRIDGE_JSON_ARTIFACT_NAME,
            "summary_markdown": BRIDGE_MARKDOWN_ARTIFACT_NAME,
            "strict_rows_csv": BRIDGE_ROWS_CSV_ARTIFACT_NAME,
            "role_summary_csv": BRIDGE_ROLE_SUMMARY_CSV_ARTIFACT_NAME,
        },
    }


def write_dfl_v2_plus_dfl_dt_bridge_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
    strict_frame: pl.DataFrame,
) -> Path:
    """Write local JSON, Markdown, and CSV bridge evidence artifacts."""

    export_dir = output_root / str(packet["run_slug"])
    export_dir.mkdir(parents=True, exist_ok=True)
    _write_rows_csv(export_dir / BRIDGE_ROWS_CSV_ARTIFACT_NAME, _frame_rows(strict_frame))
    _write_rows_csv(
        export_dir / BRIDGE_ROLE_SUMMARY_CSV_ARTIFACT_NAME,
        list(packet["role_summary"]),
    )
    (export_dir / BRIDGE_JSON_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / BRIDGE_MARKDOWN_ARTIFACT_NAME).write_text(
        _bridge_markdown(packet),
        encoding="utf-8",
    )
    return export_dir


def _role_summary(strict_frame: pl.DataFrame) -> list[dict[str, Any]]:
    summary = (
        strict_frame.group_by(["source_model_name", "selection_role"])
        .agg(
            [
                pl.len().alias("row_count"),
                pl.col("tenant_id").n_unique().alias("tenant_count"),
                pl.col("anchor_timestamp").n_unique().alias("anchor_count"),
                pl.col("regret_uah").mean().alias("mean_regret_uah"),
                pl.col("regret_uah").median().alias("median_regret_uah"),
                pl.col("decision_value_uah").mean().alias("mean_decision_value_uah"),
                pl.col("oracle_value_uah").mean().alias("mean_oracle_value_uah"),
                pl.col("safety_violation_count").sum().alias("safety_violation_count"),
            ]
        )
        .sort(["source_model_name", "selection_role"])
    )
    return _frame_rows(summary)


def _bridge_markdown(packet: dict[str, Any]) -> str:
    gate = packet["gate"]
    metrics = gate["metrics"]
    title = (
        "Negative Evidence Result"
        if packet["negative_evidence"]
        else "Offline Strategy Challenger Result"
    )
    negative_sentence = _negative_evidence_sentence(str(packet.get("bridge_scope", "")))
    lines = [
        "# DFL V2+ Residual/DT Bridge Evidence Packet",
        "",
        f"Run slug: `{packet['run_slug']}`",
        f"Bridge scope: `{packet.get('bridge_scope')}`",
        f"Dagster run: `{packet.get('dagster_run_id')}`",
        f"Asset check status: `{packet.get('asset_check_status')}`",
        "",
        "## Claim Boundary",
        "",
        "This packet is Offline Strategy Promotion evidence only. It is not live "
        "market execution, not a deployed Decision Transformer controller, and "
        "not a full DFL claim. `strict_similar_day` remains the fallback/control "
        "and `market_execution_enabled=false`.",
        "",
        f"## {title}",
        "",
        f"- Evidence check: passed - {packet['evidence_check']['description']}",
        f"- Gate decision: `{gate['decision']}`",
        f"- Gate passed: `{gate['passed']}`",
        f"- Gate description: {gate['description']}",
        f"- {negative_sentence}"
        if packet["negative_evidence"]
        else "- A residual/DT challenger beat V2+ under the strict gate.",
        f"- Market execution enabled: `{metrics['market_execution_enabled']}`",
        (
            "- Best observed challenger: "
            f"`{metrics.get('best_observed_challenger_role')}` "
            f"from `{metrics.get('best_observed_source_model_name')}`"
        ),
        (
            "- Best observed improvement vs V2+: "
            f"{metrics.get('best_observed_mean_regret_improvement_ratio_vs_v2_plus', 0.0):.2%}"
        ),
        "",
        "## Role Summary",
        "",
        "| Source | Role | Mean regret UAH | Median regret UAH | Rows |",
        "|---|---|---:|---:|---:|",
    ]
    for row in packet["role_summary"]:
        lines.append(
            "| {source} | {role} | {mean_regret:.2f} | {median_regret:.2f} | {rows} |".format(
                source=row["source_model_name"],
                role=row["selection_role"],
                mean_regret=row["mean_regret_uah"],
                median_regret=row["median_regret_uah"],
                rows=row["row_count"],
            )
        )
    lines.extend(
        [
            "",
            "## Attached Artifacts",
            "",
            f"- `{BRIDGE_JSON_ARTIFACT_NAME}`",
            f"- `{BRIDGE_MARKDOWN_ARTIFACT_NAME}`",
            f"- `{BRIDGE_ROWS_CSV_ARTIFACT_NAME}`",
            f"- `{BRIDGE_ROLE_SUMMARY_CSV_ARTIFACT_NAME}`",
            "",
        ]
    )
    return "\n".join(lines)


def _bridge_scope(strict_frame: pl.DataFrame) -> str:
    if "strategy_kind" not in strict_frame.columns or strict_frame.height == 0:
        return "unknown_v2_plus_bridge"
    strategy_kinds = {
        str(strategy_kind)
        for strategy_kind in strict_frame["strategy_kind"].unique().to_list()
    }
    if "dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark" in strategy_kinds:
        return "official_global_panel_v2_plus_teacher"
    return "compact_v2_plus_bridge"


def _negative_evidence_sentence(bridge_scope: str) -> str:
    if bridge_scope == "official_global_panel_v2_plus_teacher":
        return (
            "official global-panel V2+-teacher residual DFL / offline DT did not beat V2+."
        )
    return "compact residual DFL / offline DT did not beat V2+."


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
    "build_dfl_v2_plus_dfl_dt_bridge_packet",
    "write_dfl_v2_plus_dfl_dt_bridge_packet",
]
