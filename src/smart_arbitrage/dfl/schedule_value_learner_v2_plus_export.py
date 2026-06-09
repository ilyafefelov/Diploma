"""Local evidence export for Schedule/Value Learner V2+ comparisons."""

from __future__ import annotations

from datetime import date, datetime, timezone
import csv
import json
from pathlib import Path
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.schedule_value_learner_v2_plus import (
    evaluate_dfl_schedule_value_learner_v2_plus_gate,
    validate_dfl_schedule_value_learner_v2_plus_evidence,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus_robustness import (
    evaluate_dfl_schedule_value_learner_v2_plus_robustness_gate,
    validate_dfl_schedule_value_learner_v2_plus_robustness_evidence,
)

COMPARISON_JSON_ARTIFACT_NAME: Final[str] = (
    "dfl_schedule_value_learner_v2_plus_comparison.json"
)
COMPARISON_MARKDOWN_ARTIFACT_NAME: Final[str] = (
    "dfl_schedule_value_learner_v2_plus_comparison.md"
)
ROLE_SUMMARY_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_schedule_value_learner_v2_plus_role_summary.csv"
)
TRACE_CSV_ARTIFACT_NAME: Final[str] = "dfl_schedule_value_learner_v2_plus_trace.csv"
FAILURE_SUMMARY_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_schedule_value_regret_decomposition_summary.csv"
)
STRICT_ROWS_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_schedule_value_learner_v2_plus_strict_rows.csv"
)
ROLLING_ROBUSTNESS_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_schedule_value_learner_v2_plus_rolling_robustness.csv"
)


def build_dfl_schedule_value_learner_v2_plus_comparison_packet(
    *,
    run_slug: str,
    strict_frame: pl.DataFrame,
    learner_frame: pl.DataFrame,
    regret_decomposition_frame: pl.DataFrame,
    rolling_robustness_frame: pl.DataFrame | None = None,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
) -> dict[str, Any]:
    """Build a concise V2+ comparison packet only after checks pass."""

    evidence = validate_dfl_schedule_value_learner_v2_plus_evidence(strict_frame)
    if not evidence.passed:
        raise ValueError(f"V2+ evidence check failed; refusing export: {evidence.description}")
    gate = evaluate_dfl_schedule_value_learner_v2_plus_gate(strict_frame)
    if not gate.passed:
        raise ValueError(
            f"V2+ strict gate failed; refusing export: {gate.decision}: {gate.description}"
        )
    trace_rows = _learner_trace_rows(learner_frame)
    packet: dict[str, Any] = {
        "run_slug": run_slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "claim_boundary": {
            "offline_strategy_promotion_only": True,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "strict_fallback": "strict_similar_day",
            "frozen_reference": (
                "Schedule/Value Learner V2 remains preserved as prior thesis evidence; "
                "V2+ is a stronger comparison packet only when the unchanged gate passes."
            ),
        },
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
        "source_role_summary": _source_role_summary(strict_frame),
        "learner_trace": trace_rows,
        "selected_final_family_counts_by_source": _selected_family_counts(trace_rows),
        "failure_mode_summary": _failure_mode_summary(regret_decomposition_frame),
        "attached_artifacts": {
            "comparison_json": COMPARISON_JSON_ARTIFACT_NAME,
            "comparison_markdown": COMPARISON_MARKDOWN_ARTIFACT_NAME,
            "role_summary_csv": ROLE_SUMMARY_CSV_ARTIFACT_NAME,
            "trace_csv": TRACE_CSV_ARTIFACT_NAME,
            "failure_summary_csv": FAILURE_SUMMARY_CSV_ARTIFACT_NAME,
            "strict_rows_csv": STRICT_ROWS_CSV_ARTIFACT_NAME,
        },
    }
    if rolling_robustness_frame is not None:
        packet["rolling_robustness"] = _rolling_robustness_packet(
            rolling_robustness_frame
        )
        packet["attached_artifacts"]["rolling_robustness_csv"] = (
            ROLLING_ROBUSTNESS_CSV_ARTIFACT_NAME
        )
    return packet


def write_dfl_schedule_value_learner_v2_plus_comparison_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
    strict_frame: pl.DataFrame,
    rolling_robustness_frame: pl.DataFrame | None = None,
) -> Path:
    """Write local JSON, Markdown, and CSV evidence artifacts."""

    run_slug = str(packet["run_slug"])
    export_dir = output_root / run_slug
    export_dir.mkdir(parents=True, exist_ok=True)
    _write_rows_csv(export_dir / STRICT_ROWS_CSV_ARTIFACT_NAME, _frame_rows(strict_frame))
    _write_rows_csv(
        export_dir / ROLE_SUMMARY_CSV_ARTIFACT_NAME,
        list(packet["source_role_summary"]),
    )
    _write_rows_csv(export_dir / TRACE_CSV_ARTIFACT_NAME, list(packet["learner_trace"]))
    _write_rows_csv(
        export_dir / FAILURE_SUMMARY_CSV_ARTIFACT_NAME,
        list(packet["failure_mode_summary"]),
    )
    if rolling_robustness_frame is not None:
        _write_rows_csv(
            export_dir / ROLLING_ROBUSTNESS_CSV_ARTIFACT_NAME,
            _frame_rows(rolling_robustness_frame),
        )
    (export_dir / COMPARISON_JSON_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / COMPARISON_MARKDOWN_ARTIFACT_NAME).write_text(
        _comparison_markdown(packet),
        encoding="utf-8",
    )
    return export_dir


def _rolling_robustness_packet(robustness_frame: pl.DataFrame) -> dict[str, Any]:
    evidence = validate_dfl_schedule_value_learner_v2_plus_robustness_evidence(
        robustness_frame
    )
    if not evidence.passed:
        raise ValueError(
            "V2+ robustness evidence check failed; refusing export: "
            f"{evidence.description}"
        )
    gate = evaluate_dfl_schedule_value_learner_v2_plus_robustness_gate(
        robustness_frame
    )
    if gate.decision != "v2_plus_robust_research_challenger":
        raise ValueError(
            f"V2+ robustness gate failed; refusing export: "
            f"{gate.decision}: {gate.description}"
        )
    return {
        "evidence_check": {
            "passed": evidence.passed,
            "description": evidence.description,
            "metadata": evidence.metadata,
        },
        "gate": {
            "decision": gate.decision,
            "description": gate.description,
            "metrics": gate.metrics,
        },
    }


def _source_role_summary(strict_frame: pl.DataFrame) -> list[dict[str, Any]]:
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


def _learner_trace_rows(learner_frame: pl.DataFrame) -> list[dict[str, Any]]:
    columns = [
        "source_model_name",
        "tenant_id",
        "selected_weight_profile_name",
        "fallback_to_v2",
        "train_anchor_count",
        "final_holdout_anchor_count",
        "v2_train_mean_regret_uah",
        "v2_plus_candidate_train_mean_regret_uah",
        "selected_train_mean_regret_uah",
        "v2_final_mean_regret_uah",
        "v2_plus_candidate_final_mean_regret_uah",
        "selected_final_mean_regret_uah",
        "final_mean_regret_improvement_ratio_vs_v2",
        "final_mean_regret_improvement_ratio_vs_strict",
        "selected_final_family_counts",
    ]
    missing = sorted(set(columns).difference(learner_frame.columns))
    if missing:
        raise ValueError(f"V2+ learner trace is missing required columns: {missing}")
    return _frame_rows(learner_frame.select(columns))


def _failure_mode_summary(frame: pl.DataFrame) -> list[dict[str, Any]]:
    required = {
        "source_model_name",
        "failure_mode",
        "v2_regret_uah",
        "best_candidate_regret_uah",
        "regret_gap_v2_to_best_candidate_uah",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"V2+ regret decomposition is missing columns: {missing}")
    summary = (
        frame.group_by(["source_model_name", "failure_mode"])
        .agg(
            [
                pl.len().alias("anchor_count"),
                pl.col("v2_regret_uah").mean().alias("mean_v2_regret_uah"),
                pl.col("best_candidate_regret_uah")
                .mean()
                .alias("mean_best_candidate_regret_uah"),
                pl.col("regret_gap_v2_to_best_candidate_uah")
                .mean()
                .alias("mean_v2_to_best_gap_uah"),
            ]
        )
        .sort(["source_model_name", "failure_mode"])
    )
    return _frame_rows(summary)


def _selected_family_counts(
    learner_rows: list[dict[str, Any]],
) -> dict[str, dict[str, int]]:
    counts_by_source: dict[str, dict[str, int]] = {}
    for row in learner_rows:
        source_model_name = str(row["source_model_name"])
        counts_by_source.setdefault(source_model_name, {})
        family_counts = row.get("selected_final_family_counts", {})
        if not isinstance(family_counts, dict):
            continue
        for family, count in family_counts.items():
            source_counts = counts_by_source[source_model_name]
            source_counts[str(family)] = source_counts.get(str(family), 0) + int(
                count or 0
            )
    return counts_by_source


def _comparison_markdown(packet: dict[str, Any]) -> str:
    gate = packet["gate"]
    metrics = gate["metrics"]
    lines = [
        "# DFL Schedule/Value Learner V2+ Comparison Packet",
        "",
        f"Run slug: `{packet['run_slug']}`",
        f"Dagster run: `{packet.get('dagster_run_id')}`",
        "",
        "## Claim Boundary",
        "",
        "This packet is Offline Strategy Promotion evidence only. It is not live "
        "market execution, not a deployed Decision Transformer controller, and "
        "not a full end-to-end DFL claim. `strict_similar_day` remains the frozen "
        "fallback/control and `market_execution_enabled=false`.",
        "",
        "## Gate Result",
        "",
        f"- Evidence check: passed - {packet['evidence_check']['description']}",
        f"- Strict gate: passed - {gate['description']}",
        f"- Best source: `{metrics['best_source_model_name']}`",
        (
            "- Validation coverage: "
            f"{metrics['tenant_count']} tenants, "
            f"{metrics['validation_tenant_anchor_count']} tenant-anchors"
        ),
        f"- Strict mean regret: {metrics['strict_mean_regret_uah']:.2f} UAH",
        f"- V2 mean regret: {metrics['v2_mean_regret_uah']:.2f} UAH",
        f"- V2+ selected mean regret: {metrics['selected_mean_regret_uah']:.2f} UAH",
        (
            "- Improvement vs strict: "
            f"{metrics['mean_regret_improvement_ratio_vs_strict']:.2%}"
        ),
        (
            "- Improvement vs raw: "
            f"{metrics['mean_regret_improvement_ratio_vs_raw']:.2%}"
        ),
        (
            "- Improvement vs frozen V2: "
            f"{metrics['mean_regret_improvement_ratio_vs_v2']:.2%}"
        ),
        f"- Market execution enabled: `{metrics['market_execution_enabled']}`",
        "",
        "## Source Summary",
        "",
        "| Source | Strict mean | Raw mean | V2 mean | V2+ mean | "
        "V2+ improvement vs strict | V2+ improvement vs V2 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for summary in metrics["model_summaries"]:
        lines.append(
            "| {source} | {strict:.2f} | {raw:.2f} | {v2_mean:.2f} | "
            "{selected:.2f} | {strict_gain:.2%} | {v2_gain:.2%} |".format(
                source=summary["source_model_name"],
                strict=summary["strict_mean_regret_uah"],
                raw=summary["raw_mean_regret_uah"],
                v2_mean=summary["v2_mean_regret_uah"],
                selected=summary["selected_mean_regret_uah"],
                strict_gain=summary["mean_regret_improvement_ratio_vs_strict"],
                v2_gain=summary["mean_regret_improvement_ratio_vs_v2"],
            )
        )
    lines.extend(
        [
            "",
            "## Attached Artifacts",
            "",
            f"- `{COMPARISON_JSON_ARTIFACT_NAME}`",
            f"- `{ROLE_SUMMARY_CSV_ARTIFACT_NAME}`",
            f"- `{TRACE_CSV_ARTIFACT_NAME}`",
            f"- `{FAILURE_SUMMARY_CSV_ARTIFACT_NAME}`",
            f"- `{STRICT_ROWS_CSV_ARTIFACT_NAME}`",
        ]
    )
    if "rolling_robustness" in packet:
        robustness = packet["rolling_robustness"]
        lines.extend(
            [
                "",
                "## Rolling Robustness",
                "",
                f"- Gate: `{robustness['gate']['decision']}`",
                "- Market execution enabled: "
                f"`{robustness['gate']['metrics']['market_execution_enabled']}`",
                "",
                "| Source | Rolling windows | Result |",
                "|---|---:|---|",
            ]
        )
        for summary in robustness["gate"]["metrics"]["model_summaries"]:
            lines.append(
                "| {source} | {passes} / {windows} | {result} |".format(
                    source=summary["source_model_name"],
                    passes=summary["v2_plus_window_count"],
                    windows=summary["window_count"],
                    result=(
                        "robust V2+ research challenger"
                        if summary["robust_research_challenger"]
                        else "not robust"
                    ),
                )
            )
        lines.extend(["", f"- `{ROLLING_ROBUSTNESS_CSV_ARTIFACT_NAME}`"])
    return "\n".join(lines) + "\n"


def _write_rows_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in fieldnames})


def _frame_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return [dict(row) for row in frame.iter_rows(named=True)]


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
    "build_dfl_schedule_value_learner_v2_plus_comparison_packet",
    "write_dfl_schedule_value_learner_v2_plus_comparison_packet",
]
