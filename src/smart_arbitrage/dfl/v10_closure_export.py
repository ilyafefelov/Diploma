"""Local evidence export for V10 tail-risk transfer closure packets."""

from __future__ import annotations

from datetime import date, datetime, timezone
import csv
import json
from pathlib import Path
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.v2_plus_dfl_dt_bridge import V2_PLUS_HEADLINE_BASELINE_METRICS

V10_CLOSURE_JSON_ARTIFACT_NAME: Final[str] = (
    "dfl_v10_tail_risk_transfer_closure_summary.json"
)
V10_CLOSURE_MARKDOWN_ARTIFACT_NAME: Final[str] = (
    "dfl_v10_tail_risk_transfer_closure_summary.md"
)
V10_AUDIT_ROWS_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_v10_tail_risk_transfer_audit_rows.csv"
)
V10_DECISION_ROWS_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_v10_learning_ceiling_decision_rows.csv"
)
V10_FAILURE_CLASS_SUMMARY_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_v10_failure_class_summary.csv"
)


def build_dfl_v10_tail_risk_transfer_closure_packet(
    *,
    run_slug: str,
    tail_risk_audit_frame: pl.DataFrame,
    learning_ceiling_decision_frame: pl.DataFrame,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
    asset_check_status: str | None = None,
) -> dict[str, Any]:
    """Build a V10 closure packet from the materialized audit and decision rows."""

    _validate_v10_closure_inputs(tail_risk_audit_frame, learning_ceiling_decision_frame)
    decision_summary = _decision_summary(learning_ceiling_decision_frame)
    return {
        "run_slug": run_slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "asset_check_status": asset_check_status,
        "negative_evidence": not bool(decision_summary["dt_lava_ready"]),
        "claim_boundary": {
            "offline_strategy_promotion_only": True,
            "not_full_dfl": True,
            "not_deployed_decision_transformer_control": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "strict_fallback": "strict_similar_day",
            "comparator": "Schedule/Value Learner V2+",
            "no_european_training_rows": True,
            "no_dashboard_api_default_switch": True,
        },
        "headline_baseline": {
            **dict(V2_PLUS_HEADLINE_BASELINE_METRICS),
            "calibrated_v2_plus_median_regret_uah": 67.30,
        },
        "learning_ceiling_decision": decision_summary,
        "row_counts": _row_counts(tail_risk_audit_frame),
        "failure_class_summary": _failure_class_summary(tail_risk_audit_frame),
        "attached_artifacts": {
            "summary_json": V10_CLOSURE_JSON_ARTIFACT_NAME,
            "summary_markdown": V10_CLOSURE_MARKDOWN_ARTIFACT_NAME,
            "tail_risk_audit_rows_csv": V10_AUDIT_ROWS_CSV_ARTIFACT_NAME,
            "learning_ceiling_decision_rows_csv": V10_DECISION_ROWS_CSV_ARTIFACT_NAME,
            "failure_class_summary_csv": V10_FAILURE_CLASS_SUMMARY_CSV_ARTIFACT_NAME,
        },
    }


def write_dfl_v10_tail_risk_transfer_closure_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
    tail_risk_audit_frame: pl.DataFrame,
    learning_ceiling_decision_frame: pl.DataFrame,
) -> Path:
    """Write local JSON, Markdown, and CSV V10 closure artifacts."""

    export_dir = output_root / str(packet["run_slug"])
    export_dir.mkdir(parents=True, exist_ok=True)
    _write_rows_csv(
        export_dir / V10_AUDIT_ROWS_CSV_ARTIFACT_NAME,
        _frame_rows(tail_risk_audit_frame),
    )
    _write_rows_csv(
        export_dir / V10_DECISION_ROWS_CSV_ARTIFACT_NAME,
        _frame_rows(learning_ceiling_decision_frame),
    )
    _write_rows_csv(
        export_dir / V10_FAILURE_CLASS_SUMMARY_CSV_ARTIFACT_NAME,
        list(packet["failure_class_summary"]),
    )
    (export_dir / V10_CLOSURE_JSON_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / V10_CLOSURE_MARKDOWN_ARTIFACT_NAME).write_text(
        _closure_markdown(packet),
        encoding="utf-8",
    )
    return export_dir


def _validate_v10_closure_inputs(
    tail_risk_audit_frame: pl.DataFrame,
    learning_ceiling_decision_frame: pl.DataFrame,
) -> None:
    _require_columns(
        tail_risk_audit_frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "split_name",
                "candidate_key",
                "label_v10_material_safe_switch",
                "label_v10_tail_risk_loss",
                "v10_transfer_failure_class",
                "candidate_regret_uah",
                "v2_plus_regret_uah",
                "market_execution_enabled",
            }
        ),
        frame_name="V10 tail-risk transfer audit frame",
    )
    _require_columns(
        learning_ceiling_decision_frame,
        frozenset(
            {
                "source_model_name",
                "final_generated_candidate_count",
                "final_generated_non_tail_risk_material_safe_switch_count",
                "final_generated_tail_risk_count",
                "v10_learning_ceiling_decision",
                "dt_lava_ready",
                "recommended_next_branch",
                "market_execution_enabled",
            }
        ),
        frame_name="V10 learning-ceiling decision frame",
    )
    if tail_risk_audit_frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("V10 closure packet refuses market execution audit rows.")
    if learning_ceiling_decision_frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError("V10 closure packet refuses market execution decision rows.")


def _decision_summary(decision_frame: pl.DataFrame) -> dict[str, Any]:
    if decision_frame.height == 0:
        raise ValueError("V10 closure packet requires a learning-ceiling decision row.")
    rows = _frame_rows(decision_frame)
    if len(rows) == 1:
        return rows[0]
    return {
        "source_model_name": "multiple",
        "v10_learning_ceiling_decision": ",".join(
            sorted({str(row["v10_learning_ceiling_decision"]) for row in rows})
        ),
        "dt_lava_ready": any(bool(row["dt_lava_ready"]) for row in rows),
        "recommended_next_branch": ",".join(
            sorted({str(row["recommended_next_branch"]) for row in rows})
        ),
        "final_generated_candidate_count": sum(
            int(row["final_generated_candidate_count"]) for row in rows
        ),
        "final_generated_non_tail_risk_material_safe_switch_count": sum(
            int(row["final_generated_non_tail_risk_material_safe_switch_count"])
            for row in rows
        ),
        "final_generated_tail_risk_count": sum(
            int(row["final_generated_tail_risk_count"]) for row in rows
        ),
    }


def _row_counts(audit_frame: pl.DataFrame) -> dict[str, Any]:
    final = audit_frame.filter(pl.col("split_name") == "final_holdout")
    return {
        "transfer_audit_rows": audit_frame.height,
        "final_generated_candidate_rows": final.height,
        "final_non_tail_risk_safe_switch_rows": final.filter(
            pl.col("label_v10_material_safe_switch")
            & ~pl.col("label_v10_tail_risk_loss")
        ).height,
        "final_tail_risk_rows": final.filter(pl.col("label_v10_tail_risk_loss")).height,
    }


def _failure_class_summary(audit_frame: pl.DataFrame) -> list[dict[str, Any]]:
    summary = (
        audit_frame.group_by(["split_name", "v10_transfer_failure_class"])
        .agg(
            [
                pl.len().alias("row_count"),
                pl.col("candidate_regret_uah").mean().alias("mean_candidate_regret_uah"),
                pl.col("v2_plus_regret_uah").mean().alias("mean_v2_plus_regret_uah"),
            ]
        )
        .sort(["split_name", "v10_transfer_failure_class"])
    )
    return _frame_rows(summary)


def _closure_markdown(packet: dict[str, Any]) -> str:
    decision = packet["learning_ceiling_decision"]
    row_counts = packet["row_counts"]
    title = (
        "Negative Evidence Result"
        if packet["negative_evidence"]
        else "DT/LAVA Readiness Result"
    )
    lines = [
        "# DFL V10 Tail-Risk Transfer Closure Packet",
        "",
        f"Run slug: `{packet['run_slug']}`",
        f"Dagster run: `{packet.get('dagster_run_id')}`",
        f"Asset check status: `{packet.get('asset_check_status')}`",
        "",
        "## Claim Boundary",
        "",
        "This packet is Offline Strategy Promotion evidence only. It is not live "
        "market execution, not a deployed Decision Transformer controller, and "
        "not a dashboard/API default switch. `market_execution_enabled=false`.",
        "",
        f"## {title}",
        "",
        (
            "- Frozen comparator: calibrated Ukrainian-only V2+ mean regret "
            f"`{packet['headline_baseline']['calibrated_v2_plus_mean_regret_uah']}` "
            "UAH, median regret `67.30` UAH, rolling `4 / 4`."
        ),
        (
            "- Learning-ceiling decision: "
            f"`{decision['v10_learning_ceiling_decision']}`."
        ),
        f"- Recommended next branch: `{decision['recommended_next_branch']}`.",
        f"- DT/LAVA ready: `{decision['dt_lava_ready']}`.",
        (
            "- Final generated rows: "
            f"`{row_counts['final_generated_candidate_rows']}`; "
            f"`{row_counts['final_tail_risk_rows']}` tail-risk rows; "
            f"{row_counts['final_non_tail_risk_safe_switch_rows']} final "
            "non-tail-risk safe switches."
        ),
        "",
        "Interpretation: V10 is valid negative evidence. It shows that "
        "train/prior safe-looking templates did not transfer safely to final "
        "holdout, so the next work is Ukrainian context/data acquisition or "
        "lower-tail-risk candidate design, not another selector or DT variant.",
        "",
        "## Failure Class Summary",
        "",
        "| Split | Failure class | Rows | Mean candidate regret UAH | Mean V2+ regret UAH |",
        "|---|---|---:|---:|---:|",
    ]
    for row in packet["failure_class_summary"]:
        lines.append(
            "| {split} | {failure} | {rows} | {candidate:.2f} | {v2:.2f} |".format(
                split=row["split_name"],
                failure=row["v10_transfer_failure_class"],
                rows=row["row_count"],
                candidate=row["mean_candidate_regret_uah"],
                v2=row["mean_v2_plus_regret_uah"],
            )
        )
    lines.extend(
        [
            "",
            "## Attached Artifacts",
            "",
            f"- `{V10_CLOSURE_JSON_ARTIFACT_NAME}`",
            f"- `{V10_CLOSURE_MARKDOWN_ARTIFACT_NAME}`",
            f"- `{V10_AUDIT_ROWS_CSV_ARTIFACT_NAME}`",
            f"- `{V10_DECISION_ROWS_CSV_ARTIFACT_NAME}`",
            f"- `{V10_FAILURE_CLASS_SUMMARY_CSV_ARTIFACT_NAME}`",
            "",
        ]
    )
    return "\n".join(lines)


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


def _require_columns(
    frame: pl.DataFrame,
    required: frozenset[str],
    *,
    frame_name: str,
) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} missing required columns: {missing}")


__all__ = [
    "build_dfl_v10_tail_risk_transfer_closure_packet",
    "write_dfl_v10_tail_risk_transfer_closure_packet",
]
