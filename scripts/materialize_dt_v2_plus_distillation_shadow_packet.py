"""Materialize a DT V2+ rule-distillation shadow packet from real strict rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys
from typing import Any, Sequence

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for import_path in (PROJECT_ROOT, SRC_ROOT):
    import_path_text = str(import_path)
    if import_path_text not in sys.path:
        sys.path.insert(0, import_path_text)

from smart_arbitrage.dfl.dt_research_shadow import (  # noqa: E402
    build_dt_research_shadow_sequence_packet,
    build_dt_research_shadow_teacher_rows_from_v2_plus_strict_rows,
    run_dt_research_shadow_smoke,
    write_dt_research_shadow_sequence_packet,
)

DEFAULT_SOURCE_MODEL_NAME = "nbeatsx_official_global_panel_horizon_calibrated_v1"
SUMMARY_JSON_NAME = "dt_v2_plus_distillation_summary.json"
SUMMARY_MD_NAME = "dt_v2_plus_distillation_summary.md"
TEACHER_ROWS_CSV_NAME = "dt_research_shadow_teacher_rows.csv"
DISTILLATION_TEACHER_ROWS_CSV_NAME = "dt_v2_plus_distillation_teacher_rows.csv"
SELECTED_ROWS_CSV_NAME = "dt_v2_plus_distillation_selected_rows.csv"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a non-promotable DT V2+ rule-distillation shadow packet from "
            "real V2+ strict-row artifacts. This trains recovery of V2+ selection "
            "rule under research-only boundaries."
        )
    )
    parser.add_argument("--strict-rows-csv", type=Path, required=True)
    parser.add_argument("--regret-decomposition-csv", type=Path, default=None)
    parser.add_argument("--regret-decomposition-pickle", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--run-slug",
        default="week3_dt_v2_plus_distillation_shadow_current",
    )
    parser.add_argument("--source-model-name", default=DEFAULT_SOURCE_MODEL_NAME)
    parser.add_argument("--context-length", type=int, default=4)
    parser.add_argument("--max-epochs", type=int, default=3)
    parser.add_argument("--hidden-dim", type=int, default=48)
    parser.add_argument("--num-layers", type=int, default=2)
    parser.add_argument("--num-heads", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20260526)
    parser.add_argument(
        "--objective-kind",
        choices=(
            "cross_entropy_candidate_index",
            "decision_aware_regret_value_ranking",
            "v2_plus_rule_distillation",
        ),
        default="v2_plus_rule_distillation",
    )
    parser.add_argument("--cross-entropy-weight", type=float, default=0.0)
    parser.add_argument("--decision-aware-ranking-weight", type=float, default=0.0)
    parser.add_argument("--distillation-weight", type=float, default=1.0)
    parser.add_argument("--min-predicted-improvement-uah", type=float, default=50.0)
    parser.add_argument("--max-family-tail-risk-probability", type=float, default=0.5)
    parser.add_argument("--model-backbone", choices=("auto", "local", "hf"), default="local")
    args = parser.parse_args(argv)

    strict_rows = pl.read_csv(args.strict_rows_csv, infer_schema_length=1000)
    strict_rows = strict_rows.filter(pl.col("source_model_name") == args.source_model_name)
    if strict_rows.is_empty():
        raise ValueError(f"No strict rows found for source model: {args.source_model_name}")

    if args.regret_decomposition_csv is not None and args.regret_decomposition_pickle is not None:
        raise ValueError(
            "Use only one of --regret-decomposition-csv or "
            "--regret-decomposition-pickle."
        )
    regret_decomposition = None
    regret_decomposition_path = None
    if args.regret_decomposition_csv is not None:
        regret_decomposition_path = args.regret_decomposition_csv
        regret_decomposition = pl.read_csv(
            args.regret_decomposition_csv,
            infer_schema_length=1000,
        ).filter(pl.col("source_model_name") == args.source_model_name)
    elif args.regret_decomposition_pickle is not None:
        regret_decomposition_path = args.regret_decomposition_pickle
        regret_decomposition = _load_polars_frame(args.regret_decomposition_pickle).filter(
            pl.col("source_model_name") == args.source_model_name
        )

    teacher_rows = build_dt_research_shadow_teacher_rows_from_v2_plus_strict_rows(
        strict_rows_frame=strict_rows,
        regret_decomposition_frame=regret_decomposition,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    csv_ready_teacher_rows = _csv_ready(teacher_rows)
    teacher_rows_csv_path = args.output_dir / TEACHER_ROWS_CSV_NAME
    distillation_teacher_rows_csv_path = (
        args.output_dir / DISTILLATION_TEACHER_ROWS_CSV_NAME
    )
    csv_ready_teacher_rows.write_csv(teacher_rows_csv_path)
    csv_ready_teacher_rows.write_csv(distillation_teacher_rows_csv_path)

    packet = build_dt_research_shadow_sequence_packet(
        teacher_rows_frame=teacher_rows,
        run_slug=args.run_slug,
        context_length=args.context_length,
    )
    sequence_paths = write_dt_research_shadow_sequence_packet(
        output_dir=args.output_dir,
        packet=packet,
        teacher_rows_frame=teacher_rows,
    )
    smoke_paths = run_dt_research_shadow_smoke(
        sequence_npz_path=sequence_paths["sequence_npz"],
        output_dir=args.output_dir,
        max_epochs=args.max_epochs,
        hidden_dim=args.hidden_dim,
        num_layers=args.num_layers,
        num_heads=args.num_heads,
        seed=args.seed,
        model_backbone=args.model_backbone,
        objective_kind=args.objective_kind,
        cross_entropy_weight=args.cross_entropy_weight,
        decision_aware_ranking_weight=args.decision_aware_ranking_weight,
        distillation_weight=args.distillation_weight,
        min_predicted_improvement_uah=args.min_predicted_improvement_uah,
        max_family_tail_risk_probability=args.max_family_tail_risk_probability,
    )
    selected_rows_csv_path = _write_selected_rows_csv(
        selected_preview_json_path=smoke_paths["selected_preview_json"],
        output_dir=args.output_dir,
    )
    summary = _summary(
        run_slug=args.run_slug,
        source_model_name=args.source_model_name,
        strict_rows_path=args.strict_rows_csv,
        regret_decomposition_path=regret_decomposition_path,
        teacher_rows=teacher_rows,
        sequence_paths=sequence_paths,
        smoke_paths=smoke_paths,
        selected_rows_csv_path=selected_rows_csv_path,
    )
    summary_json_path = args.output_dir / SUMMARY_JSON_NAME
    summary_md_path = args.output_dir / SUMMARY_MD_NAME
    summary_json_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_md_path.write_text(_summary_markdown(summary), encoding="utf-8")
    json.dump(
        {
            "summary_json": str(summary_json_path),
            "summary_markdown": str(summary_md_path),
            "teacher_rows_csv": str(teacher_rows_csv_path),
            "teacher_rows_csv_alias": str(distillation_teacher_rows_csv_path),
            "selected_rows_csv": str(selected_rows_csv_path),
            "sequence_summary_json": str(sequence_paths["summary_json"]),
            "smoke_summary_json": str(smoke_paths["summary_json"]),
            "evaluation_summary_json": str(smoke_paths["evaluation_summary_json"]),
            "selected_preview_json": str(smoke_paths["selected_preview_json"]),
            "objective_kind": args.objective_kind,
            "market_execution_enabled": False,
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")
    return 0


def _summary(
    *,
    run_slug: str,
    source_model_name: str,
    strict_rows_path: Path,
    regret_decomposition_path: Path | None,
    teacher_rows: pl.DataFrame,
    sequence_paths: dict[str, Path],
    smoke_paths: dict[str, Path],
    selected_rows_csv_path: Path,
) -> dict[str, Any]:
    evaluation = json.loads(
        smoke_paths["evaluation_summary_json"].read_text(encoding="utf-8")
    )
    controls = _final_holdout_controls(teacher_rows)
    metrics = evaluation["evaluation_metrics"]
    return {
        "run_slug": run_slug,
        "claim_scope": "dt_v2_plus_distillation_shadow_not_promotable",
        "source_model_name": source_model_name,
        "input_artifacts": {
            "strict_rows_csv": str(strict_rows_path),
            "regret_decomposition_path": (
                None if regret_decomposition_path is None else str(regret_decomposition_path)
            ),
        },
        "method_note": (
            "Rule-distillation shadow trains DT to recover V2+ selector rows from "
            "mirrored research rows. This is diagnostic, not out-of-sample promotion."
        ),
        "final_holdout_controls": controls,
        "dt_evaluation_metrics": metrics,
        "regret_value_deltas": evaluation["regret_value_deltas"],
        "boundary": {
            "real_v2_plus_comparator": True,
            "mirrored_training_rows": True,
            "out_of_sample_generalization_claim": False,
            "dt_promotion_gate_passed": False,
            "market_execution_enabled": False,
            "not_market_execution": True,
            "no_dashboard_api_default_switch": True,
            "v2_plus_remains_default": True,
        },
        "attached_artifacts": {
            "teacher_rows_csv": TEACHER_ROWS_CSV_NAME,
            "teacher_rows_csv_alias": DISTILLATION_TEACHER_ROWS_CSV_NAME,
            "selected_rows_csv": selected_rows_csv_path.name,
            "sequence_summary_json": sequence_paths["summary_json"].name,
            "sequence_validation_json": sequence_paths["validation_json"].name,
            "smoke_summary_json": smoke_paths["summary_json"].name,
            "evaluation_summary_json": smoke_paths["evaluation_summary_json"].name,
            "evaluation_validation_json": smoke_paths["evaluation_validation_json"].name,
            "selected_preview_json": smoke_paths["selected_preview_json"].name,
            "summary_json": SUMMARY_JSON_NAME,
            "summary_markdown": SUMMARY_MD_NAME,
        },
    }


def _write_selected_rows_csv(
    *,
    selected_preview_json_path: Path,
    output_dir: Path,
) -> Path:
    selected_preview = json.loads(selected_preview_json_path.read_text(encoding="utf-8"))
    preview_rows = selected_preview.get("preview_rows")
    if not isinstance(preview_rows, list):
        preview_rows = []
    frame = pl.DataFrame(preview_rows) if preview_rows else pl.DataFrame([])
    selected_rows_csv_path = output_dir / SELECTED_ROWS_CSV_NAME
    frame.write_csv(selected_rows_csv_path)
    return selected_rows_csv_path


def _load_polars_frame(path: Path) -> pl.DataFrame:
    with path.open("rb") as file:
        value = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


def _final_holdout_controls(teacher_rows: pl.DataFrame) -> dict[str, dict[str, Any]]:
    final_rows = teacher_rows.filter(pl.col("split_name") == "final_holdout")
    summary = (
        final_rows.group_by("dt_schedule_family_target")
        .agg(
            pl.len().alias("row_count"),
            pl.col("tenant_id").n_unique().alias("tenant_count"),
            pl.col("anchor_timestamp").n_unique().alias("anchor_count"),
            pl.col("regret_uah").mean().alias("mean_regret_uah"),
            pl.col("regret_uah").median().alias("median_regret_uah"),
            pl.col("schedule_value_uah").mean().alias("mean_value_uah"),
            pl.col("oracle_value_uah").mean().alias("mean_oracle_value_uah"),
        )
        .sort("dt_schedule_family_target")
    )
    return {
        str(row["dt_schedule_family_target"]): {
            key: row[key]
            for key in row
            if key != "dt_schedule_family_target"
        }
        for row in summary.iter_rows(named=True)
    }


def _summary_markdown(summary: dict[str, Any]) -> str:
    metrics = summary["dt_evaluation_metrics"]
    recovery = metrics.get("v2_plus_rule_recovery_rate", 0.0)
    raw_delta = metrics.get("raw_distilled_argmax_minus_v2_plus_mean_regret_uah", 0.0)
    win_loss_tie = metrics.get("raw_distilled_argmax_win_loss_tie_vs_v2_plus", {})
    lines = [
        "# DT V2+ Rule Distillation Shadow",
        "",
        f"Run slug: `{summary['run_slug']}`",
        "",
        summary["method_note"],
        "",
        "## Recovery Metrics",
        "",
        f"- V2+ rule recovery rate: `{float(recovery):.4f}`",
        "- Raw distilled argmax mean regret: "
        f"`{float(metrics.get('raw_distilled_argmax_mean_regret_uah', 0.0)):.2f}` UAH.",
        "- Raw distilled argmax median regret: "
        f"`{float(metrics.get('raw_distilled_argmax_median_regret_uah', 0.0)):.2f}` UAH.",
        f"- Raw distilled argmax minus V2+ mean regret: `{float(raw_delta):.2f}` UAH.",
        "- Raw distilled argmax win/loss/tie vs V2+: "
        f"`{int(win_loss_tie.get('wins', 0))}` / "
        f"`{int(win_loss_tie.get('losses', 0))}` / "
        f"`{int(win_loss_tie.get('ties', 0))}`.",
        "",
        "## Boundary",
        "",
        "- `market_execution_enabled=false`",
        "- `dt_lava_ready=false`",
        "- `permits_model_training=false`",
        "- Research diagnostic shadow only; V2+ remains default comparator",
    ]
    return "\n".join(lines) + "\n"


def _csv_ready(frame: pl.DataFrame) -> pl.DataFrame:
    vector_columns = [
        "forecast_price_uah_mwh_vector",
        "actual_price_uah_mwh_vector",
        "dispatch_mw_vector",
        "soc_fraction_vector",
    ]
    expressions = []
    for column in vector_columns:
        if column in frame.columns:
            expressions.append(
                pl.col(column)
                .map_elements(_json_text, return_dtype=pl.String)
                .alias(column)
            )
    return frame.with_columns(expressions) if expressions else frame


def _json_text(value: object) -> str:
    if isinstance(value, pl.Series):
        return json.dumps(value.to_list())
    return json.dumps(value)


if __name__ == "__main__":
    raise SystemExit(main())
