"""Materialize credentialless DT research-shadow sequence and smoke artifacts."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import json
from pathlib import Path
import sys

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for import_path in (PROJECT_ROOT, SRC_ROOT):
    import_path_text = str(import_path)
    if import_path_text not in sys.path:
        sys.path.insert(0, import_path_text)

from smart_arbitrage.dfl.dt_research_shadow import (  # noqa: E402
    build_dt_research_shadow_teacher_rows_from_candidate_library,
    build_dt_research_shadow_sequence_packet,
    run_dt_research_shadow_smoke,
    write_dt_research_shadow_sequence_packet,
)

DT_RESEARCH_SHADOW_SUMMARY_JSON_NAME = "dt_research_shadow_decision_aware_summary.json"
DT_RESEARCH_SHADOW_SUMMARY_MD_NAME = "dt_research_shadow_decision_aware_summary.md"
DT_RESEARCH_SHADOW_TEACHER_ROWS_CSV_NAME = "dt_research_shadow_teacher_rows.csv"
DT_RESEARCH_SHADOW_SELECTED_ROWS_CSV_NAME = "dt_research_shadow_selected_rows.csv"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a credentialless DT research-shadow sequence dataset from "
            "V13 teacher rows and run a tiny local transformer smoke. This uses "
            "chronological delivery-time splits, keeps publication receipts "
            "unverified, and never promotes DT or market execution."
        )
    )
    parser.add_argument("--teacher-rows-csv", type=Path, required=True)
    parser.add_argument(
        "--candidate-library-csv",
        type=Path,
        action="append",
        default=[],
        help=(
            "Optional credentialless candidate-library CSV to adapt into "
            "research-shadow DT context rows. May be provided more than once; "
            "adapted rows remain non-promotable and non-executable."
        ),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-slug", default="week3_dt_research_shadow_current")
    parser.add_argument("--context-length", type=int, default=8)
    parser.add_argument("--max-sequences", type=int, default=None)
    parser.add_argument("--max-epochs", type=int, default=1)
    parser.add_argument("--hidden-dim", type=int, default=32)
    parser.add_argument("--num-layers", type=int, default=1)
    parser.add_argument("--num-heads", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20260525)
    parser.add_argument(
        "--objective-kind",
        choices=(
            "cross_entropy_candidate_index",
            "decision_aware_regret_value_ranking",
            "v2_plus_rule_distillation",
        ),
        default="decision_aware_regret_value_ranking",
    )
    parser.add_argument("--cross-entropy-weight", type=float, default=1.0)
    parser.add_argument("--decision-aware-ranking-weight", type=float, default=1.0)
    parser.add_argument("--distillation-weight", type=float, default=1.0)
    parser.add_argument("--min-predicted-improvement-uah", type=float, default=50.0)
    parser.add_argument("--max-family-tail-risk-probability", type=float, default=0.5)
    parser.add_argument(
        "--model-backbone",
        choices=("auto", "local", "hf"),
        default="auto",
        help=(
            "DT smoke backbone selector. 'auto' uses Hugging Face "
            "DecisionTransformer when importable and otherwise records an "
            "explicit local-wrapper fallback."
        ),
    )
    parser.add_argument(
        "--save-checkpoint",
        action="store_true",
        help=(
            "Persist a non-promotable research-shadow model checkpoint and run "
            "a load/forward smoke. This does not change V13 training permission "
            "or market-execution gates."
        ),
    )
    args = parser.parse_args(argv)

    teacher_rows = pl.read_csv(args.teacher_rows_csv, try_parse_dates=True)
    adapted_row_count = 0
    candidate_library_paths: list[str] = []
    row_frames = [teacher_rows]
    for candidate_library_csv in args.candidate_library_csv:
        candidate_library_frame = pl.read_csv(
            candidate_library_csv,
            try_parse_dates=True,
        )
        adapted_rows = build_dt_research_shadow_teacher_rows_from_candidate_library(
            candidate_library_frame=candidate_library_frame,
        )
        adapted_row_count += adapted_rows.height
        candidate_library_paths.append(str(candidate_library_csv))
        row_frames.append(adapted_rows)
    teacher_rows = pl.concat(row_frames, how="diagonal_relaxed")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    teacher_rows_csv_path = args.output_dir / DT_RESEARCH_SHADOW_TEACHER_ROWS_CSV_NAME
    _csv_ready(teacher_rows).write_csv(teacher_rows_csv_path)

    packet = build_dt_research_shadow_sequence_packet(
        teacher_rows_frame=teacher_rows,
        run_slug=args.run_slug,
        context_length=args.context_length,
        max_sequences=args.max_sequences,
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
        save_checkpoint=args.save_checkpoint,
    )
    selected_rows_csv_path = _write_selected_rows_csv(
        selected_preview_json_path=smoke_paths["selected_preview_json"],
        output_dir=args.output_dir,
    )
    summary = _decision_aware_summary(
        run_slug=args.run_slug,
        objective_kind=args.objective_kind,
        packet=packet,
        sequence_paths=sequence_paths,
        smoke_paths=smoke_paths,
        teacher_rows_csv_path=teacher_rows_csv_path,
        selected_rows_csv_path=selected_rows_csv_path,
    )
    summary_json_path = args.output_dir / DT_RESEARCH_SHADOW_SUMMARY_JSON_NAME
    summary_md_path = args.output_dir / DT_RESEARCH_SHADOW_SUMMARY_MD_NAME
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
            "selected_rows_csv": str(selected_rows_csv_path),
            "sequence_summary_json": str(sequence_paths["summary_json"]),
            "sequence_validation_json": str(sequence_paths["validation_json"]),
            "sequence_npz": str(sequence_paths["sequence_npz"]),
            "smoke_summary_json": str(smoke_paths["summary_json"]),
            "evaluation_summary_json": str(smoke_paths["evaluation_summary_json"]),
            "evaluation_validation_json": str(
                smoke_paths["evaluation_validation_json"]
            ),
            "selected_preview_json": str(smoke_paths["selected_preview_json"]),
            "checkpoint_dir": str(smoke_paths["checkpoint_dir"])
            if "checkpoint_dir" in smoke_paths
            else "",
            "research_shadow_training_rows": packet["dataset_summary"][
                "research_shadow_training_rows"
            ],
            "promotable_v13_permitted_training_rows": packet["dataset_summary"][
                "promotable_v13_permitted_training_rows"
            ],
            "adapted_research_shadow_rows": adapted_row_count,
            "candidate_library_csv_paths": candidate_library_paths,
            "forecast_context_coverage_status": packet["dataset_summary"][
                "forecast_context_coverage_status"
            ],
            "objective_kind": args.objective_kind,
            "market_execution_enabled": False,
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")
    return 0


def _decision_aware_summary(
    *,
    run_slug: str,
    objective_kind: str,
    packet: dict[str, object],
    sequence_paths: dict[str, Path],
    smoke_paths: dict[str, Path],
    teacher_rows_csv_path: Path,
    selected_rows_csv_path: Path,
) -> dict[str, object]:
    smoke_summary = json.loads(smoke_paths["summary_json"].read_text(encoding="utf-8"))
    evaluation = json.loads(
        smoke_paths["evaluation_summary_json"].read_text(encoding="utf-8")
    )
    selected_preview = json.loads(
        smoke_paths["selected_preview_json"].read_text(encoding="utf-8")
    )
    return {
        "run_slug": run_slug,
        "claim_scope": "dt_research_shadow_decision_aware_packet_not_market_execution",
        "objective_kind": objective_kind,
        "sequence_dataset_summary": packet["dataset_summary"],
        "training_objective": smoke_summary.get("training_objective", {}),
        "selection_policy": smoke_summary.get("selection_policy", {}),
        "dt_evaluation_metrics": evaluation.get("evaluation_metrics", {}),
        "regret_value_deltas": evaluation.get("regret_value_deltas", {}),
        "switch_quality": {
            key: evaluation.get("evaluation_metrics", {}).get(key)
            for key in (
                "non_v2_plus_switch_count",
                "abstention_count",
                "switch_win_count",
                "switch_loss_count",
                "switch_tie_count",
                "switch_mean_regret_delta_uah",
            )
        },
        "selected_rows_count": len(selected_preview.get("preview_rows", []))
        if isinstance(selected_preview.get("preview_rows"), list)
        else 0,
        "boundary": {
            "market_execution_enabled": False,
            "dt_lava_ready": False,
            "permits_model_training": False,
            "not_market_execution": True,
            "v2_plus_remains_default_comparator": True,
        },
        "attached_artifacts": {
            "teacher_rows_csv": teacher_rows_csv_path.name,
            "selected_rows_csv": selected_rows_csv_path.name,
            "sequence_summary_json": sequence_paths["summary_json"].name,
            "sequence_validation_json": sequence_paths["validation_json"].name,
            "smoke_summary_json": smoke_paths["summary_json"].name,
            "evaluation_summary_json": smoke_paths["evaluation_summary_json"].name,
            "evaluation_validation_json": smoke_paths["evaluation_validation_json"].name,
            "selected_preview_json": smoke_paths["selected_preview_json"].name,
            "summary_json": DT_RESEARCH_SHADOW_SUMMARY_JSON_NAME,
            "summary_markdown": DT_RESEARCH_SHADOW_SUMMARY_MD_NAME,
        },
    }


def _summary_markdown(summary: dict[str, object]) -> str:
    metrics_value = summary.get("dt_evaluation_metrics", {})
    metrics = metrics_value if isinstance(metrics_value, Mapping) else {}
    switch_quality_value = summary.get("switch_quality", {})
    switch_quality = (
        switch_quality_value if isinstance(switch_quality_value, Mapping) else {}
    )
    lines = [
        "# DT Decision-Aware Shadow Packet",
        "",
        f"Run slug: `{summary.get('run_slug', '')}`",
        f"Objective kind: `{summary.get('objective_kind', '')}`",
        "",
        "## Regret/Value Metrics",
        "",
        f"- DT mean regret: `{_float(metrics.get('dt_selected_mean_regret_uah')):.2f}` UAH",
        f"- DT median regret: `{_float(metrics.get('dt_selected_median_regret_uah')):.2f}` UAH",
        f"- V2+ mean regret: `{_float(metrics.get('v2_plus_mean_regret_uah')):.2f}` UAH",
        f"- V2+ median regret: `{_float(metrics.get('v2_plus_median_regret_uah')):.2f}` UAH",
        f"- Strict mean regret: `{_float(metrics.get('strict_mean_regret_uah')):.2f}` UAH",
        f"- Strict median regret: `{_float(metrics.get('strict_median_regret_uah')):.2f}` UAH",
        "",
        "## Switch Quality",
        "",
        f"- Non-V2+ switches: `{int(_float(switch_quality.get('non_v2_plus_switch_count')))}`",
        f"- Abstentions to V2+: `{int(_float(switch_quality.get('abstention_count')))}`",
        f"- Wins/losses/ties: `{int(_float(switch_quality.get('switch_win_count')))}` / "
        f"`{int(_float(switch_quality.get('switch_loss_count')))}` / "
        f"`{int(_float(switch_quality.get('switch_tie_count')))}`",
        "",
        "## Boundary",
        "",
        "- `market_execution_enabled=false`",
        "- `dt_lava_ready=false`",
        "- `permits_model_training=false`",
        "- Research diagnostic shadow only; V2+ remains default comparator",
    ]
    return "\n".join(lines) + "\n"


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
    selected_rows_csv_path = output_dir / DT_RESEARCH_SHADOW_SELECTED_ROWS_CSV_NAME
    frame.write_csv(selected_rows_csv_path)
    return selected_rows_csv_path


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


def _float(value: object) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


if __name__ == "__main__":
    raise SystemExit(main())
