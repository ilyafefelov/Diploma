"""Materialize an apples-to-apples DT shadow packet from real V2+ strict rows."""

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
APPLES_TO_APPLES_SUMMARY_JSON_NAME = "dt_v2_plus_apples_to_apples_summary.json"
APPLES_TO_APPLES_SUMMARY_MD_NAME = "dt_v2_plus_apples_to_apples_summary.md"
APPLES_TO_APPLES_TEACHER_ROWS_CSV_NAME = "dt_v2_plus_apples_to_apples_teacher_rows.csv"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a non-promotable DT research-shadow packet directly from the "
            "real V2+ strict-row comparison artifact. The final-holdout controls "
            "remain apples-to-apples with the thesis V2+ result."
        )
    )
    parser.add_argument("--strict-rows-csv", type=Path, required=True)
    parser.add_argument("--regret-decomposition-csv", type=Path, default=None)
    parser.add_argument("--regret-decomposition-pickle", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-slug", default="week3_dt_v2_plus_apples_to_apples_current")
    parser.add_argument("--source-model-name", default=DEFAULT_SOURCE_MODEL_NAME)
    parser.add_argument("--context-length", type=int, default=4)
    parser.add_argument("--max-epochs", type=int, default=3)
    parser.add_argument("--hidden-dim", type=int, default=48)
    parser.add_argument("--num-layers", type=int, default=2)
    parser.add_argument("--num-heads", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20260526)
    parser.add_argument("--model-backbone", choices=("auto", "local", "hf"), default="hf")
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
        regret_decomposition = _load_polars_frame(
            args.regret_decomposition_pickle
        ).filter(pl.col("source_model_name") == args.source_model_name)

    teacher_rows = build_dt_research_shadow_teacher_rows_from_v2_plus_strict_rows(
        strict_rows_frame=strict_rows,
        regret_decomposition_frame=regret_decomposition,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    _csv_ready(teacher_rows).write_csv(
        args.output_dir / APPLES_TO_APPLES_TEACHER_ROWS_CSV_NAME
    )

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
    )
    summary = _apples_to_apples_summary(
        run_slug=args.run_slug,
        source_model_name=args.source_model_name,
        strict_rows_path=args.strict_rows_csv,
        regret_decomposition_path=regret_decomposition_path,
        teacher_rows=teacher_rows,
        sequence_paths=sequence_paths,
        smoke_paths=smoke_paths,
    )
    summary_json_path = args.output_dir / APPLES_TO_APPLES_SUMMARY_JSON_NAME
    summary_md_path = args.output_dir / APPLES_TO_APPLES_SUMMARY_MD_NAME
    summary_json_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_md_path.write_text(_summary_markdown(summary), encoding="utf-8")
    json.dump(
        {
            "summary_json": str(summary_json_path),
            "summary_markdown": str(summary_md_path),
            "teacher_rows_csv": str(
                args.output_dir / APPLES_TO_APPLES_TEACHER_ROWS_CSV_NAME
            ),
            "sequence_summary_json": str(sequence_paths["summary_json"]),
            "smoke_summary_json": str(smoke_paths["summary_json"]),
            "evaluation_summary_json": str(smoke_paths["evaluation_summary_json"]),
            "selected_preview_json": str(smoke_paths["selected_preview_json"]),
            "real_v2_plus_mean_regret_uah": summary["final_holdout_controls"][
                "schedule_value_learner_v2_plus"
            ]["mean_regret_uah"],
            "dt_selected_mean_regret_uah": summary["dt_evaluation_metrics"][
                "dt_selected_mean_regret_uah"
            ],
            "market_execution_enabled": False,
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")
    return 0


def _apples_to_apples_summary(
    *,
    run_slug: str,
    source_model_name: str,
    strict_rows_path: Path,
    regret_decomposition_path: Path | None,
    teacher_rows: pl.DataFrame,
    sequence_paths: dict[str, Path],
    smoke_paths: dict[str, Path],
) -> dict[str, Any]:
    evaluation = json.loads(
        smoke_paths["evaluation_summary_json"].read_text(encoding="utf-8")
    )
    controls = _final_holdout_controls(teacher_rows)
    best_labels = _best_label_summary(teacher_rows)
    return {
        "run_slug": run_slug,
        "claim_scope": "dt_v2_plus_apples_to_apples_shadow_not_promotable",
        "source_model_name": source_model_name,
        "input_artifacts": {
            "strict_rows_csv": str(strict_rows_path),
            "regret_decomposition_path": (
                None if regret_decomposition_path is None else str(regret_decomposition_path)
            ),
        },
        "method_note": (
            "The strict-row artifact is a final-holdout comparison packet. "
            "Training rows are mirrored from the same rows and dated before the "
            "final holdout only to exercise the DT tensor/training path. This is "
            "an apples-to-apples comparator smoke, not an out-of-sample promotion."
        ),
        "candidate_set": sorted(controls),
        "final_holdout_controls": controls,
        "best_available_label_summary": best_labels,
        "dt_evaluation_metrics": evaluation["evaluation_metrics"],
        "regret_value_deltas": evaluation["regret_value_deltas"],
        "boundary": {
            "real_v2_plus_comparator": True,
            "mirrored_training_rows": True,
            "out_of_sample_generalization_claim": False,
            "dt_promotion_gate_passed": False,
            "market_execution_enabled": False,
            "not_market_execution": True,
            "no_dashboard_api_default_switch": True,
        },
        "attached_artifacts": {
            "teacher_rows_csv": APPLES_TO_APPLES_TEACHER_ROWS_CSV_NAME,
            "sequence_summary_json": sequence_paths["summary_json"].name,
            "sequence_validation_json": sequence_paths["validation_json"].name,
            "smoke_summary_json": smoke_paths["summary_json"].name,
            "evaluation_summary_json": smoke_paths["evaluation_summary_json"].name,
            "evaluation_validation_json": smoke_paths["evaluation_validation_json"].name,
            "selected_preview_json": smoke_paths["selected_preview_json"].name,
            "summary_json": APPLES_TO_APPLES_SUMMARY_JSON_NAME,
            "summary_markdown": APPLES_TO_APPLES_SUMMARY_MD_NAME,
        },
    }


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


def _best_label_summary(teacher_rows: pl.DataFrame) -> dict[str, Any]:
    required = {
        "best_available_candidate_regret_uah",
        "best_available_regret_gap_vs_v2_plus_uah",
    }
    if not required.issubset(set(teacher_rows.columns)):
        return {"attached": False}
    best_rows = teacher_rows.filter(pl.col("split_name") == "final_holdout").unique(
        subset=["tenant_id", "source_model_name", "anchor_timestamp"]
    )
    return {
        "attached": True,
        "row_count": best_rows.height,
        "mean_best_available_candidate_regret_uah": best_rows[
            "best_available_candidate_regret_uah"
        ].mean(),
        "mean_gap_v2_plus_to_best_available_uah": best_rows[
            "best_available_regret_gap_vs_v2_plus_uah"
        ].mean(),
        "material_better_than_v2_plus_count": best_rows.filter(
            pl.col("best_available_regret_gap_vs_v2_plus_uah") > 0.0
        ).height,
    }


def _summary_markdown(summary: dict[str, Any]) -> str:
    controls = summary["final_holdout_controls"]
    metrics = summary["dt_evaluation_metrics"]
    deltas = summary["regret_value_deltas"]
    best = summary["best_available_label_summary"]
    lines = [
        "# DT V2+ Apples-to-Apples Shadow",
        "",
        f"Run slug: `{summary['run_slug']}`",
        "",
        summary["method_note"],
        "",
        "## Final-Holdout Controls",
        "",
        "| Candidate | Rows | Mean regret UAH | Median regret UAH |",
        "|---|---:|---:|---:|",
    ]
    for name, row in controls.items():
        lines.append(
            f"| `{name}` | {row['row_count']} | "
            f"{row['mean_regret_uah']:.2f} | {row['median_regret_uah']:.2f} |"
        )
    lines.extend(
        [
            "",
            "## DT Smoke Result",
            "",
            f"- DT selected mean regret: `{metrics['dt_selected_mean_regret_uah']:.2f}` UAH.",
            "- Real V2+ comparator mean regret: "
            f"`{controls['schedule_value_learner_v2_plus']['mean_regret_uah']:.2f}` UAH.",
            f"- Strict comparator mean regret: `{controls['strict_reference']['mean_regret_uah']:.2f}` UAH.",
            f"- DT minus real V2+: `{deltas['dt_minus_v2_plus_regret_uah']:.2f}` UAH.",
            f"- DT minus strict: `{deltas['dt_minus_strict_regret_uah']:.2f}` UAH.",
            "",
            "## Boundary",
            "",
            "- The comparison uses the real V2+ strict-row comparator, not the earlier fallback row.",
            "- Training rows are mirrored from the same final-holdout packet, so this is not an out-of-sample promotion claim.",
            "- `market_execution_enabled=false`; no dashboard/API default switch; no market-submittable bid.",
        ]
    )
    if best.get("attached"):
        lines.extend(
            [
                "",
                "## Best-Available Diagnostic Label",
                "",
                "- Mean best-available diagnostic regret: "
                f"`{best['mean_best_available_candidate_regret_uah']:.2f}` UAH.",
                "- Mean V2+ to best-available gap: "
                f"`{best['mean_gap_v2_plus_to_best_available_uah']:.2f}` UAH.",
                "- Material better-than-V2+ anchors: "
                f"`{best['material_better_than_v2_plus_count']} / {best['row_count']}`.",
            ]
        )
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
