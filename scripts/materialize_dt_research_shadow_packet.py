"""Materialize credentialless DT research-shadow sequence and smoke artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Sequence

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
        "--model-backbone",
        choices=("auto", "local", "hf"),
        default="auto",
        help=(
            "DT smoke backbone selector. 'auto' uses Hugging Face "
            "DecisionTransformer when importable and otherwise records an "
            "explicit local-wrapper fallback."
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
    )
    json.dump(
        {
            "sequence_summary_json": str(sequence_paths["summary_json"]),
            "sequence_validation_json": str(sequence_paths["validation_json"]),
            "sequence_npz": str(sequence_paths["sequence_npz"]),
            "smoke_summary_json": str(smoke_paths["summary_json"]),
            "evaluation_summary_json": str(smoke_paths["evaluation_summary_json"]),
            "evaluation_validation_json": str(
                smoke_paths["evaluation_validation_json"]
            ),
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
            "market_execution_enabled": False,
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
