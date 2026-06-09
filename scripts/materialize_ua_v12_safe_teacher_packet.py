from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys

import polars as pl

from smart_arbitrage.dfl.ua_v12_safe_teacher_export import (
    build_dfl_ua_v12_safe_teacher_backfill_packet,
    write_dfl_ua_v12_safe_teacher_backfill_packet,
)

DEFAULT_RUN_SLUG = "week3_dfl_ua_context_v12_safe_teacher_backfill"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a V12 UA safe teacher-label backfill packet."
    )
    parser.add_argument("--source-inventory-pickle", type=Path, required=True)
    parser.add_argument("--expanded-context-pickle", type=Path, required=True)
    parser.add_argument("--safe-teacher-pickle", type=Path, required=True)
    parser.add_argument("--candidate-library-pickle", type=Path, required=True)
    parser.add_argument("--strict-rescore-pickle", type=Path, required=True)
    parser.add_argument("--readiness-decision-pickle", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    parser.add_argument("--asset-check-status", default=None)
    args = parser.parse_args()

    source_inventory = _load_polars_frame(args.source_inventory_pickle)
    expanded_context = _load_polars_frame(args.expanded_context_pickle)
    safe_teacher = _load_polars_frame(args.safe_teacher_pickle)
    candidate_library = _load_polars_frame(args.candidate_library_pickle)
    strict_rescore = _load_polars_frame(args.strict_rescore_pickle)
    readiness_decision = _load_polars_frame(args.readiness_decision_pickle)
    packet = build_dfl_ua_v12_safe_teacher_backfill_packet(
        run_slug=args.run_slug,
        source_inventory_frame=source_inventory,
        expanded_context_panel_frame=expanded_context,
        safe_teacher_label_panel_frame=safe_teacher,
        low_tail_candidate_library_frame=candidate_library,
        low_tail_strict_rescore_frame=strict_rescore,
        readiness_decision_frame=readiness_decision,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
        asset_check_status=args.asset_check_status,
    )
    export_dir = write_dfl_ua_v12_safe_teacher_backfill_packet(
        packet,
        output_root=args.output_root,
        source_inventory_frame=source_inventory,
        expanded_context_panel_frame=expanded_context,
        safe_teacher_label_panel_frame=safe_teacher,
        low_tail_candidate_library_frame=candidate_library,
        low_tail_strict_rescore_frame=strict_rescore,
        readiness_decision_frame=readiness_decision,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "summary_json": str(export_dir / "dfl_ua_v12_safe_teacher_summary.json"),
            "summary_markdown": str(
                export_dir / "dfl_ua_v12_safe_teacher_summary.md"
            ),
            "dt_lava_ready": packet["dt_lava_ready"],
            "readiness_decisions": packet["readiness_summary"][
                "readiness_decisions"
            ],
            "market_execution_enabled": packet["claim_boundary"][
                "market_execution_enabled"
            ],
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")


def _load_polars_frame(path: Path) -> pl.DataFrame:
    with path.open("rb") as file:
        value = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


if __name__ == "__main__":
    main()
