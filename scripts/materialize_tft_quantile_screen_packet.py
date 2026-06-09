from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys

import polars as pl

from smart_arbitrage.dfl.tft_quantile_screen_export import (
    build_dfl_tft_quantile_screen_packet,
    write_dfl_tft_quantile_screen_packet,
)

DEFAULT_RUN_SLUG = "week3_tft_quantile_screen_evidence"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a TFT quantile screen evidence packet."
    )
    parser.add_argument("--raw-strict-frame-pickle", type=Path, required=True)
    parser.add_argument("--candidate-library-pickle", type=Path, required=True)
    parser.add_argument("--augmented-gate-frame-pickle", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    parser.add_argument("--asset-check-status", default=None)
    parser.add_argument("--tft-source-models-csv", default="")
    args = parser.parse_args()

    raw_strict_frame = _load_polars_frame(args.raw_strict_frame_pickle)
    candidate_library_frame = _load_polars_frame(args.candidate_library_pickle)
    augmented_gate_frame = _load_polars_frame(args.augmented_gate_frame_pickle)
    packet = build_dfl_tft_quantile_screen_packet(
        run_slug=args.run_slug,
        raw_strict_frame=raw_strict_frame,
        candidate_library_frame=candidate_library_frame,
        augmented_gate_frame=augmented_gate_frame,
        tft_source_model_names=_parse_csv(args.tft_source_models_csv),
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
        asset_check_status=args.asset_check_status,
    )
    export_dir = write_dfl_tft_quantile_screen_packet(
        packet,
        output_root=args.output_root,
        raw_strict_frame=raw_strict_frame,
        candidate_library_frame=candidate_library_frame,
        augmented_gate_frame=augmented_gate_frame,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "summary_json": str(export_dir / "dfl_tft_quantile_screen_summary.json"),
            "summary_markdown": str(
                export_dir / "dfl_tft_quantile_screen_summary.md"
            ),
            "gate_decision": packet["gate"]["decision"],
            "gate_passed": packet["gate"]["passed"],
            "gate_blockers": packet["gate_blockers"],
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


def _parse_csv(value: str) -> tuple[str, ...] | None:
    if not value.strip():
        return None
    return tuple(item.strip() for item in value.split(",") if item.strip())


if __name__ == "__main__":
    main()
