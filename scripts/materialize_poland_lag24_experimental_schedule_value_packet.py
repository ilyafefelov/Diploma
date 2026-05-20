from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys

import polars as pl

from smart_arbitrage.dfl.poland_lag24_experimental_export import (
    build_poland_lag24_experimental_schedule_value_packet,
    write_poland_lag24_experimental_schedule_value_packet,
)

DEFAULT_RUN_SLUG = "week3_poland_lag24_experimental_schedule_value_near_miss"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export a Poland lag-24 experimental schedule/value evidence packet."
        )
    )
    parser.add_argument("--comparison-frame-pickle", type=Path, required=True)
    parser.add_argument("--raw-strict-frame-pickle", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    args = parser.parse_args()

    comparison_frame = _load_polars_frame(args.comparison_frame_pickle)
    raw_strict_frame = (
        _load_polars_frame(args.raw_strict_frame_pickle)
        if args.raw_strict_frame_pickle is not None
        else None
    )
    packet = build_poland_lag24_experimental_schedule_value_packet(
        run_slug=args.run_slug,
        comparison_frame=comparison_frame,
        raw_strict_frame=raw_strict_frame,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
    )
    export_dir = write_poland_lag24_experimental_schedule_value_packet(
        packet,
        output_root=args.output_root,
        comparison_frame=comparison_frame,
        raw_strict_frame=raw_strict_frame,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "summary_json": str(
                export_dir / "poland_lag24_experimental_schedule_value_summary.json"
            ),
            "summary_markdown": str(
                export_dir / "poland_lag24_experimental_schedule_value_summary.md"
            ),
            "promotes_over_frozen_v2_plus": packet["gate"][
                "promotes_over_frozen_v2_plus"
            ],
            "gate_blocker": packet["gate"]["blocker"],
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
