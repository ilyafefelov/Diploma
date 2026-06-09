from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys

import polars as pl

from smart_arbitrage.dfl.market_coupling_ablation_export import (
    build_dfl_market_coupling_v2_plus_ablation_packet,
    write_dfl_market_coupling_v2_plus_ablation_packet,
)

DEFAULT_RUN_SLUG = "week3_dfl_market_coupling_ablation_v1"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a governed market-coupling ablation evidence packet."
    )
    parser.add_argument("--ablation-frame-pickle", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    args = parser.parse_args()

    ablation_frame = _load_polars_frame(args.ablation_frame_pickle)
    packet = build_dfl_market_coupling_v2_plus_ablation_packet(
        run_slug=args.run_slug,
        ablation_frame=ablation_frame,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
    )
    export_dir = write_dfl_market_coupling_v2_plus_ablation_packet(
        packet,
        output_root=args.output_root,
        ablation_frame=ablation_frame,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "summary_json": str(
                export_dir / "dfl_market_coupling_v2_plus_ablation_summary.json"
            ),
            "summary_markdown": str(
                export_dir / "dfl_market_coupling_v2_plus_ablation_summary.md"
            ),
            "ablation_status_counts": packet["ablation_summary"]["status_counts"],
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
