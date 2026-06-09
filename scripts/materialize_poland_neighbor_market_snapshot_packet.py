from __future__ import annotations

import argparse
import pickle
from pathlib import Path

import polars as pl

from smart_arbitrage.forecasting.poland_neighbor_snapshot_export import (
    build_poland_neighbor_market_snapshot_packet,
    write_poland_neighbor_market_snapshot_packet,
)

DEFAULT_RUN_SLUG = "week3_poland_neighbor_market_snapshot_no_token"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export a no-token Poland neighbor-market snapshot evidence packet."
    )
    parser.add_argument("--snapshot-frame-pickle", type=Path, required=True)
    parser.add_argument("--feature-candidate-frame-pickle", type=Path, required=True)
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--output-root", type=Path, default=Path("data/research_runs"))
    args = parser.parse_args()

    snapshot_frame = _load_polars_frame(args.snapshot_frame_pickle)
    feature_candidate_frame = _load_polars_frame(args.feature_candidate_frame_pickle)
    packet = build_poland_neighbor_market_snapshot_packet(
        snapshot_frame=snapshot_frame,
        feature_candidate_frame=feature_candidate_frame,
    )
    export_dir = write_poland_neighbor_market_snapshot_packet(
        output_root=args.output_root,
        run_slug=args.run_slug,
        snapshot_frame=snapshot_frame,
        feature_candidate_frame=feature_candidate_frame,
    )
    print(
        {
            "export_dir": str(export_dir),
            "summary_json": str(
                export_dir / "poland_neighbor_market_snapshot_summary.json"
            ),
            "summary_markdown": str(
                export_dir / "poland_neighbor_market_snapshot_summary.md"
            ),
            "source_backed_rows": packet["snapshot_summary"]["source_backed_rows"],
            "candidate_rows": packet["candidate_summary"]["row_count"],
            "market_execution_enabled": packet["claim_boundary"][
                "market_execution_enabled"
            ],
        }
    )
    return 0


def _load_polars_frame(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Evidence input not found: {path}")
    with path.open("rb") as fh:
        value = pickle.load(fh)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"Expected Polars DataFrame in {path}")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
