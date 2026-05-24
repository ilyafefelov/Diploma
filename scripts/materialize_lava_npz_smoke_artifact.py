"""Materialize a tiny LAVA NPZ smoke artifact from candidate-frame evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
from typing import Sequence

import polars as pl

from smart_arbitrage.dfl.lava_npz_smoke_contract import (
    write_lava_npz_smoke_artifact_from_candidate_frame,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create a validated, research-only LAVA NPZ smoke artifact from "
            "an existing schedule-neighbor candidate Polars DataFrame."
        ),
    )
    parser.add_argument("--candidate-frame-pickle", type=Path, required=True)
    parser.add_argument("--output-npz", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    parser.add_argument("--max-instances", type=int, default=8)
    parser.add_argument("--max-neighbors", type=int, default=4)
    args = parser.parse_args(argv)

    candidate_frame = _load_polars_frame(args.candidate_frame_pickle)
    summary = write_lava_npz_smoke_artifact_from_candidate_frame(
        candidate_frame,
        args.output_npz,
        max_instances=args.max_instances,
        max_neighbors=args.max_neighbors,
    )
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        "Wrote LAVA NPZ smoke artifact to "
        f"{args.output_npz} and summary to {args.summary_json}"
    )
    return 0


def _load_polars_frame(path: Path) -> pl.DataFrame:
    with path.open("rb") as file:
        value = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
