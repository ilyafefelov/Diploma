"""Run the preregistered full-history HF candidate-ranker protocol."""

from __future__ import annotations

import argparse
import json
import pickle
from datetime import datetime
from pathlib import Path
from typing import Sequence

import polars as pl

from smart_arbitrage.dfl.full_history_hf_candidate_ranker import (
    build_full_history_ranker_candidate_frame,
    run_full_history_hf_candidate_ranker,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--candidate-library-pickle", type=Path)
    input_group.add_argument("--prepared-split-dir", type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--test-start", default="2026-04-12T23:00:00")
    parser.add_argument("--validation-anchor-count", type=int, default=28)
    parser.add_argument("--minimum-train-anchor-count", type=int, default=293)
    parser.add_argument("--max-epochs", type=int, default=80)
    args = parser.parse_args(argv)

    if args.candidate_library_pickle is not None:
        with args.candidate_library_pickle.open("rb") as handle:
            candidate_library = pickle.load(handle)
        candidate_rows = build_full_history_ranker_candidate_frame(candidate_library)
    else:
        split_dir = args.prepared_split_dir
        if split_dir is None:  # pragma: no cover - argparse guarantees one input.
            raise ValueError("A prepared split directory is required.")
        candidate_rows = pl.concat(
            [
                pl.read_parquet(split_dir / f"{name}.parquet")
                for name in ("train_rows", "validation_rows", "test_rows")
            ],
            how="vertical_relaxed",
        )
    result = run_full_history_hf_candidate_ranker(
        candidate_rows,
        test_start=datetime.fromisoformat(args.test_start),
        output_dir=args.output_dir,
        validation_anchor_count=args.validation_anchor_count,
        minimum_train_anchor_count=args.minimum_train_anchor_count,
        max_epochs=args.max_epochs,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "full_history_summary.json").write_text(
        json.dumps(result, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
