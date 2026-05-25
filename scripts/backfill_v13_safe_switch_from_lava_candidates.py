"""Backfill canonical V13 safe-switch rows from source-observed LAVA candidates."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
from typing import Any, Sequence

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_lava_candidate_safe_switch_backfill import (
    build_v13_safe_switch_examples_from_lava_candidates_frame,
    summarize_v13_safe_switch_lava_candidate_backfill,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Mine an existing LAVA schedule-candidate frame for canonical V13 "
            "safe-switch examples, but only when an OREE DAM source observation "
            "exists for the delivery date. This is acquisition evidence only; "
            "it does not permit DT/LAVA training or market execution."
        )
    )
    parser.add_argument("--candidate-frame-pickle", type=Path, required=True)
    parser.add_argument(
        "--oree-observations-csv",
        type=Path,
        action="append",
        required=True,
        help="OREE DAM download observation CSV. May be passed more than once.",
    )
    parser.add_argument("--acquisition-targets-csv", type=Path, required=True)
    parser.add_argument("--existing-safe-switch-csv", type=Path, default=None)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    parser.add_argument("--min-regret-improvement-uah", type=float, default=25.0)
    args = parser.parse_args(argv)

    candidate_frame = _load_pickle_frame(args.candidate_frame_pickle)
    observations_frame = _read_observations(args.oree_observations_csv)
    targets_frame = pl.read_csv(args.acquisition_targets_csv, infer_schema_length=0)
    existing_frame = (
        pl.read_csv(args.existing_safe_switch_csv, infer_schema_length=0)
        if args.existing_safe_switch_csv is not None
        else None
    )

    examples = build_v13_safe_switch_examples_from_lava_candidates_frame(
        candidate_frame,
        observations_frame,
        targets_frame,
        existing_frame,
        min_regret_improvement_uah=args.min_regret_improvement_uah,
    )
    summary = summarize_v13_safe_switch_lava_candidate_backfill(
        examples,
        candidate_frame,
        observations_frame,
        targets_frame,
        existing_frame,
        min_regret_improvement_uah=args.min_regret_improvement_uah,
    )
    summary.update(
        {
            "candidate_frame_pickle": str(args.candidate_frame_pickle),
            "oree_observations_csv": [str(path) for path in args.oree_observations_csv],
            "acquisition_targets_csv": str(args.acquisition_targets_csv),
            "existing_safe_switch_csv": (
                str(args.existing_safe_switch_csv)
                if args.existing_safe_switch_csv is not None
                else None
            ),
            "output_csv": str(args.output_csv),
        }
    )

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    examples.write_csv(args.output_csv)
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(
        json.dumps(_json_ready(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote V13 safe-switch LAVA candidate backfill: {args.output_csv}")
    print(f"Wrote V13 safe-switch LAVA candidate backfill summary: {args.summary_json}")
    return 0


def _load_pickle_frame(path: Path) -> pl.DataFrame:
    with path.open("rb") as file:
        value = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"Expected Polars DataFrame pickle: {path}")
    return value


def _read_observations(paths: Sequence[Path]) -> pl.DataFrame:
    frames = [pl.read_csv(path, infer_schema_length=0) for path in paths]
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


if __name__ == "__main__":
    raise SystemExit(main())
