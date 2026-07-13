"""Audit a Polars pickle artifact against the aligned DFL feature contract."""

from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path
from typing import Sequence

import polars as pl

from smart_arbitrage.dfl.aligned_differentiable_dfl import (
    assess_aligned_dfl_feature_readiness,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    with args.input.open("rb") as handle:
        frame = pickle.load(handle)
    if not isinstance(frame, pl.DataFrame):
        raise TypeError("--input must contain a Polars DataFrame.")
    readiness = assess_aligned_dfl_feature_readiness(frame)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(readiness, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0 if bool(readiness["ready"]) else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
