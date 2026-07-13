"""Materialize the preregistered v1.2 differentiable DFL research suite."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys
from typing import Any, Sequence

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for import_path in (PROJECT_ROOT, SRC_ROOT):
    import_path_text = str(import_path)
    if import_path_text not in sys.path:
        sys.path.insert(0, import_path_text)

from smart_arbitrage.dfl.differentiable_forecast_v1_2 import (  # noqa: E402
    run_v1_2_differentiable_suite,
)

DEFAULT_SOURCES = (
    "nbeatsx_official_global_panel_horizon_calibrated_v1",
    "nbeatsx_official_global_panel_v1",
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Train time-separated MLP and transformer price correctors through "
            "the storage layer and strict-score them against frozen V2+."
        )
    )
    parser.add_argument("--rolling-strict-pickle", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--source-model-names", default=",".join(DEFAULT_SOURCES))
    parser.add_argument("--seeds", default="42,2026,7")
    parser.add_argument("--epochs", type=int, default=6)
    parser.add_argument("--hidden-dim", type=int, default=32)
    parser.add_argument("--learning-rate", type=float, default=0.01)
    args = parser.parse_args(argv)

    rolling_rows = _load_frame(args.rolling_strict_pickle)
    result = run_v1_2_differentiable_suite(
        rolling_rows,
        output_dir=args.output_dir,
        source_model_names=_text_values(args.source_model_names),
        seeds=_int_values(args.seeds),
        epoch_count=args.epochs,
        hidden_dim=args.hidden_dim,
        learning_rate=args.learning_rate,
    )
    json.dump(result["summary"], sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


def _load_frame(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("rb") as file:
        value: Any = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


def _text_values(value: str) -> tuple[str, ...]:
    values = tuple(item.strip() for item in value.split(",") if item.strip())
    if not values:
        raise ValueError("At least one value is required.")
    return values


def _int_values(value: str) -> tuple[int, ...]:
    return tuple(int(item) for item in _text_values(value))


if __name__ == "__main__":
    raise SystemExit(main())
