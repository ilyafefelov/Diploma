"""Aggregate canonical offline experiment metrics for thesis evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for import_path in (PROJECT_ROOT, SRC_ROOT):
    import_path_text = str(import_path)
    if import_path_text not in sys.path:
        sys.path.insert(0, import_path_text)

from smart_arbitrage.dfl.canonical_experiment_metrics import (  # noqa: E402
    BASELINE_V2_PLUS_MEAN_REGRET_UAH,
    aggregate_canonical_model_dir,
    write_canonical_aggregate,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate runs/<model>/seed_*/metrics.json into a canonical "
            "offline pass/fail summary. This does not promote market execution."
        )
    )
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument(
        "--baseline-seed-means-json",
        type=Path,
        default=None,
        help="JSON array of V2+ seed mean regrets for Welch t-test.",
    )
    parser.add_argument(
        "--baseline-mean",
        type=float,
        default=BASELINE_V2_PLUS_MEAN_REGRET_UAH,
    )
    parser.add_argument(
        "--allow-any-seed-count",
        action="store_true",
        help="Allow smoke aggregation with fewer or more than exactly three seeds.",
    )
    args = parser.parse_args(argv)

    baseline_seed_means = _load_baseline_seed_means(args.baseline_seed_means_json)
    aggregate = aggregate_canonical_model_dir(
        args.model_dir,
        baseline_seed_means=baseline_seed_means,
        baseline_mean=args.baseline_mean,
        required_seed_count=None if args.allow_any_seed_count else 3,
    )
    output_path = args.output or args.model_dir / "aggregate.json"
    write_canonical_aggregate(output_path, aggregate)
    print(f"Wrote canonical experiment aggregate to {output_path}")
    return 0


def _load_baseline_seed_means(path: Path | None) -> list[float] | None:
    if path is None:
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("--baseline-seed-means-json must contain a JSON array.")
    values: list[float] = []
    for value in raw:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("baseline seed means must be numeric.")
        values.append(float(value))
    return values


if __name__ == "__main__":
    raise SystemExit(main())
