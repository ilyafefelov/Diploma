"""Aggregate validated DT/LAVA research metrics without promotion semantics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from smart_arbitrage.dfl.dt_lava_research_metrics import (
    REQUIRED_DT_LAVA_RESEARCH_METRIC_FIELDS,
    aggregate_dt_lava_research_metrics_payloads,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate DT/LAVA research metrics JSON files into a non-promotion "
            "summary for CI evidence."
        ),
    )
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    payloads = _load_metric_payloads(args.input_dir, output_path=args.output)
    aggregate = aggregate_dt_lava_research_metrics_payloads(payloads)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(aggregate, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote DT/LAVA research metrics aggregate to {args.output}")
    return 0


def _load_metric_payloads(
    input_dir: Path,
    *,
    output_path: Path,
) -> list[dict[str, Any]]:
    if not input_dir.exists():
        raise ValueError(f"DT/LAVA metrics input directory does not exist: {input_dir}")
    if not input_dir.is_dir():
        raise ValueError(f"DT/LAVA metrics input path must be a directory: {input_dir}")

    payloads: list[dict[str, Any]] = []
    resolved_output_path = output_path.resolve()
    for path in sorted(input_dir.rglob("*.json")):
        if path.resolve() == resolved_output_path:
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"DT/LAVA metrics JSON must be an object: {path}")
        if not REQUIRED_DT_LAVA_RESEARCH_METRIC_FIELDS.issubset(payload):
            if "metrics" in path.stem:
                missing = sorted(REQUIRED_DT_LAVA_RESEARCH_METRIC_FIELDS - set(payload))
                raise ValueError(
                    "DT/LAVA metrics JSON is missing required fields "
                    f"({', '.join(missing)}): {path}"
                )
            continue
        payloads.append(payload)
    if not payloads:
        raise ValueError(f"No DT/LAVA metrics JSON files found under {input_dir}.")
    return payloads


if __name__ == "__main__":
    raise SystemExit(main())
