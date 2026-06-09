"""Validate DT/LAVA research metrics JSON before publishing artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from smart_arbitrage.dfl.dt_lava_research_metrics import (
    validate_dt_lava_research_metrics_payload,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate and normalize one DT/LAVA research metrics JSON file.",
    )
    parser.add_argument("--input", required=True, help="Path to raw metrics JSON.")
    parser.add_argument("--output", required=True, help="Path to normalized metrics JSON.")
    args = parser.parse_args(argv)

    input_path = Path(args.input)
    output_path = Path(args.output)

    raw_payload = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(raw_payload, dict):
        raise ValueError("DT/LAVA research metrics payload must be a JSON object.")

    normalized_payload = validate_dt_lava_research_metrics_payload(raw_payload)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(normalized_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote normalized DT/LAVA research metrics to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
