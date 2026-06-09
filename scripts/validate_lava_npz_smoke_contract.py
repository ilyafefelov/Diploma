"""Validate a tiny LAVA-style NPZ smoke artifact before research use."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from smart_arbitrage.dfl.lava_npz_smoke_contract import (
    validate_lava_npz_smoke_contract,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate a LAVA-style schedule-neighbor NPZ smoke artifact.",
    )
    parser.add_argument("--input", required=True, help="Path to the NPZ artifact.")
    parser.add_argument("--output", required=True, help="Path to write summary JSON.")
    args = parser.parse_args(argv)

    summary = validate_lava_npz_smoke_contract(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote LAVA NPZ smoke summary to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
