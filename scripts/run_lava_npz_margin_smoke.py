"""Run deterministic LAVA NPZ margin diagnostics and write metrics JSON."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from smart_arbitrage.dfl.lava_npz_margin_smoke import run_lava_npz_margin_smoke


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a research-only LAVA NPZ adjacent-margin smoke.",
    )
    parser.add_argument("--input", type=Path, required=True, help="Validated NPZ path.")
    parser.add_argument("--output", type=Path, required=True, help="Metrics JSON path.")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--tenant-id", default="lava_npz_smoke_panel")
    parser.add_argument("--source-model-name", default="lava_schedule_neighbor_npz_smoke_v0")
    parser.add_argument("--window-id", default="lava_npz_smoke_window")
    parser.add_argument("--v13-gate-status", default="data_acquisition_needed")
    args = parser.parse_args(argv)

    metrics = run_lava_npz_margin_smoke(
        args.input,
        seed=args.seed,
        tenant_id=args.tenant_id,
        source_model_name=args.source_model_name,
        window_id=args.window_id,
        v13_gate_status=args.v13_gate_status,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(metrics, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote LAVA NPZ margin smoke metrics to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
