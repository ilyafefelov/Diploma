"""Write a resumable official evidence-attempt manifest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from smart_arbitrage.forecasting.official_evidence_attempts import (
    OfficialEvidenceAttemptConfig,
    build_official_evidence_attempt_manifest,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write official forecast evidence-attempt metadata as JSON."
    )
    parser.add_argument(
        "--attempt-kind",
        required=True,
        choices=("official_schedule_value", "official_global_panel_backfill"),
    )
    parser.add_argument("--generated-at-iso", required=True)
    parser.add_argument("--total-anchors", type=int, required=True)
    parser.add_argument("--batch-size", type=int, required=True)
    parser.add_argument("--start-anchor-index", type=int, default=0)
    parser.add_argument("--end-anchor-index", type=int, default=0)
    parser.add_argument(
        "--anchor-batch-order",
        choices=("chronological", "latest_first"),
        default="chronological",
    )
    parser.add_argument(
        "--enabled-official-models-csv",
        default="nbeatsx_official_v0,tft_official_v0",
    )
    parser.add_argument("--nbeatsx-max-steps", type=int, default=0)
    parser.add_argument("--tft-max-epochs", type=int, default=0)
    parser.add_argument("--asset-selection", required=True)
    parser.add_argument("--downstream-selection", default="")
    parser.add_argument("--run-root", default=".tmp_runtime/official_evidence_attempts")
    parser.add_argument("--skip-downstream-gate", action="store_true")
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    manifest = build_official_evidence_attempt_manifest(
        OfficialEvidenceAttemptConfig(
            attempt_kind=args.attempt_kind,
            generated_at_iso=args.generated_at_iso,
            total_anchors=args.total_anchors,
            batch_size=args.batch_size,
            start_anchor_index=args.start_anchor_index,
            end_anchor_index=args.end_anchor_index,
            anchor_batch_order=args.anchor_batch_order,
            enabled_official_models_csv=args.enabled_official_models_csv,
            nbeatsx_max_steps=args.nbeatsx_max_steps,
            tft_max_epochs=args.tft_max_epochs,
            asset_selection=args.asset_selection,
            downstream_gate_enabled=not args.skip_downstream_gate,
            downstream_selection=args.downstream_selection,
            run_root=args.run_root,
        )
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
