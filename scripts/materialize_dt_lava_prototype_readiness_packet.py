"""Materialize a DT/LAVA prototype readiness packet."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from smart_arbitrage.dfl.dt_lava_prototype_readiness import (
    SUMMARY_JSON_NAME,
    SUMMARY_MARKDOWN_NAME,
    write_dt_lava_prototype_readiness_packet,
)

MISSING_CANDIDATE_FRAME_BLOCKER = "candidate_frame_pickle_missing"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Write a machine-checkable DT/LAVA prototype readiness packet. "
            "The packet separates CI smoke readiness from V13 training permission "
            "and market-execution gates. The JSON artifact is "
            f"{SUMMARY_JSON_NAME}; the Markdown artifact is {SUMMARY_MARKDOWN_NAME}."
        ),
    )
    parser.add_argument("--v13-acquisition-summary-json", type=Path, required=True)
    parser.add_argument(
        "--offline-strategy-promotion-registry-json",
        type=Path,
        default=None,
        help=(
            "Optional schedule/value production gate registry JSON. "
            "When supplied, the readiness packet reports that upstream offline "
            "strategy promotion separately from DT/LAVA promotion."
        ),
    )
    parser.add_argument(
        "--lava-npz-smoke-validation-json",
        type=Path,
        default=None,
        help=(
            "Validated lava_npz_margin_smoke_packet_validation.json to attach "
            "to the prototype CI-smoke gate evidence. If omitted, the packet "
            "is written but the CI-smoke gate remains blocked."
        ),
    )
    parser.add_argument("--candidate-frame-pickle", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--materialization-blocker",
        action="append",
        default=[],
        help=(
            "Name of a missing upstream Dagster materialization input. "
            "Can be passed more than once."
        ),
    )
    args = parser.parse_args(argv)

    packet = write_dt_lava_prototype_readiness_packet(
        output_dir=args.output_dir,
        v13_acquisition_summary=_load_json_object(args.v13_acquisition_summary_json),
        candidate_frame_pickle_path=args.candidate_frame_pickle,
        lava_npz_smoke_packet_validation=(
            _load_json_object(args.lava_npz_smoke_validation_json)
            if args.lava_npz_smoke_validation_json is not None
            else None
        ),
        materialization_blockers=args.materialization_blocker,
        offline_strategy_promotion_registry=(
            _load_json_object(args.offline_strategy_promotion_registry_json)
            if args.offline_strategy_promotion_registry_json is not None
            else None
        ),
    )
    print(
        "Wrote DT/LAVA prototype readiness packet: "
        f"{packet['summary_json']} and {packet['summary_markdown']}"
    )
    return 0


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    if bool(payload.get("market_execution_enabled", False)):
        raise ValueError("V13 acquisition summary must not enable market execution.")
    return payload


if __name__ == "__main__":
    raise SystemExit(main())
