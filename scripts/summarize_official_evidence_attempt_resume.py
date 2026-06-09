"""Summarize the next resume point for an official evidence attempt."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from smart_arbitrage.forecasting.official_evidence_attempts import (
    summarize_official_evidence_attempt_resume,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Summarize official evidence-attempt resume status from a manifest."
    )
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--persisted-anchor-count", type=int)
    parser.add_argument("--persisted-anchor-counts-csv", default="")
    parser.add_argument("--strategy-kind", default="")
    parser.add_argument("--generated-at-iso", default="")
    parser.add_argument("--output", default="")
    args = parser.parse_args(argv)

    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    persisted_anchor_counts_by_source = _parse_counts_csv(
        args.persisted_anchor_counts_csv
    )
    if persisted_anchor_counts_by_source is None and args.strategy_kind.strip():
        generated_at_iso = args.generated_at_iso.strip() or str(
            manifest["resume_generated_at_iso"]
        )
        persisted_anchor_counts_by_source = _load_counts_from_strategy_store(
            strategy_kind=args.strategy_kind.strip(),
            generated_at=generated_at_iso,
        )
    summary = summarize_official_evidence_attempt_resume(
        manifest,
        persisted_anchor_count=args.persisted_anchor_count,
        persisted_anchor_counts_by_source=persisted_anchor_counts_by_source,
    )
    output = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output, encoding="utf-8")
    else:
        print(output, end="")
    return 0


def _parse_counts_csv(counts_csv: str) -> dict[str, int] | None:
    if not counts_csv.strip():
        return None
    counts: dict[str, int] = {}
    for item in counts_csv.split(","):
        if "=" not in item:
            raise ValueError(
                "persisted-anchor-counts-csv entries must use source=count."
            )
        source_name, count_text = item.split("=", 1)
        source_name = source_name.strip()
        if not source_name:
            raise ValueError("persisted anchor count source name must not be blank.")
        counts[source_name] = int(count_text.strip())
    return counts


def _load_counts_from_strategy_store(
    *,
    strategy_kind: str,
    generated_at: str,
) -> dict[str, int]:
    from smart_arbitrage.resources.strategy_evaluation_store import (
        get_strategy_evaluation_store,
    )

    counts = get_strategy_evaluation_store().anchor_counts_by_model_for_generated_at(
        strategy_kind=strategy_kind,
        generated_at=generated_at,
    )
    if not counts:
        raise ValueError(
            "No persisted anchors found for the provided strategy_kind and generated_at."
        )
    return counts


if __name__ == "__main__":
    raise SystemExit(main())
