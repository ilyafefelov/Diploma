"""Materialize weak OREE policy-deadline evidence without V13 receipt promotion."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Sequence

from smart_arbitrage.dfl.oree_policy_publication_deadline_evidence import (
    build_oree_policy_publication_deadline_evidence_frame,
    summarize_oree_policy_publication_deadline_evidence_frame,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build weak OREE publication policy-deadline evidence from a public "
            "OREE candidate audit. This does not emit source_publication_timestamp "
            "rows and does not satisfy V13 explicit receipt readiness."
        )
    )
    parser.add_argument("--candidate-audit-json", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    parser.add_argument("--generated-at", default=None)
    args = parser.parse_args(argv)

    candidate_audit = _load_candidate_audit(args.candidate_audit_json)
    generated_at = _generated_at(args.generated_at)
    frame = build_oree_policy_publication_deadline_evidence_frame(
        candidate_audit,
        generated_at=generated_at,
    )
    summary = summarize_oree_policy_publication_deadline_evidence_frame(frame)
    summary = {
        **summary,
        "candidate_audit_json": str(args.candidate_audit_json),
        "policy_evidence_csv": str(args.output_csv),
    }

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(args.output_csv)
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(
        json.dumps(_json_ready(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote OREE policy publication deadline evidence CSV: {args.output_csv}")
    print(f"Wrote OREE policy publication deadline summary: {args.summary_json}")
    return 0


def _load_candidate_audit(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain a JSON object.")
    return {str(key): item for key, item in value.items()}


def _generated_at(raw_value: str | None) -> datetime:
    if raw_value is None or not raw_value.strip():
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


if __name__ == "__main__":
    raise SystemExit(main())
