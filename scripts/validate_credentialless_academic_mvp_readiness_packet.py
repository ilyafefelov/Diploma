"""Validate a credentialless academic MVP readiness packet."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for import_path in (PROJECT_ROOT, SRC_ROOT):
    import_path_text = str(import_path)
    if import_path_text not in sys.path:
        sys.path.insert(0, import_path_text)

from smart_arbitrage.dfl.credentialless_academic_mvp_readiness import (  # noqa: E402
    validate_credentialless_academic_mvp_readiness_summary,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the credentialless academic MVP packet: DAM operator "
            "preview and DFL/DT prototype gates may pass, while market "
            "submission, DT/LAVA promotion, and execution remain blocked."
        )
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    payload = _load_json_object(args.input)
    validation = validate_credentialless_academic_mvp_readiness_summary(payload)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(validation, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    status = "passed" if validation["passed"] else "failed"
    print(f"Credentialless academic MVP validation {status}: {args.output}")
    return 0 if validation["passed"] else 1


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


if __name__ == "__main__":
    raise SystemExit(main())
