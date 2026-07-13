"""Write the machine-readable v1.3 correction for legacy DT evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from smart_arbitrage.dfl.v1_3_evidence_audit import (
    audit_legacy_temporal_dt_contract,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(audit_legacy_temporal_dt_contract(), indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
