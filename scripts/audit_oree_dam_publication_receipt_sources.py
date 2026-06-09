from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
from typing import Any

import httpx

from smart_arbitrage.assets.bronze.market_weather import (
    LEVEL1_MARKET_VENUE,
    LEVEL1_MARKET_ZONE,
    OREE_DATA_VIEW_URL,
    OREE_PRICES_URL,
)
from smart_arbitrage.dfl.oree_dam_publication_receipts import (
    build_oree_dam_publication_receipt_probe,
    build_oree_dam_publication_receipt_source_audit,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Audit OREE DAM source responses for row-level publication receipt "
            "metadata. This writes evidence, not receipt rows."
        )
    )
    parser.add_argument(
        "--months",
        default="",
        help="Comma-separated OREE months to live-probe, e.g. 03.2026,04.2026.",
    )
    parser.add_argument(
        "--probe-json",
        action="append",
        default=[],
        help="Existing single-month probe JSON to include; repeatable.",
    )
    parser.add_argument(
        "--probe-output-dir",
        type=Path,
        default=None,
        help="Optional directory for live single-month probe JSON files.",
    )
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    probes = _load_existing_probes(args.probe_json)
    for month in _months_from_csv(args.months):
        probe = _live_probe_month(month)
        probes.append(probe)
        if args.probe_output_dir is not None:
            args.probe_output_dir.mkdir(parents=True, exist_ok=True)
            (args.probe_output_dir / f"oree_dam_publication_receipt_probe_{month}.json").write_text(
                json.dumps(probe, indent=2, sort_keys=True),
                encoding="utf-8",
            )

    if not probes:
        raise ValueError("Provide --months, --probe-json, or both.")

    audit = build_oree_dam_publication_receipt_source_audit(probes)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(audit, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    json.dump(audit, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


def _load_existing_probes(paths: list[str]) -> list[dict[str, Any]]:
    probes: list[dict[str, Any]] = []
    for raw_path in paths:
        path = Path(raw_path)
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise TypeError(f"{path} must contain a JSON object.")
        probes.append(value)
    return probes


def _months_from_csv(raw_value: str) -> list[str]:
    return [month.strip() for month in raw_value.split(",") if month.strip()]


def _live_probe_month(month: str) -> dict[str, Any]:
    response = _fetch_oree_data_view(month)
    return build_oree_dam_publication_receipt_probe(
        requested_month=month,
        source_url=OREE_DATA_VIEW_URL,
        response_headers=response.headers,
        response_text=response.text,
        retrieved_at=datetime.now(UTC),
    )


def _fetch_oree_data_view(month: str) -> httpx.Response:
    with httpx.Client(timeout=45.0, follow_redirects=True) as client:
        response = client.post(
            OREE_DATA_VIEW_URL,
            data={
                "date": month,
                "market": LEVEL1_MARKET_VENUE,
                "zone": LEVEL1_MARKET_ZONE,
            },
            headers={
                "Referer": OREE_PRICES_URL,
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        response.raise_for_status()
        return response


if __name__ == "__main__":
    main()
