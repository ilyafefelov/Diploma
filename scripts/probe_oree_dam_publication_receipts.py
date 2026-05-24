from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

import httpx

from smart_arbitrage.assets.bronze.market_weather import (
    LEVEL1_MARKET_VENUE,
    LEVEL1_MARKET_ZONE,
    OREE_DATA_VIEW_URL,
    OREE_PRICES_URL,
)
from smart_arbitrage.dfl.oree_dam_publication_receipts import (
    build_oree_dam_publication_receipt_probe,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Probe the OREE DAM data_view response for row-level publication "
            "receipt metadata. This does not create receipt rows."
        )
    )
    parser.add_argument("--month", required=True, help="Month in OREE format, e.g. 04.2026")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    response = _fetch_oree_data_view(args.month)
    probe = build_oree_dam_publication_receipt_probe(
        requested_month=args.month,
        source_url=OREE_DATA_VIEW_URL,
        response_headers=response.headers,
        response_text=response.text,
        retrieved_at=datetime.now(UTC),
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(probe, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    json.dump(probe, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


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
