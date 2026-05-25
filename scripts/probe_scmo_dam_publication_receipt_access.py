"""Probe SCMO DAM publication export access for V13 receipt-source discovery."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Sequence

import httpx
import polars as pl

from smart_arbitrage.dfl.scmo_dam_receipt_access import (
    build_scmo_dam_publication_receipt_access_probe,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Probe the SCMO XMtrade/PXS DAM publication portal. This writes "
            "receipt-source lead evidence only; it does not emit V13 receipt "
            "rows and does not permit DT/LAVA training."
        )
    )
    parser.add_argument("--url", default="https://scmo.oree.com.ua/")
    parser.add_argument("--probe-output-json", type=Path, required=True)
    parser.add_argument("--lead-output-csv", type=Path, required=True)
    parser.add_argument("--response-text-override", default=None)
    parser.add_argument("--final-url-override", default=None)
    args = parser.parse_args(argv)

    retrieved_at = datetime.now(UTC)
    if args.response_text_override is None or args.final_url_override is None:
        response = _fetch(args.url)
        response_text = response.text
        final_url = str(response.url)
        status_code = response.status_code
        content_type = response.headers.get("content-type", "")
    else:
        response_text = args.response_text_override
        final_url = args.final_url_override
        status_code = 200
        content_type = "text/html"

    probe = build_scmo_dam_publication_receipt_access_probe(
        source_url=args.url,
        final_url=final_url,
        status_code=status_code,
        content_type=content_type,
        response_text=response_text,
        retrieved_at=retrieved_at,
    )

    args.probe_output_json.parent.mkdir(parents=True, exist_ok=True)
    args.probe_output_json.write_text(
        json.dumps(_json_ready(probe), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    args.lead_output_csv.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([probe["lead_row"]]).write_csv(args.lead_output_csv)
    print(f"Wrote SCMO DAM receipt access probe: {args.probe_output_json}")
    print(f"Wrote SCMO DAM receipt source lead: {args.lead_output_csv}")
    return 0


def _fetch(url: str) -> httpx.Response:
    with httpx.Client(
        timeout=30.0,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0"},
    ) as client:
        response = client.get(url)
        response.raise_for_status()
        return response


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
