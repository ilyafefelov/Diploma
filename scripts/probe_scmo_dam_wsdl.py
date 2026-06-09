"""Probe SCMO SOAP WSDL contracts for V13 DAM receipt source discovery."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Sequence

import httpx
import polars as pl

from smart_arbitrage.dfl.scmo_dam_wsdl_probe import (
    build_scmo_dam_wsdl_receipt_source_probe,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Probe the SCMO SOAP WSDL Download contract. This writes "
            "receipt-source lead evidence only; it does not emit V13 receipt "
            "rows and does not permit DT/LAVA training or market execution."
        )
    )
    parser.add_argument(
        "--url",
        default="https://scmo.oree.com.ua/interfaces/Evaluations/Service.svc?wsdl",
    )
    parser.add_argument("--service-kind", default="Evaluations")
    parser.add_argument("--input-wsdl", type=Path, default=None)
    parser.add_argument("--probe-output-json", type=Path, required=True)
    parser.add_argument("--lead-output-csv", type=Path, required=True)
    args = parser.parse_args(argv)

    retrieved_at = datetime.now(UTC)
    status_code: int | None = None
    content_type: str | None = None
    if args.input_wsdl is None:
        response = _fetch(args.url)
        wsdl_text = response.text
        status_code = response.status_code
        content_type = response.headers.get("content-type", "")
    else:
        wsdl_text = args.input_wsdl.read_text(encoding="utf-8")

    probe = build_scmo_dam_wsdl_receipt_source_probe(
        source_url=args.url,
        wsdl_text=wsdl_text,
        service_kind=args.service_kind,
        retrieved_at=retrieved_at,
    )
    probe["status_code"] = status_code
    probe["content_type"] = content_type

    args.probe_output_json.parent.mkdir(parents=True, exist_ok=True)
    args.probe_output_json.write_text(
        json.dumps(_json_ready(probe), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    args.lead_output_csv.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([probe["lead_row"]]).write_csv(args.lead_output_csv)
    print(f"Wrote SCMO DAM WSDL source probe: {args.probe_output_json}")
    print(f"Wrote SCMO DAM WSDL source lead: {args.lead_output_csv}")
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
