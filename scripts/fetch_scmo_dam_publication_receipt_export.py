"""Fetch authenticated SCMO DAM publication receipt export."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Sequence

import httpx

from smart_arbitrage.dfl.scmo_dam_receipt_export import SCMO_AUTO_COLUMN
from smart_arbitrage.dfl.scmo_dam_receipt_fetch import (
    ScmoExportResponse,
    fetch_result_from_scmo_export_response,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch an authenticated SCMO DAM export URL and optionally normalize "
            "it to the V13 receipt CSV schema. This refuses SSO/login HTML and "
            "does not enable DT/LAVA training or market execution."
        )
    )
    parser.add_argument("--url", required=True)
    parser.add_argument("--raw-output", type=Path, required=True)
    parser.add_argument("--normalized-output", type=Path, default=None)
    parser.add_argument("--summary-json", type=Path, required=True)
    parser.add_argument(
        "--input-format",
        choices=["auto", "csv", "xml", "xlsx", "zip", "html"],
        default="auto",
        help="SCMO export format. Defaults to auto detection from HTTP metadata.",
    )
    parser.add_argument("--cookie-env-var", default="SCMO_COOKIE")
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        help="Additional HTTP header as 'Name: value'. May be repeated.",
    )
    parser.add_argument("--timestamp-column", default=SCMO_AUTO_COLUMN)
    parser.add_argument(
        "--source-publication-timestamp-column",
        default=SCMO_AUTO_COLUMN,
    )
    parser.add_argument("--receipt-id-column", default=None)
    parser.add_argument(
        "--v13-base-config",
        type=Path,
        default=None,
        help="Optional V13 base Dagster config to copy and wire with this fetch output.",
    )
    parser.add_argument(
        "--v13-safe-switch-csv",
        type=Path,
        default=None,
        help="Optional already-validated V13 safe-switch CSV for the derived config.",
    )
    parser.add_argument(
        "--v13-output-config",
        type=Path,
        default=None,
        help="Optional derived V13 config output path.",
    )
    parser.add_argument(
        "--v13-preflight-output",
        type=Path,
        default=None,
        help="Optional preflight JSON output for the derived V13 config.",
    )
    args = parser.parse_args(argv)

    cookie_header = os.environ.get(args.cookie_env_var, "")
    response = _fetch(
        args.url,
        cookie_header=cookie_header,
        extra_headers=_headers(args.header),
    )
    result = fetch_result_from_scmo_export_response(
        response,
        raw_output_path=args.raw_output,
        normalized_output_path=args.normalized_output,
        timestamp_column=args.timestamp_column,
        source_publication_timestamp_column=args.source_publication_timestamp_column,
        receipt_id_column=args.receipt_id_column,
        input_format=args.input_format,
    )
    v13_input_config_summary = _maybe_write_v13_input_config(args)
    if v13_input_config_summary is not None:
        result["v13_input_config_summary"] = v13_input_config_summary
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _maybe_write_v13_input_config(args: argparse.Namespace) -> dict[str, Any] | None:
    requested = any(
        value is not None
        for value in (
            args.v13_base_config,
            args.v13_safe_switch_csv,
            args.v13_output_config,
            args.v13_preflight_output,
        )
    )
    if not requested:
        return None
    if args.normalized_output is None:
        raise ValueError(
            "--normalized-output is required when requesting V13 config/preflight output."
        )
    if args.v13_base_config is None or args.v13_output_config is None:
        raise ValueError(
            "--v13-base-config and --v13-output-config are required when "
            "requesting V13 config/preflight output."
        )

    from scripts.build_v13_acquisition_input_config import (  # noqa: PLC0415
        build_v13_acquisition_input_config,
    )

    return build_v13_acquisition_input_config(
        base_config_path=args.v13_base_config,
        dam_receipts_csv_path=args.normalized_output,
        safe_switch_csv_path=args.v13_safe_switch_csv,
        output_config_path=args.v13_output_config,
        preflight_output_path=args.v13_preflight_output,
    )


def _fetch(
    url: str,
    *,
    cookie_header: str,
    extra_headers: dict[str, str],
) -> ScmoExportResponse:
    headers = {
        "Accept": (
            "text/csv,application/csv,application/xml,text/xml,"
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
            "application/vnd.ms-excel,application/zip,text/html,"
            "application/xhtml+xml,text/plain,*/*"
        ),
        "User-Agent": "Mozilla/5.0",
        **extra_headers,
    }
    if cookie_header.strip():
        headers["Cookie"] = cookie_header.strip()
    with httpx.Client(timeout=60.0, follow_redirects=True, headers=headers) as client:
        response = client.get(url)
    return ScmoExportResponse(
        source_url=url,
        final_url=str(response.url),
        status_code=response.status_code,
        content_type=response.headers.get("content-type", ""),
        body=response.content,
    )


def _headers(values: Sequence[str]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for value in values:
        if ":" not in value:
            raise ValueError(f"Header must use 'Name: value' format: {value}")
        name, raw_value = value.split(":", 1)
        if not name.strip():
            raise ValueError("Header name must not be empty.")
        headers[name.strip()] = raw_value.strip()
    return headers


if __name__ == "__main__":
    raise SystemExit(main())
