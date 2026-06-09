from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from smart_arbitrage.forecasting.entsoe_file_library import (
    ENTSOE_ENERGY_PRICES_FOLDER,
    ENTSOE_FILE_LIBRARY_GUIDE_URL,
    decode_entsoe_fms_file_content,
    download_entsoe_fms_file,
    list_entsoe_energy_price_files,
    load_entsoe_file_library_credentials,
    normalize_energy_prices_csv_to_poland_snapshot_frame,
    request_entsoe_fms_token,
    safe_entsoe_fms_smoke_receipt,
    select_entsoe_energy_price_file,
    write_poland_snapshot_csv,
)

DEFAULT_OUTPUT_DIR = Path("data/external_sources/poland/entsoe_fms")
DEFAULT_MONTH = "2026-01"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch a research-only ENTSO-E File Library EnergyPrices snapshot "
            "and normalize Poland DAM prices for the existing governance route."
        )
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--month", default=DEFAULT_MONTH)
    parser.add_argument("--country-code", default="PL")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-csv", type=Path, default=None)
    parser.add_argument("--receipt-path", type=Path, default=None)
    parser.add_argument("--config-output", type=Path, default=None)
    parser.add_argument("--token-only", action="store_true")
    parser.add_argument("--list-only", action="store_true")
    args = parser.parse_args()

    credentials = load_entsoe_file_library_credentials(env_file=args.env_file)
    token = request_entsoe_fms_token(
        username=credentials.username,
        password=credentials.password,
    )
    token_metadata = token.safe_metadata()
    if args.token_only:
        print(
            json.dumps(
                {
                    "status": "token_ok",
                    "token_metadata": token_metadata,
                    "market_execution_enabled": False,
                },
                indent=2,
            )
        )
        return 0

    files = list_entsoe_energy_price_files(token, folder=ENTSOE_ENERGY_PRICES_FOLDER)
    if args.list_only:
        print(
            json.dumps(
                {
                    "status": "list_ok",
                    "file_count": len(files),
                    "first_files": [
                        {
                            "filename": file.filename,
                            "last_updated_timestamp": file.last_updated_timestamp,
                            "period_from": file.period_from,
                            "period_to": file.period_to,
                        }
                        for file in files[:10]
                    ],
                    "market_execution_enabled": False,
                },
                indent=2,
            )
        )
        return 0

    selected_file = select_entsoe_energy_price_file(files, month=args.month)
    content = download_entsoe_fms_file(token, selected_file)
    csv_text = decode_entsoe_fms_file_content(content)
    snapshot_frame = normalize_energy_prices_csv_to_poland_snapshot_frame(
        csv_text,
        country_code=args.country_code,
    )
    if snapshot_frame.is_empty():
        raise RuntimeError(
            f"ENTSO-E EnergyPrices file {selected_file.filename} did not contain "
            f"{args.country_code} day-ahead EUR/MWh rows."
        )

    output_csv = args.output_csv or (
        args.output_dir
        / f"entsoe-energy-prices-{args.country_code.lower()}-{args.month}.csv"
    )
    write_poland_snapshot_csv(snapshot_frame, output_csv)
    receipt = safe_entsoe_fms_smoke_receipt(
        token_metadata=token_metadata,
        selected_file=selected_file,
        output_csv_path=output_csv,
        row_count=snapshot_frame.height,
    )
    receipt_path = args.receipt_path or (args.output_dir / "entsoe-fms-smoke-receipt.json")
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    if args.config_output is not None:
        _write_dagster_config(
            path=args.config_output,
            output_csv=output_csv,
            selected_file=selected_file,
        )

    print(
        json.dumps(
            {
                "status": "snapshot_written",
                "output_csv": str(output_csv),
                "receipt_path": str(receipt_path),
                "config_output": str(args.config_output) if args.config_output else "",
                "rows": snapshot_frame.height,
                "selected_file": selected_file.filename,
                "market_execution_enabled": False,
            },
            indent=2,
        )
    )
    return 0


def _write_dagster_config(
    *,
    path: Path,
    output_csv: Path,
    selected_file,
) -> None:
    retrieved_at = datetime.now(UTC).isoformat()
    publication_timestamp = selected_file.source_publication_timestamp_utc
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "ops:",
                "  poland_neighbor_market_snapshot_bronze:",
                "    config:",
                f'      snapshot_csv_path: "{_yaml_string(output_csv)}"',
                f'      source_url: "{ENTSOE_FILE_LIBRARY_GUIDE_URL}"',
                '      source_access_method: "entsoe_fms_file_library"',
                f'      source_retrieved_at_utc: "{retrieved_at}"',
                f'      source_publication_timestamp_utc: "{publication_timestamp}"',
                '      source_license_status: "requires_entsoe_terms_mapping"',
                '      snapshot_kind: "day_ahead_price_eur_mwh"',
                "  poland_neighbor_market_snapshot_feature_candidate_frame:",
                "    config:",
                '      ua_decision_anchor_timestamp_utc: "2025-12-31T12:00:00+00:00"',
                "      prior_eur_uah_fx_rate: 0.0",
                '      prior_eur_uah_fx_timestamp_utc: ""',
                '      fx_rate_source: ""',
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _yaml_string(path: Path) -> str:
    return str(path).replace("\\", "/").replace('"', '\\"')


if __name__ == "__main__":
    raise SystemExit(main())
