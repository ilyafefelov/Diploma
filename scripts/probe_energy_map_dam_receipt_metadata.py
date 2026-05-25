"""Probe Energy Map DAM metadata as V13 receipt source leads."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

import httpx

from smart_arbitrage.dfl.energy_map_dam_receipt_metadata import (
    ENERGY_MAP_DATASET_API_BASE_URL,
    build_energy_map_dam_receipt_metadata_leads_v13_frame,
)
from smart_arbitrage.dfl.ua_context_v13_receipt_lead_audit import (
    audit_dfl_ua_context_dam_receipt_source_leads_v13_frame,
)

DEFAULT_DATASET_IDS: tuple[str, ...] = (
    "5a616fba-fbc9-4073-9532-9161592faca8",
    "c6218b35-ce7e-45c2-925e-5c8e6f5eb9fb",
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Probe Energy Map DAM dataset metadata for V13 receipt source "
            "leads. This writes metadata leads only; it does not emit receipt "
            "rows and does not permit DT/LAVA training or market execution."
        )
    )
    parser.add_argument(
        "--dataset-id",
        action="append",
        default=[],
        help="Energy Map dataset UUID to probe. Repeatable; defaults to DAM datasets.",
    )
    parser.add_argument("--locale", default="en")
    parser.add_argument(
        "--input-json",
        action="append",
        default=[],
        help="Offline Energy Map dataset metadata JSON. Repeatable.",
    )
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    args = parser.parse_args(argv)

    payloads = _load_payloads(args.input_json)
    if not payloads:
        payloads = _fetch_payloads(
            args.dataset_id or list(DEFAULT_DATASET_IDS),
            locale=args.locale,
        )

    frame = build_energy_map_dam_receipt_metadata_leads_v13_frame(
        payloads,
        locale=args.locale,
    )
    audit = audit_dfl_ua_context_dam_receipt_source_leads_v13_frame(frame)
    summary = {
        "claim_scope": "energy_map_dam_receipt_metadata_probe_not_v13_receipt",
        "dataset_count": len(payloads),
        "lead_rows": frame.height,
        "lead_status": "file_level_publication_metadata_only",
        "candidate_receipt_source_found": audit["candidate_receipt_source_found"],
        "dataset_level_metadata_only_count": audit[
            "dataset_level_metadata_only_count"
        ],
        "blocking_reasons": audit["blocking_reasons"],
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(args.output_csv)
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(
        json.dumps(_json_ready(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote Energy Map DAM receipt metadata leads: {args.output_csv}")
    print(f"Wrote Energy Map DAM receipt metadata summary: {args.summary_json}")
    return 0


def _load_payloads(paths: Sequence[str]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for raw_path in paths:
        path = Path(raw_path)
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise TypeError(f"{path} must contain a JSON object.")
        payloads.append(value)
    return payloads


def _fetch_payloads(dataset_ids: Sequence[str], *, locale: str) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    with httpx.Client(
        timeout=45.0,
        follow_redirects=True,
        headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
    ) as client:
        for dataset_id in dataset_ids:
            response = client.get(
                f"{ENERGY_MAP_DATASET_API_BASE_URL}/{dataset_id}/",
                params={"locale": locale},
            )
            response.raise_for_status()
            value = response.json()
            if not isinstance(value, dict):
                raise TypeError("Energy Map dataset metadata response must be an object.")
            payloads.append(value)
    return payloads


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
