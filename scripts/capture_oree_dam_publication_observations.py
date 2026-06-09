"""Capture OREE PXS DAM observation rows without creating V13 receipt rows."""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import json
from pathlib import Path
import time
from typing import Any, Sequence

from bs4 import BeautifulSoup
import httpx

from smart_arbitrage.dfl.oree_dam_publication_observations import (
    build_oree_dam_publication_observation_frame,
    empty_oree_dam_publication_observation_frame,
    summarize_oree_dam_publication_observation_frame,
)

OREE_DAM_RESULTS_URL = "https://www.oree.com.ua/index.php/control/results_mo/DAM"
OREE_PXS_GET_RESULTS_URL = "https://www.oree.com.ua/index.php/PXS/get_pxs_res"
OREE_PXS_HDATA_URL_PREFIX = "https://www.oree.com.ua/index.php/PXS/get_pxs_hdata"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Capture first-seen OREE PXS DAM observation evidence. This writes "
            "observation rows only; it does not emit V13 DAM publication receipt "
            "rows and does not permit DT/LAVA training or market execution."
        )
    )
    parser.add_argument(
        "--delivery-date",
        required=True,
        help="DAM delivery date as YYYY-MM-DD or DD.MM.YYYY.",
    )
    parser.add_argument(
        "--input-hdata-json",
        type=Path,
        default=None,
        help="Optional captured PXS get_pxs_hdata JSON payload for offline parsing.",
    )
    parser.add_argument(
        "--retrieved-at",
        default=None,
        help="Override retrieval timestamp for deterministic offline parsing.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=1,
        help="Number of live polling attempts before writing an empty blocker summary.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=30.0,
        help="Seconds to sleep between live polling attempts.",
    )
    parser.add_argument(
        "--attempt-log-json",
        type=Path,
        default=None,
        help="Optional JSON path for per-attempt OREE PXS first-seen evidence.",
    )
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    args = parser.parse_args(argv)

    retrieved_at = _retrieved_at(args.retrieved_at)
    attempts: list[dict[str, Any]] = []
    payload: dict[str, Any] | None
    source_probe_status = "hdata_found"
    if args.input_hdata_json is not None:
        payload = _load_payload(args.input_hdata_json)
        attempts.append(
            {
                "attempt_index": 1,
                "retrieved_at": retrieved_at.isoformat(),
                "source_probe_status": "offline_hdata_payload_loaded",
                "hdata_link_found": True,
            }
        )
    else:
        payload, attempts = _fetch_oree_pxs_hdata_payload_with_attempts(
            _delivery_date(args.delivery_date),
            max_attempts=args.max_attempts,
            sleep_seconds=args.sleep_seconds,
        )
        retrieved_at = _retrieved_at(str(attempts[-1]["retrieved_at"]))
        if payload is None:
            source_probe_status = "hdata_not_found"

    frame = (
        build_oree_dam_publication_observation_frame(
            delivery_date=args.delivery_date,
            hdata_payload=payload,
            retrieved_at=retrieved_at,
            source_url=OREE_DAM_RESULTS_URL,
        )
        if payload is not None
        else empty_oree_dam_publication_observation_frame()
    )
    summary = summarize_oree_dam_publication_observation_frame(frame)
    summary = {
        **summary,
        "source_probe_status": source_probe_status,
        "attempt_count": len(attempts),
        "first_seen_attempt_index": _first_seen_attempt_index(attempts),
        "attempt_log_json": (
            str(args.attempt_log_json) if args.attempt_log_json is not None else None
        ),
    }

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(args.output_csv)
    if args.attempt_log_json is not None:
        args.attempt_log_json.parent.mkdir(parents=True, exist_ok=True)
        args.attempt_log_json.write_text(
            json.dumps(_json_ready(attempts), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(
        json.dumps(
            {
                **summary,
                "observation_csv": str(args.output_csv),
                "source_url": OREE_DAM_RESULTS_URL,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"Wrote OREE DAM publication observation CSV: {args.output_csv}")
    print(f"Wrote OREE DAM publication observation summary: {args.summary_json}")
    return 0


def _fetch_oree_pxs_hdata_payload_with_attempts(
    delivery_date: date,
    *,
    max_attempts: int,
    sleep_seconds: float,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    if max_attempts < 1:
        raise ValueError("--max-attempts must be at least 1.")
    if sleep_seconds < 0:
        raise ValueError("--sleep-seconds must be non-negative.")

    attempts: list[dict[str, Any]] = []
    day = delivery_date.strftime("%d.%m.%Y")
    with httpx.Client(
        timeout=45.0,
        follow_redirects=True,
        headers={
            "Referer": OREE_DAM_RESULTS_URL,
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
        },
    ) as client:
        for attempt_index in range(1, max_attempts + 1):
            retrieved_at = datetime.now(UTC)
            attempt: dict[str, Any] = {
                "attempt_index": attempt_index,
                "retrieved_at": retrieved_at.isoformat(),
                "source_url": OREE_DAM_RESULTS_URL,
                "delivery_date": delivery_date.isoformat(),
                "hdata_link_found": False,
                "source_probe_status": "hdata_not_found",
                "market_execution_enabled": False,
                "permits_model_training": False,
                "validated_receipt_csv_ready": False,
            }
            try:
                results_response = client.post(OREE_PXS_GET_RESULTS_URL, data={"day": day})
                attempt["results_status_code"] = results_response.status_code
                results_response.raise_for_status()
                hdata_link = _hdata_link_or_none(results_response.text)
                attempt["hdata_link"] = hdata_link
                attempt["hdata_link_found"] = hdata_link is not None
                if hdata_link is not None:
                    hdata_response = client.post(
                        f"{OREE_PXS_HDATA_URL_PREFIX}/{hdata_link}"
                    )
                    attempt["hdata_status_code"] = hdata_response.status_code
                    hdata_response.raise_for_status()
                    payload = hdata_response.json()
                    if not isinstance(payload, dict):
                        raise TypeError("OREE PXS hdata response must be a JSON object.")
                    attempt["source_probe_status"] = "hdata_found"
                    attempt["hdata_payload_keys"] = sorted(str(key) for key in payload)
                    attempts.append(attempt)
                    return payload, attempts
            except Exception as error:
                attempt["source_probe_status"] = "probe_error_without_receipt_export"
                attempt["error_type"] = type(error).__name__
                attempt["error"] = str(error)
            attempts.append(attempt)
            if attempt_index < max_attempts:
                time.sleep(sleep_seconds)
    return None, attempts


def _fetch_oree_pxs_hdata_payload(delivery_date: date) -> dict[str, Any]:
    day = delivery_date.strftime("%d.%m.%Y")
    with httpx.Client(
        timeout=45.0,
        follow_redirects=True,
        headers={
            "Referer": OREE_DAM_RESULTS_URL,
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
        },
    ) as client:
        results_response = client.post(OREE_PXS_GET_RESULTS_URL, data={"day": day})
        results_response.raise_for_status()
        hdata_link = _hdata_link(results_response.text)
        hdata_response = client.post(f"{OREE_PXS_HDATA_URL_PREFIX}/{hdata_link}")
        hdata_response.raise_for_status()
        payload = hdata_response.json()
    if not isinstance(payload, dict):
        raise TypeError("OREE PXS hdata response must be a JSON object.")
    return payload


def _hdata_link(response_text: str) -> str:
    soup = BeautifulSoup(response_text, "html.parser")
    input_node = soup.select_one("input.hdata_link")
    if input_node is None:
        raise ValueError("OREE PXS results response did not contain hdata_link.")
    raw_value = input_node.get("value")
    if raw_value is None or not str(raw_value).strip():
        raise ValueError("OREE PXS hdata_link is blank.")
    return str(raw_value).strip()


def _hdata_link_or_none(response_text: str) -> str | None:
    soup = BeautifulSoup(response_text, "html.parser")
    input_node = soup.select_one("input.hdata_link")
    if input_node is None:
        return None
    raw_value = input_node.get("value")
    if raw_value is None or not str(raw_value).strip():
        return None
    return str(raw_value).strip()


def _load_payload(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"{path} must contain a JSON object.")
    return payload


def _retrieved_at(raw_value: str | None) -> datetime:
    if raw_value is None or not raw_value.strip():
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _delivery_date(raw_value: str) -> date:
    stripped = raw_value.strip()
    for date_format in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(stripped, date_format).date()
        except ValueError:
            continue
    raise ValueError("delivery date must use YYYY-MM-DD or DD.MM.YYYY format.")


def _first_seen_attempt_index(attempts: Sequence[dict[str, Any]]) -> int | None:
    for attempt in attempts:
        if bool(attempt.get("hdata_link_found")):
            return int(attempt["attempt_index"])
    return None


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
