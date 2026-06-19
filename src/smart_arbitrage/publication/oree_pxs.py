"""Small OREE PXS fetch helpers for public static artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
import json
import time
from typing import Any, Final

from smart_arbitrage.publication.bess_arbitrage_index import OREE_DAM_RESULTS_URL

OREE_PXS_GET_RESULTS_URL: Final[str] = "https://www.oree.com.ua/index.php/PXS/get_pxs_res"
OREE_PXS_HDATA_URL_PREFIX: Final[str] = "https://www.oree.com.ua/index.php/PXS/get_pxs_hdata"


def fetch_oree_dam_price_rows(delivery_day: date) -> list[dict[str, Any]]:
    import httpx
    from bs4 import BeautifulSoup

    day_label = delivery_day.strftime("%d.%m.%Y")
    headers = {
        "Referer": OREE_DAM_RESULTS_URL,
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
    }
    with httpx.Client(timeout=45.0, follow_redirects=True, headers=headers) as client:
        hdata_links: list[str] = []
        for attempt_index in range(5):
            results_response = client.post(OREE_PXS_GET_RESULTS_URL, data={"day": day_label})
            results_response.raise_for_status()
            soup = BeautifulSoup(_decode_response_text(results_response), "html.parser")
            hdata_links = [
                str(input_node.get("value")).strip()
                for input_node in soup.select("input.hdata_link")
                if input_node.get("value") is not None and str(input_node.get("value")).strip()
            ]
            if hdata_links:
                break
            time.sleep(0.25 * (attempt_index + 1))
        if not hdata_links:
            raise ValueError("OREE PXS results response did not expose DAM hdata links")

        rows_by_timestamp: dict[datetime, dict[str, Any]] = {}
        for hdata_link in hdata_links:
            hdata_response = client.post(f"{OREE_PXS_HDATA_URL_PREFIX}/{hdata_link}")
            hdata_response.raise_for_status()
            payload = json.loads(_decode_response_text(hdata_response))
            if not isinstance(payload, Mapping):
                raise TypeError("OREE PXS hdata response must be a JSON object")
            for row in price_rows_from_hdata_payload(
                delivery_day=delivery_day,
                hdata_link=hdata_link,
                payload=payload,
            ):
                rows_by_timestamp[row["timestamp"]] = row
    rows = [rows_by_timestamp[timestamp] for timestamp in sorted(rows_by_timestamp)]
    if len(rows) != 24:
        raise ValueError(f"OREE DAM hdata produced {len(rows)} rows; expected 24")
    return rows


def price_rows_from_hdata_payload(
    *,
    delivery_day: date,
    hdata_link: str,
    payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    prices = _sequence(payload, "pricesData")
    amounts = _sequence(payload, "amountsData")
    labels = payload.get("labels") or list(range(1, len(prices) + 1))
    if not isinstance(labels, Sequence) or isinstance(labels, (str, bytes)):
        raise TypeError("OREE PXS payload field 'labels' must be a sequence")

    rows: list[dict[str, Any]] = []
    for row_index, label in enumerate(labels):
        hour = _hour_index(label)
        timestamp = datetime.combine(
            delivery_day,
            datetime.min.time().replace(hour=hour - 1),
        )
        rows.append(
            {
                "timestamp": timestamp,
                "price_uah_mwh": _optional_float(prices[row_index]),
                "volume_mwh": _optional_float(amounts[row_index]) if row_index < len(amounts) else None,
                "source_url": f"{OREE_PXS_HDATA_URL_PREFIX}/{hdata_link}",
            }
        )
    return rows


def _decode_response_text(response: Any) -> str:
    encoding = response.encoding or "windows-1251"
    try:
        return response.content.decode(encoding)
    except UnicodeDecodeError:
        return response.content.decode("windows-1251", errors="replace")


def _sequence(payload: Mapping[str, Any], key: str) -> Sequence[Any]:
    value = payload.get(key, [])
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"OREE PXS payload field {key!r} must be a sequence")
    return value


def _hour_index(value: Any) -> int:
    hour = int(str(value).strip())
    if not 1 <= hour <= 24:
        raise ValueError("OREE PXS delivery hour must be in 1..24")
    return hour


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    stripped = str(value).strip()
    if not stripped:
        return None
    return float(stripped.replace(",", "."))
