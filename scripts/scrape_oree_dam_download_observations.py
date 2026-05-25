"""Scrape OREE PXS DAM hdata/download observations for receipt-source discovery."""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import time
from typing import Any, Sequence

from bs4 import BeautifulSoup
import httpx
import polars as pl

from smart_arbitrage.dfl.oree_dam_download_observations import (
    build_oree_dam_download_observation_frame,
    summarize_oree_dam_download_observation_frame,
)

OREE_DAM_RESULTS_URL = "https://www.oree.com.ua/index.php/control/results_mo/DAM"
OREE_PXS_GET_RESULTS_URL = "https://www.oree.com.ua/index.php/PXS/get_pxs_res"
OREE_PXS_HDATA_URL_PREFIX = "https://www.oree.com.ua/index.php/PXS/get_pxs_hdata"
OREE_PXS_DOWNLOAD_URL_PREFIX = "https://www.oree.com.ua/index.php/PXS/downloadxlsx"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape OREE PXS DAM hdata/download responses. This captures "
            "headers, hashes, and source observations only; it does not create "
            "V13 receipt rows unless the source exposes explicit publication "
            "metadata in a later implementation."
        )
    )
    parser.add_argument(
        "--delivery-date",
        action="append",
        default=[],
        help="Delivery date as YYYY-MM-DD or DD.MM.YYYY. May be repeated.",
    )
    parser.add_argument("--from-date", default=None)
    parser.add_argument("--to-date", default=None)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    parser.add_argument("--download-dir", type=Path, default=None)
    args = parser.parse_args(argv)

    delivery_dates = _delivery_dates(
        explicit=args.delivery_date,
        from_date=args.from_date,
        to_date=args.to_date,
    )
    retrieved_at = datetime.now(UTC)
    frames: list[pl.DataFrame] = []
    errors: list[dict[str, str]] = []
    with httpx.Client(
        timeout=45.0,
        follow_redirects=True,
        headers={
            "Referer": OREE_DAM_RESULTS_URL,
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
        },
    ) as client:
        for delivery_day in delivery_dates:
            try:
                frames.extend(
                    _scrape_delivery_date(
                        client,
                        delivery_day=delivery_day,
                        retrieved_at=retrieved_at,
                        download_dir=args.download_dir,
                    )
                )
            except Exception as error:
                errors.append(
                    {
                        "delivery_date": delivery_day.isoformat(),
                        "error_type": type(error).__name__,
                        "error": str(error),
                    }
                )

    frame = pl.concat(frames, how="vertical") if frames else pl.DataFrame()
    summary = summarize_oree_dam_download_observation_frame(frame)
    summary.update(
        {
            "delivery_dates_requested": [value.isoformat() for value in delivery_dates],
            "scrape_errors": errors,
            "scrape_error_count": len(errors),
            "observation_csv": str(args.output_csv),
            "download_dir": str(args.download_dir) if args.download_dir else None,
            "source_url": OREE_DAM_RESULTS_URL,
        }
    )

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(args.output_csv)
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(
        json.dumps(_json_ready(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote OREE DAM download observations: {args.output_csv}")
    print(f"Wrote OREE DAM download observation summary: {args.summary_json}")
    return 0


def _scrape_delivery_date(
    client: httpx.Client,
    *,
    delivery_day: date,
    retrieved_at: datetime,
    download_dir: Path | None,
) -> list[pl.DataFrame]:
    day_label = delivery_day.strftime("%d.%m.%Y")
    hdata_links: list[str] = []
    for attempt_index in range(5):
        results_response = client.post(OREE_PXS_GET_RESULTS_URL, data={"day": day_label})
        results_response.raise_for_status()
        hdata_links = _hdata_links(_decode_response_text(results_response))
        if hdata_links:
            break
        time.sleep(0.25 * (attempt_index + 1))
    if not hdata_links:
        raise ValueError("OREE PXS results response did not contain hdata links.")
    frames: list[pl.DataFrame] = []
    for hdata_link in hdata_links:
        hdata_response = _post_with_retry(
            client,
            f"{OREE_PXS_HDATA_URL_PREFIX}/{hdata_link}",
        )
        payload = json.loads(_decode_response_text(hdata_response))
        if not isinstance(payload, dict):
            raise TypeError("OREE PXS hdata response must be a JSON object.")
        download_response = client.get(f"{OREE_PXS_DOWNLOAD_URL_PREFIX}/{hdata_link}")
        download_response.raise_for_status()
        if download_dir is not None:
            _write_download(download_dir, hdata_link, download_response)
        frames.append(
            build_oree_dam_download_observation_frame(
                delivery_date=delivery_day,
                hdata_link=hdata_link,
                hdata_payload=payload,
                hdata_headers=hdata_response.headers,
                download_headers=download_response.headers,
                download_content=download_response.content,
                retrieved_at=retrieved_at,
                source_url=OREE_DAM_RESULTS_URL,
            )
        )
    return frames


def _post_with_retry(client: httpx.Client, url: str) -> httpx.Response:
    response = client.post(url)
    response.raise_for_status()
    if response.content.strip().startswith(b"{"):
        return response
    time.sleep(0.25)
    response = client.post(url)
    response.raise_for_status()
    return response


def _hdata_links(response_text: str) -> list[str]:
    soup = BeautifulSoup(response_text, "html.parser")
    links: list[str] = []
    for input_node in soup.select("input.hdata_link"):
        raw_value = input_node.get("value")
        if raw_value is None or not str(raw_value).strip():
            continue
        value = str(raw_value).strip()
        if value not in links:
            links.append(value)
    return links


def _decode_response_text(response: httpx.Response) -> str:
    encoding = response.encoding or "windows-1251"
    try:
        return response.content.decode(encoding)
    except UnicodeDecodeError:
        return response.content.decode("windows-1251", errors="replace")


def _write_download(
    download_dir: Path,
    hdata_link: str,
    response: httpx.Response,
) -> None:
    relative = hdata_link.replace("/", "_")
    filename = f"{relative}.xls"
    download_dir.mkdir(parents=True, exist_ok=True)
    (download_dir / filename).write_bytes(response.content)


def _delivery_dates(
    *,
    explicit: Sequence[str],
    from_date: str | None,
    to_date: str | None,
) -> list[date]:
    dates = [_delivery_date(value) for value in explicit]
    if from_date or to_date:
        if not from_date or not to_date:
            raise ValueError("--from-date and --to-date must be provided together.")
        start = _delivery_date(from_date)
        end = _delivery_date(to_date)
        if end < start:
            raise ValueError("--to-date must be on or after --from-date.")
        current = start
        while current <= end:
            dates.append(current)
            current += timedelta(days=1)
    unique = sorted(set(dates))
    if not unique:
        raise ValueError("At least one delivery date is required.")
    return unique


def _delivery_date(value: str) -> date:
    stripped = value.strip()
    for date_format in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(stripped, date_format).date()
        except ValueError:
            continue
    raise ValueError("delivery date must use YYYY-MM-DD or DD.MM.YYYY format.")


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
