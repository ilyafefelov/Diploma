"""Audit public OREE artifacts as V13 receipt candidates without promotion."""

from __future__ import annotations

import argparse
import csv
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
import json
from pathlib import Path
import time
from typing import Any, Final

from bs4 import BeautifulSoup
import httpx

from smart_arbitrage.dfl.oree_v13_receipt_candidate_audit import (
    build_oree_v13_receipt_candidate_artifact,
    summarize_oree_v13_receipt_candidate_audit,
)

OREE_BASE_URL: Final[str] = "https://www.oree.com.ua/index.php"
OREE_EU_PRICES_URL: Final[str] = f"{OREE_BASE_URL}/eu_prices"
OREE_IDM_GRAPHS_URL: Final[str] = f"{OREE_BASE_URL}/IDM_graphs"
OREE_PRICETR_URL: Final[str] = f"{OREE_BASE_URL}/pricectr"
OREE_PRICETR_DATA_VIEW_URL: Final[str] = f"{OREE_BASE_URL}/pricectr/data_view"
OREE_PRICETR_GET_FILE_URL: Final[str] = f"{OREE_BASE_URL}/pricectr/get_file"
OREE_INDEXES_URL: Final[str] = f"{OREE_BASE_URL}/indexes"
OREE_INDEXES_DATA_VIEW_URL: Final[str] = f"{OREE_BASE_URL}/indexes/data_view"
OREE_INDEXES_DOWNLOAD_URL: Final[str] = f"{OREE_BASE_URL}/indexes/downloadfile"
OREE_CONTROL_RESULTS_URLS: Final[dict[str, str]] = {
    "DAM": f"{OREE_BASE_URL}/control/results_mo/DAM",
    "IDM": f"{OREE_BASE_URL}/control/results_mo/IDM",
}
OREE_PXS_RESULTS_URLS: Final[dict[str, str]] = {
    "DAM": f"{OREE_BASE_URL}/PXS/get_pxs_res",
    "IDM": f"{OREE_BASE_URL}/PXS/get_pxs_res_idm",
}
OREE_PXS_HDATA_URL_PREFIX: Final[str] = f"{OREE_BASE_URL}/PXS/get_pxs_hdata"
OREE_PXS_DOWNLOAD_URL_PREFIX: Final[str] = f"{OREE_BASE_URL}/PXS/downloadxlsx"

_CSV_COLUMNS: Final[tuple[str, ...]] = (
    "artifact_kind",
    "source_url",
    "market",
    "month",
    "delivery_date",
    "status",
    "status_code",
    "content_type",
    "download_filename",
    "http_last_modified_present",
    "delivery_timestamps_found",
    "explicit_publication_timestamps_found",
    "hourly_result_rows_found",
    "v13_verdict",
    "verdict_reason",
    "publication_token_hits",
    "delivery_token_hits",
    "content_length_bytes",
    "content_sha256",
    "retrieved_at",
    "validated_receipt_csv_ready",
    "permits_model_training",
    "market_execution_enabled",
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit public OREE pages, AJAX endpoints, and downloads as V13 "
            "receipt candidates. This writes evidence only; it does not emit "
            "receipt rows and does not permit DT/LAVA training or market execution."
        )
    )
    parser.add_argument("--month", required=True, help="OREE month, e.g. 05.2026.")
    parser.add_argument(
        "--delivery-date",
        required=True,
        help="Daily DAM/IDM target as YYYY-MM-DD or DD.MM.YYYY.",
    )
    parser.add_argument("--zone", default="IPS", help="OREE zone, default: IPS.")
    parser.add_argument("--timeout-seconds", type=float, default=45.0)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, default=None)
    args = parser.parse_args(argv)

    artifact_rows = _fetch_oree_v13_receipt_candidate_artifacts(
        month=args.month,
        delivery_date=args.delivery_date,
        zone=args.zone,
        timeout_seconds=args.timeout_seconds,
    )
    audit = summarize_oree_v13_receipt_candidate_audit(
        artifact_rows,
        requested_month=args.month,
        requested_delivery_date=args.delivery_date,
    )

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(
        json.dumps(_json_ready(audit), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if args.output_csv is not None:
        args.output_csv.parent.mkdir(parents=True, exist_ok=True)
        _write_artifact_csv(args.output_csv, artifact_rows)
    print(f"Wrote OREE V13 receipt candidate audit: {args.output_json}")
    if args.output_csv is not None:
        print(f"Wrote OREE V13 receipt candidate artifact rows: {args.output_csv}")
    return 0


def _fetch_oree_v13_receipt_candidate_artifacts(
    *,
    month: str,
    delivery_date: str,
    zone: str,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    with httpx.Client(
        timeout=timeout_seconds,
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0",
        },
    ) as client:
        _append_get_artifact(
            artifacts,
            client,
            artifact_kind="eu_prices_page",
            source_url=OREE_EU_PRICES_URL,
            market="EU_PRICES",
            month=month,
            delivery_date=delivery_date,
        )
        _append_get_artifact(
            artifacts,
            client,
            artifact_kind="idm_graphs_page",
            source_url=OREE_IDM_GRAPHS_URL,
            market="IDM",
            month=month,
            delivery_date=delivery_date,
        )
        _append_get_artifact(
            artifacts,
            client,
            artifact_kind="pricectr_page",
            source_url=OREE_PRICETR_URL,
            market="DAM_IDM",
            month=month,
            delivery_date=delivery_date,
        )
        for market in ("DAM", "IDM"):
            _append_post_artifact(
                artifacts,
                client,
                artifact_kind="pricectr_data_view",
                source_url=OREE_PRICETR_DATA_VIEW_URL,
                market=market,
                month=month,
                delivery_date=None,
                data={"date": month, "market": market, "zone": zone},
                referer=OREE_PRICETR_URL,
            )
            _append_pricectr_get_file_artifact(
                artifacts,
                market=market,
                month=month,
                zone=zone,
                timeout_seconds=timeout_seconds,
            )
        _append_get_artifact(
            artifacts,
            client,
            artifact_kind="indexes_page",
            source_url=OREE_INDEXES_URL,
            market="INDEXES",
            month=month,
            delivery_date=delivery_date,
        )
        _append_post_artifact(
            artifacts,
            client,
            artifact_kind="indexes_data_view",
            source_url=OREE_INDEXES_DATA_VIEW_URL,
            market="INDEXES",
            month=month,
            delivery_date=None,
            data={"date": month, "market": "DAM", "zone": zone},
            referer=OREE_INDEXES_URL,
        )
        _append_get_artifact(
            artifacts,
            client,
            artifact_kind="indexes_downloadfile",
            source_url=f"{OREE_INDEXES_DOWNLOAD_URL}?date={month}&val={zone}",
            market="INDEXES",
            month=month,
            delivery_date=None,
        )
        for market, results_page_url in OREE_CONTROL_RESULTS_URLS.items():
            _append_get_artifact(
                artifacts,
                client,
                artifact_kind="control_results_page",
                source_url=results_page_url,
                market=market,
                month=month,
                delivery_date=delivery_date,
            )
            hdata_links = _append_pxs_results_artifact(
                artifacts,
                client,
                market=market,
                month=month,
                delivery_date=delivery_date,
                referer=results_page_url,
            )
            for hdata_link in hdata_links:
                _append_pxs_hdata_artifacts(
                    artifacts,
                    client,
                    hdata_link=hdata_link,
                    market=market,
                    month=month,
                    delivery_date=delivery_date,
                    referer=results_page_url,
                )
    return artifacts


def _append_get_artifact(
    artifacts: list[dict[str, Any]],
    client: httpx.Client,
    *,
    artifact_kind: str,
    source_url: str,
    market: str,
    month: str,
    delivery_date: str | None,
) -> None:
    try:
        response = client.get(source_url)
        artifacts.append(
            _artifact_from_response(
                artifact_kind=artifact_kind,
                source_url=source_url,
                market=market,
                month=month,
                delivery_date=delivery_date,
                response=response,
            )
        )
    except httpx.HTTPError as error:
        artifacts.append(
            _error_artifact(
                artifact_kind=artifact_kind,
                source_url=source_url,
                market=market,
                month=month,
                delivery_date=delivery_date,
                error=error,
            )
        )


def _append_post_artifact(
    artifacts: list[dict[str, Any]],
    client: httpx.Client,
    *,
    artifact_kind: str,
    source_url: str,
    market: str,
    month: str,
    delivery_date: str | None,
    data: Mapping[str, str],
    referer: str,
) -> dict[str, Any] | None:
    try:
        response = client.post(source_url, data=data, headers={"Referer": referer})
        artifact = _artifact_from_response(
            artifact_kind=artifact_kind,
            source_url=source_url,
            market=market,
            month=month,
            delivery_date=delivery_date,
            response=response,
        )
        artifacts.append(artifact)
        return artifact
    except httpx.HTTPError as error:
        artifacts.append(
            _error_artifact(
                artifact_kind=artifact_kind,
                source_url=source_url,
                market=market,
                month=month,
                delivery_date=delivery_date,
                error=error,
            )
        )
        return None


def _append_pricectr_get_file_artifact(
    artifacts: list[dict[str, Any]],
    *,
    market: str,
    month: str,
    zone: str,
    timeout_seconds: float,
) -> None:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": OREE_PRICETR_URL,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    try:
        response: httpx.Response | None = None
        for attempt_index in range(3):
            response = httpx.post(
                OREE_PRICETR_GET_FILE_URL,
                data={"price_date": month, "market_type": market, "zone": zone},
                headers=headers,
                follow_redirects=True,
                timeout=timeout_seconds,
            )
            if _looks_like_excel_download(response):
                break
            if attempt_index < 2:
                time.sleep(0.5 * (attempt_index + 1))
        if response is None:
            raise RuntimeError("OREE pricectr/get_file response was not captured.")
        artifacts.append(
            _artifact_from_response(
                artifact_kind="pricectr_get_file",
                source_url=OREE_PRICETR_GET_FILE_URL,
                market=market,
                month=month,
                delivery_date=None,
                response=response,
            )
        )
    except httpx.HTTPError as error:
        artifacts.append(
            _error_artifact(
                artifact_kind="pricectr_get_file",
                source_url=OREE_PRICETR_GET_FILE_URL,
                market=market,
                month=month,
                delivery_date=None,
                error=error,
            )
        )


def _looks_like_excel_download(response: httpx.Response) -> bool:
    content_type = response.headers.get("content-type", "").casefold()
    content_disposition = response.headers.get("content-disposition", "")
    return "excel" in content_type or "filename=" in content_disposition.casefold()


def _append_pxs_results_artifact(
    artifacts: list[dict[str, Any]],
    client: httpx.Client,
    *,
    market: str,
    month: str,
    delivery_date: str,
    referer: str,
) -> list[str]:
    source_url = OREE_PXS_RESULTS_URLS[market]
    try:
        response = client.post(
            source_url,
            data={"day": _oree_day_label(delivery_date)},
            headers={"Referer": referer},
        )
    except httpx.HTTPError as error:
        artifacts.append(
            _error_artifact(
                artifact_kind="pxs_results",
                source_url=source_url,
                market=market,
                month=month,
                delivery_date=delivery_date,
                error=error,
            )
        )
        return []
    artifacts.append(
        _artifact_from_response(
            artifact_kind="pxs_results",
            source_url=source_url,
            market=market,
            month=month,
            delivery_date=delivery_date,
            response=response,
        )
    )
    return _hdata_links(_decode_response_text(response))


def _append_pxs_hdata_artifacts(
    artifacts: list[dict[str, Any]],
    client: httpx.Client,
    *,
    hdata_link: str,
    market: str,
    month: str,
    delivery_date: str,
    referer: str,
) -> None:
    hdata_url = f"{OREE_PXS_HDATA_URL_PREFIX}/{hdata_link}"
    _append_post_artifact(
        artifacts,
        client,
        artifact_kind="pxs_hdata",
        source_url=hdata_url,
        market=market,
        month=month,
        delivery_date=delivery_date,
        data={},
        referer=referer,
    )
    _append_get_artifact(
        artifacts,
        client,
        artifact_kind="pxs_downloadxlsx",
        source_url=f"{OREE_PXS_DOWNLOAD_URL_PREFIX}/{hdata_link}",
        market=market,
        month=month,
        delivery_date=delivery_date,
    )


def _artifact_from_response(
    *,
    artifact_kind: str,
    source_url: str,
    market: str,
    month: str,
    delivery_date: str | None,
    response: httpx.Response,
) -> dict[str, Any]:
    return build_oree_v13_receipt_candidate_artifact(
        artifact_kind=artifact_kind,
        source_url=source_url,
        market=market,
        month=month,
        delivery_date=delivery_date,
        status_code=response.status_code,
        response_headers=response.headers,
        response_content=response.content,
        retrieved_at=datetime.now(UTC),
    )


def _error_artifact(
    *,
    artifact_kind: str,
    source_url: str,
    market: str,
    month: str,
    delivery_date: str | None,
    error: Exception,
) -> dict[str, Any]:
    return build_oree_v13_receipt_candidate_artifact(
        artifact_kind=artifact_kind,
        source_url=source_url,
        market=market,
        month=month,
        delivery_date=delivery_date,
        status_code=None,
        response_headers={},
        response_content=f"{type(error).__name__}: {error}".encode(),
        retrieved_at=datetime.now(UTC),
    )


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


def _oree_day_label(value: str) -> str:
    stripped = value.strip()
    if "." in stripped:
        return stripped
    parsed = datetime.strptime(stripped, "%Y-%m-%d")
    return parsed.strftime("%d.%m.%Y")


def _write_artifact_csv(path: Path, artifact_rows: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=_CSV_COLUMNS)
        writer.writeheader()
        for row in artifact_rows:
            writer.writerow({column: _csv_value(row.get(column)) for column in _CSV_COLUMNS})


def _csv_value(value: Any) -> str | int | bool | None:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


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
