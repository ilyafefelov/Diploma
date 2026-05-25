"""Capture OREE DAM download observations for receipt-source discovery."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
import re
import struct
from typing import Any, Final

import polars as pl

CLAIM_SCOPE: Final[str] = "oree_dam_download_observation_not_v13_receipt"
DEFAULT_SOURCE_TITLE: Final[str] = "OREE PXS DAM downloadxlsx endpoint"
MISSING_PUBLICATION_STATUS: Final[str] = (
    "download_observed_without_source_publication_timestamp"
)
LAST_MODIFIED_STATUS: Final[str] = (
    "download_last_modified_header_present_manual_review_required"
)
DOWNLOAD_URL_PREFIX: Final[str] = (
    "https://www.oree.com.ua/index.php/PXS/downloadxlsx"
)

_SUMMARY_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "timestamp",
        "hdata_link",
        "download_sha256",
        "receipt_candidate_status",
        "source_last_modified_utc",
        "workbook_summary_filetime_status",
        "validated_receipt_csv_ready",
        "permits_model_training",
        "market_execution_enabled",
    }
)

_OLE_FREE_SECTOR: Final[int] = 0xFFFFFFFF
_OLE_END_OF_CHAIN: Final[int] = 0xFFFFFFFE
_OLE_MINI_STREAM_CUTOFF: Final[int] = 4096
_SUMMARY_STREAM_NAME: Final[str] = "\x05SummaryInformation"
_SUMMARY_PID_CREATE_TIME: Final[int] = 12
_SUMMARY_PID_LAST_SAVE_TIME: Final[int] = 13
_VT_FILETIME: Final[int] = 64


def build_oree_dam_download_observation_frame(
    *,
    delivery_date: str | date,
    hdata_link: str,
    hdata_payload: Mapping[str, Any],
    hdata_headers: Mapping[str, str | Sequence[str]],
    download_headers: Mapping[str, str | Sequence[str]],
    download_content: bytes,
    retrieved_at: datetime,
    source_url: str,
    source_title: str = DEFAULT_SOURCE_TITLE,
) -> pl.DataFrame:
    """Build row-level observations from OREE PXS hdata and XLS download."""

    parsed_delivery_date = _delivery_date(delivery_date)
    labels = _labels(hdata_payload, row_count=len(_sequence(hdata_payload, "pricesData")))
    prices = _sequence(hdata_payload, "pricesData")
    amounts = _sequence(hdata_payload, "amountsData")
    hdata_headers_norm = _normalize_headers(hdata_headers)
    download_headers_norm = _normalize_headers(download_headers)
    download_http_date = _http_datetime(download_headers_norm.get("date"))
    hdata_http_date = _http_datetime(hdata_headers_norm.get("date"))
    last_modified = _http_datetime(download_headers_norm.get("last-modified"))
    content_disposition = download_headers_norm.get("content-disposition", "")
    filename = _filename_from_content_disposition(content_disposition)
    status = LAST_MODIFIED_STATUS if last_modified is not None else MISSING_PUBLICATION_STATUS
    sha256 = hashlib.sha256(download_content).hexdigest()
    workbook_metadata = extract_oree_dam_xls_summary_metadata(download_content)
    retrieved_at_utc = _utc_datetime(retrieved_at)
    rows: list[dict[str, object]] = []
    for row_index, label in enumerate(labels):
        hour = _hour_index(label)
        timestamp = datetime.combine(
            parsed_delivery_date,
            datetime.min.time().replace(hour=hour - 1),
        )
        rows.append(
            {
                "timestamp": timestamp,
                "delivery_date": parsed_delivery_date.isoformat(),
                "delivery_hour": hour,
                "price_uah_mwh": _optional_float(prices[row_index]),
                "volume_mwh": _optional_float(amounts[row_index])
                if row_index < len(amounts)
                else None,
                "hdata_link": hdata_link,
                "hdata_http_date_utc": _iso_or_none(hdata_http_date),
                "download_url": f"{DOWNLOAD_URL_PREFIX}/{hdata_link}",
                "download_filename": filename,
                "download_content_type": download_headers_norm.get("content-type", ""),
                "download_content_length": len(download_content),
                "download_sha256": sha256,
                "download_http_date_utc": _iso_or_none(download_http_date),
                "source_last_modified_utc": _iso_or_none(last_modified),
                "workbook_summary_created_at": workbook_metadata[
                    "workbook_summary_created_at"
                ],
                "workbook_summary_last_saved_at": workbook_metadata[
                    "workbook_summary_last_saved_at"
                ],
                "workbook_summary_filetime_status": workbook_metadata[
                    "workbook_summary_filetime_status"
                ],
                "source_observed_at_utc": retrieved_at_utc.isoformat(),
                "source_url": source_url,
                "source_title": source_title,
                "receipt_candidate_status": status,
                "can_satisfy_v13_explicit_receipts": False,
                "receipt_csv_generated": False,
                "validated_receipt_csv_ready": False,
                "dt_lava_ready": False,
                "permits_model_training": False,
                "market_execution_enabled": False,
                "claim_scope": CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return (
        pl.DataFrame(rows, infer_schema_length=None)
        .with_columns(
            pl.col("hdata_http_date_utc").cast(pl.Utf8),
            pl.col("download_http_date_utc").cast(pl.Utf8),
            pl.col("source_last_modified_utc").cast(pl.Utf8),
            pl.col("workbook_summary_created_at").cast(pl.Utf8),
            pl.col("workbook_summary_last_saved_at").cast(pl.Utf8),
            pl.col("workbook_summary_filetime_status").cast(pl.Utf8),
        )
        .sort(["delivery_date", "delivery_hour"])
    )


def extract_oree_dam_xls_summary_metadata(download_content: bytes) -> dict[str, str | None]:
    """Extract OLE SummaryInformation timestamps from OREE XLS bytes if present."""

    try:
        filetimes = _extract_ole_summary_filetimes(download_content)
    except (ValueError, IndexError, struct.error, UnicodeDecodeError):
        filetimes = {}
    created_at = filetimes.get(_SUMMARY_PID_CREATE_TIME)
    last_saved_at = filetimes.get(_SUMMARY_PID_LAST_SAVE_TIME)
    status = (
        "ole_summary_filetimes_present_not_publication_receipt"
        if created_at or last_saved_at
        else "ole_summary_filetimes_missing"
    )
    return {
        "workbook_summary_created_at": created_at,
        "workbook_summary_last_saved_at": last_saved_at,
        "workbook_summary_filetime_status": status,
    }


def summarize_oree_dam_download_observation_frame(frame: pl.DataFrame) -> dict[str, Any]:
    """Summarize OREE download observations without converting them to receipts."""

    if frame.height == 0:
        return {
            "claim_scope": CLAIM_SCOPE,
            "observation_rows": 0,
            "download_file_count": 0,
            "last_modified_header_rows": 0,
            "candidate_receipt_source_found": False,
            "receipt_csv_generated": False,
            "validated_receipt_csv_ready": False,
            "dt_lava_ready": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
        }
    _require_columns(frame, _SUMMARY_REQUIRED_COLUMNS)
    _refuse_true(frame, "validated_receipt_csv_ready")
    _refuse_true(frame, "permits_model_training")
    _refuse_true(frame, "market_execution_enabled")
    last_modified_rows = frame.filter(
        pl.col("source_last_modified_utc").is_not_null()
        & (pl.col("source_last_modified_utc").str.len_chars() > 0)
    ).height
    statuses = sorted(str(value) for value in frame["receipt_candidate_status"].unique())
    workbook_statuses = sorted(
        str(value) for value in frame["workbook_summary_filetime_status"].unique()
    )
    workbook_filetime_rows = frame.filter(
        (
            pl.col("workbook_summary_created_at").is_not_null()
            & (pl.col("workbook_summary_created_at").str.len_chars() > 0)
        )
        | (
            pl.col("workbook_summary_last_saved_at").is_not_null()
            & (pl.col("workbook_summary_last_saved_at").str.len_chars() > 0)
        )
    ).height
    return {
        "claim_scope": CLAIM_SCOPE,
        "observation_rows": frame.height,
        "delivery_date_count": frame.select("delivery_date").unique().height,
        "download_file_count": frame.select("download_sha256").unique().height,
        "first_timestamp": _iso_at(frame, "timestamp", 0),
        "last_timestamp": _iso_at(frame, "timestamp", -1),
        "last_modified_header_rows": last_modified_rows,
        "receipt_candidate_statuses": statuses,
        "workbook_summary_filetime_rows": workbook_filetime_rows,
        "workbook_summary_filetime_statuses": workbook_statuses,
        "candidate_receipt_source_found": False,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _normalize_headers(
    headers: Mapping[str, str | Sequence[str]],
) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in headers.items():
        if isinstance(value, str):
            normalized[key.casefold()] = value
        else:
            normalized[key.casefold()] = ",".join(str(item) for item in value)
    return normalized


def _extract_ole_summary_filetimes(content: bytes) -> dict[int, str]:
    if content[:8] != bytes.fromhex("d0cf11e0a1b11ae1"):
        raise ValueError("not an OLE compound file")
    sector_size = 1 << struct.unpack_from("<H", content, 30)[0]
    mini_sector_size = 1 << struct.unpack_from("<H", content, 32)[0]
    fat_sector_count = struct.unpack_from("<I", content, 44)[0]
    directory_start = struct.unpack_from("<I", content, 48)[0]
    mini_fat_start = struct.unpack_from("<I", content, 60)[0]
    difat = list(struct.unpack_from("<109I", content, 76))
    fat_sector_ids = [
        sector_id
        for sector_id in difat
        if sector_id not in {_OLE_FREE_SECTOR, _OLE_END_OF_CHAIN}
    ][:fat_sector_count]
    fat: list[int] = []
    for sector_id in fat_sector_ids:
        fat.extend(
            struct.unpack_from(
                "<" + "I" * (sector_size // 4),
                content,
                _sector_offset(sector_id, sector_size),
            )
        )
    directory_stream = _read_regular_stream(
        content,
        start_sector=directory_start,
        size=None,
        fat=fat,
        sector_size=sector_size,
    )
    entries = _ole_directory_entries(directory_stream)
    root_entry = entries.get("Root Entry")
    summary_entry = entries.get(_SUMMARY_STREAM_NAME)
    if root_entry is None or summary_entry is None:
        return {}
    if summary_entry["size"] >= _OLE_MINI_STREAM_CUTOFF:
        summary_stream = _read_regular_stream(
            content,
            start_sector=summary_entry["start_sector"],
            size=summary_entry["size"],
            fat=fat,
            sector_size=sector_size,
        )
    else:
        mini_fat = _read_mini_fat(
            content,
            start_sector=mini_fat_start,
            fat=fat,
            sector_size=sector_size,
        )
        mini_stream = _read_regular_stream(
            content,
            start_sector=root_entry["start_sector"],
            size=root_entry["size"],
            fat=fat,
            sector_size=sector_size,
        )
        summary_stream = _read_mini_stream(
            mini_stream,
            start_sector=summary_entry["start_sector"],
            size=summary_entry["size"],
            mini_fat=mini_fat,
            mini_sector_size=mini_sector_size,
        )
    return _property_set_filetimes(summary_stream)


def _sector_offset(sector_id: int, sector_size: int) -> int:
    return (sector_id + 1) * sector_size


def _read_regular_stream(
    content: bytes,
    *,
    start_sector: int,
    size: int | None,
    fat: Sequence[int],
    sector_size: int,
) -> bytes:
    chunks = []
    for sector_id in _sector_chain(start_sector, fat):
        offset = _sector_offset(sector_id, sector_size)
        chunks.append(content[offset : offset + sector_size])
    stream = b"".join(chunks)
    return stream if size is None else stream[:size]


def _sector_chain(start_sector: int, allocation_table: Sequence[int]) -> list[int]:
    chain: list[int] = []
    sector_id = start_sector
    while (
        sector_id not in {_OLE_FREE_SECTOR, _OLE_END_OF_CHAIN}
        and sector_id < len(allocation_table)
        and len(chain) < 10000
    ):
        chain.append(sector_id)
        sector_id = allocation_table[sector_id]
    return chain


def _ole_directory_entries(directory_stream: bytes) -> dict[str, dict[str, int]]:
    entries: dict[str, dict[str, int]] = {}
    for offset in range(0, len(directory_stream), 128):
        entry = directory_stream[offset : offset + 128]
        if len(entry) < 128:
            continue
        name_length = struct.unpack_from("<H", entry, 64)[0]
        if name_length < 2:
            continue
        name = entry[: name_length - 2].decode("utf-16le")
        entries[name] = {
            "start_sector": struct.unpack_from("<I", entry, 116)[0],
            "size": struct.unpack_from("<I", entry, 120)[0],
        }
    return entries


def _read_mini_fat(
    content: bytes,
    *,
    start_sector: int,
    fat: Sequence[int],
    sector_size: int,
) -> list[int]:
    mini_fat_stream = _read_regular_stream(
        content,
        start_sector=start_sector,
        size=None,
        fat=fat,
        sector_size=sector_size,
    )
    return list(
        struct.unpack_from(
            "<" + "I" * (len(mini_fat_stream) // 4),
            mini_fat_stream,
        )
    )


def _read_mini_stream(
    mini_stream: bytes,
    *,
    start_sector: int,
    size: int,
    mini_fat: Sequence[int],
    mini_sector_size: int,
) -> bytes:
    chunks = []
    for sector_id in _sector_chain(start_sector, mini_fat):
        offset = sector_id * mini_sector_size
        chunks.append(mini_stream[offset : offset + mini_sector_size])
    return b"".join(chunks)[:size]


def _property_set_filetimes(summary_stream: bytes) -> dict[int, str]:
    section_count = struct.unpack_from("<I", summary_stream, 24)[0]
    filetimes: dict[int, str] = {}
    for section_index in range(section_count):
        section_offset = struct.unpack_from(
            "<I",
            summary_stream,
            44 + section_index * 20,
        )[0]
        property_count = struct.unpack_from("<I", summary_stream, section_offset + 4)[0]
        for property_index in range(property_count):
            property_id, property_offset = struct.unpack_from(
                "<II",
                summary_stream,
                section_offset + 8 + property_index * 8,
            )
            value_offset = section_offset + property_offset
            value_type = struct.unpack_from("<I", summary_stream, value_offset)[0]
            if value_type != _VT_FILETIME:
                continue
            filetime = struct.unpack_from("<Q", summary_stream, value_offset + 4)[0]
            timestamp = datetime(1601, 1, 1) + timedelta(microseconds=filetime / 10)
            filetimes[property_id] = timestamp.isoformat()
    return filetimes


def _sequence(payload: Mapping[str, Any], key: str) -> Sequence[Any]:
    value = payload.get(key, [])
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"OREE PXS payload field {key!r} must be a sequence.")
    return value


def _labels(payload: Mapping[str, Any], *, row_count: int) -> Sequence[Any]:
    labels = payload.get("labels")
    if labels is None:
        return list(range(1, row_count + 1))
    if not isinstance(labels, Sequence) or isinstance(labels, (str, bytes)):
        raise TypeError("OREE PXS payload field 'labels' must be a sequence.")
    return labels


def _delivery_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    stripped = value.strip()
    for date_format in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(stripped, date_format).date()
        except ValueError:
            continue
    raise ValueError("delivery_date must use YYYY-MM-DD or DD.MM.YYYY format.")


def _hour_index(value: Any) -> int:
    if isinstance(value, int):
        hour = value
    elif isinstance(value, float) and value.is_integer():
        hour = int(value)
    elif isinstance(value, str):
        hour = int(value.strip())
    else:
        raise TypeError(f"Cannot convert {type(value).__name__} to delivery hour.")
    if not 1 <= hour <= 24:
        raise ValueError("OREE PXS delivery hour must be in 1..24.")
    return hour


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (float, int, str)):
        stripped = str(value).strip()
        if not stripped:
            return None
        return float(stripped.replace(",", "."))
    raise TypeError(f"Cannot convert {type(value).__name__} to float.")


def _http_datetime(value: str | None) -> datetime | None:
    if value is None or not value.strip():
        return None
    parsed = parsedate_to_datetime(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _iso_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _filename_from_content_disposition(value: str) -> str:
    if not value:
        return ""
    match = re.search(r'filename="?([^";]+)"?', value)
    return match.group(1) if match else ""


def _require_columns(frame: pl.DataFrame, columns: frozenset[str]) -> None:
    missing = columns.difference(frame.columns)
    if missing:
        raise ValueError(
            f"OREE DAM download observation frame is missing columns: {sorted(missing)}"
        )


def _refuse_true(frame: pl.DataFrame, column_name: str) -> None:
    if frame.is_empty():
        return
    if frame.with_columns(pl.col(column_name).cast(pl.Boolean)).filter(
        pl.col(column_name)
    ).height:
        raise ValueError(
            f"OREE DAM download observation frame contains true {column_name}."
        )


def _iso_at(frame: pl.DataFrame, column_name: str, index: int) -> str | None:
    if frame.is_empty():
        return None
    value = frame[column_name].item(index)
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


__all__ = [
    "CLAIM_SCOPE",
    "build_oree_dam_download_observation_frame",
    "summarize_oree_dam_download_observation_frame",
]
