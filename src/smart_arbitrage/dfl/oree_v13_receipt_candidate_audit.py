from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
import hashlib
import json
import re
from typing import Any, Final, Literal

OREE_V13_RECEIPT_CANDIDATE_AUDIT_CLAIM_SCOPE: Final[str] = (
    "oree_v13_receipt_candidate_audit_not_receipt"
)

OreeV13Verdict = Literal[
    "valid_receipt",
    "price_only",
    "observation_only",
    "lead_only",
]

_VERDICT_ORDER: Final[tuple[OreeV13Verdict, ...]] = (
    "valid_receipt",
    "price_only",
    "observation_only",
    "lead_only",
)

_PRICE_ONLY_ARTIFACT_KINDS: Final[frozenset[str]] = frozenset(
    {
        "pricectr_data_view",
        "pricectr_get_file",
        "indexes_data_view",
        "indexes_downloadfile",
    }
)
_OBSERVATION_ONLY_ARTIFACT_KINDS: Final[frozenset[str]] = frozenset(
    {
        "pxs_results",
        "pxs_hdata",
        "pxs_downloadxlsx",
    }
)

_EXPLICIT_PUBLICATION_TOKENS: Final[tuple[str, ...]] = (
    "source_publication_timestamp",
    "source_publication_datetime",
    "publication_timestamp",
    "publication_datetime",
    "published_at",
    "publish_time",
    "date-time",
    "дата оприлюднення",
    "час оприлюднення",
    "дата публікації",
    "час публікації",
)
_DELIVERY_TOKENS: Final[tuple[str, ...]] = (
    "timestamp",
    "delivery_date",
    "delivery_timestamp",
    "дата",
    "година",
    "hour",
)
_DATE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(?:\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4})\b"
)


def build_oree_v13_receipt_candidate_artifact(
    *,
    artifact_kind: str,
    source_url: str,
    market: str,
    month: str,
    delivery_date: str | None,
    status_code: int | None,
    response_headers: Mapping[str, str | Sequence[str]],
    response_content: bytes,
    retrieved_at: datetime,
) -> dict[str, Any]:
    """Classify one OREE public artifact without promoting it into receipt rows."""

    headers = _normalize_headers(response_headers)
    content_type = headers.get("content-type", "")
    content_disposition = headers.get("content-disposition", "")
    download_filename = _filename_from_content_disposition(content_disposition)
    text = _artifact_detection_text(response_content, content_type=content_type)
    publication_token_hits = _explicit_publication_token_hits(text)
    delivery_token_hits = _delivery_token_hits(
        text,
        artifact_kind=artifact_kind,
        month=month,
        delivery_date=delivery_date,
        download_filename=download_filename,
    )
    explicit_publication_timestamps_found = bool(publication_token_hits)
    delivery_timestamps_found = bool(delivery_token_hits)
    hourly_result_rows_found = _hourly_result_rows_found(text, artifact_kind=artifact_kind)
    verdict, reason = _classify_v13_verdict(
        artifact_kind=artifact_kind,
        fetch_status=_fetch_status(status_code),
        delivery_timestamps_found=delivery_timestamps_found,
        explicit_publication_timestamps_found=explicit_publication_timestamps_found,
    )

    return {
        "claim_scope": OREE_V13_RECEIPT_CANDIDATE_AUDIT_CLAIM_SCOPE,
        "artifact_kind": artifact_kind,
        "source_url": source_url,
        "market": market,
        "month": month,
        "delivery_date": delivery_date or "",
        "status": _fetch_status(status_code),
        "fetch_status": _fetch_status(status_code),
        "status_code": status_code,
        "content_type": content_type,
        "download_filename": download_filename,
        "content_length_bytes": len(response_content),
        "content_sha256": hashlib.sha256(response_content).hexdigest(),
        "http_date_header_present": "date" in headers,
        "http_last_modified_present": "last-modified" in headers,
        "delivery_timestamps_found": delivery_timestamps_found,
        "delivery_token_hits": delivery_token_hits,
        "explicit_publication_timestamps_found": explicit_publication_timestamps_found,
        "publication_token_hits": publication_token_hits,
        "hourly_result_rows_found": hourly_result_rows_found,
        "retrieved_at": retrieved_at.isoformat(),
        "v13_verdict": verdict,
        "verdict_reason": reason,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def summarize_oree_v13_receipt_candidate_audit(
    artifact_rows: Sequence[Mapping[str, Any]],
    *,
    requested_month: str,
    requested_delivery_date: str,
) -> dict[str, Any]:
    """Summarize OREE candidate artifacts without creating V13 receipt rows."""

    rows = [dict(row) for row in artifact_rows]
    verdict_counts = _verdict_counts(rows)
    valid_receipt_rows = [
        row for row in rows if row.get("v13_verdict") == "valid_receipt"
    ]
    return {
        "claim_scope": OREE_V13_RECEIPT_CANDIDATE_AUDIT_CLAIM_SCOPE,
        "requested_month": requested_month,
        "requested_delivery_date": requested_delivery_date,
        "artifact_count": len(rows),
        "verdict_counts": verdict_counts,
        "valid_receipt_artifact_count": len(valid_receipt_rows),
        "candidate_receipt_source_found": bool(valid_receipt_rows),
        "valid_receipt_source_urls": sorted(
            {str(row.get("source_url", "")) for row in valid_receipt_rows}
        ),
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "not_full_dfl": True,
        "not_market_execution": True,
        "artifact_rows": rows,
    }


def _normalize_headers(
    response_headers: Mapping[str, str | Sequence[str]],
) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in response_headers.items():
        if isinstance(value, str):
            normalized[key.casefold()] = value
        else:
            normalized[key.casefold()] = ",".join(str(item) for item in value)
    return normalized


def _artifact_detection_text(response_content: bytes, *, content_type: str) -> str:
    if _is_probably_text(content_type, response_content):
        return _decode_text(response_content)
    return _binary_strings(response_content)


def _is_probably_text(content_type: str, response_content: bytes) -> bool:
    normalized = content_type.casefold()
    if any(token in normalized for token in ("text/", "json", "xml", "csv", "html")):
        return True
    sample = response_content[:512]
    return b"\x00" not in sample and all(
        byte in b"\t\r\n" or 32 <= byte <= 126 or byte >= 128 for byte in sample
    )


def _decode_text(response_content: bytes) -> str:
    for encoding in ("utf-8", "windows-1251"):
        try:
            return response_content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return response_content.decode("utf-8", errors="replace")


def _binary_strings(response_content: bytes) -> str:
    ascii_parts: list[str] = []
    current: list[str] = []
    for byte in response_content:
        if 32 <= byte <= 126:
            current.append(chr(byte))
        else:
            if len(current) >= 4:
                ascii_parts.append("".join(current))
            current = []
    if len(current) >= 4:
        ascii_parts.append("".join(current))

    utf16_parts: list[str] = []
    current_utf16: list[str] = []
    for index in range(0, max(len(response_content) - 1, 0), 2):
        codepoint = response_content[index] + (response_content[index + 1] << 8)
        if codepoint in {9, 10, 13} or 32 <= codepoint <= 0x04FF:
            character = chr(codepoint)
            if character.isprintable() or character in "\t\r\n":
                current_utf16.append(character)
                continue
        if len(current_utf16) >= 4:
            utf16_parts.append("".join(current_utf16))
        current_utf16 = []
    if len(current_utf16) >= 4:
        utf16_parts.append("".join(current_utf16))

    return "\n".join(ascii_parts + utf16_parts)


def _explicit_publication_token_hits(text: str) -> list[str]:
    normalized = _normalized_text(text)
    return [
        token
        for token in _EXPLICIT_PUBLICATION_TOKENS
        if token.casefold() in normalized
    ]


def _delivery_token_hits(
    text: str,
    *,
    artifact_kind: str,
    month: str,
    delivery_date: str | None,
    download_filename: str,
) -> list[str]:
    normalized = _normalized_text(text)
    hits = [
        token
        for token in _DELIVERY_TOKENS
        if token.casefold() in normalized
    ]
    if _DATE_PATTERN.search(text) or _DATE_PATTERN.search(download_filename):
        hits.append("date_literal")
    if delivery_date and artifact_kind in _OBSERVATION_ONLY_ARTIFACT_KINDS:
        hits.append("delivery_date_argument")
    if month and month in download_filename:
        hits.append("download_filename_delivery_period")
    if artifact_kind in _PRICE_ONLY_ARTIFACT_KINDS and month and month in text:
        hits.append("requested_month")
    return _dedupe(hits)


def _hourly_result_rows_found(text: str, *, artifact_kind: str) -> bool:
    normalized = _normalized_text(text)
    if artifact_kind in {"pxs_hdata", "pxs_downloadxlsx"}:
        return True
    return any(token in normalized for token in ("година", "hour", "pricesdata"))


def _classify_v13_verdict(
    *,
    artifact_kind: str,
    fetch_status: str,
    delivery_timestamps_found: bool,
    explicit_publication_timestamps_found: bool,
) -> tuple[OreeV13Verdict, str]:
    if fetch_status != "http_ok":
        return "lead_only", "artifact_fetch_failed_or_non_2xx"
    if delivery_timestamps_found and explicit_publication_timestamps_found:
        return (
            "valid_receipt",
            "explicit_source_publication_timestamp_and_delivery_timestamp_found",
        )
    if artifact_kind in _PRICE_ONLY_ARTIFACT_KINDS:
        return "price_only", "price_or_index_rows_without_publication_timestamp"
    if artifact_kind in _OBSERVATION_ONLY_ARTIFACT_KINDS:
        return (
            "observation_only",
            "row_level_result_observed_without_publication_timestamp",
        )
    return "lead_only", "no_row_level_publication_receipt_metadata"


def _fetch_status(status_code: int | None) -> str:
    if status_code is None:
        return "fetch_error"
    if 200 <= status_code < 300:
        return "http_ok"
    return "http_error"


def _filename_from_content_disposition(content_disposition: str) -> str:
    if not content_disposition:
        return ""
    quoted = re.search(r'filename\*?=(?:"([^"]+)"|([^;]+))', content_disposition)
    if quoted is None:
        return ""
    return (quoted.group(1) or quoted.group(2) or "").strip()


def _normalized_text(text: str) -> str:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict):
        text = "\n".join(_json_leaf_strings(payload))
    return text.casefold()


def _json_leaf_strings(value: Any) -> list[str]:
    if isinstance(value, dict):
        output: list[str] = []
        for key, item in value.items():
            output.append(str(key))
            output.extend(_json_leaf_strings(item))
        return output
    if isinstance(value, list):
        output = []
        for item in value:
            output.extend(_json_leaf_strings(item))
        return output
    return [str(value)]


def _dedupe(values: Sequence[str]) -> list[str]:
    output: list[str] = []
    for value in values:
        if value not in output:
            output.append(value)
    return output


def _verdict_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for verdict in _VERDICT_ORDER:
        count = sum(1 for row in rows if row.get("v13_verdict") == verdict)
        if count:
            counts[verdict] = count
    return counts


__all__ = [
    "OREE_V13_RECEIPT_CANDIDATE_AUDIT_CLAIM_SCOPE",
    "build_oree_v13_receipt_candidate_artifact",
    "summarize_oree_v13_receipt_candidate_audit",
]
