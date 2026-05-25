"""Fetch authenticated SCMO DAM receipt exports without crossing V13 gates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from smart_arbitrage.dfl.scmo_dam_receipt_access import (
    build_scmo_dam_publication_receipt_access_probe,
)
from smart_arbitrage.dfl.scmo_dam_receipt_export import (
    SCMO_AUTO_COLUMN,
    ScmoReceiptExportFormat,
    normalize_scmo_dam_publication_receipt_export_frame,
    read_scmo_dam_receipt_export_bytes,
)

SCMO_DAM_RECEIPT_EXPORT_FETCH_CLAIM_SCOPE: Final[str] = (
    "scmo_dam_receipt_export_fetch_not_market_execution"
)


@dataclass(frozen=True)
class ScmoExportResponse:
    source_url: str
    final_url: str
    status_code: int
    content_type: str
    body: bytes


def fetch_result_from_scmo_export_response(
    response: ScmoExportResponse,
    *,
    raw_output_path: str | Path | None = None,
    normalized_output_path: str | Path | None = None,
    timestamp_column: str = SCMO_AUTO_COLUMN,
    source_publication_timestamp_column: str = SCMO_AUTO_COLUMN,
    receipt_id_column: str | None = None,
    input_format: ScmoReceiptExportFormat = "auto",
) -> dict[str, Any]:
    """Validate and optionally persist an authenticated SCMO export response."""

    text = _decode_body(response.body, response.content_type)
    access_probe = build_scmo_dam_publication_receipt_access_probe(
        source_url=response.source_url,
        final_url=response.final_url,
        status_code=response.status_code,
        content_type=response.content_type,
        response_text=text,
        retrieved_at=datetime.now(UTC),
    )
    if bool(access_probe["auth_required"]):
        raise ValueError(
            "SCMO export response is blocked by auth_required_sso_login; "
            "provide a valid authenticated export URL/cookie."
        )
    if response.status_code >= 400:
        raise ValueError(f"SCMO export response failed with HTTP {response.status_code}.")

    raw_path = Path(raw_output_path) if raw_output_path is not None else None
    if raw_path is not None:
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(response.body)

    normalized_rows = 0
    normalized_path = (
        Path(normalized_output_path) if normalized_output_path is not None else None
    )
    if normalized_path is not None:
        raw_frame = read_scmo_dam_receipt_export_bytes(
            response.body,
            input_format=input_format,
            content_type=response.content_type,
            source_name=(raw_path.name if raw_path is not None else response.final_url),
        )
        normalized = normalize_scmo_dam_publication_receipt_export_frame(
            raw_frame,
            timestamp_column=timestamp_column,
            source_publication_timestamp_column=source_publication_timestamp_column,
            receipt_id_column=receipt_id_column,
        )
        normalized_path.parent.mkdir(parents=True, exist_ok=True)
        normalized.write_csv(normalized_path)
        normalized_rows = normalized.height

    return {
        "claim_scope": SCMO_DAM_RECEIPT_EXPORT_FETCH_CLAIM_SCOPE,
        "source_url": response.source_url,
        "final_url": response.final_url,
        "status_code": response.status_code,
        "content_type": response.content_type,
        "raw_export_written": raw_path is not None,
        "raw_export_path": str(raw_path) if raw_path is not None else None,
        "normalized_receipts_written": normalized_path is not None,
        "normalized_receipts_csv": (
            str(normalized_path) if normalized_path is not None else None
        ),
        "receipt_rows": normalized_rows,
        "validated_receipt_csv_ready": normalized_path is not None
        and normalized_rows > 0,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }


def _decode_body(body: bytes, content_type: str) -> str:
    encoding = "utf-8"
    marker = "charset="
    if marker in content_type.casefold():
        encoding = content_type.casefold().split(marker, 1)[1].split(";", 1)[0].strip()
    try:
        return body.decode(encoding)
    except (LookupError, UnicodeDecodeError):
        return body.decode("utf-8", errors="replace")


__all__ = [
    "SCMO_DAM_RECEIPT_EXPORT_FETCH_CLAIM_SCOPE",
    "ScmoExportResponse",
    "fetch_result_from_scmo_export_response",
]
