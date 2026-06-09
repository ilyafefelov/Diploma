"""Probe SCMO DAM receipt-export access without creating receipt rows."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Final

SCMO_DAM_RECEIPT_ACCESS_CLAIM_SCOPE: Final[str] = (
    "scmo_dam_receipt_access_probe_not_receipt"
)
SCMO_DAM_LEAD_ID: Final[str] = "scmo_published_information_of_dam_export"
SCMO_DAM_LEAD_TITLE: Final[str] = (
    "SCMO XMtrade/PXS Published information of DAM export"
)


def build_scmo_dam_publication_receipt_access_probe(
    *,
    source_url: str,
    final_url: str,
    status_code: int,
    content_type: str,
    response_text: str,
    retrieved_at: datetime,
) -> dict[str, Any]:
    """Classify whether SCMO exposes receipt columns or blocks behind SSO."""

    auth_required = _auth_required(final_url=final_url, response_text=response_text)
    has_timestamp_column = _has_timestamp_column(response_text)
    has_source_publication_timestamp_column = (
        "source_publication_timestamp" in response_text.casefold()
    )
    candidate_receipt_source_found = (
        not auth_required and has_timestamp_column and has_source_publication_timestamp_column
    )
    if candidate_receipt_source_found:
        source_probe_status = "candidate_receipt_metadata_present"
    elif auth_required:
        source_probe_status = "auth_required_sso_login"
    else:
        source_probe_status = "explicit_receipt_columns_missing"

    lead_row = {
        "lead_id": SCMO_DAM_LEAD_ID,
        "source_url": source_url,
        "source_title": SCMO_DAM_LEAD_TITLE,
        "lead_kind": "official_credentialed_portal",
        "metadata_scope": "row_level",
        "has_timestamp_column": True,
        "has_source_publication_timestamp_column": (
            has_source_publication_timestamp_column
        ),
        "download_auth_required": auth_required,
        "source_probe_status": source_probe_status,
        "market_execution_enabled": False,
    }
    return {
        "claim_scope": SCMO_DAM_RECEIPT_ACCESS_CLAIM_SCOPE,
        "source_url": source_url,
        "final_url": final_url,
        "status_code": int(status_code),
        "content_type": content_type,
        "retrieved_at": retrieved_at.isoformat(),
        "auth_required": auth_required,
        "has_timestamp_column": has_timestamp_column,
        "has_source_publication_timestamp_column": (
            has_source_publication_timestamp_column
        ),
        "source_probe_status": source_probe_status,
        "candidate_receipt_source_found": candidate_receipt_source_found,
        "lead_row": lead_row,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "not_market_execution": True,
    }


def _auth_required(*, final_url: str, response_text: str) -> bool:
    normalized_url = final_url.casefold()
    normalized_text = response_text.casefold()
    return (
        "login-scmo.oree.com.ua" in normalized_url
        or "/login" in normalized_url
        or "login to the system" in normalized_text
        or "single sign on" in normalized_text
    )


def _has_timestamp_column(response_text: str) -> bool:
    normalized = response_text.casefold()
    return "timestamp" in normalized or "date" in normalized


__all__ = [
    "SCMO_DAM_RECEIPT_ACCESS_CLAIM_SCOPE",
    "SCMO_DAM_LEAD_ID",
    "build_scmo_dam_publication_receipt_access_probe",
]
