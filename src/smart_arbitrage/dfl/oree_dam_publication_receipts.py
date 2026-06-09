from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, Final


OREE_DAM_RECEIPT_PROBE_CLAIM_SCOPE: Final[str] = (
    "oree_dam_publication_receipt_probe_not_receipt"
)
OREE_DAM_RECEIPT_SOURCE_AUDIT_CLAIM_SCOPE: Final[str] = (
    "oree_dam_publication_receipt_source_audit"
)

_ROW_LEVEL_PUBLICATION_TOKENS: Final[tuple[str, ...]] = (
    "source_publication_timestamp",
    "publication_timestamp",
    "published_at",
    "receipt_id",
)


def build_oree_dam_publication_receipt_probe(
    *,
    requested_month: str,
    source_url: str,
    response_headers: Mapping[str, str | Sequence[str]],
    response_text: str,
    retrieved_at: datetime,
) -> dict[str, Any]:
    """Classify whether an OREE response contains V13 row-level receipt metadata."""

    normalized_headers = _normalize_headers(response_headers)
    row_level_metadata_found = _contains_row_level_publication_metadata(response_text)
    http_last_modified_present = "last-modified" in normalized_headers
    receipt_status = (
        "candidate_receipt_metadata_present"
        if row_level_metadata_found
        else "not_sufficient_for_v13_receipts"
    )
    return {
        "claim_scope": OREE_DAM_RECEIPT_PROBE_CLAIM_SCOPE,
        "http_date_header_is_retrieval_metadata_only": "date" in normalized_headers,
        "http_last_modified_present": http_last_modified_present,
        "market_execution_enabled": False,
        "not_full_dfl": True,
        "not_market_execution": True,
        "receipt_status": receipt_status,
        "requested_month": requested_month,
        "retrieved_at": _isoformat(retrieved_at),
        "row_level_publication_metadata_found": row_level_metadata_found,
        "source_url": source_url,
    }


def build_oree_dam_publication_receipt_source_audit(
    probes: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Summarize whether probed OREE sources expose V13 receipt metadata."""

    probe_rows = [dict(probe) for probe in probes]
    months_probed = [str(row["requested_month"]) for row in probe_rows]
    candidate_months = [
        str(row["requested_month"])
        for row in probe_rows
        if row.get("receipt_status") == "candidate_receipt_metadata_present"
    ]
    insufficient_months = [
        str(row["requested_month"])
        for row in probe_rows
        if row.get("receipt_status") == "not_sufficient_for_v13_receipts"
    ]
    return {
        "claim_scope": OREE_DAM_RECEIPT_SOURCE_AUDIT_CLAIM_SCOPE,
        "all_probes_insufficient_for_v13_receipts": (
            bool(probe_rows) and len(insufficient_months) == len(probe_rows)
        ),
        "candidate_receipt_months": candidate_months,
        "candidate_receipt_source_found": bool(candidate_months),
        "insufficient_months": insufficient_months,
        "market_execution_enabled": False,
        "months_probed": months_probed,
        "not_full_dfl": True,
        "not_market_execution": True,
        "probe_count": len(probe_rows),
        "receipt_csv_generated": False,
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


def _contains_row_level_publication_metadata(response_text: str) -> bool:
    normalized = response_text.casefold()
    return all(token in normalized for token in ("timestamp", "source_publication_timestamp")) or any(
        token in normalized for token in _ROW_LEVEL_PUBLICATION_TOKENS
    )


def _isoformat(value: datetime) -> str:
    return value.isoformat()


__all__ = [
    "OREE_DAM_RECEIPT_PROBE_CLAIM_SCOPE",
    "OREE_DAM_RECEIPT_SOURCE_AUDIT_CLAIM_SCOPE",
    "build_oree_dam_publication_receipt_source_audit",
    "build_oree_dam_publication_receipt_probe",
]
