"""Audit DAM publication receipt source leads before V13 receipt backfill.

This does not create receipt rows. It only separates source-discovery leads
from validated row-level publication receipt evidence.
"""

from __future__ import annotations

from typing import Any, Final

import polars as pl

CLAIM_SCOPE: Final[str] = "v13_dam_receipt_source_lead_audit_not_market_execution"

_REQUIRED_LEAD_COLUMNS: Final[tuple[str, ...]] = (
    "lead_id",
    "source_url",
    "source_title",
    "lead_kind",
    "metadata_scope",
    "has_timestamp_column",
    "has_source_publication_timestamp_column",
    "download_auth_required",
    "source_probe_status",
)
_NEGATIVE_PROBE_STATUSES: Final[frozenset[str]] = frozenset(
    {
        "not_sufficient_for_v13_receipts",
        "source_unreachable",
        "download_failed",
        "negative_probe",
    }
)


def audit_dfl_ua_context_dam_receipt_source_leads_v13_frame(
    frame: pl.DataFrame,
) -> dict[str, Any]:
    """Classify DAM receipt source leads without turning leads into receipts."""

    missing_columns = [
        column for column in _REQUIRED_LEAD_COLUMNS if column not in frame.columns
    ]
    if missing_columns:
        return _summary(
            frame=frame,
            lead_rows=[],
            candidate_receipt_lead_ids=[],
            blocking_reasons=[
                f"missing_required_column:{column}" for column in missing_columns
            ],
        )

    source_contains_market_execution_rows = _source_contains_true(
        frame,
        "market_execution_enabled",
    )
    lead_rows: list[dict[str, Any]] = []
    candidate_receipt_lead_ids: list[str] = []
    blocking_reasons: list[str] = []

    for row in frame.iter_rows(named=True):
        lead = _classify_lead_row(
            row,
            source_contains_market_execution_rows=source_contains_market_execution_rows,
        )
        lead_rows.append(lead)
        if lead["lead_status"] == "candidate_row_level_receipt_source":
            candidate_receipt_lead_ids.append(str(lead["lead_id"]))
        for reason in lead["blocking_reasons"]:
            if reason not in blocking_reasons:
                blocking_reasons.append(str(reason))

    if candidate_receipt_lead_ids:
        blocking_reasons.append("receipt_csv_not_generated")

    return _summary(
        frame=frame,
        lead_rows=lead_rows,
        candidate_receipt_lead_ids=candidate_receipt_lead_ids,
        blocking_reasons=blocking_reasons,
    )


def _classify_lead_row(
    row: dict[str, Any],
    *,
    source_contains_market_execution_rows: bool,
) -> dict[str, Any]:
    lead_id = str(row["lead_id"])
    metadata_scope = str(row["metadata_scope"]).strip().casefold()
    source_probe_status = str(row["source_probe_status"]).strip().casefold()
    has_timestamp_column = _bool_value(row["has_timestamp_column"])
    has_source_publication_timestamp_column = _bool_value(
        row["has_source_publication_timestamp_column"]
    )
    download_auth_required = _bool_value(row["download_auth_required"])
    blockers: list[str] = []

    if source_contains_market_execution_rows or _optional_bool(
        row.get("market_execution_enabled")
    ):
        blockers.append("source_contains_market_execution_rows")
        lead_status = "blocked_market_execution_claim"
    elif download_auth_required:
        blockers.append("download_auth_required")
        lead_status = "blocked_auth_required"
    elif source_probe_status in _NEGATIVE_PROBE_STATUSES:
        blockers.append("source_probe_not_sufficient_for_v13_receipts")
        lead_status = "blocked_negative_probe"
    elif metadata_scope == "dataset_level":
        blockers.append("dataset_level_metadata_only")
        lead_status = "insufficient_dataset_level_metadata_only"
    elif metadata_scope != "row_level":
        blockers.append("row_level_receipt_metadata_missing")
        lead_status = "insufficient_unknown_metadata_scope"
    elif not has_timestamp_column or not has_source_publication_timestamp_column:
        blockers.append("explicit_receipt_columns_missing")
        lead_status = "insufficient_explicit_receipt_columns"
    else:
        lead_status = "candidate_row_level_receipt_source"

    lead = {
        "lead_id": lead_id,
        "source_url": str(row["source_url"]),
        "source_title": str(row["source_title"]),
        "lead_kind": str(row["lead_kind"]),
        "metadata_scope": metadata_scope,
        "source_probe_status": source_probe_status,
        "has_timestamp_column": has_timestamp_column,
        "has_source_publication_timestamp_column": (
            has_source_publication_timestamp_column
        ),
        "download_auth_required": download_auth_required,
        "lead_status": lead_status,
        "blocking_reasons": blockers,
    }
    security_policy_requirements = row.get("security_policy_requirements")
    if security_policy_requirements is not None:
        lead["security_policy_requirements"] = str(security_policy_requirements)
    for field_name in (
        "credential_mode",
        "signed_download_request_ready",
        "credential_material_format",
        "mtls_client_cert_ready",
        "ws_security_signature_supported",
        "ws_security_signature_status",
        "ws_security_signature_applied",
    ):
        if field_name in row and row[field_name] is not None:
            lead[field_name] = _optional_lead_field_value(row[field_name])
    return lead


def _summary(
    *,
    frame: pl.DataFrame,
    lead_rows: list[dict[str, Any]],
    candidate_receipt_lead_ids: list[str],
    blocking_reasons: list[str],
) -> dict[str, Any]:
    dataset_level_metadata_only_count = sum(
        1
        for row in lead_rows
        if row["lead_status"] == "insufficient_dataset_level_metadata_only"
    )
    auth_blocked_count = sum(
        1 for row in lead_rows if row["lead_status"] == "blocked_auth_required"
    )
    probe_negative_count = sum(
        1 for row in lead_rows if row["lead_status"] == "blocked_negative_probe"
    )
    missing_required_receipt_column_count = sum(
        1
        for row in lead_rows
        if row["lead_status"] == "insufficient_explicit_receipt_columns"
    )
    candidate_receipt_lead_count = len(candidate_receipt_lead_ids)
    return {
        "claim_scope": CLAIM_SCOPE,
        "lead_count": frame.height,
        "source_columns": sorted(frame.columns),
        "candidate_receipt_source_found": candidate_receipt_lead_count > 0,
        "candidate_receipt_lead_count": candidate_receipt_lead_count,
        "candidate_receipt_lead_ids": candidate_receipt_lead_ids,
        "dataset_level_metadata_only_count": dataset_level_metadata_only_count,
        "auth_blocked_count": auth_blocked_count,
        "probe_negative_count": probe_negative_count,
        "missing_required_receipt_column_count": (
            missing_required_receipt_column_count
        ),
        "lead_rows": lead_rows,
        "blocking_reasons": blocking_reasons,
        "required_receipt_columns": ["timestamp", "source_publication_timestamp"],
        "optional_receipt_columns": ["source_url", "source_title", "receipt_id"],
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }


def _source_contains_true(frame: pl.DataFrame, column_name: str) -> bool:
    if frame.is_empty() or column_name not in frame.columns:
        return False
    return any(_optional_bool(value) for value in frame[column_name].to_list())


def _optional_bool(value: Any) -> bool:
    if value is None:
        return False
    return _bool_value(value)


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in {0, 1}:
            return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no", ""}:
            return False
    raise TypeError(f"Cannot convert {type(value).__name__} to bool.")


def _optional_lead_field_value(value: Any) -> str | bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
        return value
    return str(value)


__all__ = [
    "CLAIM_SCOPE",
    "audit_dfl_ua_context_dam_receipt_source_leads_v13_frame",
]
