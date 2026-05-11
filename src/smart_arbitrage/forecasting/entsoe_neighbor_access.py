"""ENTSO-E neighbor-market query specification gate."""

from __future__ import annotations

from typing import Final

import polars as pl

from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome
from smart_arbitrage.forecasting.market_coupling_availability import (
    EXTERNAL_TRAINING_BLOCKERS,
    REQUIRED_MARKET_COUPLING_AVAILABILITY_COLUMNS,
)

ENTSOE_NEIGHBOR_MARKET_ACCESS_CLAIM_SCOPE: Final[str] = (
    "entsoe_neighbor_market_access_research_gate"
)
REQUIRED_ENTSOE_NEIGHBOR_ACCESS_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "country_code",
        "country_name",
        "bidding_zone_eic",
        "eic_mapping_status",
        "document_type",
        "process_type",
        "market_venue",
        "query_role",
        "time_zone_policy",
        "publication_time_policy",
        "security_token_required",
        "security_token_available",
        "fetch_allowed",
        "training_use_allowed",
        "training_blockers_csv",
        "access_status",
        "next_action",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
_NEIGHBOR_BIDDING_ZONE_ROWS: Final[tuple[dict[str, str], ...]] = (
    {
        "country_code": "PL",
        "country_name": "Poland",
        "bidding_zone_eic": "10YPL-AREA-----S",
        "eic_mapping_status": "mapped",
    },
    {
        "country_code": "SK",
        "country_name": "Slovakia",
        "bidding_zone_eic": "10YSK-SEPS-----K",
        "eic_mapping_status": "mapped",
    },
    {
        "country_code": "HU",
        "country_name": "Hungary",
        "bidding_zone_eic": "10YHU-MAVIR----U",
        "eic_mapping_status": "mapped",
    },
    {
        "country_code": "RO",
        "country_name": "Romania",
        "bidding_zone_eic": "10YRO-TEL------P",
        "eic_mapping_status": "mapped",
    },
    {
        "country_code": "MD",
        "country_name": "Moldova",
        "bidding_zone_eic": "",
        "eic_mapping_status": "review_required",
    },
)


def build_entsoe_neighbor_market_query_spec_frame(
    market_coupling_temporal_availability_frame: pl.DataFrame,
    *,
    security_token: str | None,
) -> pl.DataFrame:
    """Build an ENTSO-E day-ahead price query spec without fetching data."""

    missing = sorted(
        REQUIRED_MARKET_COUPLING_AVAILABILITY_COLUMNS.difference(
            market_coupling_temporal_availability_frame.columns
        )
    )
    if missing:
        raise ValueError(
            f"market_coupling_temporal_availability_frame missing columns: {missing}"
        )
    entsoe_rows = market_coupling_temporal_availability_frame.filter(
        pl.col("source_name") == "ENTSO_E"
    )
    if entsoe_rows.height != 1:
        raise ValueError("market_coupling_temporal_availability_frame must contain one ENTSO_E row")

    token_available = bool(security_token and security_token.strip())
    rows = [
        _query_spec_row(row, token_available=token_available)
        for row in _NEIGHBOR_BIDDING_ZONE_ROWS
    ]
    return pl.DataFrame(rows).sort("country_code")


def validate_entsoe_neighbor_market_access_evidence(frame: pl.DataFrame) -> EvidenceCheckOutcome:
    """Validate ENTSO-E access/query evidence remains a non-training gate."""

    failures = _missing_column_failures(frame, REQUIRED_ENTSOE_NEIGHBOR_ACCESS_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": frame.height})
    rows = list(frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "ENTSO-E neighbor access frame has no rows", {"row_count": 0})

    training_allowed_rows = [row for row in rows if bool(row["training_use_allowed"])]
    bad_request_shape_rows = [
        row
        for row in rows
        if str(row["document_type"]) != "A44" or str(row["process_type"]) != "A01"
    ]
    fetch_without_token_rows = [
        row
        for row in rows
        if bool(row["fetch_allowed"]) and not bool(row["security_token_available"])
    ]
    missing_blocker_rows = [
        row for row in rows if str(row["training_blockers_csv"]) != EXTERNAL_TRAINING_BLOCKERS
    ]
    bad_claim_rows = [
        row
        for row in rows
        if str(row["claim_scope"]) != ENTSOE_NEIGHBOR_MARKET_ACCESS_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
    ]

    if training_allowed_rows:
        failures.append("ENTSO-E neighbor rows must not be training rows")
    if bad_request_shape_rows:
        failures.append("ENTSO-E day-ahead price rows must use A44/A01")
    if fetch_without_token_rows:
        failures.append("ENTSO-E fetch cannot be allowed without a security token")
    if missing_blocker_rows:
        failures.append("ENTSO-E neighbor rows must list every external training blocker")
    if bad_claim_rows:
        failures.append("ENTSO-E neighbor rows must keep research-only claim flags")

    metadata = {
        "row_count": len(rows),
        "mapped_eic_rows": len(
            [row for row in rows if str(row["eic_mapping_status"]) == "mapped"]
        ),
        "review_required_rows": len(
            [row for row in rows if str(row["eic_mapping_status"]) == "review_required"]
        ),
        "fetch_allowed_rows": len([row for row in rows if bool(row["fetch_allowed"])]),
        "training_allowed_rows": len(training_allowed_rows),
        "bad_request_shape_rows": len(bad_request_shape_rows),
        "fetch_without_token_rows": len(fetch_without_token_rows),
        "missing_blocker_rows": len(missing_blocker_rows),
        "bad_claim_rows": len(bad_claim_rows),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "ENTSO-E neighbor-market access gate records query specs without training use."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def _query_spec_row(row: dict[str, str], *, token_available: bool) -> dict[str, object]:
    eic_mapped = row["eic_mapping_status"] == "mapped"
    fetch_allowed = token_available and eic_mapped
    if not token_available:
        access_status = "blocked_missing_entsoe_security_token"
    elif not eic_mapped:
        access_status = "blocked_eic_mapping_review_required"
    else:
        access_status = "ready_for_manual_sample_fetch_not_training"
    return {
        **row,
        "document_type": "A44",
        "process_type": "A01",
        "market_venue": "neighbor_DAM",
        "query_role": "future_market_coupling_covariate",
        "time_zone_policy": "request_utc_align_to_europe_kyiv_anchor",
        "publication_time_policy": "must_be_published_before_ua_anchor",
        "security_token_required": True,
        "security_token_available": token_available,
        "fetch_allowed": fetch_allowed,
        "training_use_allowed": False,
        "training_blockers_csv": EXTERNAL_TRAINING_BLOCKERS,
        "access_status": access_status,
        "next_action": _next_action(row["country_code"], token_available=token_available),
        "claim_scope": ENTSOE_NEIGHBOR_MARKET_ACCESS_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _next_action(country_code: str, *, token_available: bool) -> str:
    if country_code == "MD":
        return "verify_moldova_bidding_zone_or_exclude_from_entsoe_sample"
    if not token_available:
        return "request_entsoe_security_token_before_fetching_source_backed_sample"
    return "fetch_manual_day_ahead_price_sample_and_record_publication_timestamp"


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"missing required columns: {missing}"] if missing else []
