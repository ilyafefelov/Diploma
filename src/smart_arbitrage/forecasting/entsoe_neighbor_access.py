"""ENTSO-E neighbor-market query specification gate."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from collections.abc import Callable
from typing import Final
from urllib.parse import quote
from urllib.request import urlopen
from xml.etree import ElementTree

import polars as pl

from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome
from smart_arbitrage.forecasting.market_coupling_availability import (
    EXTERNAL_TRAINING_BLOCKERS,
    REQUIRED_MARKET_COUPLING_AVAILABILITY_COLUMNS,
)

ENTSOE_NEIGHBOR_MARKET_ACCESS_CLAIM_SCOPE: Final[str] = (
    "entsoe_neighbor_market_access_research_gate"
)
ENTSOE_API_BASE_URL: Final[str] = "https://web-api.tp.entsoe.eu/api"
ENTSOE_DAY_AHEAD_PRICE_QUERY_PARAMETER_KEYS: Final[tuple[str, ...]] = (
    "securityToken",
    "documentType",
    "processType",
    "in_Domain",
    "out_Domain",
    "periodStart",
    "periodEnd",
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
        "api_base_url",
        "query_parameter_keys_csv",
        "request_url_template",
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
ENTSOE_NEIGHBOR_MARKET_SAMPLE_CLAIM_SCOPE: Final[str] = (
    "entsoe_neighbor_market_sample_audit_research_gate"
)
ENTSOE_NEIGHBOR_MARKET_FEATURE_CANDIDATE_CLAIM_SCOPE: Final[str] = (
    "entsoe_neighbor_market_feature_candidate_research_gate"
)
ENTSOE_NEIGHBOR_MARKET_ALIGNED_FEATURE_CLAIM_SCOPE: Final[str] = (
    "entsoe_neighbor_market_aligned_feature_research_gate"
)
ENTSOE_POLAND_FEATURE_GOVERNANCE_CLAIM_SCOPE: Final[str] = (
    "entsoe_poland_feature_governance_research_gate"
)
REQUIRED_ENTSOE_NEIGHBOR_SAMPLE_AUDIT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "country_code",
        "country_name",
        "bidding_zone_eic",
        "sample_period_start_utc",
        "sample_period_end_utc",
        "fetch_enabled",
        "security_token_available",
        "security_token_required",
        "source_access_method",
        "source_url",
        "source_retrieved_at_utc",
        "source_sha256",
        "source_license_status",
        "fetch_status",
        "request_url_template",
        "source_backed_row_count",
        "parsed_price_row_count",
        "first_delivery_timestamp_utc",
        "last_delivery_timestamp_utc",
        "publication_time_policy",
        "time_zone_policy",
        "training_use_allowed",
        "feature_use_allowed",
        "training_blockers_csv",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_ENTSOE_NEIGHBOR_FEATURE_CANDIDATE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "country_code",
        "country_name",
        "bidding_zone_eic",
        "feature_name",
        "feature_column",
        "delivery_timestamp_utc",
        "neighbor_market_price_eur_mwh",
        "neighbor_market_price_uah_mwh",
        "source_backed",
        "fetch_enabled",
        "security_token_required",
        "security_token_available",
        "source_access_method",
        "source_url",
        "source_retrieved_at_utc",
        "source_sha256",
        "source_license_status",
        "fetch_status",
        "sample_period_start_utc",
        "sample_period_end_utc",
        "publication_time_policy",
        "publication_timestamp_utc",
        "publication_time_status",
        "ua_decision_anchor_policy",
        "is_prior_to_ua_decision_anchor",
        "time_zone_policy",
        "currency_policy",
        "fx_rate_source",
        "fx_rate_timestamp_utc",
        "currency_normalization_status",
        "training_use_allowed",
        "feature_use_allowed",
        "training_blockers_csv",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_ENTSOE_NEIGHBOR_ALIGNED_FEATURE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "timestamp",
        "country_code",
        "country_name",
        "feature_name",
        "feature_column",
        "neighbor_market_price_eur_mwh",
        "neighbor_market_price_uah_mwh",
        "source_backed",
        "publication_time_status",
        "currency_normalization_status",
        "training_use_allowed",
        "feature_use_allowed",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_ENTSOE_POLAND_FEATURE_GOVERNANCE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "country_code",
        "country_name",
        "feature_name",
        "approved_feature_column",
        "source_backed_row_count",
        "entsoe_security_token_available",
        "publication_timestamp_utc",
        "ua_decision_anchor_timestamp_utc",
        "publication_time_status",
        "is_publication_prior_to_anchor",
        "timezone_status",
        "fx_rate_source",
        "fx_rate_timestamp_utc",
        "fx_rate_eur_uah",
        "currency_status",
        "market_rules_status",
        "licensing_status",
        "domain_shift_status",
        "temporal_availability_status",
        "readiness_status",
        "training_blockers_csv",
        "training_use_allowed",
        "feature_use_allowed",
        "approved_for_official_training",
        "market_execution_enabled",
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

FetchXmlByUrl = Callable[[str], str]


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


def build_entsoe_neighbor_market_sample_audit_frame(
    entsoe_neighbor_market_query_spec_frame: pl.DataFrame,
    *,
    sample_country_codes_csv: str,
    sample_period_start_utc: str,
    sample_period_end_utc: str,
    security_token: str | None,
    fetch_enabled: bool,
    fetch_xml_by_url: FetchXmlByUrl | None = None,
) -> pl.DataFrame:
    """Fetch or skip a tiny ENTSO-E source sample and keep it out of training."""

    failures = _missing_column_failures(
        entsoe_neighbor_market_query_spec_frame,
        REQUIRED_ENTSOE_NEIGHBOR_ACCESS_COLUMNS,
    )
    if failures:
        raise ValueError("; ".join(failures))
    country_codes = _csv_values(sample_country_codes_csv)
    if not country_codes:
        raise ValueError("sample_country_codes_csv must contain at least one country code.")
    _parse_entsoe_period_utc(sample_period_start_utc)
    _parse_entsoe_period_utc(sample_period_end_utc)

    token_available = bool(security_token and security_token.strip())
    rows: list[dict[str, object]] = []
    for country_code in country_codes:
        country_rows = entsoe_neighbor_market_query_spec_frame.filter(
            pl.col("country_code") == country_code
        )
        if country_rows.height != 1:
            raise ValueError(f"query spec missing one row for country_code={country_code!r}")
        query_row = country_rows.to_dicts()[0]
        rows.append(
            _sample_audit_row(
                query_row,
                sample_period_start_utc=sample_period_start_utc,
                sample_period_end_utc=sample_period_end_utc,
                security_token=security_token,
                token_available=token_available,
                fetch_enabled=fetch_enabled,
                fetch_xml_by_url=fetch_xml_by_url,
            )
        )
    return pl.DataFrame(rows).sort("country_code")


def validate_entsoe_neighbor_market_sample_audit_evidence(
    frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate ENTSO-E source samples are audit evidence, not training features."""

    failures = _missing_column_failures(frame, REQUIRED_ENTSOE_NEIGHBOR_SAMPLE_AUDIT_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": frame.height})
    rows = list(frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "ENTSO-E neighbor sample audit has no rows", {"row_count": 0})

    training_rows = [row for row in rows if bool(row["training_use_allowed"])]
    feature_rows = [row for row in rows if bool(row["feature_use_allowed"])]
    token_bypass_rows = [
        row
        for row in rows
        if int(row["source_backed_row_count"]) > 0
        and not bool(row["security_token_available"])
    ]
    bad_claim_rows = [
        row
        for row in rows
        if str(row["claim_scope"]) != ENTSOE_NEIGHBOR_MARKET_SAMPLE_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
    ]
    if training_rows:
        failures.append("ENTSO-E samples must not become training rows before governance passes")
    if feature_rows:
        failures.append("ENTSO-E samples must not become feature rows before governance passes")
    if token_bypass_rows:
        failures.append("ENTSO-E source-backed samples require a security token")
    if bad_claim_rows:
        failures.append("ENTSO-E samples must keep research-only claim flags")

    metadata = {
        "row_count": len(rows),
        "fetched_country_count": len(
            {str(row["country_code"]) for row in rows if int(row["source_backed_row_count"]) > 0}
        ),
        "source_backed_rows": sum(int(row["source_backed_row_count"]) for row in rows),
        "parsed_price_rows": sum(int(row["parsed_price_row_count"]) for row in rows),
        "training_allowed_rows": len(training_rows),
        "feature_allowed_rows": len(feature_rows),
        "token_bypass_rows": len(token_bypass_rows),
        "bad_claim_rows": len(bad_claim_rows),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "ENTSO-E neighbor-market sample audit remains research-only."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def build_entsoe_neighbor_market_feature_candidate_frame(
    entsoe_neighbor_market_query_spec_frame: pl.DataFrame,
    *,
    sample_country_codes_csv: str,
    sample_period_start_utc: str,
    sample_period_end_utc: str,
    security_token: str | None,
    fetch_enabled: bool,
    fetch_xml_by_url: FetchXmlByUrl | None = None,
) -> pl.DataFrame:
    """Fetch or skip normalized neighbor price candidates without approving use."""

    failures = _missing_column_failures(
        entsoe_neighbor_market_query_spec_frame,
        REQUIRED_ENTSOE_NEIGHBOR_ACCESS_COLUMNS,
    )
    if failures:
        raise ValueError("; ".join(failures))
    country_codes = _csv_values(sample_country_codes_csv)
    if not country_codes:
        raise ValueError("sample_country_codes_csv must contain at least one country code.")
    _parse_entsoe_period_utc(sample_period_start_utc)
    _parse_entsoe_period_utc(sample_period_end_utc)

    token_available = bool(security_token and security_token.strip())
    rows: list[dict[str, object]] = []
    for country_code in country_codes:
        country_rows = entsoe_neighbor_market_query_spec_frame.filter(
            pl.col("country_code") == country_code
        )
        if country_rows.height != 1:
            raise ValueError(f"query spec missing one row for country_code={country_code!r}")
        query_row = country_rows.to_dicts()[0]
        rows.extend(
            _feature_candidate_rows(
                query_row,
                sample_period_start_utc=sample_period_start_utc,
                sample_period_end_utc=sample_period_end_utc,
                security_token=security_token,
                token_available=token_available,
                fetch_enabled=fetch_enabled,
                fetch_xml_by_url=fetch_xml_by_url,
            )
        )
    return pl.DataFrame(rows).sort(["country_code", "delivery_timestamp_utc"])


def validate_entsoe_neighbor_market_feature_candidate_evidence(
    frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate source-backed neighbor prices are not approved training features."""

    failures = _missing_column_failures(frame, REQUIRED_ENTSOE_NEIGHBOR_FEATURE_CANDIDATE_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": frame.height})
    rows = list(frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(
            False,
            "ENTSO-E neighbor feature candidate frame has no rows",
            {"row_count": 0},
        )

    training_rows = [row for row in rows if bool(row["training_use_allowed"])]
    feature_rows = [row for row in rows if bool(row["feature_use_allowed"])]
    token_bypass_rows = [
        row
        for row in rows
        if bool(row["source_backed"]) and not bool(row["security_token_available"])
        and bool(row["security_token_required"])
    ]
    bad_claim_rows = [
        row
        for row in rows
        if str(row["claim_scope"]) != ENTSOE_NEIGHBOR_MARKET_FEATURE_CANDIDATE_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
    ]
    missing_publication_gate_rows = [
        row for row in rows if not str(row["publication_time_status"]).strip()
    ]
    missing_currency_gate_rows = [
        row for row in rows if not str(row["currency_normalization_status"]).strip()
    ]
    inconsistent_publication_ready_rows = [
        row
        for row in rows
        if str(row["publication_time_status"])
        == "publication_time_verified_prior_to_ua_anchor"
        and (
            not str(row["publication_timestamp_utc"]).strip()
            or not bool(row["is_prior_to_ua_decision_anchor"])
        )
    ]
    inconsistent_currency_ready_rows = [
        row
        for row in rows
        if str(row["currency_normalization_status"]) == "prior_eur_uah_normalized"
        and (
            not str(row["fx_rate_source"]).strip()
            or not str(row["fx_rate_timestamp_utc"]).strip()
            or row["neighbor_market_price_uah_mwh"] is None
        )
    ]
    if training_rows or feature_rows:
        failures.append("ENTSO-E feature candidates must remain blocked from feature/training use")
    if token_bypass_rows:
        failures.append("ENTSO-E source-backed feature candidates require a security token")
    if bad_claim_rows:
        failures.append("ENTSO-E feature candidates must keep research-only claim flags")
    if missing_publication_gate_rows:
        failures.append("ENTSO-E feature candidates must carry publication-time gate status")
    if missing_currency_gate_rows:
        failures.append("ENTSO-E feature candidates must carry currency-normalization gate status")
    if inconsistent_publication_ready_rows:
        failures.append("publication-ready rows must include a prior publication timestamp")
    if inconsistent_currency_ready_rows:
        failures.append("currency-ready rows must include prior FX metadata and UAH price")

    metadata = {
        "row_count": len(rows),
        "source_backed_rows": len([row for row in rows if bool(row["source_backed"])]),
        "training_allowed_rows": len(training_rows),
        "feature_allowed_rows": len(feature_rows),
        "token_bypass_rows": len(token_bypass_rows),
        "bad_claim_rows": len(bad_claim_rows),
        "publication_blocked_rows": len(
            [
                row
                for row in rows
                if str(row["publication_time_status"]).startswith("blocked_")
            ]
        ),
        "currency_blocked_rows": len(
            [
                row
                for row in rows
                if str(row["currency_normalization_status"]).startswith("blocked_")
            ]
        ),
        "missing_publication_gate_rows": len(missing_publication_gate_rows),
        "missing_currency_gate_rows": len(missing_currency_gate_rows),
        "inconsistent_publication_ready_rows": len(
            inconsistent_publication_ready_rows
        ),
        "inconsistent_currency_ready_rows": len(inconsistent_currency_ready_rows),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "ENTSO-E neighbor feature candidates remain source-backed research-only rows."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def build_entsoe_poland_feature_governance_frame(
    entsoe_neighbor_market_feature_candidate_frame: pl.DataFrame,
    *,
    entsoe_security_token: str | None,
    publication_timestamp_utc: str,
    ua_decision_anchor_timestamp_utc: str,
    prior_eur_uah_fx_rate: float,
    prior_eur_uah_fx_timestamp_utc: str,
    fx_rate_source: str,
    timezone_dst_mapping_ready: bool,
    licensing_approved: bool,
    market_rules_mapped: bool,
    domain_shift_validated: bool,
) -> pl.DataFrame:
    """Evaluate whether the Poland ENTSO-E feature may enter official training."""

    candidate_failures = _missing_column_failures(
        entsoe_neighbor_market_feature_candidate_frame,
        REQUIRED_ENTSOE_NEIGHBOR_FEATURE_CANDIDATE_COLUMNS,
    )
    if candidate_failures:
        raise ValueError("; ".join(candidate_failures))
    rows = [
        row
        for row in entsoe_neighbor_market_feature_candidate_frame.iter_rows(named=True)
        if str(row["country_code"]).upper() == "PL"
        and str(row["feature_name"]) == "entsoe_neighbor_day_ahead_price_context"
    ]
    source_backed_count = len([row for row in rows if bool(row["source_backed"])])
    anchor = (
        _parse_iso_utc(ua_decision_anchor_timestamp_utc)
        if ua_decision_anchor_timestamp_utc.strip()
        else None
    )
    publication = (
        _parse_iso_utc(publication_timestamp_utc)
        if publication_timestamp_utc.strip()
        else None
    )
    fx_timestamp = (
        _parse_iso_utc(prior_eur_uah_fx_timestamp_utc)
        if prior_eur_uah_fx_timestamp_utc.strip()
        else None
    )
    publication_prior = publication is not None and anchor is not None and publication < anchor
    fx_prior = fx_timestamp is not None and anchor is not None and fx_timestamp < anchor
    token_available = bool(entsoe_security_token and entsoe_security_token.strip())
    source_backed_rows = [row for row in rows if bool(row["source_backed"])]
    token_required = any(
        bool(row.get("security_token_required", True)) for row in source_backed_rows
    )
    blockers = _entsoe_poland_governance_blockers(
        token_required=token_required,
        token_available=token_available,
        source_backed_count=source_backed_count,
        publication_prior=publication_prior,
        timezone_dst_mapping_ready=timezone_dst_mapping_ready,
        prior_eur_uah_fx_rate=prior_eur_uah_fx_rate,
        fx_prior=fx_prior,
        fx_rate_source=fx_rate_source,
        licensing_approved=licensing_approved,
        market_rules_mapped=market_rules_mapped,
        domain_shift_validated=domain_shift_validated,
    )
    approved = not blockers
    return pl.DataFrame(
        [
            {
                "country_code": "PL",
                "country_name": "Poland",
                "feature_name": "entsoe_neighbor_day_ahead_price_context",
                "approved_feature_column": "entsoe_pl_day_ahead_price_uah_mwh",
                "source_backed_row_count": source_backed_count,
                "entsoe_security_token_available": token_available,
                "publication_timestamp_utc": publication.isoformat()
                if publication is not None
                else "",
                "ua_decision_anchor_timestamp_utc": anchor.isoformat()
                if anchor is not None
                else "",
                "publication_time_status": "publication_time_verified_prior_to_ua_anchor"
                if publication_prior
                else "blocked_publication_not_prior_to_anchor",
                "is_publication_prior_to_anchor": publication_prior,
                "timezone_status": "ready"
                if timezone_dst_mapping_ready
                else "blocked_until_zone_and_dst_alignment",
                "fx_rate_source": fx_rate_source,
                "fx_rate_timestamp_utc": fx_timestamp.isoformat()
                if fx_timestamp is not None
                else "",
                "fx_rate_eur_uah": prior_eur_uah_fx_rate,
                "currency_status": "ready"
                if prior_eur_uah_fx_rate > 0.0 and fx_prior and fx_rate_source.strip()
                else "blocked_missing_prior_eur_uah_fx_rate",
                "market_rules_status": "ready"
                if market_rules_mapped
                else "blocked_until_dam_gate_closure_and_price_cap_mapping",
                "licensing_status": "ready"
                if licensing_approved
                else "blocked_until_license_terms_are_recorded",
                "domain_shift_status": "ready"
                if domain_shift_validated
                else "blocked_until_ukrainian_holdout_validation",
                "temporal_availability_status": "ready"
                if publication_prior
                else "blocked_until_publication_timestamp_mapping",
                "readiness_status": "training_ready" if approved else "blocked_by_governance",
                "training_blockers_csv": ",".join(blockers),
                "training_use_allowed": approved,
                "feature_use_allowed": approved,
                "approved_for_official_training": approved,
                "market_execution_enabled": False,
                "claim_scope": ENTSOE_POLAND_FEATURE_GOVERNANCE_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        ]
    )


def validate_entsoe_poland_feature_governance_evidence(
    frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate Poland feature governance before it can drive route approval."""

    failures = _missing_column_failures(frame, REQUIRED_ENTSOE_POLAND_FEATURE_GOVERNANCE_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": frame.height})
    rows = list(frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(
            False,
            "ENTSO-E Poland feature governance frame has no rows",
            {"row_count": 0},
        )
    bad_country_rows = [row for row in rows if str(row["country_code"]) != "PL"]
    bad_claim_rows = [
        row
        for row in rows
        if str(row["claim_scope"]) != ENTSOE_POLAND_FEATURE_GOVERNANCE_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
        or bool(row["market_execution_enabled"])
    ]
    inconsistent_approved_rows = [
        row
        for row in rows
        if bool(row["approved_for_official_training"])
        and (
            str(row["training_blockers_csv"]).strip()
            or int(row["source_backed_row_count"]) <= 0
            or str(row["readiness_status"]) != "training_ready"
            or not bool(row["training_use_allowed"])
            or not bool(row["feature_use_allowed"])
        )
    ]
    unapproved_without_blockers = [
        row
        for row in rows
        if not bool(row["approved_for_official_training"])
        and not str(row["training_blockers_csv"]).strip()
    ]
    if bad_country_rows:
        failures.append("Poland governance frame must contain only PL rows")
    if bad_claim_rows:
        failures.append("Poland governance rows must keep research-only claim flags")
    if inconsistent_approved_rows:
        failures.append("approved Poland rows must be source-backed and blocker-free")
    if unapproved_without_blockers:
        failures.append("blocked Poland rows must report governance blockers")

    metadata = {
        "row_count": len(rows),
        "approved_feature_count": len(
            [row for row in rows if bool(row["approved_for_official_training"])]
        ),
        "source_backed_rows": sum(int(row["source_backed_row_count"]) for row in rows),
        "bad_country_rows": len(bad_country_rows),
        "bad_claim_rows": len(bad_claim_rows),
        "inconsistent_approved_rows": len(inconsistent_approved_rows),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "ENTSO-E Poland feature governance is explicit and claim-safe."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def build_entsoe_neighbor_market_aligned_feature_panel_frame(
    benchmark_frame: pl.DataFrame,
    entsoe_neighbor_market_feature_candidate_frame: pl.DataFrame,
    *,
    country_codes: tuple[str, ...] = ("PL",),
) -> pl.DataFrame:
    """Align ENTSO-E neighbor prices to tenant benchmark timestamps.

    The output is source/context evidence only. It does not approve a feature for
    official model training; approval stays centralized in the route frame.
    """

    missing_benchmark = sorted({"tenant_id", "timestamp"}.difference(benchmark_frame.columns))
    if missing_benchmark:
        raise ValueError(f"benchmark_frame missing columns: {missing_benchmark}")
    candidate_failures = _missing_column_failures(
        entsoe_neighbor_market_feature_candidate_frame,
        REQUIRED_ENTSOE_NEIGHBOR_FEATURE_CANDIDATE_COLUMNS,
    )
    if candidate_failures:
        raise ValueError("; ".join(candidate_failures))
    normalized_country_codes = tuple(dict.fromkeys(code.upper() for code in country_codes))
    if not normalized_country_codes:
        raise ValueError("country_codes must contain at least one country code.")

    candidates_by_country_timestamp: dict[tuple[str, datetime], dict[str, object]] = {}
    for row in entsoe_neighbor_market_feature_candidate_frame.iter_rows(named=True):
        country_code = str(row["country_code"]).upper()
        delivery_timestamp = str(row["delivery_timestamp_utc"]).strip()
        if country_code not in normalized_country_codes or not delivery_timestamp:
            continue
        timestamp = _parse_iso_utc(delivery_timestamp).replace(tzinfo=None)
        candidates_by_country_timestamp[(country_code, timestamp)] = row

    rows: list[dict[str, object]] = []
    benchmark_rows = benchmark_frame.select(["tenant_id", "timestamp"]).unique().sort(
        ["tenant_id", "timestamp"]
    )
    for benchmark_row in benchmark_rows.iter_rows(named=True):
        timestamp = _timestamp_naive_utc(benchmark_row["timestamp"])
        for country_code in normalized_country_codes:
            candidate = candidates_by_country_timestamp.get((country_code, timestamp))
            rows.append(
                _aligned_feature_row(
                    benchmark_row,
                    country_code=country_code,
                    timestamp=timestamp,
                    candidate=candidate,
                )
            )
    return pl.DataFrame(rows).sort(["tenant_id", "timestamp", "country_code"])


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
        "api_base_url": ENTSOE_API_BASE_URL,
        "query_parameter_keys_csv": ",".join(ENTSOE_DAY_AHEAD_PRICE_QUERY_PARAMETER_KEYS),
        "request_url_template": _request_url_template(row["bidding_zone_eic"])
        if eic_mapped
        else "",
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


def _request_url_template(bidding_zone_eic: str) -> str:
    return (
        f"{ENTSOE_API_BASE_URL}?"
        "securityToken=<redacted>"
        "&documentType=A44"
        "&processType=A01"
        f"&in_Domain={bidding_zone_eic}"
        f"&out_Domain={bidding_zone_eic}"
        "&periodStart={period_start_utc_yyyymmddHHMM}"
        "&periodEnd={period_end_utc_yyyymmddHHMM}"
    )


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"missing required columns: {missing}"] if missing else []


def _sample_audit_row(
    query_row: dict[str, object],
    *,
    sample_period_start_utc: str,
    sample_period_end_utc: str,
    security_token: str | None,
    token_available: bool,
    fetch_enabled: bool,
    fetch_xml_by_url: FetchXmlByUrl | None,
) -> dict[str, object]:
    eic_mapped = str(query_row["eic_mapping_status"]) == "mapped"
    parsed_points: list[tuple[datetime, float]] = []
    if not fetch_enabled:
        fetch_status = "skipped_fetch_disabled"
    elif not token_available:
        fetch_status = "blocked_missing_entsoe_security_token"
    elif not eic_mapped:
        fetch_status = "blocked_eic_mapping_review_required"
    else:
        url = _request_url(
            str(query_row["bidding_zone_eic"]),
            security_token=str(security_token),
            period_start=sample_period_start_utc,
            period_end=sample_period_end_utc,
        )
        xml_text = (
            fetch_xml_by_url(url)
            if fetch_xml_by_url is not None
            else _fetch_text(url)
        )
        parsed_points = _parse_day_ahead_price_points(xml_text)
        fetch_status = (
            "source_backed_sample_fetched_not_training"
            if parsed_points
            else "source_response_had_no_price_points"
        )

    first_timestamp = parsed_points[0][0].isoformat() if parsed_points else ""
    last_timestamp = parsed_points[-1][0].isoformat() if parsed_points else ""
    parsed_count = len(parsed_points)
    return {
        "country_code": query_row["country_code"],
        "country_name": query_row["country_name"],
        "bidding_zone_eic": query_row["bidding_zone_eic"],
        "sample_period_start_utc": sample_period_start_utc,
        "sample_period_end_utc": sample_period_end_utc,
        "fetch_enabled": fetch_enabled,
        "security_token_required": True,
        "security_token_available": token_available,
        "source_access_method": "entsoe_rest_api",
        "source_url": ENTSOE_API_BASE_URL,
        "source_retrieved_at_utc": "",
        "source_sha256": "",
        "source_license_status": "requires_entsoe_terms_mapping",
        "fetch_status": fetch_status,
        "request_url_template": query_row["request_url_template"],
        "source_backed_row_count": parsed_count,
        "parsed_price_row_count": parsed_count,
        "first_delivery_timestamp_utc": first_timestamp,
        "last_delivery_timestamp_utc": last_timestamp,
        "publication_time_policy": query_row["publication_time_policy"],
        "time_zone_policy": query_row["time_zone_policy"],
        "training_use_allowed": False,
        "feature_use_allowed": False,
        "training_blockers_csv": EXTERNAL_TRAINING_BLOCKERS,
        "claim_scope": ENTSOE_NEIGHBOR_MARKET_SAMPLE_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _feature_candidate_rows(
    query_row: dict[str, object],
    *,
    sample_period_start_utc: str,
    sample_period_end_utc: str,
    security_token: str | None,
    token_available: bool,
    fetch_enabled: bool,
    fetch_xml_by_url: FetchXmlByUrl | None,
) -> list[dict[str, object]]:
    eic_mapped = str(query_row["eic_mapping_status"]) == "mapped"
    parsed_points: list[tuple[datetime, float]] = []
    if not fetch_enabled:
        fetch_status = "skipped_fetch_disabled"
    elif not token_available:
        fetch_status = "blocked_missing_entsoe_security_token"
    elif not eic_mapped:
        fetch_status = "blocked_eic_mapping_review_required"
    else:
        url = _request_url(
            str(query_row["bidding_zone_eic"]),
            security_token=str(security_token),
            period_start=sample_period_start_utc,
            period_end=sample_period_end_utc,
        )
        xml_text = (
            fetch_xml_by_url(url)
            if fetch_xml_by_url is not None
            else _fetch_text(url)
        )
        parsed_points = _parse_day_ahead_price_points(xml_text)
        fetch_status = (
            "source_backed_feature_sample_fetched_not_training"
            if parsed_points
            else "source_response_had_no_price_points"
        )
    if not parsed_points:
        return [
            _feature_candidate_row(
                query_row,
                delivery_timestamp_utc="",
                price_eur_mwh=None,
                source_backed=False,
                fetch_enabled=fetch_enabled,
                token_available=token_available,
                fetch_status=fetch_status,
                sample_period_start_utc=sample_period_start_utc,
                sample_period_end_utc=sample_period_end_utc,
            )
        ]
    return [
        _feature_candidate_row(
            query_row,
            delivery_timestamp_utc=timestamp.isoformat(),
            price_eur_mwh=price,
            source_backed=True,
            fetch_enabled=fetch_enabled,
            token_available=token_available,
            fetch_status=fetch_status,
            sample_period_start_utc=sample_period_start_utc,
            sample_period_end_utc=sample_period_end_utc,
        )
        for timestamp, price in parsed_points
    ]


def _feature_candidate_row(
    query_row: dict[str, object],
    *,
    delivery_timestamp_utc: str,
    price_eur_mwh: float | None,
    source_backed: bool,
    fetch_enabled: bool,
    token_available: bool,
    fetch_status: str,
    sample_period_start_utc: str,
    sample_period_end_utc: str,
) -> dict[str, object]:
    country_code = str(query_row["country_code"]).lower()
    return {
        "country_code": query_row["country_code"],
        "country_name": query_row["country_name"],
        "bidding_zone_eic": query_row["bidding_zone_eic"],
        "feature_name": "entsoe_neighbor_day_ahead_price_context",
        "feature_column": f"entsoe_{country_code}_day_ahead_price_eur_mwh",
        "delivery_timestamp_utc": delivery_timestamp_utc,
        "neighbor_market_price_eur_mwh": price_eur_mwh,
        "neighbor_market_price_uah_mwh": None,
        "source_backed": source_backed,
        "fetch_enabled": fetch_enabled,
        "security_token_required": True,
        "security_token_available": token_available,
        "source_access_method": "entsoe_rest_api",
        "source_url": ENTSOE_API_BASE_URL,
        "source_retrieved_at_utc": "",
        "source_sha256": "",
        "source_license_status": "requires_entsoe_terms_mapping",
        "fetch_status": fetch_status,
        "sample_period_start_utc": sample_period_start_utc,
        "sample_period_end_utc": sample_period_end_utc,
        "publication_time_policy": query_row["publication_time_policy"],
        "publication_timestamp_utc": "",
        "publication_time_status": "blocked_missing_publication_timestamp",
        "ua_decision_anchor_policy": (
            "publication_must_precede_ukrainian_dam_decision_anchor"
        ),
        "is_prior_to_ua_decision_anchor": False,
        "time_zone_policy": query_row["time_zone_policy"],
        "currency_policy": "blocked_until_eur_to_uah_prior_only_normalization",
        "fx_rate_source": "",
        "fx_rate_timestamp_utc": "",
        "currency_normalization_status": "blocked_missing_prior_eur_uah_fx_rate",
        "training_use_allowed": False,
        "feature_use_allowed": False,
        "training_blockers_csv": EXTERNAL_TRAINING_BLOCKERS,
        "claim_scope": ENTSOE_NEIGHBOR_MARKET_FEATURE_CANDIDATE_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _aligned_feature_row(
    benchmark_row: dict[str, object],
    *,
    country_code: str,
    timestamp: datetime,
    candidate: dict[str, object] | None,
) -> dict[str, object]:
    if candidate is None:
        country_lower = country_code.lower()
        return {
            "tenant_id": str(benchmark_row["tenant_id"]),
            "timestamp": timestamp,
            "country_code": country_code,
            "country_name": "",
            "bidding_zone_eic": "",
            "feature_name": "entsoe_neighbor_day_ahead_price_context",
            "feature_column": f"entsoe_{country_lower}_day_ahead_price_eur_mwh",
            "delivery_timestamp_utc": "",
            "neighbor_market_price_eur_mwh": None,
            "neighbor_market_price_uah_mwh": None,
            "source_backed": False,
            "security_token_required": True,
            "security_token_available": False,
            "source_access_method": "",
            "source_url": "",
            "source_retrieved_at_utc": "",
            "source_sha256": "",
            "source_license_status": "",
            "fetch_status": "no_source_candidate_for_benchmark_timestamp",
            "publication_time_policy": "must_be_published_before_ua_anchor",
            "publication_timestamp_utc": "",
            "publication_time_status": "blocked_missing_publication_timestamp",
            "is_prior_to_ua_decision_anchor": False,
            "time_zone_policy": "request_utc_align_to_europe_kyiv_anchor",
            "currency_policy": "blocked_until_eur_to_uah_prior_only_normalization",
            "fx_rate_source": "",
            "fx_rate_timestamp_utc": "",
            "currency_normalization_status": "blocked_missing_prior_eur_uah_fx_rate",
            "training_use_allowed": False,
            "feature_use_allowed": False,
            "training_blockers_csv": EXTERNAL_TRAINING_BLOCKERS,
            "claim_scope": ENTSOE_NEIGHBOR_MARKET_ALIGNED_FEATURE_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    return {
        "tenant_id": str(benchmark_row["tenant_id"]),
        "timestamp": timestamp,
        "country_code": str(candidate["country_code"]).upper(),
        "country_name": str(candidate["country_name"]),
        "bidding_zone_eic": str(candidate["bidding_zone_eic"]),
        "feature_name": str(candidate["feature_name"]),
        "feature_column": str(candidate["feature_column"]),
        "delivery_timestamp_utc": str(candidate["delivery_timestamp_utc"]),
        "neighbor_market_price_eur_mwh": candidate["neighbor_market_price_eur_mwh"],
        "neighbor_market_price_uah_mwh": candidate["neighbor_market_price_uah_mwh"],
        "source_backed": bool(candidate["source_backed"]),
        "security_token_required": bool(candidate.get("security_token_required", True)),
        "security_token_available": bool(candidate["security_token_available"]),
        "source_access_method": str(candidate.get("source_access_method", "")),
        "source_url": str(candidate.get("source_url", "")),
        "source_retrieved_at_utc": str(candidate.get("source_retrieved_at_utc", "")),
        "source_sha256": str(candidate.get("source_sha256", "")),
        "source_license_status": str(candidate.get("source_license_status", "")),
        "fetch_status": str(candidate["fetch_status"]),
        "publication_time_policy": str(candidate["publication_time_policy"]),
        "publication_timestamp_utc": str(candidate["publication_timestamp_utc"]),
        "publication_time_status": str(candidate["publication_time_status"]),
        "is_prior_to_ua_decision_anchor": bool(candidate["is_prior_to_ua_decision_anchor"]),
        "time_zone_policy": str(candidate["time_zone_policy"]),
        "currency_policy": str(candidate["currency_policy"]),
        "fx_rate_source": str(candidate["fx_rate_source"]),
        "fx_rate_timestamp_utc": str(candidate["fx_rate_timestamp_utc"]),
        "currency_normalization_status": str(candidate["currency_normalization_status"]),
        "training_use_allowed": False,
        "feature_use_allowed": False,
        "training_blockers_csv": EXTERNAL_TRAINING_BLOCKERS,
        "claim_scope": ENTSOE_NEIGHBOR_MARKET_ALIGNED_FEATURE_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _entsoe_poland_governance_blockers(
    *,
    token_required: bool,
    token_available: bool,
    source_backed_count: int,
    publication_prior: bool,
    timezone_dst_mapping_ready: bool,
    prior_eur_uah_fx_rate: float,
    fx_prior: bool,
    fx_rate_source: str,
    licensing_approved: bool,
    market_rules_mapped: bool,
    domain_shift_validated: bool,
) -> list[str]:
    blockers: list[str] = []
    if token_required and not token_available:
        blockers.append("entsoe_token")
    if source_backed_count <= 0:
        blockers.append("source_backed_sample")
    if not publication_prior:
        blockers.append("publication_time")
    if not timezone_dst_mapping_ready:
        blockers.append("timezone")
    if prior_eur_uah_fx_rate <= 0.0 or not fx_prior or not fx_rate_source.strip():
        blockers.append("prior_eur_uah_fx_rate")
    if not licensing_approved:
        blockers.append("licensing")
    if not market_rules_mapped:
        blockers.append("market_rules")
    if not domain_shift_validated:
        blockers.append("domain_shift")
    return blockers


def _request_url(
    bidding_zone_eic: str,
    *,
    security_token: str,
    period_start: str,
    period_end: str,
) -> str:
    return (
        f"{ENTSOE_API_BASE_URL}?"
        f"securityToken={quote(security_token)}"
        "&documentType=A44"
        "&processType=A01"
        f"&in_Domain={quote(bidding_zone_eic)}"
        f"&out_Domain={quote(bidding_zone_eic)}"
        f"&periodStart={quote(period_start)}"
        f"&periodEnd={quote(period_end)}"
    )


def _fetch_text(url: str) -> str:
    with urlopen(url, timeout=60) as response:  # noqa: S310 - URL is ENTSO-E API.
        return response.read().decode("utf-8")


def _parse_day_ahead_price_points(xml_text: str) -> list[tuple[datetime, float]]:
    root = ElementTree.fromstring(xml_text)
    points: list[tuple[datetime, float]] = []
    for period in _descendants_named(root, "Period"):
        start_text = _child_text(_child_named(period, "timeInterval"), "start")
        if not start_text:
            continue
        start = _parse_iso_utc(start_text)
        for point in _children_named(period, "Point"):
            position_text = _child_text(point, "position")
            price_text = _child_text(point, "price.amount")
            if not position_text or not price_text:
                continue
            position = int(position_text)
            timestamp = start + timedelta(hours=position - 1)
            points.append((timestamp, float(price_text)))
    return sorted(points, key=lambda item: item[0])


def _parse_entsoe_period_utc(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%d%H%M").replace(tzinfo=UTC)


def _parse_iso_utc(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _timestamp_naive_utc(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value
    parsed = datetime.fromisoformat(str(value))
    return parsed.astimezone(UTC).replace(tzinfo=None) if parsed.tzinfo else parsed


def _csv_values(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _descendants_named(element: ElementTree.Element, name: str) -> list[ElementTree.Element]:
    return [child for child in element.iter() if _local_name(child.tag) == name]


def _children_named(element: ElementTree.Element, name: str) -> list[ElementTree.Element]:
    return [child for child in list(element) if _local_name(child.tag) == name]


def _child_named(element: ElementTree.Element | None, name: str) -> ElementTree.Element | None:
    if element is None:
        return None
    for child in list(element):
        if _local_name(child.tag) == name:
            return child
    return None


def _child_text(element: ElementTree.Element | None, name: str) -> str:
    child = _child_named(element, name)
    return (child.text or "").strip() if child is not None else ""


def _local_name(tag: str) -> str:
    return tag.rsplit("}", maxsplit=1)[-1]


__all__ = [
    "build_entsoe_neighbor_market_aligned_feature_panel_frame",
    "build_entsoe_neighbor_market_feature_candidate_frame",
    "build_entsoe_neighbor_market_query_spec_frame",
    "build_entsoe_neighbor_market_sample_audit_frame",
    "build_entsoe_poland_feature_governance_frame",
    "validate_entsoe_neighbor_market_access_evidence",
    "validate_entsoe_neighbor_market_feature_candidate_evidence",
    "validate_entsoe_neighbor_market_sample_audit_evidence",
    "validate_entsoe_poland_feature_governance_evidence",
]
