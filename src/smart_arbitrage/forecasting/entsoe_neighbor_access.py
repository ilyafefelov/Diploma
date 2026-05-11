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
REQUIRED_ENTSOE_NEIGHBOR_SAMPLE_AUDIT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "country_code",
        "country_name",
        "bidding_zone_eic",
        "sample_period_start_utc",
        "sample_period_end_utc",
        "fetch_enabled",
        "security_token_available",
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
        "security_token_available": token_available,
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
