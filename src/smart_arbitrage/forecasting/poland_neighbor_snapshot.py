"""No-token Poland neighbor-market snapshot ingestion and parser.

This module supports manually exported or public CSV snapshots. It does not
scrape protected ENTSO-E pages and it never approves training use by itself.
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Final

import polars as pl

from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome
from smart_arbitrage.forecasting.entsoe_neighbor_access import (
    ENTSOE_NEIGHBOR_MARKET_FEATURE_CANDIDATE_CLAIM_SCOPE,
)
from smart_arbitrage.forecasting.market_coupling_availability import (
    EXTERNAL_TRAINING_BLOCKERS,
)

POLAND_NEIGHBOR_MARKET_SNAPSHOT_CLAIM_SCOPE: Final[str] = (
    "poland_neighbor_market_snapshot_bronze_research_gate"
)
POLAND_NEIGHBOR_MARKET_HOURLY_FEATURE_CLAIM_SCOPE: Final[str] = (
    "poland_neighbor_market_hourly_feature_research_gate"
)
ENTSOE_POLAND_GOVERNANCE_CLOSURE_CLAIM_SCOPE: Final[str] = (
    "entsoe_poland_governance_closure_research_gate"
)
REQUIRED_POLAND_NEIGHBOR_MARKET_SNAPSHOT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "country_code",
        "country_name",
        "feature_name",
        "feature_column",
        "snapshot_kind",
        "delivery_timestamp_utc",
        "neighbor_market_price_eur_mwh",
        "source_name",
        "source_url",
        "source_access_method",
        "source_retrieved_at_utc",
        "source_publication_timestamp_utc",
        "source_license_status",
        "source_sha256",
        "source_backed",
        "snapshot_status",
        "security_token_required",
        "training_use_allowed",
        "feature_use_allowed",
        "market_execution_enabled",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_POLAND_NEIGHBOR_MARKET_HOURLY_FEATURE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "country_code",
        "country_name",
        "feature_name",
        "feature_column",
        "delivery_hour_utc",
        "hourly_neighbor_market_price_eur_mwh",
        "source_interval_count",
        "source_resolution_minutes",
        "source_name",
        "source_url",
        "source_access_method",
        "source_retrieved_at_utc",
        "source_publication_timestamp_utc",
        "source_license_status",
        "source_sha256",
        "source_backed",
        "hourly_feature_status",
        "training_use_allowed",
        "feature_use_allowed",
        "market_execution_enabled",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_ENTSOE_POLAND_GOVERNANCE_CLOSURE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "country_code",
        "country_name",
        "feature_name",
        "approved_feature_column",
        "source_backed_hour_count",
        "source_interval_count",
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

_TIMESTAMP_COLUMN_ALIASES: Final[tuple[str, ...]] = (
    "delivery_timestamp_utc",
    "timestamp_utc",
    "timestamp",
    "datetime",
    "ds",
)
_PRICE_COLUMN_ALIASES: Final[tuple[str, ...]] = (
    "price_eur_mwh",
    "neighbor_market_price_eur_mwh",
    "dam_price_eur_mwh",
    "tgebase_eur_mwh",
    "price",
    "Price",
)


def build_poland_neighbor_market_snapshot_bronze_frame(
    *,
    snapshot_csv_path: str | Path | None,
    source_url: str,
    source_retrieved_at_utc: str,
    source_publication_timestamp_utc: str = "",
    source_access_method: str = "manual_export_csv",
    source_license_status: str = "review_required",
    snapshot_kind: str = "day_ahead_price_eur_mwh",
) -> pl.DataFrame:
    """Parse a local Poland neighbor-market CSV snapshot into source rows."""

    if snapshot_csv_path is None or not str(snapshot_csv_path).strip():
        return pl.DataFrame(
            [
                _snapshot_row(
                    delivery_timestamp_utc="",
                    price_eur_mwh=None,
                    source_url=source_url,
                    source_access_method=source_access_method,
                    source_retrieved_at_utc=source_retrieved_at_utc,
                    source_publication_timestamp_utc=source_publication_timestamp_utc,
                    source_license_status=source_license_status,
                    source_sha256="",
                    source_backed=False,
                    snapshot_status="blocked_missing_snapshot_path",
                    snapshot_kind=snapshot_kind,
                )
            ]
        )

    path = Path(snapshot_csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Poland neighbor-market snapshot not found: {path}")
    raw_bytes = path.read_bytes()
    source_hash = sha256(raw_bytes).hexdigest()
    raw_frame = pl.read_csv(path)
    timestamp_column = _find_required_column(
        raw_frame,
        _TIMESTAMP_COLUMN_ALIASES,
        description="timestamp column",
    )
    price_column = _find_required_column(
        raw_frame,
        _PRICE_COLUMN_ALIASES,
        description="price column",
    )
    rows: list[dict[str, object]] = []
    for row in raw_frame.iter_rows(named=True):
        price = _optional_float(row[price_column])
        if price is None:
            raise ValueError("price column must contain numeric EUR/MWh values")
        rows.append(
            _snapshot_row(
                delivery_timestamp_utc=_normalize_iso_utc(row[timestamp_column]),
                price_eur_mwh=price,
                source_url=source_url,
                source_access_method=source_access_method,
                source_retrieved_at_utc=source_retrieved_at_utc,
                source_publication_timestamp_utc=source_publication_timestamp_utc,
                source_license_status=source_license_status,
                source_sha256=source_hash,
                source_backed=True,
                snapshot_status="source_backed_manual_snapshot_not_training",
                snapshot_kind=snapshot_kind,
            )
        )
    return pl.DataFrame(rows).sort("delivery_timestamp_utc")


def validate_poland_neighbor_market_snapshot_evidence(
    frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate snapshot rows are source evidence, not training approval."""

    failures = _missing_column_failures(
        frame,
        REQUIRED_POLAND_NEIGHBOR_MARKET_SNAPSHOT_COLUMNS,
    )
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": frame.height})
    rows = list(frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "Poland neighbor snapshot has no rows", {"row_count": 0})

    training_rows = [row for row in rows if bool(row["training_use_allowed"])]
    feature_rows = [row for row in rows if bool(row["feature_use_allowed"])]
    token_required_rows = [row for row in rows if bool(row["security_token_required"])]
    bad_claim_rows = [
        row
        for row in rows
        if str(row["claim_scope"]) != POLAND_NEIGHBOR_MARKET_SNAPSHOT_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
        or bool(row["market_execution_enabled"])
    ]
    missing_hash_rows = [
        row for row in rows if bool(row["source_backed"]) and not str(row["source_sha256"]).strip()
    ]
    if training_rows or feature_rows:
        failures.append("Poland neighbor snapshots must not approve feature/training use")
    if token_required_rows:
        failures.append("manual/public snapshot route must not require ENTSO-E token")
    if bad_claim_rows:
        failures.append("Poland neighbor snapshots must keep research-only claim flags")
    if missing_hash_rows:
        failures.append("source-backed snapshot rows must include source_sha256")

    metadata = {
        "row_count": len(rows),
        "source_backed_rows": len([row for row in rows if bool(row["source_backed"])]),
        "training_allowed_rows": len(training_rows),
        "feature_allowed_rows": len(feature_rows),
        "token_required_rows": len(token_required_rows),
        "bad_claim_rows": len(bad_claim_rows),
        "missing_hash_rows": len(missing_hash_rows),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Poland neighbor-market snapshots remain source-backed research-only evidence."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def build_poland_neighbor_market_snapshot_feature_candidate_frame(
    poland_neighbor_market_snapshot_bronze: pl.DataFrame,
    *,
    ua_decision_anchor_timestamp_utc: str = "",
    prior_eur_uah_fx_rate: float = 0.0,
    prior_eur_uah_fx_timestamp_utc: str = "",
    fx_rate_source: str = "",
) -> pl.DataFrame:
    """Convert no-token Poland snapshots into the existing feature-candidate contract."""

    outcome = validate_poland_neighbor_market_snapshot_evidence(
        poland_neighbor_market_snapshot_bronze
    )
    if not outcome.passed:
        raise ValueError(f"Poland neighbor snapshot invalid: {outcome.description}")

    anchor = _parse_optional_iso_utc(ua_decision_anchor_timestamp_utc)
    fx_timestamp = _parse_optional_iso_utc(prior_eur_uah_fx_timestamp_utc)
    fx_prior = fx_timestamp is not None and anchor is not None and fx_timestamp < anchor
    currency_ready = prior_eur_uah_fx_rate > 0.0 and fx_prior and bool(fx_rate_source.strip())

    rows: list[dict[str, object]] = []
    for row in poland_neighbor_market_snapshot_bronze.iter_rows(named=True):
        publication = _parse_optional_iso_utc(str(row["source_publication_timestamp_utc"]))
        publication_prior = (
            publication is not None and anchor is not None and publication < anchor
        )
        price_eur = row["neighbor_market_price_eur_mwh"]
        price_uah = (
            float(price_eur) * prior_eur_uah_fx_rate
            if price_eur is not None and currency_ready
            else None
        )
        rows.append(
            {
                "country_code": "PL",
                "country_name": "Poland",
                "bidding_zone_eic": "10YPL-AREA-----S",
                "feature_name": "entsoe_neighbor_day_ahead_price_context",
                "feature_column": "entsoe_pl_day_ahead_price_eur_mwh",
                "delivery_timestamp_utc": str(row["delivery_timestamp_utc"]),
                "neighbor_market_price_eur_mwh": price_eur,
                "neighbor_market_price_uah_mwh": price_uah,
                "source_backed": bool(row["source_backed"]),
                "fetch_enabled": False,
                "security_token_required": False,
                "security_token_available": False,
                "source_access_method": str(row["source_access_method"]),
                "source_url": str(row["source_url"]),
                "source_retrieved_at_utc": str(row["source_retrieved_at_utc"]),
                "source_sha256": str(row["source_sha256"]),
                "source_license_status": str(row["source_license_status"]),
                "fetch_status": str(row["snapshot_status"]),
                "sample_period_start_utc": "",
                "sample_period_end_utc": "",
                "publication_time_policy": "must_be_published_before_ua_anchor",
                "publication_timestamp_utc": publication.isoformat()
                if publication is not None
                else "",
                "publication_time_status": "publication_time_verified_prior_to_ua_anchor"
                if publication_prior
                else "blocked_missing_or_late_publication_timestamp",
                "ua_decision_anchor_policy": (
                    "publication_must_precede_ukrainian_dam_decision_anchor"
                ),
                "is_prior_to_ua_decision_anchor": publication_prior,
                "time_zone_policy": "manual_export_timestamp_must_be_utc_or_mapped_to_utc",
                "currency_policy": "prior_known_eur_uah_normalization_required",
                "fx_rate_source": fx_rate_source,
                "fx_rate_timestamp_utc": fx_timestamp.isoformat()
                if fx_timestamp is not None
                else "",
                "currency_normalization_status": "prior_eur_uah_normalized"
                if currency_ready
                else "blocked_missing_prior_eur_uah_fx_rate",
                "training_use_allowed": False,
                "feature_use_allowed": False,
                "training_blockers_csv": EXTERNAL_TRAINING_BLOCKERS,
                "claim_scope": ENTSOE_NEIGHBOR_MARKET_FEATURE_CANDIDATE_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(rows).sort(["country_code", "delivery_timestamp_utc"])


def build_poland_neighbor_market_hourly_feature_frame(
    poland_neighbor_market_snapshot_bronze: pl.DataFrame,
) -> pl.DataFrame:
    """Aggregate source-backed Poland price snapshots to hourly feature evidence."""

    outcome = validate_poland_neighbor_market_snapshot_evidence(
        poland_neighbor_market_snapshot_bronze
    )
    if not outcome.passed:
        raise ValueError(f"Poland neighbor snapshot invalid: {outcome.description}")

    source_resolution_minutes = _infer_source_resolution_minutes(
        poland_neighbor_market_snapshot_bronze
    )
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in poland_neighbor_market_snapshot_bronze.iter_rows(named=True):
        if not bool(row["source_backed"]):
            continue
        price = row["neighbor_market_price_eur_mwh"]
        if price is None:
            continue
        delivery = _parse_optional_iso_utc(str(row["delivery_timestamp_utc"]))
        if delivery is None:
            raise ValueError("source-backed Poland snapshot row missing delivery timestamp")
        delivery_hour = delivery.replace(minute=0, second=0, microsecond=0)
        grouped.setdefault(delivery_hour.isoformat(), []).append(row)

    rows: list[dict[str, object]] = []
    for delivery_hour_utc in sorted(grouped):
        source_rows = grouped[delivery_hour_utc]
        prices = [
            float(str(row["neighbor_market_price_eur_mwh"])) for row in source_rows
        ]
        first_row = source_rows[0]
        rows.append(
            {
                "country_code": "PL",
                "country_name": "Poland",
                "feature_name": "entsoe_neighbor_day_ahead_price_context_hourly",
                "feature_column": "entsoe_pl_day_ahead_price_eur_mwh_hourly",
                "delivery_hour_utc": delivery_hour_utc,
                "hourly_neighbor_market_price_eur_mwh": sum(prices) / len(prices),
                "source_interval_count": len(source_rows),
                "source_resolution_minutes": source_resolution_minutes,
                "source_name": str(first_row["source_name"]),
                "source_url": str(first_row["source_url"]),
                "source_access_method": str(first_row["source_access_method"]),
                "source_retrieved_at_utc": str(first_row["source_retrieved_at_utc"]),
                "source_publication_timestamp_utc": str(
                    first_row["source_publication_timestamp_utc"]
                ),
                "source_license_status": str(first_row["source_license_status"]),
                "source_sha256": str(first_row["source_sha256"]),
                "source_backed": True,
                "hourly_feature_status": "source_backed_hourly_not_training",
                "training_use_allowed": False,
                "feature_use_allowed": False,
                "market_execution_enabled": False,
                "claim_scope": POLAND_NEIGHBOR_MARKET_HOURLY_FEATURE_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(rows).sort("delivery_hour_utc")


def validate_poland_neighbor_market_hourly_feature_evidence(
    frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate hourly Poland features remain source-backed, research-only evidence."""

    failures = _missing_column_failures(
        frame,
        REQUIRED_POLAND_NEIGHBOR_MARKET_HOURLY_FEATURE_COLUMNS,
    )
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": frame.height})
    rows = list(frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "Poland hourly feature frame has no rows", {"row_count": 0})

    training_rows = [row for row in rows if bool(row["training_use_allowed"])]
    feature_rows = [row for row in rows if bool(row["feature_use_allowed"])]
    bad_claim_rows = [
        row
        for row in rows
        if str(row["claim_scope"]) != POLAND_NEIGHBOR_MARKET_HOURLY_FEATURE_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
        or bool(row["market_execution_enabled"])
    ]
    bad_source_rows = [
        row
        for row in rows
        if not bool(row["source_backed"])
        or int(row["source_interval_count"]) < 1
        or row["hourly_neighbor_market_price_eur_mwh"] is None
    ]
    if training_rows or feature_rows:
        failures.append("Poland hourly features must not approve feature/training use")
    if bad_claim_rows:
        failures.append("Poland hourly features must keep research-only claim flags")
    if bad_source_rows:
        failures.append("Poland hourly features must be source-backed with valid prices")

    metadata = {
        "row_count": len(rows),
        "source_backed_hour_count": len([row for row in rows if bool(row["source_backed"])]),
        "training_allowed_rows": len(training_rows),
        "feature_allowed_rows": len(feature_rows),
        "bad_claim_rows": len(bad_claim_rows),
        "bad_source_rows": len(bad_source_rows),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Poland hourly neighbor-market features remain source-backed research-only evidence."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def build_entsoe_poland_governance_closure_frame(
    poland_neighbor_market_hourly_feature_frame: pl.DataFrame,
    *,
    ua_decision_anchor_timestamp_utc: str = "2025-12-31T12:00:00+00:00",
    prior_eur_uah_fx_rate: float = 0.0,
    prior_eur_uah_fx_timestamp_utc: str = "",
    fx_rate_source: str = "",
    timezone_dst_mapping_ready: bool = False,
    licensing_approved: bool = False,
    market_rules_mapped: bool = False,
    domain_shift_validated: bool = False,
) -> pl.DataFrame:
    """Summarize whether the source-backed Poland hourly lane is training-eligible."""

    outcome = validate_poland_neighbor_market_hourly_feature_evidence(
        poland_neighbor_market_hourly_feature_frame
    )
    if not outcome.passed:
        raise ValueError(f"Poland hourly feature evidence invalid: {outcome.description}")

    anchor = _parse_optional_iso_utc(ua_decision_anchor_timestamp_utc)
    if anchor is None:
        raise ValueError("ua_decision_anchor_timestamp_utc is required")
    publication_timestamps = sorted(
        {
            str(row["source_publication_timestamp_utc"])
            for row in poland_neighbor_market_hourly_feature_frame.iter_rows(named=True)
            if str(row["source_publication_timestamp_utc"]).strip()
        }
    )
    publication_status, publication_prior, publication_timestamp = _publication_status(
        publication_timestamps,
        anchor,
    )
    fx_timestamp = _parse_optional_iso_utc(prior_eur_uah_fx_timestamp_utc)
    currency_status = _currency_status(
        prior_eur_uah_fx_rate=prior_eur_uah_fx_rate,
        prior_eur_uah_fx_timestamp=fx_timestamp,
        fx_rate_source=fx_rate_source,
        anchor=anchor,
    )
    source_backed_hour_count = poland_neighbor_market_hourly_feature_frame.filter(
        pl.col("source_backed")
    ).height
    source_interval_count = int(
        poland_neighbor_market_hourly_feature_frame.select(
            pl.col("source_interval_count").sum()
        ).item()
    )
    timezone_status = (
        "timezone_dst_mapping_ready"
        if timezone_dst_mapping_ready
        else "blocked_missing_timezone_dst_mapping"
    )
    licensing_status = "licensing_approved" if licensing_approved else "blocked_licensing"
    market_rules_status = (
        "market_rules_mapped" if market_rules_mapped else "blocked_market_rule_mapping"
    )
    domain_shift_status = (
        "domain_shift_validated" if domain_shift_validated else "blocked_domain_shift"
    )
    temporal_availability_status = (
        "temporal_availability_ready"
        if publication_prior and source_backed_hour_count > 0
        else "blocked_temporal_availability"
    )
    blockers = _governance_closure_blockers(
        source_backed_hour_count=source_backed_hour_count,
        publication_time_status=publication_status,
        timezone_status=timezone_status,
        currency_status=currency_status,
        licensing_status=licensing_status,
        market_rules_status=market_rules_status,
        domain_shift_status=domain_shift_status,
        temporal_availability_status=temporal_availability_status,
    )
    approved = not blockers
    feature_columns = sorted(
        poland_neighbor_market_hourly_feature_frame["feature_column"].unique().to_list()
    )
    row = {
        "country_code": "PL",
        "country_name": "Poland",
        "feature_name": "entsoe_neighbor_day_ahead_price_context_hourly",
        "approved_feature_column": feature_columns[0] if feature_columns else "",
        "source_backed_hour_count": source_backed_hour_count,
        "source_interval_count": source_interval_count,
        "publication_timestamp_utc": publication_timestamp,
        "ua_decision_anchor_timestamp_utc": anchor.isoformat(),
        "publication_time_status": publication_status,
        "is_publication_prior_to_anchor": publication_prior,
        "timezone_status": timezone_status,
        "fx_rate_source": fx_rate_source,
        "fx_rate_timestamp_utc": fx_timestamp.isoformat() if fx_timestamp is not None else "",
        "fx_rate_eur_uah": prior_eur_uah_fx_rate,
        "currency_status": currency_status,
        "market_rules_status": market_rules_status,
        "licensing_status": licensing_status,
        "domain_shift_status": domain_shift_status,
        "temporal_availability_status": temporal_availability_status,
        "readiness_status": "approved_for_official_training"
        if approved
        else "blocked_by_governance",
        "training_blockers_csv": ",".join(blockers),
        "training_use_allowed": approved,
        "feature_use_allowed": approved,
        "approved_for_official_training": approved,
        "market_execution_enabled": False,
        "claim_scope": ENTSOE_POLAND_GOVERNANCE_CLOSURE_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }
    return pl.DataFrame([row])


def validate_entsoe_poland_governance_closure_evidence(
    frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate Poland governance closure rows keep claim boundaries coherent."""

    failures = _missing_column_failures(
        frame,
        REQUIRED_ENTSOE_POLAND_GOVERNANCE_CLOSURE_COLUMNS,
    )
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": frame.height})
    rows = list(frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "ENTSO-E Poland governance closure has no rows", {"row_count": 0})

    approved_rows = [row for row in rows if bool(row["approved_for_official_training"])]
    inconsistent_approved_rows = [
        row
        for row in approved_rows
        if str(row["training_blockers_csv"]).strip()
        or not bool(row["training_use_allowed"])
        or not bool(row["feature_use_allowed"])
    ]
    inconsistent_blocked_rows = [
        row
        for row in rows
        if not bool(row["approved_for_official_training"])
        and (bool(row["training_use_allowed"]) or bool(row["feature_use_allowed"]))
    ]
    bad_claim_rows = [
        row
        for row in rows
        if str(row["claim_scope"]) != ENTSOE_POLAND_GOVERNANCE_CLOSURE_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
        or bool(row["market_execution_enabled"])
    ]
    if inconsistent_approved_rows:
        failures.append("approved Poland governance rows must have no blockers")
    if inconsistent_blocked_rows:
        failures.append("blocked Poland governance rows must not allow training/features")
    if bad_claim_rows:
        failures.append("Poland governance closure must keep research-only claim flags")

    metadata = {
        "row_count": len(rows),
        "approved_feature_count": len(approved_rows),
        "blocked_feature_count": len(rows) - len(approved_rows),
        "training_allowed_rows": len(
            [row for row in rows if bool(row["training_use_allowed"])]
        ),
        "bad_claim_rows": len(bad_claim_rows),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "ENTSO-E Poland governance closure is coherent research-only evidence."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def _snapshot_row(
    *,
    delivery_timestamp_utc: str,
    price_eur_mwh: float | None,
    source_url: str,
    source_access_method: str,
    source_retrieved_at_utc: str,
    source_publication_timestamp_utc: str,
    source_license_status: str,
    source_sha256: str,
    source_backed: bool,
    snapshot_status: str,
    snapshot_kind: str,
) -> dict[str, object]:
    return {
        "country_code": "PL",
        "country_name": "Poland",
        "feature_name": "entsoe_neighbor_day_ahead_price_context",
        "feature_column": "entsoe_pl_day_ahead_price_eur_mwh",
        "snapshot_kind": snapshot_kind,
        "delivery_timestamp_utc": delivery_timestamp_utc,
        "neighbor_market_price_eur_mwh": price_eur_mwh,
        "source_name": _source_name(source_url),
        "source_url": source_url,
        "source_access_method": source_access_method,
        "source_retrieved_at_utc": _normalize_iso_utc(source_retrieved_at_utc)
        if source_retrieved_at_utc.strip()
        else "",
        "source_publication_timestamp_utc": _normalize_iso_utc(
            source_publication_timestamp_utc
        )
        if source_publication_timestamp_utc.strip()
        else "",
        "source_license_status": source_license_status,
        "source_sha256": source_sha256,
        "source_backed": source_backed,
        "snapshot_status": snapshot_status,
        "security_token_required": False,
        "training_use_allowed": False,
        "feature_use_allowed": False,
        "market_execution_enabled": False,
        "claim_scope": POLAND_NEIGHBOR_MARKET_SNAPSHOT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _source_name(source_url: str) -> str:
    lowered = source_url.lower()
    if "entsoe" in lowered or "transparencyplatform.zendesk.com" in lowered:
        return "ENTSO_E_MANUAL_EXPORT"
    if "pse.pl" in lowered or "raporty.pse" in lowered:
        return "PSE_PUBLIC_EXPORT"
    if "instrat" in lowered:
        return "INSTRAT_POLISH_DAM"
    return "POLAND_NEIGHBOR_MARKET_SNAPSHOT"


def _find_required_column(
    frame: pl.DataFrame,
    aliases: tuple[str, ...],
    *,
    description: str,
) -> str:
    for alias in aliases:
        if alias in frame.columns:
            return alias
    raise ValueError(f"Poland neighbor snapshot missing {description}: {aliases}")


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return float(str(value).replace(",", "."))


def _normalize_iso_utc(value: object) -> str:
    parsed = _parse_optional_iso_utc(str(value))
    if parsed is None:
        raise ValueError(f"invalid UTC timestamp: {value!r}")
    return parsed.isoformat()


def _infer_source_resolution_minutes(frame: pl.DataFrame) -> int:
    timestamps = sorted(
        parsed
        for row in frame.iter_rows(named=True)
        if bool(row["source_backed"])
        for parsed in [_parse_optional_iso_utc(str(row["delivery_timestamp_utc"]))]
        if parsed is not None
    )
    deltas = [
        int((right - left).total_seconds() // 60)
        for left, right in zip(timestamps, timestamps[1:])
        if right > left
    ]
    return min(deltas) if deltas else 60


def _publication_status(
    publication_timestamps: list[str],
    anchor: datetime,
) -> tuple[str, bool, str]:
    if not publication_timestamps:
        return "blocked_missing_publication_timestamp", False, ""
    parsed_publications = [
        parsed
        for timestamp in publication_timestamps
        for parsed in [_parse_optional_iso_utc(timestamp)]
        if parsed is not None
    ]
    if not parsed_publications:
        return "blocked_missing_publication_timestamp", False, ""
    latest_publication = max(parsed_publications)
    publication_prior = latest_publication < anchor
    return (
        "publication_time_verified_prior_to_ua_anchor"
        if publication_prior
        else "blocked_publication_not_prior_to_anchor",
        publication_prior,
        latest_publication.isoformat(),
    )


def _currency_status(
    *,
    prior_eur_uah_fx_rate: float,
    prior_eur_uah_fx_timestamp: datetime | None,
    fx_rate_source: str,
    anchor: datetime,
) -> str:
    if prior_eur_uah_fx_rate <= 0.0 or not fx_rate_source.strip():
        return "blocked_missing_prior_eur_uah_fx_rate"
    if prior_eur_uah_fx_timestamp is None:
        return "blocked_missing_prior_eur_uah_fx_timestamp"
    if prior_eur_uah_fx_timestamp >= anchor:
        return "blocked_fx_not_prior_to_anchor"
    return "prior_eur_uah_normalized"


def _governance_closure_blockers(
    *,
    source_backed_hour_count: int,
    publication_time_status: str,
    timezone_status: str,
    currency_status: str,
    licensing_status: str,
    market_rules_status: str,
    domain_shift_status: str,
    temporal_availability_status: str,
) -> list[str]:
    blockers: list[str] = []
    if source_backed_hour_count < 1:
        blockers.append("source_backed_coverage")
    if publication_time_status != "publication_time_verified_prior_to_ua_anchor":
        blockers.append("publication_time")
    if timezone_status != "timezone_dst_mapping_ready":
        blockers.append("timezone_dst_mapping")
    if currency_status != "prior_eur_uah_normalized":
        blockers.append("prior_eur_uah_fx")
    if licensing_status != "licensing_approved":
        blockers.append("licensing")
    if market_rules_status != "market_rules_mapped":
        blockers.append("market_rule_mapping")
    if domain_shift_status != "domain_shift_validated":
        blockers.append("domain_shift")
    if temporal_availability_status != "temporal_availability_ready":
        blockers.append("temporal_availability")
    return blockers


def _parse_optional_iso_utc(value: str) -> datetime | None:
    if not value.strip():
        return None
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"missing required columns: {missing}"] if missing else []


__all__ = [
    "ENTSOE_POLAND_GOVERNANCE_CLOSURE_CLAIM_SCOPE",
    "POLAND_NEIGHBOR_MARKET_HOURLY_FEATURE_CLAIM_SCOPE",
    "REQUIRED_POLAND_NEIGHBOR_MARKET_SNAPSHOT_COLUMNS",
    "REQUIRED_POLAND_NEIGHBOR_MARKET_HOURLY_FEATURE_COLUMNS",
    "REQUIRED_ENTSOE_POLAND_GOVERNANCE_CLOSURE_COLUMNS",
    "build_entsoe_poland_governance_closure_frame",
    "build_poland_neighbor_market_hourly_feature_frame",
    "build_poland_neighbor_market_snapshot_bronze_frame",
    "build_poland_neighbor_market_snapshot_feature_candidate_frame",
    "validate_entsoe_poland_governance_closure_evidence",
    "validate_poland_neighbor_market_hourly_feature_evidence",
    "validate_poland_neighbor_market_snapshot_evidence",
]
