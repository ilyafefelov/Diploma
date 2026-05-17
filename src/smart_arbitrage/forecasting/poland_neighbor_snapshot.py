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
    "REQUIRED_POLAND_NEIGHBOR_MARKET_SNAPSHOT_COLUMNS",
    "build_poland_neighbor_market_snapshot_bronze_frame",
    "build_poland_neighbor_market_snapshot_feature_candidate_frame",
    "validate_poland_neighbor_market_snapshot_evidence",
]
