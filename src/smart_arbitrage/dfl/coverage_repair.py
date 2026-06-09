"""UA observed-data coverage repair audit helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.data_expansion import REQUIRED_COVERAGE_COLUMNS
from smart_arbitrage.dfl.strict_failure_selector import _datetime_value, _require_columns

UA_COVERAGE_REPAIR_CLAIM_SCOPE: Final[str] = "ua_coverage_repair_audit_not_full_dfl"
UA_COVERAGE_REPAIR_ACADEMIC_SCOPE: Final[str] = (
    "Observed OREE/Open-Meteo coverage repair audit. It reports exact gaps and "
    "blocks non-observed rows from thesis-grade promotion evidence."
)

REQUIRED_COVERAGE_AUDIT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "first_timestamp",
        "last_timestamp",
        "eligible_anchor_count",
        "target_anchor_count_per_tenant",
        "data_quality_tier",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_dfl_ua_coverage_repair_audit_frame(
    feature_frame: pl.DataFrame,
    coverage_audit_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    target_anchor_count_per_tenant: int = 180,
) -> pl.DataFrame:
    """Report exact source-backed gaps that prevent UA DFL coverage expansion."""

    _require_columns(feature_frame, REQUIRED_COVERAGE_COLUMNS, frame_name="feature_frame")
    _require_columns(
        coverage_audit_frame,
        REQUIRED_COVERAGE_AUDIT_COLUMNS,
        frame_name="coverage_audit_frame",
    )
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if target_anchor_count_per_tenant <= 0:
        raise ValueError("target_anchor_count_per_tenant must be positive.")

    feature_rows = list(feature_frame.iter_rows(named=True))
    audit_rows_by_tenant = {
        str(row["tenant_id"]): row
        for row in coverage_audit_frame.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        if tenant_id not in audit_rows_by_tenant:
            rows.append(
                _tenant_missing_row(
                    tenant_id=tenant_id,
                    target_anchor_count_per_tenant=target_anchor_count_per_tenant,
                )
            )
            continue
        audit_row = audit_rows_by_tenant[tenant_id]
        tenant_feature_rows = [
            row for row in feature_rows if str(row["tenant_id"]) == tenant_id
        ]
        tenant_rows = _gap_rows_for_tenant(
            tenant_id=tenant_id,
            feature_rows=tenant_feature_rows,
            audit_row=audit_row,
            target_anchor_count_per_tenant=target_anchor_count_per_tenant,
        )
        rows.extend(tenant_rows)

    if not rows:
        return _empty_repair_frame()
    return pl.DataFrame(rows).sort(["tenant_id", "missing_timestamp"])


def _gap_rows_for_tenant(
    *,
    tenant_id: str,
    feature_rows: list[dict[str, Any]],
    audit_row: dict[str, Any],
    target_anchor_count_per_tenant: int,
) -> list[dict[str, Any]]:
    first_timestamp = _datetime_value(audit_row["first_timestamp"], field_name="first_timestamp")
    last_timestamp = _datetime_value(audit_row["last_timestamp"], field_name="last_timestamp")
    if last_timestamp < first_timestamp:
        raise ValueError(f"coverage audit timestamp range is invalid for {tenant_id}")

    rows_by_timestamp: dict[datetime, list[dict[str, Any]]] = {}
    for row in feature_rows:
        timestamp = _datetime_value(row["timestamp"], field_name="timestamp")
        rows_by_timestamp.setdefault(timestamp, []).append(row)

    expected_timestamps = _hourly_range(first_timestamp, last_timestamp)
    repair_rows: list[dict[str, Any]] = []
    for timestamp in expected_timestamps:
        timestamp_rows = rows_by_timestamp.get(timestamp, [])
        price_present = _has_price(timestamp_rows)
        weather_present = bool(timestamp_rows)
        price_observed = _price_observed(timestamp_rows)
        weather_observed = _weather_observed(timestamp_rows)
        if price_observed and weather_observed:
            continue
        repair_rows.append(
            _repair_row(
                tenant_id=tenant_id,
                timestamp=timestamp,
                audit_row=audit_row,
                target_anchor_count_per_tenant=target_anchor_count_per_tenant,
                price_present=price_present,
                weather_present=weather_present,
                price_observed=price_observed,
                weather_observed=weather_observed,
            )
        )
    if repair_rows:
        return repair_rows
    return [
        _no_gap_row(
            tenant_id=tenant_id,
            audit_row=audit_row,
            target_anchor_count_per_tenant=target_anchor_count_per_tenant,
        )
    ]


def _repair_row(
    *,
    tenant_id: str,
    timestamp: datetime,
    audit_row: dict[str, Any],
    target_anchor_count_per_tenant: int,
    price_present: bool,
    weather_present: bool,
    price_observed: bool,
    weather_observed: bool,
) -> dict[str, Any]:
    gap_kind = _gap_kind(
        price_present=price_present,
        weather_present=weather_present,
        price_observed=price_observed,
        weather_observed=weather_observed,
    )
    non_observed = gap_kind.startswith("non_observed")
    repair_status = (
        "blocked_non_observed_source"
        if non_observed
        else "not_recoverable_from_current_feature_frame"
    )
    eligible_anchor_count = int(audit_row["eligible_anchor_count"])
    return {
        "tenant_id": tenant_id,
        "missing_timestamp": timestamp,
        "gap_kind": gap_kind,
        "repair_status": repair_status,
        "repair_candidate_is_observed": False,
        "source_price_present": price_present,
        "source_weather_present": weather_present,
        "source_price_observed": price_observed,
        "source_weather_observed": weather_observed,
        "coverage_data_quality_tier": str(audit_row["data_quality_tier"]),
        "coverage_ceiling_anchor_count": eligible_anchor_count,
        "target_anchor_count_per_tenant": target_anchor_count_per_tenant,
        "coverage_ceiling_documented": eligible_anchor_count < target_anchor_count_per_tenant,
        "claim_scope": UA_COVERAGE_REPAIR_CLAIM_SCOPE,
        "academic_scope": UA_COVERAGE_REPAIR_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _no_gap_row(
    *,
    tenant_id: str,
    audit_row: dict[str, Any],
    target_anchor_count_per_tenant: int,
) -> dict[str, Any]:
    eligible_anchor_count = int(audit_row["eligible_anchor_count"])
    return {
        "tenant_id": tenant_id,
        "missing_timestamp": None,
        "gap_kind": "none",
        "repair_status": "no_gap",
        "repair_candidate_is_observed": True,
        "source_price_present": True,
        "source_weather_present": True,
        "source_price_observed": True,
        "source_weather_observed": True,
        "coverage_data_quality_tier": str(audit_row["data_quality_tier"]),
        "coverage_ceiling_anchor_count": eligible_anchor_count,
        "target_anchor_count_per_tenant": target_anchor_count_per_tenant,
        "coverage_ceiling_documented": eligible_anchor_count < target_anchor_count_per_tenant,
        "claim_scope": UA_COVERAGE_REPAIR_CLAIM_SCOPE,
        "academic_scope": UA_COVERAGE_REPAIR_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _tenant_missing_row(
    *,
    tenant_id: str,
    target_anchor_count_per_tenant: int,
) -> dict[str, Any]:
    return {
        "tenant_id": tenant_id,
        "missing_timestamp": None,
        "gap_kind": "tenant_missing_from_coverage_audit",
        "repair_status": "not_recoverable_from_current_feature_frame",
        "repair_candidate_is_observed": False,
        "source_price_present": False,
        "source_weather_present": False,
        "source_price_observed": False,
        "source_weather_observed": False,
        "coverage_data_quality_tier": "coverage_gap",
        "coverage_ceiling_anchor_count": 0,
        "target_anchor_count_per_tenant": target_anchor_count_per_tenant,
        "coverage_ceiling_documented": True,
        "claim_scope": UA_COVERAGE_REPAIR_CLAIM_SCOPE,
        "academic_scope": UA_COVERAGE_REPAIR_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _gap_kind(
    *,
    price_present: bool,
    weather_present: bool,
    price_observed: bool,
    weather_observed: bool,
) -> str:
    if price_present and weather_present and not price_observed and not weather_observed:
        return "non_observed_price_and_weather"
    if price_present and not price_observed:
        return "non_observed_price"
    if weather_present and not weather_observed:
        return "non_observed_weather"
    if not price_present and not weather_present:
        return "price_and_weather_gap"
    if not price_present:
        return "price_gap"
    return "weather_gap"


def _has_price(rows: list[dict[str, Any]]) -> bool:
    return any(row.get("price_uah_mwh") is not None for row in rows)


def _price_observed(rows: list[dict[str, Any]]) -> bool:
    return any(
        row.get("price_uah_mwh") is not None and str(row.get("source_kind")) == "observed"
        for row in rows
    )


def _weather_observed(rows: list[dict[str, Any]]) -> bool:
    return any(str(row.get("weather_source_kind")) == "observed" for row in rows)


def _hourly_range(first_timestamp: datetime, last_timestamp: datetime) -> list[datetime]:
    hour_count = int((last_timestamp - first_timestamp).total_seconds() // 3600) + 1
    return [first_timestamp + timedelta(hours=offset) for offset in range(hour_count)]


def _empty_repair_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "tenant_id": pl.String,
            "missing_timestamp": pl.Datetime,
            "gap_kind": pl.String,
            "repair_status": pl.String,
            "repair_candidate_is_observed": pl.Boolean,
            "source_price_present": pl.Boolean,
            "source_weather_present": pl.Boolean,
            "source_price_observed": pl.Boolean,
            "source_weather_observed": pl.Boolean,
            "coverage_data_quality_tier": pl.String,
            "coverage_ceiling_anchor_count": pl.Int64,
            "target_anchor_count_per_tenant": pl.Int64,
            "coverage_ceiling_documented": pl.Boolean,
            "claim_scope": pl.String,
            "academic_scope": pl.String,
            "not_full_dfl": pl.Boolean,
            "not_market_execution": pl.Boolean,
        }
    )
