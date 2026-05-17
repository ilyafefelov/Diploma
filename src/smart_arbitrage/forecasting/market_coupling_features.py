"""Approved prior-only market-coupling feature routes for official forecasts."""

from __future__ import annotations

from typing import Final

import polars as pl

from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome
from smart_arbitrage.forecasting.market_coupling_availability import (
    EXTERNAL_TRAINING_BLOCKERS,
    REQUIRED_MARKET_COUPLING_AVAILABILITY_COLUMNS,
)

MARKET_COUPLING_FEATURE_ROUTE_CLAIM_SCOPE: Final[str] = (
    "market_coupling_feature_route_research_gate"
)
REQUIRED_MARKET_COUPLING_FEATURE_ROUTE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "feature_name",
        "source_name",
        "source_kind",
        "approved_feature_column",
        "feature_route_status",
        "source_backed_row_count",
        "training_use_allowed",
        "feature_use_allowed",
        "approved_for_official_training",
        "training_blockers_csv",
        "readiness_status",
        "licensing_status",
        "timezone_status",
        "currency_status",
        "market_rules_status",
        "temporal_availability_status",
        "domain_shift_status",
        "publication_time_policy",
        "decision_cutoff_policy",
        "external_feature_role",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_market_coupling_feature_route_frame(
    market_coupling_temporal_availability_frame: pl.DataFrame,
    *,
    entsoe_neighbor_market_sample_audit_frame: pl.DataFrame | None = None,
    entsoe_poland_feature_governance_frame: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Build the single approval interface for official exogenous features.

    Availability rows define governance status. Source samples can prove that
    a neighbor source is reachable, but they never unlock training on their own.
    """

    missing = sorted(
        REQUIRED_MARKET_COUPLING_AVAILABILITY_COLUMNS.difference(
            market_coupling_temporal_availability_frame.columns
        )
    )
    if missing:
        raise ValueError(
            f"market_coupling_temporal_availability_frame missing columns: {missing}"
        )

    entsoe_source_backed_rows = _entsoe_source_backed_rows(
        entsoe_neighbor_market_sample_audit_frame
    )
    entsoe_poland_governance = _entsoe_poland_governance_row(
        entsoe_poland_feature_governance_frame
    )
    rows = [
        _route_row(
            row,
            entsoe_source_backed_rows=entsoe_source_backed_rows,
            entsoe_poland_governance=entsoe_poland_governance,
        )
        for row in market_coupling_temporal_availability_frame.iter_rows(named=True)
    ]
    return pl.DataFrame(rows).sort(["source_name", "feature_name"])


def validate_market_coupling_feature_route_evidence(
    frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate that only fully governed prior-only features can train."""

    failures = _missing_column_failures(frame, REQUIRED_MARKET_COUPLING_FEATURE_ROUTE_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": frame.height})
    rows = list(frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(
            False,
            "market-coupling feature route frame has no rows",
            {"row_count": 0},
        )

    unready_approved_rows = [
        row
        for row in rows
        if (
            bool(row["training_use_allowed"])
            or bool(row["feature_use_allowed"])
            or bool(row["approved_for_official_training"])
        )
        and not _row_is_fully_governed(row)
    ]
    unbacked_approved_rows = [
        row
        for row in rows
        if bool(row["approved_for_official_training"])
        and int(row["source_backed_row_count"]) <= 0
    ]
    bad_claim_rows = [
        row
        for row in rows
        if str(row["claim_scope"]) != MARKET_COUPLING_FEATURE_ROUTE_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
    ]
    bad_status_rows = [
        row
        for row in rows
        if str(row["feature_route_status"])
        not in {
            "approved_for_training",
            "source_backed_but_governance_blocked",
            "blocked_by_governance",
        }
    ]
    if unready_approved_rows:
        failures.append("market-coupling route must not approve unready external features")
    if unbacked_approved_rows:
        failures.append("market-coupling route must not approve source-unbacked features")
    if bad_claim_rows:
        failures.append("market-coupling route rows must keep research-only claim flags")
    if bad_status_rows:
        failures.append("market-coupling route rows have invalid feature_route_status")

    metadata = {
        "row_count": len(rows),
        "approved_feature_count": len(
            [row for row in rows if bool(row["approved_for_official_training"])]
        ),
        "source_backed_rows": sum(int(row["source_backed_row_count"]) for row in rows),
        "unready_approved_rows": len(unready_approved_rows),
        "unbacked_approved_rows": len(unbacked_approved_rows),
        "bad_claim_rows": len(bad_claim_rows),
        "bad_status_rows": len(bad_status_rows),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Market-coupling feature route keeps external features blocked until governed."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def market_coupling_feature_route_metadata(
    route_frame: pl.DataFrame | None,
) -> dict[str, str]:
    """Return official-training metadata derived from the feature route frame."""

    if route_frame is None:
        return {
            "external_feature_training_status": "not_configured",
            "allowed_external_feature_columns_csv": "",
            "blocked_external_feature_columns_csv": "",
            "external_training_blockers_csv": "",
            "external_feature_governance_scope": "market_coupling_not_attached",
        }

    outcome = validate_market_coupling_feature_route_evidence(route_frame)
    if not outcome.passed:
        if "must not approve unready external features" in outcome.description:
            names = sorted(
                str(row["feature_name"])
                for row in route_frame.iter_rows(named=True)
                if bool(row["training_use_allowed"]) and not _row_is_fully_governed(row)
            )
            raise ValueError(
                "external market-coupling features cannot be training_use_allowed "
                f"before governance mapping is complete: {names}"
            )
        raise ValueError(f"market_coupling_feature_route_frame invalid: {outcome.description}")

    rows = list(route_frame.iter_rows(named=True))
    allowed = sorted(
        str(row["approved_feature_column"])
        for row in rows
        if bool(row["approved_for_official_training"])
    )
    blocked = sorted(
        str(row["approved_feature_column"])
        for row in rows
        if str(row["approved_feature_column"]).strip()
        and str(row["approved_feature_column"]) not in allowed
    )
    return {
        "external_feature_training_status": "training_ready"
        if allowed
        else "blocked_by_governance",
        "allowed_external_feature_columns_csv": ",".join(allowed),
        "blocked_external_feature_columns_csv": ",".join(blocked),
        "external_training_blockers_csv": EXTERNAL_TRAINING_BLOCKERS if blocked else "",
        "external_feature_governance_scope": "market_coupling_feature_route_frame",
    }


def _route_row(
    row: dict[str, object],
    *,
    entsoe_source_backed_rows: int,
    entsoe_poland_governance: dict[str, object] | None,
) -> dict[str, object]:
    source_name = str(row["source_name"])
    effective_row = _effective_route_row(
        row,
        entsoe_poland_governance=entsoe_poland_governance,
    )
    source_backed_row_count = (
        _governed_entsoe_source_backed_rows(
            entsoe_source_backed_rows,
            entsoe_poland_governance=entsoe_poland_governance,
        )
        if source_name == "ENTSO_E"
        else _source_observation_count(row)
    )
    fully_governed = _row_is_fully_governed(effective_row) and source_backed_row_count > 0
    if fully_governed:
        feature_route_status = "approved_for_training"
    elif source_backed_row_count > 0:
        feature_route_status = "source_backed_but_governance_blocked"
    else:
        feature_route_status = "blocked_by_governance"
    return {
        "feature_name": str(effective_row["feature_name"]),
        "source_name": source_name,
        "source_kind": str(effective_row["source_kind"]),
        "approved_feature_column": str(effective_row["approved_feature_column"]),
        "feature_route_status": feature_route_status,
        "source_backed_row_count": source_backed_row_count,
        "training_use_allowed": bool(effective_row["training_use_allowed"]),
        "feature_use_allowed": fully_governed,
        "approved_for_official_training": fully_governed,
        "training_blockers_csv": str(effective_row["training_blockers_csv"]),
        "readiness_status": str(effective_row["readiness_status"]),
        "licensing_status": str(effective_row["licensing_status"]),
        "timezone_status": str(effective_row["timezone_status"]),
        "currency_status": str(effective_row["currency_status"]),
        "market_rules_status": str(effective_row["market_rules_status"]),
        "temporal_availability_status": str(effective_row["temporal_availability_status"]),
        "domain_shift_status": str(effective_row["domain_shift_status"]),
        "publication_time_policy": str(effective_row["publication_time_policy"]),
        "decision_cutoff_policy": str(effective_row["decision_cutoff_policy"]),
        "external_feature_role": str(effective_row["external_validation_role"]),
        "claim_scope": MARKET_COUPLING_FEATURE_ROUTE_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _row_is_fully_governed(row: dict[str, object]) -> bool:
    return (
        bool(row["training_use_allowed"])
        and str(row["training_blockers_csv"]) == ""
        and str(row.get("readiness_status", "")) == "training_ready"
        and all(
            str(row[column_name]) == "ready"
            for column_name in (
                "licensing_status",
                "timezone_status",
                "currency_status",
                "market_rules_status",
                "temporal_availability_status",
                "domain_shift_status",
            )
        )
    )


def _source_observation_count(row: dict[str, object]) -> int:
    value = row.get("source_observation_count", 0)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        return int(value)
    return 0


def _entsoe_source_backed_rows(frame: pl.DataFrame | None) -> int:
    if frame is None or frame.is_empty():
        return 0
    if "source_backed_row_count" not in frame.columns:
        raise ValueError("entsoe_neighbor_market_sample_audit_frame missing source_backed_row_count")
    return int(frame.select(pl.col("source_backed_row_count").sum()).item() or 0)


def _entsoe_poland_governance_row(frame: pl.DataFrame | None) -> dict[str, object] | None:
    if frame is None or frame.is_empty():
        return None
    required_columns = {
        "country_code",
        "feature_name",
        "approved_feature_column",
        "source_backed_row_count",
        "training_use_allowed",
        "training_blockers_csv",
        "readiness_status",
        "licensing_status",
        "timezone_status",
        "currency_status",
        "market_rules_status",
        "temporal_availability_status",
        "domain_shift_status",
    }
    missing = sorted(required_columns.difference(frame.columns))
    if missing:
        raise ValueError(f"entsoe_poland_feature_governance_frame missing columns: {missing}")
    poland_rows = frame.filter(pl.col("country_code") == "PL")
    if poland_rows.height != 1:
        raise ValueError("entsoe_poland_feature_governance_frame must contain one PL row")
    return poland_rows.to_dicts()[0]


def _effective_route_row(
    row: dict[str, object],
    *,
    entsoe_poland_governance: dict[str, object] | None,
) -> dict[str, object]:
    if str(row["source_name"]) != "ENTSO_E" or entsoe_poland_governance is None:
        return {
            **row,
            "approved_feature_column": str(row["feature_name"]),
        }
    return {
        **row,
        "approved_feature_column": str(entsoe_poland_governance["approved_feature_column"]),
        "training_use_allowed": bool(entsoe_poland_governance["training_use_allowed"]),
        "training_blockers_csv": str(entsoe_poland_governance["training_blockers_csv"]),
        "readiness_status": str(entsoe_poland_governance["readiness_status"]),
        "licensing_status": str(entsoe_poland_governance["licensing_status"]),
        "timezone_status": str(entsoe_poland_governance["timezone_status"]),
        "currency_status": str(entsoe_poland_governance["currency_status"]),
        "market_rules_status": str(entsoe_poland_governance["market_rules_status"]),
        "temporal_availability_status": str(
            entsoe_poland_governance["temporal_availability_status"]
        ),
        "domain_shift_status": str(entsoe_poland_governance["domain_shift_status"]),
    }


def _governed_entsoe_source_backed_rows(
    entsoe_source_backed_rows: int,
    *,
    entsoe_poland_governance: dict[str, object] | None,
) -> int:
    if entsoe_poland_governance is None:
        return entsoe_source_backed_rows
    source_backed_row_count = entsoe_poland_governance["source_backed_row_count"]
    if isinstance(source_backed_row_count, bool):
        return int(source_backed_row_count)
    if isinstance(source_backed_row_count, int):
        return source_backed_row_count
    if isinstance(source_backed_row_count, float | str):
        return int(source_backed_row_count)
    raise TypeError("source_backed_row_count must be numeric or string-like.")


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"missing required columns: {missing}"] if missing else []
