"""Preflight readiness summary for future market-coupling training features."""

from __future__ import annotations

from typing import Final

import polars as pl

from smart_arbitrage.forecasting.market_coupling_features import (
    validate_market_coupling_feature_route_evidence,
)

MARKET_COUPLING_READINESS_CLAIM_BOUNDARY: Final[str] = (
    "market_coupling_readiness_preflight_research_only_not_market_execution"
)


def build_market_coupling_readiness_preflight(
    official_forecast_exogenous_feature_route_frame: pl.DataFrame,
    *,
    entsoe_security_token: str | None = None,
    publication_time_evidence_available: bool = False,
    prior_fx_normalization_available: bool = False,
    timezone_mapping_ready: bool = False,
    licensing_approved: bool = False,
    market_rules_mapped: bool = False,
    domain_shift_validated: bool = False,
) -> dict[str, object]:
    """Summarize whether external market-coupling rows may train official forecasts."""

    outcome = validate_market_coupling_feature_route_evidence(
        official_forecast_exogenous_feature_route_frame
    )
    if not outcome.passed:
        raise ValueError(f"market-coupling route is invalid: {outcome.description}")

    rows = list(official_forecast_exogenous_feature_route_frame.iter_rows(named=True))
    approved_columns = sorted(
        str(row["approved_feature_column"])
        for row in rows
        if bool(row["approved_for_official_training"])
    )
    blockers = _readiness_blockers(
        approved_columns,
        entsoe_security_token=entsoe_security_token,
        publication_time_evidence_available=publication_time_evidence_available,
        prior_fx_normalization_available=prior_fx_normalization_available,
        timezone_mapping_ready=timezone_mapping_ready,
        licensing_approved=licensing_approved,
        market_rules_mapped=market_rules_mapped,
        domain_shift_validated=domain_shift_validated,
    )
    return {
        "schema_version": 1,
        "external_feature_training_ready": not blockers,
        "approved_external_feature_columns_csv": ",".join(approved_columns),
        "blocked_external_feature_columns_csv": ",".join(
            sorted(
                str(row["approved_feature_column"])
                for row in rows
                if str(row["approved_feature_column"]).strip()
                and str(row["approved_feature_column"]) not in approved_columns
            )
        ),
        "source_backed_external_rows": sum(
            int(row["source_backed_row_count"]) for row in rows
        ),
        "entsoe_source_backed_rows": sum(
            int(row["source_backed_row_count"])
            for row in rows
            if str(row["source_name"]) == "ENTSO_E"
        ),
        "readiness_blockers_csv": ",".join(blockers),
        "claim_boundary": MARKET_COUPLING_READINESS_CLAIM_BOUNDARY,
        "market_execution_enabled": False,
        "training_use_allowed": not blockers,
    }


def _readiness_blockers(
    approved_columns: list[str],
    *,
    entsoe_security_token: str | None,
    publication_time_evidence_available: bool,
    prior_fx_normalization_available: bool,
    timezone_mapping_ready: bool,
    licensing_approved: bool,
    market_rules_mapped: bool,
    domain_shift_validated: bool,
) -> list[str]:
    blockers: list[str] = []
    if not approved_columns:
        blockers.append("no_approved_external_features")
    if not entsoe_security_token or not entsoe_security_token.strip():
        blockers.append("entsoe_token")
    if not publication_time_evidence_available:
        blockers.append("publication_time_evidence")
    if not prior_fx_normalization_available:
        blockers.append("prior_eur_uah_fx_rate")
    if not timezone_mapping_ready:
        blockers.append("timezone_dst_mapping")
    if not licensing_approved:
        blockers.append("licensing")
    if not market_rules_mapped:
        blockers.append("market_rules")
    if not domain_shift_validated:
        blockers.append("domain_shift_validation")
    return blockers
