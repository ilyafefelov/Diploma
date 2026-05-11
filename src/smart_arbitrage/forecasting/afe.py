"""Deterministic AFE feature catalog for forecast and DFL evidence."""

from __future__ import annotations

from typing import Final

import polars as pl

from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome
from smart_arbitrage.forecasting.grid_event_signals import GRID_EVENT_FEATURE_COLUMNS

FORECAST_AFE_FEATURE_CATALOG_CLAIM_SCOPE: Final[str] = (
    "forecast_afe_feature_catalog_research_sidecar"
)

REQUIRED_AFE_FEATURE_CATALOG_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "feature_name",
        "feature_group",
        "feature_family",
        "source_name",
        "source_kind",
        "semantic_source_kind",
        "availability_scope",
        "decision_cutoff_policy",
        "timezone",
        "currency",
        "market_venue",
        "feature_status",
        "training_use_allowed",
        "license_status",
        "leakage_risk",
        "source_url",
        "source_status",
        "training_blockers_csv",
        "temporal_resolution",
        "regions",
        "external_validation_role",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)

_IMPLEMENTED_BASE_ROWS: Final[tuple[dict[str, object], ...]] = (
    {
        "feature_name": "hourly_price_lags",
        "feature_group": "price_history",
        "feature_family": "lagged_market_price",
        "source_name": "OREE_DAM",
        "source_kind": "observed_market_data",
        "semantic_source_kind": "none",
        "availability_scope": "pre_anchor_only",
        "decision_cutoff_policy": "timestamp_lt_anchor_timestamp",
        "timezone": "Europe/Kyiv",
        "currency": "UAH",
        "market_venue": "DAM",
        "feature_status": "implemented",
        "training_use_allowed": True,
        "license_status": "project_source_contract",
        "leakage_risk": "low_when_rolling_origin",
        "notes": "Observed Ukrainian DAM price history only.",
    },
    {
        "feature_name": "calendar_hour_weekday_month",
        "feature_group": "price_history",
        "feature_family": "calendar",
        "source_name": "SYSTEM_CLOCK_FROM_MARKET_TIMESTAMP",
        "source_kind": "derived_calendar",
        "semantic_source_kind": "none",
        "availability_scope": "known_at_anchor",
        "decision_cutoff_policy": "calendar_from_anchor_timestamp",
        "timezone": "Europe/Kyiv",
        "currency": "UAH",
        "market_venue": "DAM",
        "feature_status": "implemented",
        "training_use_allowed": True,
        "license_status": "not_applicable",
        "leakage_risk": "low",
        "notes": "Calendar fields are deterministic and available at decision time.",
    },
    {
        "feature_name": "open_meteo_weather_features",
        "feature_group": "weather_forecast",
        "feature_family": "exogenous_weather",
        "source_name": "OPEN_METEO",
        "source_kind": "historical_weather",
        "semantic_source_kind": "none",
        "availability_scope": "pre_anchor_only",
        "decision_cutoff_policy": "weather_timestamp_lte_anchor_timestamp",
        "timezone": "Europe/Kyiv",
        "currency": "UAH",
        "market_venue": "DAM",
        "feature_status": "implemented",
        "training_use_allowed": True,
        "license_status": "open_data_with_attribution",
        "leakage_risk": "medium_if_historical_actuals_used_as_future_forecast",
        "notes": "Current thesis path uses historical Open-Meteo context as research evidence.",
    },
    {
        "feature_name": "tenant_static_and_load_proxy",
        "feature_group": "tenant_context",
        "feature_family": "tenant_configuration",
        "source_name": "TENANTS_YAML",
        "source_kind": "configured_proxy",
        "semantic_source_kind": "none",
        "availability_scope": "known_at_anchor",
        "decision_cutoff_policy": "static_config_loaded_before_anchor",
        "timezone": "Europe/Kyiv",
        "currency": "UAH",
        "market_venue": "DAM",
        "feature_status": "implemented",
        "training_use_allowed": True,
        "license_status": "project_configuration",
        "leakage_risk": "low",
        "notes": "Configured tenant/load proxies are not measured telemetry.",
    },
)

_EXTERNAL_BRIDGE_ROWS: Final[tuple[dict[str, object], ...]] = (
    {
        "feature_name": "entsoe_neighbor_day_ahead_price_context",
        "source_name": "ENTSO_E",
        "source_kind": "external_market_context",
        "license_status": "requires_api_terms_mapping",
        "source_url": "https://www.entsoe.eu/data/transparency-platform/",
        "source_status": "include_after_mapping",
        "temporal_resolution": "hourly",
        "regions": "european_bidding_zones",
        "external_validation_role": "future_market_coupling_covariate",
        "notes": "Future market-coupling context; blocked until timezone, currency, rules, and licensing are mapped.",
    },
    {
        "feature_name": "pricefm_european_price_context",
        "source_name": "PRICEFM_HF",
        "source_kind": "external_market_context",
        "license_status": "hf_dataset_terms_review_required",
        "source_url": "https://huggingface.co/papers/2508.04875",
        "source_status": "include_watch",
        "temporal_resolution": "15min",
        "regions": "24_countries_38_regions",
        "external_validation_role": "future_external_validation",
        "notes": "PriceFM supports cross-region European price context; not mixed into Ukrainian training.",
    },
    {
        "feature_name": "european_power_system_time_series_placeholder",
        "source_name": "OPSD",
        "source_kind": "external_validation_dataset",
        "license_status": "research_open_data_check_required",
        "source_url": "https://data.open-power-system-data.org/time_series/",
        "source_status": "include_watch",
        "temporal_resolution": "hourly",
        "regions": "european_time_series",
        "external_validation_role": "future_external_validation",
        "notes": "Future external validation source, not Ukrainian training data.",
    },
    {
        "feature_name": "europe_generation_mix_context_placeholder",
        "source_name": "EMBER",
        "source_kind": "external_power_system_context",
        "license_status": "api_terms_check_required",
        "source_url": "https://ember-energy.org/data/api/",
        "source_status": "watch",
        "temporal_resolution": "monthly_or_annual_api_context",
        "regions": "global_and_european_generation_mix",
        "external_validation_role": "future_generation_mix_context",
        "notes": "Future semantic/system context; not currently used for model training.",
    },
    {
        "feature_name": "nord_pool_price_context_placeholder",
        "source_name": "NORD_POOL",
        "source_kind": "external_market_context",
        "license_status": "commercial_or_restricted_access",
        "source_url": "https://www.nordpoolgroup.com/en/services/power-market-data-services/dataportalregistration/",
        "source_status": "watch_restricted",
        "temporal_resolution": "hourly",
        "regions": "nordic_baltic_bidding_zones",
        "external_validation_role": "watch_only_restricted_price_context",
        "notes": "Watch-only bridge because access and redistribution are restricted.",
    },
    {
        "feature_name": "thief_temporal_hierarchy_context",
        "source_name": "THIEF_HF",
        "source_kind": "external_research_dataset",
        "license_status": "dataset_viewer_unavailable_review_required",
        "source_url": "https://huggingface.co/papers/2508.11372",
        "source_status": "watch",
        "temporal_resolution": "unknown_until_dataset_available",
        "regions": "electricity_price_hierarchy_research",
        "external_validation_role": "future_temporal_hierarchy_research",
        "notes": "Relevant temporal hierarchy research; blocked until dataset and availability are reviewable.",
    },
)


def build_forecast_afe_feature_catalog_frame() -> pl.DataFrame:
    """Build the thesis-safe feature registry for automated feature engineering."""

    rows: list[dict[str, object]] = []
    for row in _IMPLEMENTED_BASE_ROWS:
        rows.append(_catalog_row(row))
    for feature_name in GRID_EVENT_FEATURE_COLUMNS:
        rows.append(
            _catalog_row(
                {
                    "feature_name": feature_name,
                    "feature_group": "semantic_grid_event",
                    "feature_family": "official_grid_event_semantic_signal",
                    "source_name": "UKRENERGO_TELEGRAM",
                    "source_kind": "official_public_message",
                    "semantic_source_kind": "official_telegram",
                    "availability_scope": "pre_anchor_only",
                    "decision_cutoff_policy": "published_at_lte_anchor_timestamp",
                    "timezone": "Europe/Kyiv",
                    "currency": "UAH",
                    "market_venue": "DAM",
                    "feature_status": "implemented",
                    "training_use_allowed": True,
                    "license_status": "official_public_telegram_review_required",
                    "leakage_risk": "low_when_published_at_lte_anchor",
                    "notes": "Parsed from Ukrenergo public Telegram posts into grid_event_signal_silver.",
                }
            )
        )
    for row in _EXTERNAL_BRIDGE_ROWS:
        rows.append(
            _catalog_row(
                {
                    **row,
                    "feature_group": "external_market_context",
                    "feature_family": "future_european_bridge",
                    "semantic_source_kind": "external_market_dataset",
                    "availability_scope": "blocked_until_mapped",
                    "decision_cutoff_policy": "blocked_until_temporal_availability_is_mapped",
                    "timezone": "blocked_until_mapped",
                    "currency": "blocked_until_normalized",
                    "market_venue": "blocked_until_market_rules_mapped",
                    "feature_status": "future_bridge",
                    "training_use_allowed": False,
                    "leakage_risk": "high_until_availability_and_domain_shift_are_mapped",
                    "training_blockers_csv": (
                        "licensing,timezone,currency,market_rules,"
                        "temporal_availability,domain_shift"
                    ),
                }
            )
        )
    return pl.DataFrame(rows).sort(["feature_group", "feature_name"])


def validate_forecast_afe_feature_catalog_evidence(frame: pl.DataFrame) -> EvidenceCheckOutcome:
    """Validate that AFE catalog rows keep leak-safe training boundaries."""

    failures = _missing_column_failures(frame, REQUIRED_AFE_FEATURE_CATALOG_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": frame.height})
    rows = list(frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "AFE feature catalog has no rows", {"row_count": 0})

    missing_cutoff_policy_rows = [
        row for row in rows if not str(row["decision_cutoff_policy"]).strip()
    ]
    missing_availability_rows = [
        row for row in rows if not str(row["availability_scope"]).strip()
    ]
    bad_claim_rows = [
        row
        for row in rows
        if str(row["claim_scope"]) != FORECAST_AFE_FEATURE_CATALOG_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
    ]
    external_training_rows = [
        row
        for row in rows
        if str(row["feature_group"]) == "external_market_context"
        and bool(row["training_use_allowed"])
    ]
    external_missing_blockers = [
        row
        for row in rows
        if str(row["feature_group"]) == "external_market_context"
        and str(row["training_blockers_csv"])
        != "licensing,timezone,currency,market_rules,temporal_availability,domain_shift"
    ]
    semantic_rows = [row for row in rows if str(row["feature_group"]) == "semantic_grid_event"]
    missing_semantic_features = sorted(
        set(GRID_EVENT_FEATURE_COLUMNS).difference(
            {str(row["feature_name"]) for row in semantic_rows}
        )
    )
    bad_semantic_rows = [
        row
        for row in semantic_rows
        if str(row["source_name"]) != "UKRENERGO_TELEGRAM"
        or str(row["semantic_source_kind"]) != "official_telegram"
        or str(row["feature_status"]) != "implemented"
        or not bool(row["training_use_allowed"])
    ]

    if missing_cutoff_policy_rows:
        failures.append("AFE feature rows must define decision_cutoff_policy")
    if missing_availability_rows:
        failures.append("AFE feature rows must define availability_scope")
    if bad_claim_rows:
        failures.append("AFE feature catalog claim flags must remain research-only")
    if external_training_rows:
        failures.append("external market bridge rows must not be allowed for training")
    if external_missing_blockers:
        failures.append("external market bridge rows must list all training blockers")
    if missing_semantic_features:
        failures.append(f"missing semantic grid-event features: {missing_semantic_features}")
    if bad_semantic_rows:
        failures.append("semantic grid-event features must use official Telegram rows")

    metadata = {
        "row_count": frame.height,
        "feature_group_count": len({str(row["feature_group"]) for row in rows}),
        "semantic_grid_event_feature_count": len(semantic_rows),
        "external_bridge_row_count": len(
            [row for row in rows if str(row["feature_group"]) == "external_market_context"]
        ),
        "missing_cutoff_policy_rows": len(missing_cutoff_policy_rows),
        "missing_availability_rows": len(missing_availability_rows),
        "bad_claim_rows": len(bad_claim_rows),
        "external_training_rows": len(external_training_rows),
        "external_missing_blocker_rows": len(external_missing_blockers),
        "bad_semantic_rows": len(bad_semantic_rows),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Forecast AFE feature catalog preserves temporal availability and training boundaries."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def _catalog_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "feature_name": str(row["feature_name"]),
        "feature_group": str(row["feature_group"]),
        "feature_family": str(row["feature_family"]),
        "source_name": str(row["source_name"]),
        "source_kind": str(row["source_kind"]),
        "semantic_source_kind": str(row["semantic_source_kind"]),
        "availability_scope": str(row["availability_scope"]),
        "decision_cutoff_policy": str(row["decision_cutoff_policy"]),
        "timezone": str(row["timezone"]),
        "currency": str(row["currency"]),
        "market_venue": str(row["market_venue"]),
        "feature_status": str(row["feature_status"]),
        "training_use_allowed": bool(row["training_use_allowed"]),
        "license_status": str(row["license_status"]),
        "leakage_risk": str(row["leakage_risk"]),
        "source_url": str(row.get("source_url", "")),
        "source_status": str(row.get("source_status", "implemented")),
        "training_blockers_csv": str(row.get("training_blockers_csv", "")),
        "temporal_resolution": str(row.get("temporal_resolution", "hourly")),
        "regions": str(row.get("regions", "ukraine")),
        "external_validation_role": str(row.get("external_validation_role", "training_feature")),
        "claim_scope": FORECAST_AFE_FEATURE_CATALOG_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "notes": str(row.get("notes", "")),
    }


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"missing required columns: {missing}"] if missing else []
