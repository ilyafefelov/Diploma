"""Dagster asset grouping and tag taxonomy for readable lineage."""

from __future__ import annotations

from typing import Literal, TypeAlias

MedallionLayer: TypeAlias = Literal["bronze", "silver", "gold"]
EltStage: TypeAlias = Literal["extract_load", "transform", "publish"]
MlStage: TypeAlias = Literal[
    "source_data",
    "feature_engineering",
    "forecasting",
    "evaluation",
    "calibration",
    "selection",
    "diagnostics",
    "training_data",
    "pilot",
]
EvidenceScope: TypeAlias = Literal["demo", "thesis_grade", "research_only", "not_market_execution"]

BRONZE_MARKET_DATA = "bronze_market_data"
BRONZE_WEATHER = "bronze_weather"
BRONZE_GRID_EVENTS = "bronze_grid_events"
BRONZE_TENANT_LOAD = "bronze_tenant_load"
BRONZE_BATTERY_TELEMETRY = "bronze_battery_telemetry"

SILVER_BATTERY_TELEMETRY = "silver_battery_telemetry"
SILVER_GRID_EVENTS = "silver_grid_events"
SILVER_FORECAST_FEATURES = "silver_forecast_features"
SILVER_FORECAST_CANDIDATES = "silver_forecast_candidates"
SILVER_REAL_DATA_BENCHMARK = "silver_real_data_benchmark"
SILVER_TENANT_LOAD = "silver_tenant_load"
SILVER_SIMULATED_TRAINING = "silver_simulated_training"
SILVER_DECISION_TRANSFORMER = "silver_decision_transformer"

GOLD_MVP_BATTERY = "gold_mvp_battery"
GOLD_MVP_DISPATCH = "gold_mvp_dispatch"
GOLD_MVP_GATEKEEPER = "gold_mvp_gatekeeper"
GOLD_MVP_BENCHMARK = "gold_mvp_benchmark"
GOLD_REAL_DATA_BENCHMARK = "gold_real_data_benchmark"
GOLD_CALIBRATION = "gold_calibration"
GOLD_SELECTOR_DIAGNOSTICS = "gold_selector_diagnostics"
GOLD_DFL_TRAINING = "gold_dfl_training"
GOLD_SIMULATED_TRAINING = "gold_simulated_training"
GOLD_DECISION_TRANSFORMER = "gold_decision_transformer"
GOLD_PAPER_TRADING = "gold_paper_trading"


def asset_tags(
    *,
    medallion: MedallionLayer,
    domain: str,
    elt_stage: EltStage,
    ml_stage: MlStage,
    evidence_scope: EvidenceScope,
    backend: str | None = None,
    market_venue: str | None = None,
) -> dict[str, str]:
    """Build the standard Dagster tag set used for asset discovery and selection."""

    tags = {
        "medallion": medallion,
        "domain": domain,
        "elt_stage": elt_stage,
        "ml_stage": ml_stage,
        "evidence_scope": evidence_scope,
    }
    if backend is not None:
        tags["backend"] = backend
    if market_venue is not None:
        tags["market_venue"] = market_venue
    return tags
