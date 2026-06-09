import polars as pl

from smart_arbitrage.forecasting.afe import build_forecast_afe_feature_catalog_frame
from smart_arbitrage.forecasting.market_coupling_availability import (
    build_market_coupling_temporal_availability_frame,
    validate_market_coupling_temporal_availability_evidence,
)


def test_market_coupling_availability_keeps_external_sources_blocked() -> None:
    catalog = build_forecast_afe_feature_catalog_frame()

    frame = build_market_coupling_temporal_availability_frame(catalog)

    assert frame.height == 6
    assert frame["training_use_allowed"].unique().to_list() == [False]
    assert frame["not_market_execution"].unique().to_list() == [True]
    assert frame["not_full_dfl"].unique().to_list() == [True]
    assert set(frame["source_name"].to_list()).issuperset(
        {"ENTSO_E", "PRICEFM_HF", "OPSD", "EMBER", "NORD_POOL", "THIEF_HF"}
    )
    assert set(frame["training_blockers_csv"].unique().to_list()) == {
        "licensing,timezone,currency,market_rules,temporal_availability,domain_shift"
    }


def test_market_coupling_availability_records_source_specific_next_actions() -> None:
    catalog = build_forecast_afe_feature_catalog_frame()

    frame = build_market_coupling_temporal_availability_frame(catalog)
    entsoe = frame.filter(pl.col("source_name") == "ENTSO_E")
    pricefm = frame.filter(pl.col("source_name") == "PRICEFM_HF")

    assert entsoe.height == 1
    assert entsoe.select("candidate_neighbor_zones").to_series().item() == "PL,SK,HU,RO,MD"
    assert (
        entsoe.select("publication_time_policy").to_series().item()
        == "requires_document_publication_time_before_ua_anchor"
    )
    assert entsoe.select("readiness_status").to_series().item() == "blocked_until_mapping"
    assert (
        entsoe.select("next_action").to_series().item()
        == "map_entsoe_document_types_bidding_zones_publication_times_and_terms"
    )

    assert pricefm.height == 1
    assert pricefm.select("dataset_viewer_status").to_series().item() == "viewer_available"
    assert pricefm.select("source_observation_count").to_series().item() == 140257
    assert pricefm.select("source_column_count").to_series().item() == 191
    assert pricefm.select("readiness_status").to_series().item() == "blocked_external_validation_only"


def test_market_coupling_availability_evidence_blocks_training_ready_external_rows() -> None:
    frame = build_market_coupling_temporal_availability_frame(
        build_forecast_afe_feature_catalog_frame()
    )
    broken = frame.with_columns(
        pl.when(pl.col("source_name") == "ENTSO_E")
        .then(pl.lit(True))
        .otherwise(pl.col("training_use_allowed"))
        .alias("training_use_allowed")
    )

    outcome = validate_market_coupling_temporal_availability_evidence(broken)

    assert outcome.passed is False
    assert "external market-coupling rows must remain blocked from training" in outcome.description
    assert outcome.metadata["training_allowed_rows"] == 1


def test_market_coupling_availability_evidence_requires_temporal_blockers() -> None:
    frame = build_market_coupling_temporal_availability_frame(
        build_forecast_afe_feature_catalog_frame()
    )
    broken = frame.with_columns(
        pl.when(pl.col("source_name") == "PRICEFM_HF")
        .then(pl.lit("licensing,timezone,currency"))
        .otherwise(pl.col("training_blockers_csv"))
        .alias("training_blockers_csv")
    )

    outcome = validate_market_coupling_temporal_availability_evidence(broken)

    assert outcome.passed is False
    assert "must list every external training blocker" in outcome.description
    assert outcome.metadata["missing_blocker_rows"] == 1
