import polars as pl

from smart_arbitrage.forecasting.entsoe_neighbor_access import (
    build_entsoe_neighbor_market_query_spec_frame,
    validate_entsoe_neighbor_market_access_evidence,
)
from smart_arbitrage.forecasting.afe import build_forecast_afe_feature_catalog_frame
from smart_arbitrage.forecasting.market_coupling_availability import (
    build_market_coupling_temporal_availability_frame,
)


def _availability_frame() -> pl.DataFrame:
    return build_market_coupling_temporal_availability_frame(
        build_forecast_afe_feature_catalog_frame()
    )


def test_entsoe_neighbor_market_query_spec_blocks_fetch_without_token() -> None:
    frame = build_entsoe_neighbor_market_query_spec_frame(
        _availability_frame(),
        security_token=None,
    )

    assert frame.height == 5
    assert frame["security_token_available"].unique().to_list() == [False]
    assert frame["fetch_allowed"].unique().to_list() == [False]
    assert frame["training_use_allowed"].unique().to_list() == [False]
    assert frame["access_status"].unique().to_list() == ["blocked_missing_entsoe_security_token"]


def test_entsoe_neighbor_market_query_spec_records_day_ahead_price_request_shape() -> None:
    frame = build_entsoe_neighbor_market_query_spec_frame(
        _availability_frame(),
        security_token="dummy-token",
    )

    assert frame["document_type"].unique().to_list() == ["A44"]
    assert frame["process_type"].unique().to_list() == ["A01"]
    assert frame["market_venue"].unique().to_list() == ["neighbor_DAM"]
    assert set(frame["country_code"].to_list()) == {"PL", "SK", "HU", "RO", "MD"}

    pl_row = frame.filter(pl.col("country_code") == "PL")
    md_row = frame.filter(pl.col("country_code") == "MD")

    assert pl_row.select("bidding_zone_eic").to_series().item() == "10YPL-AREA-----S"
    assert pl_row.select("eic_mapping_status").to_series().item() == "mapped"
    assert pl_row.select("fetch_allowed").to_series().item() is True

    assert md_row.select("bidding_zone_eic").to_series().item() == ""
    assert md_row.select("eic_mapping_status").to_series().item() == "review_required"
    assert md_row.select("fetch_allowed").to_series().item() is False


def test_entsoe_neighbor_market_access_evidence_rejects_training_rows() -> None:
    frame = build_entsoe_neighbor_market_query_spec_frame(
        _availability_frame(),
        security_token="dummy-token",
    )
    broken = frame.with_columns(
        pl.when(pl.col("country_code") == "PL")
        .then(pl.lit(True))
        .otherwise(pl.col("training_use_allowed"))
        .alias("training_use_allowed")
    )

    outcome = validate_entsoe_neighbor_market_access_evidence(broken)

    assert outcome.passed is False
    assert "ENTSO-E neighbor rows must not be training rows" in outcome.description
    assert outcome.metadata["training_allowed_rows"] == 1


def test_entsoe_neighbor_market_access_evidence_rejects_bad_document_type() -> None:
    frame = build_entsoe_neighbor_market_query_spec_frame(
        _availability_frame(),
        security_token="dummy-token",
    )
    broken = frame.with_columns(
        pl.when(pl.col("country_code") == "PL")
        .then(pl.lit("A65"))
        .otherwise(pl.col("document_type"))
        .alias("document_type")
    )

    outcome = validate_entsoe_neighbor_market_access_evidence(broken)

    assert outcome.passed is False
    assert "ENTSO-E day-ahead price rows must use A44/A01" in outcome.description
    assert outcome.metadata["bad_request_shape_rows"] == 1
