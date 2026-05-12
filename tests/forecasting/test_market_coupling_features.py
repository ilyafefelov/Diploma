import polars as pl

from smart_arbitrage.forecasting.afe import build_forecast_afe_feature_catalog_frame
from smart_arbitrage.forecasting.entsoe_neighbor_access import (
    build_entsoe_neighbor_market_query_spec_frame,
    build_entsoe_neighbor_market_sample_audit_frame,
)
from smart_arbitrage.forecasting.market_coupling_availability import (
    build_market_coupling_temporal_availability_frame,
)
from smart_arbitrage.forecasting.market_coupling_features import (
    build_market_coupling_feature_route_frame,
    market_coupling_feature_route_metadata,
    validate_market_coupling_feature_route_evidence,
)


def _availability_frame() -> pl.DataFrame:
    return build_market_coupling_temporal_availability_frame(
        build_forecast_afe_feature_catalog_frame()
    )


def test_market_coupling_feature_route_keeps_sources_blocked_by_default() -> None:
    route = build_market_coupling_feature_route_frame(_availability_frame())

    assert route.height == 6
    assert route["approved_for_official_training"].unique().to_list() == [False]
    assert route["feature_use_allowed"].unique().to_list() == [False]
    assert route["not_market_execution"].unique().to_list() == [True]

    metadata = market_coupling_feature_route_metadata(route)
    assert metadata["external_feature_training_status"] == "blocked_by_governance"
    assert "entsoe_neighbor_day_ahead_price_context" in metadata[
        "blocked_external_feature_columns_csv"
    ]
    assert metadata["allowed_external_feature_columns_csv"] == ""

    outcome = validate_market_coupling_feature_route_evidence(route)
    assert outcome.passed is True
    assert outcome.metadata["approved_feature_count"] == 0


def test_market_coupling_feature_route_records_source_backed_entsoe_sample_without_approval() -> None:
    query_spec = build_entsoe_neighbor_market_query_spec_frame(
        _availability_frame(),
        security_token="dummy-token",
    )
    xml = """
    <Publication_MarketDocument>
      <TimeSeries>
        <Period>
          <timeInterval>
            <start>2026-01-01T00:00Z</start>
            <end>2026-01-01T02:00Z</end>
          </timeInterval>
          <resolution>PT60M</resolution>
          <Point><position>1</position><price.amount>102.5</price.amount></Point>
          <Point><position>2</position><price.amount>111.0</price.amount></Point>
        </Period>
      </TimeSeries>
    </Publication_MarketDocument>
    """
    sample = build_entsoe_neighbor_market_sample_audit_frame(
        query_spec,
        sample_country_codes_csv="PL",
        sample_period_start_utc="202601010000",
        sample_period_end_utc="202601020000",
        security_token="dummy-token",
        fetch_enabled=True,
        fetch_xml_by_url=lambda _url: xml,
    )

    route = build_market_coupling_feature_route_frame(
        _availability_frame(),
        entsoe_neighbor_market_sample_audit_frame=sample,
    )

    entsoe_row = route.filter(pl.col("source_name") == "ENTSO_E").row(0, named=True)
    assert entsoe_row["source_backed_row_count"] == 2
    assert entsoe_row["feature_route_status"] == "source_backed_but_governance_blocked"
    assert entsoe_row["training_use_allowed"] is False
    assert entsoe_row["feature_use_allowed"] is False


def test_market_coupling_feature_route_approves_only_fully_governed_prior_features() -> None:
    feature_name = "entsoe_neighbor_day_ahead_price_context"
    availability = _availability_frame().with_columns(
        [
            pl.when(pl.col("feature_name") == feature_name)
            .then(pl.lit(True))
            .otherwise(pl.col("training_use_allowed"))
            .alias("training_use_allowed"),
            pl.when(pl.col("feature_name") == feature_name)
            .then(pl.lit("training_ready"))
            .otherwise(pl.col("readiness_status"))
            .alias("readiness_status"),
            pl.when(pl.col("feature_name") == feature_name)
            .then(pl.lit(""))
            .otherwise(pl.col("training_blockers_csv"))
            .alias("training_blockers_csv"),
            *[
                pl.when(pl.col("feature_name") == feature_name)
                .then(pl.lit("ready"))
                .otherwise(pl.col(column_name))
                .alias(column_name)
                for column_name in (
                    "licensing_status",
                    "timezone_status",
                    "currency_status",
                    "market_rules_status",
                    "temporal_availability_status",
                    "domain_shift_status",
                )
            ],
        ]
    )

    route = build_market_coupling_feature_route_frame(availability)

    entsoe_row = route.filter(pl.col("feature_name") == feature_name).row(0, named=True)
    assert entsoe_row["approved_for_official_training"] is True
    assert entsoe_row["feature_use_allowed"] is True
    assert entsoe_row["feature_route_status"] == "approved_for_training"

    metadata = market_coupling_feature_route_metadata(route)
    assert metadata["external_feature_training_status"] == "training_ready"
    assert metadata["allowed_external_feature_columns_csv"] == feature_name
    assert feature_name not in metadata["blocked_external_feature_columns_csv"]


def test_market_coupling_feature_route_validation_rejects_unready_training_rows() -> None:
    route = build_market_coupling_feature_route_frame(_availability_frame()).with_columns(
        pl.when(pl.col("source_name") == "ENTSO_E")
        .then(pl.lit(True))
        .otherwise(pl.col("training_use_allowed"))
        .alias("training_use_allowed")
    )

    outcome = validate_market_coupling_feature_route_evidence(route)

    assert outcome.passed is False
    assert "must not approve unready external features" in outcome.description
    assert outcome.metadata["unready_approved_rows"] == 1
