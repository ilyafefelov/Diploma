import polars as pl

from smart_arbitrage.forecasting.entsoe_neighbor_access import (
    build_entsoe_neighbor_market_sample_audit_frame,
    build_entsoe_neighbor_market_query_spec_frame,
    validate_entsoe_neighbor_market_sample_audit_evidence,
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
    assert frame["api_base_url"].unique().to_list() == ["https://web-api.tp.entsoe.eu/api"]
    assert frame["query_parameter_keys_csv"].unique().to_list() == [
        "securityToken,documentType,processType,in_Domain,out_Domain,periodStart,periodEnd"
    ]
    assert set(frame["country_code"].to_list()) == {"PL", "SK", "HU", "RO", "MD"}

    pl_row = frame.filter(pl.col("country_code") == "PL")
    md_row = frame.filter(pl.col("country_code") == "MD")

    assert pl_row.select("bidding_zone_eic").to_series().item() == "10YPL-AREA-----S"
    assert pl_row.select("eic_mapping_status").to_series().item() == "mapped"
    assert pl_row.select("fetch_allowed").to_series().item() is True
    request_template = pl_row.select("request_url_template").to_series().item()
    assert "securityToken=<redacted>" in request_template
    assert "documentType=A44" in request_template
    assert "processType=A01" in request_template
    assert "in_Domain=10YPL-AREA-----S" in request_template
    assert "out_Domain=10YPL-AREA-----S" in request_template
    assert "periodStart={period_start_utc_yyyymmddHHMM}" in request_template
    assert "periodEnd={period_end_utc_yyyymmddHHMM}" in request_template

    assert md_row.select("bidding_zone_eic").to_series().item() == ""
    assert md_row.select("eic_mapping_status").to_series().item() == "review_required"
    assert md_row.select("fetch_allowed").to_series().item() is False
    assert md_row.select("request_url_template").to_series().item() == ""


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


def test_entsoe_neighbor_market_sample_audit_blocks_fetch_without_token() -> None:
    query_spec = build_entsoe_neighbor_market_query_spec_frame(
        _availability_frame(),
        security_token=None,
    )

    frame = build_entsoe_neighbor_market_sample_audit_frame(
        query_spec,
        sample_country_codes_csv="PL",
        sample_period_start_utc="202601010000",
        sample_period_end_utc="202601020000",
        security_token=None,
        fetch_enabled=True,
    )

    assert frame.height == 1
    row = frame.to_dicts()[0]
    assert row["country_code"] == "PL"
    assert row["fetch_status"] == "blocked_missing_entsoe_security_token"
    assert row["source_backed_row_count"] == 0
    assert row["parsed_price_row_count"] == 0
    assert row["training_use_allowed"] is False
    assert row["feature_use_allowed"] is False

    outcome = validate_entsoe_neighbor_market_sample_audit_evidence(frame)
    assert outcome.passed is True
    assert outcome.metadata["source_backed_rows"] == 0


def test_entsoe_neighbor_market_sample_audit_parses_source_backed_sample_without_training_use() -> None:
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
            <end>2026-01-01T03:00Z</end>
          </timeInterval>
          <resolution>PT60M</resolution>
          <Point>
            <position>1</position>
            <price.amount>102.5</price.amount>
          </Point>
          <Point>
            <position>2</position>
            <price.amount>111.0</price.amount>
          </Point>
          <Point>
            <position>3</position>
            <price.amount>109.5</price.amount>
          </Point>
        </Period>
      </TimeSeries>
    </Publication_MarketDocument>
    """

    frame = build_entsoe_neighbor_market_sample_audit_frame(
        query_spec,
        sample_country_codes_csv="PL",
        sample_period_start_utc="202601010000",
        sample_period_end_utc="202601020000",
        security_token="dummy-token",
        fetch_enabled=True,
        fetch_xml_by_url=lambda _url: xml,
    )

    assert frame.height == 1
    row = frame.to_dicts()[0]
    assert row["fetch_status"] == "source_backed_sample_fetched_not_training"
    assert row["source_backed_row_count"] == 3
    assert row["parsed_price_row_count"] == 3
    assert row["first_delivery_timestamp_utc"] == "2026-01-01T00:00:00+00:00"
    assert row["last_delivery_timestamp_utc"] == "2026-01-01T02:00:00+00:00"
    assert row["training_use_allowed"] is False
    assert row["feature_use_allowed"] is False

    outcome = validate_entsoe_neighbor_market_sample_audit_evidence(frame)
    assert outcome.passed is True
    assert outcome.metadata["source_backed_rows"] == 3


def test_entsoe_neighbor_market_sample_audit_rejects_feature_use_before_governance() -> None:
    query_spec = build_entsoe_neighbor_market_query_spec_frame(
        _availability_frame(),
        security_token=None,
    )
    frame = build_entsoe_neighbor_market_sample_audit_frame(
        query_spec,
        sample_country_codes_csv="PL",
        sample_period_start_utc="202601010000",
        sample_period_end_utc="202601020000",
        security_token=None,
        fetch_enabled=False,
    )
    broken = frame.with_columns(pl.lit(True).alias("feature_use_allowed"))

    outcome = validate_entsoe_neighbor_market_sample_audit_evidence(broken)

    assert outcome.passed is False
    assert "ENTSO-E samples must not become feature rows before governance passes" in (
        outcome.description
    )
