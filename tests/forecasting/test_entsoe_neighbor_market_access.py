from datetime import datetime

import polars as pl

from smart_arbitrage.forecasting.entsoe_neighbor_access import (
    build_entsoe_poland_feature_governance_frame,
    build_entsoe_neighbor_market_aligned_feature_panel_frame,
    build_entsoe_neighbor_market_feature_candidate_frame,
    build_entsoe_neighbor_market_sample_audit_frame,
    build_entsoe_neighbor_market_query_spec_frame,
    validate_entsoe_poland_feature_governance_evidence,
    validate_entsoe_neighbor_market_feature_candidate_evidence,
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


def _source_backed_poland_candidates() -> pl.DataFrame:
    query_spec = build_entsoe_neighbor_market_query_spec_frame(
        _availability_frame(),
        security_token="dummy-token",
    )
    return build_entsoe_neighbor_market_feature_candidate_frame(
        query_spec,
        sample_country_codes_csv="PL",
        sample_period_start_utc="202601010000",
        sample_period_end_utc="202601010200",
        security_token="dummy-token",
        fetch_enabled=True,
        fetch_xml_by_url=lambda _url: """
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
        """,
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


def test_entsoe_poland_governance_blocks_missing_token_and_prior_fx() -> None:
    frame = build_entsoe_poland_feature_governance_frame(
        _source_backed_poland_candidates(),
        entsoe_security_token=None,
        publication_timestamp_utc="2025-12-31T11:00:00+00:00",
        ua_decision_anchor_timestamp_utc="2025-12-31T12:00:00+00:00",
        prior_eur_uah_fx_rate=0.0,
        prior_eur_uah_fx_timestamp_utc="",
        fx_rate_source="",
        timezone_dst_mapping_ready=True,
        licensing_approved=True,
        market_rules_mapped=True,
        domain_shift_validated=True,
    )

    row = frame.row(0, named=True)
    assert row["approved_for_official_training"] is False
    assert row["approved_feature_column"] == "entsoe_pl_day_ahead_price_uah_mwh"
    assert row["readiness_status"] == "blocked_by_governance"
    assert "entsoe_token" in row["training_blockers_csv"]
    assert "prior_eur_uah_fx_rate" in row["training_blockers_csv"]
    assert row["market_execution_enabled"] is False

    outcome = validate_entsoe_poland_feature_governance_evidence(frame)
    assert outcome.passed is True
    assert outcome.metadata["approved_feature_count"] == 0


def test_entsoe_poland_governance_blocks_publication_after_anchor() -> None:
    frame = build_entsoe_poland_feature_governance_frame(
        _source_backed_poland_candidates(),
        entsoe_security_token="dummy-token",
        publication_timestamp_utc="2025-12-31T13:00:00+00:00",
        ua_decision_anchor_timestamp_utc="2025-12-31T12:00:00+00:00",
        prior_eur_uah_fx_rate=45.0,
        prior_eur_uah_fx_timestamp_utc="2025-12-31T11:30:00+00:00",
        fx_rate_source="fixture_prior_fx",
        timezone_dst_mapping_ready=True,
        licensing_approved=True,
        market_rules_mapped=True,
        domain_shift_validated=True,
    )

    row = frame.row(0, named=True)
    assert row["approved_for_official_training"] is False
    assert row["publication_time_status"] == "blocked_publication_not_prior_to_anchor"
    assert "publication_time" in row["training_blockers_csv"]


def test_entsoe_poland_governance_approves_fully_governed_source_backed_feature() -> None:
    frame = build_entsoe_poland_feature_governance_frame(
        _source_backed_poland_candidates(),
        entsoe_security_token="dummy-token",
        publication_timestamp_utc="2025-12-31T11:00:00+00:00",
        ua_decision_anchor_timestamp_utc="2025-12-31T12:00:00+00:00",
        prior_eur_uah_fx_rate=45.0,
        prior_eur_uah_fx_timestamp_utc="2025-12-31T11:30:00+00:00",
        fx_rate_source="fixture_prior_fx",
        timezone_dst_mapping_ready=True,
        licensing_approved=True,
        market_rules_mapped=True,
        domain_shift_validated=True,
    )

    row = frame.row(0, named=True)
    assert row["approved_for_official_training"] is True
    assert row["training_use_allowed"] is True
    assert row["feature_use_allowed"] is True
    assert row["training_blockers_csv"] == ""
    assert row["readiness_status"] == "training_ready"
    assert row["source_backed_row_count"] == 2
    assert row["currency_status"] == "ready"
    assert row["temporal_availability_status"] == "ready"
    assert row["approved_feature_column"] == "entsoe_pl_day_ahead_price_uah_mwh"

    outcome = validate_entsoe_poland_feature_governance_evidence(frame)
    assert outcome.passed is True
    assert outcome.metadata["approved_feature_count"] == 1


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


def test_entsoe_neighbor_market_feature_candidate_parses_source_backed_prices_without_training_use() -> None:
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
          <Point><position>1</position><price.amount>102.5</price.amount></Point>
          <Point><position>2</position><price.amount>111.0</price.amount></Point>
          <Point><position>3</position><price.amount>109.5</price.amount></Point>
        </Period>
      </TimeSeries>
    </Publication_MarketDocument>
    """

    frame = build_entsoe_neighbor_market_feature_candidate_frame(
        query_spec,
        sample_country_codes_csv="PL",
        sample_period_start_utc="202601010000",
        sample_period_end_utc="202601020000",
        security_token="dummy-token",
        fetch_enabled=True,
        fetch_xml_by_url=lambda _url: xml,
    )

    assert frame.height == 3
    assert frame["feature_name"].unique().to_list() == [
        "entsoe_neighbor_day_ahead_price_context"
    ]
    assert frame["feature_column"].unique().to_list() == [
        "entsoe_pl_day_ahead_price_eur_mwh"
    ]
    assert frame["neighbor_market_price_eur_mwh"].to_list() == [102.5, 111.0, 109.5]
    assert frame["training_use_allowed"].unique().to_list() == [False]
    assert frame["feature_use_allowed"].unique().to_list() == [False]
    assert frame["source_backed"].unique().to_list() == [True]
    assert frame["publication_time_status"].unique().to_list() == [
        "blocked_missing_publication_timestamp"
    ]
    assert frame["is_prior_to_ua_decision_anchor"].unique().to_list() == [False]
    assert frame["currency_normalization_status"].unique().to_list() == [
        "blocked_missing_prior_eur_uah_fx_rate"
    ]
    assert frame["neighbor_market_price_uah_mwh"].null_count() == 3

    outcome = validate_entsoe_neighbor_market_feature_candidate_evidence(frame)
    assert outcome.passed is True
    assert outcome.metadata["source_backed_rows"] == 3
    assert outcome.metadata["publication_blocked_rows"] == 3
    assert outcome.metadata["currency_blocked_rows"] == 3


def test_entsoe_neighbor_market_aligned_feature_panel_keeps_poland_source_rows_research_only() -> None:
    query_spec = build_entsoe_neighbor_market_query_spec_frame(
        _availability_frame(),
        security_token="dummy-token",
    )
    candidates = build_entsoe_neighbor_market_feature_candidate_frame(
        query_spec,
        sample_country_codes_csv="PL",
        sample_period_start_utc="202601010000",
        sample_period_end_utc="202601010200",
        security_token="dummy-token",
        fetch_enabled=True,
        fetch_xml_by_url=lambda _url: """
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
        """,
    )
    benchmark = pl.DataFrame(
        [
            {
                "tenant_id": "client_001_kyiv_mall",
                "timestamp": datetime(2026, 1, 1, hour),
                "price_uah_mwh": 1200.0 + hour,
            }
            for hour in range(2)
        ]
        + [
            {
                "tenant_id": "client_002_lviv_office",
                "timestamp": datetime(2026, 1, 1, hour),
                "price_uah_mwh": 1300.0 + hour,
            }
            for hour in range(2)
        ]
    )

    aligned = build_entsoe_neighbor_market_aligned_feature_panel_frame(
        benchmark,
        candidates,
        country_codes=("PL",),
    )

    assert aligned.height == 4
    assert aligned.select("tenant_id").n_unique() == 2
    assert aligned.select("timestamp").n_unique() == 2
    assert aligned["source_backed"].unique().to_list() == [True]
    assert aligned["training_use_allowed"].unique().to_list() == [False]
    assert aligned["feature_use_allowed"].unique().to_list() == [False]
    assert aligned["not_market_execution"].unique().to_list() == [True]


def test_entsoe_neighbor_market_feature_candidate_rejects_training_or_feature_unlock() -> None:
    query_spec = build_entsoe_neighbor_market_query_spec_frame(
        _availability_frame(),
        security_token=None,
    )
    frame = build_entsoe_neighbor_market_feature_candidate_frame(
        query_spec,
        sample_country_codes_csv="PL",
        sample_period_start_utc="202601010000",
        sample_period_end_utc="202601020000",
        security_token=None,
        fetch_enabled=False,
    )
    broken = frame.with_columns(
        [
            pl.lit(True).alias("feature_use_allowed"),
            pl.lit(True).alias("training_use_allowed"),
        ]
    )

    outcome = validate_entsoe_neighbor_market_feature_candidate_evidence(broken)

    assert outcome.passed is False
    assert "must remain blocked from feature/training use" in outcome.description
    assert outcome.metadata["training_allowed_rows"] == 1
    assert outcome.metadata["feature_allowed_rows"] == 1


def test_entsoe_neighbor_market_feature_candidate_rejects_inconsistent_temporal_or_currency_ready_status() -> None:
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
            <end>2026-01-01T01:00Z</end>
          </timeInterval>
          <resolution>PT60M</resolution>
          <Point><position>1</position><price.amount>102.5</price.amount></Point>
        </Period>
      </TimeSeries>
    </Publication_MarketDocument>
    """
    frame = build_entsoe_neighbor_market_feature_candidate_frame(
        query_spec,
        sample_country_codes_csv="PL",
        sample_period_start_utc="202601010000",
        sample_period_end_utc="202601020000",
        security_token="dummy-token",
        fetch_enabled=True,
        fetch_xml_by_url=lambda _url: xml,
    )
    broken = frame.with_columns(
        [
            pl.lit("publication_time_verified_prior_to_ua_anchor").alias(
                "publication_time_status"
            ),
            pl.lit("prior_eur_uah_normalized").alias("currency_normalization_status"),
        ]
    )

    outcome = validate_entsoe_neighbor_market_feature_candidate_evidence(broken)

    assert outcome.passed is False
    assert "publication-ready rows must include a prior publication timestamp" in (
        outcome.description
    )
    assert "currency-ready rows must include prior FX metadata and UAH price" in (
        outcome.description
    )
    assert outcome.metadata["inconsistent_publication_ready_rows"] == 1
    assert outcome.metadata["inconsistent_currency_ready_rows"] == 1
