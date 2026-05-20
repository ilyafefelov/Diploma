from datetime import UTC, datetime, timedelta

import polars as pl

from smart_arbitrage.assets.bronze.market_weather import build_synthetic_market_price_history
from smart_arbitrage.forecasting.neural_features import (
    DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    build_neural_forecast_feature_frame,
)
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
)
from smart_arbitrage.forecasting.sota_training import build_sota_forecast_training_frame
from smart_arbitrage.forecasting.sota_training import (
    build_official_global_panel_training_frame,
    build_official_global_panel_poland_lag24_experimental_training_frame,
    POLAND_LAG24_EXPERIMENTAL_FEATURE_COLUMNS,
)


def test_sota_training_frame_uses_official_library_schema_without_future_target_leakage() -> None:
    price_history = build_synthetic_market_price_history(
        history_hours=15 * 24,
        forecast_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
        now=datetime(2026, 5, 4, 12, 0),
    )
    feature_frame = build_neural_forecast_feature_frame(price_history, future_weather_mode="forecast_only")

    training_frame = build_sota_forecast_training_frame(
        feature_frame,
        tenant_id="client_003_dnipro_factory",
    )

    assert {"unique_id", "ds", "y", "split", "tenant_id", "sota_schema_version"}.issubset(training_frame.columns)
    assert training_frame.select("unique_id").to_series().unique().to_list() == ["client_003_dnipro_factory:DAM"]
    assert training_frame.filter(pl.col("split") == "forecast").select("y").null_count().item() == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
    assert training_frame.filter(pl.col("split") == "train").select("y").drop_nulls().height > 168
    assert "known_future_feature_columns_csv" in training_frame.columns
    assert "historical_observed_feature_columns_csv" in training_frame.columns


def test_sota_training_frame_rejects_missing_required_silver_columns() -> None:
    bad_frame = pl.DataFrame({"timestamp": [datetime(2026, 5, 1)], "split": ["train"]})

    try:
        build_sota_forecast_training_frame(bad_frame, tenant_id="tenant")
    except ValueError as error:
        assert "missing required columns" in str(error)
    else:
        raise AssertionError("build_sota_forecast_training_frame should reject incomplete frames.")


def test_official_global_panel_training_frame_combines_tenants_without_future_target_leakage() -> None:
    silver_frame = _tenant_silver_frame(
        tenant_ids=("client_001_kyiv_mall", "client_003_dnipro_factory"),
        history_hours=15 * 24,
        forecast_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    )

    panel = build_official_global_panel_training_frame(
        silver_frame,
        tenant_ids=("client_001_kyiv_mall", "client_003_dnipro_factory"),
        horizon_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    )

    assert panel.select("unique_id").to_series().unique().sort().to_list() == [
        "client_001_kyiv_mall:DAM",
        "client_003_dnipro_factory:DAM",
    ]
    assert panel.filter(pl.col("split") == "forecast").select("y").null_count().item() == (
        2 * DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
    )
    assert panel.filter(pl.col("split") == "train").select("y").drop_nulls().height > 2 * 168
    assert panel.select("sota_schema_version").to_series().unique().to_list() == [
        "official_global_panel_sota_v1"
    ]
    assert panel.select("target_scaler_fit_scope").to_series().unique().to_list() == [
        "train_rows_only_per_unique_id"
    ]
    assert panel.select("temporal_scaler_type").to_series().unique().to_list() == ["robust"]
    assert "weather_temperature" in panel.select("known_future_feature_columns_csv").to_series().item(0)
    assert "lag_24_price_uah_mwh" in panel.select("historical_observed_feature_columns_csv").to_series().item(0)


def test_official_global_panel_training_frame_features_ignore_mutated_final_actuals() -> None:
    silver_frame = _tenant_silver_frame(
        tenant_ids=("client_003_dnipro_factory",),
        history_hours=15 * 24,
        forecast_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    )
    control = build_official_global_panel_training_frame(
        silver_frame,
        tenant_ids=("client_003_dnipro_factory",),
        horizon_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    )
    cutoff = control.filter(pl.col("split") == "train").select("ds").to_series().max()
    mutated = silver_frame.with_columns(
        pl.when(pl.col("timestamp") > cutoff)
        .then(pl.col("price_uah_mwh") + 90000.0)
        .otherwise(pl.col("price_uah_mwh"))
        .alias("price_uah_mwh")
    )

    mutated_panel = build_official_global_panel_training_frame(
        mutated,
        tenant_ids=("client_003_dnipro_factory",),
        horizon_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    )

    feature_columns = [
        "unique_id",
        "ds",
        "split",
        "known_future_feature_columns_csv",
        "historical_observed_feature_columns_csv",
        "target_scaler_fit_scope",
        "temporal_scaler_type",
        "weather_temperature",
        "lag_24_price_uah_mwh",
        "rolling_24h_mean_uah_mwh",
    ]
    assert (
        control.filter(pl.col("split") == "forecast")
        .select(feature_columns)
        .equals(mutated_panel.filter(pl.col("split") == "forecast").select(feature_columns))
    )
    assert mutated_panel.filter(pl.col("split") == "forecast").select("y").null_count().item() == (
        DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
    )


def test_official_global_panel_training_frame_can_pin_rolling_anchor() -> None:
    silver_frame = _tenant_silver_frame(
        tenant_ids=("client_003_dnipro_factory",),
        history_hours=20 * 24,
        forecast_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    )
    anchor_timestamp = datetime(2026, 4, 28, 23)

    panel = build_official_global_panel_training_frame(
        silver_frame,
        tenant_ids=("client_003_dnipro_factory",),
        horizon_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
        anchor_timestamp=anchor_timestamp,
    )

    train_max = panel.filter(pl.col("split") == "train").select("ds").to_series().max()
    forecast_min = panel.filter(pl.col("split") == "forecast").select("ds").to_series().min()
    forecast_max = panel.filter(pl.col("split") == "forecast").select("ds").to_series().max()
    assert train_max == anchor_timestamp
    assert forecast_min == anchor_timestamp + timedelta(hours=1)
    assert forecast_max == anchor_timestamp + timedelta(hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS)


def test_official_global_panel_training_frame_records_blocked_market_coupling_governance() -> None:
    silver_frame = _tenant_silver_frame(
        tenant_ids=("client_003_dnipro_factory",),
        history_hours=20 * 24,
        forecast_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    )
    market_coupling = build_market_coupling_temporal_availability_frame(
        build_forecast_afe_feature_catalog_frame()
    )

    panel = build_official_global_panel_training_frame(
        silver_frame,
        tenant_ids=("client_003_dnipro_factory",),
        horizon_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
        market_coupling_availability_frame=market_coupling,
    )

    assert panel.select("external_feature_training_status").to_series().unique().to_list() == [
        "blocked_by_governance"
    ]
    blocked_features = panel.select("blocked_external_feature_columns_csv").to_series().item(0)
    assert "entsoe_neighbor_day_ahead_price_context" in blocked_features
    assert "pricefm_european_price_context" in blocked_features
    assert panel.select("allowed_external_feature_columns_csv").to_series().item(0) == ""
    assert "entsoe_neighbor_day_ahead_price_context" not in panel.select(
        "known_future_feature_columns_csv"
    ).to_series().item(0)


def test_official_global_panel_training_frame_rejects_unready_training_allowed_external_feature() -> None:
    silver_frame = _tenant_silver_frame(
        tenant_ids=("client_003_dnipro_factory",),
        history_hours=20 * 24,
        forecast_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    )
    market_coupling = build_market_coupling_temporal_availability_frame(
        build_forecast_afe_feature_catalog_frame()
    ).with_columns(
        pl.when(pl.col("source_name") == "ENTSO_E")
        .then(pl.lit(True))
        .otherwise(pl.col("training_use_allowed"))
        .alias("training_use_allowed")
    )

    try:
        build_official_global_panel_training_frame(
            silver_frame,
            tenant_ids=("client_003_dnipro_factory",),
            horizon_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
            market_coupling_availability_frame=market_coupling,
        )
    except ValueError as error:
        assert "external market-coupling features cannot be training_use_allowed" in str(error)
    else:
        raise AssertionError("unready external market-coupling rows should be blocked from training.")


def test_official_global_panel_training_frame_blocks_ready_but_source_unbacked_feature() -> None:
    feature_name = "entsoe_neighbor_day_ahead_price_context"
    silver_frame = _tenant_silver_frame(
        tenant_ids=("client_003_dnipro_factory",),
        history_hours=20 * 24,
        forecast_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    ).with_columns((pl.col("price_uah_mwh") * 0.8).alias(feature_name))
    market_coupling = build_market_coupling_temporal_availability_frame(
        build_forecast_afe_feature_catalog_frame()
    ).with_columns(
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

    panel = build_official_global_panel_training_frame(
        silver_frame,
        tenant_ids=("client_003_dnipro_factory",),
        horizon_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
        market_coupling_availability_frame=market_coupling,
    )

    assert feature_name not in panel.columns
    assert panel.select("external_feature_training_status").to_series().unique().to_list() == [
        "blocked_by_governance"
    ]
    assert panel.select("allowed_external_feature_columns_csv").to_series().item(0) == ""
    assert feature_name not in panel.select("known_future_feature_columns_csv").to_series().item(0)
    assert feature_name in panel.select("blocked_external_feature_columns_csv").to_series().item(0)


def test_official_global_panel_training_frame_routes_only_approved_feature_route_rows() -> None:
    feature_name = "entsoe_neighbor_day_ahead_price_context"
    silver_frame = _tenant_silver_frame(
        tenant_ids=("client_003_dnipro_factory",),
        history_hours=20 * 24,
        forecast_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    ).with_columns((pl.col("price_uah_mwh") * 0.8).alias(feature_name))
    market_coupling = build_market_coupling_temporal_availability_frame(
        build_forecast_afe_feature_catalog_frame()
    ).with_columns(
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
    route = build_market_coupling_feature_route_frame(
        market_coupling,
        entsoe_neighbor_market_sample_audit_frame=_source_backed_entsoe_sample(),
    )

    panel = build_official_global_panel_training_frame(
        silver_frame,
        tenant_ids=("client_003_dnipro_factory",),
        horizon_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
        market_coupling_feature_route_frame=route,
    )

    assert panel.select("external_feature_governance_scope").to_series().unique().to_list() == [
        "market_coupling_feature_route_frame"
    ]
    assert feature_name in panel.columns
    assert panel.select("allowed_external_feature_columns_csv").to_series().item(0) == feature_name


def test_poland_lag24_experimental_training_frame_routes_richer_features_without_official_approval() -> None:
    silver_frame = _tenant_silver_frame(
        tenant_ids=("client_003_dnipro_factory",),
        history_hours=20 * 24,
        forecast_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    )
    route = _experimental_poland_route_frame()
    lagged = _poland_lagged_feature_frame(silver_frame)

    panel = build_official_global_panel_poland_lag24_experimental_training_frame(
        silver_frame,
        tenant_ids=("client_003_dnipro_factory",),
        horizon_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
        market_coupling_feature_route_frame=route,
        entsoe_poland_lagged_feature_candidate_frame=lagged,
    )

    known_future_csv = panel.select("known_future_feature_columns_csv").to_series().item(0)
    for column_name in POLAND_LAG24_EXPERIMENTAL_FEATURE_COLUMNS:
        assert column_name in panel.columns
        assert column_name in known_future_csv
        assert panel.select(column_name).null_count().item() == 0
    assert panel.select("external_feature_training_status").to_series().unique().to_list() == [
        "experimental_ablation_only"
    ]
    assert panel.select("allowed_external_feature_columns_csv").to_series().unique().to_list() == [
        ""
    ]
    assert panel.select("experimental_external_feature_columns_csv").to_series().unique().to_list() == [
        ",".join(POLAND_LAG24_EXPERIMENTAL_FEATURE_COLUMNS)
    ]


def test_poland_lag24_experimental_training_frame_rejects_unapproved_route() -> None:
    silver_frame = _tenant_silver_frame(
        tenant_ids=("client_003_dnipro_factory",),
        history_hours=20 * 24,
        forecast_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    )
    route = _experimental_poland_route_frame().with_columns(
        [
            pl.lit(False).alias("approved_for_experimental_ablation"),
            pl.lit("blocked_for_experimental_ablation").alias(
                "experimental_feature_route_status"
            ),
        ]
    )

    try:
        build_official_global_panel_poland_lag24_experimental_training_frame(
            silver_frame,
            tenant_ids=("client_003_dnipro_factory",),
            horizon_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
            market_coupling_feature_route_frame=route,
            entsoe_poland_lagged_feature_candidate_frame=_poland_lagged_feature_frame(
                silver_frame
            ),
        )
    except ValueError as error:
        assert "requires an ablation-ready Poland lag24 route" in str(error)
    else:
        raise AssertionError("experimental Poland training must be blocked without route approval.")


def _source_backed_entsoe_sample() -> pl.DataFrame:
    query_spec = build_entsoe_neighbor_market_query_spec_frame(
        build_market_coupling_temporal_availability_frame(
            build_forecast_afe_feature_catalog_frame()
        ),
        security_token="dummy-token",
    )
    return build_entsoe_neighbor_market_sample_audit_frame(
        query_spec,
        sample_country_codes_csv="PL",
        sample_period_start_utc="202601010000",
        sample_period_end_utc="202601020000",
        security_token="dummy-token",
        fetch_enabled=True,
        fetch_xml_by_url=lambda _url: """
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
        """,
    )


def _experimental_poland_route_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "feature_name": ["entsoe_neighbor_lagged_day_ahead_price_context"],
            "source_name": ["ENTSO_E"],
            "source_kind": ["neighbor_market_day_ahead_price"],
            "approved_feature_column": ["entsoe_pl_lag24_day_ahead_price_uah_mwh"],
            "feature_route_status": ["source_backed_but_governance_blocked"],
            "experimental_feature_route_status": ["approved_for_experimental_ablation"],
            "source_backed_row_count": [999],
            "training_use_allowed": [False],
            "feature_use_allowed": [False],
            "approved_for_official_training": [False],
            "approved_for_experimental_ablation": [True],
            "training_blockers_csv": ["domain_shift"],
            "readiness_status": ["blocked_until_domain_shift_validation"],
            "licensing_status": ["ready"],
            "timezone_status": ["ready"],
            "currency_status": ["ready"],
            "market_rules_status": ["ready"],
            "temporal_availability_status": ["ready"],
            "domain_shift_status": ["blocked_pending_validation"],
            "publication_time_policy": ["lagged_delivery_must_precede_ua_anchor"],
            "decision_cutoff_policy": ["lagged_poland_delivery_before_ua_anchor"],
            "external_feature_role": ["experimental_ablation_context"],
            "claim_scope": ["market_coupling_feature_route_research_gate"],
            "not_full_dfl": [True],
            "not_market_execution": [True],
        }
    )


def _poland_lagged_feature_frame(silver_frame: pl.DataFrame) -> pl.DataFrame:
    timestamps = (
        silver_frame
        .select("timestamp")
        .unique()
        .sort("timestamp")
        .to_series()
        .to_list()
    )
    rows: list[dict[str, object]] = []
    for index, timestamp in enumerate(timestamps):
        assert isinstance(timestamp, datetime)
        price = 3500.0 + float(index % 24) * 20.0
        rows.append(
            {
                "feature_name": "entsoe_neighbor_lagged_day_ahead_price_context",
                "feature_column": "entsoe_pl_lag24_day_ahead_price_uah_mwh",
                "delivery_timestamp_utc": timestamp.replace(tzinfo=UTC).isoformat(),
                "source_backed": True,
                "coverage_status": "full_lagged_feature_coverage",
                "entsoe_pl_lag24_day_ahead_price_uah_mwh": price,
                "entsoe_pl_lag24_delta_1h_uah_mwh": None if index == 0 else 20.0,
                "entsoe_pl_lag24_delta_24h_uah_mwh": None if index < 24 else 480.0,
                "entsoe_pl_lag24_daily_spread_uah_mwh": 460.0,
                "entsoe_pl_lag24_daily_price_rank": float(index % 24) / 23.0,
                "entsoe_pl_lag24_daily_peak_hour_utc": 23,
                "entsoe_pl_lag24_daily_trough_hour_utc": 0,
                "entsoe_pl_lag24_ua_spread_uah_mwh": price - 3000.0,
                "entsoe_pl_lag24_ua_spread_delta_24h_uah_mwh": (
                    None if index < 24 else 100.0
                ),
                "entsoe_pl_lag24_ua_spread_ratio": (price - 3000.0) / 3000.0,
                "entsoe_pl_lag24_rolling_24h_mean_uah_mwh": 3730.0,
                "entsoe_pl_lag24_rolling_24h_min_uah_mwh": 3500.0,
                "entsoe_pl_lag24_rolling_24h_max_uah_mwh": 3960.0,
                "entsoe_pl_lag24_rolling_24h_spread_uah_mwh": 460.0,
                "entsoe_pl_lag24_rolling_168h_mean_uah_mwh": 3730.0,
                "entsoe_pl_lag24_rolling_168h_spread_uah_mwh": 460.0,
                "entsoe_pl_lag24_price_vs_rolling_24h_mean_uah_mwh": price - 3730.0,
                "entsoe_pl_lag24_peak_distance_hours": float(abs(23 - index % 24)),
                "entsoe_pl_lag24_trough_distance_hours": float(index % 24),
                "entsoe_pl_lag24_is_daily_peak_hour": 1.0 if index % 24 == 23 else 0.0,
                "entsoe_pl_lag24_is_daily_trough_hour": 1.0 if index % 24 == 0 else 0.0,
                "entsoe_pl_lag24_ua_spread_rolling_24h_mean_uah_mwh": price - 3000.0,
                "entsoe_pl_lag24_ua_spread_vs_rolling_24h_mean_uah_mwh": 0.0,
                "entsoe_pl_lag24_ua_spread_abs_ratio": abs((price - 3000.0) / 3000.0),
            }
        )
    return pl.DataFrame(rows)


def _tenant_silver_frame(
    *,
    tenant_ids: tuple[str, ...],
    history_hours: int,
    forecast_hours: int,
) -> pl.DataFrame:
    base = build_synthetic_market_price_history(
        history_hours=history_hours,
        forecast_hours=forecast_hours,
        now=datetime(2026, 5, 4, 12, 0),
    )
    rows: list[pl.DataFrame] = []
    for tenant_index, tenant_id in enumerate(tenant_ids):
        rows.append(
            base.with_columns(
                [
                    pl.lit(tenant_id).alias("tenant_id"),
                    (pl.col("price_uah_mwh") + float(tenant_index) * 100.0).alias("price_uah_mwh"),
                    pl.lit("observed").alias("source_kind"),
                    pl.lit("forecast").alias("weather_source_kind"),
                    (pl.lit(10.0) + float(tenant_index)).alias("weather_temperature"),
                ]
            )
        )
    return pl.concat(rows, how="diagonal_relaxed")
