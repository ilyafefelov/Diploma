from datetime import datetime

import polars as pl

from smart_arbitrage.assets.bronze.market_weather import build_synthetic_market_price_history
from smart_arbitrage.forecasting.neural_features import (
    DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    build_neural_forecast_feature_frame,
)
from smart_arbitrage.forecasting.sota_training import build_sota_forecast_training_frame
from smart_arbitrage.forecasting.sota_training import (
    build_official_global_panel_training_frame,
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
