from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.assets.bronze.market_weather import (
    build_synthetic_market_price_history,
)
from smart_arbitrage.assets.gold.baseline_solver import (
    DEFAULT_PRICE_COLUMN,
    DEFAULT_TIMESTAMP_COLUMN,
    HourlyDamBaselineSolver,
)
from smart_arbitrage.assets.gold.forecast_strategy import (
    FORECAST_STRATEGY_GOLD_ASSETS,
    ForecastStrategyComparisonAssetConfig,
    OfficialForecastRollingOriginAssetConfig,
    RealDataRollingOriginBenchmarkAssetConfig,
    _daily_benchmark_anchors,
    forecast_strategy_comparison_frame,
    nbeatsx_official_global_panel_strict_lp_benchmark_frame,
    official_forecast_rolling_origin_benchmark_frame,
    official_forecast_strict_lp_benchmark_frame,
    real_data_rolling_origin_benchmark_frame,
)
from smart_arbitrage.assets.silver.real_data_benchmark import (
    REAL_DATA_BENCHMARK_SILVER_ASSETS,
    real_data_benchmark_silver_feature_frame,
)
from smart_arbitrage.defs import defs
from smart_arbitrage.resources.strategy_evaluation_store import (
    InMemoryStrategyEvaluationStore,
)


def _price_history() -> pl.DataFrame:
    return build_synthetic_market_price_history(
        history_hours=15 * 24,
        forecast_hours=24,
        now=datetime(2026, 5, 4, 12, 0),
    )


def _anchor_timestamp(price_history: pl.DataFrame) -> datetime:
    latest_timestamp = (
        price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(-1)
    )
    if not isinstance(latest_timestamp, datetime):
        raise TypeError("timestamp column must contain datetime values.")
    return latest_timestamp - timedelta(hours=24)


def _strict_forecast_frame(price_history: pl.DataFrame) -> pl.DataFrame:
    anchor_timestamp = _anchor_timestamp(price_history)
    historical_prices = price_history.filter(
        pl.col(DEFAULT_TIMESTAMP_COLUMN) <= anchor_timestamp
    )
    forecast = HourlyDamBaselineSolver().build_forecast(
        historical_prices,
        anchor_timestamp=anchor_timestamp,
    )
    return pl.DataFrame(
        {
            "forecast_timestamp": [point.forecast_timestamp for point in forecast],
            "source_timestamp": [point.source_timestamp for point in forecast],
            "predicted_price_uah_mwh": [
                point.predicted_price_uah_mwh for point in forecast
            ],
        }
    )


def _nbeatsx_forecast_frame(price_history: pl.DataFrame) -> pl.DataFrame:
    anchor_timestamp = _anchor_timestamp(price_history)
    actual_future = price_history.filter(
        pl.col(DEFAULT_TIMESTAMP_COLUMN) > anchor_timestamp
    ).head(24)
    return pl.DataFrame(
        {
            "forecast_timestamp": actual_future.select(DEFAULT_TIMESTAMP_COLUMN)
            .to_series()
            .to_list(),
            "model_name": ["nbeatsx_silver_v0" for _ in range(actual_future.height)],
            "predicted_price_uah_mwh": actual_future.select(DEFAULT_PRICE_COLUMN)
            .to_series()
            .to_list(),
        }
    )


def _tft_forecast_frame(price_history: pl.DataFrame) -> pl.DataFrame:
    anchor_timestamp = _anchor_timestamp(price_history)
    actual_future = price_history.filter(
        pl.col(DEFAULT_TIMESTAMP_COLUMN) > anchor_timestamp
    ).head(24)
    prices = [
        float(value)
        for value in actual_future.select(DEFAULT_PRICE_COLUMN).to_series().to_list()
    ]
    return pl.DataFrame(
        {
            "forecast_timestamp": actual_future.select(DEFAULT_TIMESTAMP_COLUMN)
            .to_series()
            .to_list(),
            "model_name": ["tft_silver_v0" for _ in range(actual_future.height)],
            "predicted_price_p50_uah_mwh": [price + 75.0 for price in prices],
        }
    )


def _official_forecast_frame(
    price_history: pl.DataFrame,
    *,
    model_name: str,
    adjustment_uah_mwh: float,
) -> pl.DataFrame:
    anchor_timestamp = _anchor_timestamp(price_history)
    actual_future = price_history.filter(
        pl.col(DEFAULT_TIMESTAMP_COLUMN) > anchor_timestamp
    ).head(24)
    prices = [
        float(value) + adjustment_uah_mwh
        for value in actual_future.select(DEFAULT_PRICE_COLUMN).to_series().to_list()
    ]
    return pl.DataFrame(
        {
            "forecast_timestamp": actual_future.select(DEFAULT_TIMESTAMP_COLUMN)
            .to_series()
            .to_list(),
            "model_name": [model_name for _ in range(actual_future.height)],
            "backend_status": ["trained" for _ in range(actual_future.height)],
            "predicted_price_uah_mwh": prices,
            "predicted_price_p50_uah_mwh": prices,
        }
    )


def test_forecast_strategy_comparison_asset_persists_gold_frame(monkeypatch) -> None:
    store = InMemoryStrategyEvaluationStore()
    price_history = _price_history()
    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.forecast_strategy.get_strategy_evaluation_store",
        lambda: store,
    )

    frame = forecast_strategy_comparison_frame(
        None,
        ForecastStrategyComparisonAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory"
        ),
        price_history,
        _strict_forecast_frame(price_history),
        _nbeatsx_forecast_frame(price_history),
        _tft_forecast_frame(price_history),
    )

    assert frame.height == 3
    assert store.evaluation_frame.height == 3
    assert set(frame.select("forecast_model_name").to_series().to_list()) == {
        "strict_similar_day",
        "nbeatsx_silver_v0",
        "tft_silver_v0",
    }
    assert frame.select("tenant_id").to_series().unique().to_list() == [
        "client_003_dnipro_factory"
    ]


def test_official_forecast_strict_lp_benchmark_asset_persists_sidecar_rows(monkeypatch) -> None:
    store = InMemoryStrategyEvaluationStore()
    price_history = _price_history()
    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.forecast_strategy.get_strategy_evaluation_store",
        lambda: store,
    )

    frame = official_forecast_strict_lp_benchmark_frame(
        None,
        ForecastStrategyComparisonAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory"
        ),
        price_history,
        _strict_forecast_frame(price_history),
        _official_forecast_frame(
            price_history,
            model_name="nbeatsx_official_v0",
            adjustment_uah_mwh=25.0,
        ),
        _official_forecast_frame(
            price_history,
            model_name="tft_official_v0",
            adjustment_uah_mwh=50.0,
        ),
    )

    assert frame.height == 3
    assert store.evaluation_frame.height == 3
    assert set(frame.select("strategy_kind").to_series().to_list()) == {
        "official_forecast_strict_lp_benchmark"
    }
    assert set(frame.select("forecast_model_name").to_series().to_list()) == {
        "strict_similar_day",
        "nbeatsx_official_v0",
        "tft_official_v0",
    }


def test_forecast_strategy_gold_asset_is_registered() -> None:
    asset_keys = {asset.key.to_user_string() for asset in FORECAST_STRATEGY_GOLD_ASSETS}
    silver_asset_keys = {asset.key.to_user_string() for asset in REAL_DATA_BENCHMARK_SILVER_ASSETS}
    registered_asset_keys = {asset.key.to_user_string() for asset in defs.assets or []}
    benchmark_deps = {
        asset_key.to_user_string()
        for asset_key in real_data_rolling_origin_benchmark_frame.dependency_keys
    }
    official_deps = {
        asset_key.to_user_string()
        for asset_key in official_forecast_rolling_origin_benchmark_frame.dependency_keys
    }

    assert {
        "forecast_strategy_comparison_frame",
        "official_forecast_strict_lp_benchmark_frame",
        "official_forecast_rolling_origin_benchmark_frame",
        "nbeatsx_official_global_panel_strict_lp_benchmark_frame",
        "real_data_rolling_origin_benchmark_frame",
    }.issubset(asset_keys)
    assert {"real_data_benchmark_silver_feature_frame"}.issubset(silver_asset_keys)
    assert asset_keys.issubset(registered_asset_keys)
    assert silver_asset_keys.issubset(registered_asset_keys)
    assert "real_data_benchmark_silver_feature_frame" in benchmark_deps
    assert "real_data_benchmark_silver_feature_frame" in official_deps
    assert "tenant_historical_weather_bronze" not in benchmark_deps


def test_official_forecast_rolling_asset_returns_persisted_resume_batch(monkeypatch) -> None:
    store = InMemoryStrategyEvaluationStore()
    generated_at = datetime(2026, 5, 11, 12)
    store.upsert_evaluation_frame(
        pl.DataFrame(
            [
                _official_rolling_row(
                    evaluation_id="batch-0",
                    tenant_id="client_003_dnipro_factory",
                    model_name="nbeatsx_official_v0",
                    anchor_timestamp=datetime(2026, 4, 1, 23),
                    generated_at=generated_at,
                )
            ]
        )
    )

    def fake_builder(
        real_data_benchmark_silver_feature_frame: pl.DataFrame,
        **kwargs: object,
    ) -> pl.DataFrame:
        assert kwargs["anchor_batch_start_index"] == 1
        assert kwargs["anchor_batch_size"] == 1
        assert kwargs["anchor_batch_order"] == "latest_first"
        assert kwargs["enabled_official_model_names"] == ("tft_official_v0",)
        assert kwargs["generated_at"] == generated_at
        return pl.DataFrame(
            [
                _official_rolling_row(
                    evaluation_id="batch-1",
                    tenant_id="client_004_kharkiv_hospital",
                    model_name="tft_official_v0",
                    anchor_timestamp=datetime(2026, 4, 2, 23),
                    generated_at=generated_at,
                )
            ]
        )

    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.forecast_strategy.get_strategy_evaluation_store",
        lambda: store,
    )
    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.forecast_strategy."
        "build_official_forecast_rolling_origin_benchmark_frame",
        fake_builder,
    )

    frame = official_forecast_rolling_origin_benchmark_frame(
        None,
        OfficialForecastRollingOriginAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory,client_004_kharkiv_hospital",
            max_eval_anchors_per_tenant=2,
            anchor_batch_start_index=1,
            anchor_batch_size=1,
            anchor_batch_order="latest_first",
            enabled_official_model_names_csv="tft_official_v0",
            resume_generated_at_iso="2026-05-11T12:00:00",
            merge_persisted_batches=True,
        ),
        pl.DataFrame(),
    )

    assert frame["evaluation_id"].to_list() == ["batch-0", "batch-1"]


def test_global_panel_nbeatsx_strict_lp_asset_persists_rows(monkeypatch) -> None:
    store = InMemoryStrategyEvaluationStore()
    generated_at = datetime(2026, 5, 11, 18)

    def fake_builder(
        real_data_benchmark_silver_feature_frame: pl.DataFrame,
        nbeatsx_official_global_panel_price_forecast: pl.DataFrame,
        **kwargs: object,
    ) -> pl.DataFrame:
        assert kwargs["tenant_ids"] == ("client_003_dnipro_factory",)
        assert real_data_benchmark_silver_feature_frame.height == 1
        assert nbeatsx_official_global_panel_price_forecast.height == 1
        return pl.DataFrame(
            [
                _official_rolling_row(
                    evaluation_id="global-panel",
                    tenant_id="client_003_dnipro_factory",
                    model_name="nbeatsx_official_global_panel_v1",
                    anchor_timestamp=datetime(2026, 4, 2, 23),
                    generated_at=generated_at,
                )
            ]
        )

    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.forecast_strategy.get_strategy_evaluation_store",
        lambda: store,
    )
    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.forecast_strategy."
        "build_official_global_panel_nbeatsx_strict_lp_benchmark_frame",
        fake_builder,
    )

    frame = nbeatsx_official_global_panel_strict_lp_benchmark_frame(
        None,
        RealDataRollingOriginBenchmarkAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            max_anchors=1,
        ),
        pl.DataFrame({"tenant_id": ["client_003_dnipro_factory"]}),
        pl.DataFrame({"unique_id": ["client_003_dnipro_factory:DAM"]}),
    )

    assert frame["evaluation_id"].to_list() == ["global-panel"]
    assert store.evaluation_frame.height == 1


def test_real_data_rolling_origin_benchmark_asset_persists_rows(monkeypatch) -> None:
    store = InMemoryStrategyEvaluationStore()
    price_history = build_synthetic_market_price_history(
        history_hours=15 * 24 + 48,
        forecast_hours=0,
        now=datetime(2026, 5, 4, 12, 0),
    ).with_columns(
        [
            pl.lit("observed").alias("source_kind"),
            pl.lit("OREE_DATA_VIEW").alias("source"),
        ]
    )
    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.forecast_strategy.get_strategy_evaluation_store",
        lambda: store,
    )

    silver_frame = real_data_benchmark_silver_feature_frame(None, price_history, pl.DataFrame())
    frame = real_data_rolling_origin_benchmark_frame(
        None,
        RealDataRollingOriginBenchmarkAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            max_anchors=1,
        ),
        silver_frame,
    )

    assert frame.height == 3
    assert store.latest_real_data_benchmark_frame(tenant_id="client_003_dnipro_factory").height == 3
    assert set(frame.select("strategy_kind").to_series().to_list()) == {
        "real_data_rolling_origin_benchmark"
    }


def test_daily_benchmark_anchors_skip_incomplete_realized_horizons() -> None:
    start_timestamp = datetime(2026, 1, 1)
    end_timestamp = datetime(2026, 5, 4, 23)
    missing_timestamp = datetime(2026, 3, 29, 23)
    timestamps: list[datetime] = []
    current_timestamp = start_timestamp
    while current_timestamp <= end_timestamp:
        if current_timestamp != missing_timestamp:
            timestamps.append(current_timestamp)
        current_timestamp += timedelta(hours=1)
    price_history = pl.DataFrame(
        {
            DEFAULT_TIMESTAMP_COLUMN: timestamps,
            DEFAULT_PRICE_COLUMN: [1000.0 for _ in timestamps],
        }
    )

    anchors = _daily_benchmark_anchors(price_history, max_anchors=90)

    assert len(anchors) == 90
    assert datetime(2026, 3, 28, 23) not in anchors
    assert datetime(2026, 3, 29, 23) not in anchors
    assert datetime(2026, 4, 4, 23) not in anchors
    assert datetime(2026, 4, 5, 23) in anchors
    assert anchors[-1] == datetime(2026, 5, 3, 23)
    available_timestamps = set(timestamps)
    for anchor in anchors:
        required_window = [
            anchor - timedelta(hours=167) + timedelta(hours=step_index)
            for step_index in range(192)
        ]
        assert all(timestamp in available_timestamps for timestamp in required_window)


def _official_rolling_row(
    *,
    evaluation_id: str,
    tenant_id: str,
    model_name: str,
    anchor_timestamp: datetime,
    generated_at: datetime,
) -> dict[str, object]:
    return {
        "evaluation_id": evaluation_id,
        "tenant_id": tenant_id,
        "forecast_model_name": model_name,
        "strategy_kind": "official_forecast_rolling_origin_benchmark",
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": 24,
        "starting_soc_fraction": 0.5,
        "starting_soc_source": "tenant_default",
        "decision_value_uah": 100.0,
        "forecast_objective_value_uah": 100.0,
        "oracle_value_uah": 125.0,
        "regret_uah": 25.0,
        "regret_ratio": 0.2,
        "total_degradation_penalty_uah": 1.0,
        "total_throughput_mwh": 1.0,
        "committed_action": "hold",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "evaluation_payload": {
            "claim_scope": "official_forecast_rolling_origin_benchmark_not_full_dfl",
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "not_full_dfl": True,
            "not_market_execution": True,
        },
    }
