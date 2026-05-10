from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import polars as pl

from smart_arbitrage.strategy.official_forecast_rolling import (
    OFFICIAL_FORECAST_ROLLING_ORIGIN_STRATEGY_KIND,
    build_official_forecast_rolling_origin_benchmark_frame,
    validate_official_forecast_rolling_origin_evidence,
)


TENANT_ID = "client_003_dnipro_factory"


def test_official_forecast_rolling_origin_scores_prior_only_builders() -> None:
    captures: list[dict[str, Any]] = []

    def fake_nbeatsx(training_frame: pl.DataFrame, **kwargs: object) -> pl.DataFrame:
        captures.append(_capture("nbeatsx", training_frame, kwargs))
        return _official_forecast(training_frame, model_name="nbeatsx_official_v0", adjustment=25.0)

    def fake_tft(training_frame: pl.DataFrame, **kwargs: object) -> pl.DataFrame:
        captures.append(_capture("tft", training_frame, kwargs))
        return _official_forecast(training_frame, model_name="tft_official_v0", adjustment=50.0)

    benchmark = build_official_forecast_rolling_origin_benchmark_frame(
        _silver_frame(),
        tenant_ids=(TENANT_ID,),
        max_eval_anchors_per_tenant=2,
        nbeatsx_builder=fake_nbeatsx,
        tft_builder=fake_tft,
        nbeatsx_max_steps=100,
        tft_max_epochs=15,
    )
    outcome = validate_official_forecast_rolling_origin_evidence(
        benchmark,
        min_tenant_count=1,
        min_anchor_count_per_tenant=2,
    )

    assert benchmark.height == 6
    assert set(benchmark["forecast_model_name"].to_list()) == {
        "strict_similar_day",
        "nbeatsx_official_v0",
        "tft_official_v0",
    }
    assert benchmark.select("strategy_kind").to_series().unique().to_list() == [
        OFFICIAL_FORECAST_ROLLING_ORIGIN_STRATEGY_KIND
    ]
    assert outcome.passed is True
    assert len(captures) == 4
    for capture in captures:
        assert capture["max_train_ds"] <= capture["anchor_timestamp"]
        assert capture["forecast_y_values"] == [None] * 24
    assert captures[0]["kwargs"]["max_steps"] == 100
    assert captures[1]["kwargs"]["max_epochs"] == 15


def test_official_forecast_rolling_origin_blocks_non_observed_rows() -> None:
    bad_frame = _silver_frame().with_columns(pl.lit("synthetic").alias("source_kind"))

    benchmark = build_official_forecast_rolling_origin_benchmark_frame(
        bad_frame,
        tenant_ids=(TENANT_ID,),
        max_eval_anchors_per_tenant=1,
        nbeatsx_builder=lambda training_frame, **kwargs: _official_forecast(
            training_frame,
            model_name="nbeatsx_official_v0",
            adjustment=25.0,
        ),
        tft_builder=lambda training_frame, **kwargs: _official_forecast(
            training_frame,
            model_name="tft_official_v0",
            adjustment=50.0,
        ),
        require_observed_source_rows=False,
    )
    outcome = validate_official_forecast_rolling_origin_evidence(
        benchmark,
        min_tenant_count=1,
        min_anchor_count_per_tenant=1,
    )

    assert outcome.passed is False
    assert "thesis_grade" in outcome.description


def _silver_frame() -> pl.DataFrame:
    start = datetime(2026, 1, 1)
    rows: list[dict[str, object]] = []
    for index in range(240):
        timestamp = start + timedelta(hours=index)
        rows.append(
            {
                "tenant_id": TENANT_ID,
                "timestamp": timestamp,
                "price_uah_mwh": 1000.0 + 250.0 * (index % 24 in {8, 9, 18, 19}),
                "source_kind": "observed",
                "weather_temperature": 10.0 + float(index % 24) / 2.0,
                "weather_wind_speed": 4.0,
                "weather_cloudcover": 50.0,
                "weather_precipitation": 0.0,
                "weather_effective_solar": 0.0,
                "weather_source_kind": "observed",
            }
        )
    return pl.DataFrame(rows)


def _capture(
    model_name: str,
    training_frame: pl.DataFrame,
    kwargs: dict[str, object],
) -> dict[str, Any]:
    train = training_frame.filter(pl.col("split") == "train")
    forecast = training_frame.filter(pl.col("split") == "forecast")
    first_forecast_ds = forecast.sort("ds")["ds"].to_list()[0]
    if not isinstance(first_forecast_ds, datetime):
        raise TypeError("ds must contain datetimes.")
    return {
        "model_name": model_name,
        "anchor_timestamp": first_forecast_ds - timedelta(hours=1),
        "max_train_ds": train.sort("ds")["ds"].to_list()[-1],
        "forecast_y_values": forecast.sort("ds")["y"].to_list(),
        "kwargs": kwargs,
    }


def _official_forecast(
    training_frame: pl.DataFrame,
    *,
    model_name: str,
    adjustment: float,
) -> pl.DataFrame:
    forecast_rows = training_frame.filter(pl.col("split") == "forecast").sort("ds")
    timestamps = forecast_rows["ds"].to_list()
    prices = [
        1100.0 + adjustment + float(index % 4) * 10.0
        for index, _timestamp in enumerate(timestamps)
    ]
    return pl.DataFrame(
        {
            "forecast_timestamp": timestamps,
            "model_name": [model_name] * len(timestamps),
            "backend_status": ["trained"] * len(timestamps),
            "predicted_price_uah_mwh": prices,
            "predicted_price_p50_uah_mwh": prices,
        }
    )
