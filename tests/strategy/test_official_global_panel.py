from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.strategy.official_global_panel import (
    OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME,
    OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATION_STRATEGY_KIND,
    OFFICIAL_GLOBAL_PANEL_NBEATSX_ROLLING_STRATEGY_KIND,
    OFFICIAL_GLOBAL_PANEL_NBEATSX_STRATEGY_KIND,
    OFFICIAL_GLOBAL_PANEL_TFT_MODEL_NAME,
    OFFICIAL_GLOBAL_PANEL_TFT_P10_CALIBRATED_MODEL_NAME,
    OFFICIAL_GLOBAL_PANEL_TFT_P10_MODEL_NAME,
    OFFICIAL_GLOBAL_PANEL_TFT_P90_CALIBRATED_MODEL_NAME,
    OFFICIAL_GLOBAL_PANEL_TFT_P90_MODEL_NAME,
    OFFICIAL_GLOBAL_PANEL_TFT_QUANTILE_CALIBRATION_STRATEGY_KIND,
    OFFICIAL_GLOBAL_PANEL_TFT_QUANTILE_CALIBRATED_MODEL_NAME,
    OFFICIAL_GLOBAL_PANEL_TFT_ROLLING_STRATEGY_KIND,
    POLAND_LAG24_EXPERIMENTAL_CALIBRATION_STRATEGY_KIND,
    POLAND_LAG24_EXPERIMENTAL_NBEATSX_CALIBRATED_MODEL_NAME,
    POLAND_LAG24_EXPERIMENTAL_NBEATSX_MODEL_NAME,
    POLAND_LAG24_EXPERIMENTAL_ROLLING_STRATEGY_KIND,
    POLAND_LAG24_EXPERIMENTAL_TFT_CALIBRATED_MODEL_NAME,
    POLAND_LAG24_EXPERIMENTAL_TFT_MODEL_NAME,
    build_official_global_panel_nbeatsx_horizon_calibration_frame,
    build_official_global_panel_nbeatsx_horizon_calibrated_strict_lp_benchmark_frame,
    build_official_global_panel_nbeatsx_rolling_strict_lp_benchmark_frame,
    build_official_global_panel_nbeatsx_strict_lp_benchmark_frame,
    build_official_global_panel_poland_lag24_experimental_horizon_calibrated_strict_lp_benchmark_frame,
    build_official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame,
    build_official_global_panel_tft_horizon_quantile_calibrated_strict_lp_benchmark_frame,
    build_official_global_panel_tft_horizon_quantile_calibration_frame,
    build_official_global_panel_tft_rolling_strict_lp_benchmark_frame,
    build_official_global_panel_tft_strict_lp_benchmark_frame,
)


TENANT_ID = "client_003_dnipro_factory"
SECOND_TENANT_ID = "client_002_lviv_office"
GENERATED_AT = datetime(2026, 5, 11, 18, tzinfo=UTC)


def test_global_panel_nbeatsx_strict_lp_benchmark_scores_against_strict_control() -> None:
    silver_frame = _silver_frame()
    forecast_frame = _global_panel_forecast_frame(anchor_timestamp=datetime(2026, 1, 10, 23))

    result = build_official_global_panel_nbeatsx_strict_lp_benchmark_frame(
        silver_frame,
        forecast_frame,
        tenant_ids=(TENANT_ID,),
        generated_at=GENERATED_AT,
    )

    assert set(result["forecast_model_name"].to_list()) == {
        "strict_similar_day",
        "nbeatsx_official_global_panel_v1",
    }
    assert result.select("strategy_kind").to_series().unique().to_list() == [
        OFFICIAL_GLOBAL_PANEL_NBEATSX_STRATEGY_KIND
    ]
    assert result.select("generated_at").to_series().unique().to_list() == [
        GENERATED_AT
    ]
    payload = result.filter(pl.col("forecast_model_name") == "nbeatsx_official_global_panel_v1").row(0, named=True)[
        "evaluation_payload"
    ]
    assert payload["claim_scope"] == "official_global_panel_nbeatsx_strict_lp_not_full_dfl"
    assert payload["data_quality_tier"] == "thesis_grade"
    assert payload["not_full_dfl"] is True
    assert payload["not_market_execution"] is True


def test_global_panel_nbeatsx_strict_lp_benchmark_rejects_missing_tenant_forecast() -> None:
    try:
        build_official_global_panel_nbeatsx_strict_lp_benchmark_frame(
            _silver_frame(),
            _global_panel_forecast_frame(anchor_timestamp=datetime(2026, 1, 10, 23)),
            tenant_ids=("missing_tenant",),
            generated_at=GENERATED_AT,
        )
    except ValueError as error:
        assert "Missing global-panel NBEATSx forecast rows" in str(error)
    else:
        raise AssertionError("missing tenant forecast rows should fail clearly")


def test_global_panel_nbeatsx_horizon_calibration_uses_prior_anchors_only() -> None:
    first_anchor = datetime(2026, 1, 10, 23)
    source_rows = [
        _evaluation_row(
            first_anchor + timedelta(days=index),
            model_name="nbeatsx_official_global_panel_v1",
            forecast_prices=[1000.0, 1000.0],
            actual_prices=[1200.0 if index < 5 else 5000.0, 900.0 if index < 5 else 5000.0],
        )
        for index in range(6)
    ]

    calibration = build_official_global_panel_nbeatsx_horizon_calibration_frame(
        pl.DataFrame(source_rows),
        min_prior_anchors=2,
        rolling_calibration_window_anchors=3,
    )

    fifth_anchor = first_anchor + timedelta(days=4)
    fifth_row = calibration.filter(pl.col("anchor_timestamp") == fifth_anchor).row(0, named=True)
    assert fifth_row["corrected_forecast_model_name"] == OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME
    assert fifth_row["horizon_biases_uah_mwh"] == pytest.approx([200.0, -100.0])
    assert fifth_row["prior_anchor_count"] == 4
    assert fifth_row["calibration_status"] == "calibrated"

    first_row = calibration.filter(pl.col("anchor_timestamp") == first_anchor).row(0, named=True)
    assert first_row["calibration_status"] == "insufficient_prior_history"
    assert first_row["horizon_biases_uah_mwh"] == [0.0, 0.0]


def test_global_panel_nbeatsx_horizon_calibrated_gate_routes_corrected_forecast_through_lp() -> None:
    anchor = datetime(2026, 1, 10, 23)
    evaluation_frame = pl.DataFrame(
        [
            _evaluation_row(anchor, model_name="strict_similar_day", forecast_prices=[1000.0, 1400.0]),
            _evaluation_row(
                anchor,
                model_name="nbeatsx_official_global_panel_v1",
                forecast_prices=[900.0, 1200.0],
            ),
        ]
    )
    calibration_frame = pl.DataFrame(
        [
            {
                "tenant_id": TENANT_ID,
                "anchor_timestamp": anchor,
                "source_forecast_model_name": "nbeatsx_official_global_panel_v1",
                "corrected_forecast_model_name": OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME,
                "horizon_biases_uah_mwh": [100.0, 300.0],
                "mean_horizon_bias_uah_mwh": 200.0,
                "max_abs_horizon_bias_uah_mwh": 300.0,
                "prior_anchor_count": 14,
                "calibration_window_anchor_count": 14,
                "calibration_status": "calibrated",
                "data_quality_tier": "thesis_grade",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        ]
    )

    result = build_official_global_panel_nbeatsx_horizon_calibrated_strict_lp_benchmark_frame(
        evaluation_frame,
        calibration_frame,
    )

    assert set(result["forecast_model_name"].to_list()) == {
        "strict_similar_day",
        "nbeatsx_official_global_panel_v1",
        OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME,
    }
    assert set(result["strategy_kind"].to_list()) == {
        OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATION_STRATEGY_KIND
    }
    corrected_payload = result.filter(
        pl.col("forecast_model_name") == OFFICIAL_GLOBAL_PANEL_NBEATSX_CALIBRATED_MODEL_NAME
    ).row(0, named=True)["evaluation_payload"]
    assert corrected_payload["source_forecast_model_name"] == "nbeatsx_official_global_panel_v1"
    assert corrected_payload["horizon_biases_uah_mwh"] == [100.0, 300.0]
    assert corrected_payload["not_full_dfl"] is True
    assert corrected_payload["not_market_execution"] is True


def test_global_panel_nbeatsx_rolling_benchmark_trains_once_per_anchor_across_tenants() -> None:
    silver_frame = _silver_frame(tenant_ids=(TENANT_ID, SECOND_TENANT_ID), day_count=14)
    builder_calls: list[pl.DataFrame] = []

    def fake_nbeatsx_builder(training_frame: pl.DataFrame, **kwargs: object) -> pl.DataFrame:
        builder_calls.append(training_frame)
        assert kwargs["max_steps"] == 1
        forecast_rows = (
            training_frame
            .filter(pl.col("is_forecast"))
            .select(["unique_id", "ds"])
            .sort(["unique_id", "ds"])
        )
        return pl.DataFrame(
            {
                "model_name": ["nbeatsx_official_global_panel_v1"] * forecast_rows.height,
                "model_family": ["NBEATSx"] * forecast_rows.height,
                "backend_name": ["neuralforecast"] * forecast_rows.height,
                "backend_status": ["trained"] * forecast_rows.height,
                "unique_id": forecast_rows["unique_id"].to_list(),
                "forecast_timestamp": forecast_rows["ds"].to_list(),
                "predicted_price_uah_mwh": [1100.0] * forecast_rows.height,
                "predicted_price_p10_uah_mwh": [None] * forecast_rows.height,
                "predicted_price_p50_uah_mwh": [1100.0] * forecast_rows.height,
                "predicted_price_p90_uah_mwh": [None] * forecast_rows.height,
                "prediction_interval_kind": ["point"] * forecast_rows.height,
                "training_rows": [training_frame.filter(pl.col("is_train")).height] * forecast_rows.height,
                "horizon_rows": [24] * forecast_rows.height,
                "adapter_scope": ["official_backend_forecast_candidate_not_live_strategy"] * forecast_rows.height,
            }
        )

    result = build_official_global_panel_nbeatsx_rolling_strict_lp_benchmark_frame(
        silver_frame,
        tenant_ids=(TENANT_ID, SECOND_TENANT_ID),
        max_eval_windows=2,
        horizon_hours=24,
        nbeatsx_max_steps=1,
        generated_at=GENERATED_AT,
        nbeatsx_builder=fake_nbeatsx_builder,
    )

    assert len(builder_calls) == 2
    assert set(result["strategy_kind"].to_list()) == {
        OFFICIAL_GLOBAL_PANEL_NBEATSX_ROLLING_STRATEGY_KIND
    }
    assert result.select("anchor_timestamp").n_unique() == 2
    assert set(result["tenant_id"].to_list()) == {TENANT_ID, SECOND_TENANT_ID}
    assert set(result["forecast_model_name"].to_list()) == {
        "strict_similar_day",
        "nbeatsx_official_global_panel_v1",
    }


def test_global_panel_nbeatsx_rolling_benchmark_slices_resumable_anchor_batches() -> None:
    silver_frame = _silver_frame(tenant_ids=(TENANT_ID, SECOND_TENANT_ID), day_count=14)
    trained_anchors: list[datetime] = []

    def fake_nbeatsx_builder(training_frame: pl.DataFrame, **kwargs: object) -> pl.DataFrame:
        forecast_rows = training_frame.filter(pl.col("is_forecast")).select(["unique_id", "ds"])
        first_forecast_timestamp = forecast_rows.select(pl.col("ds").min()).item()
        assert isinstance(first_forecast_timestamp, datetime)
        trained_anchors.append(first_forecast_timestamp - timedelta(hours=1))
        return pl.DataFrame(
            {
                "model_name": ["nbeatsx_official_global_panel_v1"] * forecast_rows.height,
                "model_family": ["NBEATSx"] * forecast_rows.height,
                "backend_name": ["neuralforecast"] * forecast_rows.height,
                "backend_status": ["trained"] * forecast_rows.height,
                "unique_id": forecast_rows["unique_id"].to_list(),
                "forecast_timestamp": forecast_rows["ds"].to_list(),
                "predicted_price_uah_mwh": [1100.0] * forecast_rows.height,
                "predicted_price_p10_uah_mwh": [None] * forecast_rows.height,
                "predicted_price_p50_uah_mwh": [1100.0] * forecast_rows.height,
                "predicted_price_p90_uah_mwh": [None] * forecast_rows.height,
                "prediction_interval_kind": ["point"] * forecast_rows.height,
                "training_rows": [training_frame.filter(pl.col("is_train")).height] * forecast_rows.height,
                "horizon_rows": [24] * forecast_rows.height,
                "adapter_scope": ["official_backend_forecast_candidate_not_live_strategy"] * forecast_rows.height,
            }
        )

    result = build_official_global_panel_nbeatsx_rolling_strict_lp_benchmark_frame(
        silver_frame,
        tenant_ids=(TENANT_ID, SECOND_TENANT_ID),
        max_eval_windows=4,
        horizon_hours=24,
        nbeatsx_max_steps=1,
        generated_at=GENERATED_AT,
        anchor_batch_order="chronological",
        anchor_batch_start_index=1,
        anchor_batch_size=2,
        nbeatsx_builder=fake_nbeatsx_builder,
    )

    assert trained_anchors == [datetime(2026, 1, 11, 23), datetime(2026, 1, 12, 23)]
    assert result.select("anchor_timestamp").n_unique() == 2


def test_global_panel_tft_strict_lp_benchmark_scores_quantile_sources() -> None:
    silver_frame = _silver_frame()
    forecast_frame = _global_panel_tft_forecast_frame(anchor_timestamp=datetime(2026, 1, 10, 23))

    result = build_official_global_panel_tft_strict_lp_benchmark_frame(
        silver_frame,
        forecast_frame,
        tenant_ids=(TENANT_ID,),
        generated_at=GENERATED_AT,
    )

    assert set(result["forecast_model_name"].to_list()) == {
        "strict_similar_day",
        OFFICIAL_GLOBAL_PANEL_TFT_P10_MODEL_NAME,
        OFFICIAL_GLOBAL_PANEL_TFT_MODEL_NAME,
        OFFICIAL_GLOBAL_PANEL_TFT_P90_MODEL_NAME,
    }
    assert result.select("strategy_kind").to_series().unique().to_list() == [
        OFFICIAL_GLOBAL_PANEL_TFT_ROLLING_STRATEGY_KIND
    ]
    p50_payload = result.filter(
        pl.col("forecast_model_name") == OFFICIAL_GLOBAL_PANEL_TFT_MODEL_NAME
    ).row(0, named=True)["evaluation_payload"]
    assert p50_payload["claim_scope"] == "official_global_panel_tft_quantile_strict_lp_not_full_dfl"
    assert p50_payload["source_quantile"] == "p50"
    assert p50_payload["not_full_dfl"] is True
    assert p50_payload["not_market_execution"] is True


def test_global_panel_tft_rolling_benchmark_trains_once_per_anchor_across_tenants() -> None:
    silver_frame = _silver_frame(tenant_ids=(TENANT_ID, SECOND_TENANT_ID), day_count=14)
    builder_calls: list[pl.DataFrame] = []

    def fake_tft_builder(training_frame: pl.DataFrame, **kwargs: object) -> pl.DataFrame:
        builder_calls.append(training_frame)
        assert kwargs["max_epochs"] == 2
        assert kwargs["max_steps"] == 7
        forecast_rows = (
            training_frame
            .filter(pl.col("is_forecast"))
            .select(["unique_id", "ds"])
            .sort(["unique_id", "ds"])
        )
        return _global_panel_tft_forecast_from_rows(forecast_rows)

    result = build_official_global_panel_tft_rolling_strict_lp_benchmark_frame(
        silver_frame,
        tenant_ids=(TENANT_ID, SECOND_TENANT_ID),
        max_eval_windows=2,
        horizon_hours=24,
        tft_max_epochs=2,
        tft_max_steps=7,
        generated_at=GENERATED_AT,
        tft_builder=fake_tft_builder,
    )

    assert len(builder_calls) == 2
    assert set(result["strategy_kind"].to_list()) == {
        OFFICIAL_GLOBAL_PANEL_TFT_ROLLING_STRATEGY_KIND
    }
    assert result.select("anchor_timestamp").n_unique() == 2
    assert set(result["tenant_id"].to_list()) == {TENANT_ID, SECOND_TENANT_ID}
    assert set(result["forecast_model_name"].to_list()) == {
        "strict_similar_day",
        OFFICIAL_GLOBAL_PANEL_TFT_P10_MODEL_NAME,
        OFFICIAL_GLOBAL_PANEL_TFT_MODEL_NAME,
        OFFICIAL_GLOBAL_PANEL_TFT_P90_MODEL_NAME,
    }


def test_poland_lag24_experimental_rolling_benchmark_scores_both_sources() -> None:
    silver_frame = _silver_frame(tenant_ids=(TENANT_ID, SECOND_TENANT_ID), day_count=14)
    training_calls: list[pl.DataFrame] = []

    def fake_training_builder(
        silver_frame: pl.DataFrame,
        **kwargs: object,
    ) -> pl.DataFrame:
        training_calls.append(silver_frame)
        assert kwargs["entsoe_poland_lagged_feature_candidate_frame"].height == 1
        assert kwargs["market_coupling_feature_route_frame"].height == 1
        anchor_timestamp = kwargs["anchor_timestamp"]
        assert isinstance(anchor_timestamp, datetime)
        return _experimental_training_frame(
            tenant_ids=kwargs["tenant_ids"],
            anchor_timestamp=anchor_timestamp,
        )

    def fake_nbeatsx_builder(training_frame: pl.DataFrame, **kwargs: object) -> pl.DataFrame:
        forecast_rows = training_frame.filter(pl.col("is_forecast")).select(["unique_id", "ds"])
        return _global_panel_point_forecast_from_rows(
            forecast_rows,
            model_name=POLAND_LAG24_EXPERIMENTAL_NBEATSX_MODEL_NAME,
            model_family="NBEATSx",
        )

    def fake_tft_builder(training_frame: pl.DataFrame, **kwargs: object) -> pl.DataFrame:
        forecast_rows = training_frame.filter(pl.col("is_forecast")).select(["unique_id", "ds"])
        return _global_panel_tft_forecast_from_rows(
            forecast_rows,
            model_name=POLAND_LAG24_EXPERIMENTAL_TFT_MODEL_NAME,
        )

    result = build_official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame(
        silver_frame,
        tenant_ids=(TENANT_ID, SECOND_TENANT_ID),
        entsoe_poland_lagged_feature_candidate_frame=pl.DataFrame(
            {"delivery_timestamp_utc": ["2026-01-01T00:00:00+00:00"]}
        ),
        market_coupling_feature_route_frame=pl.DataFrame(
            {"route_status": ["approved_route_pending_materialization"]}
        ),
        max_eval_windows=2,
        horizon_hours=24,
        nbeatsx_max_steps=1,
        tft_max_epochs=2,
        tft_max_steps=7,
        generated_at=GENERATED_AT,
        training_frame_builder=fake_training_builder,
        nbeatsx_builder=fake_nbeatsx_builder,
        tft_builder=fake_tft_builder,
    )

    assert len(training_calls) == 2
    assert result.select("anchor_timestamp").n_unique() == 2
    assert set(result["strategy_kind"].to_list()) == {
        POLAND_LAG24_EXPERIMENTAL_ROLLING_STRATEGY_KIND
    }
    assert set(result["forecast_model_name"].to_list()) == {
        "strict_similar_day",
        POLAND_LAG24_EXPERIMENTAL_NBEATSX_MODEL_NAME,
        POLAND_LAG24_EXPERIMENTAL_TFT_MODEL_NAME,
    }
    experimental_payload = result.filter(
        pl.col("forecast_model_name") == POLAND_LAG24_EXPERIMENTAL_TFT_MODEL_NAME
    ).row(0, named=True)["evaluation_payload"]
    assert experimental_payload["source_forecast_model_name"] == (
        POLAND_LAG24_EXPERIMENTAL_TFT_MODEL_NAME
    )
    assert experimental_payload["claim_scope"] == (
        "official_global_panel_poland_lag24_experimental_rolling_strict_lp_not_full_dfl"
    )
    assert experimental_payload["not_market_execution"] is True


def test_poland_lag24_nbeatsx_calibration_uses_experimental_model_names() -> None:
    first_anchor = datetime(2026, 1, 10, 23)
    source_rows = [
        _evaluation_row(
            first_anchor + timedelta(days=index),
            model_name=POLAND_LAG24_EXPERIMENTAL_NBEATSX_MODEL_NAME,
            forecast_prices=[1000.0, 1000.0],
            actual_prices=[
                1200.0 if index < 5 else 5000.0,
                900.0 if index < 5 else 5000.0,
            ],
        )
        for index in range(6)
    ]

    calibration = build_official_global_panel_nbeatsx_horizon_calibration_frame(
        pl.DataFrame(source_rows),
        min_prior_anchors=2,
        rolling_calibration_window_anchors=3,
        source_model_name=POLAND_LAG24_EXPERIMENTAL_NBEATSX_MODEL_NAME,
        corrected_model_name=POLAND_LAG24_EXPERIMENTAL_NBEATSX_CALIBRATED_MODEL_NAME,
    )

    latest_row = calibration.filter(
        pl.col("anchor_timestamp") == first_anchor + timedelta(days=4)
    ).row(0, named=True)
    assert latest_row["source_forecast_model_name"] == (
        POLAND_LAG24_EXPERIMENTAL_NBEATSX_MODEL_NAME
    )
    assert latest_row["corrected_forecast_model_name"] == (
        POLAND_LAG24_EXPERIMENTAL_NBEATSX_CALIBRATED_MODEL_NAME
    )
    assert latest_row["horizon_biases_uah_mwh"] == pytest.approx([200.0, -100.0])
    assert latest_row["calibration_status"] == "calibrated"


def test_poland_lag24_calibrated_gate_routes_nbeatsx_and_tft_through_lp() -> None:
    anchor = datetime(2026, 1, 10, 23)
    evaluation_frame = pl.DataFrame(
        [
            _evaluation_row(
                anchor,
                model_name="strict_similar_day",
                forecast_prices=[1000.0, 1400.0],
            ),
            _evaluation_row(
                anchor,
                model_name=POLAND_LAG24_EXPERIMENTAL_NBEATSX_MODEL_NAME,
                forecast_prices=[900.0, 1200.0],
            ),
            _evaluation_row(
                anchor,
                model_name=POLAND_LAG24_EXPERIMENTAL_TFT_MODEL_NAME,
                forecast_prices=[950.0, 1250.0],
            ),
        ]
    )
    nbeatsx_calibration = pl.DataFrame(
        [
            {
                "tenant_id": TENANT_ID,
                "anchor_timestamp": anchor,
                "source_forecast_model_name": POLAND_LAG24_EXPERIMENTAL_NBEATSX_MODEL_NAME,
                "corrected_forecast_model_name": (
                    POLAND_LAG24_EXPERIMENTAL_NBEATSX_CALIBRATED_MODEL_NAME
                ),
                "horizon_biases_uah_mwh": [100.0, 300.0],
                "mean_horizon_bias_uah_mwh": 200.0,
                "max_abs_horizon_bias_uah_mwh": 300.0,
                "prior_anchor_count": 14,
                "calibration_window_anchor_count": 14,
                "calibration_status": "calibrated",
                "data_quality_tier": "thesis_grade",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        ]
    )
    tft_calibration = pl.DataFrame(
        [
            _tft_calibration_row(
                anchor=anchor,
                source_model_name=POLAND_LAG24_EXPERIMENTAL_TFT_MODEL_NAME,
                corrected_model_name=POLAND_LAG24_EXPERIMENTAL_TFT_CALIBRATED_MODEL_NAME,
                source_quantile="p50",
                horizon_biases=[150.0, 250.0],
            )
        ]
    )

    result = (
        build_official_global_panel_poland_lag24_experimental_horizon_calibrated_strict_lp_benchmark_frame(
            evaluation_frame,
            nbeatsx_calibration,
            tft_calibration,
        )
    )

    assert set(result["forecast_model_name"].to_list()) == {
        "strict_similar_day",
        POLAND_LAG24_EXPERIMENTAL_NBEATSX_MODEL_NAME,
        POLAND_LAG24_EXPERIMENTAL_TFT_MODEL_NAME,
        POLAND_LAG24_EXPERIMENTAL_NBEATSX_CALIBRATED_MODEL_NAME,
        POLAND_LAG24_EXPERIMENTAL_TFT_CALIBRATED_MODEL_NAME,
    }
    assert set(result["strategy_kind"].to_list()) == {
        POLAND_LAG24_EXPERIMENTAL_CALIBRATION_STRATEGY_KIND
    }
    calibrated_tft_payload = result.filter(
        pl.col("forecast_model_name") == POLAND_LAG24_EXPERIMENTAL_TFT_CALIBRATED_MODEL_NAME
    ).row(0, named=True)["evaluation_payload"]
    assert calibrated_tft_payload["source_forecast_model_name"] == (
        POLAND_LAG24_EXPERIMENTAL_TFT_MODEL_NAME
    )
    assert calibrated_tft_payload["source_quantile"] == "p50"
    assert calibrated_tft_payload["horizon_biases_uah_mwh"] == [150.0, 250.0]
    assert calibrated_tft_payload["not_full_dfl"] is True
    assert calibrated_tft_payload["not_market_execution"] is True


def test_global_panel_tft_quantile_calibration_uses_prior_anchors_only() -> None:
    first_anchor = datetime(2026, 1, 10, 23)
    source_rows = []
    for index in range(5):
        anchor = first_anchor + timedelta(days=index)
        source_rows.extend(
            [
                _evaluation_row(
                    anchor,
                    model_name=OFFICIAL_GLOBAL_PANEL_TFT_P10_MODEL_NAME,
                    forecast_prices=[800.0, 900.0],
                    actual_prices=[1000.0 if index < 4 else 7000.0, 1000.0],
                ),
                _evaluation_row(
                    anchor,
                    model_name=OFFICIAL_GLOBAL_PANEL_TFT_MODEL_NAME,
                    forecast_prices=[1000.0, 1100.0],
                    actual_prices=[1200.0 if index < 4 else 7000.0, 1000.0],
                ),
                _evaluation_row(
                    anchor,
                    model_name=OFFICIAL_GLOBAL_PANEL_TFT_P90_MODEL_NAME,
                    forecast_prices=[1200.0, 1300.0],
                    actual_prices=[1400.0 if index < 4 else 7000.0, 1000.0],
                ),
            ]
        )

    calibration = build_official_global_panel_tft_horizon_quantile_calibration_frame(
        pl.DataFrame(source_rows),
        min_prior_anchors=2,
        rolling_calibration_window_anchors=3,
    )

    latest_p50 = calibration.filter(
        (pl.col("anchor_timestamp") == first_anchor + timedelta(days=4))
        & (pl.col("source_forecast_model_name") == OFFICIAL_GLOBAL_PANEL_TFT_MODEL_NAME)
    ).row(0, named=True)
    first_p50 = calibration.filter(
        (pl.col("anchor_timestamp") == first_anchor)
        & (pl.col("source_forecast_model_name") == OFFICIAL_GLOBAL_PANEL_TFT_MODEL_NAME)
    ).row(0, named=True)

    assert latest_p50["source_quantile"] == "p50"
    assert latest_p50["calibration_status"] == "calibrated"
    assert latest_p50["horizon_biases_uah_mwh"] == pytest.approx([200.0, -100.0])
    assert latest_p50["quantile_spread_scale"] == pytest.approx(1.0)
    assert first_p50["calibration_status"] == "insufficient_prior_history"


def test_global_panel_tft_horizon_quantile_calibrated_gate_routes_corrected_forecasts() -> None:
    anchor = datetime(2026, 1, 10, 23)
    evaluation_frame = pl.DataFrame(
        [
            _evaluation_row(
                anchor,
                model_name="strict_similar_day",
                forecast_prices=[1000.0, 1400.0],
            ),
            _evaluation_row(
                anchor,
                model_name=OFFICIAL_GLOBAL_PANEL_TFT_P10_MODEL_NAME,
                forecast_prices=[700.0, 1100.0],
            ),
            _evaluation_row(
                anchor,
                model_name=OFFICIAL_GLOBAL_PANEL_TFT_MODEL_NAME,
                forecast_prices=[900.0, 1200.0],
            ),
            _evaluation_row(
                anchor,
                model_name=OFFICIAL_GLOBAL_PANEL_TFT_P90_MODEL_NAME,
                forecast_prices=[1300.0, 1500.0],
            ),
        ]
    )
    calibration_frame = pl.DataFrame(
        [
            _tft_calibration_row(
                anchor=anchor,
                source_model_name=OFFICIAL_GLOBAL_PANEL_TFT_P10_MODEL_NAME,
                corrected_model_name=OFFICIAL_GLOBAL_PANEL_TFT_P10_CALIBRATED_MODEL_NAME,
                source_quantile="p10",
                horizon_biases=[100.0, 200.0],
            ),
            _tft_calibration_row(
                anchor=anchor,
                source_model_name=OFFICIAL_GLOBAL_PANEL_TFT_MODEL_NAME,
                corrected_model_name=OFFICIAL_GLOBAL_PANEL_TFT_QUANTILE_CALIBRATED_MODEL_NAME,
                source_quantile="p50",
                horizon_biases=[200.0, 300.0],
            ),
            _tft_calibration_row(
                anchor=anchor,
                source_model_name=OFFICIAL_GLOBAL_PANEL_TFT_P90_MODEL_NAME,
                corrected_model_name=OFFICIAL_GLOBAL_PANEL_TFT_P90_CALIBRATED_MODEL_NAME,
                source_quantile="p90",
                horizon_biases=[-100.0, 50.0],
            ),
        ]
    )

    result = build_official_global_panel_tft_horizon_quantile_calibrated_strict_lp_benchmark_frame(
        evaluation_frame,
        calibration_frame,
    )

    assert set(result["forecast_model_name"].to_list()) == {
        "strict_similar_day",
        OFFICIAL_GLOBAL_PANEL_TFT_P10_MODEL_NAME,
        OFFICIAL_GLOBAL_PANEL_TFT_MODEL_NAME,
        OFFICIAL_GLOBAL_PANEL_TFT_P90_MODEL_NAME,
        OFFICIAL_GLOBAL_PANEL_TFT_P10_CALIBRATED_MODEL_NAME,
        OFFICIAL_GLOBAL_PANEL_TFT_QUANTILE_CALIBRATED_MODEL_NAME,
        OFFICIAL_GLOBAL_PANEL_TFT_P90_CALIBRATED_MODEL_NAME,
    }
    assert set(result["strategy_kind"].to_list()) == {
        OFFICIAL_GLOBAL_PANEL_TFT_QUANTILE_CALIBRATION_STRATEGY_KIND
    }
    corrected_payload = result.filter(
        pl.col("forecast_model_name")
        == OFFICIAL_GLOBAL_PANEL_TFT_QUANTILE_CALIBRATED_MODEL_NAME
    ).row(0, named=True)["evaluation_payload"]
    assert corrected_payload["source_forecast_model_name"] == (
        OFFICIAL_GLOBAL_PANEL_TFT_MODEL_NAME
    )
    assert corrected_payload["source_quantile"] == "p50"
    assert corrected_payload["horizon_biases_uah_mwh"] == [200.0, 300.0]
    assert corrected_payload["not_full_dfl"] is True
    assert corrected_payload["not_market_execution"] is True


def _silver_frame(
    *,
    tenant_ids: tuple[str, ...] = (TENANT_ID,),
    day_count: int = 12,
) -> pl.DataFrame:
    start = datetime(2026, 1, 1)
    rows: list[dict[str, object]] = []
    for tenant_index, tenant_id in enumerate(tenant_ids):
        for index in range(day_count * 24):
            timestamp = start + timedelta(hours=index)
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "timestamp": timestamp,
                    "price_uah_mwh": (
                        1000.0
                        + 300.0 * (index % 24 in {8, 9, 18, 19})
                        + 50.0 * tenant_index
                    ),
                    "source_kind": "observed",
                }
            )
    return pl.DataFrame(rows)


def _tft_calibration_row(
    *,
    anchor: datetime,
    source_model_name: str,
    corrected_model_name: str,
    source_quantile: str,
    horizon_biases: list[float],
) -> dict[str, object]:
    return {
        "tenant_id": TENANT_ID,
        "anchor_timestamp": anchor,
        "source_forecast_model_name": source_model_name,
        "corrected_forecast_model_name": corrected_model_name,
        "source_quantile": source_quantile,
        "horizon_biases_uah_mwh": horizon_biases,
        "mean_horizon_bias_uah_mwh": sum(horizon_biases) / len(horizon_biases),
        "max_abs_horizon_bias_uah_mwh": max(abs(value) for value in horizon_biases),
        "quantile_spread_scale": 1.0,
        "prior_anchor_count": 14,
        "calibration_window_anchor_count": 14,
        "calibration_status": "calibrated",
        "data_quality_tier": "thesis_grade",
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _global_panel_forecast_frame(*, anchor_timestamp: datetime) -> pl.DataFrame:
    timestamps = [anchor_timestamp + timedelta(hours=index + 1) for index in range(24)]
    return pl.DataFrame(
        {
            "model_name": ["nbeatsx_official_global_panel_v1"] * 24,
            "model_family": ["NBEATSx"] * 24,
            "backend_name": ["neuralforecast"] * 24,
            "backend_status": ["trained"] * 24,
            "unique_id": [f"{TENANT_ID}:DAM"] * 24,
            "forecast_timestamp": timestamps,
            "predicted_price_uah_mwh": [1100.0 + float(index % 5) * 25.0 for index in range(24)],
            "predicted_price_p10_uah_mwh": [None] * 24,
            "predicted_price_p50_uah_mwh": [1100.0 + float(index % 5) * 25.0 for index in range(24)],
            "predicted_price_p90_uah_mwh": [None] * 24,
            "prediction_interval_kind": ["point"] * 24,
            "training_rows": [200] * 24,
            "horizon_rows": [24] * 24,
            "adapter_scope": ["official_backend_forecast_candidate_not_live_strategy"] * 24,
        }
    )


def _global_panel_point_forecast_from_rows(
    forecast_rows: pl.DataFrame,
    *,
    model_name: str,
    model_family: str,
) -> pl.DataFrame:
    row_count = forecast_rows.height
    return pl.DataFrame(
        {
            "model_name": [model_name] * row_count,
            "model_family": [model_family] * row_count,
            "backend_name": ["neuralforecast"] * row_count,
            "backend_status": ["trained"] * row_count,
            "unique_id": forecast_rows["unique_id"].to_list(),
            "forecast_timestamp": forecast_rows["ds"].to_list(),
            "predicted_price_uah_mwh": [1100.0] * row_count,
            "predicted_price_p10_uah_mwh": [None] * row_count,
            "predicted_price_p50_uah_mwh": [1100.0] * row_count,
            "predicted_price_p90_uah_mwh": [None] * row_count,
            "prediction_interval_kind": ["point"] * row_count,
            "training_rows": [200] * row_count,
            "horizon_rows": [24] * row_count,
            "adapter_scope": [
                "experimental_poland_lag24_forecast_candidate_not_live_strategy"
            ]
            * row_count,
        }
    )


def _global_panel_tft_forecast_frame(*, anchor_timestamp: datetime) -> pl.DataFrame:
    timestamps = [anchor_timestamp + timedelta(hours=index + 1) for index in range(24)]
    return pl.DataFrame(
        {
            "model_name": [OFFICIAL_GLOBAL_PANEL_TFT_MODEL_NAME] * 24,
            "model_family": ["TFT"] * 24,
            "backend_name": ["pytorch_forecasting"] * 24,
            "backend_status": ["trained"] * 24,
            "unique_id": [f"{TENANT_ID}:DAM"] * 24,
            "forecast_timestamp": timestamps,
            "predicted_price_uah_mwh": [1100.0 + float(index % 5) * 25.0 for index in range(24)],
            "predicted_price_p10_uah_mwh": [1000.0 + float(index % 5) * 25.0 for index in range(24)],
            "predicted_price_p50_uah_mwh": [1100.0 + float(index % 5) * 25.0 for index in range(24)],
            "predicted_price_p90_uah_mwh": [1200.0 + float(index % 5) * 25.0 for index in range(24)],
            "prediction_interval_kind": ["quantile"] * 24,
            "training_rows": [200] * 24,
            "horizon_rows": [24] * 24,
            "adapter_scope": ["official_backend_forecast_candidate_not_live_strategy"] * 24,
        }
    )


def _global_panel_tft_forecast_from_rows(
    forecast_rows: pl.DataFrame,
    *,
    model_name: str = OFFICIAL_GLOBAL_PANEL_TFT_MODEL_NAME,
) -> pl.DataFrame:
    row_count = forecast_rows.height
    return pl.DataFrame(
        {
            "model_name": [model_name] * row_count,
            "model_family": ["TFT"] * row_count,
            "backend_name": ["pytorch_forecasting"] * row_count,
            "backend_status": ["trained"] * row_count,
            "unique_id": forecast_rows["unique_id"].to_list(),
            "forecast_timestamp": forecast_rows["ds"].to_list(),
            "predicted_price_uah_mwh": [1100.0] * row_count,
            "predicted_price_p10_uah_mwh": [1000.0] * row_count,
            "predicted_price_p50_uah_mwh": [1100.0] * row_count,
            "predicted_price_p90_uah_mwh": [1200.0] * row_count,
            "prediction_interval_kind": ["quantile"] * row_count,
            "training_rows": [200] * row_count,
            "horizon_rows": [24] * row_count,
            "adapter_scope": ["official_backend_forecast_candidate_not_live_strategy"] * row_count,
        }
    )


def _experimental_training_frame(
    *,
    tenant_ids: object,
    anchor_timestamp: datetime,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in tenant_ids:
        for hour_offset in range(-48, 25):
            timestamp = anchor_timestamp + timedelta(hours=hour_offset)
            rows.append(
                {
                    "unique_id": f"{tenant_id}:DAM",
                    "ds": timestamp,
                    "y": None if hour_offset > 0 else 1000.0,
                    "is_train": hour_offset <= 0,
                    "is_forecast": hour_offset > 0,
                    "entsoe_pl_lag24_day_ahead_price_uah_mwh": 900.0,
                    "training_panel_kind": (
                        "official_global_panel_poland_lag24_experimental"
                    ),
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows)


def _evaluation_row(
    anchor: datetime,
    *,
    model_name: str,
    forecast_prices: list[float],
    actual_prices: list[float] | None = None,
) -> dict[str, object]:
    resolved_actual_prices = actual_prices or [1000.0, 1500.0]
    return {
        "evaluation_id": f"{TENANT_ID}:{model_name}:{anchor:%Y%m%dT%H%M}",
        "tenant_id": TENANT_ID,
        "forecast_model_name": model_name,
        "strategy_kind": OFFICIAL_GLOBAL_PANEL_NBEATSX_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "horizon_hours": len(forecast_prices),
        "starting_soc_fraction": 0.5,
        "starting_soc_source": "tenant_default",
        "decision_value_uah": 100.0,
        "forecast_objective_value_uah": 90.0,
        "oracle_value_uah": 120.0,
        "regret_uah": 20.0,
        "regret_ratio": 0.1,
        "total_degradation_penalty_uah": 1.0,
        "total_throughput_mwh": 0.1,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "evaluation_payload": {
            "claim_scope": "official_global_panel_nbeatsx_strict_lp_not_full_dfl",
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "not_full_dfl": True,
            "not_market_execution": True,
            "horizon": [
                {
                    "step_index": index,
                    "interval_start": (anchor + timedelta(hours=index + 1)).isoformat(),
                    "forecast_price_uah_mwh": forecast_prices[index],
                    "actual_price_uah_mwh": resolved_actual_prices[index],
                    "net_power_mw": 0.0,
                    "degradation_penalty_uah": 0.0,
                }
                for index in range(len(forecast_prices))
            ],
        },
    }
