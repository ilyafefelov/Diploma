from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.regret_weighted import (
    build_regret_weighted_forecast_calibration_frame,
    run_regret_weighted_dfl_pilot,
)


def test_regret_weighted_dfl_pilot_learns_bias_correction_from_prior_rows() -> None:
    first_anchor = datetime(2026, 5, 1, 23)
    rows = [
        {
            "tenant_id": "client_003_dnipro_factory",
            "anchor_timestamp": first_anchor + timedelta(days=index),
            "forecast_model_name": "tft_silver_v0",
            "mean_forecast_price_uah_mwh": 1000.0,
            "mean_actual_price_uah_mwh": 1200.0,
            "training_weight": 1.0 + index,
            "regret_uah": 100.0 + index,
            "regret_ratio": 0.1,
            "data_quality_tier": "thesis_grade",
        }
        for index in range(8)
    ]

    result = run_regret_weighted_dfl_pilot(
        pl.DataFrame(rows),
        tenant_id="client_003_dnipro_factory",
        forecast_model_name="tft_silver_v0",
        validation_fraction=0.25,
    )

    row = result.row(0, named=True)
    assert row["pilot_name"] == "regret_weighted_bias_correction_v0"
    assert row["scope"] == "pilot_not_full_dfl"
    assert row["regret_weighted_bias_uah_mwh"] == 200.0
    assert row["validation_weighted_mae_after"] < row["validation_weighted_mae_before"]
    assert row["expanded_to_all_tenants_ready"] is True


def test_regret_weighted_calibration_expands_tft_and_nbeatsx_without_future_lookahead() -> None:
    first_anchor = datetime(2026, 5, 1, 23)
    rows: list[dict[str, object]] = []
    for model_name in ["tft_silver_v0", "nbeatsx_silver_v0"]:
        for index in range(6):
            actual = 1100.0
            if index == 5:
                actual = 5000.0
            rows.append(
                {
                    "tenant_id": "client_003_dnipro_factory",
                    "anchor_timestamp": first_anchor + timedelta(days=index),
                    "forecast_model_name": model_name,
                    "mean_forecast_price_uah_mwh": 1000.0,
                    "mean_actual_price_uah_mwh": actual,
                    "training_weight": 1.0,
                    "regret_uah": 100.0 + index,
                    "regret_ratio": 0.1,
                    "data_quality_tier": "thesis_grade",
                }
            )

    calibration = build_regret_weighted_forecast_calibration_frame(
        pl.DataFrame(rows),
        forecast_model_names=("tft_silver_v0", "nbeatsx_silver_v0"),
        min_prior_anchors=2,
        rolling_calibration_window_anchors=3,
    )

    fifth_anchor = first_anchor + timedelta(days=4)
    fifth_rows = calibration.filter(pl.col("anchor_timestamp") == fifth_anchor)
    assert set(fifth_rows["corrected_forecast_model_name"].to_list()) == {
        "tft_regret_weighted_calibrated_v0",
        "nbeatsx_regret_weighted_calibrated_v0",
    }
    assert set(fifth_rows["regret_weighted_bias_uah_mwh"].to_list()) == {100.0}

    first_rows = calibration.filter(pl.col("anchor_timestamp") == first_anchor)
    assert set(first_rows["calibration_status"].to_list()) == {"insufficient_prior_history"}
    assert set(first_rows["regret_weighted_bias_uah_mwh"].to_list()) == {0.0}
