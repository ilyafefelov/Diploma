from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.dfl.regret_weighted import (
    build_regret_weighted_forecast_calibration_frame,
    build_horizon_regret_weighted_forecast_calibration_frame,
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


def test_horizon_regret_weighted_calibration_learns_step_biases_without_future_lookahead() -> None:
    first_anchor = datetime(2026, 5, 1, 23)
    rows: list[dict[str, object]] = []
    for index in range(6):
        anchor = first_anchor + timedelta(days=index)
        first_step_actual = 1200.0
        second_step_actual = 900.0
        if index == 5:
            first_step_actual = 5000.0
            second_step_actual = 5000.0
        rows.append(
            {
                "evaluation_id": f"eval:{index}",
                "tenant_id": "client_003_dnipro_factory",
                "forecast_model_name": "tft_silver_v0",
                "strategy_kind": "real_data_rolling_origin_benchmark",
                "market_venue": "DAM",
                "anchor_timestamp": anchor,
                "generated_at": datetime(2026, 5, 5),
                "horizon_hours": 2,
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
                    "data_quality_tier": "thesis_grade",
                    "observed_coverage_ratio": 1.0,
                    "horizon": [
                        {
                            "step_index": 0,
                            "interval_start": (anchor + timedelta(hours=1)).isoformat(),
                            "forecast_price_uah_mwh": 1000.0,
                            "actual_price_uah_mwh": first_step_actual,
                            "net_power_mw": 0.0,
                            "degradation_penalty_uah": 0.0,
                        },
                        {
                            "step_index": 1,
                            "interval_start": (anchor + timedelta(hours=2)).isoformat(),
                            "forecast_price_uah_mwh": 1000.0,
                            "actual_price_uah_mwh": second_step_actual,
                            "net_power_mw": 0.0,
                            "degradation_penalty_uah": 0.0,
                        },
                    ],
                },
            }
        )

    calibration = build_horizon_regret_weighted_forecast_calibration_frame(
        pl.DataFrame(rows),
        forecast_model_names=("tft_silver_v0",),
        min_prior_anchors=2,
        rolling_calibration_window_anchors=3,
    )

    fifth_anchor = first_anchor + timedelta(days=4)
    fifth_row = calibration.filter(pl.col("anchor_timestamp") == fifth_anchor).row(0, named=True)
    assert fifth_row["corrected_forecast_model_name"] == "tft_horizon_regret_weighted_calibrated_v0"
    assert fifth_row["horizon_biases_uah_mwh"] == pytest.approx([200.0, -100.0])
    assert fifth_row["mean_horizon_bias_uah_mwh"] == pytest.approx(50.0)
    assert fifth_row["calibration_status"] == "calibrated"

    first_row = calibration.filter(pl.col("anchor_timestamp") == first_anchor).row(0, named=True)
    assert first_row["calibration_status"] == "insufficient_prior_history"
    assert first_row["horizon_biases_uah_mwh"] == [0.0, 0.0]
