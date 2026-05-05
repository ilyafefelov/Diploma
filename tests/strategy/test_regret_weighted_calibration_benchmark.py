from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.regret_weighted import (
    REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
    build_regret_weighted_forecast_strategy_benchmark_frame,
)


def test_regret_weighted_calibration_benchmark_routes_corrected_nbeatsx_and_tft_through_lp() -> None:
    anchor = datetime(2026, 5, 1, 23)
    source_frame = pl.DataFrame(
        [
            _evaluation_row(anchor, "strict_similar_day", [1000.0, 1400.0]),
            _evaluation_row(anchor, "tft_silver_v0", [950.0, 1350.0]),
            _evaluation_row(anchor, "nbeatsx_silver_v0", [900.0, 1300.0]),
        ]
    )
    calibration_frame = pl.DataFrame(
        [
            _calibration_row(anchor, "tft_silver_v0", "tft_regret_weighted_calibrated_v0", 50.0),
            _calibration_row(anchor, "nbeatsx_silver_v0", "nbeatsx_regret_weighted_calibrated_v0", 100.0),
        ]
    )

    result = build_regret_weighted_forecast_strategy_benchmark_frame(
        source_frame,
        calibration_frame,
    )

    assert result.height == 5
    assert set(result["forecast_model_name"].to_list()) == {
        "strict_similar_day",
        "tft_silver_v0",
        "nbeatsx_silver_v0",
        "tft_regret_weighted_calibrated_v0",
        "nbeatsx_regret_weighted_calibrated_v0",
    }
    assert set(result["strategy_kind"].to_list()) == {REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND}
    corrected_payload = result.filter(
        pl.col("forecast_model_name") == "nbeatsx_regret_weighted_calibrated_v0"
    ).row(0, named=True)["evaluation_payload"]
    assert corrected_payload["source_forecast_model_name"] == "nbeatsx_silver_v0"
    assert corrected_payload["regret_weighted_bias_uah_mwh"] == 100.0
    assert corrected_payload["academic_scope"] == "Regret-weighted forecast calibration benchmark; not full differentiable DFL."


def _evaluation_row(anchor: datetime, model_name: str, forecast_prices: list[float]) -> dict[str, object]:
    actual_prices = [1000.0, 1500.0]
    return {
        "evaluation_id": f"source:{model_name}",
        "tenant_id": "client_003_dnipro_factory",
        "forecast_model_name": model_name,
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
                    "step_index": index,
                    "interval_start": (anchor + timedelta(hours=index + 1)).isoformat(),
                    "forecast_price_uah_mwh": forecast_prices[index],
                    "actual_price_uah_mwh": actual_prices[index],
                    "net_power_mw": 0.0,
                    "degradation_penalty_uah": 0.0,
                }
                for index in range(2)
            ],
        },
    }


def _calibration_row(
    anchor: datetime,
    source_model_name: str,
    corrected_model_name: str,
    bias: float,
) -> dict[str, object]:
    return {
        "tenant_id": "client_003_dnipro_factory",
        "anchor_timestamp": anchor,
        "source_forecast_model_name": source_model_name,
        "corrected_forecast_model_name": corrected_model_name,
        "regret_weighted_bias_uah_mwh": bias,
        "prior_anchor_count": 14,
        "calibration_window_anchor_count": 14,
        "calibration_status": "calibrated",
        "data_quality_tier": "thesis_grade",
    }
