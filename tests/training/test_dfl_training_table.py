from datetime import datetime

import polars as pl
import pytest

from smart_arbitrage.training.dfl_training import build_dfl_training_frame


def test_dfl_training_frame_flattens_benchmark_payload_for_value_training() -> None:
    evaluation = pl.DataFrame(
        [
            {
                "evaluation_id": "eval-1",
                "tenant_id": "client_003_dnipro_factory",
                "forecast_model_name": "tft_silver_v0",
                "strategy_kind": "real_data_rolling_origin_benchmark",
                "market_venue": "DAM",
                "anchor_timestamp": datetime(2026, 5, 3, 23),
                "generated_at": datetime(2026, 5, 5),
                "horizon_hours": 2,
                "starting_soc_fraction": 0.52,
                "starting_soc_source": "tenant_default",
                "decision_value_uah": 700.0,
                "forecast_objective_value_uah": 800.0,
                "oracle_value_uah": 1000.0,
                "regret_uah": 300.0,
                "regret_ratio": 0.3,
                "total_degradation_penalty_uah": 25.0,
                "total_throughput_mwh": 0.2,
                "committed_action": "DISCHARGE",
                "committed_power_mw": 0.2,
                "rank_by_regret": 2,
                "evaluation_payload": {
                    "data_quality_tier": "thesis_grade",
                    "observed_coverage_ratio": 1.0,
                    "efc_proxy": 0.2,
                    "forecast_diagnostics": {
                        "mae_uah_mwh": 100.0,
                        "rmse_uah_mwh": 120.0,
                        "smape": 0.1,
                    },
                    "horizon": [
                        {
                            "forecast_price_uah_mwh": 900.0,
                            "actual_price_uah_mwh": 1000.0,
                            "net_power_mw": 0.2,
                        },
                        {
                            "forecast_price_uah_mwh": 1100.0,
                            "actual_price_uah_mwh": 1200.0,
                            "net_power_mw": 0.0,
                        },
                    ],
                },
            }
        ]
    )

    training_frame = build_dfl_training_frame(evaluation)

    row = training_frame.row(0, named=True)
    assert row["tenant_id"] == "client_003_dnipro_factory"
    assert row["forecast_model_name"] == "tft_silver_v0"
    assert row["lp_committed_action"] == "DISCHARGE"
    assert row["oracle_value_uah"] == 1000.0
    assert row["regret_uah"] == 300.0
    assert row["forecast_mae_uah_mwh"] == 100.0
    assert row["mean_forecast_price_uah_mwh"] == 1000.0
    assert row["mean_actual_price_uah_mwh"] == 1100.0
    assert row["training_weight"] == pytest.approx(1.3)
    assert row["market_price_cap_max"] == 15000.0
    assert row["market_regime_id"] == "ua_neurc_621_2026"


def test_dfl_training_frame_rejects_demo_grade_rows_by_default() -> None:
    evaluation = pl.DataFrame(
        [
            {
                "evaluation_id": "eval-1",
                "tenant_id": "client_003_dnipro_factory",
                "forecast_model_name": "strict_similar_day",
                "strategy_kind": "real_data_rolling_origin_benchmark",
                "market_venue": "DAM",
                "anchor_timestamp": datetime(2026, 5, 3, 23),
                "generated_at": datetime(2026, 5, 5),
                "horizon_hours": 1,
                "starting_soc_fraction": 0.52,
                "starting_soc_source": "tenant_default",
                "decision_value_uah": 700.0,
                "forecast_objective_value_uah": 800.0,
                "oracle_value_uah": 1000.0,
                "regret_uah": 300.0,
                "regret_ratio": 0.3,
                "total_degradation_penalty_uah": 25.0,
                "total_throughput_mwh": 0.2,
                "committed_action": "HOLD",
                "committed_power_mw": 0.0,
                "rank_by_regret": 1,
                "evaluation_payload": {"data_quality_tier": "demo_grade"},
            }
        ]
    )

    with pytest.raises(ValueError, match="thesis_grade"):
        build_dfl_training_frame(evaluation)
