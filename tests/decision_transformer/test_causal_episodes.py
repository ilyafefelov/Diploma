from datetime import datetime

import polars as pl
import pytest

from smart_arbitrage.decision_transformer.causal_episodes import (
    build_causal_temporal_episode_frame,
)


def _seed_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "tenant_id": ["tenant-a"],
            "source_model_name": ["tft"],
            "anchor_timestamp": [datetime(2026, 5, 1, 23)],
            "starting_soc_fraction": [0.5],
            "forecast_p10_uah_mwh": [[900.0, 800.0]],
            "forecast_p50_uah_mwh": [[1100.0, 1000.0]],
            "forecast_p90_uah_mwh": [[1300.0, 1200.0]],
            "price_lag_24_uah_mwh": [[950.0, 850.0]],
            "weather_temperature_c": [[15.0, 14.0]],
            "calendar_hour_sin": [[0.0, 0.2]],
            "calendar_hour_cos": [[1.0, 0.98]],
            "poland_lag24_uah_mwh": [[1050.0, 950.0]],
            "actual_price_uah_mwh_vector": [[1200.0, 700.0]],
            "teacher_dispatch_mw_vector": [[0.1, -0.2]],
            "teacher_soc_before_fraction_vector": [[0.5, 0.4]],
            "teacher_soc_after_fraction_vector": [[0.4, 0.6]],
            "teacher_degradation_penalty_uah_vector": [[2.0, 4.0]],
            "teacher_solver_status": ["optimal"],
            "market_execution_enabled": [False],
        }
    )


def test_builds_time_ordered_forecast_only_state_and_post_outcome_labels() -> None:
    frame = build_causal_temporal_episode_frame(_seed_frame())

    assert frame.height == 2
    assert frame["interval_start"].to_list() == [
        datetime(2026, 5, 2, 0),
        datetime(2026, 5, 2, 1),
    ]
    assert frame["state_forecast_p50_uah_mwh"].to_list() == [1100.0, 1000.0]
    assert frame["action_signed_dispatch_mw"].to_list() == [0.1, -0.2]
    assert frame["label_reward_uah"].to_list() == [118.0, -144.0]
    assert frame["label_return_to_go_uah"].to_list() == [-26.0, -144.0]
    assert "state_actual_price_uah_mwh" not in frame.columns
    assert frame["dt_training_eligible"].unique().to_list() == [False]
    assert frame["market_execution_enabled"].unique().to_list() == [False]


def test_rejects_non_causal_or_incomplete_episode_contract() -> None:
    unsafe = _seed_frame().with_columns(pl.lit(True).alias("market_execution_enabled"))
    with pytest.raises(ValueError, match="market_execution_enabled=false"):
        build_causal_temporal_episode_frame(unsafe)

    incomplete = _seed_frame().with_columns(pl.lit([0.1]).alias("teacher_dispatch_mw_vector"))
    with pytest.raises(ValueError, match="equal vector lengths"):
        build_causal_temporal_episode_frame(incomplete)
