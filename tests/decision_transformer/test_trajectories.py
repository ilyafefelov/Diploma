from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.decision_transformer.trajectories import build_decision_transformer_trajectory_frame


def test_decision_transformer_trajectory_frame_adds_return_to_go_and_action_channels() -> None:
    start = datetime(2026, 5, 1, 12)
    transition_frame = pl.DataFrame(
        {
            "episode_id": ["e1", "e1", "e1"],
            "tenant_id": ["tenant", "tenant", "tenant"],
            "market_venue": ["DAM", "DAM", "DAM"],
            "scenario_index": [0, 0, 0],
            "step_index": [0, 1, 2],
            "interval_start": [start, start + timedelta(hours=1), start + timedelta(hours=2)],
            "state_soc_before": [0.5, 0.4, 0.6],
            "state_soc_after": [0.4, 0.6, 0.55],
            "state_soh": [0.97, 0.97, 0.97],
            "feasible_net_power_mw": [0.1, -0.2, 0.05],
            "market_price_uah_mwh": [1000.0, 500.0, 1200.0],
            "reward_uah": [100.0, -120.0, 60.0],
            "degradation_penalty_uah": [1.0, 2.0, 1.0],
            "baseline_value_uah": [40.0, 40.0, 40.0],
            "oracle_value_uah": [80.0, 80.0, 80.0],
            "regret_uah": [40.0, 40.0, 40.0],
        }
    )

    frame = build_decision_transformer_trajectory_frame(transition_frame)

    assert frame.height == 3
    assert frame.select("return_to_go_uah").to_series().to_list() == [40.0, -60.0, 60.0]
    assert frame.select("action_discharge_mw").to_series().to_list() == [0.1, 0.0, 0.05]
    assert frame.select("action_charge_mw").to_series().to_list() == [0.0, 0.2, 0.0]
