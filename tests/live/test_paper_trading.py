from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.live.paper_trading import build_simulated_live_trading_frame


def test_simulated_live_trading_frame_keeps_provenance_and_gatekeeper_status() -> None:
    start = datetime(2026, 5, 1, 12)
    transition_frame = pl.DataFrame(
        {
            "episode_id": ["e1", "e1"],
            "tenant_id": ["client_003_dnipro_factory", "client_003_dnipro_factory"],
            "interval_start": [start, start + timedelta(hours=1)],
            "step_index": [0, 1],
            "state_soc_before": [0.5, 0.45],
            "state_soc_after": [0.45, 0.5],
            "feasible_net_power_mw": [0.2, -0.2],
            "market_price_uah_mwh": [2000.0, 1000.0],
            "reward_uah": [390.0, -210.0],
            "cleared_trade_provenance": ["simulated", "simulated"],
        }
    )

    live_frame = build_simulated_live_trading_frame(transition_frame)

    assert live_frame.height == 2
    assert live_frame.select("paper_trade_provenance").to_series().unique().to_list() == ["simulated"]
    assert live_frame.select("settlement_id").null_count().item() == 2
    assert live_frame.select("gatekeeper_status").to_series().unique().to_list() == ["accepted"]
    assert "live_mode_warning" in live_frame.columns
