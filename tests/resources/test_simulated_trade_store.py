from datetime import UTC, datetime

import polars as pl

from smart_arbitrage.resources.simulated_trade_store import InMemorySimulatedTradeStore


def test_simulated_trade_store_persists_decision_policy_preview_rows() -> None:
    store = InMemorySimulatedTradeStore()
    preview_frame = pl.DataFrame(
        {
            "policy_run_id": ["dt-run-001", "dt-run-001"],
            "created_at": [
                datetime(2026, 5, 5, 12, tzinfo=UTC),
                datetime(2026, 5, 5, 12, tzinfo=UTC),
            ],
            "tenant_id": ["client_003_dnipro_factory", "client_003_dnipro_factory"],
            "episode_id": ["episode-001", "episode-001"],
            "market_venue": ["DAM", "DAM"],
            "scenario_index": [0, 0],
            "step_index": [0, 1],
            "interval_start": [
                datetime(2026, 5, 5, 0, tzinfo=UTC),
                datetime(2026, 5, 5, 1, tzinfo=UTC),
            ],
            "state_market_price_uah_mwh": [4200.0, 1600.0],
            "projected_soc_before": [0.5, 0.45],
            "projected_soc_after": [0.45, 0.55],
            "raw_charge_mw": [0.0, 0.1],
            "raw_discharge_mw": [0.1, 0.0],
            "projected_charge_mw": [0.0, 0.1],
            "projected_discharge_mw": [0.1, 0.0],
            "projected_net_power_mw": [0.1, -0.1],
            "expected_policy_value_uah": [416.0, -164.0],
            "hold_value_uah": [0.0, 0.0],
            "value_vs_hold_uah": [416.0, -164.0],
            "oracle_value_uah": [550.0, 550.0],
            "value_gap_uah": [134.0, 714.0],
            "constraint_violation": [False, False],
            "gatekeeper_status": ["accepted", "accepted"],
            "inference_latency_ms": [0.4, 0.5],
            "policy_mode": ["decision_transformer_preview", "decision_transformer_preview"],
            "readiness_status": ["ready_for_operator_preview", "ready_for_operator_preview"],
            "model_name": ["decision_transformer_policy_v0", "decision_transformer_policy_v0"],
            "academic_scope": [
                "offline_dt_policy_preview_not_market_execution",
                "offline_dt_policy_preview_not_market_execution",
            ],
        }
    )

    store.upsert_decision_transformer_policy_preview_frame(preview_frame)

    latest = store.latest_decision_transformer_policy_preview_frame(
        tenant_id="client_003_dnipro_factory",
    )
    assert latest.height == 2
    assert latest.select("policy_run_id").to_series().unique().to_list() == ["dt-run-001"]
    assert latest.select("readiness_status").to_series().unique().to_list() == [
        "ready_for_operator_preview"
    ]
