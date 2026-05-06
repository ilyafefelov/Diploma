from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.decision_transformer.policy_training import (
    DECISION_TRANSFORMER_STATE_FEATURE_NAMES,
    DecisionTransformerTrainingConfig,
    build_decision_transformer_policy_preview_frame,
    _state_features,
)


def test_policy_preview_projects_trained_dt_actions_into_feasible_steps() -> None:
    trajectory_frame = pl.DataFrame(
        {
            "episode_id": ["episode-001" for _ in range(4)],
            "tenant_id": ["client_003_dnipro_factory" for _ in range(4)],
            "market_venue": ["DAM" for _ in range(4)],
            "scenario_index": [0 for _ in range(4)],
            "step_index": [0, 1, 2, 3],
            "interval_start": [
                datetime(2026, 5, 5, hour, tzinfo=UTC)
                for hour in range(4)
            ],
            "state_soc_before": [0.50, 0.45, 0.55, 0.50],
            "state_soc_after": [0.45, 0.55, 0.50, 0.60],
            "state_soh": [0.97, 0.97, 0.97, 0.97],
            "state_market_price_uah_mwh": [4200.0, 1800.0, 3900.0, 1600.0],
            "action_charge_mw": [0.0, 0.1, 0.0, 0.1],
            "action_discharge_mw": [0.1, 0.0, 0.1, 0.0],
            "reward_uah": [416.0, -184.0, 386.0, -164.0],
            "return_to_go_uah": [454.0, 38.0, 222.0, -164.0],
            "degradation_penalty_uah": [4.0, 4.0, 4.0, 4.0],
            "baseline_value_uah": [90.0, 90.0, 90.0, 90.0],
            "oracle_value_uah": [550.0, 550.0, 550.0, 550.0],
            "regret_uah": [96.0, 96.0, 96.0, 96.0],
            "academic_scope": [
                "offline_dt_training_trajectory_not_live_policy"
                for _ in range(4)
            ],
        }
    )

    preview_frame = build_decision_transformer_policy_preview_frame(
        trajectory_frame,
        config=DecisionTransformerTrainingConfig(
            seed=7,
            max_epochs=1,
            context_length=4,
            hidden_dim=16,
            num_layers=1,
            num_heads=2,
        ),
    )

    assert preview_frame.height == 4
    assert preview_frame.select("readiness_status").to_series().unique().to_list() == [
        "ready_for_operator_preview"
    ]
    assert preview_frame.select("constraint_violation").to_series().to_list() == [
        False,
        False,
        False,
        False,
    ]
    assert max(preview_frame.select("projected_soc_after").to_series().to_list()) <= 0.95
    assert min(preview_frame.select("projected_soc_after").to_series().to_list()) >= 0.05
    assert preview_frame.select("academic_scope").to_series().unique().to_list() == [
        "offline_dt_policy_preview_not_market_execution"
    ]
    assert set(preview_frame.select("policy_mode").to_series().unique().to_list()) == {
        "decision_transformer_preview"
    }


def test_dt_state_features_include_time_and_degradation_context() -> None:
    row = {
        "interval_start": datetime(2026, 5, 5, 6, tzinfo=UTC),
        "state_soc_before": 0.5,
        "state_soh": 0.97,
        "state_market_price_uah_mwh": 4200.0,
        "degradation_penalty_uah": 12.0,
    }

    features = _state_features(row)

    assert DECISION_TRANSFORMER_STATE_FEATURE_NAMES == (
        "state_soc_before",
        "state_soh",
        "state_market_price_scaled",
        "hour_sin",
        "hour_cos",
        "degradation_penalty_scaled",
    )
    assert len(features) == len(DECISION_TRANSFORMER_STATE_FEATURE_NAMES)
    assert features[3] == pytest.approx(1.0)
    assert features[4] == pytest.approx(0.0, abs=1e-9)


def test_policy_preview_uses_latest_rows_for_requested_tenant_only() -> None:
    timestamps = [datetime(2026, 5, 5, tzinfo=UTC) + timedelta(hours=index) for index in range(2)]
    trajectory_frame = pl.DataFrame(
        {
            "episode_id": ["a", "b"],
            "tenant_id": ["client_001_kyiv_mall", "client_002_lviv_office"],
            "market_venue": ["DAM", "DAM"],
            "scenario_index": [0, 0],
            "step_index": [0, 0],
            "interval_start": timestamps,
            "state_soc_before": [0.5, 0.6],
            "state_soc_after": [0.55, 0.65],
            "state_soh": [0.96, 0.97],
            "state_market_price_uah_mwh": [1000.0, 1200.0],
            "action_charge_mw": [0.1, 0.1],
            "action_discharge_mw": [0.0, 0.0],
            "reward_uah": [-100.0, -120.0],
            "return_to_go_uah": [-100.0, -120.0],
            "degradation_penalty_uah": [2.0, 2.0],
            "baseline_value_uah": [0.0, 0.0],
            "oracle_value_uah": [50.0, 60.0],
            "regret_uah": [50.0, 60.0],
            "academic_scope": [
                "offline_dt_training_trajectory_not_live_policy",
                "offline_dt_training_trajectory_not_live_policy",
            ],
        }
    )

    preview_frame = build_decision_transformer_policy_preview_frame(
        trajectory_frame,
        tenant_id="client_002_lviv_office",
        config=DecisionTransformerTrainingConfig(max_epochs=0),
    )

    assert preview_frame.height == 1
    assert preview_frame.select("tenant_id").to_series().to_list() == [
        "client_002_lviv_office"
    ]
