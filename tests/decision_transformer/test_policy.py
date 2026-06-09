import torch

from smart_arbitrage.decision_transformer.policy import (
    BatteryActionProjectionInput,
    DecisionTransformerPolicy,
    project_action_to_feasible_battery_action,
)


def test_project_action_to_feasible_battery_action_respects_soc_and_power_limits() -> None:
    projected = project_action_to_feasible_battery_action(
        BatteryActionProjectionInput(
            raw_charge_mw=0.8,
            raw_discharge_mw=0.9,
            soc_fraction=0.94,
            capacity_mwh=1.0,
            max_power_mw=0.5,
            soc_min_fraction=0.05,
            soc_max_fraction=0.95,
            duration_hours=1.0,
        )
    )

    assert projected.charge_mw == 0.0
    assert 0.0 <= projected.discharge_mw <= 0.5
    assert projected.next_soc_fraction >= 0.05
    assert projected.next_soc_fraction <= 0.95
    assert projected.action in {"CHARGE", "DISCHARGE", "HOLD"}


def test_decision_transformer_policy_conditions_on_return_and_outputs_action_logits() -> None:
    model = DecisionTransformerPolicy(
        state_dim=6,
        action_dim=2,
        hidden_dim=16,
        context_length=4,
        num_layers=1,
        num_heads=2,
    )
    states = torch.zeros((2, 4, 6), dtype=torch.float32)
    actions = torch.zeros((2, 4, 2), dtype=torch.float32)
    returns_to_go = torch.ones((2, 4, 1), dtype=torch.float32)

    output = model(states=states, actions=actions, returns_to_go=returns_to_go)

    assert output.shape == (2, 4, 2)
    assert torch.isfinite(output).all()
