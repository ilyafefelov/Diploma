"""Decision Transformer policy primitive and deterministic battery safety projection."""

from __future__ import annotations

from dataclasses import dataclass

import torch
from torch import nn


@dataclass(frozen=True, slots=True)
class BatteryActionProjectionInput:
    raw_charge_mw: float
    raw_discharge_mw: float
    soc_fraction: float
    capacity_mwh: float
    max_power_mw: float
    soc_min_fraction: float
    soc_max_fraction: float
    duration_hours: float = 1.0


@dataclass(frozen=True, slots=True)
class FeasibleBatteryAction:
    charge_mw: float
    discharge_mw: float
    next_soc_fraction: float
    action: str


def project_action_to_feasible_battery_action(action_input: BatteryActionProjectionInput) -> FeasibleBatteryAction:
    """Project raw DT output into a one-step feasible charge/discharge action."""

    _validate_projection_input(action_input)
    wants_charge = action_input.raw_charge_mw >= action_input.raw_discharge_mw
    charge_mw = min(max(0.0, action_input.raw_charge_mw), action_input.max_power_mw) if wants_charge else 0.0
    discharge_mw = min(max(0.0, action_input.raw_discharge_mw), action_input.max_power_mw) if not wants_charge else 0.0
    max_charge_by_soc = (
        (action_input.soc_max_fraction - action_input.soc_fraction)
        * action_input.capacity_mwh
        / action_input.duration_hours
    )
    max_discharge_by_soc = (
        (action_input.soc_fraction - action_input.soc_min_fraction)
        * action_input.capacity_mwh
        / action_input.duration_hours
    )
    charge_mw = min(charge_mw, max(0.0, max_charge_by_soc))
    discharge_mw = min(discharge_mw, max(0.0, max_discharge_by_soc))
    next_soc_fraction = action_input.soc_fraction + (
        (charge_mw - discharge_mw) * action_input.duration_hours / action_input.capacity_mwh
    )
    next_soc_fraction = min(max(next_soc_fraction, action_input.soc_min_fraction), action_input.soc_max_fraction)
    projected_action = "CHARGE" if charge_mw > 0.0 else "DISCHARGE" if discharge_mw > 0.0 else "HOLD"
    return FeasibleBatteryAction(
        charge_mw=float(charge_mw),
        discharge_mw=float(discharge_mw),
        next_soc_fraction=float(next_soc_fraction),
        action=projected_action,
    )


class DecisionTransformerPolicy(nn.Module):
    """Small causal sequence model for offline return-conditioned dispatch research."""

    def __init__(
        self,
        *,
        state_dim: int,
        action_dim: int,
        hidden_dim: int = 64,
        context_length: int = 24,
        num_layers: int = 2,
        num_heads: int = 4,
        dropout: float = 0.0,
    ) -> None:
        super().__init__()
        if state_dim <= 0 or action_dim <= 0:
            raise ValueError("state_dim and action_dim must be positive.")
        if context_length <= 0:
            raise ValueError("context_length must be positive.")
        self.context_length = context_length
        self.input_projection = nn.Linear(state_dim + action_dim + 1, hidden_dim)
        self.position_embedding = nn.Parameter(torch.zeros(1, context_length, hidden_dim))
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hidden_dim,
            nhead=num_heads,
            dim_feedforward=hidden_dim * 4,
            dropout=dropout,
            batch_first=True,
            activation="gelu",
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        self.action_head = nn.Linear(hidden_dim, action_dim)

    def forward(
        self,
        *,
        states: torch.Tensor,
        actions: torch.Tensor,
        returns_to_go: torch.Tensor,
    ) -> torch.Tensor:
        if states.ndim != 3 or actions.ndim != 3 or returns_to_go.ndim != 3:
            raise ValueError("states, actions, and returns_to_go must be rank-3 tensors.")
        if states.shape[:2] != actions.shape[:2] or states.shape[:2] != returns_to_go.shape[:2]:
            raise ValueError("states, actions, and returns_to_go must share batch and sequence dimensions.")
        sequence_length = states.shape[1]
        if sequence_length > self.context_length:
            raise ValueError("sequence length exceeds configured context_length.")
        tokens = torch.cat([returns_to_go, states, actions], dim=-1)
        embedded = self.input_projection(tokens) + self.position_embedding[:, :sequence_length, :]
        mask = torch.triu(
            torch.ones(sequence_length, sequence_length, device=states.device, dtype=torch.bool),
            diagonal=1,
        )
        encoded = self.encoder(embedded, mask=mask)
        return self.action_head(encoded)


def _validate_projection_input(action_input: BatteryActionProjectionInput) -> None:
    if action_input.capacity_mwh <= 0.0:
        raise ValueError("capacity_mwh must be positive.")
    if action_input.max_power_mw <= 0.0:
        raise ValueError("max_power_mw must be positive.")
    if action_input.duration_hours <= 0.0:
        raise ValueError("duration_hours must be positive.")
    if not 0.0 <= action_input.soc_min_fraction <= action_input.soc_fraction <= action_input.soc_max_fraction <= 1.0:
        raise ValueError("SOC bounds must contain soc_fraction and stay within [0, 1].")
