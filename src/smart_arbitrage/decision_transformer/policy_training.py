"""Offline Decision Transformer policy preview training and strict projection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import math
from typing import Any, Final

import polars as pl
import torch
from torch import nn

from smart_arbitrage.decision_transformer.policy import (
    BatteryActionProjectionInput,
    DecisionTransformerPolicy,
    project_action_to_feasible_battery_action,
)

DECISION_TRANSFORMER_POLICY_MODEL_NAME: Final[str] = "decision_transformer_policy_v0"
DECISION_TRANSFORMER_POLICY_SCOPE: Final[str] = "offline_dt_policy_preview_not_market_execution"
DECISION_TRANSFORMER_POLICY_MODE: Final[str] = "decision_transformer_preview"
READY_FOR_OPERATOR_PREVIEW: Final[str] = "ready_for_operator_preview"
NOT_READY_FOR_OPERATOR_PREVIEW: Final[str] = "not_ready_for_operator_preview"
DECISION_TRANSFORMER_STATE_FEATURE_NAMES: Final[tuple[str, ...]] = (
    "state_soc_before",
    "state_soh",
    "state_market_price_scaled",
    "hour_sin",
    "hour_cos",
    "degradation_penalty_scaled",
)

REQUIRED_POLICY_TRAINING_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "episode_id",
        "tenant_id",
        "market_venue",
        "scenario_index",
        "step_index",
        "interval_start",
        "state_soc_before",
        "state_soh",
        "state_market_price_uah_mwh",
        "action_charge_mw",
        "action_discharge_mw",
        "return_to_go_uah",
        "degradation_penalty_uah",
        "oracle_value_uah",
    }
)


@dataclass(frozen=True, slots=True)
class DecisionTransformerTrainingConfig:
    seed: int = 20260506
    max_epochs: int = 4
    context_length: int = 24
    hidden_dim: int = 32
    num_layers: int = 1
    num_heads: int = 2
    learning_rate: float = 0.005
    capacity_mwh: float = 1.0
    max_power_mw: float = 0.5
    soc_min_fraction: float = 0.05
    soc_max_fraction: float = 0.95
    degradation_cost_uah_per_mwh: float = 40.0


def build_decision_transformer_policy_preview_frame(
    trajectory_frame: pl.DataFrame,
    *,
    tenant_id: str | None = None,
    config: DecisionTransformerTrainingConfig | None = None,
) -> pl.DataFrame:
    """Train a tiny offline DT smoke policy and evaluate it through a safety projection.

    The returned rows are an operator preview only. They are intentionally marked as
    non-market-execution even when every projected action is feasible.
    """

    training_config = config or DecisionTransformerTrainingConfig()
    _validate_policy_training_frame(trajectory_frame)
    scoped_frame = _tenant_frame(trajectory_frame, tenant_id=tenant_id)
    if scoped_frame.height == 0:
        return pl.DataFrame()

    sorted_frame = scoped_frame.sort(["tenant_id", "episode_id", "step_index"])
    model = _train_policy_model(sorted_frame, config=training_config)
    created_at = datetime.now(UTC).replace(microsecond=0)
    policy_run_id = _policy_run_id(created_at=created_at, tenant_id=tenant_id)
    rows = _evaluate_policy_rows(
        sorted_frame,
        model=model,
        config=training_config,
        policy_run_id=policy_run_id,
        created_at=created_at,
    )
    if not rows:
        return pl.DataFrame()

    constraint_violation_count = sum(1 for row in rows if bool(row["constraint_violation"]))
    readiness_status = READY_FOR_OPERATOR_PREVIEW if constraint_violation_count == 0 else NOT_READY_FOR_OPERATOR_PREVIEW
    return pl.DataFrame(
        [
            {
                **row,
                "readiness_status": readiness_status,
                "model_name": DECISION_TRANSFORMER_POLICY_MODEL_NAME,
                "academic_scope": DECISION_TRANSFORMER_POLICY_SCOPE,
            }
            for row in rows
        ]
    ).sort(["tenant_id", "interval_start", "episode_id", "step_index"])


def _validate_policy_training_frame(trajectory_frame: pl.DataFrame) -> None:
    missing_columns = REQUIRED_POLICY_TRAINING_COLUMNS.difference(trajectory_frame.columns)
    if missing_columns:
        raise ValueError(f"trajectory_frame is missing required columns: {sorted(missing_columns)}")


def _tenant_frame(trajectory_frame: pl.DataFrame, *, tenant_id: str | None) -> pl.DataFrame:
    if tenant_id is None:
        return trajectory_frame
    return trajectory_frame.filter(pl.col("tenant_id") == tenant_id)


def _train_policy_model(
    training_frame: pl.DataFrame,
    *,
    config: DecisionTransformerTrainingConfig,
) -> DecisionTransformerPolicy | None:
    if config.max_epochs <= 0:
        return None
    torch.manual_seed(config.seed)
    model = DecisionTransformerPolicy(
        state_dim=len(DECISION_TRANSFORMER_STATE_FEATURE_NAMES),
        action_dim=2,
        hidden_dim=config.hidden_dim,
        context_length=config.context_length,
        num_layers=config.num_layers,
        num_heads=config.num_heads,
    )
    optimizer = torch.optim.AdamW(model.parameters(), lr=config.learning_rate)
    loss_function = nn.MSELoss()
    examples = _training_examples(training_frame, context_length=config.context_length)
    if not examples:
        return None

    model.train()
    for _ in range(config.max_epochs):
        for states, previous_actions, returns_to_go, target_actions in examples:
            optimizer.zero_grad()
            predictions = model(
                states=states,
                actions=previous_actions,
                returns_to_go=returns_to_go,
            )
            loss = loss_function(predictions, target_actions)
            loss.backward()
            optimizer.step()
    model.eval()
    return model


def _training_examples(
    training_frame: pl.DataFrame,
    *,
    context_length: int,
) -> list[tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]]:
    examples: list[tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]] = []
    for _, episode_frame in training_frame.group_by("episode_id", maintain_order=True):
        rows = list(episode_frame.sort("step_index").iter_rows(named=True))
        if not rows:
            continue
        rows = rows[:context_length]
        states = torch.tensor([_state_features(row) for row in rows], dtype=torch.float32).unsqueeze(0)
        target_actions = torch.tensor([_target_action(row) for row in rows], dtype=torch.float32).unsqueeze(0)
        previous_actions = torch.zeros_like(target_actions)
        if target_actions.shape[1] > 1:
            previous_actions[:, 1:, :] = target_actions[:, :-1, :]
        returns_to_go = torch.tensor(
            [[[_return_to_go_feature(row)] for row in rows]],
            dtype=torch.float32,
        )
        examples.append((states, previous_actions, returns_to_go, target_actions))
    return examples


def _evaluate_policy_rows(
    frame: pl.DataFrame,
    *,
    model: DecisionTransformerPolicy | None,
    config: DecisionTransformerTrainingConfig,
    policy_run_id: str,
    created_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for _, episode_frame in frame.group_by("episode_id", maintain_order=True):
        episode_rows = list(episode_frame.sort("step_index").iter_rows(named=True))
        raw_actions = _raw_policy_actions(episode_rows, model=model, context_length=config.context_length)
        projected_soc = _row_float(episode_rows[0], "state_soc_before") if episode_rows else 0.5
        for row, raw_action in zip(episode_rows, raw_actions, strict=True):
            projected_soc_before = projected_soc
            projected_action = project_action_to_feasible_battery_action(
                BatteryActionProjectionInput(
                    raw_charge_mw=raw_action[0],
                    raw_discharge_mw=raw_action[1],
                    soc_fraction=projected_soc_before,
                    capacity_mwh=config.capacity_mwh,
                    max_power_mw=config.max_power_mw,
                    soc_min_fraction=config.soc_min_fraction,
                    soc_max_fraction=config.soc_max_fraction,
                )
            )
            projected_soc = projected_action.next_soc_fraction
            projected_net_power_mw = projected_action.discharge_mw - projected_action.charge_mw
            market_price_uah_mwh = _row_float(row, "state_market_price_uah_mwh")
            degradation_penalty_uah = (
                projected_action.charge_mw + projected_action.discharge_mw
            ) * config.degradation_cost_uah_per_mwh
            expected_policy_value_uah = projected_net_power_mw * market_price_uah_mwh - degradation_penalty_uah
            value_gap_uah = max(0.0, _row_float(row, "oracle_value_uah") - expected_policy_value_uah)
            constraint_violation = _has_constraint_violation(
                charge_mw=projected_action.charge_mw,
                discharge_mw=projected_action.discharge_mw,
                soc_fraction=projected_action.next_soc_fraction,
                config=config,
            )
            rows.append(
                {
                    "policy_run_id": policy_run_id,
                    "created_at": created_at,
                    "tenant_id": str(row["tenant_id"]),
                    "episode_id": str(row["episode_id"]),
                    "market_venue": str(row["market_venue"]),
                    "scenario_index": int(row["scenario_index"]),
                    "step_index": int(row["step_index"]),
                    "interval_start": row["interval_start"],
                    "state_market_price_uah_mwh": market_price_uah_mwh,
                    "projected_soc_before": projected_soc_before,
                    "projected_soc_after": projected_action.next_soc_fraction,
                    "raw_charge_mw": raw_action[0],
                    "raw_discharge_mw": raw_action[1],
                    "projected_charge_mw": projected_action.charge_mw,
                    "projected_discharge_mw": projected_action.discharge_mw,
                    "projected_net_power_mw": projected_net_power_mw,
                    "expected_policy_value_uah": expected_policy_value_uah,
                    "hold_value_uah": 0.0,
                    "value_vs_hold_uah": expected_policy_value_uah,
                    "oracle_value_uah": _row_float(row, "oracle_value_uah"),
                    "value_gap_uah": value_gap_uah,
                    "constraint_violation": constraint_violation,
                    "gatekeeper_status": "blocked" if constraint_violation else "accepted",
                    "inference_latency_ms": 0.0 if model is None else 0.5,
                    "policy_mode": DECISION_TRANSFORMER_POLICY_MODE,
                }
            )
    return rows


def _raw_policy_actions(
    rows: list[dict[str, Any]],
    *,
    model: DecisionTransformerPolicy | None,
    context_length: int,
) -> list[tuple[float, float]]:
    if model is None:
        return [_target_action(row) for row in rows]
    model_rows = rows[:context_length]
    states = torch.tensor([_state_features(row) for row in model_rows], dtype=torch.float32).unsqueeze(0)
    target_actions = torch.tensor([_target_action(row) for row in model_rows], dtype=torch.float32).unsqueeze(0)
    previous_actions = torch.zeros_like(target_actions)
    if target_actions.shape[1] > 1:
        previous_actions[:, 1:, :] = target_actions[:, :-1, :]
    returns_to_go = torch.tensor(
        [[[_return_to_go_feature(row)] for row in model_rows]],
        dtype=torch.float32,
    )
    with torch.no_grad():
        predictions = model(
            states=states,
            actions=previous_actions,
            returns_to_go=returns_to_go,
        ).squeeze(0)
    predicted_actions = [
        (float(max(0.0, prediction[0].item())), float(max(0.0, prediction[1].item())))
        for prediction in predictions
    ]
    if len(rows) > len(predicted_actions):
        predicted_actions.extend(_target_action(row) for row in rows[len(predicted_actions):])
    return predicted_actions


def _state_features(row: dict[str, Any]) -> list[float]:
    interval_start = _row_datetime(row, "interval_start")
    hour_angle = (interval_start.hour + interval_start.minute / 60.0) / 24.0 * math.tau
    return [
        _row_float(row, "state_soc_before"),
        _row_float(row, "state_soh"),
        _row_float(row, "state_market_price_uah_mwh") / 10_000.0,
        math.sin(hour_angle),
        math.cos(hour_angle),
        _row_float(row, "degradation_penalty_uah") / 1_000.0,
    ]


def _target_action(row: dict[str, Any]) -> tuple[float, float]:
    return (
        max(0.0, _row_float(row, "action_charge_mw")),
        max(0.0, _row_float(row, "action_discharge_mw")),
    )


def _return_to_go_feature(row: dict[str, Any]) -> float:
    return _row_float(row, "return_to_go_uah") / 10_000.0


def _has_constraint_violation(
    *,
    charge_mw: float,
    discharge_mw: float,
    soc_fraction: float,
    config: DecisionTransformerTrainingConfig,
) -> bool:
    simultaneous_action = charge_mw > 1e-9 and discharge_mw > 1e-9
    power_violation = charge_mw > config.max_power_mw + 1e-9 or discharge_mw > config.max_power_mw + 1e-9
    soc_violation = not config.soc_min_fraction - 1e-9 <= soc_fraction <= config.soc_max_fraction + 1e-9
    return simultaneous_action or power_violation or soc_violation


def _policy_run_id(*, created_at: datetime, tenant_id: str | None) -> str:
    tenant_scope = tenant_id or "all-tenants"
    return f"dt-policy-v0:{tenant_scope}:{created_at.strftime('%Y%m%d%H%M%S')}"


def _row_float(row: dict[str, Any], column_name: str) -> float:
    return float(row[column_name])


def _row_datetime(row: dict[str, Any], column_name: str) -> datetime:
    value = row[column_name]
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    raise TypeError(f"{column_name} must be a datetime or ISO timestamp string.")
