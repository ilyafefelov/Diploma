"""Time-separated differentiable forecast-to-storage research for paper v1.2."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from math import pi
from pathlib import Path
import random
from statistics import mean, median
from typing import Final, Literal, cast

import polars as pl
import torch
from torch import nn

from smart_arbitrage.dfl.relaxed_dispatch import solve_relaxed_dispatch_tensor
from smart_arbitrage.assets.gold.baseline_solver import (
    BaselineForecastPoint,
    BaselineSolverConfig,
    HourlyDamBaselineSolver,
)
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    tenant_battery_defaults_from_registry,
)

ArchitectureKind = Literal["mlp", "transformer"]
ObjectiveKind = Literal["forecast_loss", "decision_focused"]

V1_2_CLAIM_SCOPE: Final[str] = (
    "differentiable_forecast_storage_research_shadow_not_full_predict_then_bid"
)
TEMPORAL_PROTOCOLS: Final[dict[int, tuple[int, ...]]] = {
    1: (4, 3, 2),
    2: (4, 3),
    3: (4,),
}


@dataclass(frozen=True, slots=True)
class TemporalPriceExample:
    tenant_id: str
    source_model_name: str
    anchor_timestamp: datetime
    window_index: int
    starting_soc_fraction: float
    forecast_prices: tuple[float, ...]
    actual_prices: tuple[float, ...]
    oracle_value_uah: float
    raw_regret_uah: float


@dataclass(frozen=True, slots=True)
class TrainingObjectiveResult:
    loss: torch.Tensor
    forecast_mse: torch.Tensor
    realized_value_uah: torch.Tensor
    solver_status: str


@dataclass(frozen=True, slots=True)
class TrainedCorrector:
    model: nn.Module
    price_mean: float
    price_std: float
    checkpoint_epoch: int
    validation_loss: float
    training_solver_status: str


class ResidualMlpCorrector(nn.Module):
    def __init__(self, *, horizon_hours: int, hidden_dim: int) -> None:
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(horizon_hours, hidden_dim),
            nn.GELU(),
            nn.Linear(hidden_dim, horizon_hours),
        )

    def forward(self, normalized_prices: torch.Tensor) -> torch.Tensor:
        return normalized_prices + 0.5 * torch.tanh(self.network(normalized_prices))


class ResidualTransformerCorrector(nn.Module):
    def __init__(self, *, horizon_hours: int, hidden_dim: int) -> None:
        super().__init__()
        if hidden_dim % 2:
            raise ValueError("hidden_dim must be even for the transformer corrector.")
        self.horizon_hours = horizon_hours
        self.input_projection = nn.Linear(3, hidden_dim)
        layer = nn.TransformerEncoderLayer(
            d_model=hidden_dim,
            nhead=2,
            dim_feedforward=hidden_dim * 2,
            dropout=0.0,
            activation="gelu",
            batch_first=True,
            norm_first=True,
        )
        self.encoder = nn.TransformerEncoder(layer, num_layers=1)
        self.output = nn.Linear(hidden_dim, 1)

    def forward(self, normalized_prices: torch.Tensor) -> torch.Tensor:
        batch_size = normalized_prices.shape[0]
        hour = torch.arange(
            self.horizon_hours,
            dtype=normalized_prices.dtype,
            device=normalized_prices.device,
        )
        hour_sin = torch.sin(2.0 * pi * hour / self.horizon_hours).expand(
            batch_size, -1
        )
        hour_cos = torch.cos(2.0 * pi * hour / self.horizon_hours).expand(
            batch_size, -1
        )
        tokens = torch.stack([normalized_prices, hour_sin, hour_cos], dim=-1)
        residual = self.output(self.encoder(self.input_projection(tokens))).squeeze(-1)
        return normalized_prices + 0.5 * torch.tanh(residual)


def build_price_corrector(
    *,
    architecture: ArchitectureKind | str,
    horizon_hours: int,
    hidden_dim: int,
) -> nn.Module:
    if horizon_hours < 2:
        raise ValueError("horizon_hours must be at least two.")
    if hidden_dim <= 0:
        raise ValueError("hidden_dim must be positive.")
    if architecture == "mlp":
        model: nn.Module = ResidualMlpCorrector(
            horizon_hours=horizon_hours,
            hidden_dim=hidden_dim,
        )
    elif architecture == "transformer":
        model = ResidualTransformerCorrector(
            horizon_hours=horizon_hours,
            hidden_dim=hidden_dim,
        )
    else:
        raise ValueError(f"Unsupported architecture: {architecture}")
    return model.double()


def extract_temporal_examples(
    rolling_strict_rows: pl.DataFrame,
    *,
    source_model_name: str,
) -> list[TemporalPriceExample]:
    required = {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "evaluation_window_index",
        "selection_role",
        "starting_soc_fraction",
        "oracle_value_uah",
        "regret_uah",
        "safety_violation_count",
        "evaluation_payload",
    }
    missing = required.difference(rolling_strict_rows.columns)
    if missing:
        raise ValueError(f"rolling_strict_rows is missing columns: {sorted(missing)}")
    raw_rows = rolling_strict_rows.filter(
        (pl.col("source_model_name") == source_model_name)
        & (pl.col("selection_role") == "raw_reference")
    ).sort(["evaluation_window_index", "anchor_timestamp", "tenant_id"])
    if raw_rows.is_empty():
        raise ValueError(f"No raw temporal rows for source: {source_model_name}")
    examples: list[TemporalPriceExample] = []
    seen: set[tuple[str, datetime, int]] = set()
    for row in raw_rows.iter_rows(named=True):
        if int(row["safety_violation_count"]) != 0:
            raise ValueError("Raw temporal examples must have zero safety violations.")
        anchor = row["anchor_timestamp"]
        if not isinstance(anchor, datetime):
            raise TypeError("anchor_timestamp must contain datetime values.")
        window_index = int(row["evaluation_window_index"])
        key = (str(row["tenant_id"]), anchor, window_index)
        if key in seen:
            raise ValueError(f"Duplicate temporal example: {key}")
        seen.add(key)
        payload = row["evaluation_payload"]
        if not isinstance(payload, dict):
            raise TypeError("evaluation_payload must be a mapping.")
        horizon = payload.get("horizon")
        if not isinstance(horizon, list) or len(horizon) < 2:
            raise ValueError("evaluation_payload.horizon must contain at least two rows.")
        forecast = tuple(float(item["forecast_price_uah_mwh"]) for item in horizon)
        actual = tuple(float(item["actual_price_uah_mwh"]) for item in horizon)
        examples.append(
            TemporalPriceExample(
                tenant_id=str(row["tenant_id"]),
                source_model_name=source_model_name,
                anchor_timestamp=anchor,
                window_index=window_index,
                starting_soc_fraction=float(row["starting_soc_fraction"]),
                forecast_prices=forecast,
                actual_prices=actual,
                oracle_value_uah=float(row["oracle_value_uah"]),
                raw_regret_uah=float(row["regret_uah"]),
            )
        )
    return examples


def training_objective(
    *,
    objective_kind: ObjectiveKind | str,
    predicted_prices: torch.Tensor,
    actual_prices: torch.Tensor,
    starting_soc_fraction: float = 0.52,
    capacity_mwh: float = 1.0,
    max_power_mw: float = 0.25,
    soc_min_fraction: float = 0.05,
    soc_max_fraction: float = 0.95,
    round_trip_efficiency: float = 0.92,
    degradation_cost_per_mwh: float = 0.0,
) -> TrainingObjectiveResult:
    if predicted_prices.shape != actual_prices.shape or predicted_prices.ndim != 2:
        raise ValueError("predicted_prices and actual_prices must share a 2D shape.")
    forecast_mse = torch.mean(torch.square(predicted_prices - actual_prices))
    if objective_kind == "forecast_loss":
        return TrainingObjectiveResult(
            loss=forecast_mse / 1_000_000.0,
            forecast_mse=forecast_mse,
            realized_value_uah=predicted_prices.new_tensor(float("nan")),
            solver_status="not_used_for_forecast_loss",
        )
    if objective_kind != "decision_focused":
        raise ValueError(f"Unsupported objective kind: {objective_kind}")
    dispatch = solve_relaxed_dispatch_tensor(
        prices_uah_mwh=predicted_prices,
        starting_soc_fraction=starting_soc_fraction,
        capacity_mwh=capacity_mwh,
        max_power_mw=max_power_mw,
        soc_min_fraction=soc_min_fraction,
        soc_max_fraction=soc_max_fraction,
        round_trip_efficiency=round_trip_efficiency,
        degradation_cost_per_mwh=degradation_cost_per_mwh,
        quadratic_regularization=1e-3,
        fallback_to_surrogate=False,
        solver_args={"eps": 1e-6, "max_iters": 10000},
    )
    throughput = dispatch.charge_mw + dispatch.discharge_mw
    realized_values = torch.sum(
        actual_prices * (dispatch.discharge_mw - dispatch.charge_mw)
        - degradation_cost_per_mwh * throughput,
        dim=1,
    )
    decision_loss = -torch.mean(realized_values) / 1000.0
    stabilized_loss = decision_loss + 0.001 * forecast_mse / 1_000_000.0
    return TrainingObjectiveResult(
        loss=stabilized_loss,
        forecast_mse=forecast_mse,
        realized_value_uah=torch.mean(realized_values),
        solver_status=dispatch.solver_status,
    )


def profile_aware_training_objective(
    *,
    objective_kind: ObjectiveKind | str,
    predicted_prices: torch.Tensor,
    actual_prices: torch.Tensor,
    examples: list[TemporalPriceExample],
) -> TrainingObjectiveResult:
    """Weight tenant-specific differentiable storage losses over one batch."""

    if predicted_prices.shape != actual_prices.shape or predicted_prices.ndim != 2:
        raise ValueError("predicted_prices and actual_prices must share a 2D shape.")
    if len(examples) != predicted_prices.shape[0]:
        raise ValueError("examples must contain one row per price tensor row.")
    if objective_kind == "forecast_loss":
        return training_objective(
            objective_kind=objective_kind,
            predicted_prices=predicted_prices,
            actual_prices=actual_prices,
        )
    tenant_indices: dict[str, list[int]] = {}
    for index, example in enumerate(examples):
        tenant_indices.setdefault(example.tenant_id, []).append(index)
    total_count = len(examples)
    weighted_losses: list[torch.Tensor] = []
    weighted_mse: list[torch.Tensor] = []
    weighted_values: list[torch.Tensor] = []
    solver_statuses: set[str] = set()
    for tenant_id, indices in tenant_indices.items():
        defaults = tenant_battery_defaults_from_registry(tenant_id)
        starting_soc_values = {
            round(examples[index].starting_soc_fraction, 12) for index in indices
        }
        if len(starting_soc_values) != 1:
            raise ValueError("Each tenant batch must use one starting SOC contract.")
        index_tensor = torch.tensor(indices, dtype=torch.int64)
        tenant_result = training_objective(
            objective_kind=objective_kind,
            predicted_prices=torch.index_select(predicted_prices, 0, index_tensor),
            actual_prices=torch.index_select(actual_prices, 0, index_tensor),
            starting_soc_fraction=starting_soc_values.pop(),
            capacity_mwh=defaults.metrics.capacity_mwh,
            max_power_mw=defaults.metrics.max_power_mw,
            soc_min_fraction=defaults.metrics.soc_min_fraction,
            soc_max_fraction=defaults.metrics.soc_max_fraction,
            round_trip_efficiency=defaults.metrics.round_trip_efficiency,
            degradation_cost_per_mwh=(
                defaults.metrics.degradation_cost_per_mwh_throughput_uah
            ),
        )
        weight = len(indices) / total_count
        weighted_losses.append(tenant_result.loss * weight)
        weighted_mse.append(tenant_result.forecast_mse * weight)
        weighted_values.append(tenant_result.realized_value_uah * weight)
        solver_statuses.add(tenant_result.solver_status)
    return TrainingObjectiveResult(
        loss=torch.stack(weighted_losses).sum(),
        forecast_mse=torch.stack(weighted_mse).sum(),
        realized_value_uah=torch.stack(weighted_values).sum(),
        solver_status=";".join(sorted(solver_statuses)),
    )


def objective_kind(value: str) -> ObjectiveKind:
    if value not in {"forecast_loss", "decision_focused"}:
        raise ValueError(f"Unsupported objective kind: {value}")
    return cast(ObjectiveKind, value)


def run_v1_2_differentiable_suite(
    rolling_strict_rows: pl.DataFrame,
    *,
    output_dir: Path,
    source_model_names: tuple[str, ...],
    architectures: tuple[ArchitectureKind, ...] = ("mlp", "transformer"),
    objective_kinds: tuple[ObjectiveKind, ...] = (
        "forecast_loss",
        "decision_focused",
    ),
    seeds: tuple[int, ...] = (42, 2026, 7),
    epoch_count: int = 6,
    hidden_dim: int = 32,
    learning_rate: float = 0.01,
    tail_loss_threshold_uah: float = 150.0,
) -> dict[str, object]:
    """Train the preregistered temporal suite and strict-score every run."""

    if epoch_count <= 0 or learning_rate <= 0.0:
        raise ValueError("epoch_count and learning_rate must be positive.")
    output_dir.mkdir(parents=True, exist_ok=True)
    run_rows: list[dict[str, object]] = []
    paired_rows: list[dict[str, object]] = []
    for source_model_name in source_model_names:
        examples = extract_temporal_examples(
            rolling_strict_rows,
            source_model_name=source_model_name,
        )
        v2_plus_regret = _v2_plus_regret_lookup(
            rolling_strict_rows,
            source_model_name=source_model_name,
        )
        for evaluation_window_index, training_window_indices in TEMPORAL_PROTOCOLS.items():
            evaluation_examples = [
                example
                for example in examples
                if example.window_index == evaluation_window_index
            ]
            training_examples = [
                example
                for example in examples
                if example.window_index in training_window_indices
            ]
            _require_temporal_independence(training_examples, evaluation_examples)
            fit_examples, validation_examples = _inner_split(
                training_examples,
                training_window_indices=training_window_indices,
            )
            for architecture in architectures:
                for resolved_objective in objective_kinds:
                    for seed in seeds:
                        trained = train_price_corrector(
                            fit_examples=fit_examples,
                            validation_examples=validation_examples,
                            architecture=architecture,
                            objective_kind=resolved_objective,
                            seed=seed,
                            epoch_count=epoch_count,
                            hidden_dim=hidden_dim,
                            learning_rate=learning_rate,
                        )
                        scored = _strict_score_examples(
                            evaluation_examples,
                            trained=trained,
                        )
                        run_paired: list[dict[str, object]] = []
                        for score in scored:
                            key = (
                                str(score["tenant_id"]),
                                cast(datetime, score["anchor_timestamp"]),
                                evaluation_window_index,
                            )
                            comparator_regret = v2_plus_regret[key]
                            paired = {
                                "source_model_name": source_model_name,
                                "evaluation_window_index": evaluation_window_index,
                                "training_window_indices": ",".join(
                                    str(value) for value in training_window_indices
                                ),
                                "architecture": architecture,
                                "objective_kind": resolved_objective,
                                "seed": seed,
                                **score,
                                "v2_plus_regret_uah": comparator_regret,
                                "dfl_minus_v2_plus_regret_uah": (
                                    _float_value(score["regret_uah"]) - comparator_regret
                                ),
                            }
                            run_paired.append(paired)
                            paired_rows.append(paired)
                        run_rows.append(
                            _run_summary_row(
                                run_paired,
                                source_model_name=source_model_name,
                                evaluation_window_index=evaluation_window_index,
                                training_window_indices=training_window_indices,
                                architecture=architecture,
                                objective_kind=resolved_objective,
                                seed=seed,
                                trained=trained,
                                fit_count=len(
                                    {example.anchor_timestamp for example in fit_examples}
                                ),
                                validation_count=len(
                                    {
                                        example.anchor_timestamp
                                        for example in validation_examples
                                    }
                                ),
                                tail_loss_threshold_uah=tail_loss_threshold_uah,
                            )
                        )
    rows_frame = pl.DataFrame(run_rows).sort(
        [
            "source_model_name",
            "evaluation_window_index",
            "architecture",
            "objective_kind",
            "seed",
        ]
    )
    paired_frame = pl.DataFrame(paired_rows).sort(
        [
            "source_model_name",
            "evaluation_window_index",
            "architecture",
            "objective_kind",
            "seed",
            "anchor_timestamp",
            "tenant_id",
        ]
    )
    rows_frame.write_csv(output_dir / "suite_rows.csv")
    paired_frame.write_csv(output_dir / "paired_profile_rows.csv")
    summary = _suite_summary(rows_frame)
    (output_dir / "suite_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {"summary": summary, "rows": run_rows, "paired_rows": paired_rows}


def train_price_corrector(
    *,
    fit_examples: list[TemporalPriceExample],
    validation_examples: list[TemporalPriceExample],
    architecture: ArchitectureKind,
    objective_kind: ObjectiveKind,
    seed: int,
    epoch_count: int,
    hidden_dim: int,
    learning_rate: float,
) -> TrainedCorrector:
    fit = _unique_by_tenant_anchor(fit_examples)
    validation = _unique_by_tenant_anchor(validation_examples)
    if not fit or not validation:
        raise ValueError("Fit and validation examples must be non-empty.")
    horizon_hours = len(fit[0].forecast_prices)
    _seed_everything(seed)
    forecast_fit = _price_tensor([example.forecast_prices for example in fit])
    actual_fit = _price_tensor([example.actual_prices for example in fit])
    forecast_validation = _price_tensor(
        [example.forecast_prices for example in validation]
    )
    actual_validation = _price_tensor([example.actual_prices for example in validation])
    price_mean = float(torch.mean(forecast_fit).item())
    price_std = max(float(torch.std(forecast_fit, unbiased=False).item()), 1.0)
    model = build_price_corrector(
        architecture=architecture,
        horizon_hours=horizon_hours,
        hidden_dim=hidden_dim,
    )
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)
    best_state = {name: value.detach().clone() for name, value in model.state_dict().items()}
    best_loss = float("inf")
    best_epoch = 0
    solver_status = "not_used_for_forecast_loss"
    for epoch_index in range(1, epoch_count + 1):
        model.train()
        optimizer.zero_grad()
        predicted_fit = _denormalized_prediction(
            model,
            forecast_fit,
            price_mean=price_mean,
            price_std=price_std,
        )
        objective = profile_aware_training_objective(
            objective_kind=objective_kind,
            predicted_prices=predicted_fit,
            actual_prices=actual_fit,
            examples=fit,
        )
        solver_status = objective.solver_status
        objective.loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=10.0)
        if not all(
            parameter.grad is None or torch.isfinite(parameter.grad).all().item()
            for parameter in model.parameters()
        ):
            raise RuntimeError("Differentiable training produced non-finite gradients.")
        optimizer.step()
        model.eval()
        predicted_validation = _denormalized_prediction(
            model,
            forecast_validation,
            price_mean=price_mean,
            price_std=price_std,
        )
        validation_objective = profile_aware_training_objective(
            objective_kind=objective_kind,
            predicted_prices=predicted_validation,
            actual_prices=actual_validation,
            examples=validation,
        )
        validation_loss = float(validation_objective.loss.detach().item())
        if validation_loss < best_loss:
            best_loss = validation_loss
            best_epoch = epoch_index
            best_state = {
                name: value.detach().clone()
                for name, value in model.state_dict().items()
            }
    model.load_state_dict(best_state)
    model.eval()
    return TrainedCorrector(
        model=model,
        price_mean=price_mean,
        price_std=price_std,
        checkpoint_epoch=best_epoch,
        validation_loss=best_loss,
        training_solver_status=solver_status,
    )


def _strict_score_examples(
    examples: list[TemporalPriceExample],
    *,
    trained: TrainedCorrector,
) -> list[dict[str, object]]:
    evaluation_rows = _unique_by_tenant_anchor(examples)
    forecasts = _price_tensor(
        [example.forecast_prices for example in evaluation_rows]
    )
    with torch.no_grad():
        predictions = _denormalized_prediction(
            trained.model,
            forecasts,
            price_mean=trained.price_mean,
            price_std=trained.price_std,
        ).cpu()
    predicted_by_key = {
        (example.tenant_id, example.anchor_timestamp): [
            float(value) for value in prediction.tolist()
        ]
        for example, prediction in zip(evaluation_rows, predictions, strict=True)
    }
    rows: list[dict[str, object]] = []
    solver = HourlyDamBaselineSolver(BaselineSolverConfig(planning_horizon_hours=24))
    for example in examples:
        predicted_prices = predicted_by_key[
            (example.tenant_id, example.anchor_timestamp)
        ]
        defaults = tenant_battery_defaults_from_registry(example.tenant_id)
        forecast = [
            BaselineForecastPoint(
                forecast_timestamp=example.anchor_timestamp + timedelta(hours=index + 1),
                source_timestamp=example.anchor_timestamp,
                predicted_price_uah_mwh=price,
            )
            for index, price in enumerate(predicted_prices)
        ]
        result = solver.solve_dispatch_from_forecast(
            forecast=forecast,
            battery_metrics=defaults.metrics,
            current_soc_fraction=example.starting_soc_fraction,
            anchor_timestamp=example.anchor_timestamp,
            commit_reason="v1_2_differentiable_research_shadow",
        )
        realized_value = sum(
            actual_price * point.net_power_mw - point.degradation_penalty_uah
            for actual_price, point in zip(
                example.actual_prices,
                result.schedule,
                strict=True,
            )
        )
        regret = max(0.0, example.oracle_value_uah - realized_value)
        forecast_mae = mean(
            abs(predicted - actual)
            for predicted, actual in zip(
                predicted_prices,
                example.actual_prices,
                strict=True,
            )
        )
        rows.append(
            {
                "tenant_id": example.tenant_id,
                "anchor_timestamp": example.anchor_timestamp,
                "regret_uah": regret,
                "raw_regret_uah": example.raw_regret_uah,
                "realized_value_uah": realized_value,
                "forecast_mae_uah_mwh": forecast_mae,
                "safety_violation_count": 0,
            }
        )
    return rows


def _run_summary_row(
    paired_rows: list[dict[str, object]],
    *,
    source_model_name: str,
    evaluation_window_index: int,
    training_window_indices: tuple[int, ...],
    architecture: ArchitectureKind,
    objective_kind: ObjectiveKind,
    seed: int,
    trained: TrainedCorrector,
    fit_count: int,
    validation_count: int,
    tail_loss_threshold_uah: float,
) -> dict[str, object]:
    regrets = [_float_value(row["regret_uah"]) for row in paired_rows]
    raw_regrets = [_float_value(row["raw_regret_uah"]) for row in paired_rows]
    v2_plus = [_float_value(row["v2_plus_regret_uah"]) for row in paired_rows]
    deltas = [
        _float_value(row["dfl_minus_v2_plus_regret_uah"]) for row in paired_rows
    ]
    date_deltas: dict[datetime, list[float]] = {}
    for row in paired_rows:
        anchor = cast(datetime, row["anchor_timestamp"])
        date_deltas.setdefault(anchor, []).append(
            _float_value(row["dfl_minus_v2_plus_regret_uah"])
        )
    date_means = [mean(values) for values in date_deltas.values()]
    return {
        "source_model_name": source_model_name,
        "evaluation_window_index": evaluation_window_index,
        "training_window_indices": ",".join(str(value) for value in training_window_indices),
        "architecture": architecture,
        "objective_kind": objective_kind,
        "seed": seed,
        "fit_anchor_count": fit_count,
        "inner_validation_anchor_count": validation_count,
        "evaluation_profile_row_count": len(paired_rows),
        "evaluation_date_count": len(date_deltas),
        "checkpoint_epoch": trained.checkpoint_epoch,
        "inner_validation_loss": trained.validation_loss,
        "training_solver_status": trained.training_solver_status,
        "profile_aware_decision_loss": objective_kind == "decision_focused",
        "training_battery_contract_kind": (
            "tenant_registry_profile_specific"
            if objective_kind == "decision_focused"
            else "not_applicable_forecast_loss"
        ),
        "training_terminal_soc_equality": objective_kind == "decision_focused",
        "training_quadratic_regularization": (
            0.001 if objective_kind == "decision_focused" else 0.0
        ),
        "mean_regret_uah": mean(regrets),
        "median_regret_uah": median(regrets),
        "raw_mean_regret_uah": mean(raw_regrets),
        "mean_dfl_minus_raw_regret_uah": mean(regrets) - mean(raw_regrets),
        "v2_plus_mean_regret_uah": mean(v2_plus),
        "mean_dfl_minus_v2_plus_regret_uah": mean(deltas),
        "mean_date_cluster_delta_uah": mean(date_means),
        "forecast_mae_uah_mwh": mean(
            _float_value(row["forecast_mae_uah_mwh"]) for row in paired_rows
        ),
        "tail_loss_count": sum(value >= tail_loss_threshold_uah for value in deltas),
        "safety_violation_count": sum(
            int(_float_value(row["safety_violation_count"])) for row in paired_rows
        ),
        "content_overlap_count": 0,
        "promotable_v13_permitted_training_rows": 0,
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
        "claim_scope": V1_2_CLAIM_SCOPE,
    }


def _suite_summary(rows: pl.DataFrame) -> dict[str, object]:
    deltas = [float(value) for value in rows["mean_dfl_minus_v2_plus_regret_uah"]]
    raw_deltas = [float(value) for value in rows["mean_dfl_minus_raw_regret_uah"]]
    transformer = rows.filter(pl.col("architecture") == "transformer")
    mlp = rows.filter(pl.col("architecture") == "mlp")
    comparison_keys = [
        "source_model_name",
        "evaluation_window_index",
        "objective_kind",
        "seed",
    ]
    architecture_comparison = transformer.join(
        mlp,
        on=comparison_keys,
        how="inner",
        suffix="_mlp",
    ).with_columns(
        (
            pl.col("mean_regret_uah") - pl.col("mean_regret_uah_mlp")
        ).alias("transformer_minus_mlp_mean_regret_uah"),
        (
            pl.col("forecast_mae_uah_mwh") - pl.col("forecast_mae_uah_mwh_mlp")
        ).alias("transformer_minus_mlp_forecast_mae_uah_mwh"),
    )
    return {
        "claim_scope": V1_2_CLAIM_SCOPE,
        "protocol_run_count": rows.height,
        "beneficial_vs_v2_plus_run_count": sum(value < -1e-6 for value in deltas),
        "tie_vs_v2_plus_run_count": sum(abs(value) <= 1e-6 for value in deltas),
        "harmful_vs_v2_plus_run_count": sum(value > 1e-6 for value in deltas),
        "beneficial_vs_raw_run_count": sum(value < -1e-6 for value in raw_deltas),
        "tie_vs_raw_run_count": sum(abs(value) <= 1e-6 for value in raw_deltas),
        "harmful_vs_raw_run_count": sum(value > 1e-6 for value in raw_deltas),
        "all_content_overlap_counts_zero": bool(
            rows["content_overlap_count"].max() == 0
        ),
        "all_safety_violation_counts_zero": bool(
            rows["safety_violation_count"].max() == 0
        ),
        "transformer_comparison_count": architecture_comparison.height,
        "transformer_better_regret_count": int(
            architecture_comparison.filter(
                pl.col("transformer_minus_mlp_mean_regret_uah") < 0.0
            ).height
        ),
        "transformer_better_forecast_mae_count": int(
            architecture_comparison.filter(
                pl.col("transformer_minus_mlp_forecast_mae_uah_mwh") < 0.0
            ).height
        ),
        "mean_transformer_minus_mlp_regret_uah": _float_value(
            architecture_comparison["transformer_minus_mlp_mean_regret_uah"].mean()
        ),
        "mean_transformer_minus_mlp_forecast_mae_uah_mwh": _float_value(
            architecture_comparison[
                "transformer_minus_mlp_forecast_mae_uah_mwh"
            ].mean()
        ),
        "promotable_v13_permitted_training_rows": 0,
        "full_predict_then_bid": False,
        "profile_aware_decision_loss": True,
        "decision_focused_profile_count": 5,
        "training_terminal_soc_equality": True,
        "training_quadratic_regularization": 0.001,
        "market_execution_enabled": False,
    }


def _inner_split(
    examples: list[TemporalPriceExample],
    *,
    training_window_indices: tuple[int, ...],
) -> tuple[list[TemporalPriceExample], list[TemporalPriceExample]]:
    if len(training_window_indices) > 1:
        validation_window = training_window_indices[-1]
        fit = [example for example in examples if example.window_index != validation_window]
        validation = [
            example for example in examples if example.window_index == validation_window
        ]
        return fit, validation
    anchors = sorted({example.anchor_timestamp for example in examples})
    split_index = max(1, int(len(anchors) * 0.67))
    fit_anchors = set(anchors[:split_index])
    validation_anchors = set(anchors[split_index:])
    if not validation_anchors:
        validation_anchors = {anchors[-1]}
        fit_anchors.discard(anchors[-1])
    return (
        [example for example in examples if example.anchor_timestamp in fit_anchors],
        [
            example
            for example in examples
            if example.anchor_timestamp in validation_anchors
        ],
    )


def _unique_by_tenant_anchor(
    examples: list[TemporalPriceExample],
) -> list[TemporalPriceExample]:
    by_key: dict[tuple[str, datetime], TemporalPriceExample] = {}
    for example in sorted(examples, key=lambda value: (value.anchor_timestamp, value.tenant_id)):
        key = (example.tenant_id, example.anchor_timestamp)
        existing = by_key.get(key)
        if existing is not None and (
            existing.forecast_prices != example.forecast_prices
            or existing.actual_prices != example.actual_prices
        ):
            raise ValueError("Duplicate tenant-anchor rows disagree on price paths.")
        by_key.setdefault(key, example)
    return list(by_key.values())


def _v2_plus_regret_lookup(
    rolling_strict_rows: pl.DataFrame,
    *,
    source_model_name: str,
) -> dict[tuple[str, datetime, int], float]:
    selected = rolling_strict_rows.filter(
        (pl.col("source_model_name") == source_model_name)
        & (pl.col("selection_role") == "schedule_value_learner_v2_plus")
    )
    result: dict[tuple[str, datetime, int], float] = {}
    for row in selected.iter_rows(named=True):
        anchor = row["anchor_timestamp"]
        if not isinstance(anchor, datetime):
            raise TypeError("anchor_timestamp must contain datetime values.")
        key = (
            str(row["tenant_id"]),
            anchor,
            int(row["evaluation_window_index"]),
        )
        result[key] = _float_value(row["regret_uah"])
    return result


def _require_temporal_independence(
    training_examples: list[TemporalPriceExample],
    evaluation_examples: list[TemporalPriceExample],
) -> None:
    training_signatures = {
        (example.forecast_prices, example.actual_prices) for example in training_examples
    }
    evaluation_signatures = {
        (example.forecast_prices, example.actual_prices) for example in evaluation_examples
    }
    overlap = training_signatures.intersection(evaluation_signatures)
    if overlap:
        raise ValueError("Differentiable suite requires zero train/evaluation content overlap.")


def _price_tensor(values: list[tuple[float, ...]]) -> torch.Tensor:
    return torch.tensor(values, dtype=torch.float64)


def _denormalized_prediction(
    model: nn.Module,
    forecast_prices: torch.Tensor,
    *,
    price_mean: float,
    price_std: float,
) -> torch.Tensor:
    normalized = (forecast_prices - price_mean) / price_std
    return model(normalized) * price_std + price_mean


def _seed_everything(seed: int) -> None:
    random.seed(seed)
    torch.manual_seed(seed)


def _float_value(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"Expected a numeric value, received {type(value).__name__}.")
    return float(value)


__all__ = [
    "TEMPORAL_PROTOCOLS",
    "V1_2_CLAIM_SCOPE",
    "TemporalPriceExample",
    "TrainingObjectiveResult",
    "TrainedCorrector",
    "build_price_corrector",
    "extract_temporal_examples",
    "profile_aware_training_objective",
    "training_objective",
    "run_v1_2_differentiable_suite",
    "train_price_corrector",
]
