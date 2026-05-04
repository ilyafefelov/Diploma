"""Small NBEATSx-style forecast candidate for the Silver layer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import polars as pl
import torch
from torch import nn

from smart_arbitrage.forecasting.neural_features import (
	NEURAL_FORECAST_FEATURE_COLUMNS,
	feature_matrix,
	forecast_feature_frame,
	target_vector,
	timestamp_vector,
	training_feature_frame,
)

NBEATSX_MODEL_NAME: Final[str] = "nbeatsx_silver_v0"
NBEATSX_TORCH_SEED: Final[int] = 2026


@dataclass(frozen=True, slots=True)
class FeatureScaler:
	means: list[float]
	scales: list[float]

	def transform(self, values: list[list[float]]) -> torch.Tensor:
		if not values:
			raise ValueError("values must contain at least one row.")
		tensor = torch.tensor(values, dtype=torch.float32)
		means = torch.tensor(self.means, dtype=torch.float32)
		scales = torch.tensor(self.scales, dtype=torch.float32)
		return (tensor - means) / scales


@dataclass(frozen=True, slots=True)
class TargetScaler:
	mean: float
	scale: float

	def transform(self, values: list[float]) -> torch.Tensor:
		if not values:
			raise ValueError("values must contain at least one target.")
		return (torch.tensor(values, dtype=torch.float32).unsqueeze(-1) - self.mean) / self.scale

	def inverse_transform(self, values: torch.Tensor) -> torch.Tensor:
		return (values * self.scale) + self.mean


class NBEATSxSilverModel(nn.Module):
	"""Compact decomposition model: trend stack plus exogenous stack."""

	def __init__(self, input_dim: int, hidden_dim: int = 32) -> None:
		super().__init__()
		self.trend_stack = nn.Sequential(
			nn.Linear(input_dim, hidden_dim),
			nn.ReLU(),
			nn.Linear(hidden_dim, 1),
		)
		self.exogenous_stack = nn.Sequential(
			nn.Linear(input_dim, hidden_dim),
			nn.ReLU(),
			nn.Linear(hidden_dim, 1),
		)

	def forward(self, features: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
		trend_component = self.trend_stack(features)
		exogenous_component = self.exogenous_stack(features)
		return trend_component + exogenous_component, trend_component, exogenous_component


def build_nbeatsx_forecast(feature_frame: pl.DataFrame) -> pl.DataFrame:
	"""Train a small NBEATSx-style model and emit 24-hour forecast rows."""

	train_frame = training_feature_frame(feature_frame)
	horizon_frame = forecast_feature_frame(feature_frame)
	if train_frame.height < 24:
		raise ValueError("NBEATSx forecast requires at least 24 valid training rows.")

	feature_scaler = _fit_feature_scaler(feature_matrix(train_frame))
	target_scaler = _fit_target_scaler(target_vector(train_frame))
	model = _fit_nbeatsx_model(
		features=feature_scaler.transform(feature_matrix(train_frame)),
		targets=target_scaler.transform(target_vector(train_frame)),
	)
	with torch.no_grad():
		predictions, trend_components, exogenous_components = model(feature_scaler.transform(feature_matrix(horizon_frame)))
		predicted_prices = target_scaler.inverse_transform(predictions).squeeze(-1).clamp_min(1.0)
		trend_prices = target_scaler.inverse_transform(trend_components).squeeze(-1)
		exogenous_prices = (exogenous_components.squeeze(-1) * target_scaler.scale)

	return pl.DataFrame(
		{
			"forecast_timestamp": timestamp_vector(horizon_frame),
			"model_name": [NBEATSX_MODEL_NAME for _ in range(horizon_frame.height)],
			"predicted_price_uah_mwh": [round(float(value), 4) for value in predicted_prices.tolist()],
			"trend_component_uah_mwh": [round(float(value), 4) for value in trend_prices.tolist()],
			"exogenous_component_uah_mwh": [round(float(value), 4) for value in exogenous_prices.tolist()],
			"feature_columns": [",".join(NEURAL_FORECAST_FEATURE_COLUMNS) for _ in range(horizon_frame.height)],
		}
	)


def _fit_nbeatsx_model(*, features: torch.Tensor, targets: torch.Tensor) -> NBEATSxSilverModel:
	torch.manual_seed(NBEATSX_TORCH_SEED)
	model = NBEATSxSilverModel(input_dim=features.shape[1])
	optimizer = torch.optim.Adam(model.parameters(), lr=0.03)
	loss_function = nn.MSELoss()
	for _ in range(160):
		optimizer.zero_grad()
		predictions, _, _ = model(features)
		loss = loss_function(predictions, targets)
		loss.backward()
		optimizer.step()
	return model


def _fit_feature_scaler(values: list[list[float]]) -> FeatureScaler:
	tensor = torch.tensor(values, dtype=torch.float32)
	means = tensor.mean(dim=0)
	scales = tensor.std(dim=0).clamp_min(1.0)
	return FeatureScaler(
		means=[float(value) for value in means.tolist()],
		scales=[float(value) for value in scales.tolist()],
	)


def _fit_target_scaler(values: list[float]) -> TargetScaler:
	tensor = torch.tensor(values, dtype=torch.float32)
	return TargetScaler(
		mean=float(tensor.mean().item()),
		scale=float(tensor.std().clamp_min(1.0).item()),
	)
