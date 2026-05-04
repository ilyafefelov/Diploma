"""Compact TFT-style forecast candidate with interpretable feature weights."""

from __future__ import annotations

from typing import Final

import polars as pl
import torch
from torch import nn
from torch.nn import functional as torch_functional

from smart_arbitrage.forecasting.nbeatsx import FeatureScaler, TargetScaler, _fit_feature_scaler, _fit_target_scaler
from smart_arbitrage.forecasting.neural_features import (
	NEURAL_FORECAST_FEATURE_COLUMNS,
	feature_matrix,
	forecast_feature_frame,
	target_vector,
	timestamp_vector,
	training_feature_frame,
)

TFT_MODEL_NAME: Final[str] = "tft_silver_v0"
TFT_TORCH_SEED: Final[int] = 2027
TFT_QUANTILES: Final[tuple[float, float, float]] = (0.1, 0.5, 0.9)


class TftSilverModel(nn.Module):
	"""Small variable-selection plus Transformer encoder model."""

	def __init__(self, input_dim: int, hidden_dim: int = 32) -> None:
		super().__init__()
		self.feature_selector = nn.Linear(input_dim, input_dim)
		self.input_projection = nn.Linear(input_dim, hidden_dim)
		encoder_layer = nn.TransformerEncoderLayer(
			d_model=hidden_dim,
			nhead=2,
			dim_feedforward=hidden_dim * 2,
			dropout=0.0,
			batch_first=True,
			activation="gelu",
		)
		self.temporal_encoder = nn.TransformerEncoder(encoder_layer, num_layers=1)
		self.quantile_head = nn.Linear(hidden_dim, 3)

	def forward(self, features: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
		feature_weights = torch.softmax(self.feature_selector(features), dim=-1)
		selected_features = features * feature_weights
		encoded = self.temporal_encoder(self.input_projection(selected_features))
		raw_quantiles = self.quantile_head(encoded)
		median = raw_quantiles[..., 1:2]
		lower_delta = torch_functional.softplus(raw_quantiles[..., 0:1])
		upper_delta = torch_functional.softplus(raw_quantiles[..., 2:3])
		ordered_quantiles = torch.cat([median - lower_delta, median, median + upper_delta], dim=-1)
		return ordered_quantiles, feature_weights


def build_tft_forecast(feature_frame: pl.DataFrame) -> pl.DataFrame:
	"""Train a small TFT-style model and emit horizon forecasts with feature weights."""

	train_frame = training_feature_frame(feature_frame)
	horizon_frame = forecast_feature_frame(feature_frame)
	if train_frame.height < 24:
		raise ValueError("TFT forecast requires at least 24 valid training rows.")

	feature_scaler = _fit_feature_scaler(feature_matrix(train_frame))
	target_scaler = _fit_target_scaler(target_vector(train_frame))
	model = _fit_tft_model(
		feature_scaler=feature_scaler,
		target_scaler=target_scaler,
		train_frame=train_frame,
	)
	with torch.no_grad():
		horizon_features = feature_scaler.transform(feature_matrix(horizon_frame)).unsqueeze(0)
		normalized_quantiles, feature_weights = model(horizon_features)
		denormalized_quantiles = target_scaler.inverse_transform(normalized_quantiles.squeeze(0)).clamp_min(1.0)
		weights = feature_weights.squeeze(0)

	top_feature_names: list[str] = []
	top_feature_weights: list[float] = []
	for row_weights in weights:
		top_index = int(torch.argmax(row_weights).item())
		top_feature_names.append(NEURAL_FORECAST_FEATURE_COLUMNS[top_index])
		top_feature_weights.append(round(float(row_weights[top_index].item()), 6))

	return pl.DataFrame(
		{
			"forecast_timestamp": timestamp_vector(horizon_frame),
			"model_name": [TFT_MODEL_NAME for _ in range(horizon_frame.height)],
			"predicted_price_p10_uah_mwh": [round(float(value), 4) for value in denormalized_quantiles[:, 0].tolist()],
			"predicted_price_p50_uah_mwh": [round(float(value), 4) for value in denormalized_quantiles[:, 1].tolist()],
			"predicted_price_p90_uah_mwh": [round(float(value), 4) for value in denormalized_quantiles[:, 2].tolist()],
			"top_feature_name": top_feature_names,
			"top_feature_weight": top_feature_weights,
			"feature_columns": [",".join(NEURAL_FORECAST_FEATURE_COLUMNS) for _ in range(horizon_frame.height)],
		}
	)


def _fit_tft_model(
	*,
	feature_scaler: FeatureScaler,
	target_scaler: TargetScaler,
	train_frame: pl.DataFrame,
) -> TftSilverModel:
	torch.manual_seed(TFT_TORCH_SEED)
	features = feature_scaler.transform(feature_matrix(train_frame)).unsqueeze(0)
	targets = target_scaler.transform(target_vector(train_frame)).squeeze(-1).unsqueeze(0)
	model = TftSilverModel(input_dim=features.shape[-1])
	optimizer = torch.optim.Adam(model.parameters(), lr=0.02)
	for _ in range(120):
		optimizer.zero_grad()
		quantile_predictions, _ = model(features)
		loss = _quantile_loss(quantile_predictions, targets)
		loss.backward()
		optimizer.step()
	return model


def _quantile_loss(predictions: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
	losses: list[torch.Tensor] = []
	for quantile_index, quantile in enumerate(TFT_QUANTILES):
		errors = targets - predictions[..., quantile_index]
		losses.append(torch.maximum((quantile - 1.0) * errors, quantile * errors).mean())
	return torch.stack(losses).mean()
