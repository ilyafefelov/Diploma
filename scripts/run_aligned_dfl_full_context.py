"""Run the preregistered full-context aligned DFL comparison.

The input is the source-backed experimental Poland context frame produced by
``build_aligned_dfl_context_frame``. The script compares one transformer under
forecast loss and warm-started hybrid forecast-plus-decision loss. It keeps the
last 18 anchors of every tenant untouched until final strict-LP evaluation.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import pickle
import random
from statistics import mean
from typing import Sequence

import numpy as np
import polars as pl
import torch

from smart_arbitrage.assets.gold.baseline_solver import (
    BaselineForecastPoint,
    BaselineSolverConfig,
    HourlyDamBaselineSolver,
)
from smart_arbitrage.dfl.aligned_differentiable_dfl import (
    ALIGNED_DFL_FEATURE_COLUMNS,
    AlignedDflTransformer,
    build_aligned_dfl_context_tensor,
    hybrid_forecast_decision_loss,
    warm_start_hybrid_transformer,
)
from smart_arbitrage.dfl.differentiable_forecast_v1_2 import TemporalPriceExample
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    tenant_battery_defaults_from_registry,
)


def _examples(frame: pl.DataFrame) -> list[TemporalPriceExample]:
    return [
        TemporalPriceExample(
            tenant_id=str(row["tenant_id"]),
            source_model_name=str(row["source_model_name"]),
            anchor_timestamp=row["anchor_timestamp"],
            window_index=0,
            starting_soc_fraction=float(row["starting_soc_fraction"]),
            forecast_prices=tuple(float(value) for value in row["forecast_p50_uah_mwh"]),
            actual_prices=tuple(float(value) for value in row["actual_price_uah_mwh_vector"]),
            oracle_value_uah=float(row["oracle_value_uah"]),
            raw_regret_uah=float(row["raw_regret_uah"]),
        )
        for row in frame.iter_rows(named=True)
    ]


def _split(frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    groups = [part.sort("anchor_timestamp") for part in frame.partition_by("tenant_id", as_dict=False)]
    if any(part.height < 365 for part in groups):
        raise ValueError("Every tenant requires at least 365 chronological anchors.")
    train = pl.concat([part.head(part.height - 46) for part in groups])
    validation = pl.concat([part.slice(part.height - 46, 28) for part in groups])
    test = pl.concat([part.tail(18) for part in groups])
    return train, validation, test


def _tensors(frame: pl.DataFrame, feature_mean: torch.Tensor, feature_std: torch.Tensor, target_mean: torch.Tensor, target_std: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
    context = torch.tensor(build_aligned_dfl_context_tensor(frame).features, dtype=torch.float64)
    actual = torch.tensor(frame["actual_price_uah_mwh_vector"].to_list(), dtype=torch.float64)
    return (context - feature_mean) / feature_std, (actual - target_mean) / target_std


def _predict(model: AlignedDflTransformer, features: torch.Tensor, target_mean: torch.Tensor, target_std: torch.Tensor) -> torch.Tensor:
    return model(features) * target_std + target_mean


def _strict_regrets(examples: list[TemporalPriceExample], predictions: torch.Tensor) -> list[float]:
    solver = HourlyDamBaselineSolver(BaselineSolverConfig(planning_horizon_hours=24))
    regrets: list[float] = []
    for example, prediction in zip(examples, predictions.tolist(), strict=True):
        defaults = tenant_battery_defaults_from_registry(example.tenant_id)
        forecast = [BaselineForecastPoint(forecast_timestamp=example.anchor_timestamp + timedelta(hours=index + 1), source_timestamp=example.anchor_timestamp, predicted_price_uah_mwh=float(price)) for index, price in enumerate(prediction)]
        dispatch = solver.solve_dispatch_from_forecast(forecast=forecast, battery_metrics=defaults.metrics, current_soc_fraction=example.starting_soc_fraction, anchor_timestamp=example.anchor_timestamp, commit_reason="aligned_dfl_full_context_research")
        value = sum(actual * point.net_power_mw - point.degradation_penalty_uah for actual, point in zip(example.actual_prices, dispatch.schedule, strict=True))
        regrets.append(max(0.0, example.oracle_value_uah - value))
    return regrets


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--forecast-epochs", type=int, default=100)
    parser.add_argument("--hybrid-epochs", type=int, default=30)
    parser.add_argument("--hidden-dim", type=int, default=32)
    parser.add_argument("--seed", type=int, default=20260713)
    args = parser.parse_args(argv)
    random.seed(args.seed); np.random.seed(args.seed); torch.manual_seed(args.seed)
    with args.input.open("rb") as handle:
        frame = pickle.load(handle)
    if not isinstance(frame, pl.DataFrame):
        raise TypeError("--input must contain a Polars DataFrame.")
    train, validation, test = _split(frame)
    train_context = torch.tensor(build_aligned_dfl_context_tensor(train).features, dtype=torch.float64)
    train_actual = torch.tensor(train["actual_price_uah_mwh_vector"].to_list(), dtype=torch.float64)
    feature_mean, feature_std = train_context.mean((0, 1), keepdim=True), train_context.std((0, 1), keepdim=True).clamp_min(1.0)
    target_mean, target_std = train_actual.mean(), train_actual.std().clamp_min(1.0)
    train_x, train_y = _tensors(train, feature_mean, feature_std, target_mean, target_std)
    val_x, val_y = _tensors(validation, feature_mean, feature_std, target_mean, target_std)
    test_x, _ = _tensors(test, feature_mean, feature_std, target_mean, target_std)
    forecast = AlignedDflTransformer(feature_count=len(ALIGNED_DFL_FEATURE_COLUMNS), hidden_dim=args.hidden_dim).double()
    optimizer = torch.optim.Adam(forecast.parameters(), lr=0.003)
    best_state, best_loss, patience = deepcopy(forecast.state_dict()), float("inf"), 0
    for _ in range(args.forecast_epochs):
        optimizer.zero_grad(); loss=torch.mean(torch.square(forecast(train_x)-train_y)); loss.backward(); optimizer.step()
        with torch.no_grad(): val_loss=float(torch.mean(torch.square(forecast(val_x)-val_y)).item())
        if val_loss < best_loss: best_loss, best_state, patience = val_loss, deepcopy(forecast.state_dict()), 0
        else: patience += 1
        if patience >= 15: break
    forecast.load_state_dict(best_state); forecast.eval()
    train_examples, val_examples, test_examples = _examples(train), _examples(validation), _examples(test)
    candidates: list[tuple[float, float, dict[str, torch.Tensor], float]] = []
    for weight in (0.1, 0.3, 0.5):
        for smooth in (0.0, 0.01):
            hybrid=AlignedDflTransformer(feature_count=len(ALIGNED_DFL_FEATURE_COLUMNS), hidden_dim=args.hidden_dim).double(); warm_start_hybrid_transformer(forecast_model=forecast, hybrid_model=hybrid)
            opt=torch.optim.Adam(hybrid.parameters(), lr=0.001); best_h, best_h_loss, stalled=deepcopy(hybrid.state_dict()),float("inf"),0
            for _ in range(args.hybrid_epochs):
                opt.zero_grad(); result=hybrid_forecast_decision_loss(predicted_prices=_predict(hybrid,train_x,target_mean,target_std),actual_prices=train_actual,examples=train_examples,hybrid_weight=weight,smoothing_weight=smooth); result.loss.backward(); torch.nn.utils.clip_grad_norm_(hybrid.parameters(),10.0); opt.step()
                with torch.no_grad(): score=float(hybrid_forecast_decision_loss(predicted_prices=_predict(hybrid,val_x,target_mean,target_std),actual_prices=torch.tensor(validation['actual_price_uah_mwh_vector'].to_list(),dtype=torch.float64),examples=val_examples,hybrid_weight=weight,smoothing_weight=smooth).loss.item())
                if score < best_h_loss: best_h,best_h_loss,stalled=deepcopy(hybrid.state_dict()),score,0
                else: stalled += 1
                if stalled >= 10: break
            candidates.append((weight,smooth,best_h,best_h_loss))
    weight, smooth, state, validation_loss=min(candidates,key=lambda item:item[3])
    hybrid=AlignedDflTransformer(feature_count=len(ALIGNED_DFL_FEATURE_COLUMNS), hidden_dim=args.hidden_dim).double(); hybrid.load_state_dict(state); hybrid.eval()
    with torch.no_grad(): forecast_test=_predict(forecast,test_x,target_mean,target_std); hybrid_test=_predict(hybrid,test_x,target_mean,target_std)
    forecast_regrets, hybrid_regrets = _strict_regrets(test_examples,forecast_test), _strict_regrets(test_examples,hybrid_test)
    result={"protocol":"aligned_full_context_experimental_poland","split_per_tenant":{"train":319,"validation":28,"test":18},"forecast_epochs_limit":args.forecast_epochs,"hybrid_epochs_limit":args.hybrid_epochs,"selected_hybrid_weight":weight,"selected_smoothing_weight":smooth,"validation_loss":validation_loss,"strict_test":{"forecast_mean_regret_uah":mean(forecast_regrets),"hybrid_mean_regret_uah":mean(hybrid_regrets),"hybrid_minus_forecast_uah":mean(hybrid_regrets)-mean(forecast_regrets),"test_profile_rows":len(test_examples)},"market_execution_enabled":False,"claim_scope":"aligned_dfl_experimental_poland_not_official_headline"}
    args.output.parent.mkdir(parents=True,exist_ok=True); args.output.write_text(json.dumps(result,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print(json.dumps(result,sort_keys=True)); return 0


if __name__ == "__main__":
    raise SystemExit(main())
