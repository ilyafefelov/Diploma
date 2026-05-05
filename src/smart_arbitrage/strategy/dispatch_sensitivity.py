"""Forecast-to-dispatch diagnostics for Gold benchmark rows."""

from __future__ import annotations

from typing import Any

import polars as pl


def build_forecast_dispatch_sensitivity_frame(evaluation_frame: pl.DataFrame) -> pl.DataFrame:
    """Build diagnostic rows that explain forecast error, spread error, and LP sensitivity."""

    _validate_evaluation_frame(evaluation_frame)
    rows = [_sensitivity_row(row) for row in evaluation_frame.iter_rows(named=True)]
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(["tenant_id", "anchor_timestamp", "forecast_model_name"])


def _sensitivity_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = _payload(row)
    forecast_diagnostics = _forecast_diagnostics(payload)
    horizon_rows = _horizon_rows(payload)
    spread_diagnostics = _dispatch_spread_diagnostics(horizon_rows)
    forecast_mae_uah_mwh = float(forecast_diagnostics.get("mae_uah_mwh", _mean_abs_forecast_error(horizon_rows)))
    regret_uah = float(row["regret_uah"])
    return {
        "diagnostic_id": f"{row['evaluation_id']}:sensitivity",
        "evaluation_id": str(row["evaluation_id"]),
        "tenant_id": str(row["tenant_id"]),
        "forecast_model_name": str(row["forecast_model_name"]),
        "strategy_kind": str(row["strategy_kind"]),
        "market_venue": str(row["market_venue"]),
        "anchor_timestamp": row["anchor_timestamp"],
        "generated_at": row["generated_at"],
        "horizon_hours": int(row["horizon_hours"]),
        "decision_value_uah": float(row["decision_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": regret_uah,
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": str(row["committed_action"]),
        "committed_power_mw": float(row["committed_power_mw"]),
        "rank_by_regret": int(row["rank_by_regret"]),
        "forecast_mae_uah_mwh": forecast_mae_uah_mwh,
        "forecast_rmse_uah_mwh": float(forecast_diagnostics.get("rmse_uah_mwh", 0.0)),
        "directional_accuracy": float(forecast_diagnostics.get("directional_accuracy", 0.0)),
        "spread_ranking_quality": float(forecast_diagnostics.get("spread_ranking_quality", 0.0)),
        "top_k_price_recall": float(forecast_diagnostics.get("top_k_price_recall", 0.0)),
        "price_cap_violation_count": float(forecast_diagnostics.get("price_cap_violation_count", 0.0)),
        "mean_forecast_price_uah_mwh": float(forecast_diagnostics.get("mean_forecast_price_uah_mwh", 0.0)),
        "mean_actual_price_uah_mwh": float(forecast_diagnostics.get("mean_actual_price_uah_mwh", 0.0)),
        "mean_forecast_error_uah_mwh": _mean_forecast_error(horizon_rows),
        "forecast_dispatch_spread_uah_mwh": spread_diagnostics["forecast_dispatch_spread_uah_mwh"],
        "realized_dispatch_spread_uah_mwh": spread_diagnostics["realized_dispatch_spread_uah_mwh"],
        "dispatch_spread_error_uah_mwh": spread_diagnostics["dispatch_spread_error_uah_mwh"],
        "charge_energy_mwh": spread_diagnostics["charge_energy_mwh"],
        "discharge_energy_mwh": spread_diagnostics["discharge_energy_mwh"],
        "data_quality_tier": str(payload.get("data_quality_tier", "demo_grade")),
        "diagnostic_bucket": _diagnostic_bucket(
            regret_uah=regret_uah,
            forecast_mae_uah_mwh=forecast_mae_uah_mwh,
            dispatch_spread_error_uah_mwh=spread_diagnostics["dispatch_spread_error_uah_mwh"],
        ),
    }


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload")
    if not isinstance(payload, dict):
        raise ValueError("evaluation_payload must be a dictionary.")
    return payload


def _forecast_diagnostics(payload: dict[str, Any]) -> dict[str, Any]:
    forecast_diagnostics = payload.get("forecast_diagnostics")
    if isinstance(forecast_diagnostics, dict):
        return forecast_diagnostics
    return {}


def _horizon_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list):
        raise ValueError("evaluation_payload must include a horizon list.")
    rows = [row for row in horizon if isinstance(row, dict)]
    if not rows:
        raise ValueError("evaluation_payload horizon must contain rows.")
    return rows


def _dispatch_spread_diagnostics(horizon_rows: list[dict[str, Any]]) -> dict[str, float]:
    charge_forecast_prices: list[tuple[float, float]] = []
    charge_actual_prices: list[tuple[float, float]] = []
    discharge_forecast_prices: list[tuple[float, float]] = []
    discharge_actual_prices: list[tuple[float, float]] = []
    for horizon_row in horizon_rows:
        net_power_mw = float(horizon_row.get("net_power_mw", 0.0))
        if net_power_mw < 0.0:
            energy_mwh = abs(net_power_mw)
            charge_forecast_prices.append((float(horizon_row["forecast_price_uah_mwh"]), energy_mwh))
            charge_actual_prices.append((float(horizon_row["actual_price_uah_mwh"]), energy_mwh))
        elif net_power_mw > 0.0:
            energy_mwh = net_power_mw
            discharge_forecast_prices.append((float(horizon_row["forecast_price_uah_mwh"]), energy_mwh))
            discharge_actual_prices.append((float(horizon_row["actual_price_uah_mwh"]), energy_mwh))

    forecast_dispatch_spread = _dispatch_spread(
        discharge_prices=discharge_forecast_prices,
        charge_prices=charge_forecast_prices,
    )
    realized_dispatch_spread = _dispatch_spread(
        discharge_prices=discharge_actual_prices,
        charge_prices=charge_actual_prices,
    )
    return {
        "charge_energy_mwh": sum(energy for _, energy in charge_actual_prices),
        "discharge_energy_mwh": sum(energy for _, energy in discharge_actual_prices),
        "forecast_dispatch_spread_uah_mwh": forecast_dispatch_spread,
        "realized_dispatch_spread_uah_mwh": realized_dispatch_spread,
        "dispatch_spread_error_uah_mwh": abs(forecast_dispatch_spread - realized_dispatch_spread),
    }


def _dispatch_spread(
    *,
    discharge_prices: list[tuple[float, float]],
    charge_prices: list[tuple[float, float]],
) -> float:
    if not discharge_prices or not charge_prices:
        return 0.0
    return _weighted_mean(discharge_prices) - _weighted_mean(charge_prices)


def _weighted_mean(values: list[tuple[float, float]]) -> float:
    total_weight = sum(weight for _, weight in values)
    if total_weight <= 0.0:
        return 0.0
    return sum(value * weight for value, weight in values) / total_weight


def _mean_forecast_error(horizon_rows: list[dict[str, Any]]) -> float:
    return sum(
        float(row["actual_price_uah_mwh"]) - float(row["forecast_price_uah_mwh"])
        for row in horizon_rows
    ) / len(horizon_rows)


def _mean_abs_forecast_error(horizon_rows: list[dict[str, Any]]) -> float:
    return sum(
        abs(float(row["actual_price_uah_mwh"]) - float(row["forecast_price_uah_mwh"]))
        for row in horizon_rows
    ) / len(horizon_rows)


def _diagnostic_bucket(
    *,
    regret_uah: float,
    forecast_mae_uah_mwh: float,
    dispatch_spread_error_uah_mwh: float,
) -> str:
    if regret_uah < 250.0:
        return "low_regret"
    if forecast_mae_uah_mwh >= 1000.0:
        return "forecast_error"
    if dispatch_spread_error_uah_mwh >= 250.0:
        return "spread_objective_mismatch"
    return "lp_dispatch_sensitivity"


def _validate_evaluation_frame(evaluation_frame: pl.DataFrame) -> None:
    required_columns = {
        "evaluation_id",
        "tenant_id",
        "forecast_model_name",
        "strategy_kind",
        "market_venue",
        "anchor_timestamp",
        "generated_at",
        "horizon_hours",
        "decision_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "regret_ratio",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "committed_action",
        "committed_power_mw",
        "rank_by_regret",
        "evaluation_payload",
    }
    missing_columns = required_columns.difference(evaluation_frame.columns)
    if missing_columns:
        raise ValueError(f"evaluation_frame is missing required columns: {sorted(missing_columns)}")
