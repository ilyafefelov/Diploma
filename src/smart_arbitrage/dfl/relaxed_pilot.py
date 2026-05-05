"""Relaxed LP DFL pilot evaluation rows."""

from __future__ import annotations

from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.relaxed_dispatch import solve_relaxed_dispatch

REQUIRED_EVALUATION_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "evaluation_id",
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "starting_soc_fraction",
        "evaluation_payload",
    }
)


def build_relaxed_dfl_pilot_frame(
    evaluation_frame: pl.DataFrame,
    *,
    max_examples: int = 12,
    capacity_mwh: float = 1.0,
    max_power_mw: float = 0.25,
    soc_min_fraction: float = 0.05,
    soc_max_fraction: float = 0.95,
    degradation_cost_per_mwh: float = 0.0,
) -> pl.DataFrame:
    """Evaluate forecast rows through a differentiable relaxed LP and oracle relaxed LP."""

    _validate_inputs(evaluation_frame=evaluation_frame, max_examples=max_examples)
    rows: list[dict[str, Any]] = []
    for row in evaluation_frame.sort(["tenant_id", "anchor_timestamp", "forecast_model_name"]).iter_rows(named=True):
        if len(rows) >= max_examples:
            break
        horizon_rows = _horizon_rows(row["evaluation_payload"])
        forecast_prices = _horizon_values(horizon_rows, "forecast_price_uah_mwh")
        actual_prices = _horizon_values(horizon_rows, "actual_price_uah_mwh")
        if len(forecast_prices) < 2 or len(forecast_prices) != len(actual_prices):
            continue
        starting_soc_fraction = float(row["starting_soc_fraction"])
        forecast_dispatch = solve_relaxed_dispatch(
            prices_uah_mwh=forecast_prices,
            starting_soc_fraction=starting_soc_fraction,
            capacity_mwh=capacity_mwh,
            max_power_mw=max_power_mw,
            soc_min_fraction=soc_min_fraction,
            soc_max_fraction=soc_max_fraction,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        oracle_dispatch = solve_relaxed_dispatch(
            prices_uah_mwh=actual_prices,
            starting_soc_fraction=starting_soc_fraction,
            capacity_mwh=capacity_mwh,
            max_power_mw=max_power_mw,
            soc_min_fraction=soc_min_fraction,
            soc_max_fraction=soc_max_fraction,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        realized_value = _realized_value(
            actual_prices=actual_prices,
            charge_mw=forecast_dispatch.charge_mw,
            discharge_mw=forecast_dispatch.discharge_mw,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        oracle_value = _realized_value(
            actual_prices=actual_prices,
            charge_mw=oracle_dispatch.charge_mw,
            discharge_mw=oracle_dispatch.discharge_mw,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        rows.append(
            {
                "pilot_name": "relaxed_lp_dfl_v1",
                "evaluation_id": str(row["evaluation_id"]),
                "tenant_id": str(row["tenant_id"]),
                "forecast_model_name": str(row["forecast_model_name"]),
                "anchor_timestamp": row["anchor_timestamp"],
                "horizon_hours": len(forecast_prices),
                "relaxed_realized_value_uah": realized_value,
                "relaxed_oracle_value_uah": oracle_value,
                "relaxed_regret_uah": max(0.0, oracle_value - realized_value),
                "first_charge_mw": forecast_dispatch.charge_mw[0],
                "first_discharge_mw": forecast_dispatch.discharge_mw[0],
                "academic_scope": "differentiable_relaxed_lp_pilot_not_final_dfl",
            }
        )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


def _validate_inputs(*, evaluation_frame: pl.DataFrame, max_examples: int) -> None:
    if max_examples <= 0:
        raise ValueError("max_examples must be positive.")
    missing_columns = REQUIRED_EVALUATION_COLUMNS.difference(evaluation_frame.columns)
    if missing_columns:
        raise ValueError(f"evaluation_frame is missing required columns: {sorted(missing_columns)}")


def _horizon_rows(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    horizon = payload.get("horizon")
    if not isinstance(horizon, list):
        return []
    return [item for item in horizon if isinstance(item, dict)]


def _horizon_values(horizon_rows: list[dict[str, Any]], column_name: str) -> list[float]:
    return [float(row[column_name]) for row in horizon_rows if column_name in row]


def _realized_value(
    *,
    actual_prices: list[float],
    charge_mw: list[float],
    discharge_mw: list[float],
    degradation_cost_per_mwh: float,
) -> float:
    return float(
        sum(
            price * (discharge - charge) - degradation_cost_per_mwh * (charge + discharge)
            for price, charge, discharge in zip(actual_prices, charge_mw, discharge_mw, strict=True)
        )
    )
