"""DFL-ready training frame builders from real-data benchmark evaluations."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import polars as pl

from smart_arbitrage.market_rules import market_rule_features


def build_dfl_training_frame(
    evaluation_frame: pl.DataFrame,
    *,
    require_thesis_grade: bool = True,
) -> pl.DataFrame:
    """Flatten forecast-to-LP benchmark rows into value-oriented training examples."""

    _validate_evaluation_frame(evaluation_frame)
    rows: list[dict[str, Any]] = []
    for row in evaluation_frame.sort(["tenant_id", "anchor_timestamp", "forecast_model_name"]).iter_rows(named=True):
        payload = _payload(row)
        data_quality_tier = str(payload.get("data_quality_tier", "demo_grade"))
        if require_thesis_grade and data_quality_tier != "thesis_grade":
            raise ValueError("DFL training frame requires thesis_grade benchmark rows.")
        anchor_timestamp = row["anchor_timestamp"]
        if not isinstance(anchor_timestamp, datetime):
            raise TypeError("anchor_timestamp must be a datetime value.")
        diagnostics = _mapping(payload.get("forecast_diagnostics"))
        horizon_rows = _horizon_rows(payload)
        rule_features = market_rule_features(venue="DAM", timestamp=anchor_timestamp)
        rows.append(
            {
                "training_example_id": _training_example_id(row),
                "evaluation_id": str(row["evaluation_id"]),
                "tenant_id": str(row["tenant_id"]),
                "anchor_timestamp": anchor_timestamp,
                "forecast_model_name": str(row["forecast_model_name"]),
                "strategy_kind": str(row["strategy_kind"]),
                "market_venue": str(row["market_venue"]),
                "horizon_hours": int(row["horizon_hours"]),
                "starting_soc_fraction": float(row["starting_soc_fraction"]),
                "starting_soc_source": str(row["starting_soc_source"]),
                "lp_committed_action": str(row["committed_action"]),
                "lp_committed_power_mw": float(row["committed_power_mw"]),
                "first_action_net_power_mw": _first_action_net_power(horizon_rows),
                "decision_value_uah": float(row["decision_value_uah"]),
                "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
                "oracle_value_uah": float(row["oracle_value_uah"]),
                "regret_uah": float(row["regret_uah"]),
                "regret_ratio": float(row["regret_ratio"]),
                "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
                "total_throughput_mwh": float(row["total_throughput_mwh"]),
                "efc_proxy": _float_payload(payload, "efc_proxy", default=0.0),
                "mean_forecast_price_uah_mwh": _mean_horizon_value(horizon_rows, "forecast_price_uah_mwh"),
                "mean_actual_price_uah_mwh": _mean_horizon_value(horizon_rows, "actual_price_uah_mwh"),
                "forecast_mae_uah_mwh": _float_mapping(diagnostics, "mae_uah_mwh"),
                "forecast_rmse_uah_mwh": _float_mapping(diagnostics, "rmse_uah_mwh"),
                "forecast_smape": _float_mapping(diagnostics, "smape"),
                "directional_accuracy": _float_mapping(diagnostics, "directional_accuracy"),
                "spread_ranking_quality": _float_mapping(diagnostics, "spread_ranking_quality"),
                "top_k_price_recall": _float_mapping(diagnostics, "top_k_price_recall"),
                "training_weight": 1.0 + max(0.0, float(row["regret_ratio"])),
                "data_quality_tier": data_quality_tier,
                "observed_coverage_ratio": _float_payload(payload, "observed_coverage_ratio", default=0.0),
                "market_price_cap_max": float(rule_features["market_price_cap_max"]),
                "market_price_cap_min": float(rule_features["market_price_cap_min"]),
                "market_regime_id": str(rule_features["market_regime_id"]),
                "days_since_regime_change": float(rule_features["days_since_regime_change"]),
                "is_price_cap_changed_recently": float(rule_features["is_price_cap_changed_recently"]),
            }
        )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


def _validate_evaluation_frame(evaluation_frame: pl.DataFrame) -> None:
    required_columns = {
        "evaluation_id",
        "tenant_id",
        "forecast_model_name",
        "strategy_kind",
        "market_venue",
        "anchor_timestamp",
        "horizon_hours",
        "starting_soc_fraction",
        "starting_soc_source",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "regret_ratio",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "committed_action",
        "committed_power_mw",
        "evaluation_payload",
    }
    missing_columns = required_columns.difference(evaluation_frame.columns)
    if missing_columns:
        raise ValueError(f"evaluation_frame is missing required columns: {sorted(missing_columns)}")


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row["evaluation_payload"]
    if not isinstance(payload, dict):
        raise ValueError("evaluation_payload must be a mapping.")
    return payload


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _horizon_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list):
        return []
    return [item for item in horizon if isinstance(item, dict)]


def _first_action_net_power(horizon_rows: list[dict[str, Any]]) -> float:
    if not horizon_rows:
        return 0.0
    return float(horizon_rows[0].get("net_power_mw", 0.0))


def _mean_horizon_value(horizon_rows: list[dict[str, Any]], column_name: str) -> float:
    values = [float(row[column_name]) for row in horizon_rows if column_name in row]
    if not values:
        return 0.0
    return sum(values) / len(values)


def _float_mapping(mapping: dict[str, Any], key: str) -> float:
    value = mapping.get(key, 0.0)
    return float(value)


def _float_payload(payload: dict[str, Any], key: str, *, default: float) -> float:
    value = payload.get(key, default)
    return float(value)


def _training_example_id(row: dict[str, Any]) -> str:
    anchor_timestamp = row["anchor_timestamp"]
    anchor_slug = anchor_timestamp.strftime("%Y%m%dT%H%M") if isinstance(anchor_timestamp, datetime) else str(anchor_timestamp)
    return f"{row['tenant_id']}:{row['forecast_model_name']}:{anchor_slug}"
