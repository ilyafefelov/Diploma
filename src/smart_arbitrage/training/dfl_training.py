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
        horizon_rows = _horizon_rows(payload)
        diagnostics = _forecast_diagnostics(payload, horizon_rows)
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


def _forecast_diagnostics(payload: dict[str, Any], horizon_rows: list[dict[str, Any]]) -> dict[str, Any]:
    diagnostics = _mapping(payload.get("forecast_diagnostics"))
    fallback = _forecast_diagnostics_from_horizon(horizon_rows)
    return {
        "mae_uah_mwh": diagnostics.get("mae_uah_mwh", fallback["mae_uah_mwh"]),
        "rmse_uah_mwh": diagnostics.get("rmse_uah_mwh", fallback["rmse_uah_mwh"]),
        "smape": diagnostics.get("smape", fallback["smape"]),
        "directional_accuracy": diagnostics.get("directional_accuracy", fallback["directional_accuracy"]),
        "spread_ranking_quality": diagnostics.get("spread_ranking_quality", fallback["spread_ranking_quality"]),
        "top_k_price_recall": diagnostics.get("top_k_price_recall", fallback["top_k_price_recall"]),
    }


def _forecast_diagnostics_from_horizon(horizon_rows: list[dict[str, Any]]) -> dict[str, float]:
    forecast_values = _horizon_values(horizon_rows, "forecast_price_uah_mwh")
    actual_values = _horizon_values(horizon_rows, "actual_price_uah_mwh")
    if len(forecast_values) != len(actual_values) or not forecast_values:
        return {
            "mae_uah_mwh": 0.0,
            "rmse_uah_mwh": 0.0,
            "smape": 0.0,
            "directional_accuracy": 0.0,
            "spread_ranking_quality": 0.0,
            "top_k_price_recall": 0.0,
        }
    errors = [
        forecast_value - actual_value
        for forecast_value, actual_value in zip(forecast_values, actual_values)
    ]
    return {
        "mae_uah_mwh": _mean([abs(error) for error in errors]),
        "rmse_uah_mwh": _mean([error**2 for error in errors]) ** 0.5,
        "smape": _smape(forecast_values=forecast_values, actual_values=actual_values),
        "directional_accuracy": _directional_accuracy(
            forecast_values=forecast_values,
            actual_values=actual_values,
        ),
        "spread_ranking_quality": _rank_correlation(forecast_values, actual_values),
        "top_k_price_recall": _top_k_price_recall(
            forecast_values=forecast_values,
            actual_values=actual_values,
            k=min(3, len(actual_values)),
        ),
    }


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
    values = _horizon_values(horizon_rows, column_name)
    if not values:
        return 0.0
    return sum(values) / len(values)


def _horizon_values(horizon_rows: list[dict[str, Any]], column_name: str) -> list[float]:
    return [float(row[column_name]) for row in horizon_rows if column_name in row]


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


def _smape(*, forecast_values: list[float], actual_values: list[float]) -> float:
    values: list[float] = []
    for forecast_value, actual_value in zip(forecast_values, actual_values):
        denominator = abs(forecast_value) + abs(actual_value)
        values.append(0.0 if denominator <= 1e-9 else (2.0 * abs(forecast_value - actual_value)) / denominator)
    return _mean(values)


def _directional_accuracy(*, forecast_values: list[float], actual_values: list[float]) -> float:
    if len(forecast_values) < 2:
        return 0.0
    matches = 0
    comparisons = 0
    for index in range(1, len(forecast_values)):
        matches += 1 if _sign(forecast_values[index] - forecast_values[index - 1]) == _sign(actual_values[index] - actual_values[index - 1]) else 0
        comparisons += 1
    return matches / comparisons if comparisons else 0.0


def _rank_correlation(forecast_values: list[float], actual_values: list[float]) -> float:
    if len(forecast_values) < 2:
        return 0.0
    forecast_ranks = _ordinal_ranks(forecast_values)
    actual_ranks = _ordinal_ranks(actual_values)
    forecast_mean = _mean(forecast_ranks)
    actual_mean = _mean(actual_ranks)
    numerator = sum(
        (forecast_rank - forecast_mean) * (actual_rank - actual_mean)
        for forecast_rank, actual_rank in zip(forecast_ranks, actual_ranks)
    )
    forecast_scale = sum((forecast_rank - forecast_mean) ** 2 for forecast_rank in forecast_ranks)
    actual_scale = sum((actual_rank - actual_mean) ** 2 for actual_rank in actual_ranks)
    denominator = (forecast_scale * actual_scale) ** 0.5
    return 0.0 if denominator <= 1e-9 else numerator / denominator


def _top_k_price_recall(*, forecast_values: list[float], actual_values: list[float], k: int) -> float:
    if k <= 0:
        return 0.0
    forecast_top = set(_top_k_indices(forecast_values, k=k))
    actual_top = set(_top_k_indices(actual_values, k=k))
    return len(forecast_top.intersection(actual_top)) / k


def _top_k_indices(values: list[float], *, k: int) -> list[int]:
    return [
        index
        for index, _ in sorted(
            enumerate(values),
            key=lambda item: (item[1], -item[0]),
            reverse=True,
        )[:k]
    ]


def _ordinal_ranks(values: list[float]) -> list[float]:
    ranked = sorted(enumerate(values), key=lambda item: (item[1], item[0]))
    ranks = [0.0 for _ in values]
    for rank, (index, _) in enumerate(ranked, start=1):
        ranks[index] = float(rank)
    return ranks


def _sign(value: float) -> int:
    if value > 0.0:
        return 1
    if value < 0.0:
        return -1
    return 0


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0
