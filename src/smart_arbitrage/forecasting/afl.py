"""Arbitrage-focused learning evidence panels for forecast hardening."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any, Final

import polars as pl

AFL_PANEL_CLAIM_SCOPE: Final[str] = "arbitrage_focused_learning_panel_not_full_dfl"
FORECAST_FORENSICS_CLAIM_SCOPE: Final[str] = "forecast_candidate_forensics_not_full_dfl"

_MODEL_METADATA: Final[dict[str, dict[str, str]]] = {
    "strict_similar_day": {
        "model_family": "Strict Similar-Day",
        "candidate_kind": "frozen_control_comparator",
        "implementation_scope": "level_1_baseline_forecast_control",
    },
    "nbeatsx_silver_v0": {
        "model_family": "NBEATSx",
        "candidate_kind": "compact_silver_candidate",
        "implementation_scope": "compact_in_repo_nbeatsx_style_candidate",
    },
    "tft_silver_v0": {
        "model_family": "TFT",
        "candidate_kind": "compact_silver_candidate",
        "implementation_scope": "compact_in_repo_tft_style_candidate",
    },
    "nbeatsx_official_v0": {
        "model_family": "NBEATSx",
        "candidate_kind": "official_backend_smoke_readiness",
        "implementation_scope": "optional_neuralforecast_adapter_not_default_benchmark",
    },
    "tft_official_v0": {
        "model_family": "TFT",
        "candidate_kind": "official_backend_smoke_readiness",
        "implementation_scope": "optional_pytorch_forecasting_adapter_not_default_benchmark",
    },
}


def build_forecast_candidate_forensics_frame(benchmark_frame: pl.DataFrame) -> pl.DataFrame:
    """Summarize what each forecast candidate currently represents."""

    _validate_benchmark_frame(benchmark_frame)
    rows: list[dict[str, Any]] = []
    for model_name in sorted(benchmark_frame["forecast_model_name"].unique().to_list()):
        model_frame = benchmark_frame.filter(pl.col("forecast_model_name") == model_name)
        diagnostics = [_forecast_diagnostics(row["evaluation_payload"]) for row in model_frame.iter_rows(named=True)]
        metadata = _model_metadata(str(model_name))
        rows.append(
            {
                "forecast_model_name": str(model_name),
                **metadata,
                "row_count": model_frame.height,
                "tenant_count": model_frame.select("tenant_id").n_unique(),
                "anchor_count": model_frame.select(["tenant_id", "anchor_timestamp"]).unique().height,
                "mean_regret_uah": _column_mean(model_frame, "regret_uah"),
                "median_regret_uah": _column_median(model_frame, "regret_uah"),
                "mean_mae_uah_mwh": _mean([_float_or_zero(item.get("mae_uah_mwh")) for item in diagnostics]),
                "mean_top_k_price_recall": _mean([_float_or_zero(item.get("top_k_price_recall")) for item in diagnostics]),
                "mean_spread_ranking_quality": _mean(
                    [_float_or_zero(item.get("spread_ranking_quality")) for item in diagnostics]
                ),
                "claim_scope": FORECAST_FORENSICS_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(rows).sort("forecast_model_name")


def build_afl_training_panel_frame(
    benchmark_frame: pl.DataFrame,
    *,
    final_holdout_anchor_count_per_tenant: int = 18,
    weather_context_frame: pl.DataFrame | None = None,
    tenant_historical_net_load_frame: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Build a no-leakage sidecar panel for arbitrage-focused forecast learning.

    ``feature_*`` columns use only current identity/time or anchors strictly before
    the row anchor. ``label_*`` columns may use realized horizon outcomes and are
    therefore training/evaluation labels only.
    """

    if final_holdout_anchor_count_per_tenant <= 0:
        raise ValueError("final_holdout_anchor_count_per_tenant must be positive.")
    _validate_benchmark_frame(benchmark_frame)
    rows = sorted(
        list(benchmark_frame.iter_rows(named=True)),
        key=lambda row: (str(row["tenant_id"]), _datetime(row["anchor_timestamp"]), str(row["forecast_model_name"])),
    )
    final_anchors_by_tenant = _final_anchors_by_tenant(
        rows,
        final_holdout_anchor_count_per_tenant=final_holdout_anchor_count_per_tenant,
    )
    regrets_by_tenant_model = _prior_regrets_by_tenant_model(rows)
    strict_regrets_by_tenant = _prior_strict_regrets_by_tenant(rows)
    weather_context_rows = _context_rows_by_tenant_timestamp(
        weather_context_frame,
        required_numeric_columns=(
            "weather_temperature",
            "weather_wind_speed",
            "weather_cloudcover",
            "weather_effective_solar",
        ),
        frame_name="weather_context_frame",
    )
    net_load_context_rows = _context_rows_by_tenant_timestamp(
        tenant_historical_net_load_frame,
        required_numeric_columns=(
            "load_mw",
            "pv_estimate_mw",
            "net_load_mw",
            "btm_battery_power_mw",
        ),
        frame_name="tenant_historical_net_load_frame",
    )

    output_rows: list[dict[str, Any]] = []
    for row in rows:
        tenant_id = str(row["tenant_id"])
        model_name = str(row["forecast_model_name"])
        anchor_timestamp = _datetime(row["anchor_timestamp"])
        payload = _payload(row)
        _validate_payload(payload)
        horizon = _horizon_rows(payload, expected_horizon_hours=int(row["horizon_hours"]))
        diagnostics = _forecast_diagnostics(payload)
        model_prior_regrets = [
            regret
            for prior_anchor, regret in regrets_by_tenant_model[(tenant_id, model_name)]
            if prior_anchor < anchor_timestamp
        ]
        strict_prior_regrets = [
            regret
            for prior_anchor, regret in strict_regrets_by_tenant[tenant_id]
            if prior_anchor < anchor_timestamp
        ]
        prior_model_mean = _mean(model_prior_regrets)
        prior_strict_mean = _mean(strict_prior_regrets)
        forecast_values = [float(item["forecast_price_uah_mwh"]) for item in horizon]
        actual_values = [float(item["actual_price_uah_mwh"]) for item in horizon]
        net_power_values = [float(item.get("net_power_mw", 0.0)) for item in horizon]
        actual_spread = _spread(actual_values)
        metadata = _model_metadata(model_name)
        weather_features = _prior_weather_features(
            weather_context_rows,
            tenant_id=tenant_id,
            anchor_timestamp=anchor_timestamp,
        )
        net_load_features = _prior_net_load_features(
            net_load_context_rows,
            tenant_id=tenant_id,
            anchor_timestamp=anchor_timestamp,
        )
        output_rows.append(
            {
                "tenant_id": tenant_id,
                "forecast_model_name": model_name,
                **metadata,
                "market_venue": str(row["market_venue"]),
                "anchor_timestamp": anchor_timestamp,
                "generated_at": row.get("generated_at"),
                "split": "final_holdout"
                if anchor_timestamp in final_anchors_by_tenant[tenant_id]
                else "train_selection",
                "horizon_hours": int(row["horizon_hours"]),
                "feature_anchor_hour": float(anchor_timestamp.hour),
                "feature_anchor_weekday": float(anchor_timestamp.isoweekday()),
                "feature_prior_model_anchor_count": len(model_prior_regrets),
                "feature_prior_strict_anchor_count": len(strict_prior_regrets),
                "feature_prior_mean_model_regret_uah": prior_model_mean,
                "feature_prior_mean_strict_regret_uah": prior_strict_mean,
                "feature_prior_regret_advantage_vs_strict_uah": prior_strict_mean - prior_model_mean,
                "feature_forecast_price_spread_uah_mwh": _spread(forecast_values),
                "feature_forecast_active_hour_count": sum(1 for value in net_power_values if abs(value) > 1e-9),
                **weather_features,
                **net_load_features,
                "label_regret_uah": float(row["regret_uah"]),
                "label_regret_ratio": float(row["regret_ratio"]),
                "label_decision_value_uah": float(row["decision_value_uah"]),
                "label_oracle_value_uah": float(row["oracle_value_uah"]),
                "label_total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
                "label_total_throughput_mwh": float(row["total_throughput_mwh"]),
                "label_actual_price_spread_uah_mwh": actual_spread,
                "label_decision_weight_uah": max(1.0, float(row["regret_uah"]) + actual_spread),
                "diagnostic_mae_uah_mwh": _float_or_zero(diagnostics.get("mae_uah_mwh")),
                "diagnostic_rmse_uah_mwh": _float_or_zero(diagnostics.get("rmse_uah_mwh")),
                "diagnostic_top_k_price_recall": _float_or_zero(diagnostics.get("top_k_price_recall")),
                "diagnostic_spread_ranking_quality": _float_or_zero(diagnostics.get("spread_ranking_quality")),
                "diagnostic_forecast_top3_bottom3_rank_overlap": _top_bottom_overlap(forecast_values, actual_values),
                "claim_scope": AFL_PANEL_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(output_rows).sort(["tenant_id", "anchor_timestamp", "forecast_model_name"])


def _validate_benchmark_frame(frame: pl.DataFrame) -> None:
    required_columns = {
        "tenant_id",
        "forecast_model_name",
        "market_venue",
        "anchor_timestamp",
        "horizon_hours",
        "decision_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "regret_ratio",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "evaluation_payload",
    }
    missing_columns = required_columns.difference(frame.columns)
    if missing_columns:
        raise ValueError(f"benchmark_frame is missing required columns: {sorted(missing_columns)}")
    if frame.height == 0:
        raise ValueError("benchmark_frame must contain rows.")


def _final_anchors_by_tenant(
    rows: list[dict[str, Any]],
    *,
    final_holdout_anchor_count_per_tenant: int,
) -> dict[str, set[datetime]]:
    anchors_by_tenant: dict[str, set[datetime]] = defaultdict(set)
    for row in rows:
        anchors_by_tenant[str(row["tenant_id"])].add(_datetime(row["anchor_timestamp"]))
    return {
        tenant_id: set(sorted(anchors)[-final_holdout_anchor_count_per_tenant:])
        for tenant_id, anchors in anchors_by_tenant.items()
    }


def _prior_regrets_by_tenant_model(rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[tuple[datetime, float]]]:
    values: dict[tuple[str, str], list[tuple[datetime, float]]] = defaultdict(list)
    for row in rows:
        values[(str(row["tenant_id"]), str(row["forecast_model_name"]))].append(
            (_datetime(row["anchor_timestamp"]), float(row["regret_uah"]))
        )
    return values


def _prior_strict_regrets_by_tenant(rows: list[dict[str, Any]]) -> dict[str, list[tuple[datetime, float]]]:
    values: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
    for row in rows:
        if str(row["forecast_model_name"]) == "strict_similar_day":
            values[str(row["tenant_id"])].append((_datetime(row["anchor_timestamp"]), float(row["regret_uah"])))
    return values


def _validate_payload(payload: dict[str, Any]) -> None:
    if payload.get("data_quality_tier") != "thesis_grade":
        raise ValueError("AFL training panel requires thesis_grade benchmark rows.")
    if float(payload.get("observed_coverage_ratio", 0.0)) < 1.0:
        raise ValueError("AFL training panel requires fully observed benchmark rows.")


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload")
    if not isinstance(payload, dict):
        raise ValueError("evaluation_payload must be a dictionary.")
    return payload


def _forecast_diagnostics(payload: dict[str, Any]) -> dict[str, Any]:
    diagnostics = payload.get("forecast_diagnostics", {})
    return diagnostics if isinstance(diagnostics, dict) else {}


def _horizon_rows(payload: dict[str, Any], *, expected_horizon_hours: int) -> list[dict[str, Any]]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list):
        raise ValueError("evaluation_payload must contain horizon rows.")
    rows = [item for item in horizon if isinstance(item, dict)]
    if len(rows) != expected_horizon_hours:
        raise ValueError("horizon row count must match horizon_hours.")
    return rows


def _model_metadata(model_name: str) -> dict[str, str]:
    return _MODEL_METADATA.get(
        model_name,
        {
            "model_family": "Unknown",
            "candidate_kind": "unclassified_research_candidate",
            "implementation_scope": "unclassified_forecast_candidate",
        },
    )


def _spread(values: list[float]) -> float:
    return max(values) - min(values) if values else 0.0


def _top_bottom_overlap(forecast_values: list[float], actual_values: list[float]) -> float:
    if not forecast_values or not actual_values:
        return 0.0
    k = min(3, len(forecast_values), len(actual_values))
    forecast_top_bottom = set(_top_k_indices(forecast_values, k=k) + _bottom_k_indices(forecast_values, k=k))
    actual_top_bottom = set(_top_k_indices(actual_values, k=k) + _bottom_k_indices(actual_values, k=k))
    denominator = len(actual_top_bottom)
    return len(forecast_top_bottom.intersection(actual_top_bottom)) / denominator if denominator else 0.0


def _context_rows_by_tenant_timestamp(
    frame: pl.DataFrame | None,
    *,
    required_numeric_columns: tuple[str, ...],
    frame_name: str,
) -> dict[tuple[str, datetime], dict[str, Any]]:
    if frame is None or frame.height == 0:
        return {}
    required_columns = {"tenant_id", "timestamp", *required_numeric_columns}
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"{frame_name} is missing required columns: {missing_columns}")
    if frame_name == "tenant_historical_net_load_frame":
        _validate_net_load_claim_flags(frame)
    rows: dict[tuple[str, datetime], dict[str, Any]] = {}
    for row in frame.iter_rows(named=True):
        timestamp = row["timestamp"]
        if not isinstance(timestamp, datetime):
            raise TypeError(f"{frame_name}.timestamp must contain datetime values.")
        rows[(str(row["tenant_id"]), _naive_utc_datetime(timestamp))] = row
    return rows


def _validate_net_load_claim_flags(frame: pl.DataFrame) -> None:
    required_columns = {"claim_scope", "not_full_dfl", "not_market_execution"}
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"tenant_historical_net_load_frame is missing required columns: {missing_columns}")
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != "tenant_historical_net_load_configured_proxy":
            raise ValueError("tenant_historical_net_load_frame must remain a configured proxy.")
        if not bool(row["not_full_dfl"]) or not bool(row["not_market_execution"]):
            raise ValueError("tenant_historical_net_load_frame must remain research-only evidence.")


def _prior_weather_features(
    rows: dict[tuple[str, datetime], dict[str, Any]],
    *,
    tenant_id: str,
    anchor_timestamp: datetime,
) -> dict[str, float]:
    prior_rows = _prior_context_rows(rows, tenant_id=tenant_id, anchor_timestamp=anchor_timestamp)
    return {
        "feature_prior_weather_context_row_count": float(len(prior_rows)),
        "feature_prior_weather_temperature_mean": _mean(
            [_float_or_zero(row.get("weather_temperature")) for row in prior_rows]
        ),
        "feature_prior_weather_wind_speed_mean": _mean(
            [_float_or_zero(row.get("weather_wind_speed")) for row in prior_rows]
        ),
        "feature_prior_weather_cloudcover_mean": _mean(
            [_float_or_zero(row.get("weather_cloudcover")) for row in prior_rows]
        ),
        "feature_prior_weather_effective_solar_mean": _mean(
            [_float_or_zero(row.get("weather_effective_solar")) for row in prior_rows]
        ),
    }


def _prior_net_load_features(
    rows: dict[tuple[str, datetime], dict[str, Any]],
    *,
    tenant_id: str,
    anchor_timestamp: datetime,
) -> dict[str, float]:
    prior_rows = _prior_context_rows(rows, tenant_id=tenant_id, anchor_timestamp=anchor_timestamp)
    return {
        "feature_prior_net_load_context_row_count": float(len(prior_rows)),
        "feature_prior_load_mean_mw": _mean([_float_or_zero(row.get("load_mw")) for row in prior_rows]),
        "feature_prior_pv_estimate_mean_mw": _mean(
            [_float_or_zero(row.get("pv_estimate_mw")) for row in prior_rows]
        ),
        "feature_prior_net_load_mean_mw": _mean(
            [_float_or_zero(row.get("net_load_mw")) for row in prior_rows]
        ),
        "feature_prior_btm_battery_support_mean_mw": _mean(
            [_float_or_zero(row.get("btm_battery_power_mw")) for row in prior_rows]
        ),
        "feature_prior_load_context_configured_proxy": 1.0 if prior_rows else 0.0,
    }


def _prior_context_rows(
    rows: dict[tuple[str, datetime], dict[str, Any]],
    *,
    tenant_id: str,
    anchor_timestamp: datetime,
    lookback_hours: int = 24,
) -> list[dict[str, Any]]:
    normalized_anchor = _naive_utc_datetime(anchor_timestamp)
    window_start = normalized_anchor - timedelta(hours=lookback_hours)
    return [
        row
        for (row_tenant_id, row_timestamp), row in rows.items()
        if row_tenant_id == tenant_id and window_start <= row_timestamp <= normalized_anchor
    ]


def _top_k_indices(values: list[float], *, k: int) -> list[int]:
    return [
        index
        for index, _ in sorted(enumerate(values), key=lambda item: (item[1], -item[0]), reverse=True)[:k]
    ]


def _bottom_k_indices(values: list[float], *, k: int) -> list[int]:
    return [
        index
        for index, _ in sorted(enumerate(values), key=lambda item: (item[1], item[0]))[:k]
    ]


def _datetime(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("anchor_timestamp must contain datetime values.")
    return value


def _naive_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _column_mean(frame: pl.DataFrame, column_name: str) -> float:
    value = frame.select(column_name).mean().item()
    return float(value) if value is not None else 0.0


def _column_median(frame: pl.DataFrame, column_name: str) -> float:
    value = frame.select(column_name).median().item()
    return float(value) if value is not None else 0.0


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _float_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
