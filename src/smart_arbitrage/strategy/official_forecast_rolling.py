"""Rolling-origin strict LP evidence for official forecast adapters."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Final

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import (
    DEFAULT_PRICE_COLUMN,
    DEFAULT_TIMESTAMP_COLUMN,
    HourlyDamBaselineSolver,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome
from smart_arbitrage.forecasting.neural_features import (
    DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    MIN_NEURAL_FORECAST_TRAIN_ROWS,
    build_neural_forecast_feature_frame,
)
from smart_arbitrage.forecasting.official_adapters import (
    build_official_nbeatsx_forecast,
    build_official_tft_forecast,
)
from smart_arbitrage.forecasting.sota_training import build_sota_forecast_training_frame
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    evaluate_forecast_candidates_against_oracle,
    tenant_battery_defaults_from_registry,
)

OFFICIAL_FORECAST_ROLLING_ORIGIN_STRATEGY_KIND: Final[str] = (
    "official_forecast_rolling_origin_benchmark"
)
OFFICIAL_FORECAST_ROLLING_ORIGIN_CLAIM_SCOPE: Final[str] = (
    "official_forecast_rolling_origin_benchmark_not_full_dfl"
)
OFFICIAL_MODEL_NAMES: Final[tuple[str, ...]] = (
    "strict_similar_day",
    "nbeatsx_official_v0",
    "tft_official_v0",
)
OfficialForecastBuilder = Callable[..., pl.DataFrame]


def build_official_forecast_rolling_origin_benchmark_frame(
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    max_eval_anchors_per_tenant: int = 2,
    horizon_hours: int = DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    nbeatsx_max_steps: int = 100,
    nbeatsx_random_seed: int = 20260511,
    tft_max_epochs: int = 15,
    tft_batch_size: int = 32,
    tft_learning_rate: float = 0.005,
    tft_hidden_size: int = 12,
    tft_hidden_continuous_size: int = 6,
    require_observed_source_rows: bool = True,
    generated_at: datetime | None = None,
    nbeatsx_builder: OfficialForecastBuilder = build_official_nbeatsx_forecast,
    tft_builder: OfficialForecastBuilder = build_official_tft_forecast,
) -> pl.DataFrame:
    """Train official adapters on prior rows and strict-score their forecast schedules."""

    _validate_config(
        tenant_ids=tenant_ids,
        max_eval_anchors_per_tenant=max_eval_anchors_per_tenant,
        horizon_hours=horizon_hours,
    )
    resolved_generated_at = generated_at or datetime.now(UTC)
    rows: list[pl.DataFrame] = []
    for tenant_id in tenant_ids:
        tenant_price_history = _tenant_price_history(
            real_data_benchmark_silver_feature_frame,
            tenant_id=tenant_id,
            require_observed_source_rows=require_observed_source_rows,
        )
        anchors = _daily_anchors(
            tenant_price_history,
            max_anchors=max_eval_anchors_per_tenant,
            horizon_hours=horizon_hours,
        )
        defaults = tenant_battery_defaults_from_registry(tenant_id)
        for anchor_timestamp in anchors:
            window = _window_for_anchor(
                tenant_price_history,
                anchor_timestamp=anchor_timestamp,
                horizon_hours=horizon_hours,
            )
            feature_frame = build_neural_forecast_feature_frame(
                window,
                horizon_hours=horizon_hours,
                future_weather_mode="forecast_only",
            )
            training_frame = build_sota_forecast_training_frame(
                feature_frame,
                tenant_id=tenant_id,
            )
            nbeatsx_forecast = nbeatsx_builder(
                training_frame,
                horizon_hours=horizon_hours,
                max_steps=nbeatsx_max_steps,
                random_seed=nbeatsx_random_seed,
            )
            tft_forecast = tft_builder(
                training_frame,
                horizon_hours=horizon_hours,
                max_epochs=tft_max_epochs,
                batch_size=tft_batch_size,
                learning_rate=tft_learning_rate,
                hidden_size=tft_hidden_size,
                hidden_continuous_size=tft_hidden_continuous_size,
            )
            if nbeatsx_forecast.is_empty() or tft_forecast.is_empty():
                continue
            strict_forecast = _strict_forecast_frame(window, anchor_timestamp=anchor_timestamp)
            evaluation = evaluate_forecast_candidates_against_oracle(
                price_history=window,
                tenant_id=tenant_id,
                battery_metrics=defaults.metrics,
                starting_soc_fraction=defaults.initial_soc_fraction,
                starting_soc_source="tenant_default",
                anchor_timestamp=anchor_timestamp,
                candidates=[
                    ForecastCandidate(
                        model_name="strict_similar_day",
                        forecast_frame=strict_forecast,
                        point_prediction_column="predicted_price_uah_mwh",
                    ),
                    ForecastCandidate(
                        model_name="nbeatsx_official_v0",
                        forecast_frame=nbeatsx_forecast,
                        point_prediction_column="predicted_price_uah_mwh",
                    ),
                    ForecastCandidate(
                        model_name="tft_official_v0",
                        forecast_frame=tft_forecast,
                        point_prediction_column="predicted_price_uah_mwh",
                    ),
                ],
                evaluation_id=(
                    f"{tenant_id}:official-rolling:{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
                ),
                generated_at=resolved_generated_at,
            )
            rows.append(
                _with_official_metadata(
                    evaluation,
                    tenant_id=tenant_id,
                    anchor_timestamp=anchor_timestamp,
                    window=window,
                    nbeatsx_max_steps=nbeatsx_max_steps,
                    tft_max_epochs=tft_max_epochs,
                )
            )
    if not rows:
        return pl.DataFrame()
    return pl.concat(rows, how="diagonal_relaxed").sort(
        ["tenant_id", "anchor_timestamp", "rank_by_regret", "forecast_model_name"]
    )


def validate_official_forecast_rolling_origin_evidence(
    frame: pl.DataFrame,
    *,
    min_tenant_count: int = 1,
    min_anchor_count_per_tenant: int = 1,
) -> EvidenceCheckOutcome:
    """Validate official rolling evidence claim boundaries and coverage."""

    required_columns = {
        "tenant_id",
        "forecast_model_name",
        "strategy_kind",
        "anchor_timestamp",
        "evaluation_payload",
    }
    failures = _missing_column_failures(frame, required_columns)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": frame.height})
    if frame.height == 0:
        return EvidenceCheckOutcome(False, "official rolling benchmark has no rows", {"row_count": 0})
    rows = list(frame.iter_rows(named=True))
    tenant_ids = sorted({str(row["tenant_id"]) for row in rows})
    models = sorted({str(row["forecast_model_name"]) for row in rows})
    anchor_counts = {
        tenant_id: len(
            {
                _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
                for row in rows
                if str(row["tenant_id"]) == tenant_id
            }
        )
        for tenant_id in tenant_ids
    }
    payloads = [_payload(row) for row in rows]
    data_quality_tiers = sorted({str(payload.get("data_quality_tier", "demo_grade")) for payload in payloads})
    observed_min = min(float(payload.get("observed_coverage_ratio", 0.0)) for payload in payloads)
    claim_failures = [
        payload
        for payload in payloads
        if str(payload.get("claim_scope")) != OFFICIAL_FORECAST_ROLLING_ORIGIN_CLAIM_SCOPE
        or not bool(payload.get("not_full_dfl", False))
        or not bool(payload.get("not_market_execution", False))
    ]
    if len(tenant_ids) < min_tenant_count:
        failures.append(f"tenant_count must be at least {min_tenant_count}; observed {len(tenant_ids)}")
    if min(anchor_counts.values(), default=0) < min_anchor_count_per_tenant:
        failures.append(
            "anchor_count_per_tenant must be at least "
            f"{min_anchor_count_per_tenant}; observed {anchor_counts}"
        )
    if sorted(OFFICIAL_MODEL_NAMES) != models:
        failures.append(f"official rolling benchmark must include models {OFFICIAL_MODEL_NAMES}; observed {models}")
    if data_quality_tiers != ["thesis_grade"]:
        failures.append("official rolling benchmark evidence must contain only thesis_grade rows")
    if observed_min < 1.0:
        failures.append("official rolling benchmark evidence requires observed coverage ratio of 1.0")
    if claim_failures:
        failures.append("official rolling benchmark claim flags must remain research-only/not market execution")
    if any(str(row["strategy_kind"]) != OFFICIAL_FORECAST_ROLLING_ORIGIN_STRATEGY_KIND for row in rows):
        failures.append("official rolling benchmark rows have the wrong strategy_kind")
    return EvidenceCheckOutcome(
        not failures,
        "Official forecast rolling-origin evidence passed." if not failures else "; ".join(failures),
        {
            "row_count": frame.height,
            "tenant_count": len(tenant_ids),
            "tenant_ids": tenant_ids,
            "model_names": models,
            "anchor_counts_by_tenant": anchor_counts,
            "data_quality_tiers": data_quality_tiers,
            "observed_coverage_min": observed_min,
            "claim_flag_failure_rows": len(claim_failures),
        },
    )


def _validate_config(
    *,
    tenant_ids: tuple[str, ...],
    max_eval_anchors_per_tenant: int,
    horizon_hours: int,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if max_eval_anchors_per_tenant <= 0:
        raise ValueError("max_eval_anchors_per_tenant must be positive.")
    if horizon_hours <= 0:
        raise ValueError("horizon_hours must be positive.")


def _tenant_price_history(
    silver_frame: pl.DataFrame,
    *,
    tenant_id: str,
    require_observed_source_rows: bool,
) -> pl.DataFrame:
    required_columns = {"tenant_id", DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN}
    missing_columns = sorted(required_columns.difference(silver_frame.columns))
    if missing_columns:
        raise ValueError(f"real_data_benchmark_silver_feature_frame is missing columns: {missing_columns}")
    tenant_frame = (
        silver_frame
        .filter(pl.col("tenant_id") == tenant_id)
        .drop("tenant_id")
        .drop_nulls(subset=[DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN])
        .unique(subset=[DEFAULT_TIMESTAMP_COLUMN], keep="last")
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )
    if tenant_frame.height == 0:
        raise ValueError(f"Missing Silver benchmark rows for tenant_id={tenant_id}.")
    if require_observed_source_rows:
        if "source_kind" not in tenant_frame.columns:
            raise ValueError("official rolling benchmark requires source_kind provenance.")
        if tenant_frame.filter(pl.col("source_kind") != "observed").height:
            raise ValueError("official rolling benchmark requires observed source rows.")
    return tenant_frame


def _daily_anchors(
    price_history: pl.DataFrame,
    *,
    max_anchors: int,
    horizon_hours: int,
) -> list[datetime]:
    timestamps = [
        value
        for value in price_history.sort(DEFAULT_TIMESTAMP_COLUMN)[DEFAULT_TIMESTAMP_COLUMN].to_list()
        if isinstance(value, datetime)
    ]
    if not timestamps:
        raise ValueError("price history timestamp column must contain datetime values.")
    latest_anchor = timestamps[-1] - timedelta(hours=horizon_hours)
    earliest_anchor = timestamps[0] + timedelta(hours=MIN_NEURAL_FORECAST_TRAIN_ROWS)
    available = set(timestamps)
    anchors: list[datetime] = []
    candidate_anchor = latest_anchor
    while candidate_anchor >= earliest_anchor:
        required = [
            candidate_anchor - timedelta(hours=MIN_NEURAL_FORECAST_TRAIN_ROWS - 1) + timedelta(hours=index)
            for index in range(MIN_NEURAL_FORECAST_TRAIN_ROWS + horizon_hours)
        ]
        if all(timestamp in available for timestamp in required):
            anchors.append(candidate_anchor)
            if len(anchors) >= max_anchors:
                break
        candidate_anchor -= timedelta(hours=24)
    if not anchors:
        raise ValueError("Not enough observed rows for official rolling-origin forecast training.")
    return sorted(anchors)


def _window_for_anchor(
    price_history: pl.DataFrame,
    *,
    anchor_timestamp: datetime,
    horizon_hours: int,
) -> pl.DataFrame:
    start_timestamp = anchor_timestamp - timedelta(hours=MIN_NEURAL_FORECAST_TRAIN_ROWS - 1)
    end_timestamp = anchor_timestamp + timedelta(hours=horizon_hours)
    return price_history.filter(
        (pl.col(DEFAULT_TIMESTAMP_COLUMN) >= start_timestamp)
        & (pl.col(DEFAULT_TIMESTAMP_COLUMN) <= end_timestamp)
    ).sort(DEFAULT_TIMESTAMP_COLUMN)


def _strict_forecast_frame(window: pl.DataFrame, *, anchor_timestamp: datetime) -> pl.DataFrame:
    historical_prices = window.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) <= anchor_timestamp)
    forecast = HourlyDamBaselineSolver().build_forecast(
        historical_prices,
        anchor_timestamp=anchor_timestamp,
    )
    return pl.DataFrame(
        {
            "forecast_timestamp": [point.forecast_timestamp for point in forecast],
            "source_timestamp": [point.source_timestamp for point in forecast],
            "predicted_price_uah_mwh": [point.predicted_price_uah_mwh for point in forecast],
        }
    )


def _with_official_metadata(
    evaluation: pl.DataFrame,
    *,
    tenant_id: str,
    anchor_timestamp: datetime,
    window: pl.DataFrame,
    nbeatsx_max_steps: int,
    tft_max_epochs: int,
) -> pl.DataFrame:
    data_quality_tier = _data_quality_tier(window)
    observed_coverage_ratio = _observed_coverage_ratio(window)
    payloads: list[dict[str, Any]] = []
    for row in evaluation.iter_rows(named=True):
        payload = dict(row["evaluation_payload"])
        payload.update(
            {
                "claim_scope": OFFICIAL_FORECAST_ROLLING_ORIGIN_CLAIM_SCOPE,
                "benchmark_kind": OFFICIAL_FORECAST_ROLLING_ORIGIN_STRATEGY_KIND,
                "data_quality_tier": data_quality_tier,
                "observed_coverage_ratio": observed_coverage_ratio,
                "tenant_id": tenant_id,
                "anchor_timestamp": anchor_timestamp.isoformat(),
                "nbeatsx_max_steps": nbeatsx_max_steps,
                "tft_max_epochs": tft_max_epochs,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
        payloads.append(payload)
    return evaluation.with_columns(
        [
            pl.lit(OFFICIAL_FORECAST_ROLLING_ORIGIN_STRATEGY_KIND).alias("strategy_kind"),
            pl.Series("evaluation_payload", payloads),
        ]
    )


def _data_quality_tier(window: pl.DataFrame) -> str:
    if "source_kind" not in window.columns:
        return "demo_grade"
    source_kinds = {str(value) for value in window["source_kind"].to_list()}
    return "thesis_grade" if source_kinds == {"observed"} else "demo_grade"


def _observed_coverage_ratio(window: pl.DataFrame) -> float:
    if window.height == 0 or "source_kind" not in window.columns:
        return 0.0
    return window.filter(pl.col("source_kind") == "observed").height / window.height


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload")
    if isinstance(payload, dict):
        return payload
    return {}


def _datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    raise TypeError(f"{field_name} must be a datetime or ISO datetime string.")


def _missing_column_failures(frame: pl.DataFrame, required_columns: set[str]) -> list[str]:
    missing_columns = sorted(required_columns.difference(frame.columns))
    return [f"frame is missing required columns: {missing_columns}"] if missing_columns else []
