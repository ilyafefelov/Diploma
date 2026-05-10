from dataclasses import dataclass
from datetime import datetime, timedelta
import os
from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.assets import taxonomy
from smart_arbitrage.assets.bronze.market_weather import list_available_weather_tenants
from smart_arbitrage.resources.strategy_evaluation_store import (
    get_strategy_evaluation_store,
)
from smart_arbitrage.strategy.official_forecast_rolling import (
    OFFICIAL_FORECAST_ROLLING_ORIGIN_STRATEGY_KIND,
    build_official_forecast_rolling_origin_benchmark_frame,
    validate_official_forecast_rolling_origin_evidence,
)
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND,
    evaluate_forecast_candidates_against_oracle,
    evaluate_rolling_origin_forecast_benchmark,
    tenant_battery_defaults_from_registry,
)

OFFICIAL_FORECAST_STRICT_LP_STRATEGY_KIND = "official_forecast_strict_lp_benchmark"


class ForecastStrategyComparisonAssetConfig(dg.Config):
    """Tenant cap for Gold forecast-strategy comparison."""

    tenant_ids_csv: str = ""


class RealDataRollingOriginBenchmarkAssetConfig(dg.Config):
    """Tenant and anchor caps for thesis-grade real-data benchmark runs."""

    tenant_ids_csv: str = ""
    max_anchors: int = 90


class OfficialForecastRollingOriginAssetConfig(dg.Config):
    """CPU-safe official adapter settings for rolling-origin strict LP evidence."""

    tenant_ids_csv: str = ""
    max_eval_anchors_per_tenant: int = 2
    horizon_hours: int = 24
    nbeatsx_max_steps: int = 100
    nbeatsx_random_seed: int = 20260511
    tft_max_epochs: int = 15
    tft_batch_size: int = 32
    tft_learning_rate: float = 0.005
    tft_hidden_size: int = 12
    tft_hidden_continuous_size: int = 6


@dataclass(frozen=True, slots=True)
class _StartingSoc:
    fraction: float
    source: str


@dg.asset(
    group_name=taxonomy.GOLD_MVP_BENCHMARK,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="forecast_strategy",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def forecast_strategy_comparison_frame(
    context,
    config: ForecastStrategyComparisonAssetConfig,
    dam_price_history: pl.DataFrame,
    strict_similar_day_forecast: pl.DataFrame,
    nbeatsx_price_forecast: pl.DataFrame,
    tft_price_forecast: pl.DataFrame,
    battery_state_hourly_silver=None,
) -> pl.DataFrame:
    """Gold comparison of Silver forecasts routed through the LP and oracle benchmark."""

    rows: list[pl.DataFrame] = []
    anchor_timestamp = _anchor_from_forecast(strict_similar_day_forecast)
    for tenant_id in _tenant_ids_from_csv(config.tenant_ids_csv):
        defaults = tenant_battery_defaults_from_registry(tenant_id)
        starting_soc = _starting_soc_for_tenant(
            tenant_id=tenant_id,
            default_soc_fraction=defaults.initial_soc_fraction,
            battery_state_hourly_silver=battery_state_hourly_silver,
        )
        rows.append(
            evaluate_forecast_candidates_against_oracle(
                price_history=dam_price_history,
                tenant_id=tenant_id,
                battery_metrics=defaults.metrics,
                starting_soc_fraction=starting_soc.fraction,
                starting_soc_source=starting_soc.source,
                anchor_timestamp=anchor_timestamp,
                candidates=[
                    ForecastCandidate(
                        model_name="strict_similar_day",
                        forecast_frame=strict_similar_day_forecast,
                        point_prediction_column="predicted_price_uah_mwh",
                    ),
                    ForecastCandidate(
                        model_name="nbeatsx_silver_v0",
                        forecast_frame=nbeatsx_price_forecast,
                        point_prediction_column="predicted_price_uah_mwh",
                    ),
                    ForecastCandidate(
                        model_name="tft_silver_v0",
                        forecast_frame=tft_price_forecast,
                        point_prediction_column="predicted_price_p50_uah_mwh",
                    ),
                ],
            )
        )
    frame = pl.concat(rows) if rows else pl.DataFrame()
    get_strategy_evaluation_store().upsert_evaluation_frame(frame)
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "tenant_count": frame.select("tenant_id").n_unique() if frame.height else 0,
            "forecast_candidate_count": frame.select("forecast_model_name").n_unique()
            if frame.height
            else 0,
            "market_venue": "DAM",
            "strategy_kind": "forecast_driven_lp",
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_MVP_BENCHMARK,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="forecast_strategy",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="research_only",
        backend="official_forecast_adapters",
        market_venue="DAM",
    ),
)
def official_forecast_strict_lp_benchmark_frame(
    context,
    config: ForecastStrategyComparisonAssetConfig,
    dam_price_history: pl.DataFrame,
    strict_similar_day_forecast: pl.DataFrame,
    nbeatsx_official_price_forecast: pl.DataFrame,
    tft_official_price_forecast: pl.DataFrame,
    battery_state_hourly_silver=None,
) -> pl.DataFrame:
    """Strict LP/oracle evidence for official NBEATSx/TFT forecast adapters."""

    if nbeatsx_official_price_forecast.is_empty() or tft_official_price_forecast.is_empty():
        frame = pl.DataFrame()
        _add_metadata(
            context,
            {
                "rows": 0,
                "tenant_count": 0,
                "forecast_candidate_count": 0,
                "market_venue": "DAM",
                "strategy_kind": OFFICIAL_FORECAST_STRICT_LP_STRATEGY_KIND,
                "scope": "official_backend_unavailable_or_unmaterialized",
            },
        )
        return frame

    rows: list[pl.DataFrame] = []
    anchor_timestamp = _anchor_from_forecast(strict_similar_day_forecast)
    for tenant_id in _tenant_ids_from_csv(config.tenant_ids_csv):
        defaults = tenant_battery_defaults_from_registry(tenant_id)
        starting_soc = _starting_soc_for_tenant(
            tenant_id=tenant_id,
            default_soc_fraction=defaults.initial_soc_fraction,
            battery_state_hourly_silver=battery_state_hourly_silver,
        )
        rows.append(
            evaluate_forecast_candidates_against_oracle(
                price_history=dam_price_history,
                tenant_id=tenant_id,
                battery_metrics=defaults.metrics,
                starting_soc_fraction=starting_soc.fraction,
                starting_soc_source=starting_soc.source,
                anchor_timestamp=anchor_timestamp,
                candidates=[
                    ForecastCandidate(
                        model_name="strict_similar_day",
                        forecast_frame=strict_similar_day_forecast,
                        point_prediction_column="predicted_price_uah_mwh",
                    ),
                    ForecastCandidate(
                        model_name="nbeatsx_official_v0",
                        forecast_frame=nbeatsx_official_price_forecast,
                        point_prediction_column="predicted_price_uah_mwh",
                    ),
                    ForecastCandidate(
                        model_name="tft_official_v0",
                        forecast_frame=tft_official_price_forecast,
                        point_prediction_column="predicted_price_uah_mwh",
                    ),
                ],
            ).with_columns(
                pl.lit(OFFICIAL_FORECAST_STRICT_LP_STRATEGY_KIND).alias("strategy_kind")
            )
        )
    frame = pl.concat(rows) if rows else pl.DataFrame()
    get_strategy_evaluation_store().upsert_evaluation_frame(frame)
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "tenant_count": frame.select("tenant_id").n_unique() if frame.height else 0,
            "forecast_candidate_count": frame.select("forecast_model_name").n_unique()
            if frame.height
            else 0,
            "market_venue": "DAM",
            "strategy_kind": OFFICIAL_FORECAST_STRICT_LP_STRATEGY_KIND,
            "scope": "official_forecast_adapter_strict_lp_evidence_not_live_strategy",
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_REAL_DATA_BENCHMARK,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="forecast_strategy",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="research_only",
        backend="official_forecast_adapters",
        market_venue="DAM",
    ),
)
def official_forecast_rolling_origin_benchmark_frame(
    context,
    config: OfficialForecastRollingOriginAssetConfig,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling-origin strict LP evidence for official NBEATSx/TFT adapters."""

    tenant_ids = tuple(_tenant_ids_from_csv(config.tenant_ids_csv))
    frame = build_official_forecast_rolling_origin_benchmark_frame(
        real_data_benchmark_silver_feature_frame,
        tenant_ids=tenant_ids,
        max_eval_anchors_per_tenant=config.max_eval_anchors_per_tenant,
        horizon_hours=config.horizon_hours,
        nbeatsx_max_steps=config.nbeatsx_max_steps,
        nbeatsx_random_seed=config.nbeatsx_random_seed,
        tft_max_epochs=config.tft_max_epochs,
        tft_batch_size=config.tft_batch_size,
        tft_learning_rate=config.tft_learning_rate,
        tft_hidden_size=config.tft_hidden_size,
        tft_hidden_continuous_size=config.tft_hidden_continuous_size,
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(frame)
    outcome = validate_official_forecast_rolling_origin_evidence(
        frame,
        min_tenant_count=len(tenant_ids),
        min_anchor_count_per_tenant=config.max_eval_anchors_per_tenant,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "tenant_count": frame.select("tenant_id").n_unique() if frame.height else 0,
            "anchor_count_per_tenant_target": config.max_eval_anchors_per_tenant,
            "forecast_candidate_count": frame.select("forecast_model_name").n_unique()
            if frame.height
            else 0,
            "market_venue": "DAM",
            "strategy_kind": OFFICIAL_FORECAST_ROLLING_ORIGIN_STRATEGY_KIND,
            "evidence_passed": outcome.passed,
            "evidence_description": outcome.description,
            **outcome.metadata,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_REAL_DATA_BENCHMARK,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="forecast_strategy",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="thesis_grade",
        market_venue="DAM",
    ),
)
def real_data_rolling_origin_benchmark_frame(
    context,
    config: RealDataRollingOriginBenchmarkAssetConfig,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    battery_state_hourly_silver=None,
) -> pl.DataFrame:
    """Rolling-origin real-data benchmark for strict similar-day, NBEATSx, and TFT."""

    rows: list[pl.DataFrame] = []
    anchor_timestamps = _daily_benchmark_anchors(
        _market_price_history_from_silver(real_data_benchmark_silver_feature_frame),
        max_anchors=config.max_anchors,
    )
    for tenant_id in _tenant_ids_from_csv(config.tenant_ids_csv):
        defaults = tenant_battery_defaults_from_registry(tenant_id)
        starting_soc = _starting_soc_for_tenant(
            tenant_id=tenant_id,
            default_soc_fraction=defaults.initial_soc_fraction,
            battery_state_hourly_silver=battery_state_hourly_silver,
        )
        price_history = _tenant_price_history_from_silver(
            real_data_benchmark_silver_feature_frame,
            tenant_id=tenant_id,
        )
        rows.append(
            evaluate_rolling_origin_forecast_benchmark(
                price_history=price_history,
                tenant_id=tenant_id,
                battery_metrics=defaults.metrics,
                starting_soc_fraction=starting_soc.fraction,
                starting_soc_source=starting_soc.source,
                anchor_timestamps=anchor_timestamps,
                max_anchors=config.max_anchors,
            )
        )
    frame = pl.concat(rows, how="diagonal_relaxed") if rows else pl.DataFrame()
    get_strategy_evaluation_store().upsert_evaluation_frame(frame)
    _log_real_data_benchmark_to_mlflow(frame)
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "tenant_count": frame.select("tenant_id").n_unique() if frame.height else 0,
            "anchor_count": frame.select("anchor_timestamp").n_unique() if frame.height else 0,
            "forecast_candidate_count": frame.select("forecast_model_name").n_unique()
            if frame.height
            else 0,
            "market_venue": "DAM",
            "strategy_kind": REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND,
        },
    )
    return frame


real_data_benchmark_daily_job = dg.define_asset_job(
    "real_data_benchmark_daily",
    selection=dg.AssetSelection.assets(
        "real_data_benchmark_silver_feature_frame",
        "real_data_rolling_origin_benchmark_frame",
    ),
)

real_data_benchmark_daily_schedule = dg.ScheduleDefinition(
    name="real_data_benchmark_daily_schedule",
    job=real_data_benchmark_daily_job,
    cron_schedule="0 3 * * *",
    execution_timezone="Europe/Kyiv",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description="Daily stopped-by-default materialization job for real-data rolling-origin benchmark assets.",
)


FORECAST_STRATEGY_GOLD_ASSETS = [
    forecast_strategy_comparison_frame,
    official_forecast_strict_lp_benchmark_frame,
    official_forecast_rolling_origin_benchmark_frame,
    real_data_rolling_origin_benchmark_frame,
]

FORECAST_STRATEGY_GOLD_SCHEDULES = [
    real_data_benchmark_daily_schedule,
]


def _tenant_ids_from_csv(value: str) -> list[str]:
    tenant_ids = [item.strip() for item in value.split(",") if item.strip()]
    if tenant_ids:
        return tenant_ids
    return [
        str(tenant["tenant_id"])
        for tenant in list_available_weather_tenants()
        if tenant.get("tenant_id") is not None
    ]


def _anchor_from_forecast(forecast_frame: pl.DataFrame) -> datetime:
    if "forecast_timestamp" not in forecast_frame.columns:
        raise ValueError("forecast frame is missing forecast_timestamp.")
    first_timestamp = (
        forecast_frame.sort("forecast_timestamp")
        .select("forecast_timestamp")
        .to_series()
        .item(0)
    )
    if not isinstance(first_timestamp, datetime):
        raise TypeError("forecast_timestamp column must contain datetime values.")
    return first_timestamp - timedelta(hours=1)


def _daily_benchmark_anchors(price_history: pl.DataFrame, *, max_anchors: int) -> list[datetime]:
    if max_anchors <= 0:
        raise ValueError("max_anchors must be positive.")
    if price_history.height == 0:
        raise ValueError("observed_market_price_history_bronze must contain rows.")
    timestamps = [
        value
        for value in price_history.sort("timestamp").select("timestamp").to_series().to_list()
        if isinstance(value, datetime)
    ]
    if not timestamps:
        raise ValueError("price history timestamp column must contain datetime values.")
    latest_anchor = timestamps[-1] - timedelta(hours=24)
    earliest_anchor = timestamps[0] + timedelta(hours=168)
    available_timestamps = set(timestamps)
    anchors: list[datetime] = []
    candidate_anchor = latest_anchor
    while candidate_anchor >= earliest_anchor:
        required_window_timestamps = [
            candidate_anchor - timedelta(hours=167) + timedelta(hours=step_index)
            for step_index in range(168 + 24)
        ]
        if all(
            required_timestamp in available_timestamps
            for required_timestamp in required_window_timestamps
        ):
            anchors.append(candidate_anchor)
            if len(anchors) >= max_anchors:
                break
        candidate_anchor -= timedelta(hours=24)
    if not anchors:
        raise ValueError("Not enough observed history for a 168h train window plus 24h benchmark horizon.")
    return sorted(anchors)


def _join_tenant_weather_features(
    price_history: pl.DataFrame,
    weather_history: pl.DataFrame,
    *,
    tenant_id: str,
) -> pl.DataFrame:
    if weather_history.height == 0:
        return price_history
    required_columns = {"tenant_id", "timestamp"}
    if not required_columns.issubset(weather_history.columns):
        return price_history
    tenant_weather = (
        weather_history
        .filter(pl.col("tenant_id") == tenant_id)
        .select(
            [
                "timestamp",
                *[
                    column_name
                    for column_name in [
                        "temperature",
                        "wind_speed",
                        "cloudcover",
                        "precipitation",
                        "effective_solar",
                        "source_kind",
                    ]
                    if column_name in weather_history.columns
                ],
            ]
        )
        .rename(
            {
                "temperature": "weather_temperature",
                "wind_speed": "weather_wind_speed",
                "cloudcover": "weather_cloudcover",
                "precipitation": "weather_precipitation",
                "effective_solar": "weather_effective_solar",
                "source_kind": "weather_source_kind",
            }
        )
    )
    if tenant_weather.height == 0:
        return price_history
    return price_history.join(tenant_weather, on="timestamp", how="left")


def _market_price_history_from_silver(silver_frame: pl.DataFrame) -> pl.DataFrame:
    required_columns = {"timestamp", "price_uah_mwh"}
    missing_columns = required_columns.difference(silver_frame.columns)
    if missing_columns:
        raise ValueError(f"real_data_benchmark_silver_feature_frame is missing required columns: {sorted(missing_columns)}")
    return (
        silver_frame
        .select(["timestamp", "price_uah_mwh"])
        .drop_nulls()
        .unique(subset=["timestamp"], keep="last")
        .sort("timestamp")
    )


def _tenant_price_history_from_silver(silver_frame: pl.DataFrame, *, tenant_id: str) -> pl.DataFrame:
    if "tenant_id" not in silver_frame.columns:
        return silver_frame
    tenant_frame = silver_frame.filter(pl.col("tenant_id") == tenant_id)
    if tenant_frame.height == 0:
        raise ValueError(f"Missing real-data Silver benchmark rows for tenant_id={tenant_id}.")
    return tenant_frame.drop("tenant_id")


def _starting_soc_for_tenant(
    *,
    tenant_id: str,
    default_soc_fraction: float,
    battery_state_hourly_silver: Any,
) -> _StartingSoc:
    if (
        isinstance(battery_state_hourly_silver, pl.DataFrame)
        and battery_state_hourly_silver.height
    ):
        required_columns = {
            "tenant_id",
            "snapshot_hour",
            "soc_close",
            "telemetry_freshness",
        }
        if required_columns.issubset(battery_state_hourly_silver.columns):
            tenant_snapshots = battery_state_hourly_silver.filter(
                (pl.col("tenant_id") == tenant_id)
                & (pl.col("telemetry_freshness") == "fresh")
            ).sort("snapshot_hour")
            if tenant_snapshots.height:
                return _StartingSoc(
                    fraction=float(
                        tenant_snapshots.select("soc_close").to_series().item(-1)
                    ),
                    source="telemetry_hourly",
                )
    return _StartingSoc(fraction=default_soc_fraction, source="tenant_default")


def _add_metadata(
    context: dg.AssetExecutionContext | None, metadata: dict[str, Any]
) -> None:
    if context is not None:
        context.add_output_metadata(metadata)


def _log_real_data_benchmark_to_mlflow(frame: pl.DataFrame) -> None:
    mlflow = _try_import_mlflow()
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if mlflow is None or tracking_uri is None or frame.height == 0:
        return None
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("smart-arbitrage-real-data-benchmark")
    with mlflow.start_run(run_name="real-data-rolling-origin-benchmark"):
        mlflow.log_param("strategy_kind", REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND)
        mlflow.log_param("market_venue", "DAM")
        mlflow.log_param("tenant_count", frame.select("tenant_id").n_unique())
        mlflow.log_param("anchor_count", frame.select("anchor_timestamp").n_unique())
        mlflow.log_metric("mean_regret_uah", float(frame.select("regret_uah").mean().item()))
        mlflow.log_metric("median_regret_uah", float(frame.select("regret_uah").median().item()))
        mlflow.log_metric("mean_decision_value_uah", float(frame.select("decision_value_uah").mean().item()))
        for model_name, win_rate in _win_rate_by_model(frame).items():
            mlflow.log_metric(f"win_rate_{model_name}", win_rate)
        mlflow.set_tag("benchmark_kind", "real_data_rolling_origin")
        mlflow.set_tag("model_registry_policy", "forecast_candidates_only")


def _win_rate_by_model(frame: pl.DataFrame) -> dict[str, float]:
    if frame.height == 0:
        return {}
    anchor_count = frame.select(["tenant_id", "anchor_timestamp"]).unique().height
    if anchor_count == 0:
        return {}
    return {
        str(row["forecast_model_name"]): float(row["wins"]) / anchor_count
        for row in (
            frame
            .filter(pl.col("rank_by_regret") == 1)
            .group_by("forecast_model_name")
            .agg(pl.len().alias("wins"))
            .iter_rows(named=True)
        )
    }


def _try_import_mlflow() -> Any | None:
    try:
        import mlflow
    except ModuleNotFoundError:
        return None
    return mlflow
