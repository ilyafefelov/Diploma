import os
from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.assets import taxonomy
from smart_arbitrage.dfl.regret_weighted import (
    HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
    REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
    build_horizon_regret_weighted_forecast_calibration_frame,
    build_horizon_regret_weighted_forecast_strategy_benchmark_frame,
    build_regret_weighted_forecast_calibration_frame,
    build_regret_weighted_forecast_strategy_benchmark_frame,
    run_regret_weighted_dfl_pilot,
)
from smart_arbitrage.dfl.relaxed_pilot import build_relaxed_dfl_pilot_frame
from smart_arbitrage.dfl.offline_experiment import (
    build_offline_dfl_experiment_frame,
    build_offline_dfl_panel_experiment_frame,
)
from smart_arbitrage.dfl.promotion_gate import evaluate_offline_dfl_panel_development_gate
from smart_arbitrage.resources.dfl_training_store import get_dfl_training_store
from smart_arbitrage.resources.strategy_evaluation_store import get_strategy_evaluation_store
from smart_arbitrage.strategy.ensemble_gate import (
    CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND,
    RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND,
    build_calibrated_value_aware_ensemble_frame,
    build_risk_adjusted_value_gate_frame,
    build_value_aware_ensemble_frame,
)
from smart_arbitrage.strategy.dispatch_sensitivity import build_forecast_dispatch_sensitivity_frame
from smart_arbitrage.training.dfl_training import build_dfl_training_frame
from smart_arbitrage.dfl.training_examples import build_dfl_training_example_frame


class DflTrainingAssetConfig(dg.Config):
    """DFL training-table behavior for thesis-grade benchmark rows."""

    require_thesis_grade: bool = True


class RegretWeightedDflPilotAssetConfig(dg.Config):
    """Small regret-weighted DFL pilot scope."""

    tenant_id: str = "client_003_dnipro_factory"
    forecast_model_name: str = "tft_silver_v0"
    validation_fraction: float = 0.2


class RegretWeightedForecastCalibrationAssetConfig(dg.Config):
    """Regret-weighted calibration expansion for TFT and NBEATSx."""

    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    min_prior_anchors: int = 14
    rolling_calibration_window_anchors: int = 28


class HorizonRegretWeightedForecastCalibrationAssetConfig(dg.Config):
    """Horizon-aware regret-weighted calibration expansion."""

    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    min_prior_anchors: int = 14
    rolling_calibration_window_anchors: int = 28


class RelaxedDflPilotAssetConfig(dg.Config):
    """Small differentiable relaxed-LP DFL pilot scope."""

    max_examples: int = 12


class OfflineDflExperimentAssetConfig(dg.Config):
    """Bounded offline relaxed-LP DFL experiment scope."""

    tenant_id: str = "client_003_dnipro_factory"
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    validation_fraction: float = 0.2
    max_train_anchors: int = 32
    max_validation_anchors: int = 18
    epoch_count: int = 8
    learning_rate: float = 10.0


class OfflineDflPanelExperimentAssetConfig(dg.Config):
    """All-tenant offline relaxed-LP DFL panel scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_validation_anchor_count_per_tenant: int = 18
    max_train_anchors_per_tenant: int = 72
    inner_validation_fraction: float = 0.2
    epoch_count: int = 8
    learning_rate: float = 10.0


@dg.asset(
    group_name=taxonomy.GOLD_SELECTOR_DIAGNOSTICS,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="thesis_grade",
        market_venue="DAM",
    ),
)
def real_data_value_aware_ensemble_frame(
    context,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Gold value-aware ensemble gate using prior-anchor validation regret only."""

    ensemble_frame = build_value_aware_ensemble_frame(real_data_rolling_origin_benchmark_frame)
    get_strategy_evaluation_store().upsert_evaluation_frame(ensemble_frame)
    _add_metadata(
        context,
        {
            "rows": ensemble_frame.height,
            "tenant_count": ensemble_frame.select("tenant_id").n_unique() if ensemble_frame.height else 0,
            "anchor_count": ensemble_frame.select("anchor_timestamp").n_unique() if ensemble_frame.height else 0,
            "strategy_kind": "value_aware_ensemble_gate",
            "selection_policy": "prior_anchor_validation_regret_only",
        },
    )
    return ensemble_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def dfl_training_frame(
    context,
    config: DflTrainingAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    real_data_value_aware_ensemble_frame: pl.DataFrame,
) -> pl.DataFrame:
    """DFL-ready supervised examples from benchmark and ensemble rows."""

    source_frame = pl.concat(
        [real_data_rolling_origin_benchmark_frame, real_data_value_aware_ensemble_frame],
        how="diagonal_relaxed",
    )
    training_frame = build_dfl_training_frame(
        source_frame,
        require_thesis_grade=config.require_thesis_grade,
    )
    get_dfl_training_store().upsert_training_frame(training_frame)
    _add_metadata(
        context,
        {
            "rows": training_frame.height,
            "tenant_count": training_frame.select("tenant_id").n_unique() if training_frame.height else 0,
            "model_count": training_frame.select("forecast_model_name").n_unique() if training_frame.height else 0,
            "scope": "dfl_ready_training_examples_not_full_dfl",
        },
    )
    return training_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def dfl_training_example_frame(
    context,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Vector-rich sidecar DFL examples from strict LP/oracle benchmark rows."""

    training_example_frame = build_dfl_training_example_frame(real_data_rolling_origin_benchmark_frame)
    get_dfl_training_store().upsert_training_example_frame(training_example_frame)
    _add_metadata(
        context,
        {
            "rows": training_example_frame.height,
            "tenant_count": training_example_frame.select("tenant_id").n_unique()
            if training_example_frame.height
            else 0,
            "anchor_count": training_example_frame.select("anchor_timestamp").n_unique()
            if training_example_frame.height
            else 0,
            "scope": "dfl_training_examples_not_full_dfl",
        },
    )
    return training_example_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="pilot",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def regret_weighted_dfl_pilot_frame(
    context,
    config: RegretWeightedDflPilotAssetConfig,
    dfl_training_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Small regret-weighted forecast-calibration pilot for one tenant/model."""

    pilot_frame = run_regret_weighted_dfl_pilot(
        dfl_training_frame,
        tenant_id=config.tenant_id,
        forecast_model_name=config.forecast_model_name,
        validation_fraction=config.validation_fraction,
    )
    get_dfl_training_store().upsert_pilot_frame(pilot_frame)
    _add_metadata(
        context,
        {
            "rows": pilot_frame.height,
            "tenant_id": config.tenant_id,
            "forecast_model_name": config.forecast_model_name,
            "scope": "pilot_not_full_dfl",
        },
    )
    return pilot_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="pilot",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_relaxed_lp_pilot_frame(
    context,
    config: RelaxedDflPilotAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Differentiable relaxed-LP pilot rows for future full DFL training."""

    pilot_frame = build_relaxed_dfl_pilot_frame(
        real_data_rolling_origin_benchmark_frame,
        max_examples=config.max_examples,
    )
    get_dfl_training_store().upsert_relaxed_pilot_frame(pilot_frame)
    _add_metadata(
        context,
        {
            "rows": pilot_frame.height,
            "scope": "differentiable_relaxed_lp_pilot_not_final_dfl",
        },
    )
    return pilot_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="pilot",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def offline_dfl_experiment_frame(
    context,
    config: OfflineDflExperimentAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Bounded offline DFL experiment over prior-anchor relaxed-LP training."""

    experiment_frame = build_offline_dfl_experiment_frame(
        real_data_rolling_origin_benchmark_frame,
        tenant_id=config.tenant_id,
        forecast_model_names=_forecast_model_names(config.forecast_model_names_csv),
        validation_fraction=config.validation_fraction,
        max_train_anchors=config.max_train_anchors,
        max_validation_anchors=config.max_validation_anchors,
        epoch_count=config.epoch_count,
        learning_rate=config.learning_rate,
    )
    _add_metadata(
        context,
        {
            "rows": experiment_frame.height,
            "tenant_id": config.tenant_id,
            "model_count": experiment_frame.select("forecast_model_name").n_unique()
            if experiment_frame.height
            else 0,
            "scope": "offline_dfl_experiment_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return experiment_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="pilot",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def offline_dfl_panel_experiment_frame(
    context,
    config: OfflineDflPanelExperimentAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """All-tenant offline DFL panel with prior-anchor checkpoint selection."""

    panel_frame = build_offline_dfl_panel_experiment_frame(
        real_data_rolling_origin_benchmark_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=_forecast_model_names(config.forecast_model_names_csv),
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        max_train_anchors_per_tenant=config.max_train_anchors_per_tenant,
        inner_validation_fraction=config.inner_validation_fraction,
        epoch_count=config.epoch_count,
        learning_rate=config.learning_rate,
    )
    development_gate = evaluate_offline_dfl_panel_development_gate(panel_frame)
    _add_metadata(
        context,
        {
            "rows": panel_frame.height,
            "tenant_count": panel_frame.select("tenant_id").n_unique() if panel_frame.height else 0,
            "model_count": panel_frame.select("forecast_model_name").n_unique()
            if panel_frame.height
            else 0,
            "final_validation_tenant_anchor_count": development_gate.metrics.get(
                "validation_tenant_anchor_count",
                0,
            ),
            "development_gate_decision": development_gate.decision,
            "development_gate_description": development_gate.description,
            "scope": "offline_dfl_panel_experiment_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return panel_frame


@dg.asset(
    group_name=taxonomy.GOLD_CALIBRATION,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="calibration",
        evidence_scope="thesis_grade",
        market_venue="DAM",
    ),
)
def regret_weighted_forecast_calibration_frame(
    context,
    config: RegretWeightedForecastCalibrationAssetConfig,
    dfl_training_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Pre-anchor regret-weighted forecast bias rows for TFT and NBEATSx."""

    calibration_frame = build_regret_weighted_forecast_calibration_frame(
        dfl_training_frame,
        forecast_model_names=_forecast_model_names(config.forecast_model_names_csv),
        min_prior_anchors=config.min_prior_anchors,
        rolling_calibration_window_anchors=config.rolling_calibration_window_anchors,
    )
    _add_metadata(
        context,
        {
            "rows": calibration_frame.height,
            "tenant_count": calibration_frame.select("tenant_id").n_unique() if calibration_frame.height else 0,
            "source_model_count": calibration_frame.select("source_forecast_model_name").n_unique()
            if calibration_frame.height
            else 0,
            "scope": "regret_weighted_forecast_calibration_not_full_dfl",
        },
    )
    return calibration_frame


@dg.asset(
    group_name=taxonomy.GOLD_CALIBRATION,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="calibration",
        evidence_scope="thesis_grade",
        market_venue="DAM",
    ),
)
def regret_weighted_forecast_strategy_benchmark_frame(
    context,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    regret_weighted_forecast_calibration_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP benchmark for original and regret-weight calibrated forecasts."""

    benchmark_frame = build_regret_weighted_forecast_strategy_benchmark_frame(
        real_data_rolling_origin_benchmark_frame,
        regret_weighted_forecast_calibration_frame,
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(benchmark_frame)
    _add_metadata(
        context,
        {
            "rows": benchmark_frame.height,
            "tenant_count": benchmark_frame.select("tenant_id").n_unique() if benchmark_frame.height else 0,
            "anchor_count": benchmark_frame.select("anchor_timestamp").n_unique() if benchmark_frame.height else 0,
            "model_count": benchmark_frame.select("forecast_model_name").n_unique() if benchmark_frame.height else 0,
            "strategy_kind": REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
            "scope": "regret_weighted_forecast_calibration_benchmark_not_full_dfl",
        },
    )
    _log_mlflow_summary(benchmark_frame)
    return benchmark_frame


@dg.asset(
    group_name=taxonomy.GOLD_CALIBRATION,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="calibration",
        evidence_scope="thesis_grade",
        market_venue="DAM",
    ),
)
def horizon_regret_weighted_forecast_calibration_frame(
    context,
    config: HorizonRegretWeightedForecastCalibrationAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Pre-anchor horizon-specific regret-weighted price-bias rows."""

    calibration_frame = build_horizon_regret_weighted_forecast_calibration_frame(
        real_data_rolling_origin_benchmark_frame,
        forecast_model_names=_forecast_model_names(config.forecast_model_names_csv),
        min_prior_anchors=config.min_prior_anchors,
        rolling_calibration_window_anchors=config.rolling_calibration_window_anchors,
    )
    _add_metadata(
        context,
        {
            "rows": calibration_frame.height,
            "tenant_count": calibration_frame.select("tenant_id").n_unique()
            if calibration_frame.height
            else 0,
            "source_model_count": calibration_frame.select("source_forecast_model_name").n_unique()
            if calibration_frame.height
            else 0,
            "scope": "horizon_regret_weighted_forecast_calibration_not_full_dfl",
        },
    )
    return calibration_frame


@dg.asset(
    group_name=taxonomy.GOLD_CALIBRATION,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="calibration",
        evidence_scope="thesis_grade",
        market_venue="DAM",
    ),
)
def horizon_regret_weighted_forecast_strategy_benchmark_frame(
    context,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    horizon_regret_weighted_forecast_calibration_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP benchmark for horizon-aware corrected forecasts."""

    benchmark_frame = build_horizon_regret_weighted_forecast_strategy_benchmark_frame(
        real_data_rolling_origin_benchmark_frame,
        horizon_regret_weighted_forecast_calibration_frame,
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(benchmark_frame)
    _add_metadata(
        context,
        {
            "rows": benchmark_frame.height,
            "tenant_count": benchmark_frame.select("tenant_id").n_unique() if benchmark_frame.height else 0,
            "anchor_count": benchmark_frame.select("anchor_timestamp").n_unique()
            if benchmark_frame.height
            else 0,
            "model_count": benchmark_frame.select("forecast_model_name").n_unique()
            if benchmark_frame.height
            else 0,
            "strategy_kind": HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
            "scope": "horizon_regret_weighted_forecast_calibration_benchmark_not_full_dfl",
        },
    )
    _log_mlflow_summary(
        benchmark_frame,
        experiment_name="smart-arbitrage-horizon-regret-weighted-dfl-expansion",
        strategy_kind=HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
    )
    return benchmark_frame


@dg.asset(
    group_name=taxonomy.GOLD_SELECTOR_DIAGNOSTICS,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="thesis_grade",
        market_venue="DAM",
    ),
)
def calibrated_value_aware_ensemble_frame(
    context,
    horizon_regret_weighted_forecast_strategy_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Gold gate over strict control and horizon-aware calibrated forecast candidates."""

    ensemble_frame = build_calibrated_value_aware_ensemble_frame(
        horizon_regret_weighted_forecast_strategy_benchmark_frame
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(ensemble_frame)
    _add_metadata(
        context,
        {
            "rows": ensemble_frame.height,
            "tenant_count": ensemble_frame.select("tenant_id").n_unique()
            if ensemble_frame.height
            else 0,
            "anchor_count": ensemble_frame.select("anchor_timestamp").n_unique()
            if ensemble_frame.height
            else 0,
            "strategy_kind": CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND,
            "selection_policy": "prior_anchor_validation_regret_only",
            "scope": "selector_not_full_dfl",
        },
    )
    _log_mlflow_summary(
        ensemble_frame,
        experiment_name="smart-arbitrage-calibrated-ensemble-gate",
        strategy_kind=CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND,
    )
    return ensemble_frame


@dg.asset(
    group_name=taxonomy.GOLD_SELECTOR_DIAGNOSTICS,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="diagnostics",
        evidence_scope="thesis_grade",
        market_venue="DAM",
    ),
)
def forecast_dispatch_sensitivity_frame(
    context,
    horizon_regret_weighted_forecast_strategy_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Gold diagnostics connecting forecast errors to LP dispatch and realized regret."""

    sensitivity_frame = build_forecast_dispatch_sensitivity_frame(
        horizon_regret_weighted_forecast_strategy_benchmark_frame
    )
    _add_metadata(
        context,
        {
            "rows": sensitivity_frame.height,
            "tenant_count": sensitivity_frame.select("tenant_id").n_unique()
            if sensitivity_frame.height
            else 0,
            "anchor_count": sensitivity_frame.select("anchor_timestamp").n_unique()
            if sensitivity_frame.height
            else 0,
            "scope": "forecast_to_dispatch_diagnostics",
        },
    )
    return sensitivity_frame


@dg.asset(
    group_name=taxonomy.GOLD_SELECTOR_DIAGNOSTICS,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="thesis_grade",
        market_venue="DAM",
    ),
)
def risk_adjusted_value_gate_frame(
    context,
    horizon_regret_weighted_forecast_strategy_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Gold risk-adjusted gate using prior median regret, tail regret, and win rate."""

    gate_frame = build_risk_adjusted_value_gate_frame(
        horizon_regret_weighted_forecast_strategy_benchmark_frame
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(gate_frame)
    _add_metadata(
        context,
        {
            "rows": gate_frame.height,
            "tenant_count": gate_frame.select("tenant_id").n_unique()
            if gate_frame.height
            else 0,
            "anchor_count": gate_frame.select("anchor_timestamp").n_unique()
            if gate_frame.height
            else 0,
            "strategy_kind": RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND,
            "selection_policy": "risk_adjusted_prior_anchor_regret_tail_and_win_rate",
            "scope": "selector_not_full_dfl",
        },
    )
    _log_mlflow_summary(
        gate_frame,
        experiment_name="smart-arbitrage-risk-adjusted-value-gate",
        strategy_kind=RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND,
    )
    return gate_frame


DFL_RESEARCH_GOLD_ASSETS = [
    real_data_value_aware_ensemble_frame,
    dfl_training_frame,
    dfl_training_example_frame,
    regret_weighted_dfl_pilot_frame,
    dfl_relaxed_lp_pilot_frame,
    offline_dfl_experiment_frame,
    offline_dfl_panel_experiment_frame,
    regret_weighted_forecast_calibration_frame,
    regret_weighted_forecast_strategy_benchmark_frame,
    horizon_regret_weighted_forecast_calibration_frame,
    horizon_regret_weighted_forecast_strategy_benchmark_frame,
    calibrated_value_aware_ensemble_frame,
    forecast_dispatch_sensitivity_frame,
    risk_adjusted_value_gate_frame,
]


def _add_metadata(context: dg.AssetExecutionContext | None, metadata: dict[str, Any]) -> None:
    if context is not None:
        context.add_output_metadata(metadata)


def _forecast_model_names(raw_value: str) -> tuple[str, ...]:
    return _csv_values(raw_value, field_name="forecast_model_names_csv")


def _csv_values(raw_value: str, *, field_name: str) -> tuple[str, ...]:
    values = tuple(value.strip() for value in raw_value.split(",") if value.strip())
    if not values:
        raise ValueError(f"{field_name} must contain at least one value.")
    return values


def _log_mlflow_summary(
    benchmark_frame: pl.DataFrame,
    *,
    experiment_name: str = "smart-arbitrage-regret-weighted-dfl-expansion",
    strategy_kind: str = REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
) -> None:
    if benchmark_frame.height == 0:
        return
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if tracking_uri is None:
        return
    try:
        import mlflow
    except ImportError:
        return

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)
    summary = (
        benchmark_frame
        .group_by("forecast_model_name")
        .agg(
            [
                pl.len().alias("rows"),
                pl.mean("regret_uah").alias("mean_regret_uah"),
                pl.median("regret_uah").alias("median_regret_uah"),
                pl.mean("decision_value_uah").alias("mean_decision_value_uah"),
            ]
        )
    )
    with mlflow.start_run(run_name=strategy_kind):
        mlflow.set_tags(
            {
                "strategy_kind": strategy_kind,
                "academic_scope": "not_full_differentiable_dfl",
            }
        )
        mlflow.log_metric("rows", benchmark_frame.height)
        mlflow.log_metric("tenant_count", benchmark_frame.select("tenant_id").n_unique())
        mlflow.log_metric("anchor_count", benchmark_frame.select("anchor_timestamp").n_unique())
        for row in summary.iter_rows(named=True):
            model_name = str(row["forecast_model_name"]).replace("-", "_")
            mlflow.log_metric(f"{model_name}_mean_regret_uah", float(row["mean_regret_uah"]))
            mlflow.log_metric(f"{model_name}_median_regret_uah", float(row["median_regret_uah"]))
