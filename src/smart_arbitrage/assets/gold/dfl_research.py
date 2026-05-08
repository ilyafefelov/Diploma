from datetime import datetime
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
from smart_arbitrage.dfl.action_targeting import (
    ACTION_TARGET_STRICT_LP_STRATEGY_KIND,
    build_offline_dfl_action_target_panel_frame,
    build_offline_dfl_action_target_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.decision_targeting import (
    DECISION_TARGET_STRICT_LP_STRATEGY_KIND,
    build_offline_dfl_decision_target_panel_frame,
    build_offline_dfl_decision_target_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.panel_strict import (
    OFFLINE_DFL_PANEL_STRICT_LP_STRATEGY_KIND,
    build_offline_dfl_panel_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.promotion_gate import (
    evaluate_offline_dfl_action_target_promotion_gate,
    evaluate_offline_dfl_decision_target_promotion_gate,
    evaluate_offline_dfl_panel_development_gate,
    evaluate_offline_dfl_panel_strict_promotion_gate,
)
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
from smart_arbitrage.dfl.action_classifier import (
    DFL_ACTION_CLASSIFIER_STRICT_LP_STRATEGY_KIND,
    DFL_VALUE_AWARE_ACTION_CLASSIFIER_STRICT_LP_STRATEGY_KIND,
    build_dfl_action_classifier_baseline_frame,
    build_dfl_action_classifier_strict_lp_benchmark_frame,
    build_dfl_value_aware_action_classifier_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.data_expansion import (
    build_dfl_action_label_panel_frame,
    build_dfl_data_coverage_audit_frame,
)
from smart_arbitrage.dfl.failure_analysis import (
    build_dfl_action_classifier_failure_analysis_frame,
)
from smart_arbitrage.dfl.trajectory_value import (
    TRAJECTORY_VALUE_SELECTOR_STRICT_LP_STRATEGY_KIND,
    build_dfl_trajectory_value_candidate_panel_frame,
    build_dfl_trajectory_value_selector_frame,
    build_dfl_trajectory_value_selector_strict_lp_benchmark_frame,
    evaluate_dfl_trajectory_value_selector_gate,
)


class DflTrainingAssetConfig(dg.Config):
    """DFL training-table behavior for thesis-grade benchmark rows."""

    require_thesis_grade: bool = True


class DflDataCoverageAuditAssetConfig(dg.Config):
    """UA-first observed data coverage audit for DFL panel readiness."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    target_anchor_count_per_tenant: int = 90
    required_past_hours: int = 168
    horizon_hours: int = 24


class DflActionLabelPanelAssetConfig(dg.Config):
    """Strict LP/oracle action-label panel scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_holdout_anchor_count_per_tenant: int = 18


class DflActionClassifierBaselineAssetConfig(dg.Config):
    """Transparent supervised action classifier baseline over DFL action labels."""

    baseline_name: str = "dfl_action_classifier_v0"


class DflActionClassifierStrictLpProjectionAssetConfig(dg.Config):
    """Strict LP projection scope for the supervised action classifier baseline."""

    baseline_name: str = "dfl_action_classifier_v0"


class DflValueAwareActionClassifierStrictLpProjectionAssetConfig(dg.Config):
    """Strict LP projection scope for the value-aware action classifier baseline."""

    baseline_name: str = "dfl_value_aware_action_classifier_v1"
    value_weight_scale_uah: float = 500.0


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


class OfflineDflPanelStrictLpBenchmarkAssetConfig(dg.Config):
    """Strict LP/oracle promotion-gate scope for the all-tenant offline DFL panel."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_validation_anchor_count_per_tenant: int = 18


class OfflineDflDecisionTargetAssetConfig(dg.Config):
    """Decision-targeted v3 strict LP candidate scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_validation_anchor_count_per_tenant: int = 18
    max_train_anchors_per_tenant: int = 72
    inner_validation_fraction: float = 0.2
    spread_scale_grid_csv: str = "0.75,1.0,1.25,1.5"
    mean_shift_grid_uah_mwh_csv: str = "-500.0,0.0,500.0"
    include_panel_v2_bias_options_csv: str = "false,true"


class OfflineDflActionTargetAssetConfig(dg.Config):
    """Action-targeted v4 strict LP candidate scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_validation_anchor_count_per_tenant: int = 18
    max_train_anchors_per_tenant: int = 72
    inner_validation_fraction: float = 0.2
    charge_hour_count_grid_csv: str = "2,3"
    discharge_hour_count_grid_csv: str = "2,3"
    action_spread_grid_uah_mwh_csv: str = "500.0,1000.0,1500.0"
    include_panel_v2_bias_options_csv: str = "false,true"
    include_decision_v3_correction_options_csv: str = "false,true"


class OfflineDflTrajectoryValueSelectorAssetConfig(dg.Config):
    """Prior-only trajectory/value selector over strict LP-scored schedules."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_validation_anchor_count_per_tenant: int = 18
    max_train_anchors_per_tenant: int = 72
    min_final_holdout_tenant_anchor_count_per_source_model: int = 90


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
        ml_stage="diagnostics",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_data_coverage_audit_frame(
    context,
    config: DflDataCoverageAuditAssetConfig,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Observed OREE/Open-Meteo coverage ceiling for larger DFL panels."""

    audit_frame = build_dfl_data_coverage_audit_frame(
        real_data_benchmark_silver_feature_frame,
        real_data_rolling_origin_benchmark_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        target_anchor_count_per_tenant=config.target_anchor_count_per_tenant,
        required_past_hours=config.required_past_hours,
        horizon_hours=config.horizon_hours,
    )
    _add_metadata(
        context,
        {
            "rows": audit_frame.height,
            "tenant_count": audit_frame.select("tenant_id").n_unique() if audit_frame.height else 0,
            "target_anchor_count_per_tenant": config.target_anchor_count_per_tenant,
            "minimum_eligible_anchor_count": audit_frame.select("eligible_anchor_count").min().item()
            if audit_frame.height
            else 0,
            "scope": "ua_observed_dfl_data_coverage_audit_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return audit_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_action_label_panel_frame(
    context,
    config: DflActionLabelPanelAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    dfl_data_coverage_audit_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Oracle action-label sidecar panel for future DFL training."""

    action_label_frame = build_dfl_action_label_panel_frame(
        real_data_rolling_origin_benchmark_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=_forecast_model_names(config.forecast_model_names_csv),
        final_holdout_anchor_count_per_tenant=config.final_holdout_anchor_count_per_tenant,
    )
    get_dfl_training_store().upsert_action_label_frame(action_label_frame)
    final_holdout_rows = (
        action_label_frame.filter(pl.col("is_final_holdout"))
        if action_label_frame.height
        else pl.DataFrame()
    )
    _add_metadata(
        context,
        {
            "rows": action_label_frame.height,
            "tenant_count": action_label_frame.select("tenant_id").n_unique()
            if action_label_frame.height
            else 0,
            "source_model_count": action_label_frame.select("forecast_model_name").n_unique()
            if action_label_frame.height
            else 0,
            "final_holdout_rows": final_holdout_rows.height,
            "coverage_audit_rows": dfl_data_coverage_audit_frame.height,
            "scope": "dfl_action_label_panel_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return action_label_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_action_classifier_baseline_frame(
    context,
    config: DflActionClassifierBaselineAssetConfig,
    dfl_action_label_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Transparent supervised baseline for future DFL action-label training."""

    classifier_frame = build_dfl_action_classifier_baseline_frame(
        dfl_action_label_panel_frame,
        baseline_name=config.baseline_name,
    )
    final_holdout_summary = classifier_frame.filter(
        (pl.col("split_name") == "final_holdout")
        & (pl.col("forecast_model_name") == "all_source_models")
    )
    _add_metadata(
        context,
        {
            "rows": classifier_frame.height,
            "baseline_name": config.baseline_name,
            "final_holdout_accuracy": final_holdout_summary.select("accuracy").item()
            if final_holdout_summary.height
            else None,
            "final_holdout_macro_f1": final_holdout_summary.select("macro_f1").item()
            if final_holdout_summary.height
            else None,
            "promotion_status": final_holdout_summary.select("promotion_status").item()
            if final_holdout_summary.height
            else "blocked_classification_only_no_strict_lp_value",
            "scope": "dfl_action_classifier_baseline_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return classifier_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_action_classifier_strict_lp_benchmark_frame(
    context,
    config: DflActionClassifierStrictLpProjectionAssetConfig,
    dfl_action_label_panel_frame: pl.DataFrame,
    dfl_action_classifier_baseline_frame: pl.DataFrame,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle value check for supervised action-classifier labels."""

    strict_frame = build_dfl_action_classifier_strict_lp_benchmark_frame(
        dfl_action_label_panel_frame,
        real_data_rolling_origin_benchmark_frame,
        baseline_name=config.baseline_name,
        generated_at=_latest_generated_at(dfl_action_label_panel_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    candidate_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with(f"{config.baseline_name}_")
    )
    strict_rows = strict_frame.filter(pl.col("forecast_model_name") == "strict_similar_day")
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "candidate_rows": candidate_rows.height,
            "strict_control_rows": strict_rows.height,
            "classifier_summary_rows": dfl_action_classifier_baseline_frame.height,
            "mean_candidate_regret_uah": candidate_rows.select("regret_uah").mean().item()
            if candidate_rows.height
            else None,
            "mean_strict_regret_uah": strict_rows.select("regret_uah").mean().item()
            if strict_rows.height
            else None,
            "strategy_kind": DFL_ACTION_CLASSIFIER_STRICT_LP_STRATEGY_KIND,
            "scope": "dfl_action_classifier_strict_lp_projection_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return strict_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_value_aware_action_classifier_strict_lp_benchmark_frame(
    context,
    config: DflValueAwareActionClassifierStrictLpProjectionAssetConfig,
    dfl_action_label_panel_frame: pl.DataFrame,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle value check for value-weighted supervised action labels."""

    strict_frame = build_dfl_value_aware_action_classifier_strict_lp_benchmark_frame(
        dfl_action_label_panel_frame,
        real_data_rolling_origin_benchmark_frame,
        baseline_name=config.baseline_name,
        value_weight_scale_uah=config.value_weight_scale_uah,
        generated_at=_latest_generated_at(dfl_action_label_panel_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    candidate_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with(f"{config.baseline_name}_")
    )
    strict_rows = strict_frame.filter(pl.col("forecast_model_name") == "strict_similar_day")
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "candidate_rows": candidate_rows.height,
            "strict_control_rows": strict_rows.height,
            "value_weight_scale_uah": config.value_weight_scale_uah,
            "mean_candidate_regret_uah": candidate_rows.select("regret_uah").mean().item()
            if candidate_rows.height
            else None,
            "mean_strict_regret_uah": strict_rows.select("regret_uah").mean().item()
            if strict_rows.height
            else None,
            "strategy_kind": DFL_VALUE_AWARE_ACTION_CLASSIFIER_STRICT_LP_STRATEGY_KIND,
            "scope": "dfl_value_aware_action_classifier_strict_lp_projection_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return strict_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="diagnostics",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_action_classifier_failure_analysis_frame(
    context,
    dfl_action_label_panel_frame: pl.DataFrame,
    dfl_action_classifier_strict_lp_benchmark_frame: pl.DataFrame,
    dfl_value_aware_action_classifier_strict_lp_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Diagnostics explaining why action-classifier probes are blocked."""

    failure_frame = build_dfl_action_classifier_failure_analysis_frame(
        dfl_action_label_panel_frame,
        dfl_action_classifier_strict_lp_benchmark_frame,
        dfl_value_aware_action_classifier_strict_lp_benchmark_frame,
    )
    _add_metadata(
        context,
        {
            "rows": failure_frame.height,
            "tenant_count": failure_frame.select("tenant_id").n_unique()
            if failure_frame.height
            else 0,
            "source_model_count": failure_frame.select("source_model_name").n_unique()
            if failure_frame.height
            else 0,
            "variant_count": failure_frame.select("classifier_variant").n_unique()
            if failure_frame.height
            else 0,
            "scope": "dfl_action_classifier_failure_analysis_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return failure_frame


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
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def offline_dfl_panel_strict_lp_benchmark_frame(
    context,
    config: OfflineDflPanelStrictLpBenchmarkAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    offline_dfl_panel_experiment_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle benchmark for panel v2 candidates against the frozen control."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_panel_frame = build_offline_dfl_panel_strict_lp_benchmark_frame(
        real_data_rolling_origin_benchmark_frame,
        offline_dfl_panel_experiment_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        generated_at=_latest_generated_at(real_data_rolling_origin_benchmark_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_panel_frame)
    promotion_gate = evaluate_offline_dfl_panel_strict_promotion_gate(
        strict_panel_frame,
        source_model_names=source_model_names,
    )
    v2_rows = strict_panel_frame.filter(pl.col("forecast_model_name").str.starts_with("offline_dfl_panel_v2_"))
    _add_metadata(
        context,
        {
            "rows": strict_panel_frame.height,
            "tenant_count": strict_panel_frame.select("tenant_id").n_unique()
            if strict_panel_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "v2_validation_tenant_anchor_count": v2_rows.height,
            "strategy_kind": OFFLINE_DFL_PANEL_STRICT_LP_STRATEGY_KIND,
            "promotion_gate_decision": promotion_gate.decision,
            "promotion_gate_description": promotion_gate.description,
            "scope": "offline_dfl_panel_strict_lp_gate_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return strict_panel_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def offline_dfl_decision_target_panel_frame(
    context,
    config: OfflineDflDecisionTargetAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    offline_dfl_panel_experiment_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Decision-targeted v3 parameter selection from prior strict LP/oracle regret."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    decision_panel_frame = build_offline_dfl_decision_target_panel_frame(
        real_data_rolling_origin_benchmark_frame,
        offline_dfl_panel_experiment_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        max_train_anchors_per_tenant=config.max_train_anchors_per_tenant,
        inner_validation_fraction=config.inner_validation_fraction,
        spread_scale_grid=_float_csv_values(config.spread_scale_grid_csv, field_name="spread_scale_grid_csv"),
        mean_shift_grid_uah_mwh=_float_csv_values(
            config.mean_shift_grid_uah_mwh_csv,
            field_name="mean_shift_grid_uah_mwh_csv",
        ),
        include_panel_v2_bias_options=_bool_csv_values(
            config.include_panel_v2_bias_options_csv,
            field_name="include_panel_v2_bias_options_csv",
        ),
    )
    _add_metadata(
        context,
        {
            "rows": decision_panel_frame.height,
            "tenant_count": decision_panel_frame.select("tenant_id").n_unique()
            if decision_panel_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "scope": "offline_dfl_decision_target_v3_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return decision_panel_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def offline_dfl_decision_target_strict_lp_benchmark_frame(
    context,
    config: OfflineDflDecisionTargetAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    offline_dfl_panel_experiment_frame: pl.DataFrame,
    offline_dfl_decision_target_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle benchmark for decision-targeted v3 candidates."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_offline_dfl_decision_target_strict_lp_benchmark_frame(
        real_data_rolling_origin_benchmark_frame,
        offline_dfl_panel_experiment_frame,
        offline_dfl_decision_target_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        generated_at=_latest_generated_at(real_data_rolling_origin_benchmark_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    promotion_gate = evaluate_offline_dfl_decision_target_promotion_gate(
        strict_frame,
        source_model_names=source_model_names,
    )
    v3_rows = strict_frame.filter(pl.col("forecast_model_name").str.starts_with("offline_dfl_decision_target_v3_"))
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique() if strict_frame.height else 0,
            "source_model_count": len(source_model_names),
            "v3_validation_tenant_anchor_count": v3_rows.height,
            "strategy_kind": DECISION_TARGET_STRICT_LP_STRATEGY_KIND,
            "promotion_gate_decision": promotion_gate.decision,
            "promotion_gate_description": promotion_gate.description,
            "scope": "offline_dfl_decision_target_v3_strict_lp_gate_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return strict_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def offline_dfl_action_target_panel_frame(
    context,
    config: OfflineDflActionTargetAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    offline_dfl_panel_experiment_frame: pl.DataFrame,
    offline_dfl_decision_target_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Action-targeted v4 parameter selection from prior strict LP/oracle regret."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    action_panel_frame = build_offline_dfl_action_target_panel_frame(
        real_data_rolling_origin_benchmark_frame,
        offline_dfl_panel_experiment_frame,
        offline_dfl_decision_target_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        max_train_anchors_per_tenant=config.max_train_anchors_per_tenant,
        inner_validation_fraction=config.inner_validation_fraction,
        charge_hour_count_grid=_int_csv_values(
            config.charge_hour_count_grid_csv,
            field_name="charge_hour_count_grid_csv",
        ),
        discharge_hour_count_grid=_int_csv_values(
            config.discharge_hour_count_grid_csv,
            field_name="discharge_hour_count_grid_csv",
        ),
        action_spread_grid_uah_mwh=_float_csv_values(
            config.action_spread_grid_uah_mwh_csv,
            field_name="action_spread_grid_uah_mwh_csv",
        ),
        include_panel_v2_bias_options=_bool_csv_values(
            config.include_panel_v2_bias_options_csv,
            field_name="include_panel_v2_bias_options_csv",
        ),
        include_decision_v3_correction_options=_bool_csv_values(
            config.include_decision_v3_correction_options_csv,
            field_name="include_decision_v3_correction_options_csv",
        ),
    )
    _add_metadata(
        context,
        {
            "rows": action_panel_frame.height,
            "tenant_count": action_panel_frame.select("tenant_id").n_unique()
            if action_panel_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "scope": "offline_dfl_action_target_v4_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return action_panel_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def offline_dfl_action_target_strict_lp_benchmark_frame(
    context,
    config: OfflineDflActionTargetAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    offline_dfl_panel_experiment_frame: pl.DataFrame,
    offline_dfl_decision_target_panel_frame: pl.DataFrame,
    offline_dfl_action_target_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle benchmark for action-targeted v4 candidates."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_offline_dfl_action_target_strict_lp_benchmark_frame(
        real_data_rolling_origin_benchmark_frame,
        offline_dfl_panel_experiment_frame,
        offline_dfl_decision_target_panel_frame,
        offline_dfl_action_target_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        generated_at=_latest_generated_at(real_data_rolling_origin_benchmark_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    promotion_gate = evaluate_offline_dfl_action_target_promotion_gate(
        strict_frame,
        source_model_names=source_model_names,
    )
    v4_rows = strict_frame.filter(pl.col("forecast_model_name").str.starts_with("offline_dfl_action_target_v4_"))
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique() if strict_frame.height else 0,
            "source_model_count": len(source_model_names),
            "v4_validation_tenant_anchor_count": v4_rows.height,
            "strategy_kind": ACTION_TARGET_STRICT_LP_STRATEGY_KIND,
            "promotion_gate_decision": promotion_gate.decision,
            "promotion_gate_description": promotion_gate.description,
            "scope": "offline_dfl_action_target_v4_strict_lp_gate_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return strict_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_trajectory_value_candidate_panel_frame(
    context,
    config: OfflineDflTrajectoryValueSelectorAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    offline_dfl_panel_strict_lp_benchmark_frame: pl.DataFrame,
    offline_dfl_decision_target_strict_lp_benchmark_frame: pl.DataFrame,
    offline_dfl_action_target_strict_lp_benchmark_frame: pl.DataFrame,
    offline_dfl_panel_experiment_frame: pl.DataFrame,
    offline_dfl_decision_target_panel_frame: pl.DataFrame,
    offline_dfl_action_target_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Feasible strict-LP trajectory candidates plus prior-only selection metrics."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    candidate_panel = build_dfl_trajectory_value_candidate_panel_frame(
        real_data_rolling_origin_benchmark_frame,
        offline_dfl_panel_strict_lp_benchmark_frame,
        offline_dfl_decision_target_strict_lp_benchmark_frame,
        offline_dfl_action_target_strict_lp_benchmark_frame,
        offline_dfl_panel_experiment_frame,
        offline_dfl_decision_target_panel_frame,
        offline_dfl_action_target_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        max_train_anchors_per_tenant=config.max_train_anchors_per_tenant,
    )
    _add_metadata(
        context,
        {
            "rows": candidate_panel.height,
            "tenant_count": candidate_panel.select("tenant_id").n_unique() if candidate_panel.height else 0,
            "source_model_count": len(source_model_names),
            "candidate_family_count": candidate_panel.select("candidate_family").n_unique()
            if candidate_panel.height
            else 0,
            "scope": "dfl_trajectory_value_candidate_panel_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return candidate_panel


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_trajectory_value_selector_frame(
    context,
    config: OfflineDflTrajectoryValueSelectorAssetConfig,
    dfl_trajectory_value_candidate_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Select one prior-best feasible schedule family per tenant/source model."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    selector_frame = build_dfl_trajectory_value_selector_frame(
        dfl_trajectory_value_candidate_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        min_final_holdout_tenant_anchor_count_per_source_model=(
            config.min_final_holdout_tenant_anchor_count_per_source_model
        ),
    )
    _add_metadata(
        context,
        {
            "rows": selector_frame.height,
            "tenant_count": selector_frame.select("tenant_id").n_unique() if selector_frame.height else 0,
            "source_model_count": len(source_model_names),
            "development_gate_rows": selector_frame.filter(pl.col("development_gate_passed")).height
            if selector_frame.height
            else 0,
            "production_promotion_rows": selector_frame.filter(pl.col("production_promotion_passed")).height
            if selector_frame.height
            else 0,
            "scope": "dfl_trajectory_value_selector_v1_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return selector_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_trajectory_value_selector_strict_lp_benchmark_frame(
    context,
    config: OfflineDflTrajectoryValueSelectorAssetConfig,
    dfl_trajectory_value_candidate_panel_frame: pl.DataFrame,
    dfl_trajectory_value_selector_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle rows for the trajectory/value selector gate."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_trajectory_value_selector_strict_lp_benchmark_frame(
        dfl_trajectory_value_candidate_panel_frame,
        dfl_trajectory_value_selector_frame,
        generated_at=_latest_generated_at(dfl_trajectory_value_candidate_panel_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    promotion_gate = evaluate_dfl_trajectory_value_selector_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=config.min_final_holdout_tenant_anchor_count_per_source_model,
    )
    selector_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("dfl_trajectory_value_selector_v1_")
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique() if strict_frame.height else 0,
            "source_model_count": len(source_model_names),
            "selector_validation_tenant_anchor_count": selector_rows.height,
            "strategy_kind": TRAJECTORY_VALUE_SELECTOR_STRICT_LP_STRATEGY_KIND,
            "promotion_gate_decision": promotion_gate.decision,
            "promotion_gate_description": promotion_gate.description,
            "development_gate_passed": promotion_gate.metrics.get("development_gate_passed", False),
            "scope": "dfl_trajectory_value_selector_v1_strict_lp_gate_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return strict_frame


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
    dfl_data_coverage_audit_frame,
    dfl_action_label_panel_frame,
    dfl_action_classifier_baseline_frame,
    dfl_action_classifier_strict_lp_benchmark_frame,
    dfl_value_aware_action_classifier_strict_lp_benchmark_frame,
    dfl_action_classifier_failure_analysis_frame,
    regret_weighted_dfl_pilot_frame,
    dfl_relaxed_lp_pilot_frame,
    offline_dfl_experiment_frame,
    offline_dfl_panel_experiment_frame,
    offline_dfl_panel_strict_lp_benchmark_frame,
    offline_dfl_decision_target_panel_frame,
    offline_dfl_decision_target_strict_lp_benchmark_frame,
    offline_dfl_action_target_panel_frame,
    offline_dfl_action_target_strict_lp_benchmark_frame,
    dfl_trajectory_value_candidate_panel_frame,
    dfl_trajectory_value_selector_frame,
    dfl_trajectory_value_selector_strict_lp_benchmark_frame,
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


def _float_csv_values(raw_value: str, *, field_name: str) -> tuple[float, ...]:
    values: list[float] = []
    for raw_part in raw_value.split(","):
        part = raw_part.strip()
        if not part:
            continue
        try:
            values.append(float(part))
        except ValueError as exc:
            raise ValueError(f"{field_name} must contain only numeric values.") from exc
    if not values:
        raise ValueError(f"{field_name} must contain at least one value.")
    return tuple(values)


def _int_csv_values(raw_value: str, *, field_name: str) -> tuple[int, ...]:
    values: list[int] = []
    for raw_part in raw_value.split(","):
        part = raw_part.strip()
        if not part:
            continue
        try:
            values.append(int(part))
        except ValueError as exc:
            raise ValueError(f"{field_name} must contain only integer values.") from exc
    if not values:
        raise ValueError(f"{field_name} must contain at least one value.")
    return tuple(values)


def _bool_csv_values(raw_value: str, *, field_name: str) -> tuple[bool, ...]:
    values: list[bool] = []
    for raw_part in raw_value.split(","):
        part = raw_part.strip().lower()
        if not part:
            continue
        if part in {"true", "1", "yes"}:
            values.append(True)
        elif part in {"false", "0", "no"}:
            values.append(False)
        else:
            raise ValueError(f"{field_name} must contain boolean values.")
    if not values:
        raise ValueError(f"{field_name} must contain at least one value.")
    return tuple(values)


def _latest_generated_at(frame: pl.DataFrame) -> datetime | None:
    if frame.height == 0 or "generated_at" not in frame.columns:
        return None
    values = [
        value
        for value in frame.select("generated_at").to_series().to_list()
        if isinstance(value, datetime)
    ]
    return max(values) if values else None


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
