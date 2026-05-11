from datetime import UTC, datetime
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
from smart_arbitrage.dfl.coverage_repair import (
    build_dfl_ua_coverage_repair_audit_frame,
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
from smart_arbitrage.dfl.trajectory_ranker import (
    DFL_TRAJECTORY_FEATURE_RANKER_STRICT_LP_STRATEGY_KIND,
    build_dfl_schedule_candidate_library_from_strict_benchmark_frame,
    build_dfl_schedule_candidate_library_frame,
    build_dfl_trajectory_feature_ranker_frame,
    build_dfl_trajectory_feature_ranker_strict_lp_benchmark_frame,
    evaluate_dfl_trajectory_feature_ranker_gate,
)
from smart_arbitrage.dfl.strict_challenger import (
    build_dfl_non_strict_oracle_upper_bound_frame as build_non_strict_oracle_upper_bound_frame,
    build_dfl_pipeline_integrity_audit_frame as build_pipeline_integrity_audit_frame,
    build_dfl_schedule_candidate_library_v2_frame as build_schedule_candidate_library_v2_frame,
    build_dfl_strict_baseline_autopsy_frame as build_strict_baseline_autopsy_frame,
    validate_dfl_non_strict_upper_bound_evidence,
)
from smart_arbitrage.dfl.strict_failure_selector import (
    DFL_STRICT_FAILURE_SELECTOR_STRICT_LP_STRATEGY_KIND,
    build_dfl_strict_failure_selector_frame,
    build_dfl_strict_failure_selector_strict_lp_benchmark_frame,
    evaluate_dfl_strict_failure_selector_gate,
)
from smart_arbitrage.dfl.strict_failure_robustness import (
    build_dfl_strict_failure_selector_robustness_frame,
    evaluate_dfl_strict_failure_selector_robustness_gate,
)
from smart_arbitrage.dfl.strict_failure_features import (
    build_dfl_strict_failure_feature_audit_frame,
    build_dfl_strict_failure_prior_feature_panel_frame,
)
from smart_arbitrage.dfl.strict_failure_feature_selector import (
    DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_STRICT_LP_STRATEGY_KIND,
    build_dfl_feature_aware_strict_failure_selector_frame,
    build_dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame,
    evaluate_dfl_feature_aware_strict_failure_selector_gate,
)
from smart_arbitrage.dfl.regime_gated_tft_selector import (
    DFL_REGIME_GATED_TFT_SELECTOR_V2_STRATEGY_KIND,
    build_dfl_regime_gated_tft_selector_v2_frame,
    build_dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame,
    evaluate_dfl_regime_gated_tft_selector_v2_gate,
)
from smart_arbitrage.dfl.forecast_dfl_v1 import (
    DFL_FORECAST_DFL_V1_STRICT_LP_STRATEGY_KIND,
    build_dfl_forecast_dfl_v1_panel_frame,
    build_dfl_forecast_dfl_v1_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.offline_dt_candidate import (
    DFL_OFFLINE_DT_STRICT_LP_STRATEGY_KIND,
    build_dfl_offline_dt_candidate_frame,
    build_dfl_offline_dt_candidate_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.residual_schedule_value import (
    DFL_RESIDUAL_DT_FALLBACK_STRICT_LP_STRATEGY_KIND,
    DFL_RESIDUAL_SCHEDULE_VALUE_STRICT_LP_STRATEGY_KIND,
    build_dfl_residual_dt_fallback_strict_lp_benchmark_frame,
    build_dfl_residual_schedule_value_model_frame,
    build_dfl_residual_schedule_value_strict_lp_benchmark_frame,
    evaluate_dfl_residual_dt_fallback_gate,
)
from smart_arbitrage.dfl.trajectory_dataset import (
    build_dfl_real_data_trajectory_dataset_frame,
)
from smart_arbitrage.dfl.source_specific_challenger import (
    build_dfl_source_specific_research_challenger_frame,
    evaluate_dfl_source_specific_research_challenger_gate,
)
from smart_arbitrage.dfl.schedule_value_learner import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_LP_STRATEGY_KIND,
    build_dfl_schedule_value_learner_v2_frame,
    build_dfl_schedule_value_learner_v2_strict_lp_benchmark_frame,
    evaluate_dfl_schedule_value_learner_v2_gate,
)
from smart_arbitrage.dfl.schedule_value_learner_robustness import (
    build_dfl_schedule_value_learner_v2_robustness_frame,
    evaluate_dfl_schedule_value_learner_v2_robustness_gate,
)
from smart_arbitrage.dfl.schedule_value_promotion_gate import (
    build_dfl_schedule_value_production_gate_frame,
    evaluate_dfl_schedule_value_production_gate,
)
from smart_arbitrage.dfl.production_promotion_gate import (
    build_dfl_production_promotion_gate_frame,
    evaluate_dfl_production_promotion_gate,
)
from smart_arbitrage.dfl.forecast_pipeline_truth import (
    build_forecast_pipeline_truth_audit_frame as build_forecast_pipeline_truth_audit,
    validate_forecast_pipeline_truth_audit_evidence,
)
from smart_arbitrage.forecasting.afl import (
    build_afl_training_panel_frame,
    build_forecast_candidate_forensics_frame,
)
from smart_arbitrage.forecasting.afl_error_audit import (
    build_afl_forecast_error_audit_frame,
)
from smart_arbitrage.forecasting.afe import build_forecast_afe_feature_catalog_frame
from smart_arbitrage.forecasting.market_coupling_availability import (
    build_market_coupling_temporal_availability_frame,
)
from smart_arbitrage.forecasting.entsoe_neighbor_access import (
    build_entsoe_neighbor_market_query_spec_frame,
)
from smart_arbitrage.forecasting.grid_event_signals import build_grid_event_signal_frame
from smart_arbitrage.dfl.semantic_event_failure_audit import (
    build_dfl_semantic_event_strict_failure_audit_frame,
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


class DflUaCoverageRepairAuditAssetConfig(dg.Config):
    """Exact UA OREE/Open-Meteo gap repair audit scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    target_anchor_count_per_tenant: int = 180


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


class DflTrajectoryFeatureRankerAssetConfig(dg.Config):
    """Prior-only feature ranker over feasible LP-scored schedule candidates."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_validation_anchor_count_per_tenant: int = 18
    perturb_spread_scale_grid_csv: str = "0.9,1.1"
    perturb_mean_shift_grid_uah_mwh_csv: str = "-250.0,250.0"
    min_final_holdout_tenant_anchor_count_per_source_model: int = 90


class DflStrictChallengerAssetConfig(dg.Config):
    """Strict-control challenger diagnostics and candidate library scope."""

    blend_weights_csv: str = "0.25,0.5,0.75"
    residual_min_prior_anchors: int = 3
    min_final_holdout_tenant_anchor_count_per_source_model: int = 90


class DflStrictFailureSelectorAssetConfig(dg.Config):
    """Prior-only selector that learns when strict control is likely to fail."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    switch_threshold_grid_uah_csv: str = "0.0,50.0,100.0,200.0,400.0"
    min_prior_anchor_count: int = 3
    min_final_holdout_tenant_anchor_count_per_source_model: int = 90


class DflStrictFailureSelectorRobustnessAssetConfig(dg.Config):
    """Rolling-window robustness evidence for the strict-failure selector."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    validation_window_count: int = 4
    validation_anchor_count: int = 18
    min_prior_anchors_before_window: int = 30
    min_prior_anchor_count: int = 3
    switch_threshold_grid_uah_csv: str = "0.0,50.0,100.0,200.0,400.0"
    min_robust_passing_windows: int = 3
    min_validation_tenant_anchor_count_per_source_model: int = 90


class DflStrictFailureFeatureAuditAssetConfig(dg.Config):
    """Prior-window feature audit scope for strict-failure selector behavior."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    validation_window_count: int = 4
    validation_anchor_count: int = 18
    min_prior_anchors_before_window: int = 30
    min_prior_anchor_count: int = 3


class DflFeatureAwareStrictFailureSelectorAssetConfig(dg.Config):
    """Feature-aware prior-only strict-failure selector scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_window_index: int = 1
    min_training_window_count: int = 3
    switch_threshold_grid_uah_csv: str = "0.0,50.0,100.0,200.0,400.0"
    rank_overlap_floor_grid_csv: str = "0.0,0.5,0.75"
    price_regime_policies_csv: str = "all,low_medium,high_only"
    volatility_policies_csv: str = "all,non_volatile"
    min_validation_tenant_anchor_count_per_source_model: int = 90


class DflRegimeGatedTftSelectorV2AssetConfig(dg.Config):
    """Regime-gated prior-only TFT selector v2 scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    tft_source_model_name: str = "tft_silver_v0"
    min_training_window_count: int = 3
    min_mean_regret_improvement_ratio: float = 0.05
    min_validation_tenant_anchor_count_per_source_model: int = 90


class DflForecastDflV1AssetConfig(dg.Config):
    """Tiny decision-loss DFL v1 correction scope."""

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


class DflRealDataTrajectoryDatasetAssetConfig(dg.Config):
    """Real-data trajectory dataset scope for residual DFL and offline DT."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_validation_anchor_count_per_tenant: int = 18


class DflResidualScheduleValueAssetConfig(dg.Config):
    """Prior-only residual schedule/value selector scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_validation_anchor_count_per_tenant: int = 18
    switch_margin_grid_uah_csv: str = "0.0,50.0,100.0,200.0,400.0"


class DflOfflineDtCandidateAssetConfig(dg.Config):
    """Tiny offline DT candidate scope over high-value train trajectories."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_validation_anchor_count_per_tenant: int = 18
    high_value_quantile: float = 0.75
    context_length: int = 24
    hidden_dim: int = 32
    num_layers: int = 1
    num_heads: int = 2
    max_epochs: int = 5
    random_seed: int = 2026


class DflResidualDtFallbackAssetConfig(dg.Config):
    """Strict fallback wrapper for residual DFL and offline DT challengers."""

    final_validation_anchor_count_per_tenant: int = 18
    min_confidence_improvement_ratio: float = 0.05
    min_validation_tenant_anchor_count_per_source_model: int = 90


class DflSourceSpecificResearchChallengerAssetConfig(dg.Config):
    """Source-specific TFT/NBEATSx research challenger gate scope."""

    source_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    min_tenant_count: int = 5
    min_validation_tenant_anchor_count_per_source_model: int = 90
    min_mean_regret_improvement_ratio: float = 0.05
    min_rolling_strict_pass_windows: int = 3
    min_rolling_window_count: int = 4


class DflScheduleValueLearnerV2AssetConfig(dg.Config):
    """Prior-only schedule/value learner v2 scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_validation_anchor_count_per_tenant: int = 18
    min_validation_tenant_anchor_count_per_source_model: int = 90


class DflScheduleValueLearnerV2RobustnessAssetConfig(dg.Config):
    """Rolling-window robustness evidence for schedule/value learner v2."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    validation_window_count: int = 4
    validation_anchor_count: int = 18
    min_prior_anchors_before_window: int = 30
    min_robust_passing_windows: int = 3
    min_validation_tenant_anchor_count_per_source_model: int = 90


class DflScheduleValueProductionGateAssetConfig(dg.Config):
    """Offline/read-model promotion gate for schedule/value learner v2 evidence."""

    source_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    min_tenant_count: int = 5
    min_validation_tenant_anchor_count_per_source_model: int = 90
    min_mean_regret_improvement_ratio: float = 0.05
    min_rolling_window_count: int = 4
    min_rolling_strict_pass_windows: int = 3


class DflOfficialScheduleCandidateLibraryAssetConfig(dg.Config):
    """Official NBEATSx/TFT schedule-candidate library scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "nbeatsx_official_v0,tft_official_v0"
    final_validation_anchor_count_per_tenant: int = 18
    perturb_spread_scale_grid_csv: str = "0.9,1.1"
    perturb_mean_shift_grid_uah_mwh_csv: str = "-250.0,250.0"


class DflOfficialScheduleValueLearnerV2AssetConfig(dg.Config):
    """Official forecast schedule/value learner v2 scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "nbeatsx_official_v0,tft_official_v0"
    final_validation_anchor_count_per_tenant: int = 18
    min_validation_tenant_anchor_count_per_source_model: int = 90


class DflOfficialScheduleValueLearnerV2RobustnessAssetConfig(dg.Config):
    """Rolling-window robustness evidence for official schedule/value learner v2."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "nbeatsx_official_v0,tft_official_v0"
    validation_window_count: int = 4
    validation_anchor_count: int = 18
    min_prior_anchors_before_window: int = 30
    min_robust_passing_windows: int = 3
    min_validation_tenant_anchor_count_per_source_model: int = 90


class DflOfficialScheduleValueProductionGateAssetConfig(dg.Config):
    """Offline promotion gate for official schedule/value learner v2 evidence."""

    source_model_names_csv: str = "nbeatsx_official_v0,tft_official_v0"
    min_tenant_count: int = 5
    min_validation_tenant_anchor_count_per_source_model: int = 90
    min_mean_regret_improvement_ratio: float = 0.05
    min_rolling_window_count: int = 4
    min_rolling_strict_pass_windows: int = 3


class DflProductionPromotionGateAssetConfig(dg.Config):
    """Offline/read-model production-promotion gate scope."""

    source_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    min_tenant_count: int = 5
    min_validation_tenant_anchor_count_per_source_model: int = 90
    min_mean_regret_improvement_ratio: float = 0.05
    min_rolling_strict_pass_windows: int = 3
    min_rolling_window_count: int = 4
    backfill_target_anchor_count_per_tenant: int = 180


class DflForecastPipelineTruthAuditAssetConfig(dg.Config):
    """Forecast-vector truth-audit scope before serious DFL reruns."""

    price_floor_uah_mwh: float = 0.0
    price_cap_uah_mwh: float = 16_000.0
    horizon_shift_offsets_csv: str = "-2,-1,0,1,2"


class AflTrainingPanelAssetConfig(dg.Config):
    """Arbitrage-focused forecast-learning panel scope."""

    final_holdout_anchor_count_per_tenant: int = 18


class AflForecastErrorAuditAssetConfig(dg.Config):
    """AFL forecast failure classification thresholds."""

    spread_shape_failure_threshold_ratio: float = 0.25
    rank_extrema_failure_threshold: float = 0.5
    lp_value_failure_margin_uah: float = 0.0


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
        ml_stage="feature_engineering",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def forecast_afe_feature_catalog_frame(context) -> pl.DataFrame:
    """Sidecar AFE catalog separating usable UA signals from future bridges."""

    catalog_frame = build_forecast_afe_feature_catalog_frame()
    _add_metadata(
        context,
        {
            "rows": catalog_frame.height,
            "feature_group_count": catalog_frame.select("feature_group").n_unique()
            if catalog_frame.height
            else 0,
            "implemented_feature_count": catalog_frame.filter(
                pl.col("feature_status") == "implemented"
            ).height
            if catalog_frame.height
            else 0,
            "external_bridge_training_allowed_rows": catalog_frame.filter(
                (pl.col("feature_group") == "external_market_context")
                & (pl.col("training_use_allowed"))
            ).height
            if catalog_frame.height
            else 0,
            "scope": "forecast_afe_feature_catalog_research_sidecar",
            "not_market_execution": True,
        },
    )
    return catalog_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="feature_engineering",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def market_coupling_temporal_availability_frame(
    context,
    forecast_afe_feature_catalog_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Research gate for EU/neighboring-market features before training use."""

    availability_frame = build_market_coupling_temporal_availability_frame(
        forecast_afe_feature_catalog_frame
    )
    _add_metadata(
        context,
        {
            "rows": availability_frame.height,
            "source_count": availability_frame.select("source_name").n_unique()
            if availability_frame.height
            else 0,
            "training_allowed_rows": availability_frame.filter(
                pl.col("training_use_allowed")
            ).height
            if availability_frame.height
            else 0,
            "pricefm_observation_count": availability_frame.filter(
                pl.col("source_name") == "PRICEFM_HF"
            )
            .select("source_observation_count")
            .to_series()
            .item()
            if availability_frame.filter(pl.col("source_name") == "PRICEFM_HF").height
            else 0,
            "scope": "market_coupling_temporal_availability_research_gate",
            "not_market_execution": True,
        },
    )
    return availability_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="feature_engineering",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def entsoe_neighbor_market_query_spec_frame(
    context,
    market_coupling_temporal_availability_frame: pl.DataFrame,
) -> pl.DataFrame:
    """ENTSO-E neighbor-market query spec and access blocker evidence."""

    security_token = os.environ.get("ENTSOE_SECURITY_TOKEN") or os.environ.get(
        "ENTSO_E_SECURITY_TOKEN"
    )
    query_spec_frame = build_entsoe_neighbor_market_query_spec_frame(
        market_coupling_temporal_availability_frame,
        security_token=security_token,
    )
    _add_metadata(
        context,
        {
            "rows": query_spec_frame.height,
            "mapped_eic_rows": query_spec_frame.filter(
                pl.col("eic_mapping_status") == "mapped"
            ).height
            if query_spec_frame.height
            else 0,
            "fetch_allowed_rows": query_spec_frame.filter(pl.col("fetch_allowed")).height
            if query_spec_frame.height
            else 0,
            "security_token_available": bool(security_token),
            "scope": "entsoe_neighbor_market_access_research_gate",
            "not_market_execution": True,
        },
    )
    return query_spec_frame


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
def dfl_semantic_event_strict_failure_audit_frame(
    context,
    dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame: pl.DataFrame,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    ukrenergo_grid_events_bronze: pl.DataFrame,
    forecast_afe_feature_catalog_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Audit whether official grid-event semantics explain strict-control failures."""

    grid_event_signal_frame = build_grid_event_signal_frame(
        price_history=real_data_benchmark_silver_feature_frame,
        grid_events=ukrenergo_grid_events_bronze,
    )
    audit_frame = build_dfl_semantic_event_strict_failure_audit_frame(
        dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame,
        grid_event_signal_frame,
        forecast_afe_feature_catalog_frame,
    )
    _add_metadata(
        context,
        {
            "rows": audit_frame.height,
            "tenant_count": audit_frame.select("tenant_id").n_unique()
            if audit_frame.height
            else 0,
            "source_model_count": audit_frame.select("source_model_name").n_unique()
            if audit_frame.height
            else 0,
            "event_anchor_count": audit_frame.select("event_anchor_count").sum().item()
            if audit_frame.height
            else 0,
            "strict_failure_with_event_count": audit_frame.select(
                "strict_failure_with_event_count"
            ).sum().item()
            if audit_frame.height
            else 0,
            "scope": "dfl_semantic_event_strict_failure_audit_not_full_dfl",
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
        ml_stage="diagnostics",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def forecast_candidate_forensics_frame(
    context,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Classify forecast candidates before stronger AFL/DFL claims."""

    forensics_frame = build_forecast_candidate_forensics_frame(
        real_data_rolling_origin_benchmark_frame
    )
    _add_metadata(
        context,
        {
            "rows": forensics_frame.height,
            "candidate_kind_count": forensics_frame.select("candidate_kind").n_unique()
            if forensics_frame.height
            else 0,
            "compact_candidate_count": forensics_frame.filter(
                pl.col("candidate_kind") == "compact_silver_candidate"
            ).height
            if forensics_frame.height
            else 0,
            "scope": "forecast_candidate_forensics_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return forensics_frame


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
def afl_training_panel_frame(
    context,
    config: AflTrainingPanelAssetConfig,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    tenant_historical_net_load_silver: pl.DataFrame,
) -> pl.DataFrame:
    """AFL sidecar panel with prior-only features and decision-value labels."""

    panel_frame = build_afl_training_panel_frame(
        real_data_rolling_origin_benchmark_frame,
        final_holdout_anchor_count_per_tenant=config.final_holdout_anchor_count_per_tenant,
        weather_context_frame=real_data_benchmark_silver_feature_frame,
        tenant_historical_net_load_frame=tenant_historical_net_load_silver,
    )
    _add_metadata(
        context,
        {
            "rows": panel_frame.height,
            "tenant_count": panel_frame.select("tenant_id").n_unique()
            if panel_frame.height
            else 0,
            "model_count": panel_frame.select("forecast_model_name").n_unique()
            if panel_frame.height
            else 0,
            "final_holdout_rows": panel_frame.filter(pl.col("split") == "final_holdout").height
            if panel_frame.height
            else 0,
            "scope": "arbitrage_focused_learning_panel_not_full_dfl",
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
        ml_stage="diagnostics",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def afl_forecast_error_audit_frame(
    context,
    config: AflForecastErrorAuditAssetConfig,
    forecast_candidate_forensics_frame: pl.DataFrame,
    afl_training_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """AFL forecast-error audit before official training or DFL loss work."""

    audit_frame = build_afl_forecast_error_audit_frame(
        forecast_candidate_forensics_frame,
        afl_training_panel_frame,
        spread_shape_failure_threshold_ratio=config.spread_shape_failure_threshold_ratio,
        rank_extrema_failure_threshold=config.rank_extrema_failure_threshold,
        lp_value_failure_margin_uah=config.lp_value_failure_margin_uah,
    )
    _add_metadata(
        context,
        {
            "rows": audit_frame.height,
            "tenant_count": audit_frame.select("tenant_id").n_unique()
            if audit_frame.height
            else 0,
            "model_count": audit_frame.select("forecast_model_name").n_unique()
            if audit_frame.height
            else 0,
            "mean_lp_value_failure_rate": audit_frame.select(
                "lp_value_failure_rate"
            ).mean().item()
            if audit_frame.height
            else 0.0,
            "mean_spread_shape_failure_rate": audit_frame.select(
                "spread_shape_failure_rate"
            ).mean().item()
            if audit_frame.height
            else 0.0,
            "mean_rank_extrema_failure_rate": audit_frame.select(
                "rank_extrema_failure_rate"
            ).mean().item()
            if audit_frame.height
            else 0.0,
            "scope": "afl_forecast_error_audit_not_full_dfl",
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
        ml_stage="diagnostics",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_ua_coverage_repair_audit_frame(
    context,
    config: DflUaCoverageRepairAuditAssetConfig,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    dfl_data_coverage_audit_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Exact OREE/Open-Meteo timestamp gap audit before DFL coverage promotion."""

    repair_frame = build_dfl_ua_coverage_repair_audit_frame(
        real_data_benchmark_silver_feature_frame,
        dfl_data_coverage_audit_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        target_anchor_count_per_tenant=config.target_anchor_count_per_tenant,
    )
    gap_rows = repair_frame.filter(pl.col("gap_kind") != "none") if repair_frame.height else pl.DataFrame()
    _add_metadata(
        context,
        {
            "rows": repair_frame.height,
            "gap_rows": gap_rows.height,
            "tenant_count": repair_frame.select("tenant_id").n_unique()
            if repair_frame.height
            else 0,
            "target_anchor_count_per_tenant": config.target_anchor_count_per_tenant,
            "repair_statuses": sorted(repair_frame["repair_status"].unique().to_list())
            if repair_frame.height
            else [],
            "scope": "ua_coverage_repair_audit_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return repair_frame


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
def dfl_schedule_value_learner_v2_robustness_frame(
    context,
    config: DflScheduleValueLearnerV2RobustnessAssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling-window robustness evidence for the schedule/value learner v2."""

    tenant_ids = _csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv")
    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    robustness_frame = build_dfl_schedule_value_learner_v2_robustness_frame(
        dfl_schedule_candidate_library_v2_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=source_model_names,
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_prior_anchors_before_window=config.min_prior_anchors_before_window,
        min_robust_passing_windows=config.min_robust_passing_windows,
        min_validation_tenant_anchor_count_per_source_model=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
    )
    gate = evaluate_dfl_schedule_value_learner_v2_robustness_gate(
        robustness_frame,
        source_model_names=source_model_names,
    )
    _add_metadata(
        context,
        {
            "rows": robustness_frame.height,
            "source_model_count": len(source_model_names),
            "validation_window_count": config.validation_window_count,
            "validation_anchor_count": config.validation_anchor_count,
            "robust_source_model_names": gate.metrics.get(
                "robust_source_model_names",
                [],
            ),
            "promotion_gate_decision": gate.decision,
            "promotion_gate_description": gate.description,
            "production_gate_passed": gate.metrics.get("production_gate_passed", False),
            "scope": "dfl_schedule_value_learner_v2_robustness_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return robustness_frame


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
def dfl_schedule_value_production_gate_frame(
    context,
    config: DflScheduleValueProductionGateAssetConfig,
    dfl_schedule_value_learner_v2_strict_lp_benchmark_frame: pl.DataFrame,
    dfl_schedule_value_learner_v2_robustness_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Offline promotion/fallback decision for robust schedule/value evidence."""

    source_model_names = _forecast_model_names(config.source_model_names_csv)
    gate_frame = build_dfl_schedule_value_production_gate_frame(
        dfl_schedule_value_learner_v2_strict_lp_benchmark_frame,
        dfl_schedule_value_learner_v2_robustness_frame,
        source_model_names=source_model_names,
        min_tenant_count=config.min_tenant_count,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio=config.min_mean_regret_improvement_ratio,
        min_rolling_window_count=config.min_rolling_window_count,
        min_rolling_strict_pass_windows=config.min_rolling_strict_pass_windows,
    )
    generated_at = _latest_generated_at(dfl_schedule_value_learner_v2_strict_lp_benchmark_frame)
    if generated_at is None:
        generated_at = datetime.now(UTC)
    gate_frame = gate_frame.with_columns(pl.lit(generated_at).alias("generated_at"))
    gate = evaluate_dfl_schedule_value_production_gate(
        gate_frame,
        source_model_names=source_model_names,
    )
    get_dfl_training_store().upsert_schedule_value_production_gate_frame(gate_frame)
    _add_metadata(
        context,
        {
            "rows": gate_frame.height,
            "source_model_count": len(source_model_names),
            "promoted_source_model_names": gate.metrics.get(
                "promoted_source_model_names",
                [],
            ),
            "production_promote_count": gate.metrics.get("production_promote_count", 0),
            "promotion_gate_decision": gate.decision,
            "promotion_gate_description": gate.description,
            "market_execution_enabled": False,
            "scope": "dfl_schedule_value_production_gate_offline_strategy_not_market_execution",
            "not_market_execution": True,
        },
    )
    return gate_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="official_forecast_adapters",
        market_venue="DAM",
    ),
)
def dfl_official_schedule_candidate_library_frame(
    context,
    config: DflOfficialScheduleCandidateLibraryAssetConfig,
    official_forecast_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Schedule candidate library for serious official NBEATSx/TFT rolling forecasts."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    library_frame = build_dfl_schedule_candidate_library_from_strict_benchmark_frame(
        official_forecast_rolling_origin_benchmark_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        perturb_spread_scale_grid=_float_csv_values(
            config.perturb_spread_scale_grid_csv,
            field_name="perturb_spread_scale_grid_csv",
        ),
        perturb_mean_shift_grid_uah_mwh=_float_csv_values(
            config.perturb_mean_shift_grid_uah_mwh_csv,
            field_name="perturb_mean_shift_grid_uah_mwh_csv",
        ),
    )
    _add_metadata(
        context,
        {
            "rows": library_frame.height,
            "tenant_count": library_frame.select("tenant_id").n_unique()
            if library_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "candidate_family_count": library_frame.select("candidate_family").n_unique()
            if library_frame.height
            else 0,
            "final_holdout_rows": library_frame.filter(pl.col("split_name") == "final_holdout").height
            if library_frame.height
            else 0,
            "scope": "dfl_official_schedule_candidate_library_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return library_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="official_forecast_adapters",
        market_venue="DAM",
    ),
)
def dfl_official_schedule_candidate_library_v2_frame(
    context,
    config: DflStrictChallengerAssetConfig,
    dfl_official_schedule_candidate_library_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Blend/residual schedule candidates for official forecast schedules."""

    library_frame = build_schedule_candidate_library_v2_frame(
        dfl_official_schedule_candidate_library_frame,
        blend_weights=_float_csv_values(config.blend_weights_csv, field_name="blend_weights_csv"),
        residual_min_prior_anchors=config.residual_min_prior_anchors,
    )
    _add_metadata(
        context,
        {
            "rows": library_frame.height,
            "tenant_count": library_frame.select("tenant_id").n_unique()
            if library_frame.height
            else 0,
            "source_model_count": library_frame.select("source_model_name").n_unique()
            if library_frame.height
            else 0,
            "candidate_family_count": library_frame.select("candidate_family").n_unique()
            if library_frame.height
            else 0,
            "residual_min_prior_anchors": config.residual_min_prior_anchors,
            "scope": "dfl_official_schedule_candidate_library_v2_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return library_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_forecast_adapters",
        market_venue="DAM",
    ),
)
def dfl_official_schedule_value_learner_v2_frame(
    context,
    config: DflOfficialScheduleValueLearnerV2AssetConfig,
    dfl_official_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only schedule/value learner over official forecast schedule candidates."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    learner_frame = build_dfl_schedule_value_learner_v2_frame(
        dfl_official_schedule_candidate_library_v2_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
    )
    _add_metadata(
        context,
        {
            "rows": learner_frame.height,
            "tenant_count": learner_frame.select("tenant_id").n_unique()
            if learner_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "profile_names": sorted(learner_frame["selected_weight_profile_name"].unique().to_list())
            if learner_frame.height
            else [],
            "scope": "dfl_official_schedule_value_learner_v2_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return learner_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_forecast_adapters",
        market_venue="DAM",
    ),
)
def dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame(
    context,
    config: DflOfficialScheduleValueLearnerV2AssetConfig,
    dfl_official_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_official_schedule_value_learner_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle gate rows for official schedule/value learner evidence."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_schedule_value_learner_v2_strict_lp_benchmark_frame(
        dfl_official_schedule_candidate_library_v2_frame,
        dfl_official_schedule_value_learner_v2_frame,
        generated_at=_latest_generated_at(dfl_official_schedule_candidate_library_v2_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_schedule_value_learner_v2_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
    )
    learner_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("dfl_schedule_value_learner_v2_")
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "learner_validation_tenant_anchor_count": learner_rows.height,
            "strategy_kind": DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "development_gate_passed": gate.metrics.get("development_gate_passed", False),
            "production_gate_passed": gate.metrics.get("production_gate_passed", False),
            "scope": "dfl_official_schedule_value_learner_v2_strict_lp_gate_not_full_dfl",
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
        backend="official_forecast_adapters",
        market_venue="DAM",
    ),
)
def dfl_official_schedule_value_learner_v2_robustness_frame(
    context,
    config: DflOfficialScheduleValueLearnerV2RobustnessAssetConfig,
    dfl_official_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling-window robustness for official schedule/value learner evidence."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    robustness_frame = build_dfl_schedule_value_learner_v2_robustness_frame(
        dfl_official_schedule_candidate_library_v2_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_prior_anchors_before_window=config.min_prior_anchors_before_window,
        min_robust_passing_windows=config.min_robust_passing_windows,
        min_validation_tenant_anchor_count_per_source_model=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
    )
    gate = evaluate_dfl_schedule_value_learner_v2_robustness_gate(
        robustness_frame,
        source_model_names=source_model_names,
    )
    _add_metadata(
        context,
        {
            "rows": robustness_frame.height,
            "source_model_count": len(source_model_names),
            "validation_window_count": config.validation_window_count,
            "validation_anchor_count": config.validation_anchor_count,
            "robust_source_model_names": gate.metrics.get("robust_source_model_names", []),
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "production_promote": False,
            "scope": "dfl_official_schedule_value_learner_v2_robustness_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return robustness_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_forecast_adapters",
        market_venue="DAM",
    ),
)
def dfl_official_schedule_value_production_gate_frame(
    context,
    config: DflOfficialScheduleValueProductionGateAssetConfig,
    dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame: pl.DataFrame,
    dfl_official_schedule_value_learner_v2_robustness_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Offline promotion/fallback decision for official schedule/value evidence."""

    source_model_names = _forecast_model_names(config.source_model_names_csv)
    gate_frame = build_dfl_schedule_value_production_gate_frame(
        dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame,
        dfl_official_schedule_value_learner_v2_robustness_frame,
        source_model_names=source_model_names,
        min_tenant_count=config.min_tenant_count,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio=config.min_mean_regret_improvement_ratio,
        min_rolling_window_count=config.min_rolling_window_count,
        min_rolling_strict_pass_windows=config.min_rolling_strict_pass_windows,
    )
    generated_at = _latest_generated_at(dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame)
    if generated_at is None:
        generated_at = datetime.now(UTC)
    gate_frame = gate_frame.with_columns(pl.lit(generated_at).alias("generated_at"))
    gate = evaluate_dfl_schedule_value_production_gate(
        gate_frame,
        source_model_names=source_model_names,
    )
    _add_metadata(
        context,
        {
            "rows": gate_frame.height,
            "source_model_count": len(source_model_names),
            "promoted_source_model_names": gate.metrics.get("promoted_source_model_names", []),
            "production_promote_count": gate.metrics.get("production_promote_count", 0),
            "promotion_gate_decision": gate.decision,
            "promotion_gate_description": gate.description,
            "market_execution_enabled": False,
            "scope": "dfl_official_schedule_value_production_gate_not_market_execution",
            "not_market_execution": True,
        },
    )
    return gate_frame


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
def dfl_schedule_candidate_library_frame(
    context,
    config: DflTrajectoryFeatureRankerAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    dfl_trajectory_value_candidate_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Feasible strict-LP-scored schedule library for trajectory feature ranking."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    library_frame = build_dfl_schedule_candidate_library_frame(
        real_data_rolling_origin_benchmark_frame,
        dfl_trajectory_value_candidate_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        perturb_spread_scale_grid=_float_csv_values(
            config.perturb_spread_scale_grid_csv,
            field_name="perturb_spread_scale_grid_csv",
        ),
        perturb_mean_shift_grid_uah_mwh=_float_csv_values(
            config.perturb_mean_shift_grid_uah_mwh_csv,
            field_name="perturb_mean_shift_grid_uah_mwh_csv",
        ),
    )
    _add_metadata(
        context,
        {
            "rows": library_frame.height,
            "tenant_count": library_frame.select("tenant_id").n_unique() if library_frame.height else 0,
            "source_model_count": len(source_model_names),
            "candidate_family_count": library_frame.select("candidate_family").n_unique()
            if library_frame.height
            else 0,
            "final_holdout_rows": library_frame.filter(pl.col("split_name") == "final_holdout").height
            if library_frame.height
            else 0,
            "scope": "dfl_schedule_candidate_library_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return library_frame


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
def dfl_trajectory_feature_ranker_frame(
    context,
    config: DflTrajectoryFeatureRankerAssetConfig,
    dfl_schedule_candidate_library_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Select a prior-only feature scoring profile for feasible schedule candidates."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    ranker_frame = build_dfl_trajectory_feature_ranker_frame(
        dfl_schedule_candidate_library_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        min_final_holdout_tenant_anchor_count_per_source_model=(
            config.min_final_holdout_tenant_anchor_count_per_source_model
        ),
    )
    _add_metadata(
        context,
        {
            "rows": ranker_frame.height,
            "tenant_count": ranker_frame.select("tenant_id").n_unique() if ranker_frame.height else 0,
            "source_model_count": len(source_model_names),
            "weight_profile_count": ranker_frame.select("selected_weight_profile_name").n_unique()
            if ranker_frame.height
            else 0,
            "scope": "dfl_trajectory_feature_ranker_v1_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return ranker_frame


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
def dfl_trajectory_feature_ranker_strict_lp_benchmark_frame(
    context,
    config: DflTrajectoryFeatureRankerAssetConfig,
    dfl_schedule_candidate_library_frame: pl.DataFrame,
    dfl_trajectory_feature_ranker_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle rows for the trajectory feature-ranker gate."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_trajectory_feature_ranker_strict_lp_benchmark_frame(
        dfl_schedule_candidate_library_frame,
        dfl_trajectory_feature_ranker_frame,
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    promotion_gate = evaluate_dfl_trajectory_feature_ranker_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=config.min_final_holdout_tenant_anchor_count_per_source_model,
    )
    ranker_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("dfl_trajectory_feature_ranker_v1_")
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique() if strict_frame.height else 0,
            "source_model_count": len(source_model_names),
            "ranker_validation_tenant_anchor_count": ranker_rows.height,
            "strategy_kind": DFL_TRAJECTORY_FEATURE_RANKER_STRICT_LP_STRATEGY_KIND,
            "promotion_gate_decision": promotion_gate.decision,
            "promotion_gate_description": promotion_gate.description,
            "development_gate_passed": promotion_gate.metrics.get("development_gate_passed", False),
            "scope": "dfl_trajectory_feature_ranker_v1_strict_lp_gate_not_full_dfl",
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
def dfl_pipeline_integrity_audit_frame(
    context,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    dfl_schedule_candidate_library_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Point-in-time and feature-boundary audit for strict-challenger DFL evidence."""

    audit_frame = build_pipeline_integrity_audit_frame(
        real_data_rolling_origin_benchmark_frame,
        dfl_schedule_candidate_library_frame,
    )
    row = audit_frame.row(0, named=True) if audit_frame.height else {}
    _add_metadata(
        context,
        {
            "rows": audit_frame.height,
            "passed": bool(row.get("passed", False)),
            "market_anchor_count": row.get("market_anchor_count", 0),
            "tenant_anchor_count": row.get("tenant_anchor_count", 0),
            "forbidden_ranker_feature_overlap_count": row.get(
                "forbidden_ranker_feature_overlap_count",
                0,
            ),
            "leaky_horizon_rows": row.get("leaky_horizon_rows", 0),
            "scope": "dfl_pipeline_integrity_audit_not_full_dfl",
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
        ml_stage="diagnostics",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def forecast_pipeline_truth_audit_frame(
    context,
    config: DflForecastPipelineTruthAuditAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Forecast-vector truth audit before serious official/DFL reruns."""

    audit_frame = build_forecast_pipeline_truth_audit(
        real_data_rolling_origin_benchmark_frame,
        price_floor_uah_mwh=config.price_floor_uah_mwh,
        price_cap_uah_mwh=config.price_cap_uah_mwh,
        horizon_shift_offsets=_int_csv_values(
            config.horizon_shift_offsets_csv,
            field_name="horizon_shift_offsets_csv",
        ),
    )
    outcome = validate_forecast_pipeline_truth_audit_evidence(audit_frame)
    _add_metadata(
        context,
        {
            "rows": audit_frame.height,
            "passed": outcome.passed,
            "description": outcome.description,
            **outcome.metadata,
            "scope": "dfl_forecast_pipeline_truth_audit_not_full_dfl",
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
def dfl_schedule_candidate_library_v2_frame(
    context,
    config: DflStrictChallengerAssetConfig,
    dfl_schedule_candidate_library_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict-control challenger schedule library with blend and prior-residual candidates."""

    library_frame = build_schedule_candidate_library_v2_frame(
        dfl_schedule_candidate_library_frame,
        blend_weights=_float_csv_values(config.blend_weights_csv, field_name="blend_weights_csv"),
        residual_min_prior_anchors=config.residual_min_prior_anchors,
    )
    _add_metadata(
        context,
        {
            "rows": library_frame.height,
            "tenant_count": library_frame.select("tenant_id").n_unique() if library_frame.height else 0,
            "source_model_count": library_frame.select("source_model_name").n_unique()
            if library_frame.height
            else 0,
            "candidate_family_count": library_frame.select("candidate_family").n_unique()
            if library_frame.height
            else 0,
            "residual_min_prior_anchors": config.residual_min_prior_anchors,
            "scope": "dfl_schedule_candidate_library_v2_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return library_frame


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
def dfl_non_strict_oracle_upper_bound_frame(
    context,
    config: DflStrictChallengerAssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Best possible non-strict candidate diagnostic on the final holdout."""

    upper_bound_frame = build_non_strict_oracle_upper_bound_frame(
        dfl_schedule_candidate_library_v2_frame,
        min_final_holdout_tenant_anchor_count_per_source_model=(
            config.min_final_holdout_tenant_anchor_count_per_source_model
        ),
    )
    outcome = validate_dfl_non_strict_upper_bound_evidence(
        upper_bound_frame,
        minimum_validation_tenant_anchor_count_per_source_model=(
            config.min_final_holdout_tenant_anchor_count_per_source_model
        ),
    )
    _add_metadata(
        context,
        {
            "rows": upper_bound_frame.height,
            "passed": outcome.passed,
            "description": outcome.description,
            **outcome.metadata,
            "scope": "dfl_non_strict_oracle_upper_bound_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return upper_bound_frame


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
def dfl_strict_baseline_autopsy_frame(
    context,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_non_strict_oracle_upper_bound_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict-similar-day high-regret autopsy and non-strict opportunity map."""

    autopsy_frame = build_strict_baseline_autopsy_frame(
        dfl_schedule_candidate_library_v2_frame,
        dfl_non_strict_oracle_upper_bound_frame,
    )
    _add_metadata(
        context,
        {
            "rows": autopsy_frame.height,
            "tenant_count": autopsy_frame.select("tenant_id").n_unique() if autopsy_frame.height else 0,
            "source_model_count": autopsy_frame.select("source_model_name").n_unique()
            if autopsy_frame.height
            else 0,
            "strict_failure_opportunity_rows": autopsy_frame.filter(
                pl.col("recommended_next_action") == "train_selector_to_detect_strict_failure"
            ).height
            if autopsy_frame.height
            else 0,
            "scope": "dfl_strict_baseline_autopsy_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return autopsy_frame


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
def dfl_strict_failure_selector_frame(
    context,
    config: DflStrictFailureSelectorAssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_strict_baseline_autopsy_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only selector for anchors where strict_similar_day is likely to fail."""

    tenant_ids = _csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv")
    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    selector_frame = build_dfl_strict_failure_selector_frame(
        dfl_schedule_candidate_library_v2_frame,
        dfl_strict_baseline_autopsy_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=source_model_names,
        switch_threshold_grid_uah=_float_csv_values(
            config.switch_threshold_grid_uah_csv,
            field_name="switch_threshold_grid_uah_csv",
        ),
        min_prior_anchor_count=config.min_prior_anchor_count,
        min_final_holdout_tenant_anchor_count_per_source_model=(
            config.min_final_holdout_tenant_anchor_count_per_source_model
        ),
    )
    _add_metadata(
        context,
        {
            "rows": selector_frame.height,
            "tenant_count": selector_frame.select("tenant_id").n_unique()
            if selector_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "final_switch_count": selector_frame.select("final_switch_count").sum().item()
            if selector_frame.height
            else 0,
            "min_prior_anchor_count": config.min_prior_anchor_count,
            "scope": "dfl_strict_failure_selector_v1_not_full_dfl",
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
def dfl_strict_failure_selector_strict_lp_benchmark_frame(
    context,
    config: DflStrictFailureSelectorAssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_strict_failure_selector_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle rows for the prior-only strict-failure selector gate."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_strict_failure_selector_strict_lp_benchmark_frame(
        dfl_schedule_candidate_library_v2_frame,
        dfl_strict_failure_selector_frame,
        generated_at=_latest_generated_at(dfl_schedule_candidate_library_v2_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    promotion_gate = evaluate_dfl_strict_failure_selector_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_final_holdout_tenant_anchor_count_per_source_model
        ),
    )
    selector_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("dfl_strict_failure_selector_v1_")
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "selector_validation_tenant_anchor_count": selector_rows.height,
            "strategy_kind": DFL_STRICT_FAILURE_SELECTOR_STRICT_LP_STRATEGY_KIND,
            "promotion_gate_decision": promotion_gate.decision,
            "promotion_gate_description": promotion_gate.description,
            "development_gate_passed": promotion_gate.metrics.get(
                "development_gate_passed",
                False,
            ),
            "scope": "dfl_strict_failure_selector_v1_strict_lp_gate_not_full_dfl",
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
def dfl_strict_failure_selector_robustness_frame(
    context,
    config: DflStrictFailureSelectorRobustnessAssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling-window robustness evidence for the strict-failure selector."""

    tenant_ids = _csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv")
    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    robustness_frame = build_dfl_strict_failure_selector_robustness_frame(
        dfl_schedule_candidate_library_v2_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=source_model_names,
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_prior_anchors_before_window=config.min_prior_anchors_before_window,
        min_prior_anchor_count=config.min_prior_anchor_count,
        switch_threshold_grid_uah=_float_csv_values(
            config.switch_threshold_grid_uah_csv,
            field_name="switch_threshold_grid_uah_csv",
        ),
        min_robust_passing_windows=config.min_robust_passing_windows,
        min_validation_tenant_anchor_count_per_source_model=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
    )
    gate = evaluate_dfl_strict_failure_selector_robustness_gate(
        robustness_frame,
        source_model_names=source_model_names,
    )
    _add_metadata(
        context,
        {
            "rows": robustness_frame.height,
            "source_model_count": len(source_model_names),
            "validation_window_count": config.validation_window_count,
            "validation_anchor_count": config.validation_anchor_count,
            "robust_source_model_names": gate.metrics.get(
                "robust_source_model_names",
                [],
            ),
            "promotion_gate_decision": gate.decision,
            "promotion_gate_description": gate.description,
            "production_gate_passed": gate.metrics.get("production_gate_passed", False),
            "scope": "dfl_strict_failure_selector_robustness_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return robustness_frame


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
def dfl_strict_failure_prior_feature_panel_frame(
    context,
    config: DflStrictFailureFeatureAuditAssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_strict_failure_selector_robustness_frame: pl.DataFrame,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    tenant_historical_net_load_silver: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only feature panel explaining strict-failure selector behavior."""

    tenant_ids = _csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv")
    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    feature_panel = build_dfl_strict_failure_prior_feature_panel_frame(
        dfl_schedule_candidate_library_v2_frame,
        dfl_strict_failure_selector_robustness_frame,
        real_data_benchmark_silver_feature_frame,
        tenant_historical_net_load_silver,
        tenant_ids=tenant_ids,
        forecast_model_names=source_model_names,
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_prior_anchors_before_window=config.min_prior_anchors_before_window,
        min_prior_anchor_count=config.min_prior_anchor_count,
    )
    _add_metadata(
        context,
        {
            "rows": feature_panel.height,
            "tenant_count": feature_panel.select("tenant_id").n_unique()
            if feature_panel.height
            else 0,
            "source_model_count": len(source_model_names),
            "validation_window_count": config.validation_window_count,
            "validation_anchor_count": config.validation_anchor_count,
            "scope": "strict_failure_prior_feature_audit_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return feature_panel


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
def dfl_strict_failure_feature_audit_frame(
    context,
    dfl_strict_failure_prior_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Deterministic feature-cluster audit for strict-failure selector outcomes."""

    audit_frame = build_dfl_strict_failure_feature_audit_frame(
        dfl_strict_failure_prior_feature_panel_frame
    )
    _add_metadata(
        context,
        {
            "rows": audit_frame.height,
            "tenant_count": audit_frame.select("tenant_id").n_unique()
            if audit_frame.height
            else 0,
            "source_model_count": audit_frame.select("source_model_name").n_unique()
            if audit_frame.height
            else 0,
            "window_count": audit_frame.select("window_index").n_unique()
            if audit_frame.height
            else 0,
            "failure_clusters": sorted(audit_frame["failure_cluster"].unique().to_list())
            if audit_frame.height
            else [],
            "scope": "strict_failure_feature_audit_not_full_dfl",
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
        ml_stage="selection",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_feature_aware_strict_failure_selector_frame(
    context,
    config: DflFeatureAwareStrictFailureSelectorAssetConfig,
    dfl_strict_failure_prior_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Select feature-aware strict-failure rules from prior rolling windows only."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    selector_frame = build_dfl_feature_aware_strict_failure_selector_frame(
        dfl_strict_failure_prior_feature_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_window_index=config.final_window_index,
        min_training_window_count=config.min_training_window_count,
        switch_threshold_grid_uah=_float_csv_values(
            config.switch_threshold_grid_uah_csv,
            field_name="switch_threshold_grid_uah_csv",
        ),
        rank_overlap_floor_grid=_float_csv_values(
            config.rank_overlap_floor_grid_csv,
            field_name="rank_overlap_floor_grid_csv",
        ),
        price_regime_policies=_csv_values(
            config.price_regime_policies_csv,
            field_name="price_regime_policies_csv",
        ),
        volatility_policies=_csv_values(
            config.volatility_policies_csv,
            field_name="volatility_policies_csv",
        ),
    )
    _add_metadata(
        context,
        {
            "rows": selector_frame.height,
            "tenant_count": selector_frame.select("tenant_id").n_unique()
            if selector_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "final_switch_count": selector_frame.select("final_switch_count").sum().item()
            if selector_frame.height
            else 0,
            "scope": "dfl_feature_aware_strict_failure_selector_v2_not_full_dfl",
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
def dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame(
    context,
    config: DflFeatureAwareStrictFailureSelectorAssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_feature_aware_strict_failure_selector_frame: pl.DataFrame,
    dfl_strict_failure_prior_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle rows for the feature-aware strict-failure selector."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame(
        dfl_schedule_candidate_library_v2_frame,
        dfl_feature_aware_strict_failure_selector_frame,
        dfl_strict_failure_prior_feature_panel_frame,
        generated_at=_latest_generated_at(dfl_schedule_candidate_library_v2_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    promotion_gate = evaluate_dfl_feature_aware_strict_failure_selector_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
    )
    selector_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("dfl_feature_aware_strict_failure_selector_v2_")
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "selector_validation_tenant_anchor_count": selector_rows.height,
            "strategy_kind": DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_STRICT_LP_STRATEGY_KIND,
            "promotion_gate_decision": promotion_gate.decision,
            "promotion_gate_description": promotion_gate.description,
            "development_gate_passed": promotion_gate.metrics.get(
                "development_gate_passed",
                False,
            ),
            "scope": "dfl_feature_aware_strict_failure_selector_v2_strict_lp_gate_not_full_dfl",
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
        ml_stage="selection",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_regime_gated_tft_selector_v2_frame(
    context,
    config: DflRegimeGatedTftSelectorV2AssetConfig,
    dfl_strict_failure_prior_feature_panel_frame: pl.DataFrame,
    dfl_strict_failure_feature_audit_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Select source/regime rules for a prior-only TFT challenger switch."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    selector_frame = build_dfl_regime_gated_tft_selector_v2_frame(
        dfl_strict_failure_prior_feature_panel_frame,
        dfl_strict_failure_feature_audit_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        source_model_names=source_model_names,
        tft_source_model_name=config.tft_source_model_name,
        min_training_window_count=config.min_training_window_count,
        min_mean_regret_improvement_ratio=config.min_mean_regret_improvement_ratio,
    )
    _add_metadata(
        context,
        {
            "rows": selector_frame.height,
            "source_model_count": len(source_model_names),
            "allow_challenger_rows": selector_frame.filter(pl.col("allow_challenger")).height
            if selector_frame.height
            else 0,
            "strict_default_rows": selector_frame.filter(~pl.col("allow_challenger")).height
            if selector_frame.height
            else 0,
            "scope": "dfl_regime_gated_tft_selector_v2_not_full_dfl",
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
def dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame(
    context,
    config: DflRegimeGatedTftSelectorV2AssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_regime_gated_tft_selector_v2_frame: pl.DataFrame,
    dfl_strict_failure_prior_feature_panel_frame: pl.DataFrame,
    dfl_strict_failure_feature_audit_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle rows for the regime-gated TFT selector v2."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame(
        dfl_schedule_candidate_library_v2_frame,
        dfl_regime_gated_tft_selector_v2_frame,
        dfl_strict_failure_prior_feature_panel_frame,
        dfl_strict_failure_feature_audit_frame,
        generated_at=_latest_generated_at(dfl_schedule_candidate_library_v2_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_regime_gated_tft_selector_v2_gate(
        strict_frame,
        source_model_names=source_model_names,
    )
    selector_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("dfl_regime_gated_tft_selector_v2_")
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "selector_validation_tenant_anchor_count": selector_rows.height,
            "strategy_kind": DFL_REGIME_GATED_TFT_SELECTOR_V2_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "production_gate_passed": gate.metrics.get("production_gate_passed", False),
            "scope": "dfl_regime_gated_tft_selector_v2_strict_lp_gate_not_full_dfl",
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
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_forecast_dfl_v1_panel_frame(
    context,
    config: DflForecastDflV1AssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Tiny prior-only decision-loss correction panel for DFL v1 research."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    panel_frame = build_dfl_forecast_dfl_v1_panel_frame(
        real_data_rolling_origin_benchmark_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        max_train_anchors_per_tenant=config.max_train_anchors_per_tenant,
        inner_validation_fraction=config.inner_validation_fraction,
        epoch_count=config.epoch_count,
        learning_rate=config.learning_rate,
    )
    _add_metadata(
        context,
        {
            "rows": panel_frame.height,
            "tenant_count": panel_frame.select("tenant_id").n_unique()
            if panel_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "final_validation_anchor_count": panel_frame.select(pl.sum("final_validation_anchor_count")).item()
            if panel_frame.height
            else 0,
            "scope": "dfl_forecast_decision_loss_v1_not_full_dfl",
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
def dfl_forecast_dfl_v1_strict_lp_benchmark_frame(
    context,
    config: DflForecastDflV1AssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    dfl_forecast_dfl_v1_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle score for the tiny DFL v1 correction candidate."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_forecast_dfl_v1_strict_lp_benchmark_frame(
        real_data_rolling_origin_benchmark_frame,
        dfl_forecast_dfl_v1_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        generated_at=_latest_generated_at(real_data_rolling_origin_benchmark_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    dfl_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("dfl_forecast_dfl_v1_")
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "dfl_validation_tenant_anchor_count": dfl_rows.height,
            "strategy_kind": DFL_FORECAST_DFL_V1_STRICT_LP_STRATEGY_KIND,
            "scope": "dfl_forecast_decision_loss_v1_strict_lp_gate_not_full_dfl",
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
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_real_data_trajectory_dataset_frame(
    context,
    config: DflRealDataTrajectoryDatasetAssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_strict_failure_prior_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Step-level real-data trajectories for residual DFL and offline DT research."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    trajectory_frame = build_dfl_real_data_trajectory_dataset_frame(
        dfl_schedule_candidate_library_v2_frame,
        dfl_strict_failure_prior_feature_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
    )
    _add_metadata(
        context,
        {
            "rows": trajectory_frame.height,
            "episode_count": trajectory_frame.select("episode_id").n_unique()
            if trajectory_frame.height
            else 0,
            "tenant_count": trajectory_frame.select("tenant_id").n_unique()
            if trajectory_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "final_validation_anchor_count_per_tenant": config.final_validation_anchor_count_per_tenant,
            "scope": "dfl_real_data_trajectory_dataset_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return trajectory_frame


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
def dfl_residual_schedule_value_model_frame(
    context,
    config: DflResidualScheduleValueAssetConfig,
    dfl_real_data_trajectory_dataset_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only residual schedule/value selector model card."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    model_frame = build_dfl_residual_schedule_value_model_frame(
        dfl_real_data_trajectory_dataset_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        switch_margin_grid_uah=_float_csv_values(
            config.switch_margin_grid_uah_csv,
            field_name="switch_margin_grid_uah_csv",
        ),
    )
    _add_metadata(
        context,
        {
            "rows": model_frame.height,
            "tenant_count": model_frame.select("tenant_id").n_unique()
            if model_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "best_train_improvement_ratio": model_frame.select(
                pl.max("train_mean_regret_improvement_ratio_vs_strict")
            ).item()
            if model_frame.height
            else 0.0,
            "scope": "dfl_residual_schedule_value_v1_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return model_frame


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
def dfl_residual_schedule_value_strict_lp_benchmark_frame(
    context,
    config: DflResidualScheduleValueAssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_residual_schedule_value_model_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle evidence rows for residual schedule/value candidates."""

    strict_frame = build_dfl_residual_schedule_value_strict_lp_benchmark_frame(
        dfl_schedule_candidate_library_v2_frame,
        dfl_residual_schedule_value_model_frame,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        generated_at=_latest_generated_at(dfl_schedule_candidate_library_v2_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    residual_rows = strict_frame.filter(pl.col("selection_role") == "residual_selector")
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "residual_validation_tenant_anchor_count": residual_rows.height,
            "strategy_kind": DFL_RESIDUAL_SCHEDULE_VALUE_STRICT_LP_STRATEGY_KIND,
            "scope": "dfl_residual_schedule_value_v1_strict_lp_gate_not_full_dfl",
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
        ml_stage="selection",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_offline_dt_candidate_frame(
    context,
    config: DflOfflineDtCandidateAssetConfig,
    dfl_real_data_trajectory_dataset_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Tiny offline DT candidate selected from high-value train trajectories only."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    candidate_frame = build_dfl_offline_dt_candidate_frame(
        dfl_real_data_trajectory_dataset_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        high_value_quantile=config.high_value_quantile,
        context_length=config.context_length,
        hidden_dim=config.hidden_dim,
        num_layers=config.num_layers,
        num_heads=config.num_heads,
        max_epochs=config.max_epochs,
        random_seed=config.random_seed,
    )
    _add_metadata(
        context,
        {
            "rows": candidate_frame.height,
            "tenant_count": candidate_frame.select("tenant_id").n_unique()
            if candidate_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "dt_context_length": config.context_length,
            "dt_hidden_dim": config.hidden_dim,
            "max_epochs": config.max_epochs,
            "scope": "dfl_offline_dt_candidate_v1_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return candidate_frame


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
def dfl_offline_dt_candidate_strict_lp_benchmark_frame(
    context,
    config: DflOfflineDtCandidateAssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_offline_dt_candidate_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle evidence for offline DT and filtered behavior cloning."""

    strict_frame = build_dfl_offline_dt_candidate_strict_lp_benchmark_frame(
        dfl_schedule_candidate_library_v2_frame,
        dfl_offline_dt_candidate_frame,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        generated_at=_latest_generated_at(dfl_schedule_candidate_library_v2_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    dt_rows = strict_frame.filter(pl.col("selection_role") == "offline_dt")
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "offline_dt_validation_tenant_anchor_count": dt_rows.height,
            "strategy_kind": DFL_OFFLINE_DT_STRICT_LP_STRATEGY_KIND,
            "scope": "dfl_offline_dt_candidate_v1_strict_lp_gate_not_full_dfl",
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
def dfl_residual_dt_fallback_strict_lp_benchmark_frame(
    context,
    config: DflResidualDtFallbackAssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_residual_schedule_value_model_frame: pl.DataFrame,
    dfl_offline_dt_candidate_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle evidence for the strict-default residual/DT fallback wrapper."""

    strict_frame = build_dfl_residual_dt_fallback_strict_lp_benchmark_frame(
        dfl_schedule_candidate_library_v2_frame,
        dfl_residual_schedule_value_model_frame,
        dfl_offline_dt_candidate_frame,
        final_validation_anchor_count_per_tenant=config.final_validation_anchor_count_per_tenant,
        min_confidence_improvement_ratio=config.min_confidence_improvement_ratio,
        generated_at=_latest_generated_at(dfl_schedule_candidate_library_v2_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    source_model_names = tuple(sorted(strict_frame["source_model_name"].unique().to_list()))
    promotion_gate = evaluate_dfl_residual_dt_fallback_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
    )
    fallback_rows = strict_frame.filter(pl.col("selection_role") == "fallback_strategy")
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "fallback_validation_tenant_anchor_count": fallback_rows.height,
            "strategy_kind": DFL_RESIDUAL_DT_FALLBACK_STRICT_LP_STRATEGY_KIND,
            "promotion_gate_decision": promotion_gate.decision,
            "promotion_gate_description": promotion_gate.description,
            "production_promote": promotion_gate.metrics.get("production_promote", False),
            "scope": "dfl_residual_dt_fallback_v1_strict_lp_gate_not_full_dfl",
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
def dfl_source_specific_research_challenger_frame(
    context,
    config: DflSourceSpecificResearchChallengerAssetConfig,
    dfl_residual_dt_fallback_strict_lp_benchmark_frame: pl.DataFrame,
    dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame: pl.DataFrame,
    dfl_strict_failure_selector_robustness_frame: pl.DataFrame,
    dfl_strict_failure_feature_audit_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Combine source-specific strict evidence into a research-challenger gate."""

    source_model_names = _forecast_model_names(config.source_model_names_csv)
    challenger_frame = build_dfl_source_specific_research_challenger_frame(
        dfl_residual_dt_fallback_strict_lp_benchmark_frame,
        dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame,
        dfl_strict_failure_selector_robustness_frame,
        dfl_strict_failure_feature_audit_frame,
        source_model_names=source_model_names,
        min_tenant_count=config.min_tenant_count,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio=config.min_mean_regret_improvement_ratio,
        min_rolling_strict_pass_windows=config.min_rolling_strict_pass_windows,
        min_rolling_window_count=config.min_rolling_window_count,
    )
    gate = evaluate_dfl_source_specific_research_challenger_gate(
        challenger_frame,
        source_model_names=source_model_names,
    )
    _add_metadata(
        context,
        {
            "rows": challenger_frame.height,
            "source_model_count": len(source_model_names),
            "latest_signal_source_model_names": gate.metrics.get(
                "latest_signal_source_model_names",
                [],
            ),
            "robust_source_model_names": gate.metrics.get("robust_source_model_names", []),
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "production_promote": False,
            "scope": "dfl_source_specific_research_challenger_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return challenger_frame


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
def dfl_schedule_value_learner_v2_frame(
    context,
    config: DflScheduleValueLearnerV2AssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only DFL v2 schedule/value learner over feasible candidate schedules."""

    tenant_ids = _csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv")
    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    learner_frame = build_dfl_schedule_value_learner_v2_frame(
        dfl_schedule_candidate_library_v2_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
    )
    _add_metadata(
        context,
        {
            "rows": learner_frame.height,
            "tenant_count": learner_frame.select("tenant_id").n_unique()
            if learner_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "profile_names": sorted(learner_frame["selected_weight_profile_name"].unique().to_list())
            if learner_frame.height
            else [],
            "scope": "dfl_schedule_value_learner_v2_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return learner_frame


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
def dfl_schedule_value_learner_v2_strict_lp_benchmark_frame(
    context,
    config: DflScheduleValueLearnerV2AssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_schedule_value_learner_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle evidence for the schedule/value learner v2."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_schedule_value_learner_v2_strict_lp_benchmark_frame(
        dfl_schedule_candidate_library_v2_frame,
        dfl_schedule_value_learner_v2_frame,
        generated_at=_latest_generated_at(dfl_schedule_candidate_library_v2_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_schedule_value_learner_v2_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
    )
    learner_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("dfl_schedule_value_learner_v2_")
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "learner_validation_tenant_anchor_count": learner_rows.height,
            "strategy_kind": DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "development_gate_passed": gate.metrics.get("development_gate_passed", False),
            "production_gate_passed": gate.metrics.get("production_gate_passed", False),
            "scope": "dfl_schedule_value_learner_v2_strict_lp_gate_not_full_dfl",
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
        ml_stage="selection",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_production_promotion_gate_frame(
    context,
    config: DflProductionPromotionGateAssetConfig,
    dfl_source_specific_research_challenger_frame: pl.DataFrame,
    dfl_strict_failure_selector_robustness_frame: pl.DataFrame,
    dfl_strict_failure_feature_audit_frame: pl.DataFrame,
    dfl_data_coverage_audit_frame: pl.DataFrame,
    dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Offline production-promotion gate for source/regime-specific DFL evidence."""

    source_model_names = _forecast_model_names(config.source_model_names_csv)
    gate_frame = build_dfl_production_promotion_gate_frame(
        dfl_source_specific_research_challenger_frame,
        dfl_strict_failure_selector_robustness_frame,
        dfl_strict_failure_feature_audit_frame,
        dfl_data_coverage_audit_frame,
        dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame,
        source_model_names=source_model_names,
        min_tenant_count=config.min_tenant_count,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio=config.min_mean_regret_improvement_ratio,
        min_rolling_strict_pass_windows=config.min_rolling_strict_pass_windows,
        min_rolling_window_count=config.min_rolling_window_count,
        backfill_target_anchor_count_per_tenant=(
            config.backfill_target_anchor_count_per_tenant
        ),
    )
    gate = evaluate_dfl_production_promotion_gate(
        gate_frame,
        source_model_names=source_model_names,
    )
    _add_metadata(
        context,
        {
            "rows": gate_frame.height,
            "source_model_count": len(source_model_names),
            "promoted_source_model_names": gate.metrics.get(
                "promoted_source_model_names",
                [],
            ),
            "production_promote_count": gate.metrics.get("production_promote_count", 0),
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "market_execution_enabled": False,
            "scope": "dfl_production_promotion_gate_offline_strategy_not_market_execution",
            "not_market_execution": True,
        },
    )
    return gate_frame


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
    forecast_afe_feature_catalog_frame,
    market_coupling_temporal_availability_frame,
    entsoe_neighbor_market_query_spec_frame,
    dfl_semantic_event_strict_failure_audit_frame,
    forecast_candidate_forensics_frame,
    afl_training_panel_frame,
    afl_forecast_error_audit_frame,
    dfl_data_coverage_audit_frame,
    dfl_ua_coverage_repair_audit_frame,
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
    dfl_schedule_candidate_library_frame,
    dfl_trajectory_feature_ranker_frame,
    dfl_trajectory_feature_ranker_strict_lp_benchmark_frame,
    dfl_pipeline_integrity_audit_frame,
    forecast_pipeline_truth_audit_frame,
    dfl_schedule_candidate_library_v2_frame,
    dfl_non_strict_oracle_upper_bound_frame,
    dfl_strict_baseline_autopsy_frame,
    dfl_strict_failure_selector_frame,
    dfl_strict_failure_selector_strict_lp_benchmark_frame,
    dfl_strict_failure_selector_robustness_frame,
    dfl_strict_failure_prior_feature_panel_frame,
    dfl_strict_failure_feature_audit_frame,
    dfl_feature_aware_strict_failure_selector_frame,
    dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame,
    dfl_regime_gated_tft_selector_v2_frame,
    dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame,
    dfl_forecast_dfl_v1_panel_frame,
    dfl_forecast_dfl_v1_strict_lp_benchmark_frame,
    dfl_real_data_trajectory_dataset_frame,
    dfl_residual_schedule_value_model_frame,
    dfl_residual_schedule_value_strict_lp_benchmark_frame,
    dfl_offline_dt_candidate_frame,
    dfl_offline_dt_candidate_strict_lp_benchmark_frame,
    dfl_residual_dt_fallback_strict_lp_benchmark_frame,
    dfl_source_specific_research_challenger_frame,
    dfl_schedule_value_learner_v2_frame,
    dfl_schedule_value_learner_v2_strict_lp_benchmark_frame,
    dfl_schedule_value_learner_v2_robustness_frame,
    dfl_schedule_value_production_gate_frame,
    dfl_official_schedule_candidate_library_frame,
    dfl_official_schedule_candidate_library_v2_frame,
    dfl_official_schedule_value_learner_v2_frame,
    dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame,
    dfl_official_schedule_value_learner_v2_robustness_frame,
    dfl_official_schedule_value_production_gate_frame,
    dfl_production_promotion_gate_frame,
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
