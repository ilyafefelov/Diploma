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
from smart_arbitrage.resources.strategy_evaluation_store import (
    get_strategy_evaluation_store,
)
from smart_arbitrage.strategy.ensemble_gate import (
    CALIBRATED_VALUE_AWARE_ENSEMBLE_STRATEGY_KIND,
    RISK_ADJUSTED_VALUE_GATE_STRATEGY_KIND,
    build_calibrated_value_aware_ensemble_frame,
    build_risk_adjusted_value_gate_frame,
    build_value_aware_ensemble_frame,
)
from smart_arbitrage.strategy.dispatch_sensitivity import (
    build_forecast_dispatch_sensitivity_frame,
)
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
from smart_arbitrage.dfl.v2_plus_dfl_dt_bridge import (
    DFL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND,
    V2_PLUS_HEADLINE_BASELINE_METRICS,
    build_dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame,
    evaluate_dfl_v2_plus_dfl_dt_bridge_gate,
)
from smart_arbitrage.dfl.official_v2_plus_dfl_dt_bridge import (
    DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND,
    OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    build_dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame as build_official_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame,
    build_dfl_official_global_panel_v2_plus_offline_dt_candidate_frame as build_official_v2_plus_offline_dt_candidate_frame,
    build_dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame as build_official_v2_plus_residual_schedule_value_model_frame,
    build_dfl_official_global_panel_v2_plus_trajectory_dataset_frame as build_official_v2_plus_trajectory_dataset_frame,
)
from smart_arbitrage.dfl.official_v2_plus_bridge_failure_audit import (
    DFL_OFFICIAL_V2_PLUS_BRIDGE_FAILURE_AUDIT_CLAIM_SCOPE,
    build_dfl_official_v2_plus_bridge_failure_audit_frame,
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
from smart_arbitrage.dfl.schedule_value_learner_v3 import (
    DFL_SCHEDULE_VALUE_LEARNER_V3_STRICT_LP_STRATEGY_KIND,
    build_dfl_schedule_value_learner_v3_frame,
    build_dfl_schedule_value_learner_v3_strict_lp_benchmark_frame,
    evaluate_dfl_schedule_value_learner_v3_gate,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND,
    build_dfl_schedule_candidate_library_v2_plus_frame,
    build_dfl_schedule_value_learner_v2_plus_frame,
    build_dfl_schedule_value_learner_v2_plus_oracle_gap_audit_frame,
    build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
    build_dfl_schedule_value_regret_decomposition_frame,
    evaluate_dfl_schedule_value_learner_v2_plus_gate,
)
from smart_arbitrage.dfl.schedule_value_dfl_v2 import (
    DFL_SCHEDULE_VALUE_DFL_V2_STRICT_LP_STRATEGY_KIND,
    build_dfl_schedule_value_dfl_v2_frame,
    build_dfl_schedule_value_dfl_v2_strict_lp_benchmark_frame,
    evaluate_dfl_schedule_value_dfl_v2_gate,
)
from smart_arbitrage.dfl.candidate_value_dfl_v3 import (
    CANDIDATE_VALUE_DFL_V3_STRICT_LP_STRATEGY_KIND,
    build_dfl_candidate_value_dfl_v3_failure_audit_frame,
    build_dfl_candidate_value_dfl_v3_frame,
    build_dfl_candidate_value_dfl_v3_strict_lp_benchmark_frame,
    build_dfl_candidate_value_label_panel_v3_frame,
    build_dfl_schedule_candidate_library_v3_frame,
    evaluate_dfl_candidate_value_dfl_v3_gate,
)
from smart_arbitrage.dfl.candidate_value_dfl_v4 import (
    CANDIDATE_VALUE_DFL_V4_STRICT_LP_STRATEGY_KIND,
    build_dfl_candidate_value_dfl_v4_frame,
    build_dfl_candidate_value_dfl_v4_strict_lp_benchmark_frame,
    build_dfl_candidate_value_label_panel_v4_frame,
    build_dfl_plateau_data_quality_audit_frame,
    build_dfl_schedule_candidate_library_v4_frame,
    build_dfl_v2_v3_plateau_autopsy_frame,
    evaluate_dfl_candidate_value_dfl_v4_gate,
)
from smart_arbitrage.dfl.point_in_time_context_v5 import (
    CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_STRICT_LP_STRATEGY_KIND,
    build_dfl_context_enriched_candidate_value_dfl_v5_frame,
    build_dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame,
    build_dfl_context_enriched_candidate_value_label_panel_v5_frame,
    build_dfl_context_enriched_schedule_candidate_library_v5_frame,
    build_dfl_point_in_time_context_feature_panel_frame,
    build_dfl_point_in_time_context_repair_audit_frame,
    evaluate_dfl_context_enriched_candidate_value_dfl_v5_gate,
)
from smart_arbitrage.dfl.tft_quantile_schedule_value import (
    DFL_TFT_AUGMENTED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
    DFL_TFT_COMBINED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
    FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    TFT_QUANTILE_CALIBRATED_SOURCE_MODELS,
    TFT_QUANTILE_SOURCE_MODELS,
    build_dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame,
    build_dfl_tft_combined_v2_plus_strict_lp_benchmark_frame,
    build_dfl_tft_quantile_schedule_candidate_library_frame,
    evaluate_dfl_tft_augmented_v2_plus_gate,
    evaluate_dfl_tft_combined_v2_plus_gate,
)
from smart_arbitrage.dfl.nbeatsx_tft_combined_portfolio import (
    DEFAULT_COMBINED_SOURCE_MODEL_NAME,
    DFL_NBEATSX_TFT_META_SELECTOR_ROLLING_STRICT_LP_STRATEGY_KIND,
    DFL_NBEATSX_TFT_META_SELECTOR_STRICT_LP_STRATEGY_KIND,
    build_dfl_nbeatsx_tft_candidate_portfolio_v1_frame,
    build_dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame,
    build_dfl_nbeatsx_tft_complementarity_audit_frame,
    build_dfl_nbeatsx_tft_meta_selector_robustness_frame,
    build_dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame,
    build_dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame,
    evaluate_dfl_nbeatsx_tft_meta_selector_gate,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus_robustness import (
    build_dfl_schedule_value_learner_v2_plus_robustness_frame,
    evaluate_dfl_schedule_value_learner_v2_plus_robustness_gate,
)
from smart_arbitrage.dfl.schedule_value_learner_robustness import (
    build_dfl_schedule_value_learner_v2_robustness_frame,
    evaluate_dfl_schedule_value_learner_v2_robustness_gate,
)
from smart_arbitrage.dfl.schedule_value_promotion_gate import (
    build_dfl_schedule_value_production_gate_frame,
    evaluate_dfl_schedule_value_production_gate,
)
from smart_arbitrage.dfl.market_coupling_ablation import (
    build_dfl_market_coupling_v2_plus_ablation_frame,
)
from smart_arbitrage.dfl.market_coupled_v2_plus import (
    DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND,
    build_dfl_market_coupled_schedule_value_learner_v2_plus_frame,
    build_dfl_market_coupled_schedule_value_learner_v2_plus_robustness_frame,
    build_dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.poland_lag24_prior_veto import (
    build_poland_lag24_prior_veto_frame,
    build_poland_lag24_prior_veto_packet,
)
from smart_arbitrage.dfl.poland_lag24_feature_audit import (
    build_poland_lag24_feature_consumption_audit_frame,
    build_poland_lag24_rolling_vs_frozen_v2_plus_gate_frame,
)
from smart_arbitrage.dfl.poland_lag24_tail_risk_audit import (
    build_poland_lag24_tail_risk_audit_frame,
)
from smart_arbitrage.dfl.poland_lag24_candidate_value_ranker import (
    build_poland_lag24_candidate_value_label_panel_frame,
    build_poland_lag24_candidate_value_ranker_frame,
    build_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.lava_schedule_neighbor_bridge import (
    DFL_LAVA_CANDIDATE_VALUE_STRICT_LP_STRATEGY_KIND,
    build_dfl_lava_candidate_value_scorer_frame,
    build_dfl_lava_candidate_value_strict_lp_benchmark_frame,
    build_dfl_lava_schedule_neighbor_candidate_frame,
    build_dfl_v2_plus_schedule_neighbor_teacher_label_frame,
    evaluate_dfl_lava_candidate_value_gate,
)
from smart_arbitrage.dfl.lava_tail_risk_target import (
    DFL_LAVA_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
    DFL_LAVA_SAFE_SWITCH_V2_STRICT_LP_STRATEGY_KIND,
    DFL_LAVA_SCHEDULE_NEIGHBOR_DT_STRICT_LP_STRATEGY_KIND,
    DFL_LAVA_TAIL_RISK_AVOIDANCE_V3_STRICT_LP_STRATEGY_KIND,
    DFL_LAVA_TAIL_RISK_AWARE_STRICT_LP_STRATEGY_KIND,
    build_dfl_lava_schedule_neighbor_dt_policy_frame,
    build_dfl_lava_schedule_neighbor_dt_strict_lp_benchmark_frame,
    build_dfl_lava_schedule_neighbor_dt_training_frame,
    build_dfl_lava_tail_risk_avoidance_label_frame,
    build_dfl_lava_tail_risk_avoidance_scorer_v3_frame,
    build_dfl_lava_tail_risk_avoidance_strict_lp_benchmark_v3_frame,
    build_dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame,
    build_dfl_lava_tail_risk_safe_switch_scorer_v2_frame,
    build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_v2_frame,
    build_dfl_lava_tail_risk_safe_switch_scorer_frame,
    build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame,
    build_dfl_lava_tail_risk_aware_strict_lp_benchmark_frame,
    build_dfl_lava_tail_risk_aware_target_frame,
    build_dfl_lava_tail_risk_diagnostic_frame,
    evaluate_dfl_lava_tail_risk_avoidance_v3_gate,
    evaluate_dfl_lava_schedule_neighbor_dt_gate,
    evaluate_dfl_lava_tail_risk_safe_switch_v2_gate,
    evaluate_dfl_lava_tail_risk_safe_switch_gate,
    evaluate_dfl_lava_tail_risk_aware_gate,
)
from smart_arbitrage.dfl.oracle_gap_safe_switch import (
    ORACLE_GAP_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
    build_dfl_oracle_gap_safe_switch_feature_panel_frame,
    build_dfl_oracle_gap_safe_switch_label_frame,
    build_dfl_oracle_gap_safe_switch_rolling_robustness_frame,
    build_dfl_oracle_gap_safe_switch_scorer_frame,
    build_dfl_oracle_gap_safe_switch_strict_lp_benchmark_frame,
    evaluate_dfl_oracle_gap_safe_switch_gate,
)
from smart_arbitrage.dfl.ua_context_safe_switch import (
    UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_SKLEARN,
    UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_TORCH,
    UA_CONTEXT_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
    build_dfl_ua_calendar_publication_context_frame,
    build_dfl_ua_context_oracle_gap_feature_panel_frame,
    build_dfl_ua_context_safe_switch_rolling_robustness_frame,
    build_dfl_ua_context_safe_switch_scorer_frame,
    build_dfl_ua_context_safe_switch_separability_audit_frame,
    build_dfl_ua_context_safe_switch_strict_lp_benchmark_frame,
    build_dfl_ua_grid_event_context_frame,
    build_dfl_ua_weather_load_context_frame,
    evaluate_dfl_ua_context_safe_switch_gate,
)
from smart_arbitrage.dfl.ua_context_lava_dt import (
    UA_CONTEXT_LAVA_STRICT_LP_STRATEGY_KIND,
    build_dfl_ua_context_lava_candidate_policy_frame,
    build_dfl_ua_context_lava_rolling_robustness_frame,
    build_dfl_ua_context_lava_sequence_training_frame,
    build_dfl_ua_context_lava_strict_lp_benchmark_frame,
    build_dfl_ua_context_lava_teacher_frame,
    evaluate_dfl_ua_context_lava_gate,
)
from smart_arbitrage.dfl.regret_surrogate_v1 import (
    REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE,
    REGRET_SURROGATE_CONTEXTUAL_STRICT_LP_STRATEGY_KIND,
    REGRET_SURROGATE_STRICT_LP_STRATEGY_KIND,
    build_dfl_expanded_schedule_value_teacher_label_panel_v1_frame,
    build_dfl_regret_surrogate_contextual_candidate_value_v2_frame,
    build_dfl_regret_surrogate_contextual_rolling_robustness_frame,
    build_dfl_regret_surrogate_contextual_strict_lp_benchmark_frame,
    build_dfl_regret_surrogate_candidate_value_v1_frame,
    build_dfl_regret_surrogate_forecast_correction_v1_frame,
    build_dfl_regret_surrogate_rolling_robustness_frame,
    build_dfl_regret_surrogate_safe_switch_context_audit_frame,
    build_dfl_regret_surrogate_strict_lp_benchmark_frame,
    build_dfl_regret_surrogate_teacher_label_panel_v2_frame,
    build_dfl_v2_plus_learning_limit_audit_frame,
    evaluate_dfl_regret_surrogate_gate,
)
from smart_arbitrage.strategy.official_global_panel import (
    POLAND_LAG24_EXPERIMENTAL_CALIBRATED_SOURCE_MODEL_NAMES,
    POLAND_LAG24_EXPERIMENTAL_CALIBRATION_STRATEGY_KIND,
    POLAND_LAG24_EXPERIMENTAL_NBEATSX_CALIBRATED_MODEL_NAME,
    POLAND_LAG24_EXPERIMENTAL_NBEATSX_MODEL_NAME,
    POLAND_LAG24_EXPERIMENTAL_ROLLING_STRATEGY_KIND,
    POLAND_LAG24_EXPERIMENTAL_TFT_CALIBRATED_MODEL_NAME,
    POLAND_LAG24_EXPERIMENTAL_TFT_MODEL_NAME,
    build_official_global_panel_nbeatsx_horizon_calibration_frame,
    build_official_global_panel_poland_lag24_experimental_horizon_calibrated_strict_lp_benchmark_frame,
    build_official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame,
    build_official_global_panel_tft_horizon_quantile_calibration_frame,
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
from smart_arbitrage.forecasting.nbu_fx import build_nbu_eur_uah_fx_metadata_frame
from smart_arbitrage.forecasting.entsoe_neighbor_access import (
    build_entsoe_poland_lagged_feature_candidate_frame,
    build_entsoe_neighbor_market_aligned_feature_panel_frame,
    build_entsoe_neighbor_market_feature_candidate_frame,
    build_entsoe_neighbor_market_sample_audit_frame,
    build_entsoe_neighbor_market_query_spec_frame,
    build_entsoe_poland_feature_governance_frame,
    load_entsoe_security_token,
)
from smart_arbitrage.forecasting.poland_neighbor_snapshot import (
    build_entsoe_poland_governance_closure_frame,
    build_poland_neighbor_market_hourly_feature_frame,
    build_poland_neighbor_market_snapshot_feature_candidate_frame,
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


class EntsoeNeighborMarketSampleAuditAssetConfig(dg.Config):
    """Tiny ENTSO-E neighbor-market sample audit scope."""

    sample_country_codes_csv: str = "PL"
    sample_period_start_utc: str = "202601010000"
    sample_period_end_utc: str = "202601020000"
    fetch_enabled: bool = False


class EntsoeNeighborMarketAlignedFeaturePanelAssetConfig(dg.Config):
    """Timestamp-aligned ENTSO-E neighbor feature panel scope."""

    country_codes_csv: str = "PL"


class EntsoePolandLaggedFeatureCandidateAssetConfig(dg.Config):
    """Prior-safe lagged Poland ENTSO-E feature candidate controls."""

    lag_hours: int = 24
    prior_eur_uah_fx_rate: float = 0.0
    prior_eur_uah_fx_timestamp_utc: str = ""
    fx_rate_source: str = ""


class NbuEurUahFxMetadataAssetConfig(dg.Config):
    """NBU EUR/UAH metadata scope for lagged Poland feature normalization."""

    lag_hours: int = 24
    fetch_enabled: bool = True


class EntsoePolandFeatureGovernanceAssetConfig(dg.Config):
    """Point-in-time Poland ENTSO-E feature governance controls."""

    publication_timestamp_utc: str = ""
    ua_decision_anchor_timestamp_utc: str = "2025-12-31T12:00:00+00:00"
    prior_eur_uah_fx_rate: float = 0.0
    prior_eur_uah_fx_timestamp_utc: str = ""
    fx_rate_source: str = ""
    timezone_dst_mapping_ready: bool = False
    licensing_approved: bool = False
    market_rules_mapped: bool = False
    domain_shift_validated: bool = False


class PolandNeighborMarketSnapshotFeatureCandidateAssetConfig(dg.Config):
    """No-token Poland snapshot candidate normalization controls."""

    ua_decision_anchor_timestamp_utc: str = "2025-12-31T12:00:00+00:00"
    prior_eur_uah_fx_rate: float = 0.0
    prior_eur_uah_fx_timestamp_utc: str = ""
    fx_rate_source: str = ""


class EntsoePolandGovernanceClosureAssetConfig(dg.Config):
    """Source-backed Poland hourly feature governance closure controls."""

    ua_decision_anchor_timestamp_utc: str = "2025-12-31T12:00:00+00:00"
    prior_eur_uah_fx_rate: float = 0.0
    prior_eur_uah_fx_timestamp_utc: str = ""
    fx_rate_source: str = ""
    timezone_dst_mapping_ready: bool = False
    licensing_approved: bool = False
    market_rules_mapped: bool = False
    domain_shift_validated: bool = False


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


class DflV2PlusDflDtBridgeAssetConfig(dg.Config):
    """V2+-anchored strict comparison scope for residual DFL and offline DT."""

    source_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    min_tenant_count: int = 5
    min_validation_tenant_anchor_count_per_source_model: int = 90
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.0
    min_mean_regret_improvement_ratio_vs_strict: float = 0.05


class DflOfficialGlobalPanelV2PlusDflDtBridgeAssetConfig(dg.Config):
    """Official global-panel V2+-teacher residual DFL/offline DT bridge scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = ",".join(
        OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS
    )
    final_validation_anchor_count_per_tenant: int = 18
    min_validation_tenant_anchor_count_per_source_model: int = 90
    min_tenant_count: int = 5
    switch_margin_grid_uah_csv: str = "0.0,50.0,100.0,200.0,400.0"
    min_confidence_improvement_ratio: float = 0.05
    high_value_quantile: float = 0.75
    context_length: int = 24
    hidden_dim: int = 32
    num_layers: int = 1
    num_heads: int = 2
    max_epochs: int = 5
    random_seed: int = 2026
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.0
    min_mean_regret_improvement_ratio_vs_strict: float = 0.05


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


class DflScheduleValueLearnerV3AssetConfig(dg.Config):
    """Prior-only schedule/value learner v3 ridge-ranker scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_validation_anchor_count_per_tenant: int = 18
    min_validation_tenant_anchor_count_per_source_model: int = 90
    ridge_regularization: float = 1.0


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


class DflOfficialGlobalPanelScheduleCandidateLibraryAssetConfig(dg.Config):
    """Official global-panel NBEATSx schedule-candidate library scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = (
        "nbeatsx_official_global_panel_v1,"
        "nbeatsx_official_global_panel_horizon_calibrated_v1"
    )
    final_validation_anchor_count_per_tenant: int = 1
    perturb_spread_scale_grid_csv: str = "0.9,1.1"
    perturb_mean_shift_grid_uah_mwh_csv: str = "-250.0,250.0"


class DflOfficialGlobalPanelScheduleValueLearnerV2AssetConfig(dg.Config):
    """Official global-panel schedule/value learner v2 scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = (
        "nbeatsx_official_global_panel_v1,"
        "nbeatsx_official_global_panel_horizon_calibrated_v1"
    )
    final_validation_anchor_count_per_tenant: int = 1
    min_validation_tenant_anchor_count_per_source_model: int = 5


class DflOfficialGlobalPanelScheduleValueLearnerV3AssetConfig(dg.Config):
    """Official global-panel schedule/value learner v3 scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = (
        "nbeatsx_official_global_panel_v1,"
        "nbeatsx_official_global_panel_horizon_calibrated_v1"
    )
    final_validation_anchor_count_per_tenant: int = 1
    min_validation_tenant_anchor_count_per_source_model: int = 5
    ridge_regularization: float = 1.0


class DflScheduleCandidateLibraryV2PlusAssetConfig(dg.Config):
    """V2+ deterministic schedule-candidate expansion scope."""

    rank_perturbation_delta_uah_mwh: float = 250.0
    robust_spread_scales_csv: str = "0.8,0.9"
    strict_neighborhood_shift_hours_csv: str = "-1,1"
    block_reconcile_hours_csv: str = "3,6"
    terminal_target_shift_uah_mwh: float = 100.0


class DflScheduleValueLearnerV2PlusAssetConfig(dg.Config):
    """V2+ schedule/value selector scope with frozen V2 fallback."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = "tft_silver_v0,nbeatsx_silver_v0"
    final_validation_anchor_count_per_tenant: int = 18
    min_validation_tenant_anchor_count_per_source_model: int = 90
    min_prior_mean_improvement_ratio_vs_v2: float = 0.01


class DflOfficialGlobalPanelScheduleValueLearnerV2PlusAssetConfig(dg.Config):
    """Official global-panel V2+ selector scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = (
        "nbeatsx_official_global_panel_v1,"
        "nbeatsx_official_global_panel_horizon_calibrated_v1"
    )
    final_validation_anchor_count_per_tenant: int = 18
    min_validation_tenant_anchor_count_per_source_model: int = 90
    min_prior_mean_improvement_ratio_vs_v2: float = 0.01


class DflPolandLag24ExperimentalRollingStrictAssetConfig(dg.Config):
    """Poland lag-24 experimental official rolling strict LP/oracle scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    enabled_forecast_model_names_csv: str = (
        "nbeatsx_official_global_panel_poland_lag24_experimental_v1,"
        "tft_official_global_panel_poland_lag24_experimental_v1"
    )
    max_eval_windows: int = 18
    horizon_hours: int = 24
    nbeatsx_max_steps: int = 20
    nbeatsx_random_seed: int = 20260520
    tft_max_epochs: int = 5
    tft_max_steps: int = 8
    tft_batch_size: int = 8
    tft_learning_rate: float = 0.005
    tft_hidden_size: int = 8
    tft_hidden_continuous_size: int = 4
    tft_accelerator: str = "auto"
    tft_devices: str = "auto"
    anchor_batch_order: str = "latest_first"
    anchor_batch_start_index: int = 0
    anchor_batch_size: int = 0
    generated_at_iso: str = ""
    resume_generated_at_iso: str = ""
    merge_persisted_batches: bool = False


class DflPolandLag24ExperimentalCalibrationAssetConfig(dg.Config):
    """Prior-only calibration scope for Poland lag-24 experimental forecasts."""

    min_prior_anchors: int = 14
    rolling_calibration_window_anchors: int = 28


class DflOfficialGlobalPanelScheduleValueDflV2AssetConfig(dg.Config):
    """Official global-panel V2+-anchored pairwise DFL v2 scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = (
        "nbeatsx_official_global_panel_v1,"
        "nbeatsx_official_global_panel_horizon_calibrated_v1"
    )
    final_validation_anchor_count_per_tenant: int = 18
    min_validation_tenant_anchor_count_per_source_model: int = 90
    min_prior_mean_improvement_ratio_vs_v2_plus: float = 0.01
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.0
    min_mean_regret_improvement_ratio_vs_strict: float = 0.05


class DflOfficialGlobalPanelCandidateValueDflV3AssetConfig(dg.Config):
    """Official global-panel V2+-anchored candidate-value DFL v3 scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = (
        "nbeatsx_official_global_panel_v1,"
        "nbeatsx_official_global_panel_horizon_calibrated_v1"
    )
    final_validation_anchor_count_per_tenant: int = 18
    min_validation_tenant_anchor_count_per_source_model: int = 90
    strict_neighborhood_shift_hours_csv: str = "-2,-1,1,2"
    terminal_target_shift_uah_mwh_csv: str = "-150.0,150.0"
    peak_trough_delta_uah_mwh: float = 350.0
    uncertainty_spread_scales_csv: str = "0.7,1.1"
    degradation_spread_scales_csv: str = "0.6,0.85"
    include_train_oracle_neighborhood: bool = True
    max_train_generation_anchor_count_per_tenant: int = 60
    min_prior_template_anchor_count: int = 3
    min_prior_mean_improvement_ratio_vs_v2_plus: float = 0.01
    pairwise_loss_weight: float = 0.05
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.0
    min_mean_regret_improvement_ratio_vs_strict: float = 0.05


class DflOfficialGlobalPanelCandidateValueDflV4AssetConfig(dg.Config):
    """Official global-panel plateau-breaker candidate-value DFL v4 scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = (
        "nbeatsx_official_global_panel_v1,"
        "nbeatsx_official_global_panel_horizon_calibrated_v1"
    )
    final_validation_anchor_count_per_tenant: int = 18
    min_validation_tenant_anchor_count_per_source_model: int = 90
    quantile_risk_spread_scales_csv: str = "1.25,1.5"
    block_peak_delta_uah_mwh: float = 225.0
    terminal_reserve_shift_uah_mwh: float = 250.0
    spread_volatility_scale: float = 0.65
    tenant_sweep_spread_scales_csv: str = "0.75,1.35"
    include_train_oracle_neighborhood: bool = True
    max_train_generation_anchor_count_per_tenant: int = 20
    min_prior_mean_improvement_ratio_vs_v2_plus: float = 0.01
    ridge_l2: float = 1.0
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.0
    min_mean_regret_improvement_ratio_vs_strict: float = 0.05


class DflContextEnrichedCandidateValueDflV5AssetConfig(
    DflOfficialGlobalPanelCandidateValueDflV4AssetConfig
):
    """Context-enriched candidate-value DFL v5 scope."""


class DflTftQuantileScheduleValueAssetConfig(dg.Config):
    """TFT quantile schedule/value contributor scope against frozen V2+."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = ",".join(TFT_QUANTILE_SOURCE_MODELS)
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME
    final_validation_anchor_count_per_tenant: int = 18
    min_validation_tenant_anchor_count_per_source_model: int = 90
    min_prior_mean_improvement_ratio_vs_v2: float = 0.01
    min_mean_regret_improvement_ratio_vs_baseline: float = 0.0
    perturb_spread_scale_grid_csv: str = "0.9,1.1"
    perturb_mean_shift_grid_uah_mwh_csv: str = "-250.0,250.0"


class DflTftCalibratedQuantileScheduleValueAssetConfig(
    DflTftQuantileScheduleValueAssetConfig
):
    """Calibrated TFT quantile contributor scope against frozen V2+."""

    forecast_model_names_csv: str = ",".join(TFT_QUANTILE_CALIBRATED_SOURCE_MODELS)


class DflNbeatsxTftCombinedPortfolioAssetConfig(dg.Config):
    """Candidate-level NBEATSx V2+ plus TFT portfolio selector scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME
    tft_source_model_names_csv: str = ",".join(TFT_QUANTILE_CALIBRATED_SOURCE_MODELS)
    combined_source_model_name: str = DEFAULT_COMBINED_SOURCE_MODEL_NAME
    final_validation_anchor_count_per_tenant: int = 18
    min_validation_tenant_anchor_count: int = 90
    min_prior_mean_improvement_ratio_vs_v2_plus: float = 0.05
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.05
    validation_window_count: int = 4
    validation_anchor_count: int = 18
    min_prior_anchors_before_window: int = 30
    max_tft_candidates_per_anchor_source_family: int = 3


class DflOfficialGlobalPanelScheduleValueLearnerV2RobustnessAssetConfig(dg.Config):
    """Rolling robustness scope for official global-panel schedule/value v2."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = (
        "nbeatsx_official_global_panel_v1,"
        "nbeatsx_official_global_panel_horizon_calibrated_v1"
    )
    validation_window_count: int = 2
    validation_anchor_count: int = 1
    min_prior_anchors_before_window: int = 1
    min_robust_passing_windows: int = 1
    min_validation_tenant_anchor_count_per_source_model: int = 5


class DflOfficialGlobalPanelScheduleValueLearnerV2PlusRobustnessAssetConfig(dg.Config):
    """Rolling robustness scope for official global-panel schedule/value v2+."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = (
        "nbeatsx_official_global_panel_v1,"
        "nbeatsx_official_global_panel_horizon_calibrated_v1"
    )
    validation_window_count: int = 4
    validation_anchor_count: int = 18
    min_prior_anchors_before_window: int = 30
    min_robust_passing_windows: int = 3
    min_validation_tenant_anchor_count_per_source_model: int = 90
    min_prior_mean_improvement_ratio_vs_v2: float = 0.01


class DflPolandLag24CalibratedRobustnessAssetConfig(
    DflOfficialGlobalPanelScheduleValueLearnerV2PlusRobustnessAssetConfig
):
    """Rolling robustness scope for calibrated Poland-enhanced V2+ schedules."""

    forecast_model_names_csv: str = (
        "nbeatsx_official_global_panel_poland_lag24_horizon_calibrated_v1,"
        "tft_official_global_panel_poland_lag24_horizon_quantile_calibrated_v1"
    )


class DflPolandLag24RollingVsFrozenGateAssetConfig(dg.Config):
    """Gate calibrated Poland rolling windows against frozen Ukrainian V2+."""

    min_mean_regret_improvement_ratio_vs_frozen_v2_plus: float = 0.05
    min_passing_windows: int = 3


class DflPolandLag24CandidateValueRankerAssetConfig(dg.Config):
    """Prior-only tabular ranker over Poland-enhanced schedule candidates."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    forecast_model_names_csv: str = (
        "nbeatsx_official_global_panel_poland_lag24_horizon_calibrated_v1,"
        "tft_official_global_panel_poland_lag24_horizon_quantile_calibrated_v1"
    )
    min_prior_mean_improvement_ratio_vs_frozen_proxy: float = 0.01
    ridge_l2: float = 1.0


class DflLavaScheduleNeighborBridgeAssetConfig(dg.Config):
    """V2+-anchored LAVA schedule-neighbor teacher-label bridge scope."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME
    poland_source_model_names_csv: str = (
        "nbeatsx_official_global_panel_poland_lag24_horizon_calibrated_v1,"
        "tft_official_global_panel_poland_lag24_horizon_quantile_calibrated_v1"
    )
    tail_risk_delta_uah: float = 150.0
    min_prior_mean_improvement_ratio_vs_v2_plus: float = 0.05
    min_validation_tenant_anchor_count: int = 90
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.0
    min_mean_regret_improvement_ratio_vs_strict: float = 0.05
    ridge_l2: float = 10.0
    include_oracle_train_diagnostics: bool = True


class DflLavaTailRiskTargetAssetConfig(DflLavaScheduleNeighborBridgeAssetConfig):
    """Tail-risk-aware LAVA/DT candidate-index target scope."""

    min_prior_safe_win_count: int = 1
    max_prior_tail_loss_count: int = 0
    min_prior_precision: float = 0.75
    min_prior_mean_improvement_uah: float = 1.0
    min_predicted_improvement_uah: float = 1.0
    min_dt_return_to_go_delta_uah: float = 1.0
    max_dt_tail_risk_probability: float = 0.25
    max_predicted_tail_risk_probability: float = 0.25
    safe_switch_candidate_sources_csv: str = "poland_shadow_candidate"
    require_family_tail_loss_free: bool = True
    hard_blocked_candidate_families_csv: str = "rank_extrema_perturbation_v2_plus"


class DflOracleGapSafeSwitchAssetConfig(dg.Config):
    """Oracle-gap safe-switch gate before schedule-neighbor DT/LAVA."""

    tenant_ids_csv: str = (
        "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,"
        "client_004_kharkiv_hospital,client_005_odesa_hotel"
    )
    source_model_names_csv: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME
    tail_risk_delta_uah: float = 150.0
    min_prior_safe_win_count: int = 1
    min_prior_mean_improvement_uah: float = 1.0
    min_predicted_improvement_uah: float = 1.0
    max_predicted_tail_risk_probability: float = 0.25
    allowed_candidate_sources_csv: str = (
        "oracle_gap_candidate,poland_shadow_candidate,tft_shadow_candidate"
    )
    ridge_l2: float = 10.0
    min_validation_tenant_anchor_count: int = 90
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.05
    min_mean_regret_improvement_ratio_vs_strict: float = 0.05
    validation_window_count: int = 4
    validation_anchor_count: int = 18
    min_prior_anchors_before_window: int = 30


class DflUaContextSafeSwitchAssetConfig(DflOracleGapSafeSwitchAssetConfig):
    """Ukrainian-context safe-switch gate before DT/LAVA."""

    scorer_kinds_csv: str = "sklearn,torch"
    torch_hidden_size: int = 8
    torch_max_epochs: int = 20
    use_cuda_if_available: bool = True


class DflUaContextLavaDtAssetConfig(DflUaContextSafeSwitchAssetConfig):
    """UA-context LAVA/DT candidate-index policy gate."""

    max_prior_tail_loss_count: int = 0
    min_prior_precision: float = 0.75
    hard_blocked_candidate_families_csv: str = "rank_extrema_perturbation_v2_plus"
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME
    random_seed: int = 23
    torch_max_epochs: int = 25


class DflRegretSurrogateV1AssetConfig(DflUaContextSafeSwitchAssetConfig):
    """Learning-limit audit plus regret-surrogate candidate-value DFL v1."""

    allowed_candidate_sources_csv: str = (
        "oracle_gap_candidate,poland_shadow_candidate,tft_shadow_candidate,"
        "ua_context_candidate,lava_candidate"
    )
    min_oracle_improvement_ratio_vs_v2_plus: float = 0.05
    material_switch_delta_uah: float = 25.0
    high_v2_regret_uah: float = 500.0
    high_forecast_spread_uah_mwh: float = 10_000.0
    min_material_schedule_distance: float = 0.02
    min_context_prior_support_count: int = 1
    min_context_prior_safe_win_count: int = 1
    min_context_prior_mean_improvement_uah: float = 1.0
    max_context_tail_risk_probability: float = 0.25


class DflOfficialGlobalPanelScheduleValueProductionGateAssetConfig(dg.Config):
    """Offline promotion gate for official global-panel schedule/value evidence."""

    source_model_names_csv: str = (
        "nbeatsx_official_global_panel_v1,"
        "nbeatsx_official_global_panel_horizon_calibrated_v1"
    )
    min_tenant_count: int = 5
    min_validation_tenant_anchor_count_per_source_model: int = 90
    min_mean_regret_improvement_ratio: float = 0.05
    min_rolling_window_count: int = 4
    min_rolling_strict_pass_windows: int = 3


class DflMarketCouplingV2PlusAblationAssetConfig(dg.Config):
    """Governed market-coupling ablation gate for official global-panel V2+."""

    source_model_names_csv: str = (
        "nbeatsx_official_global_panel_v1,"
        "nbeatsx_official_global_panel_horizon_calibrated_v1"
    )
    min_tenant_count: int = 5
    min_validation_tenant_anchor_count_per_source_model: int = 90
    min_window_count: int = 4


class DflMarketCoupledScheduleValueLearnerV2PlusAssetConfig(
    DflOfficialGlobalPanelScheduleValueLearnerV2PlusAssetConfig
):
    """Experimental Poland-lagged B variant against frozen Ukrainian-only V2+."""

    min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus: float = 0.01


class DflMarketCoupledScheduleValueLearnerV2PlusRobustnessAssetConfig(
    DflOfficialGlobalPanelScheduleValueLearnerV2PlusRobustnessAssetConfig
):
    """Rolling robustness scope for the experimental Poland-lagged B variant."""

    min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus: float = 0.01


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

    ensemble_frame = build_value_aware_ensemble_frame(
        real_data_rolling_origin_benchmark_frame
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
        [
            real_data_rolling_origin_benchmark_frame,
            real_data_value_aware_ensemble_frame,
        ],
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
            "tenant_count": training_frame.select("tenant_id").n_unique()
            if training_frame.height
            else 0,
            "model_count": training_frame.select("forecast_model_name").n_unique()
            if training_frame.height
            else 0,
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

    training_example_frame = build_dfl_training_example_frame(
        real_data_rolling_origin_benchmark_frame
    )
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

    security_token = _entsoe_security_token()
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
            "fetch_allowed_rows": query_spec_frame.filter(
                pl.col("fetch_allowed")
            ).height
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
        ml_stage="feature_engineering",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def entsoe_neighbor_market_sample_audit_frame(
    context,
    config: EntsoeNeighborMarketSampleAuditAssetConfig,
    entsoe_neighbor_market_query_spec_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Tiny ENTSO-E sample audit that never unlocks training use by itself."""

    security_token = _entsoe_security_token()
    sample_frame = build_entsoe_neighbor_market_sample_audit_frame(
        entsoe_neighbor_market_query_spec_frame,
        sample_country_codes_csv=config.sample_country_codes_csv,
        sample_period_start_utc=config.sample_period_start_utc,
        sample_period_end_utc=config.sample_period_end_utc,
        security_token=security_token,
        fetch_enabled=config.fetch_enabled,
    )
    _add_metadata(
        context,
        {
            "rows": sample_frame.height,
            "fetched_country_count": sample_frame.filter(
                pl.col("source_backed_row_count") > 0
            )
            .select("country_code")
            .n_unique()
            if sample_frame.height
            else 0,
            "source_backed_rows": sample_frame.select(
                pl.col("source_backed_row_count").sum()
            )
            .to_series()
            .item()
            if sample_frame.height
            else 0,
            "fetch_enabled": config.fetch_enabled,
            "security_token_available": bool(security_token),
            "scope": "entsoe_neighbor_market_sample_audit_research_gate",
            "not_market_execution": True,
        },
    )
    return sample_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="feature_engineering",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def entsoe_neighbor_market_feature_candidate_frame(
    context,
    config: EntsoeNeighborMarketSampleAuditAssetConfig,
    entsoe_neighbor_market_query_spec_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Source-backed ENTSO-E feature candidates that remain blocked from training."""

    security_token = _entsoe_security_token()
    candidate_frame = build_entsoe_neighbor_market_feature_candidate_frame(
        entsoe_neighbor_market_query_spec_frame,
        sample_country_codes_csv=config.sample_country_codes_csv,
        sample_period_start_utc=config.sample_period_start_utc,
        sample_period_end_utc=config.sample_period_end_utc,
        security_token=security_token,
        fetch_enabled=config.fetch_enabled,
    )
    _add_metadata(
        context,
        {
            "rows": candidate_frame.height,
            "source_backed_rows": candidate_frame.filter(pl.col("source_backed")).height
            if candidate_frame.height
            else 0,
            "feature_allowed_rows": candidate_frame.filter(
                pl.col("feature_use_allowed")
            ).height
            if candidate_frame.height
            else 0,
            "training_allowed_rows": candidate_frame.filter(
                pl.col("training_use_allowed")
            ).height
            if candidate_frame.height
            else 0,
            "fetch_enabled": config.fetch_enabled,
            "security_token_available": bool(security_token),
            "scope": "entsoe_neighbor_market_feature_candidate_research_gate",
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
        ml_stage="feature_engineering",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def nbu_eur_uah_fx_metadata_frame(
    context,
    config: NbuEurUahFxMetadataAssetConfig,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-known NBU EUR/UAH metadata for lagged market-coupling features."""

    fx_frame = build_nbu_eur_uah_fx_metadata_frame(
        real_data_benchmark_silver_feature_frame,
        lag_hours=config.lag_hours,
        fetch_enabled=config.fetch_enabled,
    )
    _add_metadata(
        context,
        {
            "rows": fx_frame.height,
            "source_backed_rows": fx_frame.filter(pl.col("source_backed")).height
            if fx_frame.height
            else 0,
            "first_effective_date": fx_frame.select(
                pl.col("fx_rate_effective_date").min()
            ).item()
            if fx_frame.height
            else "",
            "last_effective_date": fx_frame.select(
                pl.col("fx_rate_effective_date").max()
            ).item()
            if fx_frame.height
            else "",
            "fetch_enabled": config.fetch_enabled,
            "scope": "nbu_eur_uah_fx_metadata_research_gate",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return fx_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="feature_engineering",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def entsoe_poland_lagged_feature_candidate_frame(
    context,
    config: EntsoePolandLaggedFeatureCandidateAssetConfig,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    entsoe_neighbor_market_feature_candidate_frame: pl.DataFrame,
    nbu_eur_uah_fx_metadata_frame: pl.DataFrame | None = None,
    poland_neighbor_market_snapshot_feature_candidate_frame: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Prior-safe lagged Poland feature candidates with FX metadata."""

    candidate_frame = _concat_feature_candidate_frames(
        entsoe_neighbor_market_feature_candidate_frame,
        poland_neighbor_market_snapshot_feature_candidate_frame,
    )
    lagged_frame = build_entsoe_poland_lagged_feature_candidate_frame(
        real_data_benchmark_silver_feature_frame,
        candidate_frame,
        lag_hours=config.lag_hours,
        prior_eur_uah_fx_rate=config.prior_eur_uah_fx_rate,
        prior_eur_uah_fx_timestamp_utc=config.prior_eur_uah_fx_timestamp_utc,
        fx_rate_source=config.fx_rate_source,
        nbu_eur_uah_fx_metadata_frame=nbu_eur_uah_fx_metadata_frame,
    )
    _add_metadata(
        context,
        {
            "rows": lagged_frame.height,
            "source_backed_rows": lagged_frame.filter(pl.col("source_backed")).height
            if lagged_frame.height
            else 0,
            "coverage_status": lagged_frame.select("coverage_status").to_series().item(0)
            if lagged_frame.height and "coverage_status" in lagged_frame.columns
            else "empty",
            "feature_column": lagged_frame.select("feature_column").to_series().item(0)
            if lagged_frame.height
            else "",
            "lag_hours": config.lag_hours,
            "fx_rate_source": config.fx_rate_source,
            "scope": "entsoe_poland_lagged_feature_candidate_research_gate",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return lagged_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="feature_engineering",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def poland_neighbor_market_snapshot_feature_candidate_frame(
    context,
    config: PolandNeighborMarketSnapshotFeatureCandidateAssetConfig,
    poland_neighbor_market_snapshot_bronze: pl.DataFrame,
) -> pl.DataFrame:
    """Convert no-token Poland snapshots into governed feature candidates."""

    candidate_frame = build_poland_neighbor_market_snapshot_feature_candidate_frame(
        poland_neighbor_market_snapshot_bronze,
        ua_decision_anchor_timestamp_utc=config.ua_decision_anchor_timestamp_utc,
        prior_eur_uah_fx_rate=config.prior_eur_uah_fx_rate,
        prior_eur_uah_fx_timestamp_utc=config.prior_eur_uah_fx_timestamp_utc,
        fx_rate_source=config.fx_rate_source,
    )
    _add_metadata(
        context,
        {
            "rows": candidate_frame.height,
            "source_backed_rows": candidate_frame.filter(pl.col("source_backed")).height
            if candidate_frame.height
            else 0,
            "training_allowed_rows": candidate_frame.filter(
                pl.col("training_use_allowed")
            ).height
            if candidate_frame.height
            else 0,
            "security_token_required_rows": candidate_frame.filter(
                pl.col("security_token_required")
            ).height
            if candidate_frame.height
            else 0,
            "scope": "poland_neighbor_market_snapshot_feature_candidate_research_gate",
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
        ml_stage="feature_engineering",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def poland_neighbor_market_hourly_feature_frame(
    context,
    poland_neighbor_market_snapshot_bronze: pl.DataFrame,
) -> pl.DataFrame:
    """Aggregate source-backed Poland snapshots to hourly exogenous evidence."""

    hourly_frame = build_poland_neighbor_market_hourly_feature_frame(
        poland_neighbor_market_snapshot_bronze
    )
    _add_metadata(
        context,
        {
            "rows": hourly_frame.height,
            "source_backed_hour_count": hourly_frame.filter(
                pl.col("source_backed")
            ).height
            if hourly_frame.height
            else 0,
            "training_allowed_rows": hourly_frame.filter(
                pl.col("training_use_allowed")
            ).height
            if hourly_frame.height
            else 0,
            "feature_allowed_rows": hourly_frame.filter(
                pl.col("feature_use_allowed")
            ).height
            if hourly_frame.height
            else 0,
            "feature_columns": hourly_frame["feature_column"].unique().to_list()
            if hourly_frame.height
            else [],
            "scope": "poland_neighbor_market_hourly_feature_research_gate",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return hourly_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="feature_engineering",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def entsoe_poland_governance_closure_frame(
    context,
    config: EntsoePolandGovernanceClosureAssetConfig,
    poland_neighbor_market_hourly_feature_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Close the source-backed Poland feature lane with explicit governance blockers."""

    closure_frame = build_entsoe_poland_governance_closure_frame(
        poland_neighbor_market_hourly_feature_frame,
        ua_decision_anchor_timestamp_utc=config.ua_decision_anchor_timestamp_utc,
        prior_eur_uah_fx_rate=config.prior_eur_uah_fx_rate,
        prior_eur_uah_fx_timestamp_utc=config.prior_eur_uah_fx_timestamp_utc,
        fx_rate_source=config.fx_rate_source,
        timezone_dst_mapping_ready=config.timezone_dst_mapping_ready,
        licensing_approved=config.licensing_approved,
        market_rules_mapped=config.market_rules_mapped,
        domain_shift_validated=config.domain_shift_validated,
    )
    _add_metadata(
        context,
        {
            "rows": closure_frame.height,
            "approved_feature_count": closure_frame.filter(
                pl.col("approved_for_official_training")
            ).height
            if closure_frame.height
            else 0,
            "training_allowed_rows": closure_frame.filter(
                pl.col("training_use_allowed")
            ).height
            if closure_frame.height
            else 0,
            "training_blockers": closure_frame["training_blockers_csv"].to_list()
            if closure_frame.height
            else [],
            "scope": "entsoe_poland_governance_closure_research_gate",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return closure_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="feature_engineering",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def entsoe_poland_feature_governance_frame(
    context,
    config: EntsoePolandFeatureGovernanceAssetConfig,
    entsoe_neighbor_market_feature_candidate_frame: pl.DataFrame,
    poland_neighbor_market_snapshot_feature_candidate_frame: pl.DataFrame | None = None,
    entsoe_poland_lagged_feature_candidate_frame: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Point-in-time governance gate for one Poland ENTSO-E exogenous feature."""

    security_token = _entsoe_security_token()
    candidate_frame = _concat_feature_candidate_frames(
        entsoe_neighbor_market_feature_candidate_frame,
        poland_neighbor_market_snapshot_feature_candidate_frame,
    )
    candidate_frame = _concat_feature_candidate_frames(
        candidate_frame,
        entsoe_poland_lagged_feature_candidate_frame,
    )
    governance_frame = build_entsoe_poland_feature_governance_frame(
        candidate_frame,
        entsoe_security_token=security_token,
        publication_timestamp_utc=config.publication_timestamp_utc,
        ua_decision_anchor_timestamp_utc=config.ua_decision_anchor_timestamp_utc,
        prior_eur_uah_fx_rate=config.prior_eur_uah_fx_rate,
        prior_eur_uah_fx_timestamp_utc=config.prior_eur_uah_fx_timestamp_utc,
        fx_rate_source=config.fx_rate_source,
        timezone_dst_mapping_ready=config.timezone_dst_mapping_ready,
        licensing_approved=config.licensing_approved,
        market_rules_mapped=config.market_rules_mapped,
        domain_shift_validated=config.domain_shift_validated,
    )
    _add_metadata(
        context,
        {
            "rows": governance_frame.height,
            "approved_feature_count": governance_frame.filter(
                pl.col("approved_for_official_training")
            ).height
            if governance_frame.height
            else 0,
            "source_backed_rows": governance_frame.select(
                pl.col("source_backed_row_count").sum()
            ).item()
            if governance_frame.height
            else 0,
            "training_blockers": governance_frame["training_blockers_csv"].to_list()
            if governance_frame.height
            else [],
            "security_token_available": bool(security_token),
            "scope": "entsoe_poland_feature_governance_research_gate",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return governance_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="feature_engineering",
        evidence_scope="research_only",
        market_venue="DAM",
    ),
)
def entsoe_neighbor_market_aligned_feature_panel_frame(
    context,
    config: EntsoeNeighborMarketAlignedFeaturePanelAssetConfig,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    entsoe_neighbor_market_feature_candidate_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Timestamp-align ENTSO-E neighbor features without approving training use."""

    aligned_frame = build_entsoe_neighbor_market_aligned_feature_panel_frame(
        real_data_benchmark_silver_feature_frame,
        entsoe_neighbor_market_feature_candidate_frame,
        country_codes=_csv_values(
            config.country_codes_csv, field_name="country_codes_csv"
        ),
    )
    _add_metadata(
        context,
        {
            "rows": aligned_frame.height,
            "tenant_count": aligned_frame.select("tenant_id").n_unique()
            if aligned_frame.height
            else 0,
            "country_codes": sorted(aligned_frame["country_code"].unique().to_list())
            if aligned_frame.height
            else [],
            "source_backed_rows": aligned_frame.filter(pl.col("source_backed")).height
            if aligned_frame.height
            else 0,
            "feature_allowed_rows": aligned_frame.filter(
                pl.col("feature_use_allowed")
            ).height
            if aligned_frame.height
            else 0,
            "training_allowed_rows": aligned_frame.filter(
                pl.col("training_use_allowed")
            ).height
            if aligned_frame.height
            else 0,
            "scope": "entsoe_neighbor_market_aligned_feature_research_gate",
            "not_market_execution": True,
        },
    )
    return aligned_frame


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
            )
            .sum()
            .item()
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
            "final_holdout_rows": panel_frame.filter(
                pl.col("split") == "final_holdout"
            ).height
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
            "mean_lp_value_failure_rate": audit_frame.select("lp_value_failure_rate")
            .mean()
            .item()
            if audit_frame.height
            else 0.0,
            "mean_spread_shape_failure_rate": audit_frame.select(
                "spread_shape_failure_rate"
            )
            .mean()
            .item()
            if audit_frame.height
            else 0.0,
            "mean_rank_extrema_failure_rate": audit_frame.select(
                "rank_extrema_failure_rate"
            )
            .mean()
            .item()
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
            "tenant_count": audit_frame.select("tenant_id").n_unique()
            if audit_frame.height
            else 0,
            "target_anchor_count_per_tenant": config.target_anchor_count_per_tenant,
            "minimum_eligible_anchor_count": audit_frame.select("eligible_anchor_count")
            .min()
            .item()
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
    gap_rows = (
        repair_frame.filter(pl.col("gap_kind") != "none")
        if repair_frame.height
        else pl.DataFrame()
    )
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
            "source_model_count": action_label_frame.select(
                "forecast_model_name"
            ).n_unique()
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
    strict_rows = strict_frame.filter(
        pl.col("forecast_model_name") == "strict_similar_day"
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "candidate_rows": candidate_rows.height,
            "strict_control_rows": strict_rows.height,
            "classifier_summary_rows": dfl_action_classifier_baseline_frame.height,
            "mean_candidate_regret_uah": candidate_rows.select("regret_uah")
            .mean()
            .item()
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
    generated_at = _latest_generated_at(
        dfl_schedule_value_learner_v2_strict_lp_benchmark_frame
    )
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
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
            if library_frame.height
            else 0,
            "final_holdout_rows": library_frame.filter(
                pl.col("split_name") == "final_holdout"
            ).height
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
        blend_weights=_float_csv_values(
            config.blend_weights_csv, field_name="blend_weights_csv"
        ),
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
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
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
            "profile_names": sorted(
                learner_frame["selected_weight_profile_name"].unique().to_list()
            )
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
        generated_at=_latest_generated_at(
            dfl_official_schedule_candidate_library_v2_frame
        ),
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
            "development_gate_passed": gate.metrics.get(
                "development_gate_passed", False
            ),
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
            "robust_source_model_names": gate.metrics.get(
                "robust_source_model_names", []
            ),
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
    generated_at = _latest_generated_at(
        dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame
    )
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
            "promoted_source_model_names": gate.metrics.get(
                "promoted_source_model_names", []
            ),
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
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_candidate_library_frame(
    context,
    config: DflOfficialGlobalPanelScheduleCandidateLibraryAssetConfig,
    nbeatsx_official_global_panel_rolling_calibrated_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Schedule library for official global-panel NBEATSx rolling evidence."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    library_frame = build_dfl_schedule_candidate_library_from_strict_benchmark_frame(
        nbeatsx_official_global_panel_rolling_calibrated_strict_lp_benchmark_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
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
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
            if library_frame.height
            else 0,
            "final_holdout_rows": library_frame.filter(
                pl.col("split_name") == "final_holdout"
            ).height
            if library_frame.height
            else 0,
            "scope": "dfl_official_global_panel_schedule_library_screen_not_full_dfl",
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_candidate_library_v2_frame(
    context,
    config: DflStrictChallengerAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Blend/residual candidates for official global-panel NBEATSx schedules."""

    library_frame = build_schedule_candidate_library_v2_frame(
        dfl_official_global_panel_schedule_candidate_library_frame,
        blend_weights=_float_csv_values(
            config.blend_weights_csv, field_name="blend_weights_csv"
        ),
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
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
            if library_frame.height
            else 0,
            "residual_min_prior_anchors": config.residual_min_prior_anchors,
            "scope": "dfl_official_global_panel_schedule_library_v2_screen_not_full_dfl",
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_candidate_library_v2_plus_frame(
    context,
    config: DflScheduleCandidateLibraryV2PlusAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Expanded V2+ candidate library for official global-panel schedules."""

    library_frame = build_dfl_schedule_candidate_library_v2_plus_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_frame,
        rank_perturbation_delta_uah_mwh=config.rank_perturbation_delta_uah_mwh,
        robust_spread_scales=_float_csv_values(
            config.robust_spread_scales_csv,
            field_name="robust_spread_scales_csv",
        ),
        strict_neighborhood_shift_hours=_int_csv_values(
            config.strict_neighborhood_shift_hours_csv,
            field_name="strict_neighborhood_shift_hours_csv",
        ),
        block_reconcile_hours=_int_csv_values(
            config.block_reconcile_hours_csv,
            field_name="block_reconcile_hours_csv",
        ),
        terminal_target_shift_uah_mwh=config.terminal_target_shift_uah_mwh,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_candidate_library_v2_frame
        ),
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
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
            if library_frame.height
            else 0,
            "scope": "dfl_official_global_panel_schedule_library_v2_plus_not_full_dfl",
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
        ml_stage="diagnostics",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_value_regret_decomposition_frame(
    context,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Regret autopsy for official global-panel V2 remaining losses."""

    decomposition_frame = build_dfl_schedule_value_regret_decomposition_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        dfl_official_global_panel_schedule_value_learner_v2_frame,
    )
    _add_metadata(
        context,
        {
            "rows": decomposition_frame.height,
            "failure_modes": sorted(
                decomposition_frame["failure_mode"].unique().to_list()
            )
            if decomposition_frame.height
            else [],
            "scope": "dfl_official_global_panel_schedule_value_regret_autopsy_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return decomposition_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_value_learner_v2_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV2AssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only schedule/value learner over global-panel schedule candidates."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    learner_frame = build_dfl_schedule_value_learner_v2_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
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
            "profile_names": sorted(
                learner_frame["selected_weight_profile_name"].unique().to_list()
            )
            if learner_frame.height
            else [],
            "scope": "dfl_official_global_panel_schedule_value_v2_screen_not_full_dfl",
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_value_learner_v2_strict_lp_benchmark_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV2AssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle rows for global-panel schedule/value screening."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_schedule_value_learner_v2_strict_lp_benchmark_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_frame,
        dfl_official_global_panel_schedule_value_learner_v2_frame,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_candidate_library_v2_frame
        ),
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
            "development_gate_passed": gate.metrics.get(
                "development_gate_passed", False
            ),
            "production_gate_passed": gate.metrics.get("production_gate_passed", False),
            "scope": (
                "dfl_official_global_panel_schedule_value_v2_strict_gate_"
                "screen_not_full_dfl"
            ),
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
def dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame(
    context,
    config: DflV2PlusDflDtBridgeAssetConfig,
    dfl_residual_dt_fallback_strict_lp_benchmark_frame: pl.DataFrame,
    dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Compare residual DFL and offline DT challengers against frozen V2+."""

    source_model_names = _forecast_model_names(config.source_model_names_csv)
    strict_frame = build_dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame(
        dfl_residual_dt_fallback_strict_lp_benchmark_frame,
        dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        source_model_names=source_model_names,
        generated_at=_latest_generated_at(
            dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_v2_plus_dfl_dt_bridge_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_tenant_count=config.min_tenant_count,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "strategy_kind": DFL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "best_challenger_role": gate.metrics.get("best_challenger_role"),
            "best_source_model_name": gate.metrics.get("best_source_model_name"),
            "offline_strategy_challenger_passed": gate.metrics.get(
                "offline_strategy_challenger_passed",
                False,
            ),
            "v2_plus_headline_baseline": V2_PLUS_HEADLINE_BASELINE_METRICS,
            "production_promote": False,
            "market_execution_enabled": False,
            "scope": "dfl_v2_plus_dfl_dt_bridge_not_full_dfl",
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_value_learner_v3_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV3AssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only V3 ridge ranker over global-panel schedule candidates."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    learner_frame = build_dfl_schedule_value_learner_v3_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        ridge_regularization=config.ridge_regularization,
    )
    _add_metadata(
        context,
        {
            "rows": learner_frame.height,
            "tenant_count": learner_frame.select("tenant_id").n_unique()
            if learner_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "profile_names": sorted(
                learner_frame["selected_weight_profile_name"].unique().to_list()
            )
            if learner_frame.height
            else [],
            "scope": "dfl_official_global_panel_schedule_value_v3_screen_not_full_dfl",
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_value_learner_v3_strict_lp_benchmark_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV3AssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v3_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle rows for global-panel schedule/value V3 screening."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_schedule_value_learner_v3_strict_lp_benchmark_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_frame,
        dfl_official_global_panel_schedule_value_learner_v3_frame,
        dfl_official_global_panel_schedule_value_learner_v2_frame,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_candidate_library_v2_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_schedule_value_learner_v3_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
    )
    learner_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("dfl_schedule_value_learner_v3_")
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
            "strategy_kind": DFL_SCHEDULE_VALUE_LEARNER_V3_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "development_gate_passed": gate.metrics.get(
                "development_gate_passed", False
            ),
            "production_gate_passed": gate.metrics.get("production_gate_passed", False),
            "market_execution_enabled": False,
            "scope": (
                "dfl_official_global_panel_schedule_value_v3_strict_gate_"
                "screen_not_full_dfl"
            ),
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_value_learner_v2_plus_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV2PlusAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only V2+ selector with frozen V2 fallback for official schedules."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    learner_frame = build_dfl_schedule_value_learner_v2_plus_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        dfl_official_global_panel_schedule_value_learner_v2_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_v2=(
            config.min_prior_mean_improvement_ratio_vs_v2
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
            "fallback_rows": learner_frame.filter(pl.col("fallback_to_v2")).height
            if learner_frame.height
            else 0,
            "scope": "dfl_official_global_panel_schedule_value_v2_plus_screen_not_full_dfl",
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV2PlusAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle rows for official global-panel V2+ screening."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_frame,
        dfl_official_global_panel_schedule_value_learner_v2_frame,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_candidate_library_v2_plus_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_schedule_value_learner_v2_plus_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
    )
    learner_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with(
            "dfl_schedule_value_learner_v2_plus_"
        )
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
            "strategy_kind": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "development_gate_passed": gate.metrics.get(
                "development_gate_passed", False
            ),
            "production_gate_passed": gate.metrics.get("production_gate_passed", False),
            "market_execution_enabled": False,
            "scope": (
                "dfl_official_global_panel_schedule_value_v2_plus_strict_gate_"
                "screen_not_full_dfl"
            ),
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_value_learner_v2_plus_oracle_gap_audit_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV2PlusAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Audit where V2+ has no better candidate versus misses a better schedule."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    audit_frame = build_dfl_schedule_value_learner_v2_plus_oracle_gap_audit_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        source_model_names=source_model_names,
    )
    _add_metadata(
        context,
        {
            "rows": audit_frame.height,
            "oracle_gap_classes": sorted(audit_frame["oracle_gap_class"].unique().to_list())
            if audit_frame.height
            else [],
            "positive_oracle_gap_rows": audit_frame.filter(
                pl.col("oracle_gap_to_best_candidate_uah") > 0.0
            ).height
            if audit_frame.height
            else 0,
            "scope": (
                "dfl_official_global_panel_schedule_value_v2_plus_oracle_gap_"
                "audit_not_full_dfl"
            ),
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
        backend="official_global_panel_oracle_gap_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_oracle_gap_safe_switch_label_frame(
    context,
    config: DflOracleGapSafeSwitchAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_oracle_gap_audit_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Teacher labels for oracle-gap safe switches against corrected V2+."""

    source_model_names = _forecast_model_names(config.source_model_names_csv)
    label_frame = build_dfl_oracle_gap_safe_switch_label_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_frame,
        dfl_official_global_panel_schedule_value_learner_v2_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_oracle_gap_audit_frame,
        source_model_names=source_model_names,
        tail_risk_delta_uah=config.tail_risk_delta_uah,
    )
    _add_metadata(
        context,
        {
            "rows": label_frame.height,
            "source_model_names": list(source_model_names),
            "safe_switch_label_rows": label_frame.filter(
                pl.col("label_safe_switch_win")
            ).height
            if label_frame.height
            else 0,
            "tail_risk_label_rows": label_frame.filter(
                pl.col("label_tail_risk_loss")
            ).height
            if label_frame.height
            else 0,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_oracle_gap_safe_switch_labels_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return label_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="feature_engineering",
        evidence_scope="not_market_execution",
        backend="official_global_panel_oracle_gap_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_oracle_gap_safe_switch_feature_panel_frame(
    context,
    dfl_oracle_gap_safe_switch_label_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only selector features for oracle-gap safe switches."""

    feature_panel = build_dfl_oracle_gap_safe_switch_feature_panel_frame(
        dfl_oracle_gap_safe_switch_label_frame
    )
    feature_columns = [
        column for column in feature_panel.columns if column.startswith("selector_feature_")
    ]
    _add_metadata(
        context,
        {
            "rows": feature_panel.height,
            "selector_feature_columns": feature_columns,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_oracle_gap_safe_switch_feature_panel_not_full_dfl",
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
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_oracle_gap_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_oracle_gap_safe_switch_scorer_frame(
    context,
    config: DflOracleGapSafeSwitchAssetConfig,
    dfl_oracle_gap_safe_switch_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Train the conservative prior-only oracle-gap safe-switch scorer."""

    scorer_frame = build_dfl_oracle_gap_safe_switch_scorer_frame(
        dfl_oracle_gap_safe_switch_feature_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=_forecast_model_names(config.source_model_names_csv),
        min_prior_safe_win_count=config.min_prior_safe_win_count,
        min_prior_mean_improvement_uah=config.min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=config.min_predicted_improvement_uah,
        max_predicted_tail_risk_probability=(
            config.max_predicted_tail_risk_probability
        ),
        allowed_candidate_sources=_csv_values(
            config.allowed_candidate_sources_csv,
            field_name="allowed_candidate_sources_csv",
        ),
        ridge_l2=config.ridge_l2,
    )
    _add_metadata(
        context,
        {
            "rows": scorer_frame.height,
            "candidate_selected_tenant_sources": scorer_frame.filter(
                ~pl.col("fallback_to_v2_plus")
            ).height
            if scorer_frame.height
            else 0,
            "fallback_tenant_sources": scorer_frame.filter(
                pl.col("fallback_to_v2_plus")
            ).height
            if scorer_frame.height
            else 0,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_oracle_gap_safe_switch_scorer_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return scorer_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_oracle_gap_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_oracle_gap_safe_switch_strict_lp_benchmark_frame(
    context,
    config: DflOracleGapSafeSwitchAssetConfig,
    dfl_oracle_gap_safe_switch_feature_panel_frame: pl.DataFrame,
    dfl_oracle_gap_safe_switch_scorer_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle comparison for oracle-gap safe-switch rows."""

    strict_frame = build_dfl_oracle_gap_safe_switch_strict_lp_benchmark_frame(
        dfl_oracle_gap_safe_switch_feature_panel_frame,
        dfl_oracle_gap_safe_switch_scorer_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
    )
    gate = evaluate_dfl_oracle_gap_safe_switch_gate(
        strict_frame,
        min_validation_tenant_anchor_count=config.min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "strategy_kind": ORACLE_GAP_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "diagnostic_signal_passed": gate.metrics.get(
                "diagnostic_signal_passed", False
            ),
            "offline_strategy_challenger_passed": gate.metrics.get(
                "offline_strategy_challenger_passed", False
            ),
            "mean_regret_improvement_ratio_vs_v2_plus": gate.metrics.get(
                "mean_regret_improvement_ratio_vs_v2_plus", 0.0
            ),
            "market_execution_enabled": False,
            "scope": "dfl_oracle_gap_safe_switch_strict_lp_gate_not_full_dfl",
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
        backend="official_global_panel_oracle_gap_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_oracle_gap_safe_switch_rolling_robustness_frame(
    context,
    config: DflOracleGapSafeSwitchAssetConfig,
    dfl_oracle_gap_safe_switch_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling prior-only safe-switch robustness against corrected V2+."""

    robustness_frame = build_dfl_oracle_gap_safe_switch_rolling_robustness_frame(
        dfl_oracle_gap_safe_switch_feature_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=_forecast_model_names(config.source_model_names_csv),
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_prior_anchors_before_window=config.min_prior_anchors_before_window,
        min_prior_safe_win_count=config.min_prior_safe_win_count,
        min_prior_mean_improvement_uah=config.min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=config.min_predicted_improvement_uah,
        max_predicted_tail_risk_probability=(
            config.max_predicted_tail_risk_probability
        ),
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
        allowed_candidate_sources=_csv_values(
            config.allowed_candidate_sources_csv,
            field_name="allowed_candidate_sources_csv",
        ),
        ridge_l2=config.ridge_l2,
    )
    _add_metadata(
        context,
        {
            "rows": robustness_frame.height,
            "rolling_pass_windows": robustness_frame.filter(
                pl.col("rolling_window_passed")
            ).height
            if robustness_frame.height
            else 0,
            "diagnostic_signal_windows": robustness_frame.filter(
                pl.col("diagnostic_window_passed")
            ).height
            if robustness_frame.height
            else 0,
            "market_execution_enabled": False,
            "scope": "dfl_oracle_gap_safe_switch_rolling_robustness_not_full_dfl",
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
        ml_stage="feature_engineering",
        evidence_scope="not_market_execution",
        backend="ua_context_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_ua_calendar_publication_context_frame(
    context,
    dfl_oracle_gap_safe_switch_feature_panel_frame: pl.DataFrame,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only Ukrainian calendar and DAM-publication context."""

    frame = build_dfl_ua_calendar_publication_context_frame(
        dfl_oracle_gap_safe_switch_feature_panel_frame,
        real_data_benchmark_silver_feature_frame,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "context_ready_rows": frame.filter(
                pl.col("calendar_publication_context_blocker") == "context_ready"
            ).height
            if frame.height
            else 0,
            "market_execution_enabled": False,
            "scope": "dfl_ua_calendar_publication_context_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="feature_engineering",
        evidence_scope="not_market_execution",
        backend="ua_context_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_ua_weather_load_context_frame(
    context,
    dfl_oracle_gap_safe_switch_feature_panel_frame: pl.DataFrame,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    tenant_historical_net_load_silver: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only Ukrainian Open-Meteo/weather and tenant load context."""

    frame = build_dfl_ua_weather_load_context_frame(
        dfl_oracle_gap_safe_switch_feature_panel_frame,
        real_data_benchmark_silver_feature_frame,
        tenant_historical_net_load_silver,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "context_ready_rows": frame.filter(
                pl.col("weather_load_context_blocker") == "context_ready"
            ).height
            if frame.height
            else 0,
            "market_execution_enabled": False,
            "scope": "dfl_ua_weather_load_context_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="feature_engineering",
        evidence_scope="not_market_execution",
        backend="ua_context_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_ua_grid_event_context_frame(
    context,
    dfl_oracle_gap_safe_switch_feature_panel_frame: pl.DataFrame,
    grid_event_signal_silver: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only Ukrainian grid-event/outage context from Ukrenergo signals."""

    frame = build_dfl_ua_grid_event_context_frame(
        dfl_oracle_gap_safe_switch_feature_panel_frame,
        grid_event_signal_silver,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "context_ready_rows": frame.filter(
                pl.col("grid_event_context_blocker") == "context_ready"
            ).height
            if frame.height
            else 0,
            "market_execution_enabled": False,
            "scope": "dfl_ua_grid_event_context_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="ua_context_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_ua_context_oracle_gap_feature_panel_frame(
    context,
    dfl_oracle_gap_safe_switch_feature_panel_frame: pl.DataFrame,
    dfl_ua_calendar_publication_context_frame: pl.DataFrame,
    dfl_ua_weather_load_context_frame: pl.DataFrame,
    dfl_ua_grid_event_context_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Merge UA context onto the oracle-gap candidate feature panel."""

    frame = build_dfl_ua_context_oracle_gap_feature_panel_frame(
        dfl_oracle_gap_safe_switch_feature_panel_frame,
        dfl_ua_calendar_publication_context_frame,
        dfl_ua_weather_load_context_frame,
        dfl_ua_grid_event_context_frame,
    )
    selector_features = [
        column for column in frame.columns if column.startswith("selector_feature_")
    ]
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "selector_feature_columns": selector_features,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_ua_context_oracle_gap_feature_panel_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="diagnostics",
        evidence_scope="not_market_execution",
        backend="ua_context_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_ua_context_safe_switch_separability_audit_frame(
    context,
    dfl_ua_context_oracle_gap_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Audit whether missed V2+ candidate wins are separable before scoring."""

    frame = build_dfl_ua_context_safe_switch_separability_audit_frame(
        dfl_ua_context_oracle_gap_feature_panel_frame
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "pre_anchor_distinguishable_rows": frame.filter(
                pl.col("pre_anchor_distinguishable")
            ).height
            if frame.height
            else 0,
            "market_execution_enabled": False,
            "scope": "dfl_ua_context_safe_switch_separability_audit_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="ua_context_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_ua_context_safe_switch_scorer_frame(
    context,
    config: DflUaContextSafeSwitchAssetConfig,
    dfl_ua_context_oracle_gap_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Train sklearn and Torch UA-context safe-switch scorers."""

    frame = build_dfl_ua_context_safe_switch_scorer_frame(
        dfl_ua_context_oracle_gap_feature_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=_forecast_model_names(config.source_model_names_csv),
        scorer_kinds=_csv_values(config.scorer_kinds_csv, field_name="scorer_kinds_csv"),
        min_prior_safe_win_count=config.min_prior_safe_win_count,
        min_prior_mean_improvement_uah=config.min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=config.min_predicted_improvement_uah,
        max_predicted_tail_risk_probability=(
            config.max_predicted_tail_risk_probability
        ),
        allowed_candidate_sources=_csv_values(
            config.allowed_candidate_sources_csv,
            field_name="allowed_candidate_sources_csv",
        ),
        ridge_l2=config.ridge_l2,
        torch_hidden_size=config.torch_hidden_size,
        torch_max_epochs=config.torch_max_epochs,
        use_cuda_if_available=config.use_cuda_if_available,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "scorer_kinds": sorted(frame["scorer_kind"].unique().to_list())
            if frame.height
            else [],
            "candidate_selected_tenant_sources": frame.filter(
                ~pl.col("fallback_to_v2_plus")
            ).height
            if frame.height
            else 0,
            "market_execution_enabled": False,
            "scope": "dfl_ua_context_safe_switch_scorer_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="ua_context_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_ua_context_safe_switch_strict_lp_benchmark_frame(
    context,
    config: DflUaContextSafeSwitchAssetConfig,
    dfl_ua_context_oracle_gap_feature_panel_frame: pl.DataFrame,
    dfl_ua_context_safe_switch_scorer_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle comparison for UA-context safe-switch scorers."""

    strict_frame = build_dfl_ua_context_safe_switch_strict_lp_benchmark_frame(
        dfl_ua_context_oracle_gap_feature_panel_frame,
        dfl_ua_context_safe_switch_scorer_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
    )
    sklearn_gate = evaluate_dfl_ua_context_safe_switch_gate(
        strict_frame,
        selection_role=UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_SKLEARN,
        min_validation_tenant_anchor_count=config.min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    torch_gate = evaluate_dfl_ua_context_safe_switch_gate(
        strict_frame,
        selection_role=UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_TORCH,
        min_validation_tenant_anchor_count=config.min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "strategy_kind": UA_CONTEXT_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
            "sklearn_gate_decision": sklearn_gate.decision,
            "sklearn_gate_description": sklearn_gate.description,
            "torch_gate_decision": torch_gate.decision,
            "torch_gate_description": torch_gate.description,
            "sklearn_diagnostic_signal_passed": sklearn_gate.metrics.get(
                "diagnostic_signal_passed", False
            ),
            "torch_diagnostic_signal_passed": torch_gate.metrics.get(
                "diagnostic_signal_passed", False
            ),
            "market_execution_enabled": False,
            "scope": "dfl_ua_context_safe_switch_strict_lp_gate_not_full_dfl",
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
        backend="ua_context_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_ua_context_safe_switch_rolling_robustness_frame(
    context,
    config: DflUaContextSafeSwitchAssetConfig,
    dfl_ua_context_oracle_gap_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling prior-only robustness for UA-context safe-switch scorers."""

    frame = build_dfl_ua_context_safe_switch_rolling_robustness_frame(
        dfl_ua_context_oracle_gap_feature_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=_forecast_model_names(config.source_model_names_csv),
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_prior_anchors_before_window=config.min_prior_anchors_before_window,
        scorer_kinds=_csv_values(config.scorer_kinds_csv, field_name="scorer_kinds_csv"),
        min_prior_safe_win_count=config.min_prior_safe_win_count,
        min_prior_mean_improvement_uah=config.min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=config.min_predicted_improvement_uah,
        max_predicted_tail_risk_probability=(
            config.max_predicted_tail_risk_probability
        ),
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
        allowed_candidate_sources=_csv_values(
            config.allowed_candidate_sources_csv,
            field_name="allowed_candidate_sources_csv",
        ),
        ridge_l2=config.ridge_l2,
        torch_hidden_size=config.torch_hidden_size,
        torch_max_epochs=config.torch_max_epochs,
        use_cuda_if_available=config.use_cuda_if_available,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "rolling_pass_windows": frame.filter(pl.col("rolling_window_passed")).height
            if frame.height
            else 0,
            "diagnostic_signal_windows": frame.filter(
                pl.col("diagnostic_window_passed")
            ).height
            if frame.height
            else 0,
            "market_execution_enabled": False,
            "scope": "dfl_ua_context_safe_switch_rolling_robustness_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="ua_context_lava_dt",
        market_venue="DAM",
    ),
)
def dfl_ua_context_lava_teacher_frame(
    context,
    config: DflUaContextLavaDtAssetConfig,
    dfl_ua_context_oracle_gap_feature_panel_frame: pl.DataFrame,
    dfl_lava_tail_risk_avoidance_label_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build UA-context LAVA/DT teacher labels over feasible candidates."""

    frame = build_dfl_ua_context_lava_teacher_frame(
        dfl_ua_context_oracle_gap_feature_panel_frame,
        dfl_lava_tail_risk_avoidance_label_frame,
        tail_risk_delta_uah=config.tail_risk_delta_uah,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "training_rows": frame.filter(pl.col("is_training_row")).height
            if frame.height
            else 0,
            "teacher_classes": sorted(
                frame["teacher_schedule_candidate_class"].unique().to_list()
            )
            if frame.height
            else [],
            "target_label_space": "ua_context_schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_ua_context_lava_teacher_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="ua_context_lava_dt",
        market_venue="DAM",
    ),
)
def dfl_ua_context_lava_sequence_training_frame(
    context,
    dfl_ua_context_lava_teacher_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build return-conditioned candidate-index sequence rows."""

    frame = build_dfl_ua_context_lava_sequence_training_frame(
        dfl_ua_context_lava_teacher_frame
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "training_rows": frame.filter(pl.col("is_training_row")).height
            if frame.height
            else 0,
            "target_label_space": "ua_context_schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_ua_context_lava_sequence_training_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="ua_context_lava_dt",
        market_venue="DAM",
    ),
)
def dfl_ua_context_lava_candidate_policy_frame(
    context,
    config: DflUaContextLavaDtAssetConfig,
    dfl_ua_context_lava_sequence_training_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Train a conservative UA-context candidate-index policy."""

    frame = build_dfl_ua_context_lava_candidate_policy_frame(
        dfl_ua_context_lava_sequence_training_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        source_model_names=_forecast_model_names(config.source_model_names_csv),
        min_prior_safe_win_count=config.min_prior_safe_win_count,
        max_prior_tail_loss_count=config.max_prior_tail_loss_count,
        min_prior_precision=config.min_prior_precision,
        min_prior_mean_improvement_uah=config.min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=config.min_predicted_improvement_uah,
        max_predicted_tail_risk_probability=(
            config.max_predicted_tail_risk_probability
        ),
        allowed_candidate_sources=_csv_values(
            config.allowed_candidate_sources_csv,
            field_name="allowed_candidate_sources_csv",
        ),
        hard_blocked_candidate_families=_csv_values(
            config.hard_blocked_candidate_families_csv,
            field_name="hard_blocked_candidate_families_csv",
        ),
        torch_hidden_size=config.torch_hidden_size,
        torch_max_epochs=config.torch_max_epochs,
        use_cuda_if_available=config.use_cuda_if_available,
        random_seed=config.random_seed,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "candidate_selected_tenant_sources": frame.filter(
                ~pl.col("fallback_to_v2_plus")
            ).height
            if frame.height
            else 0,
            "target_label_space": "ua_context_schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_ua_context_lava_candidate_policy_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="ua_context_lava_dt",
        market_venue="DAM",
    ),
)
def dfl_ua_context_lava_strict_lp_benchmark_frame(
    context,
    config: DflUaContextLavaDtAssetConfig,
    dfl_ua_context_lava_sequence_training_frame: pl.DataFrame,
    dfl_ua_context_lava_candidate_policy_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict-score UA-context DT/LAVA candidate policy against V2+."""

    strict_frame = build_dfl_ua_context_lava_strict_lp_benchmark_frame(
        dfl_ua_context_lava_sequence_training_frame,
        dfl_ua_context_lava_candidate_policy_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_ua_context_lava_gate(
        strict_frame,
        min_validation_tenant_anchor_count=config.min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "strategy_kind": UA_CONTEXT_LAVA_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "diagnostic_signal_passed": gate.metrics.get(
                "diagnostic_signal_passed", False
            ),
            "production_promote": False,
            "target_label_space": "ua_context_schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_ua_context_lava_strict_lp_gate_not_full_dfl",
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
        backend="ua_context_lava_dt",
        market_venue="DAM",
    ),
)
def dfl_ua_context_lava_rolling_robustness_frame(
    context,
    config: DflUaContextLavaDtAssetConfig,
    dfl_ua_context_lava_sequence_training_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling prior-only robustness for the UA-context LAVA/DT policy."""

    frame = build_dfl_ua_context_lava_rolling_robustness_frame(
        dfl_ua_context_lava_sequence_training_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        source_model_names=_forecast_model_names(config.source_model_names_csv),
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_prior_anchors_before_window=config.min_prior_anchors_before_window,
        min_prior_safe_win_count=config.min_prior_safe_win_count,
        max_prior_tail_loss_count=config.max_prior_tail_loss_count,
        min_prior_precision=config.min_prior_precision,
        min_prior_mean_improvement_uah=config.min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=config.min_predicted_improvement_uah,
        max_predicted_tail_risk_probability=(
            config.max_predicted_tail_risk_probability
        ),
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
        allowed_candidate_sources=_csv_values(
            config.allowed_candidate_sources_csv,
            field_name="allowed_candidate_sources_csv",
        ),
        hard_blocked_candidate_families=_csv_values(
            config.hard_blocked_candidate_families_csv,
            field_name="hard_blocked_candidate_families_csv",
        ),
        torch_hidden_size=config.torch_hidden_size,
        torch_max_epochs=config.torch_max_epochs,
        use_cuda_if_available=config.use_cuda_if_available,
        random_seed=config.random_seed,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "rolling_pass_windows": frame.filter(pl.col("rolling_window_passed")).height
            if frame.height
            else 0,
            "diagnostic_signal_windows": frame.filter(
                pl.col("diagnostic_window_passed")
            ).height
            if frame.height
            else 0,
            "target_label_space": "ua_context_schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_ua_context_lava_rolling_robustness_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="diagnostics",
        evidence_scope="not_market_execution",
        backend="regret_surrogate_v1",
        market_venue="DAM",
    ),
)
def dfl_v2_plus_learning_limit_audit_frame(
    context,
    config: DflRegretSurrogateV1AssetConfig,
    dfl_ua_context_oracle_gap_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Audit whether the current candidate universe can beat frozen V2+."""

    frame = build_dfl_v2_plus_learning_limit_audit_frame(
        dfl_ua_context_oracle_gap_feature_panel_frame,
        min_oracle_improvement_ratio_vs_v2_plus=(
            config.min_oracle_improvement_ratio_vs_v2_plus
        ),
        tail_risk_delta_uah=config.tail_risk_delta_uah,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "candidate_universe_can_beat_v2_plus_gate": (
                frame.select(pl.col("candidate_universe_can_beat_v2_plus_gate").any()).item()
                if frame.height
                else False
            ),
            "failure_modes": sorted(
                frame["learning_limit_failure_mode"].unique().to_list()
            )
            if frame.height
            else [],
            "recommended_next_branches": sorted(
                frame["recommended_next_branch"].unique().to_list()
            )
            if frame.height
            else [],
            "market_execution_enabled": False,
            "scope": "dfl_v2_plus_learning_limit_audit_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="regret_surrogate_v1",
        market_venue="DAM",
    ),
)
def dfl_expanded_schedule_value_teacher_label_panel_v1_frame(
    context,
    dfl_ua_context_oracle_gap_feature_panel_frame: pl.DataFrame,
    dfl_v2_plus_learning_limit_audit_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build expanded schedule/value teacher labels for regret-surrogate DFL."""

    frame = build_dfl_expanded_schedule_value_teacher_label_panel_v1_frame(
        dfl_ua_context_oracle_gap_feature_panel_frame,
        dfl_v2_plus_learning_limit_audit_frame,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "training_rows": frame.filter(pl.col("is_training_row")).height
            if frame.height
            else 0,
            "target_label_space": "schedule_candidate_value_delta",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_expanded_schedule_value_teacher_labels_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="regret_surrogate_v1",
        market_venue="DAM",
    ),
)
def dfl_regret_surrogate_forecast_correction_v1_frame(
    context,
    config: DflRegretSurrogateV1AssetConfig,
    dfl_expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Fit a conservative regret-surrogate over candidate/value labels."""

    frame = build_dfl_regret_surrogate_forecast_correction_v1_frame(
        dfl_expanded_schedule_value_teacher_label_panel_v1_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        source_model_names=_forecast_model_names(config.source_model_names_csv),
        min_prior_safe_win_count=config.min_prior_safe_win_count,
        min_prior_mean_improvement_uah=config.min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=config.min_predicted_improvement_uah,
        max_predicted_tail_risk_probability=(
            config.max_predicted_tail_risk_probability
        ),
        allowed_candidate_sources=_csv_values(
            config.allowed_candidate_sources_csv,
            field_name="allowed_candidate_sources_csv",
        ),
        use_cuda_if_available=config.use_cuda_if_available,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "candidate_selected_tenant_sources": frame.filter(
                ~pl.col("fallback_to_v2_plus")
            ).height
            if frame.height
            else 0,
            "target_label_space": "schedule_candidate_value_delta",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_regret_surrogate_forecast_correction_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="regret_surrogate_v1",
        market_venue="DAM",
    ),
)
def dfl_regret_surrogate_candidate_value_v1_frame(
    context,
    dfl_expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
    dfl_regret_surrogate_forecast_correction_v1_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Resolve final candidate choices for regret-surrogate DFL v1."""

    frame = build_dfl_regret_surrogate_candidate_value_v1_frame(
        dfl_expanded_schedule_value_teacher_label_panel_v1_frame,
        dfl_regret_surrogate_forecast_correction_v1_frame,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "selected_final_candidate_count": int(
                frame["selected_final_candidate_count"].sum()
            )
            if frame.height
            else 0,
            "fallback_final_anchor_count": int(
                frame["fallback_final_anchor_count"].sum()
            )
            if frame.height
            else 0,
            "target_label_space": "schedule_candidate_value_delta",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_regret_surrogate_candidate_value_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="regret_surrogate_v1",
        market_venue="DAM",
    ),
)
def dfl_regret_surrogate_strict_lp_benchmark_frame(
    context,
    config: DflRegretSurrogateV1AssetConfig,
    dfl_expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
    dfl_regret_surrogate_candidate_value_v1_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict-score regret-surrogate DFL v1 against frozen V2+."""

    strict_frame = build_dfl_regret_surrogate_strict_lp_benchmark_frame(
        dfl_expanded_schedule_value_teacher_label_panel_v1_frame,
        dfl_regret_surrogate_candidate_value_v1_frame,
        generated_at=_latest_generated_at(
            dfl_expanded_schedule_value_teacher_label_panel_v1_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_regret_surrogate_gate(
        strict_frame,
        min_validation_tenant_anchor_count=config.min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "strategy_kind": REGRET_SURROGATE_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "diagnostic_signal_passed": gate.metrics.get(
                "diagnostic_signal_passed", False
            ),
            "production_promote": False,
            "target_label_space": "schedule_candidate_value_delta",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_regret_surrogate_strict_lp_gate_not_full_dfl",
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
        backend="regret_surrogate_v1",
        market_venue="DAM",
    ),
)
def dfl_regret_surrogate_rolling_robustness_frame(
    context,
    config: DflRegretSurrogateV1AssetConfig,
    dfl_expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling prior-only robustness for regret-surrogate DFL v1."""

    frame = build_dfl_regret_surrogate_rolling_robustness_frame(
        dfl_expanded_schedule_value_teacher_label_panel_v1_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        source_model_names=_forecast_model_names(config.source_model_names_csv),
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_prior_anchors_before_window=config.min_prior_anchors_before_window,
        min_prior_safe_win_count=config.min_prior_safe_win_count,
        min_prior_mean_improvement_uah=config.min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=config.min_predicted_improvement_uah,
        max_predicted_tail_risk_probability=(
            config.max_predicted_tail_risk_probability
        ),
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        allowed_candidate_sources=_csv_values(
            config.allowed_candidate_sources_csv,
            field_name="allowed_candidate_sources_csv",
        ),
        use_cuda_if_available=config.use_cuda_if_available,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "rolling_pass_windows": frame.filter(pl.col("rolling_window_passed")).height
            if frame.height
            else 0,
            "diagnostic_signal_windows": frame.filter(
                pl.col("diagnostic_window_passed")
            ).height
            if frame.height
            else 0,
            "target_label_space": "schedule_candidate_value_delta",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_regret_surrogate_rolling_robustness_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="diagnostics",
        evidence_scope="not_market_execution",
        backend="regret_surrogate_context_v2",
        market_venue="DAM",
    ),
)
def dfl_regret_surrogate_safe_switch_context_audit_frame(
    context,
    config: DflRegretSurrogateV1AssetConfig,
    dfl_expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Audit whether rare safe-switch opportunities have prior context support."""

    frame = build_dfl_regret_surrogate_safe_switch_context_audit_frame(
        dfl_expanded_schedule_value_teacher_label_panel_v1_frame,
        material_switch_delta_uah=config.material_switch_delta_uah,
        high_v2_regret_uah=config.high_v2_regret_uah,
        high_forecast_spread_uah_mwh=config.high_forecast_spread_uah_mwh,
        min_material_schedule_distance=config.min_material_schedule_distance,
        min_context_prior_safe_win_count=config.min_context_prior_safe_win_count,
        min_context_prior_mean_improvement_uah=(
            config.min_context_prior_mean_improvement_uah
        ),
        max_context_tail_risk_probability=config.max_context_tail_risk_probability,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "material_safe_switch_rows": frame.filter(
                pl.col("material_safe_switch_available")
            ).height
            if frame.height
            else 0,
            "context_failure_modes": sorted(
                frame["safe_switch_context_failure_mode"].unique().to_list()
            )
            if frame.height
            else [],
            "market_execution_enabled": False,
            "scope": "dfl_regret_surrogate_safe_switch_context_audit_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="regret_surrogate_context_v2",
        market_venue="DAM",
    ),
)
def dfl_regret_surrogate_teacher_label_panel_v2_frame(
    context,
    config: DflRegretSurrogateV1AssetConfig,
    dfl_expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
    dfl_regret_surrogate_safe_switch_context_audit_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Add prior-supported safe-switch context labels to Regret-Surrogate teachers."""

    frame = build_dfl_regret_surrogate_teacher_label_panel_v2_frame(
        dfl_expanded_schedule_value_teacher_label_panel_v1_frame,
        dfl_regret_surrogate_safe_switch_context_audit_frame,
        material_switch_delta_uah=config.material_switch_delta_uah,
        high_v2_regret_uah=config.high_v2_regret_uah,
        high_forecast_spread_uah_mwh=config.high_forecast_spread_uah_mwh,
        min_material_schedule_distance=config.min_material_schedule_distance,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "material_safe_switch_rows": frame.filter(
                pl.col("label_context_material_safe_switch")
            ).height
            if frame.height
            else 0,
            "target_label_space": "schedule_candidate_contextual_value_delta",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_regret_surrogate_teacher_context_v2_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="regret_surrogate_context_v2",
        market_venue="DAM",
    ),
)
def dfl_regret_surrogate_contextual_candidate_value_v2_frame(
    context,
    config: DflRegretSurrogateV1AssetConfig,
    dfl_regret_surrogate_teacher_label_panel_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Select candidates only when the safe-switch context has prior support."""

    frame = build_dfl_regret_surrogate_contextual_candidate_value_v2_frame(
        dfl_regret_surrogate_teacher_label_panel_v2_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        source_model_names=_forecast_model_names(config.source_model_names_csv),
        min_context_prior_support_count=config.min_context_prior_support_count,
        min_context_prior_safe_win_count=config.min_context_prior_safe_win_count,
        min_context_prior_mean_improvement_uah=(
            config.min_context_prior_mean_improvement_uah
        ),
        min_predicted_improvement_uah=config.min_predicted_improvement_uah,
        max_context_tail_risk_probability=config.max_context_tail_risk_probability,
        allowed_candidate_sources=_csv_values(
            config.allowed_candidate_sources_csv,
            field_name="allowed_candidate_sources_csv",
        ),
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "selected_final_candidate_count": int(
                frame["selected_final_candidate_count"].sum()
            )
            if frame.height
            else 0,
            "fallback_final_anchor_count": int(
                frame["fallback_final_anchor_count"].sum()
            )
            if frame.height
            else 0,
            "target_label_space": "schedule_candidate_contextual_value_delta",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": (
                "dfl_regret_surrogate_contextual_candidate_value_v2_not_full_dfl"
            ),
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="regret_surrogate_context_v2",
        market_venue="DAM",
    ),
)
def dfl_regret_surrogate_contextual_strict_lp_benchmark_frame(
    context,
    config: DflRegretSurrogateV1AssetConfig,
    dfl_regret_surrogate_teacher_label_panel_v2_frame: pl.DataFrame,
    dfl_regret_surrogate_contextual_candidate_value_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict-score contextual Regret-Surrogate V2 against frozen V2+."""

    strict_frame = build_dfl_regret_surrogate_contextual_strict_lp_benchmark_frame(
        dfl_regret_surrogate_teacher_label_panel_v2_frame,
        dfl_regret_surrogate_contextual_candidate_value_v2_frame,
        generated_at=_latest_generated_at(dfl_regret_surrogate_teacher_label_panel_v2_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_regret_surrogate_gate(
        strict_frame.with_columns(
            pl.when(
                pl.col("selection_role") == REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE
            )
            .then(pl.lit("regret_surrogate_candidate_value"))
            .otherwise(pl.col("selection_role"))
            .alias("selection_role")
        ),
        min_validation_tenant_anchor_count=config.min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "strategy_kind": REGRET_SURROGATE_CONTEXTUAL_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "diagnostic_signal_passed": gate.metrics.get(
                "diagnostic_signal_passed", False
            ),
            "production_promote": False,
            "target_label_space": "schedule_candidate_contextual_value_delta",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_regret_surrogate_contextual_strict_lp_gate_not_full_dfl",
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
        backend="regret_surrogate_context_v2",
        market_venue="DAM",
    ),
)
def dfl_regret_surrogate_contextual_rolling_robustness_frame(
    context,
    config: DflRegretSurrogateV1AssetConfig,
    dfl_expanded_schedule_value_teacher_label_panel_v1_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling prior-only robustness for contextual Regret-Surrogate V2."""

    frame = build_dfl_regret_surrogate_contextual_rolling_robustness_frame(
        dfl_expanded_schedule_value_teacher_label_panel_v1_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        source_model_names=_forecast_model_names(config.source_model_names_csv),
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_prior_anchors_before_window=config.min_prior_anchors_before_window,
        material_switch_delta_uah=config.material_switch_delta_uah,
        high_v2_regret_uah=config.high_v2_regret_uah,
        high_forecast_spread_uah_mwh=config.high_forecast_spread_uah_mwh,
        min_material_schedule_distance=config.min_material_schedule_distance,
        min_context_prior_support_count=config.min_context_prior_support_count,
        min_context_prior_safe_win_count=config.min_context_prior_safe_win_count,
        min_context_prior_mean_improvement_uah=(
            config.min_context_prior_mean_improvement_uah
        ),
        min_predicted_improvement_uah=config.min_predicted_improvement_uah,
        max_context_tail_risk_probability=config.max_context_tail_risk_probability,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        allowed_candidate_sources=_csv_values(
            config.allowed_candidate_sources_csv,
            field_name="allowed_candidate_sources_csv",
        ),
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "rolling_pass_windows": frame.filter(pl.col("rolling_window_passed")).height
            if frame.height
            else 0,
            "diagnostic_signal_windows": frame.filter(
                pl.col("diagnostic_window_passed")
            ).height
            if frame.height
            else 0,
            "target_label_space": "schedule_candidate_contextual_value_delta",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_regret_surrogate_contextual_rolling_robustness_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_poland_lag24_experimental",
        market_venue="DAM",
    ),
)
def official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame(
    context,
    config: DflPolandLag24ExperimentalRollingStrictAssetConfig,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    entsoe_poland_lagged_feature_candidate_frame: pl.DataFrame,
    official_forecast_exogenous_feature_route_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling strict rows for Poland-lag24 experimental NBEATSx/TFT candidates."""

    source_model_names = _csv_values(
        config.enabled_forecast_model_names_csv,
        field_name="enabled_forecast_model_names_csv",
    )
    generated_at_iso = config.resume_generated_at_iso or config.generated_at_iso
    strict_frame = (
        build_official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame(
            real_data_benchmark_silver_feature_frame,
            tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
            entsoe_poland_lagged_feature_candidate_frame=(
                entsoe_poland_lagged_feature_candidate_frame
            ),
            market_coupling_feature_route_frame=(
                official_forecast_exogenous_feature_route_frame
            ),
            enabled_forecast_model_names=source_model_names,
            max_eval_windows=config.max_eval_windows,
            horizon_hours=config.horizon_hours,
            nbeatsx_max_steps=config.nbeatsx_max_steps,
            nbeatsx_random_seed=config.nbeatsx_random_seed,
            tft_max_epochs=config.tft_max_epochs,
            tft_max_steps=config.tft_max_steps,
            tft_batch_size=config.tft_batch_size,
            tft_learning_rate=config.tft_learning_rate,
            tft_hidden_size=config.tft_hidden_size,
            tft_hidden_continuous_size=config.tft_hidden_continuous_size,
            tft_accelerator=config.tft_accelerator,
            tft_devices=config.tft_devices,
            anchor_batch_order=config.anchor_batch_order,
            anchor_batch_start_index=config.anchor_batch_start_index,
            anchor_batch_size=config.anchor_batch_size,
            generated_at=_optional_datetime_config(generated_at_iso),
        )
    )
    store = get_strategy_evaluation_store()
    store.upsert_evaluation_frame(strict_frame)
    generated_at = _optional_datetime_config(generated_at_iso)
    if config.merge_persisted_batches and generated_at is not None:
        persisted_frame = store.strategy_kind_frame_for_generated_at(
            strategy_kind=POLAND_LAG24_EXPERIMENTAL_ROLLING_STRATEGY_KIND,
            generated_at=generated_at,
        )
        if persisted_frame.height:
            strict_frame = persisted_frame
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "anchor_count": strict_frame.select("anchor_timestamp").n_unique()
            if strict_frame.height
            else 0,
            "anchor_batch_start_index": config.anchor_batch_start_index,
            "anchor_batch_size": config.anchor_batch_size,
            "merge_persisted_batches": config.merge_persisted_batches,
            "source_model_names": source_model_names,
            "strategy_kind": POLAND_LAG24_EXPERIMENTAL_ROLLING_STRATEGY_KIND,
            "scope": (
                "official_global_panel_poland_lag24_experimental_rolling_"
                "strict_lp_not_full_dfl"
            ),
            "market_execution_enabled": False,
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
        ml_stage="calibration",
        evidence_scope="not_market_execution",
        backend="official_global_panel_poland_lag24_experimental",
        market_venue="DAM",
    ),
)
def official_global_panel_poland_lag24_experimental_nbeatsx_horizon_calibration_frame(
    context,
    config: DflPolandLag24ExperimentalCalibrationAssetConfig,
    official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Prior-only horizon calibration for Poland-enhanced NBEATSx rows."""

    calibration_frame = build_official_global_panel_nbeatsx_horizon_calibration_frame(
        official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame,
        min_prior_anchors=config.min_prior_anchors,
        rolling_calibration_window_anchors=(
            config.rolling_calibration_window_anchors
        ),
        source_model_name=POLAND_LAG24_EXPERIMENTAL_NBEATSX_MODEL_NAME,
        corrected_model_name=POLAND_LAG24_EXPERIMENTAL_NBEATSX_CALIBRATED_MODEL_NAME,
    )
    _add_metadata(
        context,
        {
            "rows": calibration_frame.height,
            "source_model_name": POLAND_LAG24_EXPERIMENTAL_NBEATSX_MODEL_NAME,
            "corrected_model_name": (
                POLAND_LAG24_EXPERIMENTAL_NBEATSX_CALIBRATED_MODEL_NAME
            ),
            "scope": (
                "official_global_panel_poland_lag24_nbeatsx_prior_only_"
                "horizon_calibration_not_full_dfl"
            ),
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return calibration_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="calibration",
        evidence_scope="not_market_execution",
        backend="official_global_panel_poland_lag24_experimental",
        market_venue="DAM",
    ),
)
def official_global_panel_poland_lag24_experimental_tft_horizon_quantile_calibration_frame(
    context,
    config: DflPolandLag24ExperimentalCalibrationAssetConfig,
    official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Prior-only p50 horizon/quantile calibration for Poland-enhanced TFT rows."""

    calibration_frame = (
        build_official_global_panel_tft_horizon_quantile_calibration_frame(
            official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame,
            min_prior_anchors=config.min_prior_anchors,
            rolling_calibration_window_anchors=(
                config.rolling_calibration_window_anchors
            ),
            source_model_names=(POLAND_LAG24_EXPERIMENTAL_TFT_MODEL_NAME,),
            corrected_model_names=(
                POLAND_LAG24_EXPERIMENTAL_TFT_CALIBRATED_MODEL_NAME,
            ),
            source_quantiles=("p50",),
        )
    )
    _add_metadata(
        context,
        {
            "rows": calibration_frame.height,
            "source_model_name": POLAND_LAG24_EXPERIMENTAL_TFT_MODEL_NAME,
            "corrected_model_name": (
                POLAND_LAG24_EXPERIMENTAL_TFT_CALIBRATED_MODEL_NAME
            ),
            "source_quantile": "p50",
            "scope": (
                "official_global_panel_poland_lag24_tft_prior_only_"
                "horizon_quantile_calibration_not_full_dfl"
            ),
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return calibration_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_poland_lag24_experimental",
        market_venue="DAM",
    ),
)
def official_global_panel_poland_lag24_experimental_horizon_calibrated_strict_lp_benchmark_frame(
    context,
    official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    official_global_panel_poland_lag24_experimental_nbeatsx_horizon_calibration_frame: (
        pl.DataFrame
    ),
    official_global_panel_poland_lag24_experimental_tft_horizon_quantile_calibration_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict LP/oracle rows for raw and calibrated Poland-enhanced forecasts."""

    strict_frame = (
        build_official_global_panel_poland_lag24_experimental_horizon_calibrated_strict_lp_benchmark_frame(
            official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame,
            official_global_panel_poland_lag24_experimental_nbeatsx_horizon_calibration_frame,
            official_global_panel_poland_lag24_experimental_tft_horizon_quantile_calibration_frame,
        )
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "anchor_count": strict_frame.select("anchor_timestamp").n_unique()
            if strict_frame.height
            else 0,
            "source_model_names": POLAND_LAG24_EXPERIMENTAL_CALIBRATED_SOURCE_MODEL_NAMES,
            "strategy_kind": POLAND_LAG24_EXPERIMENTAL_CALIBRATION_STRATEGY_KIND,
            "scope": (
                "official_global_panel_poland_lag24_horizon_calibrated_"
                "strict_lp_not_full_dfl"
            ),
            "market_execution_enabled": False,
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
        backend="official_global_panel_poland_lag24_calibrated",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_calibrated_schedule_candidate_library_frame(
    context,
    config: DflOfficialGlobalPanelScheduleCandidateLibraryAssetConfig,
    official_global_panel_poland_lag24_experimental_horizon_calibrated_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Schedule library for raw and calibrated Poland-enhanced forecast names."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    library_frame = build_dfl_schedule_candidate_library_from_strict_benchmark_frame(
        official_global_panel_poland_lag24_experimental_horizon_calibrated_strict_lp_benchmark_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
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
            "source_model_names": source_model_names,
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
            if library_frame.height
            else 0,
            "scope": "dfl_poland_lag24_calibrated_schedule_library_not_full_dfl",
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
        backend="official_global_panel_poland_lag24_calibrated",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_calibrated_schedule_candidate_library_v2_frame(
    context,
    config: DflStrictChallengerAssetConfig,
    dfl_poland_lag24_calibrated_schedule_candidate_library_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Blend/residual candidates for calibrated Poland-enhanced schedules."""

    library_frame = build_schedule_candidate_library_v2_frame(
        dfl_poland_lag24_calibrated_schedule_candidate_library_frame,
        blend_weights=_float_csv_values(
            config.blend_weights_csv,
            field_name="blend_weights_csv",
        ),
        residual_min_prior_anchors=config.residual_min_prior_anchors,
    )
    _add_metadata(
        context,
        {
            "rows": library_frame.height,
            "scope": "dfl_poland_lag24_calibrated_schedule_library_v2_not_full_dfl",
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
        backend="official_global_panel_poland_lag24_calibrated",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame(
    context,
    config: DflScheduleCandidateLibraryV2PlusAssetConfig,
    dfl_poland_lag24_calibrated_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """V2+ candidate expansion for calibrated Poland-enhanced schedules."""

    library_frame = build_dfl_schedule_candidate_library_v2_plus_frame(
        dfl_poland_lag24_calibrated_schedule_candidate_library_v2_frame,
        rank_perturbation_delta_uah_mwh=config.rank_perturbation_delta_uah_mwh,
        robust_spread_scales=_float_csv_values(
            config.robust_spread_scales_csv,
            field_name="robust_spread_scales_csv",
        ),
        strict_neighborhood_shift_hours=_int_csv_values(
            config.strict_neighborhood_shift_hours_csv,
            field_name="strict_neighborhood_shift_hours_csv",
        ),
        block_reconcile_hours=_int_csv_values(
            config.block_reconcile_hours_csv,
            field_name="block_reconcile_hours_csv",
        ),
        terminal_target_shift_uah_mwh=config.terminal_target_shift_uah_mwh,
        generated_at=_latest_generated_at(
            dfl_poland_lag24_calibrated_schedule_candidate_library_v2_frame
        ),
    )
    _add_metadata(
        context,
        {
            "rows": library_frame.height,
            "scope": (
                "dfl_poland_lag24_calibrated_schedule_library_v2_plus_not_full_dfl"
            ),
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
        backend="official_global_panel_poland_lag24_calibrated",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_calibrated_schedule_value_learner_v2_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV2AssetConfig,
    dfl_poland_lag24_calibrated_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only schedule/value learner over calibrated Poland-enhanced schedules."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    learner_frame = build_dfl_schedule_value_learner_v2_frame(
        dfl_poland_lag24_calibrated_schedule_candidate_library_v2_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
    )
    _add_metadata(
        context,
        {
            "rows": learner_frame.height,
            "source_model_names": source_model_names,
            "scope": "dfl_poland_lag24_calibrated_schedule_value_v2_not_full_dfl",
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
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_poland_lag24_calibrated",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV2PlusAssetConfig,
    dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_poland_lag24_calibrated_schedule_value_learner_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """V2+ selector for calibrated Poland-enhanced source schedules."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    learner_frame = build_dfl_schedule_value_learner_v2_plus_frame(
        dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame,
        dfl_poland_lag24_calibrated_schedule_value_learner_v2_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_v2=(
            config.min_prior_mean_improvement_ratio_vs_v2
        ),
    )
    _add_metadata(
        context,
        {
            "rows": learner_frame.height,
            "source_model_names": source_model_names,
            "fallback_rows": learner_frame.filter(pl.col("fallback_to_v2")).height
            if learner_frame.height
            else 0,
            "scope": "dfl_poland_lag24_calibrated_schedule_value_v2_plus_not_full_dfl",
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
        backend="official_global_panel_poland_lag24_calibrated",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV2PlusAssetConfig,
    dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_poland_lag24_calibrated_schedule_value_learner_v2_frame: pl.DataFrame,
    dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle schedule-value rows for calibrated Poland-enhanced sources."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame,
        dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_frame,
        dfl_poland_lag24_calibrated_schedule_value_learner_v2_frame,
        generated_at=_latest_generated_at(
            dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_schedule_value_learner_v2_plus_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "source_model_names": source_model_names,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "production_gate_passed": gate.metrics.get("production_gate_passed", False),
            "market_execution_enabled": False,
            "scope": (
                "dfl_poland_lag24_calibrated_schedule_value_v2_plus_"
                "strict_gate_not_full_dfl"
            ),
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
        backend="official_global_panel_poland_lag24_calibrated",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_calibrated_vs_v2_plus_comparison_frame(
    context,
    dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Compare calibrated Poland-enhanced schedule/value rows with frozen V2+."""

    comparison_rows = _summarize_poland_lag24_vs_v2_plus(
        dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
    )
    _add_metadata(
        context,
        {
            "rows": comparison_rows.height,
            "scope": (
                "dfl_poland_lag24_calibrated_vs_frozen_ukrainian_v2_plus_"
                "comparison_not_full_dfl"
            ),
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return comparison_rows


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_poland_lag24_calibrated",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_prior_tail_risk_veto_frame(
    context,
    dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Prior-only veto for using Poland-enhanced TFT schedules over frozen V2+."""

    baseline_model_name = (
        f"dfl_schedule_value_learner_v2_plus_{FROZEN_V2_PLUS_BASELINE_MODEL_NAME}"
    )
    challenger_model_name = (
        "dfl_schedule_value_learner_v2_plus_"
        f"{POLAND_LAG24_EXPERIMENTAL_TFT_CALIBRATED_MODEL_NAME}"
    )
    audit_frame = build_poland_lag24_tail_risk_audit_frame(
        baseline_frame=(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
        challenger_frame=(
            dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
        baseline_model_name=baseline_model_name,
        challenger_model_name=challenger_model_name,
    )
    veto_frame = build_poland_lag24_prior_veto_frame(audit_frame)
    packet = build_poland_lag24_prior_veto_packet(
        run_slug="dagster_poland_lag24_prior_tail_risk_veto",
        veto_frame=veto_frame,
        dagster_run_id=context.run_id if context is not None else None,
        materialization_command=(
            "dagster asset materialize --select "
            "dfl_poland_lag24_prior_tail_risk_veto_frame"
        ),
    )
    summary = packet["summary"]
    gate = packet["gate"]
    anchor_count = int(summary["anchor_count"])
    coverage_status = (
        "full_365_ready"
        if anchor_count >= 365
        else "insufficient_for_365_anchor_claim"
    )
    rolling_coverage_status = (
        "rolling_ready"
        if anchor_count >= 72
        else "insufficient_for_4x18_rolling_windows"
    )
    veto_frame = veto_frame.with_columns(
        pl.lit(coverage_status).alias("coverage_status"),
        pl.lit(rolling_coverage_status).alias("rolling_coverage_status"),
        pl.lit(False).alias("market_execution_enabled"),
        pl.lit(True).alias("not_market_execution"),
    )
    _add_metadata(
        context,
        {
            "rows": veto_frame.height,
            "tenant_count": summary["tenant_count"],
            "anchor_count": anchor_count,
            "selected_challenger_rows": summary["selected_challenger_rows"],
            "selected_mean_regret_uah": summary["selected_mean_regret_uah"],
            "baseline_mean_regret_uah": summary["baseline_mean_regret_uah"],
            "mean_regret_improvement_ratio_vs_baseline": (
                summary["mean_regret_improvement_ratio_vs_baseline"]
            ),
            "coverage_status": coverage_status,
            "rolling_coverage_status": rolling_coverage_status,
            "gate_blocker": gate["blocker"],
            "promotes_over_frozen_v2_plus": gate["promotes_over_frozen_v2_plus"],
            "market_execution_enabled": False,
            "scope": "dfl_poland_lag24_prior_tail_risk_veto_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return veto_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="official_global_panel_poland_lag24_experimental",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_feature_consumption_audit_frame(
    context,
    entsoe_poland_lagged_feature_candidate_frame: pl.DataFrame,
    official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Audit Poland feature routing before treating the lane as ML signal."""

    audit_frame = build_poland_lag24_feature_consumption_audit_frame(
        entsoe_poland_lagged_feature_candidate_frame,
        strict_benchmark_frame=(
            official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame
        ),
    )
    _add_metadata(
        context,
        {
            "rows": audit_frame.height,
            "passing_feature_count": audit_frame.filter(
                pl.col("consumption_status") == "passes_training_consumption_audit"
            ).height,
            "feature_count": audit_frame.select("feature_column").n_unique(),
            "scope": "poland_lag24_feature_consumption_audit_not_market_execution",
            "market_execution_enabled": False,
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
        backend="official_global_panel_poland_lag24_calibrated",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_candidate_value_label_panel_frame(
    context,
    dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    entsoe_poland_lagged_feature_candidate_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Attach prior-safe Poland context to schedule-level value labels."""

    label_frame = build_poland_lag24_candidate_value_label_panel_frame(
        dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame,
        entsoe_poland_lagged_feature_candidate_frame,
    )
    _add_metadata(
        context,
        {
            "rows": label_frame.height,
            "selector_feature_count": len(
                [column for column in label_frame.columns if column.startswith("selector_feature_")]
            ),
            "source_model_count": label_frame.select("source_model_name").n_unique()
            if label_frame.height
            else 0,
            "scope": "dfl_poland_lag24_candidate_value_label_panel_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return label_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_poland_lag24_calibrated",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_candidate_value_ranker_frame(
    context,
    config: DflPolandLag24CandidateValueRankerAssetConfig,
    dfl_poland_lag24_candidate_value_label_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Train a small prior-only tabular candidate-value ranker before DT/LAVA."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    ranker_frame = build_poland_lag24_candidate_value_ranker_frame(
        dfl_poland_lag24_candidate_value_label_panel_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        min_prior_mean_improvement_ratio_vs_frozen_proxy=(
            config.min_prior_mean_improvement_ratio_vs_frozen_proxy
        ),
        ridge_l2=config.ridge_l2,
    )
    _add_metadata(
        context,
        {
            "rows": ranker_frame.height,
            "source_model_names": source_model_names,
            "fallback_rows": ranker_frame.filter(
                pl.col("fallback_to_frozen_v2_plus")
            ).height
            if ranker_frame.height
            else 0,
            "scope": "dfl_poland_lag24_candidate_value_ranker_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_poland_lag24_calibrated",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame(
    context,
    dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_poland_lag24_candidate_value_ranker_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict LP/oracle benchmark for the Poland candidate-value ranker."""

    strict_frame = build_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame(
        dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame,
        dfl_poland_lag24_candidate_value_ranker_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "ranker_rows": strict_frame.filter(
                pl.col("selection_role") == "poland_lag24_candidate_value_ranker_v1"
            ).height
            if strict_frame.height
            else 0,
            "market_execution_enabled": False,
            "scope": "dfl_poland_lag24_candidate_value_ranker_strict_gate_not_full_dfl",
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
        backend="official_global_panel_lava_schedule_neighbor",
        market_venue="DAM",
    ),
)
def dfl_v2_plus_schedule_neighbor_teacher_label_frame(
    context,
    config: DflLavaScheduleNeighborBridgeAssetConfig,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_poland_lag24_prior_tail_risk_veto_frame: pl.DataFrame,
    dfl_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Classify V2+/Poland evidence into prior-safe teacher labels for LAVA."""

    poland_source_model_names = _forecast_model_names(
        config.poland_source_model_names_csv
    )
    label_frame = build_dfl_v2_plus_schedule_neighbor_teacher_label_frame(
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        dfl_poland_lag24_prior_tail_risk_veto_frame,
        dfl_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        poland_source_model_names=poland_source_model_names,
        tail_risk_delta_uah=config.tail_risk_delta_uah,
    )
    _add_metadata(
        context,
        {
            "rows": label_frame.height,
            "teacher_class_count": label_frame.select("teacher_class").n_unique()
            if label_frame.height
            else 0,
            "teacher_classes": sorted(label_frame["teacher_class"].unique().to_list())
            if label_frame.height
            else [],
            "poland_source_model_names": poland_source_model_names,
            "scope": "dfl_v2_plus_schedule_neighbor_teacher_labels_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return label_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="official_global_panel_lava_schedule_neighbor",
        market_venue="DAM",
    ),
)
def dfl_lava_schedule_neighbor_candidate_frame(
    context,
    config: DflLavaScheduleNeighborBridgeAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Build feasible V2+/strict/Poland schedule-neighbor candidates for LAVA."""

    poland_source_model_names = _forecast_model_names(
        config.poland_source_model_names_csv
    )
    candidate_frame = build_dfl_lava_schedule_neighbor_candidate_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        poland_source_model_names=poland_source_model_names,
        include_oracle_train_diagnostics=config.include_oracle_train_diagnostics,
    )
    _add_metadata(
        context,
        {
            "rows": candidate_frame.height,
            "candidate_source_count": candidate_frame.select(
                "candidate_source"
            ).n_unique()
            if candidate_frame.height
            else 0,
            "candidate_sources": sorted(
                candidate_frame["candidate_source"].unique().to_list()
            )
            if candidate_frame.height
            else [],
            "oracle_train_diagnostic_rows": candidate_frame.filter(
                pl.col("candidate_source") == "oracle_neighbor_train_diagnostic"
            ).height
            if candidate_frame.height
            else 0,
            "scope": "dfl_lava_schedule_neighbor_candidates_not_full_dfl",
            "market_execution_enabled": False,
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
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_lava_schedule_neighbor",
        market_venue="DAM",
    ),
)
def dfl_lava_candidate_value_scorer_frame(
    context,
    config: DflLavaScheduleNeighborBridgeAssetConfig,
    dfl_lava_schedule_neighbor_candidate_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Train a conservative prior-only schedule-neighbor scorer before DT."""

    scorer_frame = build_dfl_lava_candidate_value_scorer_frame(
        dfl_lava_schedule_neighbor_candidate_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        min_prior_mean_improvement_ratio_vs_v2_plus=(
            config.min_prior_mean_improvement_ratio_vs_v2_plus
        ),
        ridge_l2=config.ridge_l2,
    )
    _add_metadata(
        context,
        {
            "rows": scorer_frame.height,
            "fallback_rows": scorer_frame.filter(pl.col("fallback_to_v2_plus")).height
            if scorer_frame.height
            else 0,
            "selector_gate_blockers": sorted(
                scorer_frame["selector_gate_blocker"].unique().to_list()
            )
            if scorer_frame.height
            else [],
            "scope": "dfl_lava_candidate_value_scorer_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return scorer_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_lava_schedule_neighbor",
        market_venue="DAM",
    ),
)
def dfl_lava_candidate_value_strict_lp_benchmark_frame(
    context,
    config: DflLavaScheduleNeighborBridgeAssetConfig,
    dfl_lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    dfl_lava_candidate_value_scorer_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict-score LAVA schedule-neighbor scorer against frozen V2+."""

    strict_frame = build_dfl_lava_candidate_value_strict_lp_benchmark_frame(
        dfl_lava_schedule_neighbor_candidate_frame,
        dfl_lava_candidate_value_scorer_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_lava_candidate_value_gate(
        strict_frame,
        min_validation_tenant_anchor_count=config.min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "strategy_kind": DFL_LAVA_CANDIDATE_VALUE_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_replacement_passed": gate.metrics.get(
                "offline_strategy_replacement_passed",
                False,
            ),
            "production_promote": False,
            "market_execution_enabled": False,
            "scope": "dfl_lava_candidate_value_strict_lp_gate_not_full_dfl",
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
        backend="official_global_panel_lava_tail_risk_target",
        market_venue="DAM",
    ),
)
def dfl_lava_tail_risk_diagnostic_frame(
    context,
    config: DflLavaTailRiskTargetAssetConfig,
    dfl_lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    dfl_lava_candidate_value_strict_lp_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Use the failed LAVA bridge as diagnostic data for tail-risk targets."""

    diagnostic_frame = build_dfl_lava_tail_risk_diagnostic_frame(
        dfl_lava_schedule_neighbor_candidate_frame,
        dfl_lava_candidate_value_strict_lp_benchmark_frame,
        tail_risk_delta_uah=config.tail_risk_delta_uah,
    )
    _add_metadata(
        context,
        {
            "rows": diagnostic_frame.height,
            "diagnostic_classes": sorted(
                diagnostic_frame["tail_risk_diagnostic_class"].unique().to_list()
            )
            if diagnostic_frame.height
            else [],
            "target_recommendations": sorted(
                diagnostic_frame["target_recommendation"].unique().to_list()
            )
            if diagnostic_frame.height
            else [],
            "scope": "dfl_lava_tail_risk_diagnostic_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return diagnostic_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="official_global_panel_lava_tail_risk_target",
        market_venue="DAM",
    ),
)
def dfl_lava_tail_risk_aware_target_frame(
    context,
    config: DflLavaTailRiskTargetAssetConfig,
    dfl_lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    dfl_lava_tail_risk_diagnostic_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build candidate-index DT/LAVA targets that avoid tail-risk perturbations."""

    target_frame = build_dfl_lava_tail_risk_aware_target_frame(
        dfl_lava_schedule_neighbor_candidate_frame,
        dfl_lava_tail_risk_diagnostic_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        min_prior_safe_win_count=config.min_prior_safe_win_count,
        max_prior_tail_loss_count=config.max_prior_tail_loss_count,
        hard_blocked_candidate_families=_csv_values(
            config.hard_blocked_candidate_families_csv,
            field_name="hard_blocked_candidate_families_csv",
        ),
    )
    _add_metadata(
        context,
        {
            "rows": target_frame.height,
            "fallback_rows": target_frame.filter(pl.col("fallback_to_v2_plus")).height
            if target_frame.height
            else 0,
            "selector_gate_blockers": sorted(
                target_frame["selector_gate_blocker"].unique().to_list()
            )
            if target_frame.height
            else [],
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "scope": "dfl_lava_tail_risk_aware_target_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return target_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_lava_tail_risk_target",
        market_venue="DAM",
    ),
)
def dfl_lava_tail_risk_aware_strict_lp_benchmark_frame(
    context,
    config: DflLavaTailRiskTargetAssetConfig,
    dfl_lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    dfl_lava_tail_risk_aware_target_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict-score the tail-risk-aware target against frozen V2+."""

    strict_frame = build_dfl_lava_tail_risk_aware_strict_lp_benchmark_frame(
        dfl_lava_schedule_neighbor_candidate_frame,
        dfl_lava_tail_risk_aware_target_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_lava_tail_risk_aware_gate(
        strict_frame,
        min_validation_tenant_anchor_count=config.min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "strategy_kind": DFL_LAVA_TAIL_RISK_AWARE_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_replacement_passed": gate.metrics.get(
                "offline_strategy_challenger_passed",
                False,
            ),
            "production_promote": False,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_lava_tail_risk_aware_strict_lp_gate_not_full_dfl",
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
        backend="official_global_panel_lava_tail_risk_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_lava_tail_risk_safe_switch_scorer_frame(
    context,
    config: DflLavaTailRiskTargetAssetConfig,
    dfl_lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    dfl_lava_tail_risk_diagnostic_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Train a prior-profile safe-switch scorer over schedule candidates."""

    scorer_frame = build_dfl_lava_tail_risk_safe_switch_scorer_frame(
        dfl_lava_schedule_neighbor_candidate_frame,
        dfl_lava_tail_risk_diagnostic_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        min_prior_safe_win_count=config.min_prior_safe_win_count,
        max_prior_tail_loss_count=config.max_prior_tail_loss_count,
        min_prior_precision=config.min_prior_precision,
        min_prior_mean_improvement_uah=config.min_prior_mean_improvement_uah,
        allowed_candidate_sources=_csv_values(
            config.safe_switch_candidate_sources_csv,
            field_name="safe_switch_candidate_sources_csv",
        ),
        require_family_tail_loss_free=config.require_family_tail_loss_free,
        hard_blocked_candidate_families=_csv_values(
            config.hard_blocked_candidate_families_csv,
            field_name="hard_blocked_candidate_families_csv",
        ),
    )
    _add_metadata(
        context,
        {
            "rows": scorer_frame.height,
            "selector_gate_blockers": sorted(
                scorer_frame["selector_gate_blocker"].unique().to_list()
            )
            if scorer_frame.height
            else [],
            "uses_v2_plus_anchor_fallback_rows": scorer_frame.filter(
                pl.col("uses_v2_plus_anchor_fallback")
            ).height
            if scorer_frame.height
            else 0,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "scope": "dfl_lava_tail_risk_safe_switch_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return scorer_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_lava_tail_risk_safe_switch",
        market_venue="DAM",
    ),
)
def dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame(
    context,
    config: DflLavaTailRiskTargetAssetConfig,
    dfl_lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    dfl_lava_tail_risk_safe_switch_scorer_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict-score the safe-switch scorer against frozen V2+."""

    strict_frame = build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame(
        dfl_lava_schedule_neighbor_candidate_frame,
        dfl_lava_tail_risk_safe_switch_scorer_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_lava_tail_risk_safe_switch_gate(
        strict_frame,
        min_validation_tenant_anchor_count=config.min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "strategy_kind": DFL_LAVA_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_replacement_passed": gate.metrics.get(
                "offline_strategy_challenger_passed",
                False,
            ),
            "production_promote": False,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_lava_tail_risk_safe_switch_strict_lp_gate_not_full_dfl",
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
        backend="official_global_panel_lava_tail_risk_safe_switch_v2",
        market_venue="DAM",
    ),
)
def dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame(
    context,
    dfl_lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    dfl_lava_tail_risk_diagnostic_frame: pl.DataFrame,
    entsoe_poland_lagged_feature_candidate_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Attach repaired rich Poland context to LAVA safe-switch candidates."""

    feature_panel = build_dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame(
        dfl_lava_schedule_neighbor_candidate_frame,
        dfl_lava_tail_risk_diagnostic_frame,
        entsoe_poland_lagged_feature_candidate_frame,
    )
    _add_metadata(
        context,
        {
            "rows": feature_panel.height,
            "repaired_rows": feature_panel.filter(
                pl.col("selector_feature_repaired_null_count") > 0
            ).height
            if feature_panel.height
            else 0,
            "selector_feature_count": len(
                [column for column in feature_panel.columns if column.startswith("selector_feature_")]
            ),
            "scope": "dfl_lava_tail_risk_safe_switch_v2_not_full_dfl",
            "market_execution_enabled": False,
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
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_lava_tail_risk_safe_switch_v2",
        market_venue="DAM",
    ),
)
def dfl_lava_tail_risk_safe_switch_scorer_v2_frame(
    context,
    config: DflLavaTailRiskTargetAssetConfig,
    dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Train the richer prior-safe LAVA switch scorer."""

    scorer_frame = build_dfl_lava_tail_risk_safe_switch_scorer_v2_frame(
        dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        min_prior_safe_win_count=config.min_prior_safe_win_count,
        max_prior_tail_loss_count=config.max_prior_tail_loss_count,
        min_prior_precision=config.min_prior_precision,
        min_prior_mean_improvement_uah=config.min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=config.min_predicted_improvement_uah,
        allowed_candidate_sources=_csv_values(
            config.safe_switch_candidate_sources_csv,
            field_name="safe_switch_candidate_sources_csv",
        ),
        hard_blocked_candidate_families=_csv_values(
            config.hard_blocked_candidate_families_csv,
            field_name="hard_blocked_candidate_families_csv",
        ),
        ridge_l2=config.ridge_l2,
    )
    _add_metadata(
        context,
        {
            "rows": scorer_frame.height,
            "selector_gate_blockers": sorted(
                scorer_frame["selector_gate_blocker"].unique().to_list()
            )
            if scorer_frame.height
            else [],
            "uses_v2_plus_anchor_fallback_rows": scorer_frame.filter(
                pl.col("uses_v2_plus_anchor_fallback")
            ).height
            if scorer_frame.height
            else 0,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "scope": "dfl_lava_tail_risk_safe_switch_v2_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return scorer_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_lava_tail_risk_safe_switch_v2",
        market_venue="DAM",
    ),
)
def dfl_lava_tail_risk_safe_switch_v2_strict_lp_benchmark_frame(
    context,
    config: DflLavaTailRiskTargetAssetConfig,
    dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame: pl.DataFrame,
    dfl_lava_tail_risk_safe_switch_scorer_v2_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict-score the richer LAVA safe-switch scorer against V2+."""

    strict_frame = build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_v2_frame(
        dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame,
        dfl_lava_tail_risk_safe_switch_scorer_v2_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_lava_tail_risk_safe_switch_v2_gate(
        strict_frame,
        min_validation_tenant_anchor_count=config.min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "strategy_kind": DFL_LAVA_SAFE_SWITCH_V2_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_replacement_passed": gate.metrics.get(
                "offline_strategy_challenger_passed",
                False,
            ),
            "production_promote": False,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_lava_tail_risk_safe_switch_v2_strict_lp_gate_not_full_dfl",
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
        backend="official_global_panel_lava_tail_risk_avoidance_v3",
        market_venue="DAM",
    ),
)
def dfl_lava_tail_risk_avoidance_label_frame(
    context,
    config: DflLavaTailRiskTargetAssetConfig,
    dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build explicit safe-switch/tail-risk labels for the v3 LAVA bridge."""

    label_frame = build_dfl_lava_tail_risk_avoidance_label_frame(
        dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame,
        tail_risk_delta_uah=config.tail_risk_delta_uah,
    )
    _add_metadata(
        context,
        {
            "rows": label_frame.height,
            "tail_risk_rows": label_frame.filter(
                pl.col("label_tail_risk_switch")
            ).height
            if label_frame.height
            else 0,
            "safe_switch_rows": label_frame.filter(
                pl.col("label_safe_switch_win")
            ).height
            if label_frame.height
            else 0,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "scope": "dfl_lava_tail_risk_avoidance_v3_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return label_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_lava_tail_risk_avoidance_v3",
        market_venue="DAM",
    ),
)
def dfl_lava_tail_risk_avoidance_scorer_v3_frame(
    context,
    config: DflLavaTailRiskTargetAssetConfig,
    dfl_lava_tail_risk_avoidance_label_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Train the v3 LAVA scorer with an explicit predicted tail-risk veto."""

    scorer_frame = build_dfl_lava_tail_risk_avoidance_scorer_v3_frame(
        dfl_lava_tail_risk_avoidance_label_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        min_prior_safe_win_count=config.min_prior_safe_win_count,
        max_prior_tail_loss_count=config.max_prior_tail_loss_count,
        min_prior_precision=config.min_prior_precision,
        min_prior_mean_improvement_uah=config.min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=config.min_predicted_improvement_uah,
        max_predicted_tail_risk_probability=(
            config.max_predicted_tail_risk_probability
        ),
        allowed_candidate_sources=_csv_values(
            config.safe_switch_candidate_sources_csv,
            field_name="safe_switch_candidate_sources_csv",
        ),
        hard_blocked_candidate_families=_csv_values(
            config.hard_blocked_candidate_families_csv,
            field_name="hard_blocked_candidate_families_csv",
        ),
        ridge_l2=config.ridge_l2,
    )
    _add_metadata(
        context,
        {
            "rows": scorer_frame.height,
            "selector_gate_blockers": sorted(
                scorer_frame["selector_gate_blocker"].unique().to_list()
            )
            if scorer_frame.height
            else [],
            "uses_v2_plus_anchor_fallback_rows": scorer_frame.filter(
                pl.col("uses_v2_plus_anchor_fallback")
            ).height
            if scorer_frame.height
            else 0,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "scope": "dfl_lava_tail_risk_avoidance_v3_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return scorer_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_lava_tail_risk_avoidance_v3",
        market_venue="DAM",
    ),
)
def dfl_lava_tail_risk_avoidance_v3_strict_lp_benchmark_frame(
    context,
    config: DflLavaTailRiskTargetAssetConfig,
    dfl_lava_tail_risk_avoidance_label_frame: pl.DataFrame,
    dfl_lava_tail_risk_avoidance_scorer_v3_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict-score the v3 tail-risk avoidance scorer against V2+."""

    strict_frame = build_dfl_lava_tail_risk_avoidance_strict_lp_benchmark_v3_frame(
        dfl_lava_tail_risk_avoidance_label_frame,
        dfl_lava_tail_risk_avoidance_scorer_v3_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_lava_tail_risk_avoidance_v3_gate(
        strict_frame,
        min_validation_tenant_anchor_count=config.min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "strategy_kind": DFL_LAVA_TAIL_RISK_AVOIDANCE_V3_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_replacement_passed": gate.metrics.get(
                "offline_strategy_challenger_passed",
                False,
            ),
            "production_promote": False,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_lava_tail_risk_avoidance_v3_strict_lp_gate_not_full_dfl",
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
        backend="official_global_panel_lava_schedule_neighbor_dt",
        market_venue="DAM",
    ),
)
def dfl_lava_schedule_neighbor_dt_training_frame(
    context,
    dfl_lava_tail_risk_avoidance_label_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build DT/LAVA schedule-neighbor supervision from the v3 label frame."""

    training_frame = build_dfl_lava_schedule_neighbor_dt_training_frame(
        dfl_lava_tail_risk_avoidance_label_frame
    )
    _add_metadata(
        context,
        {
            "rows": training_frame.height,
            "training_rows": training_frame.filter(pl.col("is_training_row")).height
            if training_frame.height
            else 0,
            "teacher_classes": sorted(
                training_frame["teacher_schedule_neighbor_class"].unique().to_list()
            )
            if training_frame.height
            else [],
            "target_label_space": "schedule_neighbor_candidate_index",
            "raw_hourly_action_imitation": False,
            "scope": "dfl_lava_schedule_neighbor_dt_supervision_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return training_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_lava_schedule_neighbor_dt",
        market_venue="DAM",
    ),
)
def dfl_lava_schedule_neighbor_dt_policy_frame(
    context,
    config: DflLavaTailRiskTargetAssetConfig,
    dfl_lava_schedule_neighbor_dt_training_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Fit a conservative schedule-neighbor policy from DT-ready teacher rows."""

    policy_frame = build_dfl_lava_schedule_neighbor_dt_policy_frame(
        dfl_lava_schedule_neighbor_dt_training_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        min_prior_safe_win_count=config.min_prior_safe_win_count,
        max_prior_tail_loss_count=config.max_prior_tail_loss_count,
        min_prior_precision=config.min_prior_precision,
        min_prior_mean_improvement_uah=config.min_prior_mean_improvement_uah,
        min_return_to_go_delta_uah=config.min_dt_return_to_go_delta_uah,
        max_dt_tail_risk_probability=config.max_dt_tail_risk_probability,
        allowed_candidate_sources=_csv_values(
            config.safe_switch_candidate_sources_csv,
            field_name="safe_switch_candidate_sources_csv",
        ),
        hard_blocked_candidate_families=_csv_values(
            config.hard_blocked_candidate_families_csv,
            field_name="hard_blocked_candidate_families_csv",
        ),
        ridge_l2=config.ridge_l2,
    )
    _add_metadata(
        context,
        {
            "rows": policy_frame.height,
            "selector_gate_blockers": sorted(
                policy_frame["selector_gate_blocker"].unique().to_list()
            )
            if policy_frame.height
            else [],
            "uses_v2_plus_anchor_fallback_rows": policy_frame.filter(
                pl.col("uses_v2_plus_anchor_fallback")
            ).height
            if policy_frame.height
            else 0,
            "target_label_space": "schedule_neighbor_candidate_index",
            "raw_hourly_action_imitation": False,
            "scope": "dfl_lava_schedule_neighbor_dt_supervision_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return policy_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_lava_schedule_neighbor_dt",
        market_venue="DAM",
    ),
)
def dfl_lava_schedule_neighbor_dt_strict_lp_benchmark_frame(
    context,
    config: DflLavaTailRiskTargetAssetConfig,
    dfl_lava_schedule_neighbor_dt_training_frame: pl.DataFrame,
    dfl_lava_schedule_neighbor_dt_policy_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict-score schedule-neighbor DT/LAVA supervision against V2+."""

    strict_frame = build_dfl_lava_schedule_neighbor_dt_strict_lp_benchmark_frame(
        dfl_lava_schedule_neighbor_dt_training_frame,
        dfl_lava_schedule_neighbor_dt_policy_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_lava_schedule_neighbor_dt_gate(
        strict_frame,
        min_validation_tenant_anchor_count=config.min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "strategy_kind": DFL_LAVA_SCHEDULE_NEIGHBOR_DT_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_replacement_passed": gate.metrics.get(
                "offline_strategy_challenger_passed",
                False,
            ),
            "production_promote": False,
            "target_label_space": "schedule_neighbor_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
            "scope": "dfl_lava_schedule_neighbor_dt_strict_lp_gate_not_full_dfl",
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
        backend="official_global_panel_poland_lag24_calibrated",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_robustness_frame(
    context,
    config: DflPolandLag24CalibratedRobustnessAssetConfig,
    dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Replay calibrated Poland-enhanced V2+ over rolling windows."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    robustness_frame = build_dfl_schedule_value_learner_v2_plus_robustness_frame(
        dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_prior_anchors_before_window=config.min_prior_anchors_before_window,
        min_robust_passing_windows=config.min_robust_passing_windows,
        min_validation_tenant_anchor_count_per_source_model=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_prior_mean_improvement_ratio_vs_v2=(
            config.min_prior_mean_improvement_ratio_vs_v2
        ),
    ).with_columns(
        pl.lit(False).alias("market_execution_enabled"),
        pl.lit(True).alias("not_market_execution"),
    )
    gate = evaluate_dfl_schedule_value_learner_v2_plus_robustness_gate(
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
            "decision": gate.decision,
            "market_execution_enabled": False,
            "scope": (
                "dfl_poland_lag24_calibrated_schedule_value_v2_plus_"
                "rolling_robustness_not_full_dfl"
            ),
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
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_poland_lag24_calibrated",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_rolling_vs_frozen_v2_plus_gate_frame(
    context,
    config: DflPolandLag24RollingVsFrozenGateAssetConfig,
    dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_robustness_frame: (
        pl.DataFrame
    ),
    dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Compare Poland rolling windows with the frozen Ukrainian-only V2+ gate."""

    gate_frame = build_poland_lag24_rolling_vs_frozen_v2_plus_gate_frame(
        dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_robustness_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame,
        min_mean_regret_improvement_ratio_vs_frozen_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_frozen_v2_plus
        ),
        min_passing_windows=config.min_passing_windows,
    )
    _add_metadata(
        context,
        {
            "rows": gate_frame.height,
            "passing_windows": gate_frame.filter(pl.col("poland_window_passed")).height,
            "source_model_count": gate_frame.select("source_model_name").n_unique(),
            "gate_statuses": gate_frame.select(
                "rolling_gate_status"
            ).to_series().unique().sort().to_list(),
            "market_execution_enabled": False,
            "scope": "poland_lag24_rolling_vs_frozen_v2_plus_gate_not_full_dfl",
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
        backend="official_global_panel_poland_lag24_experimental",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_experimental_schedule_candidate_library_frame(
    context,
    config: DflOfficialGlobalPanelScheduleCandidateLibraryAssetConfig,
    official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Schedule library for Poland-lag24 experimental official forecast names."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    library_frame = build_dfl_schedule_candidate_library_from_strict_benchmark_frame(
        official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
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
            "source_model_names": source_model_names,
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
            if library_frame.height
            else 0,
            "scope": "dfl_poland_lag24_experimental_schedule_library_not_full_dfl",
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
        backend="official_global_panel_poland_lag24_experimental",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_experimental_schedule_candidate_library_v2_frame(
    context,
    config: DflStrictChallengerAssetConfig,
    dfl_poland_lag24_experimental_schedule_candidate_library_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Blend/residual candidates for Poland-lag24 experimental schedules."""

    library_frame = build_schedule_candidate_library_v2_frame(
        dfl_poland_lag24_experimental_schedule_candidate_library_frame,
        blend_weights=_float_csv_values(
            config.blend_weights_csv,
            field_name="blend_weights_csv",
        ),
        residual_min_prior_anchors=config.residual_min_prior_anchors,
    )
    _add_metadata(
        context,
        {
            "rows": library_frame.height,
            "scope": "dfl_poland_lag24_experimental_schedule_library_v2_not_full_dfl",
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
        backend="official_global_panel_poland_lag24_experimental",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_experimental_schedule_candidate_library_v2_plus_frame(
    context,
    config: DflScheduleCandidateLibraryV2PlusAssetConfig,
    dfl_poland_lag24_experimental_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """V2+ candidate expansion for Poland-lag24 experimental schedules."""

    library_frame = build_dfl_schedule_candidate_library_v2_plus_frame(
        dfl_poland_lag24_experimental_schedule_candidate_library_v2_frame,
        rank_perturbation_delta_uah_mwh=config.rank_perturbation_delta_uah_mwh,
        robust_spread_scales=_float_csv_values(
            config.robust_spread_scales_csv,
            field_name="robust_spread_scales_csv",
        ),
        strict_neighborhood_shift_hours=_int_csv_values(
            config.strict_neighborhood_shift_hours_csv,
            field_name="strict_neighborhood_shift_hours_csv",
        ),
        block_reconcile_hours=_int_csv_values(
            config.block_reconcile_hours_csv,
            field_name="block_reconcile_hours_csv",
        ),
        terminal_target_shift_uah_mwh=config.terminal_target_shift_uah_mwh,
        generated_at=_latest_generated_at(
            dfl_poland_lag24_experimental_schedule_candidate_library_v2_frame
        ),
    )
    _add_metadata(
        context,
        {
            "rows": library_frame.height,
            "scope": "dfl_poland_lag24_experimental_schedule_library_v2_plus_not_full_dfl",
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
        backend="official_global_panel_poland_lag24_experimental",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_experimental_schedule_value_learner_v2_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV2AssetConfig,
    dfl_poland_lag24_experimental_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only schedule/value learner over experimental schedules."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    learner_frame = build_dfl_schedule_value_learner_v2_frame(
        dfl_poland_lag24_experimental_schedule_candidate_library_v2_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
    )
    _add_metadata(
        context,
        {
            "rows": learner_frame.height,
            "source_model_names": source_model_names,
            "scope": "dfl_poland_lag24_experimental_schedule_value_v2_not_full_dfl",
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
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_poland_lag24_experimental",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_experimental_schedule_value_learner_v2_plus_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV2PlusAssetConfig,
    dfl_poland_lag24_experimental_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_poland_lag24_experimental_schedule_value_learner_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """V2+ selector for Poland-lag24 experimental source schedules."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    learner_frame = build_dfl_schedule_value_learner_v2_plus_frame(
        dfl_poland_lag24_experimental_schedule_candidate_library_v2_plus_frame,
        dfl_poland_lag24_experimental_schedule_value_learner_v2_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_v2=(
            config.min_prior_mean_improvement_ratio_vs_v2
        ),
    )
    _add_metadata(
        context,
        {
            "rows": learner_frame.height,
            "source_model_names": source_model_names,
            "fallback_rows": learner_frame.filter(pl.col("fallback_to_v2")).height
            if learner_frame.height
            else 0,
            "scope": (
                "dfl_poland_lag24_experimental_schedule_value_v2_plus_not_full_dfl"
            ),
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
        backend="official_global_panel_poland_lag24_experimental",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_experimental_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV2PlusAssetConfig,
    dfl_poland_lag24_experimental_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_poland_lag24_experimental_schedule_value_learner_v2_frame: pl.DataFrame,
    dfl_poland_lag24_experimental_schedule_value_learner_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle schedule-value rows for Poland-lag24 experimental sources."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        dfl_poland_lag24_experimental_schedule_candidate_library_v2_plus_frame,
        dfl_poland_lag24_experimental_schedule_value_learner_v2_plus_frame,
        dfl_poland_lag24_experimental_schedule_value_learner_v2_frame,
        generated_at=_latest_generated_at(
            dfl_poland_lag24_experimental_schedule_candidate_library_v2_plus_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_schedule_value_learner_v2_plus_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "source_model_names": source_model_names,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "production_gate_passed": gate.metrics.get("production_gate_passed", False),
            "market_execution_enabled": False,
            "scope": (
                "dfl_poland_lag24_experimental_schedule_value_v2_plus_"
                "strict_gate_not_full_dfl"
            ),
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
        backend="official_global_panel_poland_lag24_experimental",
        market_venue="DAM",
    ),
)
def dfl_poland_lag24_experimental_vs_v2_plus_comparison_frame(
    context,
    dfl_poland_lag24_experimental_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Compare experimental Poland-lag24 schedule/value rows with frozen V2+."""

    experimental_frame = (
        dfl_poland_lag24_experimental_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
    )
    baseline_frame = (
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
    )
    comparison_rows = _summarize_poland_lag24_vs_v2_plus(
        experimental_frame,
        baseline_frame,
    )
    _add_metadata(
        context,
        {
            "rows": comparison_rows.height,
            "scope": (
                "dfl_poland_lag24_experimental_vs_frozen_ukrainian_v2_plus_"
                "comparison_not_full_dfl"
            ),
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return comparison_rows


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="official_global_panel_tft",
        market_venue="DAM",
    ),
)
def dfl_tft_quantile_schedule_candidate_library_frame(
    context,
    config: DflTftQuantileScheduleValueAssetConfig,
    tft_official_global_panel_rolling_strict_lp_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """TFT p10/p50/p90 schedule library for V2+-anchored contribution tests."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    library_frame = build_dfl_tft_quantile_schedule_candidate_library_frame(
        tft_official_global_panel_rolling_strict_lp_benchmark_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
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
            "source_model_names": list(source_model_names),
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
            if library_frame.height
            else 0,
            "scope": "dfl_tft_quantile_schedule_library_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_tft",
        market_venue="DAM",
    ),
)
def dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame(
    context,
    config: DflTftQuantileScheduleValueAssetConfig,
    dfl_tft_quantile_schedule_candidate_library_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict gate for TFT quantile candidates against frozen official V2+."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame(
        dfl_tft_quantile_schedule_candidate_library_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_v2=(
            config.min_prior_mean_improvement_ratio_vs_v2
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_tft_augmented_v2_plus_gate(
        strict_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        tft_source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio_vs_baseline=(
            config.min_mean_regret_improvement_ratio_vs_baseline
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "strategy_kind": DFL_TFT_AUGMENTED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_challenger_passed": gate.passed,
            "baseline_source_model_name": config.baseline_source_model_name,
            "best_tft_source_model_name": gate.metrics.get(
                "best_tft_source_model_name"
            ),
            "market_execution_enabled": False,
            "scope": "dfl_tft_augmented_v2_plus_gate_not_full_dfl",
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
        backend="official_global_panel_nbeatsx_tft",
        market_venue="DAM",
    ),
)
def dfl_tft_combined_v2_plus_strict_lp_benchmark_frame(
    context,
    config: DflTftQuantileScheduleValueAssetConfig,
    dfl_tft_quantile_schedule_candidate_library_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_official_global_panel_schedule_value_learner_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict gate for TFT complementary schedules on top of frozen V2+."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_tft_combined_v2_plus_strict_lp_benchmark_frame(
        dfl_tft_quantile_schedule_candidate_library_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        baseline_source_model_name=config.baseline_source_model_name,
        tft_source_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_v2_plus=(
            config.min_prior_mean_improvement_ratio_vs_v2
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_tft_combined_v2_plus_gate(
        strict_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio_vs_baseline=(
            config.min_mean_regret_improvement_ratio_vs_baseline
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "strategy_kind": DFL_TFT_COMBINED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_challenger_passed": gate.passed,
            "baseline_source_model_name": config.baseline_source_model_name,
            "combined_mean_regret_uah": gate.metrics.get("combined_mean_regret_uah"),
            "baseline_mean_regret_uah": gate.metrics.get("baseline_mean_regret_uah"),
            "selected_tft_count": gate.metrics.get("selected_tft_count"),
            "fallback_to_v2_plus_count": gate.metrics.get(
                "fallback_to_v2_plus_count"
            ),
            "market_execution_enabled": False,
            "scope": "dfl_tft_combined_v2_plus_gate_not_full_dfl",
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
        backend="official_global_panel_tft",
        market_venue="DAM",
    ),
)
def dfl_tft_calibrated_quantile_schedule_candidate_library_frame(
    context,
    config: DflTftCalibratedQuantileScheduleValueAssetConfig,
    tft_official_global_panel_horizon_quantile_calibrated_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Calibrated TFT p10/p50/p90 schedule library for V2+-anchored tests."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    library_frame = build_dfl_tft_quantile_schedule_candidate_library_frame(
        tft_official_global_panel_horizon_quantile_calibrated_strict_lp_benchmark_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
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
            "source_model_names": list(source_model_names),
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
            if library_frame.height
            else 0,
            "scope": "dfl_tft_calibrated_quantile_schedule_library_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_tft",
        market_venue="DAM",
    ),
)
def dfl_tft_calibrated_augmented_v2_plus_strict_lp_benchmark_frame(
    context,
    config: DflTftCalibratedQuantileScheduleValueAssetConfig,
    dfl_tft_calibrated_quantile_schedule_candidate_library_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict gate for calibrated TFT quantile candidates against frozen V2+."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame(
        dfl_tft_calibrated_quantile_schedule_candidate_library_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_v2=(
            config.min_prior_mean_improvement_ratio_vs_v2
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_tft_augmented_v2_plus_gate(
        strict_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        tft_source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio_vs_baseline=(
            config.min_mean_regret_improvement_ratio_vs_baseline
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "strategy_kind": DFL_TFT_AUGMENTED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_challenger_passed": gate.passed,
            "baseline_source_model_name": config.baseline_source_model_name,
            "best_tft_source_model_name": gate.metrics.get(
                "best_tft_source_model_name"
            ),
            "market_execution_enabled": False,
            "scope": "dfl_tft_calibrated_augmented_v2_plus_gate_not_full_dfl",
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
        backend="official_global_panel_nbeatsx_tft",
        market_venue="DAM",
    ),
)
def dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame(
    context,
    config: DflTftCalibratedQuantileScheduleValueAssetConfig,
    dfl_tft_calibrated_quantile_schedule_candidate_library_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_official_global_panel_schedule_value_learner_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict gate for calibrated TFT complementary schedules on top of V2+."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_tft_combined_v2_plus_strict_lp_benchmark_frame(
        dfl_tft_calibrated_quantile_schedule_candidate_library_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        baseline_source_model_name=config.baseline_source_model_name,
        tft_source_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_v2_plus=(
            config.min_prior_mean_improvement_ratio_vs_v2
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_tft_combined_v2_plus_gate(
        strict_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio_vs_baseline=(
            config.min_mean_regret_improvement_ratio_vs_baseline
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "strategy_kind": DFL_TFT_COMBINED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_challenger_passed": gate.passed,
            "baseline_source_model_name": config.baseline_source_model_name,
            "combined_mean_regret_uah": gate.metrics.get("combined_mean_regret_uah"),
            "baseline_mean_regret_uah": gate.metrics.get("baseline_mean_regret_uah"),
            "selected_tft_count": gate.metrics.get("selected_tft_count"),
            "fallback_to_v2_plus_count": gate.metrics.get(
                "fallback_to_v2_plus_count"
            ),
            "market_execution_enabled": False,
            "scope": "dfl_tft_calibrated_combined_v2_plus_gate_not_full_dfl",
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
        backend="official_global_panel_nbeatsx_tft",
        market_venue="DAM",
    ),
)
def dfl_nbeatsx_tft_complementarity_audit_frame(
    context,
    config: DflNbeatsxTftCombinedPortfolioAssetConfig,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_tft_calibrated_quantile_schedule_candidate_library_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Audit whether calibrated TFT has per-anchor candidates that beat V2+."""

    tft_source_model_names = _forecast_model_names(config.tft_source_model_names_csv)
    audit_frame = build_dfl_nbeatsx_tft_complementarity_audit_frame(
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        dfl_tft_calibrated_quantile_schedule_candidate_library_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        tft_source_model_names=tft_source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
    )
    _add_metadata(
        context,
        {
            "rows": audit_frame.height,
            "tenant_count": audit_frame.select("tenant_id").n_unique()
            if audit_frame.height
            else 0,
            "complementarity_classes": sorted(
                audit_frame["complementarity_class"].unique().to_list()
            )
            if audit_frame.height
            else [],
            "baseline_source_model_name": config.baseline_source_model_name,
            "tft_source_model_names": list(tft_source_model_names),
            "scope": "dfl_nbeatsx_tft_complementarity_audit_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx_tft",
        market_venue="DAM",
    ),
)
def dfl_nbeatsx_tft_candidate_portfolio_v1_frame(
    context,
    config: DflNbeatsxTftCombinedPortfolioAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_tft_calibrated_quantile_schedule_candidate_library_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_nbeatsx_tft_complementarity_audit_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Candidate-level NBEATSx V2+ plus calibrated TFT schedule portfolio."""

    tft_source_model_names = _forecast_model_names(config.tft_source_model_names_csv)
    portfolio_frame = build_dfl_nbeatsx_tft_candidate_portfolio_v1_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        dfl_tft_calibrated_quantile_schedule_candidate_library_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        dfl_nbeatsx_tft_complementarity_audit_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        tft_source_model_names=tft_source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
    )
    _add_metadata(
        context,
        {
            "rows": portfolio_frame.height,
            "tenant_count": portfolio_frame.select("tenant_id").n_unique()
            if portfolio_frame.height
            else 0,
            "portfolio_source_count": portfolio_frame.select(
                "portfolio_source"
            ).n_unique()
            if portfolio_frame.height
            else 0,
            "candidate_family_count": portfolio_frame.select(
                "candidate_family"
            ).n_unique()
            if portfolio_frame.height
            else 0,
            "scope": "dfl_nbeatsx_tft_candidate_portfolio_v1_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return portfolio_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx_tft",
        market_venue="DAM",
    ),
)
def dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame(
    context,
    config: DflNbeatsxTftCombinedPortfolioAssetConfig,
    dfl_nbeatsx_tft_candidate_portfolio_v1_frame: pl.DataFrame,
    dfl_nbeatsx_tft_complementarity_audit_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only candidate-level value selector with frozen V2+ fallback."""

    selector_frame = build_dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame(
        dfl_nbeatsx_tft_candidate_portfolio_v1_frame,
        dfl_nbeatsx_tft_complementarity_audit_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        baseline_source_model_name=config.baseline_source_model_name,
        combined_source_model_name=config.combined_source_model_name,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_v2_plus=(
            config.min_prior_mean_improvement_ratio_vs_v2_plus
        ),
    )
    _add_metadata(
        context,
        {
            "rows": selector_frame.height,
            "tenant_count": selector_frame.select("tenant_id").n_unique()
            if selector_frame.height
            else 0,
            "fallback_to_v2_plus_count": selector_frame.filter(
                pl.col("fallback_to_v2_plus")
            ).height
            if selector_frame.height
            else 0,
            "combined_source_model_name": config.combined_source_model_name,
            "scope": "dfl_nbeatsx_tft_candidate_value_meta_selector_v1_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx_tft",
        market_venue="DAM",
    ),
)
def dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame(
    context,
    config: DflNbeatsxTftCombinedPortfolioAssetConfig,
    dfl_nbeatsx_tft_candidate_portfolio_v1_frame: pl.DataFrame,
    dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict LP/oracle benchmark for the candidate-portfolio meta-selector."""

    strict_frame = build_dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame(
        dfl_nbeatsx_tft_candidate_portfolio_v1_frame,
        dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_nbeatsx_tft_meta_selector_gate(
        strict_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        combined_source_model_name=config.combined_source_model_name,
        min_validation_tenant_anchor_count=config.min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "strategy_kind": DFL_NBEATSX_TFT_META_SELECTOR_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_replacement_passed": gate.passed,
            "v2_plus_mean_regret_uah": gate.metrics.get("v2_plus_mean_regret_uah"),
            "selected_mean_regret_uah": gate.metrics.get("selected_mean_regret_uah"),
            "fallback_to_v2_plus_count": gate.metrics.get(
                "fallback_to_v2_plus_count"
            ),
            "combined_source_model_name": config.combined_source_model_name,
            "market_execution_enabled": False,
            "scope": "dfl_nbeatsx_tft_meta_selector_strict_lp_gate_not_full_dfl",
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
        backend="official_global_panel_nbeatsx_tft",
        market_venue="DAM",
    ),
)
def dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame(
    context,
    config: DflNbeatsxTftCombinedPortfolioAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_tft_calibrated_quantile_schedule_candidate_library_frame: pl.DataFrame,
) -> pl.DataFrame:
    """True rolling strict LP/oracle benchmark for the portfolio selector."""

    tft_source_model_names = _forecast_model_names(config.tft_source_model_names_csv)
    strict_frame = build_dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        dfl_tft_calibrated_quantile_schedule_candidate_library_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        baseline_source_model_name=config.baseline_source_model_name,
        tft_source_model_names=tft_source_model_names,
        combined_source_model_name=config.combined_source_model_name,
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_prior_anchors_before_window=config.min_prior_anchors_before_window,
        min_prior_mean_improvement_ratio_vs_v2_plus=(
            config.min_prior_mean_improvement_ratio_vs_v2_plus
        ),
        max_tft_candidates_per_anchor_source_family=(
            config.max_tft_candidates_per_anchor_source_family
        ),
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_candidate_library_v2_plus_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_nbeatsx_tft_meta_selector_gate(
        strict_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        combined_source_model_name=config.combined_source_model_name,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count
            * config.validation_window_count
        ),
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "strategy_kind": DFL_NBEATSX_TFT_META_SELECTOR_ROLLING_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_replacement_passed": gate.passed,
            "v2_plus_mean_regret_uah": gate.metrics.get("v2_plus_mean_regret_uah"),
            "selected_mean_regret_uah": gate.metrics.get("selected_mean_regret_uah"),
            "fallback_to_v2_plus_count": gate.metrics.get(
                "fallback_to_v2_plus_count"
            ),
            "validation_window_count": config.validation_window_count,
            "validation_anchor_count": config.validation_anchor_count,
            "min_prior_anchors_before_window": config.min_prior_anchors_before_window,
            "combined_source_model_name": config.combined_source_model_name,
            "market_execution_enabled": False,
            "scope": (
                "dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_gate_not_full_dfl"
            ),
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
        backend="official_global_panel_nbeatsx_tft",
        market_venue="DAM",
    ),
)
def dfl_nbeatsx_tft_meta_selector_prior_rolling_robustness_frame(
    context,
    config: DflNbeatsxTftCombinedPortfolioAssetConfig,
    dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Robustness summary from the true rolling portfolio strict frame."""

    robustness_frame = build_dfl_nbeatsx_tft_meta_selector_robustness_frame(
        dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        combined_source_model_name=config.combined_source_model_name,
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
    )
    _add_metadata(
        context,
        {
            "rows": robustness_frame.height,
            "rolling_pass_count": robustness_frame.filter(
                pl.col("rolling_pass")
            ).height
            if robustness_frame.height
            else 0,
            "validation_window_count": config.validation_window_count,
            "validation_anchor_count": config.validation_anchor_count,
            "combined_source_model_name": config.combined_source_model_name,
            "market_execution_enabled": False,
            "scope": "dfl_nbeatsx_tft_meta_selector_prior_rolling_robustness_not_full_dfl",
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
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx_tft",
        market_venue="DAM",
    ),
)
def dfl_nbeatsx_tft_meta_selector_robustness_frame(
    context,
    config: DflNbeatsxTftCombinedPortfolioAssetConfig,
    dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling robustness for the NBEATSx+TFT candidate portfolio."""

    robustness_frame = build_dfl_nbeatsx_tft_meta_selector_robustness_frame(
        dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame,
        baseline_source_model_name=config.baseline_source_model_name,
        combined_source_model_name=config.combined_source_model_name,
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
    )
    _add_metadata(
        context,
        {
            "rows": robustness_frame.height,
            "rolling_pass_count": robustness_frame.filter(
                pl.col("rolling_pass")
            ).height
            if robustness_frame.height
            else 0,
            "validation_window_count": config.validation_window_count,
            "validation_anchor_count": config.validation_anchor_count,
            "combined_source_model_name": config.combined_source_model_name,
            "market_execution_enabled": False,
            "scope": "dfl_nbeatsx_tft_meta_selector_robustness_not_full_dfl",
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_value_dfl_v2_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueDflV2AssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only pairwise schedule-family ranking with frozen V2+ fallback."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    learner_frame = build_dfl_schedule_value_dfl_v2_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        dfl_official_global_panel_schedule_value_learner_v2_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_v2_plus=(
            config.min_prior_mean_improvement_ratio_vs_v2_plus
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
            "fallback_rows": learner_frame.filter(pl.col("fallback_to_v2_plus")).height
            if learner_frame.height
            else 0,
            "scope": "dfl_official_global_panel_schedule_value_dfl_v2_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_value_dfl_v2_strict_lp_benchmark_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueDflV2AssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_dfl_v2_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Compare pairwise DFL v2 against frozen official V2+ strict evidence."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_schedule_value_dfl_v2_strict_lp_benchmark_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        dfl_official_global_panel_schedule_value_dfl_v2_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_schedule_value_dfl_v2_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "strategy_kind": DFL_SCHEDULE_VALUE_DFL_V2_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "best_source_model_name": gate.metrics.get("best_source_model_name"),
            "offline_strategy_replacement_passed": gate.metrics.get(
                "offline_strategy_replacement_passed",
                False,
            ),
            "market_execution_enabled": False,
            "scope": (
                "dfl_official_global_panel_schedule_value_dfl_v2_strict_gate_"
                "not_full_dfl"
            ),
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_candidate_library_v3_frame(
    context,
    config: DflOfficialGlobalPanelCandidateValueDflV3AssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Expanded V3 candidate library around audited V2+ failure modes."""

    library_frame = build_dfl_schedule_candidate_library_v3_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        strict_neighborhood_shift_hours=tuple(
            int(value)
            for value in _csv_values(
                config.strict_neighborhood_shift_hours_csv,
                field_name="strict_neighborhood_shift_hours_csv",
            )
        ),
        terminal_target_shift_uah_mwh=_float_csv_values(
            config.terminal_target_shift_uah_mwh_csv,
            field_name="terminal_target_shift_uah_mwh_csv",
        ),
        peak_trough_delta_uah_mwh=config.peak_trough_delta_uah_mwh,
        uncertainty_spread_scales=_float_csv_values(
            config.uncertainty_spread_scales_csv,
            field_name="uncertainty_spread_scales_csv",
        ),
        degradation_spread_scales=_float_csv_values(
            config.degradation_spread_scales_csv,
            field_name="degradation_spread_scales_csv",
        ),
        include_train_oracle_neighborhood=config.include_train_oracle_neighborhood,
        max_train_generation_anchor_count_per_tenant=(
            config.max_train_generation_anchor_count_per_tenant
        ),
        min_prior_template_anchor_count=config.min_prior_template_anchor_count,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_candidate_library_v2_plus_frame
        ),
    )
    _add_metadata(
        context,
        {
            "rows": library_frame.height,
            "tenant_count": library_frame.select("tenant_id").n_unique()
            if library_frame.height
            else 0,
            "source_model_count": len(
                _forecast_model_names(config.forecast_model_names_csv)
            ),
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
            if library_frame.height
            else 0,
            "max_train_generation_anchor_count_per_tenant": (
                config.max_train_generation_anchor_count_per_tenant
            ),
            "min_prior_template_anchor_count": config.min_prior_template_anchor_count,
            "scope": "dfl_official_global_panel_schedule_candidate_library_v3_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_candidate_value_label_panel_v3_frame(
    context,
    dfl_official_global_panel_schedule_candidate_library_v3_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-safe candidate features and realized value labels for V3 objective work."""

    label_panel_frame = build_dfl_candidate_value_label_panel_v3_frame(
        dfl_official_global_panel_schedule_candidate_library_v3_frame
    )
    selector_feature_count = len(
        [
            column
            for column in label_panel_frame.columns
            if column.startswith("selector_feature_")
        ]
    )
    label_count = len(
        [column for column in label_panel_frame.columns if column.startswith("label_")]
    )
    _add_metadata(
        context,
        {
            "rows": label_panel_frame.height,
            "tenant_count": label_panel_frame.select("tenant_id").n_unique()
            if label_panel_frame.height
            else 0,
            "source_model_count": label_panel_frame.select(
                "source_model_name"
            ).n_unique()
            if label_panel_frame.height
            else 0,
            "selector_feature_count": selector_feature_count,
            "label_count": label_count,
            "scope": "dfl_official_global_panel_candidate_value_label_panel_v3_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return label_panel_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_candidate_value_dfl_v3_frame(
    context,
    config: DflOfficialGlobalPanelCandidateValueDflV3AssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v3_frame: pl.DataFrame,
    dfl_official_global_panel_candidate_value_label_panel_v3_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Candidate-level value scorer selected on prior anchors with V2+ fallback."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    model_frame = build_dfl_candidate_value_dfl_v3_frame(
        dfl_official_global_panel_schedule_candidate_library_v3_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_frame,
        dfl_official_global_panel_candidate_value_label_panel_v3_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_v2_plus=(
            config.min_prior_mean_improvement_ratio_vs_v2_plus
        ),
        pairwise_loss_weight=config.pairwise_loss_weight,
    )
    _add_metadata(
        context,
        {
            "rows": model_frame.height,
            "tenant_count": model_frame.select("tenant_id").n_unique()
            if model_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "fallback_rows": model_frame.filter(pl.col("fallback_to_v2_plus")).height
            if model_frame.height
            else 0,
            "scope": "dfl_official_global_panel_candidate_value_dfl_v3_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame(
    context,
    config: DflOfficialGlobalPanelCandidateValueDflV3AssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v3_frame: pl.DataFrame,
    dfl_official_global_panel_candidate_value_dfl_v3_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict-score candidate-value DFL v3 against frozen V2+."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_candidate_value_dfl_v3_strict_lp_benchmark_frame(
        dfl_official_global_panel_schedule_candidate_library_v3_frame,
        dfl_official_global_panel_candidate_value_dfl_v3_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_candidate_value_dfl_v3_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "strategy_kind": CANDIDATE_VALUE_DFL_V3_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "best_source_model_name": gate.metrics.get("best_source_model_name"),
            "offline_strategy_replacement_passed": gate.metrics.get(
                "offline_strategy_replacement_passed",
                False,
            ),
            "market_execution_enabled": False,
            "scope": "dfl_official_global_panel_candidate_value_dfl_v3_strict_gate_not_full_dfl",
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_frame(
    context,
    dfl_official_global_panel_candidate_value_label_panel_v3_frame: pl.DataFrame,
    dfl_official_global_panel_candidate_value_dfl_v3_frame: pl.DataFrame,
    dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Analysis-only audit of why V3 did or did not beat frozen V2+."""

    audit_frame = build_dfl_candidate_value_dfl_v3_failure_audit_frame(
        dfl_official_global_panel_candidate_value_label_panel_v3_frame,
        dfl_official_global_panel_candidate_value_dfl_v3_frame,
        dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame,
    )
    _add_metadata(
        context,
        {
            "rows": audit_frame.height,
            "source_model_count": audit_frame.select("source_model_name").n_unique()
            if audit_frame.height
            else 0,
            "candidate_family_count": audit_frame.select("candidate_family").n_unique()
            if audit_frame.height
            else 0,
            "scope": "dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_v2_v3_plateau_autopsy_frame(
    context,
    dfl_official_global_panel_schedule_candidate_library_v3_frame: pl.DataFrame,
    dfl_official_global_panel_candidate_value_label_panel_v3_frame: pl.DataFrame,
    dfl_official_global_panel_candidate_value_dfl_v3_frame: pl.DataFrame,
    dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Classify why Candidate-Value DFL v3 did not replace frozen V2+."""

    autopsy_frame = build_dfl_v2_v3_plateau_autopsy_frame(
        dfl_official_global_panel_schedule_candidate_library_v3_frame,
        dfl_official_global_panel_candidate_value_label_panel_v3_frame,
        dfl_official_global_panel_candidate_value_dfl_v3_frame,
        dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame,
    )
    _add_metadata(
        context,
        {
            "rows": autopsy_frame.height,
            "source_model_count": autopsy_frame.select("source_model_name").n_unique()
            if autopsy_frame.height
            else 0,
            "plateau_cause_count": autopsy_frame.select("plateau_cause").n_unique()
            if autopsy_frame.height
            else 0,
            "scope": "dfl_official_global_panel_v2_v3_plateau_autopsy_not_full_dfl",
            "market_execution_enabled": False,
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
        ml_stage="diagnostics",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_plateau_data_quality_audit_frame(
    context,
    dfl_official_global_panel_schedule_candidate_library_v3_frame: pl.DataFrame,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    dfl_official_global_panel_v2_v3_plateau_autopsy_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Audit whether the plateau points to missing data/context before new DT work."""

    audit_frame = build_dfl_plateau_data_quality_audit_frame(
        dfl_official_global_panel_schedule_candidate_library_v3_frame,
        real_data_benchmark_silver_feature_frame,
        dfl_official_global_panel_v2_v3_plateau_autopsy_frame,
    )
    _add_metadata(
        context,
        {
            "rows": audit_frame.height,
            "gap_count": audit_frame.filter(pl.col("audit_status") != "ready").height
            if audit_frame.height
            else 0,
            "scope": "dfl_official_global_panel_plateau_data_quality_audit_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_candidate_library_v4_frame(
    context,
    config: DflOfficialGlobalPanelCandidateValueDflV4AssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v3_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Expand V3 with stronger plateau-breaker candidate schedules."""

    library_frame = build_dfl_schedule_candidate_library_v4_frame(
        dfl_official_global_panel_schedule_candidate_library_v3_frame,
        quantile_risk_spread_scales=_float_csv_values(
            config.quantile_risk_spread_scales_csv,
            field_name="quantile_risk_spread_scales_csv",
        ),
        block_peak_delta_uah_mwh=config.block_peak_delta_uah_mwh,
        terminal_reserve_shift_uah_mwh=config.terminal_reserve_shift_uah_mwh,
        spread_volatility_scale=config.spread_volatility_scale,
        tenant_sweep_spread_scales=_float_csv_values(
            config.tenant_sweep_spread_scales_csv,
            field_name="tenant_sweep_spread_scales_csv",
        ),
        include_train_oracle_neighborhood=config.include_train_oracle_neighborhood,
        max_train_generation_anchor_count_per_tenant=(
            config.max_train_generation_anchor_count_per_tenant
        ),
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_candidate_library_v3_frame
        ),
    )
    _add_metadata(
        context,
        {
            "rows": library_frame.height,
            "tenant_count": library_frame.select("tenant_id").n_unique()
            if library_frame.height
            else 0,
            "source_model_count": len(
                _forecast_model_names(config.forecast_model_names_csv)
            ),
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
            if library_frame.height
            else 0,
            "scope": "dfl_official_global_panel_schedule_candidate_library_v4_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_candidate_value_label_panel_v4_frame(
    context,
    dfl_official_global_panel_schedule_candidate_library_v4_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-safe V4 candidate features and realized value labels."""

    label_panel_frame = build_dfl_candidate_value_label_panel_v4_frame(
        dfl_official_global_panel_schedule_candidate_library_v4_frame
    )
    _add_metadata(
        context,
        {
            "rows": label_panel_frame.height,
            "selector_feature_count": len(
                [
                    column
                    for column in label_panel_frame.columns
                    if column.startswith("selector_feature_")
                ]
            ),
            "label_count": len(
                [
                    column
                    for column in label_panel_frame.columns
                    if column.startswith("label_")
                ]
            ),
            "scope": "dfl_official_global_panel_candidate_value_label_panel_v4_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return label_panel_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_candidate_value_dfl_v4_frame(
    context,
    config: DflOfficialGlobalPanelCandidateValueDflV4AssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v4_frame: pl.DataFrame,
    dfl_official_global_panel_candidate_value_label_panel_v4_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Candidate-level V4 value scorer selected on prior anchors with V2+ fallback."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    model_frame = build_dfl_candidate_value_dfl_v4_frame(
        dfl_official_global_panel_schedule_candidate_library_v4_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_frame,
        dfl_official_global_panel_candidate_value_label_panel_v4_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_v2_plus=(
            config.min_prior_mean_improvement_ratio_vs_v2_plus
        ),
        ridge_l2=config.ridge_l2,
    )
    _add_metadata(
        context,
        {
            "rows": model_frame.height,
            "source_model_count": len(source_model_names),
            "fallback_rows": model_frame.filter(pl.col("fallback_to_v2_plus")).height
            if model_frame.height
            else 0,
            "scope": "dfl_official_global_panel_candidate_value_dfl_v4_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_candidate_value_dfl_v4_strict_lp_benchmark_frame(
    context,
    config: DflOfficialGlobalPanelCandidateValueDflV4AssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v4_frame: pl.DataFrame,
    dfl_official_global_panel_candidate_value_dfl_v4_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict-score candidate-value DFL v4 against frozen Ukrainian-only V2+."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_candidate_value_dfl_v4_strict_lp_benchmark_frame(
        dfl_official_global_panel_schedule_candidate_library_v4_frame,
        dfl_official_global_panel_candidate_value_dfl_v4_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_candidate_value_dfl_v4_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "source_model_count": len(source_model_names),
            "strategy_kind": CANDIDATE_VALUE_DFL_V4_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_replacement_passed": gate.metrics.get(
                "offline_strategy_replacement_passed",
                False,
            ),
            "market_execution_enabled": False,
            "scope": "dfl_official_global_panel_candidate_value_dfl_v4_strict_gate_not_full_dfl",
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_point_in_time_context_repair_audit_frame(
    context,
    dfl_official_global_panel_schedule_candidate_library_v4_frame: pl.DataFrame,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    dfl_official_global_panel_v2_v3_plateau_autopsy_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Report exact point-in-time context blockers by tenant/source/anchor."""

    audit_frame = build_dfl_point_in_time_context_repair_audit_frame(
        dfl_official_global_panel_schedule_candidate_library_v4_frame,
        real_data_benchmark_silver_feature_frame,
        dfl_official_global_panel_v2_v3_plateau_autopsy_frame,
    )
    _add_metadata(
        context,
        {
            "rows": audit_frame.height,
            "blocker_count": audit_frame.select("blocker").n_unique()
            if audit_frame.height
            else 0,
            "missing_context_rows": audit_frame.filter(
                pl.col("blocker").str.starts_with("missing_")
            ).height
            if audit_frame.height
            else 0,
            "scope": "dfl_point_in_time_context_repair_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_point_in_time_context_feature_panel_frame(
    context,
    dfl_official_global_panel_schedule_candidate_library_v4_frame: pl.DataFrame,
    dfl_point_in_time_context_repair_audit_frame: pl.DataFrame,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build prior-only Ukrainian context features for context-aware V5 selection."""

    panel_frame = build_dfl_point_in_time_context_feature_panel_frame(
        dfl_official_global_panel_schedule_candidate_library_v4_frame,
        dfl_point_in_time_context_repair_audit_frame,
        real_data_benchmark_silver_feature_frame,
    )
    _add_metadata(
        context,
        {
            "rows": panel_frame.height,
            "selector_feature_count": len(
                [
                    column
                    for column in panel_frame.columns
                    if column.startswith("selector_feature_")
                ]
            ),
            "label_count": len(
                [
                    column
                    for column in panel_frame.columns
                    if column.startswith("label_")
                ]
            ),
            "scope": "dfl_point_in_time_context_feature_panel_not_full_dfl",
            "market_execution_enabled": False,
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
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_context_enriched_schedule_candidate_library_v5_frame(
    context,
    dfl_official_global_panel_schedule_candidate_library_v4_frame: pl.DataFrame,
    dfl_point_in_time_context_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Attach point-in-time context features to V4 candidate schedules."""

    library_frame = build_dfl_context_enriched_schedule_candidate_library_v5_frame(
        dfl_official_global_panel_schedule_candidate_library_v4_frame,
        dfl_point_in_time_context_feature_panel_frame,
    )
    _add_metadata(
        context,
        {
            "rows": library_frame.height,
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
            if library_frame.height
            else 0,
            "scope": "dfl_context_enriched_schedule_candidate_library_v5_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_context_enriched_candidate_value_label_panel_v5_frame(
    context,
    dfl_context_enriched_schedule_candidate_library_v5_frame: pl.DataFrame,
    dfl_point_in_time_context_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build V5 context-enriched prior features and realized value labels."""

    label_panel_frame = build_dfl_context_enriched_candidate_value_label_panel_v5_frame(
        dfl_context_enriched_schedule_candidate_library_v5_frame,
        dfl_point_in_time_context_feature_panel_frame,
    )
    _add_metadata(
        context,
        {
            "rows": label_panel_frame.height,
            "selector_feature_count": len(
                [
                    column
                    for column in label_panel_frame.columns
                    if column.startswith("selector_feature_")
                ]
            ),
            "label_count": len(
                [
                    column
                    for column in label_panel_frame.columns
                    if column.startswith("label_")
                ]
            ),
            "scope": "dfl_context_enriched_candidate_value_label_panel_v5_not_full_dfl",
            "market_execution_enabled": False,
            "not_market_execution": True,
        },
    )
    return label_panel_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_context_enriched_candidate_value_dfl_v5_frame(
    context,
    config: DflContextEnrichedCandidateValueDflV5AssetConfig,
    dfl_context_enriched_schedule_candidate_library_v5_frame: pl.DataFrame,
    dfl_context_enriched_candidate_value_label_panel_v5_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Train context-enriched candidate-value DFL v5 with frozen V2+ fallback."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    model_frame = build_dfl_context_enriched_candidate_value_dfl_v5_frame(
        dfl_context_enriched_schedule_candidate_library_v5_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_frame,
        dfl_context_enriched_candidate_value_label_panel_v5_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_v2_plus=(
            config.min_prior_mean_improvement_ratio_vs_v2_plus
        ),
        ridge_l2=config.ridge_l2,
    )
    _add_metadata(
        context,
        {
            "rows": model_frame.height,
            "source_model_count": len(source_model_names),
            "fallback_rows": model_frame.filter(pl.col("fallback_to_v2_plus")).height
            if model_frame.height
            else 0,
            "scope": "dfl_context_enriched_candidate_value_dfl_v5_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame(
    context,
    config: DflContextEnrichedCandidateValueDflV5AssetConfig,
    dfl_context_enriched_schedule_candidate_library_v5_frame: pl.DataFrame,
    dfl_context_enriched_candidate_value_dfl_v5_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Strict-score context-enriched DFL v5 against frozen Ukrainian-only V2+."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame(
        dfl_context_enriched_schedule_candidate_library_v5_frame,
        dfl_context_enriched_candidate_value_dfl_v5_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_context_enriched_candidate_value_dfl_v5_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "source_model_count": len(source_model_names),
            "strategy_kind": CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "offline_strategy_replacement_passed": gate.metrics.get(
                "offline_strategy_replacement_passed",
                False,
            ),
            "market_execution_enabled": False,
            "scope": "dfl_context_enriched_candidate_value_dfl_v5_strict_gate_not_full_dfl",
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV2PlusRobustnessAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling-window robustness for global-panel schedule/value V2+ screening."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    robustness_frame = build_dfl_schedule_value_learner_v2_plus_robustness_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_prior_anchors_before_window=config.min_prior_anchors_before_window,
        min_robust_passing_windows=config.min_robust_passing_windows,
        min_validation_tenant_anchor_count_per_source_model=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_prior_mean_improvement_ratio_vs_v2=(
            config.min_prior_mean_improvement_ratio_vs_v2
        ),
    )
    gate = evaluate_dfl_schedule_value_learner_v2_plus_robustness_gate(
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
                "robust_source_model_names", []
            ),
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "production_promote": False,
            "market_execution_enabled": False,
            "scope": (
                "dfl_official_global_panel_schedule_value_v2_plus_robustness_"
                "screen_not_full_dfl"
            ),
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
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_v2_plus_trajectory_dataset_frame(
    context,
    config: DflOfficialGlobalPanelV2PlusDflDtBridgeAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Step-level official V2+-teacher trajectories for residual DFL/offline DT."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    trajectory_frame = build_official_v2_plus_trajectory_dataset_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
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
            "final_validation_anchor_count_per_tenant": (
                config.final_validation_anchor_count_per_tenant
            ),
            "scope": "dfl_official_global_panel_v2_plus_trajectory_dataset_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame(
    context,
    config: DflOfficialGlobalPanelV2PlusDflDtBridgeAssetConfig,
    dfl_official_global_panel_v2_plus_trajectory_dataset_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Residual schedule/value selector over official V2+-teacher trajectories."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    model_frame = build_official_v2_plus_residual_schedule_value_model_frame(
        dfl_official_global_panel_v2_plus_trajectory_dataset_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
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
            "scope": "dfl_official_global_panel_v2_plus_residual_schedule_value_not_full_dfl",
            "market_execution_enabled": False,
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
        ml_stage="selection",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_v2_plus_offline_dt_candidate_frame(
    context,
    config: DflOfficialGlobalPanelV2PlusDflDtBridgeAssetConfig,
    dfl_official_global_panel_v2_plus_trajectory_dataset_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Tiny offline DT candidate over official V2+-teacher trajectories."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    candidate_frame = build_official_v2_plus_offline_dt_candidate_frame(
        dfl_official_global_panel_v2_plus_trajectory_dataset_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
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
            "scope": "dfl_official_global_panel_v2_plus_offline_dt_candidate_not_full_dfl",
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame(
    context,
    config: DflOfficialGlobalPanelV2PlusDflDtBridgeAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame: (
        pl.DataFrame
    ),
    dfl_official_global_panel_v2_plus_offline_dt_candidate_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Compare official V2+-teacher residual/DT challengers against V2+."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_official_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame,
        dfl_official_global_panel_v2_plus_offline_dt_candidate_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        source_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_confidence_improvement_ratio=config.min_confidence_improvement_ratio,
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_v2_plus_dfl_dt_bridge_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_tenant_count=config.min_tenant_count,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            config.min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            config.min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "strategy_kind": (
                DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND
            ),
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "best_challenger_role": gate.metrics.get("best_challenger_role"),
            "best_source_model_name": gate.metrics.get("best_source_model_name"),
            "offline_strategy_challenger_passed": gate.metrics.get(
                "offline_strategy_challenger_passed",
                False,
            ),
            "v2_plus_headline_baseline": V2_PLUS_HEADLINE_BASELINE_METRICS,
            "production_promote": False,
            "market_execution_enabled": False,
            "scope": "dfl_official_global_panel_v2_plus_dfl_dt_bridge_not_full_dfl",
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_v2_plus_bridge_failure_audit_frame(
    context,
    dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Explain why official V2+-teacher residual/DT challengers lose to V2+."""

    audit_frame = build_dfl_official_v2_plus_bridge_failure_audit_frame(
        dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame
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
            "failure_modes": sorted(
                str(mode)
                for mode in audit_frame["analysis_only_failure_mode"].unique().to_list()
            )
            if audit_frame.height
            else [],
            "scope": DFL_OFFICIAL_V2_PLUS_BRIDGE_FAILURE_AUDIT_CLAIM_SCOPE,
            "market_execution_enabled": False,
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_market_coupled_schedule_value_learner_v2_plus_frame(
    context,
    config: DflMarketCoupledScheduleValueLearnerV2PlusAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_frame: pl.DataFrame,
    official_forecast_exogenous_feature_route_frame: pl.DataFrame,
    entsoe_poland_lagged_feature_candidate_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Experimental Ukrainian-plus-Poland selector over V2+ candidates."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    learner_frame = build_dfl_market_coupled_schedule_value_learner_v2_plus_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        dfl_official_global_panel_schedule_value_learner_v2_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_frame,
        official_forecast_exogenous_feature_route_frame,
        entsoe_poland_lagged_feature_candidate_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus=(
            config.min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus
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
            "fallback_rows": learner_frame.filter(
                pl.col("fallback_to_ukrainian_v2_plus")
            ).height
            if learner_frame.height
            else 0,
            "market_execution_enabled": False,
            "scope": "dfl_market_coupled_schedule_value_learner_v2_plus_not_full_dfl",
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
    context,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_frame: pl.DataFrame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_frame: pl.DataFrame,
    dfl_market_coupled_schedule_value_learner_v2_plus_frame: pl.DataFrame,
    entsoe_poland_lagged_feature_candidate_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle rows for the experimental Poland B variant."""

    strict_frame = build_dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        dfl_official_global_panel_schedule_value_learner_v2_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_frame,
        dfl_market_coupled_schedule_value_learner_v2_plus_frame,
        entsoe_poland_lagged_feature_candidate_frame=(
            entsoe_poland_lagged_feature_candidate_frame
        ),
        generated_at=_latest_generated_at(
            dfl_official_global_panel_schedule_candidate_library_v2_plus_frame
        ),
    )
    if strict_frame.height:
        get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "strategy_kind": DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND,
            "market_execution_enabled": False,
            "scope": "dfl_market_coupled_schedule_value_learner_v2_plus_strict_gate_not_full_dfl",
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_market_coupled_schedule_value_learner_v2_plus_robustness_frame(
    context,
    config: DflMarketCoupledScheduleValueLearnerV2PlusRobustnessAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    official_forecast_exogenous_feature_route_frame: pl.DataFrame,
    entsoe_poland_lagged_feature_candidate_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling robustness rows for the experimental Poland B variant."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    robustness_frame = build_dfl_market_coupled_schedule_value_learner_v2_plus_robustness_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
        official_forecast_exogenous_feature_route_frame,
        entsoe_poland_lagged_feature_candidate_frame,
        tenant_ids=_csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv"),
        forecast_model_names=source_model_names,
        validation_window_count=config.validation_window_count,
        validation_anchor_count=config.validation_anchor_count,
        min_prior_anchors_before_window=config.min_prior_anchors_before_window,
        min_robust_passing_windows=config.min_robust_passing_windows,
        min_validation_tenant_anchor_count_per_source_model=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus=(
            config.min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus
        ),
    )
    _add_metadata(
        context,
        {
            "rows": robustness_frame.height,
            "source_model_count": len(source_model_names),
            "passing_windows": int(
                robustness_frame.filter(pl.col("v2_plus_window_passed")).height
            )
            if robustness_frame.height
            else 0,
            "market_execution_enabled": False,
            "scope": "dfl_market_coupled_schedule_value_learner_v2_plus_robustness_not_full_dfl",
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
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_market_coupling_v2_plus_ablation_frame(
    context,
    config: DflMarketCouplingV2PlusAblationAssetConfig,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame: (
        pl.DataFrame
    ),
    official_forecast_exogenous_feature_route_frame: pl.DataFrame,
    dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_market_coupled_schedule_value_learner_v2_plus_robustness_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Compare Ukrainian-only V2+ against governed neighbor-market features.

    When no external feature is approved by governance, the asset emits a
    blocked readiness row and intentionally does not train a B variant.
    """

    source_model_names = _forecast_model_names(config.source_model_names_csv)
    ablation_frame = build_dfl_market_coupling_v2_plus_ablation_frame(
        dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
        dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame,
        official_forecast_exogenous_feature_route_frame,
        market_coupled_strict_frame=(
            dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
        market_coupled_robustness_frame=(
            dfl_market_coupled_schedule_value_learner_v2_plus_robustness_frame
        ),
        source_model_names=source_model_names,
        min_tenant_count=config.min_tenant_count,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_window_count=config.min_window_count,
    )
    _add_metadata(
        context,
        {
            "rows": ablation_frame.height,
            "source_model_count": len(source_model_names),
            "ablation_statuses": sorted(
                ablation_frame["ablation_status"].unique().to_list()
            )
            if ablation_frame.height
            else [],
            "approved_external_feature_columns": sorted(
                {
                    value
                    for row in ablation_frame.iter_rows(named=True)
                    for value in str(
                        row["approved_external_feature_columns_csv"]
                    ).split(",")
                    if value
                }
            ),
            "passed_rows": ablation_frame.filter(pl.col("ablation_passed")).height
            if ablation_frame.height
            else 0,
            "market_execution_enabled": False,
            "scope": "dfl_market_coupling_v2_plus_ablation_not_market_execution",
            "not_market_execution": True,
        },
    )
    return ablation_frame


@dg.asset(
    group_name=taxonomy.GOLD_DFL_TRAINING,
    tags=taxonomy.asset_tags(
        medallion="gold",
        domain="dfl_research",
        elt_stage="publish",
        ml_stage="evaluation",
        evidence_scope="not_market_execution",
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_value_learner_v2_robustness_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueLearnerV2RobustnessAssetConfig,
    dfl_official_global_panel_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling-window robustness for global-panel schedule/value screening."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    robustness_frame = build_dfl_schedule_value_learner_v2_robustness_frame(
        dfl_official_global_panel_schedule_candidate_library_v2_frame,
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
            "robust_source_model_names": gate.metrics.get(
                "robust_source_model_names", []
            ),
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "production_promote": False,
            "scope": (
                "dfl_official_global_panel_schedule_value_v2_robustness_"
                "screen_not_full_dfl"
            ),
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
        backend="official_global_panel_nbeatsx",
        market_venue="DAM",
    ),
)
def dfl_official_global_panel_schedule_value_production_gate_frame(
    context,
    config: DflOfficialGlobalPanelScheduleValueProductionGateAssetConfig,
    dfl_official_global_panel_schedule_value_learner_v2_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
    dfl_official_global_panel_schedule_value_learner_v2_robustness_frame: (
        pl.DataFrame
    ),
) -> pl.DataFrame:
    """Offline promotion/fallback decision for global-panel schedule/value screen."""

    source_model_names = _forecast_model_names(config.source_model_names_csv)
    gate_frame = build_dfl_schedule_value_production_gate_frame(
        dfl_official_global_panel_schedule_value_learner_v2_strict_lp_benchmark_frame,
        dfl_official_global_panel_schedule_value_learner_v2_robustness_frame,
        source_model_names=source_model_names,
        min_tenant_count=config.min_tenant_count,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
        min_mean_regret_improvement_ratio=config.min_mean_regret_improvement_ratio,
        min_rolling_window_count=config.min_rolling_window_count,
        min_rolling_strict_pass_windows=config.min_rolling_strict_pass_windows,
    )
    generated_at = _latest_generated_at(
        dfl_official_global_panel_schedule_value_learner_v2_strict_lp_benchmark_frame
    )
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
            "promoted_source_model_names": gate.metrics.get(
                "promoted_source_model_names", []
            ),
            "production_promote_count": gate.metrics.get("production_promote_count", 0),
            "promotion_gate_decision": gate.decision,
            "promotion_gate_description": gate.description,
            "market_execution_enabled": False,
            "scope": "dfl_official_global_panel_schedule_value_gate_not_market_execution",
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
    strict_rows = strict_frame.filter(
        pl.col("forecast_model_name") == "strict_similar_day"
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "candidate_rows": candidate_rows.height,
            "strict_control_rows": strict_rows.height,
            "value_weight_scale_uah": config.value_weight_scale_uah,
            "mean_candidate_regret_uah": candidate_rows.select("regret_uah")
            .mean()
            .item()
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
            "tenant_count": panel_frame.select("tenant_id").n_unique()
            if panel_frame.height
            else 0,
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
    v2_rows = strict_panel_frame.filter(
        pl.col("forecast_model_name").str.starts_with("offline_dfl_panel_v2_")
    )
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
        spread_scale_grid=_float_csv_values(
            config.spread_scale_grid_csv, field_name="spread_scale_grid_csv"
        ),
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
    v3_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("offline_dfl_decision_target_v3_")
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
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
    v4_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("offline_dfl_action_target_v4_")
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
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
            "tenant_count": candidate_panel.select("tenant_id").n_unique()
            if candidate_panel.height
            else 0,
            "source_model_count": len(source_model_names),
            "candidate_family_count": candidate_panel.select(
                "candidate_family"
            ).n_unique()
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
            "tenant_count": selector_frame.select("tenant_id").n_unique()
            if selector_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "development_gate_rows": selector_frame.filter(
                pl.col("development_gate_passed")
            ).height
            if selector_frame.height
            else 0,
            "production_promotion_rows": selector_frame.filter(
                pl.col("production_promotion_passed")
            ).height
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
        pl.col("forecast_model_name").str.starts_with(
            "dfl_trajectory_value_selector_v1_"
        )
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
            "strategy_kind": TRAJECTORY_VALUE_SELECTOR_STRICT_LP_STRATEGY_KIND,
            "promotion_gate_decision": promotion_gate.decision,
            "promotion_gate_description": promotion_gate.description,
            "development_gate_passed": promotion_gate.metrics.get(
                "development_gate_passed", False
            ),
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
            "tenant_count": library_frame.select("tenant_id").n_unique()
            if library_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
            if library_frame.height
            else 0,
            "final_holdout_rows": library_frame.filter(
                pl.col("split_name") == "final_holdout"
            ).height
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
            "tenant_count": ranker_frame.select("tenant_id").n_unique()
            if ranker_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "weight_profile_count": ranker_frame.select(
                "selected_weight_profile_name"
            ).n_unique()
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
        pl.col("forecast_model_name").str.starts_with(
            "dfl_trajectory_feature_ranker_v1_"
        )
    )
    _add_metadata(
        context,
        {
            "rows": strict_frame.height,
            "tenant_count": strict_frame.select("tenant_id").n_unique()
            if strict_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "ranker_validation_tenant_anchor_count": ranker_rows.height,
            "strategy_kind": DFL_TRAJECTORY_FEATURE_RANKER_STRICT_LP_STRATEGY_KIND,
            "promotion_gate_decision": promotion_gate.decision,
            "promotion_gate_description": promotion_gate.description,
            "development_gate_passed": promotion_gate.metrics.get(
                "development_gate_passed", False
            ),
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
        blend_weights=_float_csv_values(
            config.blend_weights_csv, field_name="blend_weights_csv"
        ),
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
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
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
        ml_stage="training_data",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_schedule_candidate_library_v2_plus_frame(
    context,
    config: DflScheduleCandidateLibraryV2PlusAssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Expanded V2+ candidate library for compact/offline schedule evidence."""

    library_frame = build_dfl_schedule_candidate_library_v2_plus_frame(
        dfl_schedule_candidate_library_v2_frame,
        rank_perturbation_delta_uah_mwh=config.rank_perturbation_delta_uah_mwh,
        robust_spread_scales=_float_csv_values(
            config.robust_spread_scales_csv,
            field_name="robust_spread_scales_csv",
        ),
        strict_neighborhood_shift_hours=_int_csv_values(
            config.strict_neighborhood_shift_hours_csv,
            field_name="strict_neighborhood_shift_hours_csv",
        ),
        block_reconcile_hours=_int_csv_values(
            config.block_reconcile_hours_csv,
            field_name="block_reconcile_hours_csv",
        ),
        terminal_target_shift_uah_mwh=config.terminal_target_shift_uah_mwh,
        generated_at=_latest_generated_at(dfl_schedule_candidate_library_v2_frame),
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
            "candidate_family_count": library_frame.select(
                "candidate_family"
            ).n_unique()
            if library_frame.height
            else 0,
            "scope": "dfl_schedule_candidate_library_v2_plus_not_full_dfl",
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
            "tenant_count": autopsy_frame.select("tenant_id").n_unique()
            if autopsy_frame.height
            else 0,
            "source_model_count": autopsy_frame.select("source_model_name").n_unique()
            if autopsy_frame.height
            else 0,
            "strict_failure_opportunity_rows": autopsy_frame.filter(
                pl.col("recommended_next_action")
                == "train_selector_to_detect_strict_failure"
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
            "final_switch_count": selector_frame.select("final_switch_count")
            .sum()
            .item()
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
            "failure_clusters": sorted(
                audit_frame["failure_cluster"].unique().to_list()
            )
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
            "final_switch_count": selector_frame.select("final_switch_count")
            .sum()
            .item()
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
    strict_frame = (
        build_dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame(
            dfl_schedule_candidate_library_v2_frame,
            dfl_feature_aware_strict_failure_selector_frame,
            dfl_strict_failure_prior_feature_panel_frame,
            generated_at=_latest_generated_at(dfl_schedule_candidate_library_v2_frame),
        )
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
        pl.col("forecast_model_name").str.starts_with(
            "dfl_feature_aware_strict_failure_selector_v2_"
        )
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
            "allow_challenger_rows": selector_frame.filter(
                pl.col("allow_challenger")
            ).height
            if selector_frame.height
            else 0,
            "strict_default_rows": selector_frame.filter(
                ~pl.col("allow_challenger")
            ).height
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
        pl.col("forecast_model_name").str.starts_with(
            "dfl_regime_gated_tft_selector_v2_"
        )
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
            "final_validation_anchor_count": panel_frame.select(
                pl.sum("final_validation_anchor_count")
            ).item()
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
    source_model_names = tuple(
        sorted(strict_frame["source_model_name"].unique().to_list())
    )
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
            "production_promote": promotion_gate.metrics.get(
                "production_promote", False
            ),
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
            "robust_source_model_names": gate.metrics.get(
                "robust_source_model_names", []
            ),
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
            "profile_names": sorted(
                learner_frame["selected_weight_profile_name"].unique().to_list()
            )
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
            "development_gate_passed": gate.metrics.get(
                "development_gate_passed", False
            ),
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
        ml_stage="diagnostics",
        evidence_scope="not_market_execution",
        market_venue="DAM",
    ),
)
def dfl_schedule_value_regret_decomposition_frame(
    context,
    dfl_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_schedule_value_learner_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Regret decomposition for frozen V2 remaining losses."""

    decomposition_frame = build_dfl_schedule_value_regret_decomposition_frame(
        dfl_schedule_candidate_library_v2_plus_frame,
        dfl_schedule_value_learner_v2_frame,
    )
    _add_metadata(
        context,
        {
            "rows": decomposition_frame.height,
            "failure_modes": sorted(
                decomposition_frame["failure_mode"].unique().to_list()
            )
            if decomposition_frame.height
            else [],
            "scope": "dfl_schedule_value_regret_decomposition_not_full_dfl",
            "not_market_execution": True,
        },
    )
    return decomposition_frame


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
def dfl_schedule_value_learner_v2_plus_frame(
    context,
    config: DflScheduleValueLearnerV2PlusAssetConfig,
    dfl_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_schedule_value_learner_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only V2+ selector with frozen V2 fallback."""

    tenant_ids = _csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv")
    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    learner_frame = build_dfl_schedule_value_learner_v2_plus_frame(
        dfl_schedule_candidate_library_v2_plus_frame,
        dfl_schedule_value_learner_v2_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        min_prior_mean_improvement_ratio_vs_v2=(
            config.min_prior_mean_improvement_ratio_vs_v2
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
            "fallback_rows": learner_frame.filter(pl.col("fallback_to_v2")).height
            if learner_frame.height
            else 0,
            "scope": "dfl_schedule_value_learner_v2_plus_not_full_dfl",
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
def dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
    context,
    config: DflScheduleValueLearnerV2PlusAssetConfig,
    dfl_schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    dfl_schedule_value_learner_v2_frame: pl.DataFrame,
    dfl_schedule_value_learner_v2_plus_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle evidence for V2+ with frozen V2 fallback."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        dfl_schedule_candidate_library_v2_plus_frame,
        dfl_schedule_value_learner_v2_plus_frame,
        dfl_schedule_value_learner_v2_frame,
        generated_at=_latest_generated_at(dfl_schedule_candidate_library_v2_plus_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_schedule_value_learner_v2_plus_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
    )
    learner_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with(
            "dfl_schedule_value_learner_v2_plus_"
        )
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
            "strategy_kind": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "development_gate_passed": gate.metrics.get(
                "development_gate_passed", False
            ),
            "production_gate_passed": gate.metrics.get("production_gate_passed", False),
            "market_execution_enabled": False,
            "scope": "dfl_schedule_value_learner_v2_plus_strict_lp_gate_not_full_dfl",
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
def dfl_schedule_value_learner_v3_frame(
    context,
    config: DflScheduleValueLearnerV3AssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Prior-only DFL v3 schedule/value ridge ranker over feasible candidate schedules."""

    tenant_ids = _csv_values(config.tenant_ids_csv, field_name="tenant_ids_csv")
    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    learner_frame = build_dfl_schedule_value_learner_v3_frame(
        dfl_schedule_candidate_library_v2_frame,
        tenant_ids=tenant_ids,
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=(
            config.final_validation_anchor_count_per_tenant
        ),
        ridge_regularization=config.ridge_regularization,
    )
    _add_metadata(
        context,
        {
            "rows": learner_frame.height,
            "tenant_count": learner_frame.select("tenant_id").n_unique()
            if learner_frame.height
            else 0,
            "source_model_count": len(source_model_names),
            "profile_names": sorted(
                learner_frame["selected_weight_profile_name"].unique().to_list()
            )
            if learner_frame.height
            else [],
            "scope": "dfl_schedule_value_learner_v3_not_full_dfl",
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
def dfl_schedule_value_learner_v3_strict_lp_benchmark_frame(
    context,
    config: DflScheduleValueLearnerV3AssetConfig,
    dfl_schedule_candidate_library_v2_frame: pl.DataFrame,
    dfl_schedule_value_learner_v2_frame: pl.DataFrame,
    dfl_schedule_value_learner_v3_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Strict LP/oracle evidence for the schedule/value learner v3."""

    source_model_names = _forecast_model_names(config.forecast_model_names_csv)
    strict_frame = build_dfl_schedule_value_learner_v3_strict_lp_benchmark_frame(
        dfl_schedule_candidate_library_v2_frame,
        dfl_schedule_value_learner_v3_frame,
        dfl_schedule_value_learner_v2_frame,
        generated_at=_latest_generated_at(dfl_schedule_candidate_library_v2_frame),
    )
    get_strategy_evaluation_store().upsert_evaluation_frame(strict_frame)
    gate = evaluate_dfl_schedule_value_learner_v3_gate(
        strict_frame,
        source_model_names=source_model_names,
        min_validation_tenant_anchor_count=(
            config.min_validation_tenant_anchor_count_per_source_model
        ),
    )
    learner_rows = strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("dfl_schedule_value_learner_v3_")
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
            "strategy_kind": DFL_SCHEDULE_VALUE_LEARNER_V3_STRICT_LP_STRATEGY_KIND,
            "gate_decision": gate.decision,
            "gate_description": gate.description,
            "development_gate_passed": gate.metrics.get(
                "development_gate_passed", False
            ),
            "production_gate_passed": gate.metrics.get("production_gate_passed", False),
            "market_execution_enabled": False,
            "scope": "dfl_schedule_value_learner_v3_strict_lp_gate_not_full_dfl",
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
            "tenant_count": calibration_frame.select("tenant_id").n_unique()
            if calibration_frame.height
            else 0,
            "source_model_count": calibration_frame.select(
                "source_forecast_model_name"
            ).n_unique()
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
            "tenant_count": benchmark_frame.select("tenant_id").n_unique()
            if benchmark_frame.height
            else 0,
            "anchor_count": benchmark_frame.select("anchor_timestamp").n_unique()
            if benchmark_frame.height
            else 0,
            "model_count": benchmark_frame.select("forecast_model_name").n_unique()
            if benchmark_frame.height
            else 0,
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
            "source_model_count": calibration_frame.select(
                "source_forecast_model_name"
            ).n_unique()
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
            "tenant_count": benchmark_frame.select("tenant_id").n_unique()
            if benchmark_frame.height
            else 0,
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
    entsoe_neighbor_market_sample_audit_frame,
    entsoe_neighbor_market_feature_candidate_frame,
    nbu_eur_uah_fx_metadata_frame,
    entsoe_poland_lagged_feature_candidate_frame,
    poland_neighbor_market_snapshot_feature_candidate_frame,
    poland_neighbor_market_hourly_feature_frame,
    entsoe_poland_governance_closure_frame,
    entsoe_poland_feature_governance_frame,
    entsoe_neighbor_market_aligned_feature_panel_frame,
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
    dfl_schedule_candidate_library_v2_plus_frame,
    dfl_schedule_value_regret_decomposition_frame,
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
    dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame,
    dfl_source_specific_research_challenger_frame,
    dfl_schedule_value_learner_v2_frame,
    dfl_schedule_value_learner_v2_strict_lp_benchmark_frame,
    dfl_schedule_value_learner_v2_robustness_frame,
    dfl_schedule_value_learner_v3_frame,
    dfl_schedule_value_learner_v3_strict_lp_benchmark_frame,
    dfl_schedule_value_learner_v2_plus_frame,
    dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
    dfl_schedule_value_production_gate_frame,
    dfl_official_schedule_candidate_library_frame,
    dfl_official_schedule_candidate_library_v2_frame,
    dfl_official_schedule_value_learner_v2_frame,
    dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame,
    dfl_official_schedule_value_learner_v2_robustness_frame,
    dfl_official_schedule_value_production_gate_frame,
    dfl_official_global_panel_schedule_candidate_library_frame,
    dfl_official_global_panel_schedule_candidate_library_v2_frame,
    dfl_official_global_panel_schedule_candidate_library_v2_plus_frame,
    dfl_official_global_panel_schedule_value_regret_decomposition_frame,
    dfl_official_global_panel_schedule_value_learner_v2_frame,
    dfl_official_global_panel_schedule_value_learner_v2_strict_lp_benchmark_frame,
    dfl_official_global_panel_schedule_value_learner_v2_robustness_frame,
    dfl_official_global_panel_schedule_value_learner_v3_frame,
    dfl_official_global_panel_schedule_value_learner_v3_strict_lp_benchmark_frame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_frame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_oracle_gap_audit_frame,
    dfl_oracle_gap_safe_switch_label_frame,
    dfl_oracle_gap_safe_switch_feature_panel_frame,
    dfl_oracle_gap_safe_switch_scorer_frame,
    dfl_oracle_gap_safe_switch_strict_lp_benchmark_frame,
    dfl_oracle_gap_safe_switch_rolling_robustness_frame,
    dfl_ua_calendar_publication_context_frame,
    dfl_ua_weather_load_context_frame,
    dfl_ua_grid_event_context_frame,
    dfl_ua_context_oracle_gap_feature_panel_frame,
    dfl_ua_context_safe_switch_separability_audit_frame,
    dfl_ua_context_safe_switch_scorer_frame,
    dfl_ua_context_safe_switch_strict_lp_benchmark_frame,
    dfl_ua_context_safe_switch_rolling_robustness_frame,
    dfl_ua_context_lava_teacher_frame,
    dfl_ua_context_lava_sequence_training_frame,
    dfl_ua_context_lava_candidate_policy_frame,
    dfl_ua_context_lava_strict_lp_benchmark_frame,
    dfl_ua_context_lava_rolling_robustness_frame,
    dfl_v2_plus_learning_limit_audit_frame,
    dfl_expanded_schedule_value_teacher_label_panel_v1_frame,
    dfl_regret_surrogate_forecast_correction_v1_frame,
    dfl_regret_surrogate_candidate_value_v1_frame,
    dfl_regret_surrogate_strict_lp_benchmark_frame,
    dfl_regret_surrogate_rolling_robustness_frame,
    dfl_regret_surrogate_safe_switch_context_audit_frame,
    dfl_regret_surrogate_teacher_label_panel_v2_frame,
    dfl_regret_surrogate_contextual_candidate_value_v2_frame,
    dfl_regret_surrogate_contextual_strict_lp_benchmark_frame,
    dfl_regret_surrogate_contextual_rolling_robustness_frame,
    official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame,
    official_global_panel_poland_lag24_experimental_nbeatsx_horizon_calibration_frame,
    official_global_panel_poland_lag24_experimental_tft_horizon_quantile_calibration_frame,
    official_global_panel_poland_lag24_experimental_horizon_calibrated_strict_lp_benchmark_frame,
    dfl_poland_lag24_calibrated_schedule_candidate_library_frame,
    dfl_poland_lag24_calibrated_schedule_candidate_library_v2_frame,
    dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame,
    dfl_poland_lag24_calibrated_schedule_value_learner_v2_frame,
    dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_frame,
    dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
    dfl_poland_lag24_calibrated_vs_v2_plus_comparison_frame,
    dfl_poland_lag24_prior_tail_risk_veto_frame,
    dfl_poland_lag24_feature_consumption_audit_frame,
    dfl_poland_lag24_candidate_value_label_panel_frame,
    dfl_poland_lag24_candidate_value_ranker_frame,
    dfl_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame,
    dfl_v2_plus_schedule_neighbor_teacher_label_frame,
    dfl_lava_schedule_neighbor_candidate_frame,
    dfl_lava_candidate_value_scorer_frame,
    dfl_lava_candidate_value_strict_lp_benchmark_frame,
    dfl_lava_tail_risk_diagnostic_frame,
    dfl_lava_tail_risk_aware_target_frame,
    dfl_lava_tail_risk_aware_strict_lp_benchmark_frame,
    dfl_lava_tail_risk_safe_switch_scorer_frame,
    dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame,
    dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame,
    dfl_lava_tail_risk_safe_switch_scorer_v2_frame,
    dfl_lava_tail_risk_safe_switch_v2_strict_lp_benchmark_frame,
    dfl_lava_tail_risk_avoidance_label_frame,
    dfl_lava_tail_risk_avoidance_scorer_v3_frame,
    dfl_lava_tail_risk_avoidance_v3_strict_lp_benchmark_frame,
    dfl_lava_schedule_neighbor_dt_training_frame,
    dfl_lava_schedule_neighbor_dt_policy_frame,
    dfl_lava_schedule_neighbor_dt_strict_lp_benchmark_frame,
    dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_robustness_frame,
    dfl_poland_lag24_rolling_vs_frozen_v2_plus_gate_frame,
    dfl_poland_lag24_experimental_schedule_candidate_library_frame,
    dfl_poland_lag24_experimental_schedule_candidate_library_v2_frame,
    dfl_poland_lag24_experimental_schedule_candidate_library_v2_plus_frame,
    dfl_poland_lag24_experimental_schedule_value_learner_v2_frame,
    dfl_poland_lag24_experimental_schedule_value_learner_v2_plus_frame,
    dfl_poland_lag24_experimental_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
    dfl_poland_lag24_experimental_vs_v2_plus_comparison_frame,
    dfl_tft_quantile_schedule_candidate_library_frame,
    dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame,
    dfl_tft_combined_v2_plus_strict_lp_benchmark_frame,
    dfl_tft_calibrated_quantile_schedule_candidate_library_frame,
    dfl_tft_calibrated_augmented_v2_plus_strict_lp_benchmark_frame,
    dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame,
    dfl_nbeatsx_tft_complementarity_audit_frame,
    dfl_nbeatsx_tft_candidate_portfolio_v1_frame,
    dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame,
    dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame,
    dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame,
    dfl_nbeatsx_tft_meta_selector_prior_rolling_robustness_frame,
    dfl_nbeatsx_tft_meta_selector_robustness_frame,
    dfl_official_global_panel_schedule_value_dfl_v2_frame,
    dfl_official_global_panel_schedule_value_dfl_v2_strict_lp_benchmark_frame,
    dfl_official_global_panel_schedule_candidate_library_v3_frame,
    dfl_official_global_panel_candidate_value_label_panel_v3_frame,
    dfl_official_global_panel_candidate_value_dfl_v3_frame,
    dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame,
    dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_frame,
    dfl_official_global_panel_v2_v3_plateau_autopsy_frame,
    dfl_official_global_panel_plateau_data_quality_audit_frame,
    dfl_official_global_panel_schedule_candidate_library_v4_frame,
    dfl_official_global_panel_candidate_value_label_panel_v4_frame,
    dfl_official_global_panel_candidate_value_dfl_v4_frame,
    dfl_official_global_panel_candidate_value_dfl_v4_strict_lp_benchmark_frame,
    dfl_point_in_time_context_repair_audit_frame,
    dfl_point_in_time_context_feature_panel_frame,
    dfl_context_enriched_schedule_candidate_library_v5_frame,
    dfl_context_enriched_candidate_value_label_panel_v5_frame,
    dfl_context_enriched_candidate_value_dfl_v5_frame,
    dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame,
    dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame,
    dfl_official_global_panel_v2_plus_trajectory_dataset_frame,
    dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame,
    dfl_official_global_panel_v2_plus_offline_dt_candidate_frame,
    dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame,
    dfl_official_v2_plus_bridge_failure_audit_frame,
    dfl_market_coupled_schedule_value_learner_v2_plus_frame,
    dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
    dfl_market_coupled_schedule_value_learner_v2_plus_robustness_frame,
    dfl_market_coupling_v2_plus_ablation_frame,
    dfl_official_global_panel_schedule_value_production_gate_frame,
    dfl_production_promotion_gate_frame,
    regret_weighted_forecast_calibration_frame,
    regret_weighted_forecast_strategy_benchmark_frame,
    horizon_regret_weighted_forecast_calibration_frame,
    horizon_regret_weighted_forecast_strategy_benchmark_frame,
    calibrated_value_aware_ensemble_frame,
    forecast_dispatch_sensitivity_frame,
    risk_adjusted_value_gate_frame,
]


def _add_metadata(
    context: dg.AssetExecutionContext | None, metadata: dict[str, Any]
) -> None:
    if context is not None:
        context.add_output_metadata(_normalize_asset_metadata(metadata))


def _normalize_asset_metadata(value: Any) -> Any:
    if isinstance(value, tuple):
        return [_normalize_asset_metadata(item) for item in value]
    if isinstance(value, list):
        return [_normalize_asset_metadata(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _normalize_asset_metadata(item) for key, item in value.items()
        }
    return value


def _forecast_model_names(raw_value: str) -> tuple[str, ...]:
    return _csv_values(raw_value, field_name="forecast_model_names_csv")


def _csv_values(raw_value: str, *, field_name: str) -> tuple[str, ...]:
    values = tuple(value.strip() for value in raw_value.split(",") if value.strip())
    if not values:
        raise ValueError(f"{field_name} must contain at least one value.")
    return values


def _concat_feature_candidate_frames(
    primary_frame: pl.DataFrame,
    optional_frame: pl.DataFrame | None,
) -> pl.DataFrame:
    if optional_frame is None or optional_frame.is_empty():
        return primary_frame
    if primary_frame.is_empty():
        return optional_frame
    for column_name in optional_frame.columns:
        if column_name not in primary_frame.columns:
            primary_frame = primary_frame.with_columns(pl.lit(None).alias(column_name))
    for column_name in primary_frame.columns:
        if column_name not in optional_frame.columns:
            optional_frame = optional_frame.with_columns(
                pl.lit(None).alias(column_name)
            )
    return pl.concat(
        [
            primary_frame.select(optional_frame.columns),
            optional_frame,
        ],
        how="vertical_relaxed",
    )


def _entsoe_security_token() -> str | None:
    """Return the local ENTSO-E token without recording it in evidence artifacts."""

    return load_entsoe_security_token()


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


def _optional_datetime_config(raw_value: str) -> datetime | None:
    value = raw_value.strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("generated_at_iso must be an ISO datetime.") from exc


def _summarize_poland_lag24_vs_v2_plus(
    experimental_frame: pl.DataFrame,
    baseline_frame: pl.DataFrame,
) -> pl.DataFrame:
    experimental_summary = _learner_regret_summary_rows(
        experimental_frame,
        comparison_group="poland_lag24_experimental",
    )
    baseline_summary = _learner_regret_summary_rows(
        baseline_frame,
        comparison_group="frozen_ukrainian_v2_plus",
    )
    if not experimental_summary or not baseline_summary:
        raise ValueError(
            "comparison requires learner rows for both Poland-lag24 experimental "
            "and frozen Ukrainian-only V2+ frames."
        )
    best_baseline_mean = min(
        float(row["mean_regret_uah"]) for row in baseline_summary
    )
    best_baseline_name = min(
        baseline_summary,
        key=lambda row: float(row["mean_regret_uah"]),
    )["forecast_model_name"]
    rows: list[dict[str, Any]] = []
    for row in [*baseline_summary, *experimental_summary]:
        mean_regret = float(row["mean_regret_uah"])
        row["best_frozen_v2_plus_model_name"] = best_baseline_name
        row["best_frozen_v2_plus_mean_regret_uah"] = best_baseline_mean
        row["mean_regret_delta_vs_best_frozen_v2_plus_uah"] = (
            mean_regret - best_baseline_mean
        )
        row["mean_regret_improvement_ratio_vs_best_frozen_v2_plus"] = (
            0.0
            if best_baseline_mean == 0.0
            else (best_baseline_mean - mean_regret) / best_baseline_mean
        )
        row["market_execution_enabled"] = False
        row["not_full_dfl"] = True
        row["not_market_execution"] = True
        rows.append(row)
    return pl.DataFrame(rows).sort(["comparison_group", "mean_regret_uah"])


def _learner_regret_summary_rows(
    frame: pl.DataFrame,
    *,
    comparison_group: str,
) -> list[dict[str, Any]]:
    if frame.is_empty():
        return []
    required_columns = {"forecast_model_name", "regret_uah", "tenant_id", "anchor_timestamp"}
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"learner regret frame is missing columns: {missing_columns}")
    learner_frame = frame.filter(
        pl.col("forecast_model_name").str.starts_with(
            "dfl_schedule_value_learner_v2_plus_"
        )
    )
    if learner_frame.is_empty():
        return []
    summary = learner_frame.group_by("forecast_model_name").agg(
        [
            pl.len().alias("row_count"),
            pl.n_unique("tenant_id").alias("tenant_count"),
            pl.n_unique("anchor_timestamp").alias("anchor_count"),
            pl.mean("regret_uah").alias("mean_regret_uah"),
            pl.median("regret_uah").alias("median_regret_uah"),
        ]
    )
    rows: list[dict[str, Any]] = []
    for row in summary.iter_rows(named=True):
        rows.append(
            {
                "comparison_group": comparison_group,
                "forecast_model_name": str(row["forecast_model_name"]),
                "row_count": int(row["row_count"]),
                "tenant_count": int(row["tenant_count"]),
                "anchor_count": int(row["anchor_count"]),
                "mean_regret_uah": float(row["mean_regret_uah"]),
                "median_regret_uah": float(row["median_regret_uah"]),
            }
        )
    return rows


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
    summary = benchmark_frame.group_by("forecast_model_name").agg(
        [
            pl.len().alias("rows"),
            pl.mean("regret_uah").alias("mean_regret_uah"),
            pl.median("regret_uah").alias("median_regret_uah"),
            pl.mean("decision_value_uah").alias("mean_decision_value_uah"),
        ]
    )
    with mlflow.start_run(run_name=strategy_kind):
        mlflow.set_tags(
            {
                "strategy_kind": strategy_kind,
                "academic_scope": "not_full_differentiable_dfl",
            }
        )
        mlflow.log_metric("rows", benchmark_frame.height)
        mlflow.log_metric(
            "tenant_count", benchmark_frame.select("tenant_id").n_unique()
        )
        mlflow.log_metric(
            "anchor_count", benchmark_frame.select("anchor_timestamp").n_unique()
        )
        for row in summary.iter_rows(named=True):
            model_name = str(row["forecast_model_name"]).replace("-", "_")
            mlflow.log_metric(
                f"{model_name}_mean_regret_uah", float(row["mean_regret_uah"])
            )
            mlflow.log_metric(
                f"{model_name}_median_regret_uah", float(row["median_regret_uah"])
            )
