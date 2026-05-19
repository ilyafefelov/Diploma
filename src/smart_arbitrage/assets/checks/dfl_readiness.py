from __future__ import annotations

from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.evidence.quality_checks import (
    EvidenceCheckOutcome,
    validate_dfl_action_label_panel_evidence,
    validate_dfl_training_evidence,
    validate_horizon_calibration_evidence,
    validate_real_data_benchmark_evidence,
    validate_selector_evidence,
)
from smart_arbitrage.dfl.failure_analysis import (
    validate_dfl_action_classifier_failure_analysis_evidence,
)
from smart_arbitrage.dfl.strict_challenger import (
    validate_dfl_non_strict_upper_bound_evidence,
)
from smart_arbitrage.dfl.strict_failure_selector import (
    validate_dfl_strict_failure_selector_evidence,
)
from smart_arbitrage.dfl.strict_failure_robustness import (
    validate_dfl_strict_failure_selector_robustness_evidence,
)
from smart_arbitrage.dfl.strict_failure_features import (
    validate_dfl_strict_failure_feature_audit_evidence,
)
from smart_arbitrage.dfl.strict_failure_feature_selector import (
    validate_dfl_feature_aware_strict_failure_selector_evidence,
)
from smart_arbitrage.dfl.regime_gated_tft_selector import (
    validate_dfl_regime_gated_tft_selector_v2_evidence,
)
from smart_arbitrage.dfl.semantic_event_failure_audit import (
    validate_dfl_semantic_event_strict_failure_audit_evidence,
)
from smart_arbitrage.dfl.residual_schedule_value import (
    validate_dfl_residual_dt_fallback_evidence,
)
from smart_arbitrage.dfl.v2_plus_dfl_dt_bridge import (
    validate_dfl_v2_plus_dfl_dt_bridge_evidence,
)
from smart_arbitrage.dfl.official_v2_plus_dfl_dt_bridge import (
    OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
)
from smart_arbitrage.dfl.official_v2_plus_bridge_failure_audit import (
    validate_dfl_official_v2_plus_bridge_failure_audit_evidence,
)
from smart_arbitrage.dfl.source_specific_challenger import (
    validate_dfl_source_specific_research_challenger_evidence,
)
from smart_arbitrage.dfl.production_promotion_gate import (
    validate_dfl_production_promotion_gate_evidence,
)
from smart_arbitrage.dfl.schedule_value_learner import (
    validate_dfl_schedule_value_learner_v2_evidence,
)
from smart_arbitrage.dfl.schedule_value_learner_v3 import (
    validate_dfl_schedule_value_learner_v3_evidence,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus import (
    validate_dfl_schedule_value_learner_v2_plus_evidence,
)
from smart_arbitrage.dfl.schedule_value_dfl_v2 import (
    validate_dfl_schedule_value_dfl_v2_evidence,
)
from smart_arbitrage.dfl.candidate_value_dfl_v3 import (
    validate_dfl_candidate_value_dfl_v3_evidence,
    validate_dfl_candidate_value_dfl_v3_failure_audit_evidence,
    validate_dfl_candidate_value_label_panel_v3_evidence,
)
from smart_arbitrage.dfl.candidate_value_dfl_v4 import (
    validate_dfl_candidate_value_dfl_v4_evidence,
    validate_dfl_candidate_value_label_panel_v4_evidence,
    validate_dfl_plateau_data_quality_audit_evidence,
    validate_dfl_v2_v3_plateau_autopsy_evidence,
)
from smart_arbitrage.dfl.point_in_time_context_v5 import (
    validate_dfl_context_enriched_candidate_value_dfl_v5_evidence,
    validate_dfl_point_in_time_context_feature_panel_evidence,
    validate_dfl_point_in_time_context_repair_audit_evidence,
)
from smart_arbitrage.dfl.tft_quantile_schedule_value import (
    FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    TFT_QUANTILE_CALIBRATED_SOURCE_MODELS,
    TFT_QUANTILE_SOURCE_MODELS,
    validate_dfl_tft_augmented_v2_plus_evidence,
    validate_dfl_tft_combined_v2_plus_evidence,
)
from smart_arbitrage.dfl.nbeatsx_tft_combined_portfolio import (
    DEFAULT_COMBINED_SOURCE_MODEL_NAME,
    validate_dfl_nbeatsx_tft_meta_selector_evidence,
    validate_dfl_nbeatsx_tft_meta_selector_robustness_evidence,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus_robustness import (
    validate_dfl_schedule_value_learner_v2_plus_robustness_evidence,
)
from smart_arbitrage.dfl.schedule_value_learner_robustness import (
    validate_dfl_schedule_value_learner_v2_robustness_evidence,
)
from smart_arbitrage.dfl.schedule_value_promotion_gate import (
    validate_dfl_schedule_value_production_gate_evidence,
)
from smart_arbitrage.dfl.market_coupling_ablation import (
    validate_dfl_market_coupling_v2_plus_ablation_evidence,
)
from smart_arbitrage.dfl.forecast_pipeline_truth import (
    validate_forecast_pipeline_truth_audit_evidence,
)
from smart_arbitrage.forecasting.afl_error_audit import (
    validate_afl_forecast_error_audit_evidence,
)
from smart_arbitrage.forecasting.market_coupling_availability import (
    validate_market_coupling_temporal_availability_evidence,
)
from smart_arbitrage.forecasting.market_coupling_features import (
    validate_market_coupling_feature_route_evidence,
)
from smart_arbitrage.forecasting.entsoe_neighbor_access import (
    validate_entsoe_neighbor_market_access_evidence,
    validate_entsoe_neighbor_market_feature_candidate_evidence,
    validate_entsoe_neighbor_market_sample_audit_evidence,
    validate_entsoe_poland_feature_governance_evidence,
)
from smart_arbitrage.forecasting.poland_neighbor_snapshot import (
    validate_entsoe_poland_governance_closure_evidence,
    validate_poland_neighbor_market_hourly_feature_evidence,
    validate_poland_neighbor_market_snapshot_evidence,
)


@dg.asset_check(
    asset="real_data_rolling_origin_benchmark_frame",
    name="dnipro_thesis_grade_90_anchor_evidence",
    description="Checks Dnipro 90-anchor thesis-grade rolling-origin evidence.",
)
def dnipro_thesis_grade_90_anchor_evidence(
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_real_data_benchmark_evidence(real_data_rolling_origin_benchmark_frame)
    )


@dg.asset_check(
    asset="dfl_training_frame",
    name="dfl_training_readiness_evidence",
    description="Checks whether DFL training rows are ready as research evidence.",
)
def dfl_training_readiness_evidence(
    dfl_training_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_training_evidence(dfl_training_frame),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_action_label_panel_frame",
    name="dfl_action_label_panel_readiness_evidence",
    description="Checks all-tenant DFL action-label vectors are ready as research data.",
)
def dfl_action_label_panel_readiness_evidence(
    dfl_action_label_panel_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_action_label_panel_evidence(dfl_action_label_panel_frame)
    )


@dg.asset_check(
    asset="dfl_action_classifier_failure_analysis_frame",
    name="dfl_action_classifier_failure_analysis_evidence",
    description="Checks action-classifier failure diagnostics are no-leakage research evidence.",
)
def dfl_action_classifier_failure_analysis_evidence(
    dfl_action_classifier_failure_analysis_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_action_classifier_failure_analysis_evidence(
            dfl_action_classifier_failure_analysis_frame
        )
    )


@dg.asset_check(
    asset="dfl_non_strict_oracle_upper_bound_frame",
    name="dfl_non_strict_oracle_upper_bound_evidence",
    description="Checks whether non-strict schedule candidates can theoretically challenge strict control.",
)
def dfl_non_strict_oracle_upper_bound_evidence(
    dfl_non_strict_oracle_upper_bound_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_non_strict_upper_bound_evidence(
            dfl_non_strict_oracle_upper_bound_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_strict_failure_selector_strict_lp_benchmark_frame",
    name="dfl_strict_failure_selector_evidence",
    description="Checks strict-failure selector coverage and no-leakage claim boundaries.",
)
def dfl_strict_failure_selector_evidence(
    dfl_strict_failure_selector_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_strict_failure_selector_evidence(
            dfl_strict_failure_selector_strict_lp_benchmark_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_strict_failure_selector_robustness_frame",
    name="dfl_strict_failure_selector_robustness_evidence",
    description="Checks rolling-window robustness evidence for the strict-failure selector.",
)
def dfl_strict_failure_selector_robustness_evidence(
    dfl_strict_failure_selector_robustness_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_strict_failure_selector_robustness_evidence(
            dfl_strict_failure_selector_robustness_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_strict_failure_feature_audit_frame",
    name="dfl_strict_failure_feature_audit_evidence",
    description="Checks prior-window feature audit evidence for the strict-failure selector.",
)
def dfl_strict_failure_feature_audit_evidence(
    dfl_strict_failure_feature_audit_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_strict_failure_feature_audit_evidence(
            dfl_strict_failure_feature_audit_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame",
    name="dfl_feature_aware_strict_failure_selector_evidence",
    description="Checks feature-aware strict-failure selector coverage and claim boundaries.",
)
def dfl_feature_aware_strict_failure_selector_evidence(
    dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_feature_aware_strict_failure_selector_evidence(
            dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame",
    name="dfl_regime_gated_tft_selector_v2_evidence",
    description="Checks regime-gated TFT selector v2 strict LP coverage and claim boundaries.",
)
def dfl_regime_gated_tft_selector_v2_evidence(
    dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_regime_gated_tft_selector_v2_evidence(
            dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_semantic_event_strict_failure_audit_frame",
    name="dfl_semantic_event_strict_failure_audit_evidence",
    description="Checks official grid-event semantic strict-failure audit boundaries.",
)
def dfl_semantic_event_strict_failure_audit_evidence(
    dfl_semantic_event_strict_failure_audit_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_semantic_event_strict_failure_audit_evidence(
            dfl_semantic_event_strict_failure_audit_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="afl_forecast_error_audit_frame",
    name="afl_forecast_error_audit_evidence",
    description="Checks AFL forecast-error audit claim boundaries and selector-safe features.",
)
def afl_forecast_error_audit_evidence(
    afl_forecast_error_audit_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_afl_forecast_error_audit_evidence(afl_forecast_error_audit_frame),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_residual_dt_fallback_strict_lp_benchmark_frame",
    name="dfl_residual_dt_fallback_evidence",
    description="Checks residual DFL/offline DT fallback strict-gate evidence boundaries.",
)
def dfl_residual_dt_fallback_evidence(
    dfl_residual_dt_fallback_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_residual_dt_fallback_evidence(
            dfl_residual_dt_fallback_strict_lp_benchmark_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame",
    name="dfl_v2_plus_dfl_dt_bridge_evidence",
    description="Checks V2+-anchored residual DFL/offline DT comparison evidence boundaries.",
)
def dfl_v2_plus_dfl_dt_bridge_evidence(
    dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_v2_plus_dfl_dt_bridge_evidence(
            dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame",
    name="dfl_official_global_panel_v2_plus_dfl_dt_bridge_evidence",
    description=(
        "Checks official V2+-teacher residual DFL/offline DT comparison evidence "
        "boundaries."
    ),
)
def dfl_official_global_panel_v2_plus_dfl_dt_bridge_evidence(
    dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_v2_plus_dfl_dt_bridge_evidence(
            dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame,
            source_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
            min_validation_tenant_anchor_count=90,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_official_v2_plus_bridge_failure_audit_frame",
    name="dfl_official_v2_plus_bridge_failure_audit_evidence",
    description=(
        "Checks official V2+-teacher bridge failure audit remains "
        "analysis-only evidence."
    ),
)
def dfl_official_v2_plus_bridge_failure_audit_evidence(
    dfl_official_v2_plus_bridge_failure_audit_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_official_v2_plus_bridge_failure_audit_evidence(
            dfl_official_v2_plus_bridge_failure_audit_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_source_specific_research_challenger_frame",
    name="dfl_source_specific_research_challenger_evidence",
    description="Checks source-specific TFT/NBEATSx research challenger evidence boundaries.",
)
def dfl_source_specific_research_challenger_evidence(
    dfl_source_specific_research_challenger_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_source_specific_research_challenger_evidence(
            dfl_source_specific_research_challenger_frame
        )
    )


@dg.asset_check(
    asset="dfl_production_promotion_gate_frame",
    name="dfl_production_promotion_gate_evidence",
    description="Checks offline source/regime promotion evidence while market execution remains disabled.",
)
def dfl_production_promotion_gate_evidence(
    dfl_production_promotion_gate_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_production_promotion_gate_evidence(
            dfl_production_promotion_gate_frame
        )
    )


@dg.asset_check(
    asset="dfl_schedule_value_learner_v2_strict_lp_benchmark_frame",
    name="dfl_schedule_value_learner_v2_evidence",
    description="Checks schedule/value learner v2 strict LP coverage and claim boundaries.",
)
def dfl_schedule_value_learner_v2_evidence(
    dfl_schedule_value_learner_v2_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_schedule_value_learner_v2_evidence(
            dfl_schedule_value_learner_v2_strict_lp_benchmark_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_schedule_value_learner_v2_robustness_frame",
    name="dfl_schedule_value_learner_v2_robustness_evidence",
    description="Checks schedule/value learner v2 rolling-window robustness boundaries.",
)
def dfl_schedule_value_learner_v2_robustness_evidence(
    dfl_schedule_value_learner_v2_robustness_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_schedule_value_learner_v2_robustness_evidence(
            dfl_schedule_value_learner_v2_robustness_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_schedule_value_production_gate_frame",
    name="dfl_schedule_value_production_gate_evidence",
    description="Checks offline schedule/value promotion decisions while market execution remains disabled.",
)
def dfl_schedule_value_production_gate_evidence(
    dfl_schedule_value_production_gate_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_schedule_value_production_gate_evidence(
            dfl_schedule_value_production_gate_frame
        )
    )


@dg.asset_check(
    asset="dfl_schedule_value_learner_v3_strict_lp_benchmark_frame",
    name="dfl_schedule_value_learner_v3_evidence",
    description="Checks schedule/value learner v3 strict LP coverage and claim boundaries.",
)
def dfl_schedule_value_learner_v3_evidence(
    dfl_schedule_value_learner_v3_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_schedule_value_learner_v3_evidence(
            dfl_schedule_value_learner_v3_strict_lp_benchmark_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    name="dfl_schedule_value_learner_v2_plus_evidence",
    description="Checks schedule/value learner v2+ strict LP coverage and claim boundaries.",
)
def dfl_schedule_value_learner_v2_plus_evidence(
    dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_schedule_value_learner_v2_plus_evidence(
            dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_official_global_panel_schedule_value_learner_v3_strict_lp_benchmark_frame",
    name="dfl_official_global_panel_schedule_value_learner_v3_evidence",
    description="Checks official global-panel schedule/value learner v3 strict LP coverage and claim boundaries.",
)
def dfl_official_global_panel_schedule_value_learner_v3_evidence(
    dfl_official_global_panel_schedule_value_learner_v3_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_schedule_value_learner_v3_evidence(
            dfl_official_global_panel_schedule_value_learner_v3_strict_lp_benchmark_frame,
            source_model_names=(
                "nbeatsx_official_global_panel_v1",
                "nbeatsx_official_global_panel_horizon_calibrated_v1",
            ),
            min_validation_tenant_anchor_count=90,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    name="dfl_official_global_panel_schedule_value_learner_v2_plus_evidence",
    description="Checks official global-panel schedule/value learner v2+ strict LP coverage and claim boundaries.",
)
def dfl_official_global_panel_schedule_value_learner_v2_plus_evidence(
    dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_schedule_value_learner_v2_plus_evidence(
            dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
            source_model_names=(
                "nbeatsx_official_global_panel_v1",
                "nbeatsx_official_global_panel_horizon_calibrated_v1",
            ),
            min_validation_tenant_anchor_count=90,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_official_global_panel_schedule_value_dfl_v2_strict_lp_benchmark_frame",
    name="dfl_official_global_panel_schedule_value_dfl_v2_evidence",
    description=(
        "Checks official global-panel pairwise schedule-value DFL v2 strict LP "
        "coverage and claim boundaries."
    ),
)
def dfl_official_global_panel_schedule_value_dfl_v2_evidence(
    dfl_official_global_panel_schedule_value_dfl_v2_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_schedule_value_dfl_v2_evidence(
            dfl_official_global_panel_schedule_value_dfl_v2_strict_lp_benchmark_frame,
            source_model_names=(
                "nbeatsx_official_global_panel_v1",
                "nbeatsx_official_global_panel_horizon_calibrated_v1",
            ),
            min_validation_tenant_anchor_count=90,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_official_global_panel_candidate_value_label_panel_v3_frame",
    name="dfl_official_global_panel_candidate_value_label_panel_v3_evidence",
    description=(
        "Checks official global-panel candidate-value DFL v3 label-panel "
        "feature/label separation and claim boundaries."
    ),
)
def dfl_official_global_panel_candidate_value_label_panel_v3_evidence(
    dfl_official_global_panel_candidate_value_label_panel_v3_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_candidate_value_label_panel_v3_evidence(
            dfl_official_global_panel_candidate_value_label_panel_v3_frame,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame",
    name="dfl_official_global_panel_candidate_value_dfl_v3_evidence",
    description=(
        "Checks official global-panel candidate-value DFL v3 strict LP coverage "
        "and claim boundaries."
    ),
)
def dfl_official_global_panel_candidate_value_dfl_v3_evidence(
    dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_candidate_value_dfl_v3_evidence(
            dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame,
            source_model_names=(
                "nbeatsx_official_global_panel_v1",
                "nbeatsx_official_global_panel_horizon_calibrated_v1",
            ),
            min_validation_tenant_anchor_count=90,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_frame",
    name="dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_evidence",
    description=(
        "Checks official global-panel candidate-value DFL v3 failure-audit "
        "claim boundaries."
    ),
)
def dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_evidence(
    dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_candidate_value_dfl_v3_failure_audit_evidence(
            dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_frame,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_official_global_panel_v2_v3_plateau_autopsy_frame",
    name="dfl_official_global_panel_v2_v3_plateau_autopsy_evidence",
    description=(
        "Checks official global-panel V2+/V3 plateau autopsy coverage and "
        "research claim boundaries."
    ),
)
def dfl_official_global_panel_v2_v3_plateau_autopsy_evidence(
    dfl_official_global_panel_v2_v3_plateau_autopsy_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_v2_v3_plateau_autopsy_evidence(
            dfl_official_global_panel_v2_v3_plateau_autopsy_frame,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_official_global_panel_plateau_data_quality_audit_frame",
    name="dfl_official_global_panel_plateau_data_quality_audit_evidence",
    description=(
        "Checks official global-panel plateau data-quality audit rows and "
        "research claim boundaries."
    ),
)
def dfl_official_global_panel_plateau_data_quality_audit_evidence(
    dfl_official_global_panel_plateau_data_quality_audit_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_plateau_data_quality_audit_evidence(
            dfl_official_global_panel_plateau_data_quality_audit_frame,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_official_global_panel_candidate_value_label_panel_v4_frame",
    name="dfl_official_global_panel_candidate_value_label_panel_v4_evidence",
    description=(
        "Checks official global-panel candidate-value DFL v4 label-panel "
        "feature/label separation and claim boundaries."
    ),
)
def dfl_official_global_panel_candidate_value_label_panel_v4_evidence(
    dfl_official_global_panel_candidate_value_label_panel_v4_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_candidate_value_label_panel_v4_evidence(
            dfl_official_global_panel_candidate_value_label_panel_v4_frame,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_official_global_panel_candidate_value_dfl_v4_strict_lp_benchmark_frame",
    name="dfl_official_global_panel_candidate_value_dfl_v4_evidence",
    description=(
        "Checks official global-panel candidate-value DFL v4 strict LP coverage "
        "and claim boundaries."
    ),
)
def dfl_official_global_panel_candidate_value_dfl_v4_evidence(
    dfl_official_global_panel_candidate_value_dfl_v4_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_candidate_value_dfl_v4_evidence(
            dfl_official_global_panel_candidate_value_dfl_v4_strict_lp_benchmark_frame,
            source_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
            min_validation_tenant_anchor_count=90,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame",
    name="dfl_tft_augmented_v2_plus_evidence",
    description=(
        "Checks TFT quantile V2+ contributor coverage and claim boundaries "
        "against frozen official V2+."
    ),
)
def dfl_tft_augmented_v2_plus_evidence(
    dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_tft_augmented_v2_plus_evidence(
            dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame,
            baseline_source_model_name=FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
            tft_source_model_names=TFT_QUANTILE_SOURCE_MODELS,
            min_validation_tenant_anchor_count=90,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_tft_combined_v2_plus_strict_lp_benchmark_frame",
    name="dfl_tft_combined_v2_plus_evidence",
    description=(
        "Checks combined NBEATSx V2+ plus TFT complementary schedule coverage "
        "and claim boundaries."
    ),
)
def dfl_tft_combined_v2_plus_evidence(
    dfl_tft_combined_v2_plus_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_tft_combined_v2_plus_evidence(
            dfl_tft_combined_v2_plus_strict_lp_benchmark_frame,
            baseline_source_model_name=FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
            min_validation_tenant_anchor_count=90,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_tft_calibrated_augmented_v2_plus_strict_lp_benchmark_frame",
    name="dfl_tft_calibrated_augmented_v2_plus_evidence",
    description=(
        "Checks calibrated TFT quantile V2+ contributor coverage and claim "
        "boundaries against frozen official V2+."
    ),
)
def dfl_tft_calibrated_augmented_v2_plus_evidence(
    dfl_tft_calibrated_augmented_v2_plus_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_tft_augmented_v2_plus_evidence(
            dfl_tft_calibrated_augmented_v2_plus_strict_lp_benchmark_frame,
            baseline_source_model_name=FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
            tft_source_model_names=TFT_QUANTILE_CALIBRATED_SOURCE_MODELS,
            min_validation_tenant_anchor_count=90,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame",
    name="dfl_tft_calibrated_combined_v2_plus_evidence",
    description=(
        "Checks combined NBEATSx V2+ plus calibrated TFT complementary "
        "schedule coverage and claim boundaries."
    ),
)
def dfl_tft_calibrated_combined_v2_plus_evidence(
    dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_tft_combined_v2_plus_evidence(
            dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame,
            baseline_source_model_name=FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
            min_validation_tenant_anchor_count=90,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame",
    name="dfl_nbeatsx_tft_meta_selector_evidence",
    description=(
        "Checks candidate-level NBEATSx V2+ plus calibrated TFT portfolio "
        "coverage and claim boundaries."
    ),
)
def dfl_nbeatsx_tft_meta_selector_evidence(
    dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_nbeatsx_tft_meta_selector_evidence(
            dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame,
            baseline_source_model_name=FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
            combined_source_model_name=DEFAULT_COMBINED_SOURCE_MODEL_NAME,
            min_validation_tenant_anchor_count=90,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_nbeatsx_tft_meta_selector_robustness_frame",
    name="dfl_nbeatsx_tft_meta_selector_robustness_evidence",
    description=(
        "Checks rolling-window evidence for the NBEATSx+TFT candidate "
        "portfolio meta-selector."
    ),
)
def dfl_nbeatsx_tft_meta_selector_robustness_evidence(
    dfl_nbeatsx_tft_meta_selector_robustness_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_nbeatsx_tft_meta_selector_robustness_evidence(
            dfl_nbeatsx_tft_meta_selector_robustness_frame,
            validation_window_count=4,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame",
    name="dfl_nbeatsx_tft_meta_selector_rolling_strict_evidence",
    description=(
        "Checks true rolling strict LP/oracle evidence for the NBEATSx+TFT "
        "candidate portfolio meta-selector."
    ),
)
def dfl_nbeatsx_tft_meta_selector_rolling_strict_evidence(
    dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_nbeatsx_tft_meta_selector_evidence(
            dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame,
            baseline_source_model_name=FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
            combined_source_model_name=DEFAULT_COMBINED_SOURCE_MODEL_NAME,
            min_validation_tenant_anchor_count=360,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_nbeatsx_tft_meta_selector_prior_rolling_robustness_frame",
    name="dfl_nbeatsx_tft_meta_selector_prior_rolling_evidence",
    description=(
        "Checks true rolling-window robustness evidence for the NBEATSx+TFT "
        "candidate portfolio meta-selector."
    ),
)
def dfl_nbeatsx_tft_meta_selector_prior_rolling_evidence(
    dfl_nbeatsx_tft_meta_selector_prior_rolling_robustness_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_nbeatsx_tft_meta_selector_robustness_evidence(
            dfl_nbeatsx_tft_meta_selector_prior_rolling_robustness_frame,
            validation_window_count=4,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_point_in_time_context_repair_audit_frame",
    name="dfl_point_in_time_context_repair_audit_evidence",
    description=(
        "Checks point-in-time context repair audit rows and research claim boundaries."
    ),
)
def dfl_point_in_time_context_repair_audit_evidence(
    dfl_point_in_time_context_repair_audit_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_point_in_time_context_repair_audit_evidence(
            dfl_point_in_time_context_repair_audit_frame,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_point_in_time_context_feature_panel_frame",
    name="dfl_point_in_time_context_feature_panel_evidence",
    description=(
        "Checks V5 point-in-time context features stay prior-only and Ukrainian-only."
    ),
)
def dfl_point_in_time_context_feature_panel_evidence(
    dfl_point_in_time_context_feature_panel_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_point_in_time_context_feature_panel_evidence(
            dfl_point_in_time_context_feature_panel_frame,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame",
    name="dfl_context_enriched_candidate_value_dfl_v5_evidence",
    description=(
        "Checks context-enriched candidate-value DFL v5 strict LP coverage and "
        "claim boundaries."
    ),
)
def dfl_context_enriched_candidate_value_dfl_v5_evidence(
    dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame: (
        pl.DataFrame
    ),
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_context_enriched_candidate_value_dfl_v5_evidence(
            dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame,
            source_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
            min_validation_tenant_anchor_count=90,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame",
    name="dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_evidence",
    description="Checks official global-panel schedule/value learner v2+ rolling robustness evidence.",
)
def dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_evidence(
    dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame: (
        pl.DataFrame
    ),
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_schedule_value_learner_v2_plus_robustness_evidence(
            dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame,
            source_model_names=(
                "nbeatsx_official_global_panel_v1",
                "nbeatsx_official_global_panel_horizon_calibrated_v1",
            ),
            min_validation_tenant_anchor_count=90,
            min_window_count=4,
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="dfl_market_coupling_v2_plus_ablation_frame",
    name="dfl_market_coupling_v2_plus_ablation_evidence",
    description=(
        "Checks governed market-coupling ablation evidence preserves Offline "
        "Strategy Promotion boundaries."
    ),
)
def dfl_market_coupling_v2_plus_ablation_evidence(
    dfl_market_coupling_v2_plus_ablation_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_market_coupling_v2_plus_ablation_evidence(
            dfl_market_coupling_v2_plus_ablation_frame
        )
    )


@dg.asset_check(
    asset="dfl_official_schedule_value_production_gate_frame",
    name="dfl_official_schedule_value_production_gate_evidence",
    description="Checks official NBEATSx/TFT schedule/value promotion decisions while market execution remains disabled.",
)
def dfl_official_schedule_value_production_gate_evidence(
    dfl_official_schedule_value_production_gate_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_dfl_schedule_value_production_gate_evidence(
            dfl_official_schedule_value_production_gate_frame,
            source_model_names=("nbeatsx_official_v0", "tft_official_v0"),
        ),
        failed_severity=dg.AssetCheckSeverity.WARN,
    )


@dg.asset_check(
    asset="forecast_pipeline_truth_audit_frame",
    name="forecast_pipeline_truth_audit_evidence",
    description="Checks forecast-vector truth audit source, unit, timestamp, and round-trip evidence.",
)
def forecast_pipeline_truth_audit_evidence(
    forecast_pipeline_truth_audit_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_forecast_pipeline_truth_audit_evidence(
            forecast_pipeline_truth_audit_frame
        )
    )


@dg.asset_check(
    asset="market_coupling_temporal_availability_frame",
    name="market_coupling_temporal_availability_evidence",
    description="Checks EU/neighbor-market sources remain blocked until availability mapping is complete.",
)
def market_coupling_temporal_availability_evidence(
    market_coupling_temporal_availability_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_market_coupling_temporal_availability_evidence(
            market_coupling_temporal_availability_frame
        )
    )


@dg.asset_check(
    asset="official_forecast_exogenous_feature_route_frame",
    name="official_forecast_exogenous_feature_route_evidence",
    description="Checks official forecast external-feature routing remains governed before training.",
)
def official_forecast_exogenous_feature_route_evidence(
    official_forecast_exogenous_feature_route_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_market_coupling_feature_route_evidence(
            official_forecast_exogenous_feature_route_frame
        )
    )


@dg.asset_check(
    asset="entsoe_neighbor_market_query_spec_frame",
    name="entsoe_neighbor_market_access_evidence",
    description="Checks ENTSO-E neighbor-market query specs remain research-only before sample fetch.",
)
def entsoe_neighbor_market_access_evidence(
    entsoe_neighbor_market_query_spec_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_entsoe_neighbor_market_access_evidence(
            entsoe_neighbor_market_query_spec_frame
        )
    )


@dg.asset_check(
    asset="entsoe_neighbor_market_sample_audit_frame",
    name="entsoe_neighbor_market_sample_audit_evidence",
    description="Checks ENTSO-E neighbor-market samples stay out of training before governance passes.",
)
def entsoe_neighbor_market_sample_audit_evidence(
    entsoe_neighbor_market_sample_audit_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_entsoe_neighbor_market_sample_audit_evidence(
            entsoe_neighbor_market_sample_audit_frame
        )
    )


@dg.asset_check(
    asset="entsoe_neighbor_market_feature_candidate_frame",
    name="entsoe_neighbor_market_feature_candidate_evidence",
    description="Checks ENTSO-E source-backed feature candidates remain blocked until governance passes.",
)
def entsoe_neighbor_market_feature_candidate_evidence(
    entsoe_neighbor_market_feature_candidate_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_entsoe_neighbor_market_feature_candidate_evidence(
            entsoe_neighbor_market_feature_candidate_frame
        )
    )


@dg.asset_check(
    asset="poland_neighbor_market_snapshot_bronze",
    name="poland_neighbor_market_snapshot_evidence",
    description="Checks no-token Poland neighbor-market snapshots remain source-only evidence.",
)
def poland_neighbor_market_snapshot_evidence(
    poland_neighbor_market_snapshot_bronze: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_poland_neighbor_market_snapshot_evidence(
            poland_neighbor_market_snapshot_bronze
        )
    )


@dg.asset_check(
    asset="poland_neighbor_market_snapshot_feature_candidate_frame",
    name="poland_neighbor_market_snapshot_feature_candidate_evidence",
    description="Checks no-token Poland snapshot candidates remain governed before training.",
)
def poland_neighbor_market_snapshot_feature_candidate_evidence(
    poland_neighbor_market_snapshot_feature_candidate_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_entsoe_neighbor_market_feature_candidate_evidence(
            poland_neighbor_market_snapshot_feature_candidate_frame
        )
    )


@dg.asset_check(
    asset="poland_neighbor_market_hourly_feature_frame",
    name="poland_neighbor_market_hourly_feature_evidence",
    description="Checks hourly Poland neighbor-market features remain source-only evidence.",
)
def poland_neighbor_market_hourly_feature_evidence(
    poland_neighbor_market_hourly_feature_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_poland_neighbor_market_hourly_feature_evidence(
            poland_neighbor_market_hourly_feature_frame
        )
    )


@dg.asset_check(
    asset="entsoe_poland_governance_closure_frame",
    name="entsoe_poland_governance_closure_evidence",
    description="Checks source-backed Poland feature governance closure before training approval.",
)
def entsoe_poland_governance_closure_evidence(
    entsoe_poland_governance_closure_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_entsoe_poland_governance_closure_evidence(
            entsoe_poland_governance_closure_frame
        )
    )


@dg.asset_check(
    asset="entsoe_poland_feature_governance_frame",
    name="entsoe_poland_feature_governance_evidence",
    description="Checks Poland ENTSO-E feature governance before official training approval.",
)
def entsoe_poland_feature_governance_evidence(
    entsoe_poland_feature_governance_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_entsoe_poland_feature_governance_evidence(
            entsoe_poland_feature_governance_frame
        )
    )


@dg.asset_check(
    asset="horizon_regret_weighted_forecast_strategy_benchmark_frame",
    name="horizon_calibration_no_leakage_evidence",
    description="Checks horizon-aware calibration anchor coverage and prior-anchor metadata.",
)
def horizon_calibration_no_leakage_evidence(
    horizon_regret_weighted_forecast_strategy_benchmark_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_horizon_calibration_evidence(
            horizon_regret_weighted_forecast_strategy_benchmark_frame
        )
    )


@dg.asset_check(
    asset="calibrated_value_aware_ensemble_frame",
    name="calibrated_selector_cardinality_evidence",
    description="Checks calibrated selector rows are one-per-anchor and thesis-grade.",
)
def calibrated_selector_cardinality_evidence(
    calibrated_value_aware_ensemble_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_selector_evidence(
            calibrated_value_aware_ensemble_frame,
            expected_strategy_kind="calibrated_value_aware_ensemble_gate",
            expected_model_name="calibrated_value_aware_ensemble_v0",
        )
    )


@dg.asset_check(
    asset="risk_adjusted_value_gate_frame",
    name="risk_adjusted_selector_cardinality_evidence",
    description="Checks risk-adjusted selector rows are one-per-anchor and thesis-grade.",
)
def risk_adjusted_selector_cardinality_evidence(
    risk_adjusted_value_gate_frame: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _asset_check_result(
        validate_selector_evidence(
            risk_adjusted_value_gate_frame,
            expected_strategy_kind="risk_adjusted_value_gate",
            expected_model_name="risk_adjusted_value_gate_v0",
        )
    )


DFL_EVIDENCE_ASSET_CHECKS = [
    dnipro_thesis_grade_90_anchor_evidence,
    dfl_training_readiness_evidence,
    dfl_action_label_panel_readiness_evidence,
    dfl_action_classifier_failure_analysis_evidence,
    dfl_non_strict_oracle_upper_bound_evidence,
    dfl_strict_failure_selector_evidence,
    dfl_strict_failure_selector_robustness_evidence,
    dfl_strict_failure_feature_audit_evidence,
    dfl_feature_aware_strict_failure_selector_evidence,
    dfl_regime_gated_tft_selector_v2_evidence,
    dfl_semantic_event_strict_failure_audit_evidence,
    afl_forecast_error_audit_evidence,
    dfl_residual_dt_fallback_evidence,
    dfl_v2_plus_dfl_dt_bridge_evidence,
    dfl_official_global_panel_v2_plus_dfl_dt_bridge_evidence,
    dfl_official_v2_plus_bridge_failure_audit_evidence,
    dfl_source_specific_research_challenger_evidence,
    dfl_production_promotion_gate_evidence,
    dfl_schedule_value_learner_v2_evidence,
    dfl_schedule_value_learner_v2_robustness_evidence,
    dfl_schedule_value_production_gate_evidence,
    dfl_schedule_value_learner_v3_evidence,
    dfl_schedule_value_learner_v2_plus_evidence,
    dfl_official_global_panel_schedule_value_learner_v3_evidence,
    dfl_official_global_panel_schedule_value_learner_v2_plus_evidence,
    dfl_official_global_panel_schedule_value_dfl_v2_evidence,
    dfl_official_global_panel_candidate_value_label_panel_v3_evidence,
    dfl_official_global_panel_candidate_value_dfl_v3_evidence,
    dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_evidence,
    dfl_official_global_panel_v2_v3_plateau_autopsy_evidence,
    dfl_official_global_panel_plateau_data_quality_audit_evidence,
    dfl_official_global_panel_candidate_value_label_panel_v4_evidence,
    dfl_official_global_panel_candidate_value_dfl_v4_evidence,
    dfl_tft_augmented_v2_plus_evidence,
    dfl_tft_combined_v2_plus_evidence,
    dfl_tft_calibrated_augmented_v2_plus_evidence,
    dfl_tft_calibrated_combined_v2_plus_evidence,
    dfl_nbeatsx_tft_meta_selector_evidence,
    dfl_nbeatsx_tft_meta_selector_robustness_evidence,
    dfl_nbeatsx_tft_meta_selector_rolling_strict_evidence,
    dfl_nbeatsx_tft_meta_selector_prior_rolling_evidence,
    dfl_point_in_time_context_repair_audit_evidence,
    dfl_point_in_time_context_feature_panel_evidence,
    dfl_context_enriched_candidate_value_dfl_v5_evidence,
    dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_evidence,
    dfl_market_coupling_v2_plus_ablation_evidence,
    dfl_official_schedule_value_production_gate_evidence,
    forecast_pipeline_truth_audit_evidence,
    market_coupling_temporal_availability_evidence,
    official_forecast_exogenous_feature_route_evidence,
    entsoe_neighbor_market_access_evidence,
    entsoe_neighbor_market_sample_audit_evidence,
    entsoe_neighbor_market_feature_candidate_evidence,
    poland_neighbor_market_snapshot_evidence,
    poland_neighbor_market_snapshot_feature_candidate_evidence,
    poland_neighbor_market_hourly_feature_evidence,
    entsoe_poland_governance_closure_evidence,
    entsoe_poland_feature_governance_evidence,
    horizon_calibration_no_leakage_evidence,
    calibrated_selector_cardinality_evidence,
    risk_adjusted_selector_cardinality_evidence,
]


def _asset_check_result(
    outcome: EvidenceCheckOutcome,
    *,
    failed_severity: dg.AssetCheckSeverity = dg.AssetCheckSeverity.ERROR,
) -> dg.AssetCheckResult:
    return dg.AssetCheckResult(
        passed=outcome.passed,
        description=outcome.description,
        metadata=_metadata(outcome.metadata),
        severity=dg.AssetCheckSeverity.ERROR if outcome.passed else failed_severity,
    )


def _metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    return {key: _metadata_value(value) for key, value in metadata.items()}


def _metadata_value(value: Any) -> Any:
    if isinstance(value, tuple):
        return list(value)
    return value
