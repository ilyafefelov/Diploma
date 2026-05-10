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
from smart_arbitrage.dfl.semantic_event_failure_audit import (
    validate_dfl_semantic_event_strict_failure_audit_evidence,
)
from smart_arbitrage.dfl.residual_schedule_value import (
    validate_dfl_residual_dt_fallback_evidence,
)
from smart_arbitrage.dfl.source_specific_challenger import (
    validate_dfl_source_specific_research_challenger_evidence,
)
from smart_arbitrage.forecasting.afl_error_audit import (
    validate_afl_forecast_error_audit_evidence,
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
    dfl_semantic_event_strict_failure_audit_evidence,
    afl_forecast_error_audit_evidence,
    dfl_residual_dt_fallback_evidence,
    dfl_source_specific_research_challenger_evidence,
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
