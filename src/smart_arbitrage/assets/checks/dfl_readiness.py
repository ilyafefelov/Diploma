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
