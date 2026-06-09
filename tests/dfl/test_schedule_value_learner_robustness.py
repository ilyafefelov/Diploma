from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable

import polars as pl
import pytest

from smart_arbitrage.dfl.schedule_value_learner_robustness import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_ROBUSTNESS_CLAIM_SCOPE,
    build_dfl_schedule_value_learner_v2_robustness_frame,
    evaluate_dfl_schedule_value_learner_v2_robustness_gate,
    validate_dfl_schedule_value_learner_v2_robustness_evidence,
)

TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
SOURCE_MODELS: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0")
FIRST_ANCHOR = datetime(2026, 1, 1, 23)
GENERATED_AT = datetime(2026, 5, 11, 12)


def test_schedule_value_robustness_generates_four_latest_first_windows() -> None:
    library = _candidate_library_104(selected_window_regrets=[90.0, 90.0, 90.0, 90.0])

    result = _build_robustness(library)
    evidence = validate_dfl_schedule_value_learner_v2_robustness_evidence(result)

    assert result.height == 8
    assert evidence.passed is True
    assert set(result["claim_scope"]) == {
        DFL_SCHEDULE_VALUE_LEARNER_V2_ROBUSTNESS_CLAIM_SCOPE
    }
    tft = result.filter(pl.col("source_model_name") == "tft_silver_v0").sort("window_index")
    assert tft["window_index"].to_list() == [1, 2, 3, 4]
    assert tft["validation_tenant_anchor_count"].to_list() == [90, 90, 90, 90]
    assert tft["minimum_prior_anchor_count_before_window"].to_list() == [86, 68, 50, 32]
    assert tft["validation_start_anchor_timestamp"].to_list() == [
        FIRST_ANCHOR + timedelta(days=86),
        FIRST_ANCHOR + timedelta(days=68),
        FIRST_ANCHOR + timedelta(days=50),
        FIRST_ANCHOR + timedelta(days=32),
    ]


def test_schedule_value_robustness_selection_uses_prior_not_validation_actuals() -> None:
    library = _candidate_library_104(selected_window_regrets=[90.0, 90.0, 90.0, 90.0])
    mutated = _candidate_library_104(selected_window_regrets=[700.0, 90.0, 90.0, 90.0])

    base = _build_robustness(library)
    after_mutation = _build_robustness(mutated)

    latest_base = _latest_tft_window(base)
    latest_mutated = _latest_tft_window(after_mutation)
    assert latest_base["selected_weight_profiles_by_tenant"] == latest_mutated[
        "selected_weight_profiles_by_tenant"
    ]
    assert latest_base["selected_family_counts"] == latest_mutated["selected_family_counts"]
    assert latest_base["selected_mean_regret_uah"] == 90.0
    assert latest_mutated["selected_mean_regret_uah"] == 700.0
    assert latest_base["source_specific_strict_passed"] is True
    assert latest_mutated["source_specific_strict_passed"] is False


def test_schedule_value_robustness_gate_requires_three_strict_control_passes() -> None:
    robust_library = _candidate_library_104(selected_window_regrets=[90.0, 90.0, 90.0, 180.0])
    weak_library = _candidate_library_104(selected_window_regrets=[90.0, 180.0, 180.0, 180.0])

    robust = _build_robustness(robust_library)
    weak = _build_robustness(weak_library)
    robust_gate = evaluate_dfl_schedule_value_learner_v2_robustness_gate(robust)
    weak_gate = evaluate_dfl_schedule_value_learner_v2_robustness_gate(weak)

    assert robust_gate.passed is False
    assert robust_gate.decision == "robust_research_challenger_production_blocked"
    assert robust_gate.metrics["robust_source_model_names"] == [
        "nbeatsx_silver_v0",
        "tft_silver_v0",
    ]
    assert set(robust["robust_research_challenger"]) == {True}
    assert weak_gate.passed is False
    assert weak_gate.decision == "diagnostic_pass_production_blocked"
    assert weak_gate.metrics["robust_source_model_names"] == []


@pytest.mark.parametrize(
    ("bad_frame", "message"),
    [
        (
            lambda: _candidate_library_104(
                selected_window_regrets=[90.0, 90.0, 90.0, 90.0],
                data_quality_tier="demo_grade",
            ),
            "thesis_grade",
        ),
        (
            lambda: _candidate_library_104(
                selected_window_regrets=[90.0, 90.0, 90.0, 90.0],
                safety_violation_count=1,
            ),
            "zero safety",
        ),
        (
            lambda: _candidate_library_104(
                selected_window_regrets=[90.0, 90.0, 90.0, 90.0],
                anchor_count=100,
            ),
            "requires at least",
        ),
    ],
)
def test_schedule_value_robustness_blocks_invalid_evidence(
    bad_frame: Callable[[], pl.DataFrame], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _build_robustness(bad_frame())


def _build_robustness(frame: pl.DataFrame) -> pl.DataFrame:
    return build_dfl_schedule_value_learner_v2_robustness_frame(
        frame,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        validation_window_count=4,
        validation_anchor_count=18,
        min_prior_anchors_before_window=30,
        min_robust_passing_windows=3,
    )


def _latest_tft_window(frame: pl.DataFrame) -> dict[str, object]:
    return frame.filter(
        (pl.col("source_model_name") == "tft_silver_v0") & (pl.col("window_index") == 1)
    ).row(0, named=True)


def _candidate_library_104(
    *,
    selected_window_regrets: list[float],
    anchor_count: int = 104,
    data_quality_tier: str = "thesis_grade",
    safety_violation_count: int = 0,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    window_for_anchor = {
        anchor_index: window_index
        for window_index, start in enumerate((86, 68, 50, 32), start=1)
        for anchor_index in range(start, start + 18)
    }
    for tenant_id in TENANTS:
        for source_model_name in SOURCE_MODELS:
            for anchor_index in range(anchor_count):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                split_name = "final_holdout" if anchor_index >= anchor_count - 18 else "train_selection"
                selected_regret = (
                    selected_window_regrets[window_for_anchor[anchor_index] - 1]
                    if anchor_index in window_for_anchor
                    else 40.0
                )
                rows.extend(
                    [
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="strict_control",
                            candidate_model_name="strict_similar_day",
                            anchor=anchor,
                            split_name=split_name,
                            regret=100.0,
                            prior_regret=100.0,
                            forecast_prices=(1000.0, 5000.0),
                            data_quality_tier=data_quality_tier,
                            safety_violation_count=safety_violation_count,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="raw_source",
                            candidate_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            regret=500.0,
                            prior_regret=500.0,
                            forecast_prices=(5000.0, 1000.0),
                            data_quality_tier=data_quality_tier,
                            safety_violation_count=safety_violation_count,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="forecast_perturbation",
                            candidate_model_name=f"dfl_forecast_perturbation_v1_{source_model_name}",
                            anchor=anchor,
                            split_name=split_name,
                            regret=selected_regret,
                            prior_regret=40.0,
                            forecast_prices=(900.0, 6500.0),
                            data_quality_tier=data_quality_tier,
                            safety_violation_count=safety_violation_count,
                        ),
                    ]
                )
    return pl.DataFrame(rows)


def _candidate_row(
    *,
    tenant_id: str,
    source_model_name: str,
    candidate_family: str,
    candidate_model_name: str,
    anchor: datetime,
    split_name: str,
    regret: float,
    prior_regret: float,
    forecast_prices: tuple[float, float],
    data_quality_tier: str,
    safety_violation_count: int,
) -> dict[str, object]:
    actual_prices = [1000.0, 5000.0]
    dispatch = [0.0, 1.0]
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "candidate_family": candidate_family,
        "candidate_model_name": candidate_model_name,
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "split_name": split_name,
        "horizon_hours": 2,
        "forecast_price_uah_mwh_vector": list(forecast_prices),
        "actual_price_uah_mwh_vector": actual_prices,
        "dispatch_mw_vector": dispatch,
        "soc_fraction_vector": [0.5, 0.4],
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 5.0,
        "total_throughput_mwh": 0.2,
        "forecast_spread_uah_mwh": max(forecast_prices) - min(forecast_prices),
        "actual_spread_uah_mwh": 4000.0,
        "forecast_top_k_actual_overlap": 1.0,
        "forecast_bottom_k_actual_overlap": 1.0,
        "peak_index_abs_error": 0.0,
        "trough_index_abs_error": 0.0,
        "soc_min_slack_fraction": 0.4,
        "prior_family_mean_regret_uah": prior_regret,
        "safety_violation_count": safety_violation_count,
        "data_quality_tier": data_quality_tier,
        "observed_coverage_ratio": 1.0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "claim_scope": "dfl_schedule_candidate_library_v2_not_full_dfl",
        "evaluation_payload": {
            "data_quality_tier": data_quality_tier,
            "observed_coverage_ratio": 1.0,
            "not_full_dfl": True,
            "not_market_execution": True,
            "safety_violation_count": safety_violation_count,
            "source_forecast_model_name": source_model_name,
        },
    }
