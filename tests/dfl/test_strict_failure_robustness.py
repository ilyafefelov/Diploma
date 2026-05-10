from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import polars as pl
import pytest

from smart_arbitrage.dfl.strict_failure_robustness import (
    DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS_CLAIM_SCOPE,
    build_dfl_strict_failure_selector_robustness_frame,
    evaluate_dfl_strict_failure_selector_robustness_gate,
    validate_dfl_strict_failure_selector_robustness_evidence,
)


CANONICAL_TENANTS = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
SOURCE_MODELS = ("tft_silver_v0", "nbeatsx_silver_v0")
FIRST_ANCHOR = datetime(2026, 1, 1, 23, tzinfo=UTC)


def _candidate_library_104(
    *,
    validation_challenger_regret_by_source: dict[str, float] | None = None,
    data_quality_tier: str = "thesis_grade",
    not_full_dfl: bool = True,
    safety_violation_count: int = 0,
) -> pl.DataFrame:
    validation_challenger_regret_by_source = (
        validation_challenger_regret_by_source
        or {
            "tft_silver_v0": 150.0,
            "nbeatsx_silver_v0": 290.0,
        }
    )
    rows: list[dict[str, Any]] = []
    for tenant_id in CANONICAL_TENANTS:
        for source_model_name in SOURCE_MODELS:
            for anchor_index in range(104):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                split_name = "final_holdout" if anchor_index >= 86 else "train_selection"
                challenger_regret = (
                    100.0
                    if anchor_index < 32
                    else validation_challenger_regret_by_source[source_model_name]
                )
                rows.extend(
                    [
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            candidate_family="strict_control",
                            candidate_model_name="strict_similar_day",
                            regret_uah=300.0,
                            data_quality_tier=data_quality_tier,
                            not_full_dfl=not_full_dfl,
                            safety_violation_count=safety_violation_count,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            candidate_family="raw_source",
                            candidate_model_name=source_model_name,
                            regret_uah=500.0,
                            data_quality_tier=data_quality_tier,
                            not_full_dfl=not_full_dfl,
                            safety_violation_count=safety_violation_count,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            candidate_family="strict_raw_blend_v2",
                            candidate_model_name=f"strict_raw_blend_v2_{source_model_name}",
                            regret_uah=challenger_regret,
                            data_quality_tier=data_quality_tier,
                            not_full_dfl=not_full_dfl,
                            safety_violation_count=safety_violation_count,
                        ),
                    ]
                )
    return pl.DataFrame(rows)


def _candidate_row(
    *,
    tenant_id: str,
    source_model_name: str,
    anchor: datetime,
    split_name: str,
    candidate_family: str,
    candidate_model_name: str,
    regret_uah: float,
    data_quality_tier: str,
    not_full_dfl: bool,
    safety_violation_count: int,
) -> dict[str, Any]:
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "anchor_timestamp": anchor,
        "generated_at": datetime(2026, 5, 8, tzinfo=UTC),
        "split_name": split_name,
        "candidate_family": candidate_family,
        "candidate_model_name": candidate_model_name,
        "horizon_hours": 2,
        "forecast_price_uah_mwh_vector": [1_000.0, 1_250.0],
        "actual_price_uah_mwh_vector": [1_100.0, 1_300.0],
        "dispatch_mw_vector": [0.0, 1.0],
        "soc_fraction_vector": [0.5, 0.45],
        "decision_value_uah": 1_000.0 - regret_uah,
        "forecast_objective_value_uah": 950.0,
        "oracle_value_uah": 1_000.0,
        "regret_uah": regret_uah,
        "regret_ratio": regret_uah / 1_000.0,
        "total_degradation_penalty_uah": 10.0,
        "total_throughput_mwh": 1.0,
        "forecast_spread_uah_mwh": 250.0,
        "actual_spread_uah_mwh": 200.0,
        "forecast_top_k_actual_overlap": 1.0,
        "forecast_bottom_k_actual_overlap": 1.0,
        "peak_index_abs_error": 0.0,
        "trough_index_abs_error": 0.0,
        "soc_min_slack_fraction": 0.45,
        "prior_family_mean_regret_uah": regret_uah,
        "safety_violation_count": safety_violation_count,
        "data_quality_tier": data_quality_tier,
        "observed_coverage_ratio": 1.0,
        "not_full_dfl": not_full_dfl,
        "not_market_execution": True,
        "claim_scope": "schedule_candidate_library_v2_not_full_dfl",
        "candidate_library_version": "v2_test",
        "evaluation_payload": {
            "data_quality_tier": data_quality_tier,
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": safety_violation_count,
            "not_full_dfl": not_full_dfl,
            "not_market_execution": True,
        },
    }


def _mutate_latest_challenger_regret(frame: pl.DataFrame, regret_uah: float) -> pl.DataFrame:
    latest_window_start = FIRST_ANCHOR + timedelta(days=86)
    latest_challenger = (
        (pl.col("anchor_timestamp") >= latest_window_start)
        & (pl.col("candidate_family") == "strict_raw_blend_v2")
    )
    return frame.with_columns(
        pl.when(latest_challenger)
        .then(pl.lit(regret_uah))
        .otherwise(pl.col("regret_uah"))
        .alias("regret_uah"),
        pl.when(latest_challenger)
        .then(pl.lit(1_000.0 - regret_uah))
        .otherwise(pl.col("decision_value_uah"))
        .alias("decision_value_uah"),
        pl.when(latest_challenger)
        .then(pl.lit(regret_uah / 1_000.0))
        .otherwise(pl.col("regret_ratio"))
        .alias("regret_ratio"),
    )


def _build_robustness(frame: pl.DataFrame) -> pl.DataFrame:
    return build_dfl_strict_failure_selector_robustness_frame(
        frame,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        validation_window_count=4,
        validation_anchor_count=18,
        min_prior_anchors_before_window=30,
        min_prior_anchor_count=3,
        min_robust_passing_windows=3,
    )


def test_robustness_generates_four_latest_first_rolling_windows() -> None:
    frame = _candidate_library_104()

    result = _build_robustness(frame)

    tft = (
        result.filter(pl.col("source_model_name") == "tft_silver_v0")
        .sort("window_index")
        .to_dicts()
    )
    assert [row["window_index"] for row in tft] == [1, 2, 3, 4]
    assert [row["validation_start_anchor_timestamp"] for row in tft] == [
        FIRST_ANCHOR + timedelta(days=86),
        FIRST_ANCHOR + timedelta(days=68),
        FIRST_ANCHOR + timedelta(days=50),
        FIRST_ANCHOR + timedelta(days=32),
    ]
    assert [row["minimum_prior_anchor_count_before_window"] for row in tft] == [
        86,
        68,
        50,
        32,
    ]
    assert {row["validation_anchor_count_per_tenant"] for row in tft} == {18}
    assert {row["validation_tenant_anchor_count"] for row in tft} == {90}
    assert set(result["claim_scope"]) == {
        DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS_CLAIM_SCOPE
    }


def test_threshold_selection_uses_prior_anchors_not_validation_actuals() -> None:
    frame = _candidate_library_104()
    mutated = _mutate_latest_challenger_regret(frame, regret_uah=700.0)

    base = _build_robustness(frame)
    after_mutation = _build_robustness(mutated)

    latest_base = base.filter(
        (pl.col("source_model_name") == "tft_silver_v0")
        & (pl.col("window_index") == 1)
    ).row(0, named=True)
    latest_mutated = after_mutation.filter(
        (pl.col("source_model_name") == "tft_silver_v0")
        & (pl.col("window_index") == 1)
    ).row(0, named=True)

    assert latest_mutated["selected_thresholds_by_tenant"] == latest_base[
        "selected_thresholds_by_tenant"
    ]
    assert latest_mutated["selected_mean_regret_uah"] > latest_base[
        "selected_mean_regret_uah"
    ]
    assert latest_mutated["source_specific_strict_passed"] is False


def test_robustness_gate_marks_only_repeated_source_specific_winners_as_challengers() -> None:
    frame = _candidate_library_104()

    result = _build_robustness(frame)
    gate = evaluate_dfl_strict_failure_selector_robustness_gate(result)
    evidence = validate_dfl_strict_failure_selector_robustness_evidence(result)

    assert evidence.passed is True
    assert gate.passed is False
    assert gate.decision == "robust_research_challenger_production_blocked"
    assert gate.metrics["robust_source_model_names"] == ["tft_silver_v0"]
    tft = result.filter(pl.col("source_model_name") == "tft_silver_v0")
    nbeatsx = result.filter(pl.col("source_model_name") == "nbeatsx_silver_v0")
    assert set(tft["robust_research_challenger"]) == {True}
    assert set(nbeatsx["robust_research_challenger"]) == {False}
    assert set(result["production_promote"]) == {False}


@pytest.mark.parametrize(
    ("bad_frame", "message"),
    [
        (_candidate_library_104().filter(pl.col("anchor_timestamp") < FIRST_ANCHOR + timedelta(days=101)), "rolling validation"),
        (_candidate_library_104(data_quality_tier="demo_grade"), "thesis_grade"),
        (_candidate_library_104(not_full_dfl=False), "not_full_dfl"),
        (_candidate_library_104(safety_violation_count=1), "zero safety"),
        (
            _candidate_library_104().filter(
                ~(
                    (pl.col("tenant_id") == "client_005_odesa_hotel")
                    & (pl.col("source_model_name") == "tft_silver_v0")
                )
            ),
            "coverage",
        ),
    ],
)
def test_robustness_builder_blocks_invalid_evidence(
    bad_frame: pl.DataFrame, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _build_robustness(bad_frame)
