from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.dfl.schedule_value_learner import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_LP_STRATEGY_KIND,
    build_dfl_schedule_value_learner_v2_frame,
    build_dfl_schedule_value_learner_v2_strict_lp_benchmark_frame,
    evaluate_dfl_schedule_value_learner_v2_gate,
    schedule_value_learner_v2_model_name,
    validate_dfl_schedule_value_learner_v2_evidence,
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


def test_schedule_value_learner_uses_train_only_when_final_scores_change() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=250.0,
        perturb_train_regret=40.0,
    )
    mutated_final_library = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=10.0,
        perturb_train_regret=40.0,
    )

    model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )
    mutated_model = build_dfl_schedule_value_learner_v2_frame(
        mutated_final_library,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )

    assert model["selected_weight_profile_name"].to_list() == mutated_model[
        "selected_weight_profile_name"
    ].to_list()
    assert model["selected_train_family_counts"].to_list() == mutated_model[
        "selected_train_family_counts"
    ].to_list()
    assert set(model["selected_final_mean_regret_uah"].to_list()) == {250.0}
    assert set(mutated_model["selected_final_mean_regret_uah"].to_list()) == {10.0}


def test_schedule_value_learner_strict_benchmark_and_gate_promote_only_when_selected_beats_strict() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=90.0,
        perturb_train_regret=40.0,
    )
    model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )

    strict_frame = build_dfl_schedule_value_learner_v2_strict_lp_benchmark_frame(
        library,
        model,
        generated_at=GENERATED_AT,
    )
    evidence = validate_dfl_schedule_value_learner_v2_evidence(
        strict_frame,
        source_model_names=SOURCE_MODELS,
    )
    gate = evaluate_dfl_schedule_value_learner_v2_gate(strict_frame, source_model_names=SOURCE_MODELS)

    assert strict_frame.height == 5 * 2 * 18 * 3
    assert strict_frame.select("strategy_kind").to_series().unique().to_list() == [
        DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_LP_STRATEGY_KIND
    ]
    assert strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("dfl_schedule_value_learner_v2_")
    ).height == 180
    assert schedule_value_learner_v2_model_name("tft_silver_v0") in strict_frame[
        "forecast_model_name"
    ].unique().to_list()
    assert evidence.passed is True
    assert gate.passed is True
    assert gate.decision == "promote"
    assert gate.metrics["mean_regret_improvement_ratio_vs_strict"] == 0.1


def test_schedule_value_learner_blocks_when_selected_loses_to_strict_control() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=250.0,
        perturb_train_regret=40.0,
    )
    model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )
    strict_frame = build_dfl_schedule_value_learner_v2_strict_lp_benchmark_frame(library, model)

    gate = evaluate_dfl_schedule_value_learner_v2_gate(strict_frame, source_model_names=SOURCE_MODELS)

    assert gate.passed is False
    assert gate.decision == "diagnostic_pass_production_blocked"
    assert gate.metrics["development_gate_passed"] is True
    assert "strict_similar_day" in gate.description


def test_schedule_value_learner_rejects_non_thesis_bad_vectors_and_undercoverage() -> None:
    non_thesis = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=90.0,
        perturb_train_regret=40.0,
        data_quality_tier="demo_grade",
    )
    bad_vectors = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=90.0,
        perturb_train_regret=40.0,
    )
    bad_vector_rows = bad_vectors.to_dicts()
    bad_vector_rows[0]["forecast_price_uah_mwh_vector"] = [1000.0]
    under_coverage = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=90.0,
        perturb_train_regret=40.0,
        final_anchor_count=17,
    )

    for frame, message in [
        (non_thesis, "thesis_grade"),
        (pl.DataFrame(bad_vector_rows), "vector length"),
        (under_coverage, "final-holdout tenant-anchor count"),
    ]:
        with pytest.raises(ValueError, match=message):
            build_dfl_schedule_value_learner_v2_frame(
                frame,
                tenant_ids=TENANTS,
                forecast_model_names=SOURCE_MODELS,
            )


def _candidate_library_from_regrets(
    *,
    strict_final_regret: float,
    raw_final_regret: float,
    perturb_final_regret: float,
    perturb_train_regret: float,
    final_anchor_count: int = 18,
    data_quality_tier: str = "thesis_grade",
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    train_anchor_count = 3
    total_anchor_count = train_anchor_count + final_anchor_count
    for tenant_id in TENANTS:
        for source_model_name in SOURCE_MODELS:
            for anchor_index in range(total_anchor_count):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                split_name = "final_holdout" if anchor_index >= train_anchor_count else "train_selection"
                rows.extend(
                    [
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="strict_control",
                            candidate_model_name="strict_similar_day",
                            anchor=anchor,
                            split_name=split_name,
                            regret=strict_final_regret if split_name == "final_holdout" else 100.0,
                            forecast_prices=(1000.0, 5000.0),
                            data_quality_tier=data_quality_tier,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="raw_source",
                            candidate_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            regret=raw_final_regret if split_name == "final_holdout" else 500.0,
                            forecast_prices=(5000.0, 1000.0),
                            data_quality_tier=data_quality_tier,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="forecast_perturbation",
                            candidate_model_name=f"dfl_forecast_perturbation_v1_{source_model_name}",
                            anchor=anchor,
                            split_name=split_name,
                            regret=perturb_final_regret if split_name == "final_holdout" else perturb_train_regret,
                            forecast_prices=(900.0, 6500.0),
                            data_quality_tier=data_quality_tier,
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
    forecast_prices: tuple[float, float],
    data_quality_tier: str,
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
        "prior_family_mean_regret_uah": 40.0 if candidate_family == "forecast_perturbation" else regret,
        "safety_violation_count": 0,
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
            "source_forecast_model_name": source_model_name,
            "horizon": [
                {
                    "step_index": 0,
                    "interval_start": (anchor + timedelta(hours=1)).isoformat(),
                    "forecast_price_uah_mwh": forecast_prices[0],
                    "actual_price_uah_mwh": actual_prices[0],
                    "net_power_mw": dispatch[0],
                    "soc_fraction": 0.5,
                    "degradation_penalty_uah": 0.0,
                },
                {
                    "step_index": 1,
                    "interval_start": (anchor + timedelta(hours=2)).isoformat(),
                    "forecast_price_uah_mwh": forecast_prices[1],
                    "actual_price_uah_mwh": actual_prices[1],
                    "net_power_mw": dispatch[1],
                    "soc_fraction": 0.4,
                    "degradation_penalty_uah": 5.0,
                },
            ],
        },
    }
