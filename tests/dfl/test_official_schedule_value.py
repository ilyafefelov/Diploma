from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.schedule_value_learner import (
    build_dfl_schedule_value_learner_v2_frame,
    build_dfl_schedule_value_learner_v2_strict_lp_benchmark_frame,
    evaluate_dfl_schedule_value_learner_v2_gate,
    schedule_value_learner_v2_model_name,
)
from smart_arbitrage.dfl.schedule_value_promotion_gate import (
    build_dfl_schedule_value_production_gate_frame,
    validate_dfl_schedule_value_production_gate_evidence,
)
from smart_arbitrage.dfl.trajectory_ranker import (
    build_dfl_schedule_candidate_library_from_strict_benchmark_frame,
)
from smart_arbitrage.dfl.strict_challenger import (
    build_dfl_schedule_candidate_library_v2_frame,
)
from smart_arbitrage.dfl.schedule_value_learner_robustness import (
    build_dfl_schedule_value_learner_v2_robustness_frame,
)

TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
OFFICIAL_SOURCE_MODELS: tuple[str, ...] = ("nbeatsx_official_v0", "tft_official_v0")
FIRST_ANCHOR = datetime(2026, 1, 1, 23)
GENERATED_AT = datetime(2026, 5, 11, 12)


def test_official_rolling_rows_build_schedule_candidate_library_without_final_only_dependencies() -> None:
    result = build_dfl_schedule_candidate_library_from_strict_benchmark_frame(
        _official_strict_benchmark_frame(anchor_count_per_tenant=4),
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
        perturb_spread_scale_grid=(1.0,),
        perturb_mean_shift_grid_uah_mwh=(100.0,),
        generated_at=GENERATED_AT,
    )

    assert result.height == 5 * 2 * 4 * 3
    assert set(result["source_model_name"].unique().to_list()) == set(OFFICIAL_SOURCE_MODELS)
    assert set(result["candidate_family"].unique().to_list()) == {
        "strict_control",
        "raw_source",
        "forecast_perturbation",
    }
    assert result.filter(pl.col("split_name") == "final_holdout").height == 5 * 2 * 2 * 3
    assert result.filter(pl.col("candidate_model_name") == "strict_similar_day").height == 5 * 2 * 4

    perturb = result.filter(pl.col("candidate_family") == "forecast_perturbation").row(0, named=True)
    assert perturb["forecast_price_uah_mwh_vector"] == [2100.0, 6100.0]
    assert perturb["actual_price_uah_mwh_vector"] == [1000.0, 5000.0]
    assert perturb["not_full_dfl"] is True
    assert perturb["not_market_execution"] is True


def test_official_schedule_value_gate_uses_official_source_names_and_blocks_undercoverage() -> None:
    library = build_dfl_schedule_candidate_library_from_strict_benchmark_frame(
        _official_strict_benchmark_frame(anchor_count_per_tenant=22),
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=18,
        perturb_spread_scale_grid=(1.0,),
        perturb_mean_shift_grid_uah_mwh=(100.0,),
        generated_at=GENERATED_AT,
    )
    library_v2 = build_dfl_schedule_candidate_library_v2_frame(
        library,
        blend_weights=(0.5,),
        residual_min_prior_anchors=3,
        generated_at=GENERATED_AT,
    )
    learner = build_dfl_schedule_value_learner_v2_frame(
        library_v2,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=18,
    )
    strict_frame = build_dfl_schedule_value_learner_v2_strict_lp_benchmark_frame(
        library_v2,
        learner,
        generated_at=GENERATED_AT,
    )
    robustness = build_dfl_schedule_value_learner_v2_robustness_frame(
        library_v2,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_SOURCE_MODELS,
        validation_window_count=1,
        validation_anchor_count=18,
        min_prior_anchors_before_window=3,
        min_robust_passing_windows=1,
        min_validation_tenant_anchor_count_per_source_model=90,
    )
    gate_frame = build_dfl_schedule_value_production_gate_frame(
        strict_frame,
        robustness,
        source_model_names=OFFICIAL_SOURCE_MODELS,
        min_rolling_window_count=1,
        min_rolling_strict_pass_windows=1,
    )

    gate = evaluate_dfl_schedule_value_learner_v2_gate(
        strict_frame,
        source_model_names=OFFICIAL_SOURCE_MODELS,
    )
    evidence = validate_dfl_schedule_value_production_gate_evidence(
        gate_frame,
        source_model_names=OFFICIAL_SOURCE_MODELS,
        min_rolling_window_count=1,
    )

    assert set(learner["source_model_name"].to_list()) == set(OFFICIAL_SOURCE_MODELS)
    assert schedule_value_learner_v2_model_name("tft_official_v0") in strict_frame[
        "forecast_model_name"
    ].unique().to_list()
    assert gate.metrics["validation_tenant_anchor_count"] == 90
    assert evidence.passed is True
    assert gate_frame.select("market_execution_enabled").to_series().unique().to_list() == [False]


def _official_strict_benchmark_frame(*, anchor_count_per_tenant: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index in range(anchor_count_per_tenant):
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            rows.append(
                _evaluation_row(
                    tenant_id=tenant_id,
                    model_name="strict_similar_day",
                    anchor=anchor,
                    regret=100.0,
                    forecast_prices=(1000.0, 5000.0),
                )
            )
            for model_name in OFFICIAL_SOURCE_MODELS:
                rows.append(
                    _evaluation_row(
                        tenant_id=tenant_id,
                        model_name=model_name,
                        anchor=anchor,
                        regret=500.0,
                        forecast_prices=(2000.0, 6000.0),
                    )
                )
    return pl.DataFrame(rows)


def _evaluation_row(
    *,
    tenant_id: str,
    model_name: str,
    anchor: datetime,
    regret: float,
    forecast_prices: tuple[float, float],
) -> dict[str, object]:
    actual_prices = [1000.0, 5000.0]
    dispatch = [0.0, 1.0]
    return {
        "evaluation_id": f"{tenant_id}:{model_name}:{anchor:%Y%m%dT%H%M}",
        "tenant_id": tenant_id,
        "forecast_model_name": model_name,
        "strategy_kind": "official_forecast_rolling_origin_benchmark",
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "horizon_hours": 2,
        "starting_soc_fraction": 0.5,
        "starting_soc_source": "tenant_default",
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 5.0,
        "total_throughput_mwh": 0.2,
        "committed_action": "DISCHARGE",
        "committed_power_mw": 1.0,
        "rank_by_regret": 1,
        "evaluation_payload": {
            "claim_scope": "official_forecast_rolling_origin_benchmark_not_full_dfl",
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": 0,
            "not_full_dfl": True,
            "not_market_execution": True,
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
