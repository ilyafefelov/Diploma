from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.strict_challenger import (
    ANALYSIS_ONLY_SCHEDULE_FEATURE_COLUMNS,
    RANKER_SELECTION_FEATURE_COLUMNS,
    build_dfl_non_strict_oracle_upper_bound_frame,
    build_dfl_pipeline_integrity_audit_frame,
    build_dfl_schedule_candidate_library_v2_frame,
    build_dfl_strict_baseline_autopsy_frame,
    validate_dfl_non_strict_upper_bound_evidence,
)

CANONICAL_TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
SOURCE_MODELS: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0")
FIRST_ANCHOR = datetime(2026, 1, 1, 23)
GENERATED_AT = datetime(2026, 5, 8, 12)


def test_pipeline_integrity_audit_keeps_actual_features_out_of_ranker_inputs() -> None:
    audit = build_dfl_pipeline_integrity_audit_frame(
        _benchmark_frame(anchor_count_per_tenant=4),
        _candidate_library(anchor_count_per_tenant=4, final_anchor_count=2),
    )

    row = audit.row(0, named=True)

    assert row["passed"] is True
    assert row["market_anchor_count"] == 4
    assert row["tenant_anchor_count"] == 20
    assert row["source_model_count"] == 2
    assert row["leaky_horizon_rows"] == 0
    assert row["forbidden_ranker_feature_overlap_count"] == 0
    assert set(RANKER_SELECTION_FEATURE_COLUMNS).isdisjoint(ANALYSIS_ONLY_SCHEDULE_FEATURE_COLUMNS)
    assert row["not_full_dfl"] is True
    assert row["not_market_execution"] is True


def test_schedule_candidate_library_v2_adds_prior_only_strict_challengers() -> None:
    base_library = _candidate_library(anchor_count_per_tenant=5, final_anchor_count=2)

    result = build_dfl_schedule_candidate_library_v2_frame(
        base_library,
        blend_weights=(0.5,),
        residual_min_prior_anchors=1,
        generated_at=GENERATED_AT,
    )

    families = set(result["candidate_family"].unique().to_list())
    assert "strict_raw_blend_v2" in families
    assert "strict_prior_residual_v2" in families
    assert result.height > base_library.height

    blend = result.filter(pl.col("candidate_family") == "strict_raw_blend_v2").row(0, named=True)
    assert blend["forecast_price_uah_mwh_vector"] == [1500.0, 5500.0]
    assert blend["evaluation_payload"]["blend_weight_raw"] == 0.5
    assert blend["evaluation_payload"]["no_leakage_prior_only"] is True

    final_residual = result.filter(
        (pl.col("candidate_family") == "strict_prior_residual_v2")
        & (pl.col("split_name") == "final_holdout")
    ).row(0, named=True)
    assert final_residual["evaluation_payload"]["prior_residual_anchor_count"] == 3
    assert final_residual["evaluation_payload"]["no_leakage_prior_only"] is True


def test_non_strict_upper_bound_selects_best_available_non_strict_candidate() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=250.0,
        perturb_final_regret=80.0,
        final_anchor_count=18,
    )

    upper_bound = build_dfl_non_strict_oracle_upper_bound_frame(
        library,
        min_final_holdout_tenant_anchor_count_per_source_model=90,
        generated_at=GENERATED_AT,
    )
    outcome = validate_dfl_non_strict_upper_bound_evidence(upper_bound)

    assert upper_bound.height == 180
    assert set(upper_bound["selected_candidate_family"].unique().to_list()) == {"forecast_perturbation"}
    assert upper_bound.select("non_strict_upper_bound_beats_strict").to_series().unique().to_list() == [True]
    assert outcome.passed is True
    assert outcome.metadata["validation_tenant_anchor_count_per_source_model"] == 90


def test_non_strict_upper_bound_blocks_when_no_candidate_can_beat_strict() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=250.0,
        perturb_final_regret=125.0,
        final_anchor_count=18,
    )

    upper_bound = build_dfl_non_strict_oracle_upper_bound_frame(
        library,
        min_final_holdout_tenant_anchor_count_per_source_model=90,
    )
    outcome = validate_dfl_non_strict_upper_bound_evidence(upper_bound)

    assert upper_bound.select("non_strict_upper_bound_beats_strict").to_series().unique().to_list() == [False]
    assert outcome.passed is False
    assert "non-strict oracle upper bound" in outcome.description


def test_strict_baseline_autopsy_marks_high_regret_and_best_non_strict_gap() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=500.0,
        perturb_final_regret=225.0,
        final_anchor_count=18,
    )
    upper_bound = build_dfl_non_strict_oracle_upper_bound_frame(
        library,
        min_final_holdout_tenant_anchor_count_per_source_model=90,
    )

    autopsy = build_dfl_strict_baseline_autopsy_frame(library, upper_bound)

    assert autopsy.height == 180
    assert set(autopsy["strict_high_regret_flag"].unique().to_list()) == {True}
    assert set(autopsy["strict_gap_to_best_non_strict_uah"].unique().to_list()) == {75.0}
    assert set(autopsy["recommended_next_action"].unique().to_list()) == {
        "train_selector_to_detect_strict_failure"
    }


def test_non_strict_upper_bound_rejects_under_coverage_and_non_thesis_rows() -> None:
    under_coverage = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=90.0,
        perturb_final_regret=80.0,
        final_anchor_count=17,
    )
    non_thesis = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=90.0,
        perturb_final_regret=80.0,
        final_anchor_count=18,
        data_quality_tier="demo_grade",
    )

    under_coverage_outcome = validate_dfl_non_strict_upper_bound_evidence(
        build_dfl_non_strict_oracle_upper_bound_frame(
            under_coverage,
            min_final_holdout_tenant_anchor_count_per_source_model=1,
        )
    )
    non_thesis_outcome = validate_dfl_non_strict_upper_bound_evidence(
        build_dfl_non_strict_oracle_upper_bound_frame(
            non_thesis,
            min_final_holdout_tenant_anchor_count_per_source_model=90,
        )
    )

    assert under_coverage_outcome.passed is False
    assert under_coverage_outcome.metadata["minimum_final_holdout_anchors_per_tenant_model"] == 17
    assert non_thesis_outcome.passed is False
    assert "thesis_grade" in non_thesis_outcome.description


def _candidate_library(
    *,
    anchor_count_per_tenant: int,
    final_anchor_count: int,
    data_quality_tier: str = "thesis_grade",
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in CANONICAL_TENANTS:
        for source_model_name in SOURCE_MODELS:
            for anchor_index in range(anchor_count_per_tenant):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                split_name = (
                    "final_holdout"
                    if anchor_index >= anchor_count_per_tenant - final_anchor_count
                    else "train_selection"
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
                            regret=300.0,
                            forecast_prices=(2000.0, 6000.0),
                            data_quality_tier=data_quality_tier,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="forecast_perturbation",
                            candidate_model_name=f"dfl_forecast_perturbation_v1_{source_model_name}",
                            anchor=anchor,
                            split_name=split_name,
                            regret=150.0,
                            forecast_prices=(900.0, 6500.0),
                            data_quality_tier=data_quality_tier,
                        ),
                    ]
                )
    return pl.DataFrame(rows)


def _candidate_library_from_regrets(
    *,
    strict_final_regret: float,
    raw_final_regret: float,
    perturb_final_regret: float,
    final_anchor_count: int,
    data_quality_tier: str = "thesis_grade",
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    train_anchor_count = 2
    total_anchor_count = train_anchor_count + final_anchor_count
    for tenant_id in CANONICAL_TENANTS:
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
                            regret=raw_final_regret if split_name == "final_holdout" else 250.0,
                            forecast_prices=(2000.0, 6000.0),
                            data_quality_tier=data_quality_tier,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="forecast_perturbation",
                            candidate_model_name=f"dfl_forecast_perturbation_v1_{source_model_name}",
                            anchor=anchor,
                            split_name=split_name,
                            regret=perturb_final_regret if split_name == "final_holdout" else 125.0,
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
        "prior_family_mean_regret_uah": regret,
        "safety_violation_count": 0,
        "data_quality_tier": data_quality_tier,
        "observed_coverage_ratio": 1.0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "claim_scope": "dfl_schedule_candidate_library_not_full_dfl",
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


def _benchmark_frame(*, anchor_count_per_tenant: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in CANONICAL_TENANTS:
        for anchor_index in range(anchor_count_per_tenant):
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            for model_name in ("strict_similar_day", *SOURCE_MODELS):
                rows.append(
                    {
                        "tenant_id": tenant_id,
                        "forecast_model_name": model_name,
                        "anchor_timestamp": anchor,
                        "generated_at": GENERATED_AT,
                        "horizon_hours": 2,
                        "evaluation_payload": {
                            "data_quality_tier": "thesis_grade",
                            "observed_coverage_ratio": 1.0,
                            "horizon": [
                                {"interval_start": (anchor + timedelta(hours=1)).isoformat()},
                                {"interval_start": (anchor + timedelta(hours=2)).isoformat()},
                            ],
                        },
                    }
                )
    return pl.DataFrame(rows)
