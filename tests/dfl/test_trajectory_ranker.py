from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.trajectory_ranker import (
    DFL_TRAJECTORY_FEATURE_RANKER_STRICT_LP_STRATEGY_KIND,
    build_dfl_schedule_candidate_library_frame,
    build_dfl_trajectory_feature_ranker_frame,
    build_dfl_trajectory_feature_ranker_strict_lp_benchmark_frame,
    evaluate_dfl_trajectory_feature_ranker_gate,
    trajectory_feature_ranker_model_name,
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


def test_schedule_candidate_library_builds_train_final_and_perturbation_rows() -> None:
    result = build_dfl_schedule_candidate_library_frame(
        _benchmark_frame(anchor_count_per_tenant=4),
        _trajectory_value_panel_frame(anchor_count_per_tenant=4, final_anchor_count=2),
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
        perturb_spread_scale_grid=(1.0,),
        perturb_mean_shift_grid_uah_mwh=(100.0,),
        generated_at=GENERATED_AT,
    )

    assert result.height == 5 * 2 * ((4 * 3) + (2 * 3))
    assert set(result["split_name"].unique().to_list()) == {"train_selection", "final_holdout"}
    assert {
        "strict_control",
        "raw_source",
        "forecast_perturbation",
        "panel_v2",
        "decision_target_v3",
        "action_target_v4",
    }.issubset(set(result["candidate_family"].unique().to_list()))

    perturb = result.filter(pl.col("candidate_family") == "forecast_perturbation").row(0, named=True)
    assert perturb["forecast_price_uah_mwh_vector"] == [2100.0, 6100.0]
    assert perturb["actual_price_uah_mwh_vector"] == [1000.0, 5000.0]
    assert perturb["not_full_dfl"] is True
    assert perturb["not_market_execution"] is True


def test_feature_ranker_uses_train_selection_only_when_final_scores_change() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=300.0,
        perturb_train_regret=50.0,
    )
    mutated_final_library = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=5.0,
        perturb_train_regret=50.0,
    )

    ranker = build_dfl_trajectory_feature_ranker_frame(
        library,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        min_final_holdout_tenant_anchor_count_per_source_model=90,
    )
    mutated_ranker = build_dfl_trajectory_feature_ranker_frame(
        mutated_final_library,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        min_final_holdout_tenant_anchor_count_per_source_model=90,
    )

    assert ranker["selected_weight_profile_name"].to_list() == mutated_ranker[
        "selected_weight_profile_name"
    ].to_list()
    assert ranker["selected_family_counts"].to_list() == mutated_ranker["selected_family_counts"].to_list()
    assert ranker["selected_final_mean_regret_uah"].to_list() == [300.0] * 10
    assert mutated_ranker["selected_final_mean_regret_uah"].to_list() == [5.0] * 10


def test_feature_ranker_strict_benchmark_and_gate_block_against_strict_control() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=300.0,
        perturb_train_regret=50.0,
    )
    ranker = build_dfl_trajectory_feature_ranker_frame(
        library,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        min_final_holdout_tenant_anchor_count_per_source_model=90,
    )

    strict_frame = build_dfl_trajectory_feature_ranker_strict_lp_benchmark_frame(
        library,
        ranker,
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_trajectory_feature_ranker_gate(strict_frame, source_model_names=SOURCE_MODELS)

    assert strict_frame.height == 5 * 2 * 18 * 3
    assert strict_frame.select("strategy_kind").to_series().unique().to_list() == [
        DFL_TRAJECTORY_FEATURE_RANKER_STRICT_LP_STRATEGY_KIND
    ]
    assert strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("dfl_trajectory_feature_ranker_v1_")
    ).height == 180
    assert trajectory_feature_ranker_model_name("tft_silver_v0") in strict_frame[
        "forecast_model_name"
    ].unique().to_list()
    assert gate.passed is False
    assert gate.decision == "diagnostic_pass_production_blocked"
    assert gate.metrics["development_gate_passed"] is True
    assert "strict_similar_day" in gate.description


def test_feature_ranker_gate_promotes_only_when_ranker_beats_strict_control() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=90.0,
        perturb_train_regret=50.0,
    )
    ranker = build_dfl_trajectory_feature_ranker_frame(
        library,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        min_final_holdout_tenant_anchor_count_per_source_model=90,
    )
    strict_frame = build_dfl_trajectory_feature_ranker_strict_lp_benchmark_frame(library, ranker)

    gate = evaluate_dfl_trajectory_feature_ranker_gate(strict_frame, source_model_names=SOURCE_MODELS)

    assert gate.passed is True
    assert gate.decision == "promote"
    assert gate.metrics["mean_regret_improvement_ratio_vs_strict"] == 0.1


def test_feature_ranker_rejects_non_thesis_bad_vectors_and_under_coverage() -> None:
    non_thesis = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=90.0,
        perturb_train_regret=50.0,
        data_quality_tier="demo_grade",
    )
    bad_vectors = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=90.0,
        perturb_train_regret=50.0,
    )
    bad_vector_values = [
        [1000.0] if row["candidate_family"] == "forecast_perturbation" else row["forecast_price_uah_mwh_vector"]
        for row in bad_vectors.iter_rows(named=True)
    ]
    bad_vectors = bad_vectors.with_columns(pl.Series("forecast_price_uah_mwh_vector", bad_vector_values))
    under_coverage = _candidate_library_from_regrets(
        strict_final_regret=100.0,
        raw_final_regret=500.0,
        perturb_final_regret=90.0,
        perturb_train_regret=50.0,
        final_anchor_count=17,
    )

    for frame, message in [
        (non_thesis, "thesis_grade"),
        (bad_vectors, "vector length"),
        (under_coverage, "final-holdout tenant-anchor count"),
    ]:
        try:
            build_dfl_trajectory_feature_ranker_frame(
                frame,
                tenant_ids=CANONICAL_TENANTS,
                forecast_model_names=SOURCE_MODELS,
            )
        except ValueError as exc:
            assert message in str(exc)
        else:
            raise AssertionError("expected invalid ranker evidence to fail")


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
    train_anchor_count = 2
    total_anchor_count = train_anchor_count + final_anchor_count
    for tenant_id in CANONICAL_TENANTS:
        for source_model_name in SOURCE_MODELS:
            for anchor_index in range(total_anchor_count):
                split_name = "final_holdout" if anchor_index >= train_anchor_count else "train_selection"
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
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
        "prior_family_mean_regret_uah": 50.0 if candidate_family == "forecast_perturbation" else regret,
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
        for source_model_name in SOURCE_MODELS:
            for anchor_index in range(anchor_count_per_tenant):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                rows.append(
                    _evaluation_row(
                        tenant_id=tenant_id,
                        source_model_name=source_model_name,
                        model_name="strict_similar_day",
                        anchor=anchor,
                        regret=100.0,
                        forecast_prices=(1000.0, 5000.0),
                    )
                )
                rows.append(
                    _evaluation_row(
                        tenant_id=tenant_id,
                        source_model_name=source_model_name,
                        model_name=source_model_name,
                        anchor=anchor,
                        regret=500.0,
                        forecast_prices=(2000.0, 6000.0),
                    )
                )
    return pl.DataFrame(rows)


def _trajectory_value_panel_frame(*, anchor_count_per_tenant: int, final_anchor_count: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in CANONICAL_TENANTS:
        for source_model_name in SOURCE_MODELS:
            for anchor_index in range(anchor_count_per_tenant - final_anchor_count, anchor_count_per_tenant):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                for family, model_name, regret, prices in [
                    ("panel_v2", f"offline_dfl_panel_v2_{source_model_name}", 400.0, (1200.0, 5200.0)),
                    (
                        "decision_target_v3",
                        f"offline_dfl_decision_target_v3_{source_model_name}",
                        300.0,
                        (1300.0, 5300.0),
                    ),
                    (
                        "action_target_v4",
                        f"offline_dfl_action_target_v4_{source_model_name}",
                        200.0,
                        (1400.0, 5400.0),
                    ),
                ]:
                    rows.append(
                        {
                            **_candidate_row(
                                tenant_id=tenant_id,
                                source_model_name=source_model_name,
                                candidate_family=family,
                                candidate_model_name=model_name,
                                anchor=anchor,
                                split_name="final_holdout",
                                regret=regret,
                                forecast_prices=prices,
                                data_quality_tier="thesis_grade",
                            ),
                            "prior_selection_mean_regret_uah": regret - 25.0,
                        }
                    )
    return pl.DataFrame(rows)


def _evaluation_row(
    *,
    tenant_id: str,
    source_model_name: str,
    model_name: str,
    anchor: datetime,
    regret: float,
    forecast_prices: tuple[float, float],
) -> dict[str, object]:
    return {
        "evaluation_id": f"{tenant_id}:{source_model_name}:{model_name}:{anchor.isoformat()}",
        "tenant_id": tenant_id,
        "forecast_model_name": model_name,
        "strategy_kind": "real_data_rolling_origin_benchmark",
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
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": 0,
            "not_full_dfl": True,
            "not_market_execution": True,
            "source_forecast_model_name": source_model_name,
            "horizon": [
                {
                    "step_index": 0,
                    "interval_start": (anchor + timedelta(hours=1)).isoformat(),
                    "forecast_price_uah_mwh": forecast_prices[0],
                    "actual_price_uah_mwh": 1000.0,
                    "net_power_mw": 0.0,
                    "soc_fraction": 0.5,
                    "degradation_penalty_uah": 0.0,
                },
                {
                    "step_index": 1,
                    "interval_start": (anchor + timedelta(hours=2)).isoformat(),
                    "forecast_price_uah_mwh": forecast_prices[1],
                    "actual_price_uah_mwh": 5000.0,
                    "net_power_mw": 1.0,
                    "soc_fraction": 0.4,
                    "degradation_penalty_uah": 5.0,
                },
            ],
        },
    }
