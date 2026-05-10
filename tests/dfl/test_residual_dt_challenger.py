from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.dfl.offline_dt_candidate import (
    build_dfl_offline_dt_candidate_frame,
    build_dfl_offline_dt_candidate_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.residual_schedule_value import (
    build_dfl_residual_dt_fallback_strict_lp_benchmark_frame,
    build_dfl_residual_schedule_value_model_frame,
    build_dfl_residual_schedule_value_strict_lp_benchmark_frame,
    evaluate_dfl_residual_dt_fallback_gate,
)
from smart_arbitrage.dfl.trajectory_dataset import (
    build_dfl_real_data_trajectory_dataset_frame,
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
GENERATED_AT = datetime(2026, 5, 10, 12)


def test_real_data_trajectory_dataset_computes_return_to_go_and_blocks_final_teachers() -> None:
    library = _candidate_library(
        anchor_count_per_tenant=4,
        final_anchor_count=1,
        residual_train_regret=80.0,
        residual_final_regret=70.0,
    )

    trajectory = build_dfl_real_data_trajectory_dataset_frame(
        library,
        _prior_feature_panel(library),
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=1,
    )

    strict_episode = trajectory.filter(
        (pl.col("tenant_id") == TENANTS[0])
        & (pl.col("source_model_name") == SOURCE_MODELS[0])
        & (pl.col("candidate_model_name") == "strict_similar_day")
        & (pl.col("anchor_timestamp") == FIRST_ANCHOR)
    ).sort("horizon_step")

    assert strict_episode["label_reward_uah"].to_list() == [10.0, 20.0]
    assert strict_episode["label_return_to_go_uah"].to_list() == [30.0, 20.0]
    assert strict_episode["teacher_label_allowed"].to_list() == [True, True]
    assert set(strict_episode["teacher_candidate_model_name"].to_list()) == {
        "dfl_residual_family_tft_silver_v0"
    }

    final_rows = trajectory.filter(pl.col("split_name") == "final_holdout")
    assert final_rows.select("teacher_label_allowed").to_series().unique().to_list() == [False]
    assert final_rows.select("teacher_candidate_model_name").to_series().unique().to_list() == [None]
    assert final_rows.select("not_full_dfl").to_series().unique().to_list() == [True]
    assert final_rows.select("not_market_execution").to_series().unique().to_list() == [True]


def test_real_data_trajectory_dataset_rejects_non_thesis_rows_and_bad_vectors() -> None:
    non_thesis = _candidate_library(
        anchor_count_per_tenant=4,
        final_anchor_count=1,
        residual_train_regret=80.0,
        residual_final_regret=70.0,
        data_quality_tier="demo_grade",
    )
    bad_vector_rows = _candidate_library(
        anchor_count_per_tenant=4,
        final_anchor_count=1,
        residual_train_regret=80.0,
        residual_final_regret=70.0,
    ).to_dicts()
    bad_vector_rows[0]["dispatch_mw_vector"] = [1.0]

    with pytest.raises(ValueError, match="thesis_grade"):
        build_dfl_real_data_trajectory_dataset_frame(
            non_thesis,
            _prior_feature_panel(non_thesis),
            tenant_ids=TENANTS,
            forecast_model_names=SOURCE_MODELS,
            final_validation_anchor_count_per_tenant=1,
        )
    with pytest.raises(ValueError, match="vector length"):
        build_dfl_real_data_trajectory_dataset_frame(
            pl.DataFrame(bad_vector_rows),
            _prior_feature_panel(pl.DataFrame(bad_vector_rows)),
            tenant_ids=TENANTS,
            forecast_model_names=SOURCE_MODELS,
            final_validation_anchor_count_per_tenant=1,
        )


def test_residual_model_uses_train_anchors_and_final_mutation_does_not_change_weights() -> None:
    library = _candidate_library(
        anchor_count_per_tenant=5,
        final_anchor_count=2,
        residual_train_regret=80.0,
        residual_final_regret=70.0,
    )
    mutated_final = _mutate_final_actuals_and_regret(library, final_regret=40.0)
    trajectory = build_dfl_real_data_trajectory_dataset_frame(
        library,
        _prior_feature_panel(library),
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
    )
    mutated_trajectory = build_dfl_real_data_trajectory_dataset_frame(
        mutated_final,
        _prior_feature_panel(library),
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
    )

    model = build_dfl_residual_schedule_value_model_frame(
        trajectory,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
        switch_margin_grid_uah=(0.0, 50.0),
    )
    mutated_model = build_dfl_residual_schedule_value_model_frame(
        mutated_trajectory,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
        switch_margin_grid_uah=(0.0, 50.0),
    )

    assert model.select("selected_switch_margin_uah").to_series().to_list() == (
        mutated_model.select("selected_switch_margin_uah").to_series().to_list()
    )
    assert model.select("train_mean_regret_improvement_ratio_vs_strict").to_series().to_list() == (
        mutated_model.select("train_mean_regret_improvement_ratio_vs_strict").to_series().to_list()
    )

    strict_frame = build_dfl_residual_schedule_value_strict_lp_benchmark_frame(
        library,
        model,
        final_validation_anchor_count_per_tenant=2,
        generated_at=GENERATED_AT,
    )
    mutated_strict_frame = build_dfl_residual_schedule_value_strict_lp_benchmark_frame(
        mutated_final,
        mutated_model,
        final_validation_anchor_count_per_tenant=2,
        generated_at=GENERATED_AT,
    )

    assert strict_frame.filter(pl.col("selection_role") == "residual_selector")["regret_uah"].mean() == 70.0
    assert mutated_strict_frame.filter(pl.col("selection_role") == "residual_selector")["regret_uah"].mean() == 40.0
    assert _strategy_store_columns().issubset(set(strict_frame.columns))


def test_offline_dt_candidate_filters_train_trajectories_and_compares_behavior_cloning() -> None:
    library = _candidate_library(
        anchor_count_per_tenant=5,
        final_anchor_count=2,
        residual_train_regret=75.0,
        residual_final_regret=60.0,
    )
    trajectory = build_dfl_real_data_trajectory_dataset_frame(
        library,
        _prior_feature_panel(library),
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
    )

    candidate = build_dfl_offline_dt_candidate_frame(
        trajectory,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
        high_value_quantile=0.5,
        max_epochs=2,
        random_seed=7,
    )
    strict_frame = build_dfl_offline_dt_candidate_strict_lp_benchmark_frame(
        library,
        candidate,
        final_validation_anchor_count_per_tenant=2,
        generated_at=GENERATED_AT,
    )

    assert candidate.select("filtered_train_trajectory_count").to_series().min() > 0
    assert candidate.select("dt_context_length").to_series().unique().to_list() == [24]
    assert candidate.select("dt_hidden_dim").to_series().unique().to_list() == [32]
    assert {"offline_dt", "filtered_behavior_cloning", "strict_reference"}.issubset(
        set(strict_frame["selection_role"].unique().to_list())
    )
    assert strict_frame.filter(pl.col("selection_role") == "offline_dt").height == 20
    assert _strategy_store_columns().issubset(set(strict_frame.columns))


def test_residual_dt_fallback_defaults_to_strict_without_confidence_and_gate_passes_only_when_it_beats_strict() -> None:
    weak_library = _candidate_library(
        anchor_count_per_tenant=5,
        final_anchor_count=2,
        residual_train_regret=98.0,
        residual_final_regret=50.0,
    )
    weak_trajectory = build_dfl_real_data_trajectory_dataset_frame(
        weak_library,
        _prior_feature_panel(weak_library),
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
    )
    weak_residual = build_dfl_residual_schedule_value_model_frame(
        weak_trajectory,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
    )
    weak_dt = build_dfl_offline_dt_candidate_frame(
        weak_trajectory,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
    )
    weak_fallback = build_dfl_residual_dt_fallback_strict_lp_benchmark_frame(
        weak_library,
        weak_residual,
        weak_dt,
        final_validation_anchor_count_per_tenant=2,
        generated_at=GENERATED_AT,
    )
    weak_gate = evaluate_dfl_residual_dt_fallback_gate(
        weak_fallback,
        source_model_names=SOURCE_MODELS,
        min_validation_tenant_anchor_count=10,
    )

    assert set(weak_fallback.filter(pl.col("selection_role") == "fallback_strategy")["selected_strategy_source"].unique()) == {
        "strict_similar_day"
    }
    assert weak_gate.passed is False

    strong_library = _candidate_library(
        anchor_count_per_tenant=5,
        final_anchor_count=2,
        residual_train_regret=75.0,
        residual_final_regret=50.0,
    )
    strong_trajectory = build_dfl_real_data_trajectory_dataset_frame(
        strong_library,
        _prior_feature_panel(strong_library),
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
    )
    strong_residual = build_dfl_residual_schedule_value_model_frame(
        strong_trajectory,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
    )
    strong_dt = build_dfl_offline_dt_candidate_frame(
        strong_trajectory,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=2,
    )
    strong_fallback = build_dfl_residual_dt_fallback_strict_lp_benchmark_frame(
        strong_library,
        strong_residual,
        strong_dt,
        final_validation_anchor_count_per_tenant=2,
        generated_at=GENERATED_AT,
    )
    strong_gate = evaluate_dfl_residual_dt_fallback_gate(
        strong_fallback,
        source_model_names=SOURCE_MODELS,
        min_validation_tenant_anchor_count=10,
    )

    assert set(strong_fallback.filter(pl.col("selection_role") == "fallback_strategy")["selected_strategy_source"].unique()) == {
        "dfl_residual_schedule_value_v1"
    }
    assert strong_gate.passed is True
    assert strong_gate.metrics["best_source_model_name"] in SOURCE_MODELS
    assert strong_gate.metrics["validation_tenant_anchor_count"] == 10
    assert _strategy_store_columns().issubset(set(strong_fallback.columns))


def _candidate_library(
    *,
    anchor_count_per_tenant: int,
    final_anchor_count: int,
    residual_train_regret: float,
    residual_final_regret: float,
    data_quality_tier: str = "thesis_grade",
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for source_model_name in SOURCE_MODELS:
            for anchor_index in range(anchor_count_per_tenant):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                split_name = (
                    "final_holdout"
                    if anchor_index >= anchor_count_per_tenant - final_anchor_count
                    else "train_selection"
                )
                residual_regret = (
                    residual_final_regret if split_name == "final_holdout" else residual_train_regret
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
                            decision_value=900.0,
                            forecast_prices=[10.0, 20.0],
                            actual_prices=[10.0, 20.0],
                            dispatch=[1.0, 1.0],
                            data_quality_tier=data_quality_tier,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="raw_source",
                            candidate_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            regret=160.0,
                            decision_value=840.0,
                            forecast_prices=[11.0, 19.0],
                            actual_prices=[10.0, 20.0],
                            dispatch=[0.0, 1.0],
                            data_quality_tier=data_quality_tier,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="strict_prior_residual_v2",
                            candidate_model_name=f"dfl_residual_family_{source_model_name}",
                            anchor=anchor,
                            split_name=split_name,
                            regret=residual_regret,
                            decision_value=1000.0 - residual_regret,
                            forecast_prices=[9.0, 21.0],
                            actual_prices=[10.0, 20.0],
                            dispatch=[1.0, 1.0],
                            data_quality_tier=data_quality_tier,
                        ),
                    ]
                )
    return pl.DataFrame(rows)


def _strategy_store_columns() -> set[str]:
    return {
        "starting_soc_fraction",
        "starting_soc_source",
        "committed_action",
        "committed_power_mw",
        "rank_by_regret",
    }


def _candidate_row(
    *,
    tenant_id: str,
    source_model_name: str,
    candidate_family: str,
    candidate_model_name: str,
    anchor: datetime,
    split_name: str,
    regret: float,
    decision_value: float,
    forecast_prices: list[float],
    actual_prices: list[float],
    dispatch: list[float],
    data_quality_tier: str,
) -> dict[str, object]:
    horizon = [
        {
            "step_index": step_index,
            "interval_start": (anchor + timedelta(hours=step_index + 1)).isoformat(),
            "forecast_price_uah_mwh": forecast_prices[step_index],
            "actual_price_uah_mwh": actual_prices[step_index],
            "net_power_mw": dispatch[step_index],
            "soc_fraction": 0.5 + step_index * 0.01,
            "degradation_penalty_uah": 0.0,
        }
        for step_index in range(len(forecast_prices))
    ]
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "candidate_family": candidate_family,
        "candidate_model_name": candidate_model_name,
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "split_name": split_name,
        "horizon_hours": len(forecast_prices),
        "forecast_price_uah_mwh_vector": forecast_prices,
        "actual_price_uah_mwh_vector": actual_prices,
        "dispatch_mw_vector": dispatch,
        "soc_fraction_vector": [0.5, 0.51],
        "decision_value_uah": decision_value,
        "forecast_objective_value_uah": decision_value - 10.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": sum(abs(value) for value in dispatch),
        "forecast_spread_uah_mwh": max(forecast_prices) - min(forecast_prices),
        "safety_violation_count": 0,
        "data_quality_tier": data_quality_tier,
        "observed_coverage_ratio": 1.0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evaluation_payload": {
            "data_quality_tier": data_quality_tier,
            "observed_coverage_ratio": 1.0,
            "source_forecast_model_name": source_model_name,
            "candidate_family": candidate_family,
            "candidate_model_name": candidate_model_name,
            "safety_violation_count": 0,
            "horizon": horizon,
            "not_full_dfl": True,
            "not_market_execution": True,
        },
    }


def _prior_feature_panel(library: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in library.iter_rows(named=True):
        anchor = row["anchor_timestamp"]
        rows.append(
            {
                "tenant_id": row["tenant_id"],
                "source_model_name": row["source_model_name"],
                "window_index": 1 if row["split_name"] == "final_holdout" else 2,
                "anchor_timestamp": anchor,
                "prior_cutoff_timestamp": anchor,
                "selector_feature_prior_anchor_count": 3,
                "selector_feature_prior_strict_mean_regret_uah": 100.0,
                "selector_feature_prior_raw_mean_regret_uah": 160.0,
                "selector_feature_prior_best_non_strict_mean_regret_uah": 80.0,
                "selector_feature_prior_price_spread_std_uah_mwh": 5.0,
                "selector_feature_prior_net_load_mean_mw": 1.25,
                "analysis_only_strict_regret_uah": 100.0,
                "analysis_only_raw_regret_uah": 160.0,
                "analysis_only_selected_regret_uah": 80.0,
                "analysis_only_selected_candidate_family": "strict_prior_residual_v2",
                "analysis_only_selector_beats_strict": True,
                "claim_scope": "dfl_strict_failure_prior_feature_panel_not_full_dfl",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(rows).unique(
        subset=["tenant_id", "source_model_name", "anchor_timestamp"],
        keep="first",
    )


def _mutate_final_actuals_and_regret(library: pl.DataFrame, *, final_regret: float) -> pl.DataFrame:
    rows = []
    for row in library.iter_rows(named=True):
        copied = dict(row)
        if row["split_name"] == "final_holdout" and row["candidate_family"] == "strict_prior_residual_v2":
            copied["regret_uah"] = final_regret
            copied["decision_value_uah"] = 1000.0 - final_regret
            copied["actual_price_uah_mwh_vector"] = [25.0, 50.0]
            payload = dict(copied["evaluation_payload"])
            horizon = [dict(point) for point in payload["horizon"]]
            for step_index, point in enumerate(horizon):
                point["actual_price_uah_mwh"] = [25.0, 50.0][step_index]
            payload["horizon"] = horizon
            copied["evaluation_payload"] = payload
        rows.append(copied)
    return pl.DataFrame(rows)
