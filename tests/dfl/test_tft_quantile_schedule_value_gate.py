from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.tft_quantile_schedule_value import (
    DFL_TFT_AUGMENTED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
    DFL_TFT_COMBINED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
    TFT_QUANTILE_CALIBRATED_SOURCE_MODELS,
    TFT_QUANTILE_SOURCE_MODELS,
    build_dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame,
    build_dfl_tft_combined_v2_plus_strict_lp_benchmark_frame,
    build_dfl_tft_quantile_schedule_candidate_library_frame,
    evaluate_dfl_tft_augmented_v2_plus_gate,
    evaluate_dfl_tft_combined_v2_plus_gate,
)

TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
GENERATED_AT = datetime(2026, 5, 18, 12)
FIRST_ANCHOR = datetime(2026, 1, 1, 23)


def test_tft_quantile_candidate_library_keeps_quantile_sources() -> None:
    benchmark = _strict_benchmark_frame(
        tenant_ids=TENANTS[:1],
        source_model_names=TFT_QUANTILE_SOURCE_MODELS,
        anchor_count=2,
    )

    library = build_dfl_tft_quantile_schedule_candidate_library_frame(
        benchmark,
        tenant_ids=TENANTS[:1],
        final_validation_anchor_count_per_tenant=1,
    )

    assert set(library["source_model_name"].unique().to_list()) == set(
        TFT_QUANTILE_SOURCE_MODELS
    )
    assert set(library["source_quantile"].unique().to_list()) == {"p10", "p50", "p90"}
    assert library.filter(pl.col("split_name") == "final_holdout").height > 0
    assert set(library["market_execution_enabled"].unique().to_list()) == {False}


def test_tft_quantile_candidate_library_keeps_calibrated_quantile_sources() -> None:
    benchmark = _strict_benchmark_frame(
        tenant_ids=TENANTS[:1],
        source_model_names=TFT_QUANTILE_CALIBRATED_SOURCE_MODELS,
        anchor_count=2,
    )

    library = build_dfl_tft_quantile_schedule_candidate_library_frame(
        benchmark,
        tenant_ids=TENANTS[:1],
        forecast_model_names=TFT_QUANTILE_CALIBRATED_SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=1,
    )

    assert set(library["source_model_name"].unique().to_list()) == set(
        TFT_QUANTILE_CALIBRATED_SOURCE_MODELS
    )
    assert set(library["source_quantile"].unique().to_list()) == {"p10", "p50", "p90"}


def test_tft_augmented_gate_requires_beating_frozen_v2_plus() -> None:
    baseline = _v2_plus_strict_frame(
        source_model_name="nbeatsx_official_global_panel_horizon_calibrated_v1",
        learner_regret=170.0,
        median_regret=150.0,
    )
    weak_tft = _v2_plus_strict_frame(
        source_model_name="tft_official_global_panel_v1",
        learner_regret=190.0,
        median_regret=160.0,
    )
    strong_tft = _v2_plus_strict_frame(
        source_model_name="tft_official_global_panel_v1",
        learner_regret=150.0,
        median_regret=140.0,
    )

    weak_gate = evaluate_dfl_tft_augmented_v2_plus_gate(
        pl.concat([baseline, weak_tft], how="diagonal_relaxed"),
        baseline_source_model_name="nbeatsx_official_global_panel_horizon_calibrated_v1",
        tft_source_model_names=("tft_official_global_panel_v1",),
        min_validation_tenant_anchor_count=5,
    )
    strong_gate = evaluate_dfl_tft_augmented_v2_plus_gate(
        pl.concat([baseline, strong_tft], how="diagonal_relaxed"),
        baseline_source_model_name="nbeatsx_official_global_panel_horizon_calibrated_v1",
        tft_source_model_names=("tft_official_global_panel_v1",),
        min_validation_tenant_anchor_count=5,
    )

    assert weak_gate.passed is False
    assert weak_gate.decision == "blocked"
    assert strong_gate.passed is True
    assert strong_gate.decision == "promote"
    assert strong_gate.metrics["market_execution_enabled"] is False


def test_tft_augmented_frame_blocks_screen_only_library_without_train_rows() -> None:
    benchmark = _strict_benchmark_frame(
        tenant_ids=TENANTS,
        source_model_names=TFT_QUANTILE_SOURCE_MODELS,
        anchor_count=1,
    )
    library = build_dfl_tft_quantile_schedule_candidate_library_frame(
        benchmark,
        tenant_ids=TENANTS,
        final_validation_anchor_count_per_tenant=1,
    )
    baseline = _v2_plus_strict_frame(
        source_model_name="nbeatsx_official_global_panel_horizon_calibrated_v1",
        learner_regret=170.0,
        median_regret=150.0,
    )

    strict_frame = build_dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame(
        library,
        baseline,
        tenant_ids=TENANTS,
        final_validation_anchor_count_per_tenant=1,
    )

    assert strict_frame.height == baseline.height
    assert set(strict_frame["tft_gate_blocker"].unique().to_list()) == {
        "missing_tft_train_rows"
    }
    gate = evaluate_dfl_tft_augmented_v2_plus_gate(
        strict_frame,
        min_validation_tenant_anchor_count=5,
    )
    assert gate.passed is False
    assert gate.decision == "blocked"


def test_tft_combined_selector_falls_back_to_frozen_v2_plus_when_prior_is_weak() -> None:
    tft_library = _tft_candidate_library(
        tft_train_regret=220.0,
        tft_final_regret=80.0,
    )
    frozen_strict = _v2_plus_strict_frame(
        source_model_name="nbeatsx_official_global_panel_horizon_calibrated_v1",
        learner_regret=170.0,
        median_regret=150.0,
    )
    frozen_model = _frozen_v2_plus_model_frame(selected_train_mean_regret=170.0)

    strict_frame = build_dfl_tft_combined_v2_plus_strict_lp_benchmark_frame(
        tft_library,
        frozen_strict,
        frozen_model,
        tenant_ids=TENANTS,
        tft_source_model_names=("tft_official_global_panel_v1",),
        min_prior_mean_improvement_ratio_vs_v2_plus=0.01,
    )

    combined_rows = strict_frame.filter(
        pl.col("selection_role") == "tft_combined_v2_plus"
    )
    gate = evaluate_dfl_tft_combined_v2_plus_gate(
        strict_frame,
        min_validation_tenant_anchor_count=5,
    )

    assert combined_rows.height == 5
    assert set(combined_rows["fallback_to_v2_plus"].to_list()) == {True}
    assert combined_rows["regret_uah"].to_list() == (
        frozen_strict.filter(
            pl.col("selection_role") == "schedule_value_learner_v2_plus"
        )["regret_uah"].to_list()
    )
    assert gate.passed is False
    assert gate.decision == "blocked"
    assert gate.metrics["fallback_to_v2_plus_count"] == 5


def test_tft_combined_selector_can_pass_when_tft_prior_and_final_beat_v2_plus() -> None:
    tft_library = _tft_candidate_library(
        tft_train_regret=90.0,
        tft_final_regret=110.0,
    )
    frozen_strict = _v2_plus_strict_frame(
        source_model_name="nbeatsx_official_global_panel_horizon_calibrated_v1",
        learner_regret=170.0,
        median_regret=150.0,
    )
    frozen_model = _frozen_v2_plus_model_frame(selected_train_mean_regret=170.0)

    strict_frame = build_dfl_tft_combined_v2_plus_strict_lp_benchmark_frame(
        tft_library,
        frozen_strict,
        frozen_model,
        tenant_ids=TENANTS,
        tft_source_model_names=("tft_official_global_panel_v1",),
        min_prior_mean_improvement_ratio_vs_v2_plus=0.01,
    )

    combined_rows = strict_frame.filter(
        pl.col("selection_role") == "tft_combined_v2_plus"
    )
    gate = evaluate_dfl_tft_combined_v2_plus_gate(
        strict_frame,
        min_validation_tenant_anchor_count=5,
    )

    assert set(combined_rows["fallback_to_v2_plus"].to_list()) == {False}
    assert set(combined_rows["source_model_name"].unique().to_list()) == {
        "nbeatsx_tft_official_global_panel_combined_v1"
    }
    assert set(combined_rows["selected_tft_source_model_name"].unique().to_list()) == {
        "tft_official_global_panel_v1"
    }
    assert set(strict_frame["strategy_kind"].unique().to_list()) == {
        DFL_TFT_COMBINED_V2_PLUS_STRICT_LP_STRATEGY_KIND
    }
    assert gate.passed is True
    assert gate.decision == "promote"
    assert gate.metrics["combined_mean_regret_uah"] < gate.metrics[
        "baseline_mean_regret_uah"
    ]


def _strict_benchmark_frame(
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    anchor_count: int,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in tenant_ids:
        for anchor_index in range(anchor_count):
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            rows.append(
                _evaluation_row(
                    tenant_id=tenant_id,
                    source_model_name="strict_similar_day",
                    model_name="strict_similar_day",
                    anchor=anchor,
                    regret=300.0,
                )
            )
            for source_model_name in source_model_names:
                rows.append(
                    _evaluation_row(
                        tenant_id=tenant_id,
                        source_model_name=source_model_name,
                        model_name=source_model_name,
                        anchor=anchor,
                        regret=250.0,
                    )
                )
    return pl.DataFrame(rows)


def _tft_candidate_library(
    *,
    tft_train_regret: float,
    tft_final_regret: float,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_index, tenant_id in enumerate(TENANTS):
        for split_name, anchor in [
            ("train_selection", FIRST_ANCHOR - timedelta(days=1)),
            ("final_holdout", FIRST_ANCHOR + timedelta(days=tenant_index)),
        ]:
            rows.append(
                _candidate_row(
                    tenant_id=tenant_id,
                    source_model_name="tft_official_global_panel_v1",
                    candidate_family="raw_source",
                    candidate_model_name="tft_official_global_panel_v1",
                    anchor=anchor,
                    split_name=split_name,
                    regret=tft_final_regret
                    if split_name == "final_holdout"
                    else tft_train_regret,
                    forecast_prices=(5000.0, 1000.0),
                    prior_family_mean_regret=tft_train_regret,
                )
            )
    return pl.DataFrame(rows).with_columns(
        [
            pl.lit("p50").alias("source_quantile"),
            pl.lit(True).alias("quantile_candidate_lane"),
            pl.lit(False).alias("market_execution_enabled"),
        ]
    )


def _frozen_v2_plus_model_frame(*, selected_train_mean_regret: float) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "tenant_id": tenant_id,
                "source_model_name": (
                    "nbeatsx_official_global_panel_horizon_calibrated_v1"
                ),
                "selected_train_mean_regret_uah": selected_train_mean_regret,
                "selected_candidate_family": "rank_extrema_perturbation_v2_plus",
                "fallback_to_v2": False,
                "fallback_to_v2_plus": False,
                "market_execution_enabled": False,
                "not_market_execution": True,
            }
            for tenant_id in TENANTS
        ]
    )


def _v2_plus_strict_frame(
    *,
    source_model_name: str,
    learner_regret: float,
    median_regret: float,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_index, tenant_id in enumerate(TENANTS):
        regret = median_regret if tenant_index < 3 else learner_regret + 30.0
        anchor = FIRST_ANCHOR + timedelta(days=tenant_index)
        for role, model_name, role_regret in [
            ("strict_reference", "strict_similar_day", 310.0),
            ("raw_reference", source_model_name, 400.0),
            (
                "schedule_value_learner_v2_reference",
                f"dfl_schedule_value_learner_v2_{source_model_name}",
                210.0,
            ),
            (
                "schedule_value_learner_v2_plus",
                f"dfl_schedule_value_learner_v2_plus_{source_model_name}",
                regret,
            ),
        ]:
            row = _evaluation_row(
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                model_name=model_name,
                anchor=anchor,
                regret=role_regret,
            )
            row["strategy_kind"] = DFL_TFT_AUGMENTED_V2_PLUS_STRICT_LP_STRATEGY_KIND
            row["selection_role"] = role
            row["market_execution_enabled"] = False
            rows.append(row)
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
    forecast_prices: tuple[float, ...],
    prior_family_mean_regret: float,
) -> dict[str, object]:
    actual_prices = [1000.0, 5200.0][: len(forecast_prices)]
    dispatch = [0.0, 1.0][: len(forecast_prices)]
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "candidate_family": candidate_family,
        "candidate_model_name": candidate_model_name,
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "split_name": split_name,
        "horizon_hours": len(forecast_prices),
        "forecast_price_uah_mwh_vector": list(forecast_prices),
        "actual_price_uah_mwh_vector": actual_prices,
        "dispatch_mw_vector": dispatch,
        "soc_fraction_vector": [0.5, 0.4][: len(forecast_prices)],
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": 0.1,
        "forecast_spread_uah_mwh": max(forecast_prices) - min(forecast_prices),
        "actual_spread_uah_mwh": max(actual_prices) - min(actual_prices),
        "forecast_top_k_actual_overlap": 0.5,
        "forecast_bottom_k_actual_overlap": 0.5,
        "peak_index_abs_error": 1.0,
        "trough_index_abs_error": 1.0,
        "soc_min_slack_fraction": 0.4,
        "prior_family_mean_regret_uah": prior_family_mean_regret,
        "safety_violation_count": 0,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "claim_scope": "dfl_tft_quantile_schedule_candidate_library_not_full_dfl",
        "evaluation_payload": {
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "not_full_dfl": True,
            "not_market_execution": True,
            "source_forecast_model_name": source_model_name,
            "horizon": [
                {
                    "step_index": step_index,
                    "interval_start": (
                        anchor + timedelta(hours=step_index + 1)
                    ).isoformat(),
                    "forecast_price_uah_mwh": forecast_price,
                    "actual_price_uah_mwh": actual_prices[step_index],
                    "net_power_mw": dispatch[step_index],
                    "soc_fraction": [0.5, 0.4][step_index],
                    "degradation_penalty_uah": 0.0,
                }
                for step_index, forecast_price in enumerate(forecast_prices)
            ],
        },
    }


def _evaluation_row(
    *,
    tenant_id: str,
    source_model_name: str,
    model_name: str,
    anchor: datetime,
    regret: float,
) -> dict[str, object]:
    forecast_prices = [1000.0, 5000.0]
    actual_prices = [1000.0, 5200.0]
    return {
        "evaluation_id": f"{tenant_id}:{model_name}:{anchor:%Y%m%dT%H%M}",
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "forecast_model_name": model_name,
        "strategy_kind": "unit_strict_lp",
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
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": 0.1,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "selection_role": "raw_reference",
        "evaluation_payload": {
            "claim_scope": "unit",
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "not_full_dfl": True,
            "not_market_execution": True,
            "source_forecast_model_name": source_model_name,
            "horizon": [
                {
                    "step_index": index,
                    "interval_start": (anchor + timedelta(hours=index + 1)).isoformat(),
                    "forecast_price_uah_mwh": forecast_prices[index],
                    "actual_price_uah_mwh": actual_prices[index],
                    "net_power_mw": 0.0,
                    "soc_fraction": 0.5,
                    "degradation_penalty_uah": 0.0,
                }
                for index in range(2)
            ],
        },
    }
