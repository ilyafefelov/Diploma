from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.schedule_value_learner import (
    build_dfl_schedule_value_learner_v2_frame,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND,
    build_dfl_schedule_candidate_library_v2_plus_frame,
    build_dfl_schedule_value_regret_decomposition_frame,
    build_dfl_schedule_value_learner_v2_plus_frame,
    build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
    evaluate_dfl_schedule_value_learner_v2_plus_gate,
    schedule_value_learner_v2_plus_model_name,
    validate_dfl_schedule_value_learner_v2_plus_evidence,
)

TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
SOURCE_MODELS: tuple[str, ...] = (
    "nbeatsx_official_global_panel_v1",
    "nbeatsx_official_global_panel_horizon_calibrated_v1",
)
FIRST_ANCHOR = datetime(2026, 1, 1, 23)
GENERATED_AT = datetime(2026, 5, 15, 12)


def test_regret_decomposition_identifies_selector_wrong_family() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=700.0,
        v2_final_regret=220.0,
        v2_plus_final_regret=120.0,
        v2_plus_train_regret=30.0,
    )
    v2_model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )

    frame = build_dfl_schedule_value_regret_decomposition_frame(library, v2_model)

    assert frame.height == len(TENANTS) * len(SOURCE_MODELS) * 18
    assert set(frame["failure_mode"].unique().to_list()) == {
        "selector_chose_wrong_family"
    }
    assert frame["regret_gap_v2_to_best_candidate_uah"].min() == 100.0
    assert set(frame["not_market_execution"].unique().to_list()) == {True}


def test_regret_decomposition_fails_without_strict_or_oracle_rows() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=700.0,
        v2_final_regret=220.0,
        v2_plus_final_regret=120.0,
        v2_plus_train_regret=30.0,
    )
    v2_model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )
    no_strict = library.filter(pl.col("candidate_family") != "strict_control")
    no_oracle = library.drop("oracle_value_uah")

    for bad_frame, message in [
        (no_strict, "missing strict_control"),
        (no_oracle, "oracle_value_uah"),
    ]:
        try:
            build_dfl_schedule_value_regret_decomposition_frame(bad_frame, v2_model)
        except ValueError as exc:
            assert message in str(exc)
        else:
            raise AssertionError(f"expected ValueError containing {message}")


def test_candidate_library_v2_plus_adds_deterministic_prior_safe_families() -> None:
    source_library = _source_candidate_library_for_generation()

    first = build_dfl_schedule_candidate_library_v2_plus_frame(
        source_library,
        rank_perturbation_delta_uah_mwh=100.0,
        robust_spread_scales=(0.8,),
        strict_neighborhood_shift_hours=(1,),
        block_reconcile_hours=(2,),
        terminal_target_shift_uah_mwh=50.0,
    )
    second = build_dfl_schedule_candidate_library_v2_plus_frame(
        source_library,
        rank_perturbation_delta_uah_mwh=100.0,
        robust_spread_scales=(0.8,),
        strict_neighborhood_shift_hours=(1,),
        block_reconcile_hours=(2,),
        terminal_target_shift_uah_mwh=50.0,
    )

    assert first.to_dicts() == second.to_dicts()
    assert {
        "rank_extrema_perturbation_v2_plus",
        "robust_spread_penalty_v2_plus",
        "strict_neighborhood_shift_v2_plus",
        "temporal_block_reconciled_v2_plus",
        "soc_terminal_target_v2_plus",
    }.issubset(set(first["candidate_family"].unique().to_list()))
    assert {tuple(values) for values in first["actual_price_uah_mwh_vector"].to_list()} == {
        (1000.0, 5000.0, 1500.0, 4000.0)
    }


def test_v2_plus_selector_falls_back_to_v2_without_prior_confidence() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=700.0,
        v2_final_regret=200.0,
        v2_plus_final_regret=50.0,
        v2_plus_train_regret=250.0,
    )
    v2_model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )

    v2_plus_model = build_dfl_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )

    assert set(v2_plus_model["fallback_to_v2"].to_list()) == {True}
    assert set(v2_plus_model["selected_final_mean_regret_uah"].to_list()) == {200.0}


def test_v2_plus_strict_benchmark_gate_passes_only_when_beating_v2_and_strict() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=700.0,
        v2_final_regret=220.0,
        v2_plus_final_regret=180.0,
        v2_plus_train_regret=30.0,
    )
    v2_model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )
    v2_plus_model = build_dfl_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )

    strict_frame = build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        library,
        v2_plus_model,
        v2_model,
        generated_at=GENERATED_AT,
    )
    evidence = validate_dfl_schedule_value_learner_v2_plus_evidence(
        strict_frame,
        source_model_names=SOURCE_MODELS,
    )
    gate = evaluate_dfl_schedule_value_learner_v2_plus_gate(
        strict_frame,
        source_model_names=SOURCE_MODELS,
    )

    assert strict_frame.height == 5 * 2 * 18 * 4
    assert strict_frame.select("strategy_kind").to_series().unique().to_list() == [
        DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND
    ]
    assert set(strict_frame["selection_role"].unique().to_list()) == {
        "raw_reference",
        "schedule_value_learner_v2_plus",
        "schedule_value_learner_v2_reference",
        "strict_reference",
    }
    assert (
        schedule_value_learner_v2_plus_model_name(
            "nbeatsx_official_global_panel_v1"
        )
        in strict_frame["forecast_model_name"].unique().to_list()
    )
    assert evidence.passed is True
    assert gate.passed is True
    assert gate.decision == "promote"
    assert gate.metrics["mean_regret_improvement_ratio_vs_v2"] > 0.0
    assert gate.metrics["mean_regret_improvement_ratio_vs_strict"] >= 0.05


def _candidate_library_from_regrets(
    *,
    strict_final_regret: float,
    raw_final_regret: float,
    v2_final_regret: float,
    v2_plus_final_regret: float,
    v2_plus_train_regret: float,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    train_anchor_count = 3
    final_anchor_count = 18
    for tenant_id in TENANTS:
        for source_model_name in SOURCE_MODELS:
            for anchor_index in range(train_anchor_count + final_anchor_count):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                split_name = (
                    "final_holdout"
                    if anchor_index >= train_anchor_count
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
                            regret=(
                                strict_final_regret
                                if split_name == "final_holdout"
                                else 300.0
                            ),
                            forecast_prices=(1000.0, 5000.0),
                            prior_family_mean_regret=300.0,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="raw_source",
                            candidate_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            regret=(
                                raw_final_regret
                                if split_name == "final_holdout"
                                else 700.0
                            ),
                            forecast_prices=(5000.0, 1000.0),
                            prior_family_mean_regret=700.0,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="strict_prior_residual_v2",
                            candidate_model_name=(
                                "dfl_schedule_library_v2_prior_residual_"
                                f"{source_model_name}"
                            ),
                            anchor=anchor,
                            split_name=split_name,
                            regret=(
                                v2_final_regret
                                if split_name == "final_holdout"
                                else 80.0
                            ),
                            forecast_prices=(900.0, 6500.0),
                            prior_family_mean_regret=80.0,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="rank_extrema_perturbation_v2_plus",
                            candidate_model_name=(
                                "dfl_schedule_library_v2_plus_rank_extrema_"
                                f"{source_model_name}"
                            ),
                            anchor=anchor,
                            split_name=split_name,
                            regret=(
                                v2_plus_final_regret
                                if split_name == "final_holdout"
                                else v2_plus_train_regret
                            ),
                            forecast_prices=(700.0, 7000.0),
                            prior_family_mean_regret=v2_plus_train_regret,
                        ),
                    ]
                )
    return pl.DataFrame(rows)


def _source_candidate_library_for_generation() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    anchor = FIRST_ANCHOR
    for tenant_id in TENANTS[:1]:
        for source_model_name in SOURCE_MODELS[:1]:
            rows.extend(
                [
                    _candidate_row(
                        tenant_id=tenant_id,
                        source_model_name=source_model_name,
                        candidate_family="strict_control",
                        candidate_model_name="strict_similar_day",
                        anchor=anchor,
                        split_name="final_holdout",
                        regret=300.0,
                        forecast_prices=(1000.0, 5000.0, 1500.0, 4000.0),
                        prior_family_mean_regret=300.0,
                    ),
                    _candidate_row(
                        tenant_id=tenant_id,
                        source_model_name=source_model_name,
                        candidate_family="raw_source",
                        candidate_model_name=source_model_name,
                        anchor=anchor,
                        split_name="final_holdout",
                        regret=700.0,
                        forecast_prices=(5000.0, 1000.0, 4500.0, 1200.0),
                        prior_family_mean_regret=700.0,
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
    forecast_prices: tuple[float, ...],
    prior_family_mean_regret: float,
) -> dict[str, object]:
    actual_prices = [1000.0, 5000.0, 1500.0, 4000.0][: len(forecast_prices)]
    dispatch = [0.0, 1.0, 0.0, -1.0][: len(forecast_prices)]
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
        "soc_fraction_vector": [0.5, 0.4, 0.6, 0.5][: len(forecast_prices)],
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 5.0,
        "total_throughput_mwh": 0.2,
        "forecast_spread_uah_mwh": max(forecast_prices) - min(forecast_prices),
        "actual_spread_uah_mwh": max(actual_prices) - min(actual_prices),
        "forecast_top_k_actual_overlap": 1.0,
        "forecast_bottom_k_actual_overlap": 1.0,
        "peak_index_abs_error": 0.0,
        "trough_index_abs_error": 0.0,
        "soc_min_slack_fraction": 0.4,
        "prior_family_mean_regret_uah": prior_family_mean_regret,
        "safety_violation_count": 0,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "claim_scope": "dfl_schedule_candidate_library_v2_not_full_dfl",
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
                    "soc_fraction": [0.5, 0.4, 0.6, 0.5][step_index],
                    "degradation_penalty_uah": 0.0,
                }
                for step_index, forecast_price in enumerate(forecast_prices)
            ],
        },
    }
