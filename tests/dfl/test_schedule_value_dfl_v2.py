from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.official_v2_plus_dfl_dt_bridge import (
    OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
)
from smart_arbitrage.dfl.schedule_value_dfl_v2 import (
    DFL_SCHEDULE_VALUE_DFL_V2_STRICT_LP_STRATEGY_KIND,
    build_dfl_schedule_value_dfl_v2_frame,
    build_dfl_schedule_value_dfl_v2_strict_lp_benchmark_frame,
    evaluate_dfl_schedule_value_dfl_v2_gate,
    schedule_value_dfl_v2_model_name,
    validate_dfl_schedule_value_dfl_v2_evidence,
)
from smart_arbitrage.dfl.schedule_value_learner import (
    build_dfl_schedule_value_learner_v2_frame,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus import (
    build_dfl_schedule_value_learner_v2_plus_frame,
    build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
)

TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
FIRST_ANCHOR = datetime(2026, 1, 1, 23)
GENERATED_AT = datetime(2026, 5, 17, 16)


def test_dfl_v2_pairwise_family_selector_beats_v2_plus_when_prior_signal_exists() -> None:
    base_library = _candidate_library(
        include_dfl_family=False,
        dfl_train_regret=20.0,
        dfl_final_regret=120.0,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
    )
    full_library = _candidate_library(
        include_dfl_family=True,
        dfl_train_regret=20.0,
        dfl_final_regret=120.0,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
    )
    v2_model, v2_plus_model, v2_plus_strict = _v2_plus_reference(base_library)

    dfl_v2_model = build_dfl_schedule_value_dfl_v2_frame(
        full_library,
        v2_model,
        v2_plus_model,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    strict_frame = build_dfl_schedule_value_dfl_v2_strict_lp_benchmark_frame(
        full_library,
        dfl_v2_model,
        v2_plus_strict,
        generated_at=GENERATED_AT,
    )
    evidence = validate_dfl_schedule_value_dfl_v2_evidence(
        strict_frame,
        source_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
        min_validation_tenant_anchor_count=90,
    )
    gate = evaluate_dfl_schedule_value_dfl_v2_gate(
        strict_frame,
        source_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
        min_validation_tenant_anchor_count=90,
    )

    assert set(dfl_v2_model["fallback_to_v2_plus"].to_list()) == {False}
    assert set(dfl_v2_model["selected_schedule_family"].to_list()) == {
        "return_conditioned_schedule_family_dfl_v2"
    }
    assert dfl_v2_model["selected_final_mean_regret_uah"].unique().to_list() == [120.0]
    assert strict_frame.height == 5 * 2 * 18 * 4
    assert strict_frame["strategy_kind"].unique().to_list() == [
        DFL_SCHEDULE_VALUE_DFL_V2_STRICT_LP_STRATEGY_KIND
    ]
    assert set(strict_frame["selection_role"].unique().to_list()) == {
        "raw_reference",
        "schedule_value_dfl_v2",
        "schedule_value_learner_v2_plus_reference",
        "strict_reference",
    }
    assert (
        schedule_value_dfl_v2_model_name("nbeatsx_official_global_panel_v1")
        in strict_frame["forecast_model_name"].unique().to_list()
    )
    assert evidence.passed is True
    assert gate.passed is True
    assert gate.metrics["mean_regret_improvement_ratio_vs_v2_plus"] > 0.0
    assert gate.metrics["market_execution_enabled"] is False


def test_dfl_v2_falls_back_to_v2_plus_without_prior_non_degradation_signal() -> None:
    base_library = _candidate_library(
        include_dfl_family=False,
        dfl_train_regret=40.0,
        dfl_final_regret=90.0,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
    )
    full_library = _candidate_library(
        include_dfl_family=True,
        dfl_train_regret=40.0,
        dfl_final_regret=90.0,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
    )
    v2_model, v2_plus_model, v2_plus_strict = _v2_plus_reference(base_library)

    dfl_v2_model = build_dfl_schedule_value_dfl_v2_frame(
        full_library,
        v2_model,
        v2_plus_model,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    strict_frame = build_dfl_schedule_value_dfl_v2_strict_lp_benchmark_frame(
        full_library,
        dfl_v2_model,
        v2_plus_strict,
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_schedule_value_dfl_v2_gate(
        strict_frame,
        source_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
        min_validation_tenant_anchor_count=90,
    )

    assert set(dfl_v2_model["fallback_to_v2_plus"].to_list()) == {True}
    assert dfl_v2_model["selected_final_mean_regret_uah"].unique().to_list() == [180.0]
    assert (
        strict_frame.filter(pl.col("selection_role") == "schedule_value_dfl_v2")[
            "regret_uah"
        ]
        .unique()
        .to_list()
        == [180.0]
    )
    assert gate.passed is False
    assert gate.decision == "diagnostic_pass_replacement_blocked"


def test_dfl_v2_final_actual_mutation_changes_scores_not_selected_family() -> None:
    base_library = _candidate_library(
        include_dfl_family=False,
        dfl_train_regret=20.0,
        dfl_final_regret=120.0,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
    )
    full_library = _candidate_library(
        include_dfl_family=True,
        dfl_train_regret=20.0,
        dfl_final_regret=120.0,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
    )
    mutated_full_library = _mutate_final_dfl_family_regret(full_library, regret=70.0)
    v2_model, v2_plus_model, _ = _v2_plus_reference(base_library)

    original = build_dfl_schedule_value_dfl_v2_frame(
        full_library,
        v2_model,
        v2_plus_model,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    mutated = build_dfl_schedule_value_dfl_v2_frame(
        mutated_full_library,
        v2_model,
        v2_plus_model,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )

    prior_columns = [
        "tenant_id",
        "source_model_name",
        "selected_objective_name",
        "selected_schedule_family",
        "selected_pairwise_family_scores",
        "fallback_to_v2_plus",
    ]
    assert original.select(prior_columns).to_dicts() == mutated.select(prior_columns).to_dicts()
    assert (
        original["selected_final_mean_regret_uah"].to_list()
        != mutated["selected_final_mean_regret_uah"].to_list()
    )


def test_dfl_v2_learns_from_overlapping_train_family_coverage() -> None:
    base_library = _candidate_library(
        include_dfl_family=False,
        dfl_train_regret=20.0,
        dfl_final_regret=120.0,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
    )
    full_library = _candidate_library(
        include_dfl_family=True,
        dfl_train_regret=20.0,
        dfl_final_regret=120.0,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
    ).filter(
        ~(
            (pl.col("split_name") == "train_selection")
            & (pl.col("candidate_family") == "return_conditioned_schedule_family_dfl_v2")
            & (pl.col("anchor_timestamp") == FIRST_ANCHOR)
        )
    )
    v2_model, v2_plus_model, _ = _v2_plus_reference(base_library)

    dfl_v2_model = build_dfl_schedule_value_dfl_v2_frame(
        full_library,
        v2_model,
        v2_plus_model,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )

    assert set(dfl_v2_model["fallback_to_v2_plus"].to_list()) == {False}
    assert set(dfl_v2_model["selected_schedule_family"].to_list()) == {
        "return_conditioned_schedule_family_dfl_v2"
    }


def _v2_plus_reference(
    library: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    v2_model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    v2_plus_model = build_dfl_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    v2_plus_strict = build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        library,
        v2_plus_model,
        v2_model,
        generated_at=GENERATED_AT,
    )
    return v2_model, v2_plus_model, v2_plus_strict


def _candidate_library(
    *,
    include_dfl_family: bool,
    dfl_train_regret: float,
    dfl_final_regret: float,
    v2_plus_train_regret: float,
    v2_plus_final_regret: float,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    train_anchor_count = 3
    final_anchor_count = 18
    for tenant_id in TENANTS:
        for source_model_name in OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS:
            for anchor_index in range(train_anchor_count + final_anchor_count):
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
                            regret=310.0,
                            forecast_prices=(1000.0, 5000.0),
                            dispatch=(1.0, -1.0),
                            prior_family_mean_regret=310.0,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="raw_source",
                            candidate_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            regret=620.0,
                            forecast_prices=(5000.0, 1000.0),
                            dispatch=(0.0, 1.0),
                            prior_family_mean_regret=620.0,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="strict_prior_residual_v2",
                            candidate_model_name=f"dfl_residual_family_{source_model_name}",
                            anchor=anchor,
                            split_name=split_name,
                            regret=220.0,
                            forecast_prices=(900.0, 6500.0),
                            dispatch=(1.0, -1.0),
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
                            dispatch=(1.0, -1.0),
                            prior_family_mean_regret=v2_plus_train_regret,
                        ),
                    ]
                )
                if include_dfl_family:
                    rows.append(
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="return_conditioned_schedule_family_dfl_v2",
                            candidate_model_name=f"dfl_v2_return_family_{source_model_name}",
                            anchor=anchor,
                            split_name=split_name,
                            regret=(
                                dfl_final_regret
                                if split_name == "final_holdout"
                                else dfl_train_regret
                            ),
                            forecast_prices=(800.0, 7200.0),
                            dispatch=(1.0, -1.0),
                            prior_family_mean_regret=dfl_train_regret,
                        )
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
    dispatch: tuple[float, ...],
    prior_family_mean_regret: float,
) -> dict[str, object]:
    actual_prices = (1000.0, 5000.0)
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
        "actual_price_uah_mwh_vector": list(actual_prices),
        "dispatch_mw_vector": list(dispatch),
        "soc_fraction_vector": [0.5, 0.51],
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 950.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": sum(abs(value) for value in dispatch),
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
        "claim_scope": "dfl_schedule_candidate_library_v2_plus_not_full_dfl",
        "evaluation_payload": {
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "source_forecast_model_name": source_model_name,
            "candidate_family": candidate_family,
            "candidate_model_name": candidate_model_name,
            "safety_violation_count": 0,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        },
    }


def _mutate_final_dfl_family_regret(frame: pl.DataFrame, *, regret: float) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in frame.iter_rows(named=True):
        copied = dict(row)
        if (
            str(row["split_name"]) == "final_holdout"
            and str(row["candidate_family"]) == "return_conditioned_schedule_family_dfl_v2"
        ):
            copied["regret_uah"] = regret
            copied["decision_value_uah"] = 1000.0 - regret
            copied["actual_price_uah_mwh_vector"] = [1200.0, 4500.0]
        rows.append(copied)
    return pl.DataFrame(rows)
