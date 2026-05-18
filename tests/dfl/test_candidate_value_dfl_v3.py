from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.candidate_value_dfl_v3 import (
    CANDIDATE_FAMILY_DEGRADATION_SWEEP_V3,
    CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V3,
    CANDIDATE_FAMILY_PRIOR_BEST_TEMPLATE_V3,
    CANDIDATE_FAMILY_PRIOR_ORACLE_RESIDUAL_V3,
    CANDIDATE_VALUE_DFL_V3_STRICT_LP_STRATEGY_KIND,
    build_dfl_candidate_value_dfl_v3_failure_audit_frame,
    build_dfl_candidate_value_dfl_v3_frame,
    build_dfl_candidate_value_dfl_v3_strict_lp_benchmark_frame,
    build_dfl_candidate_value_label_panel_v3_frame,
    build_dfl_schedule_candidate_library_v3_frame,
    candidate_value_dfl_v3_model_name,
    evaluate_dfl_candidate_value_dfl_v3_gate,
    validate_dfl_candidate_value_dfl_v3_evidence,
)
from smart_arbitrage.dfl.official_v2_plus_dfl_dt_bridge import (
    OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
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
GENERATED_AT = datetime(2026, 5, 17, 21)


def test_v3_candidate_library_adds_failure_mode_families_and_train_only_oracle() -> None:
    base_library = _candidate_library(
        include_v3_value_family=False,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
        value_train_regret=20.0,
        value_final_regret=120.0,
        tenant_ids=(TENANTS[0],),
        source_model_names=(OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS[0],),
        final_anchor_count=2,
    )

    expanded = build_dfl_schedule_candidate_library_v3_frame(base_library)

    families = set(expanded["candidate_family"].unique().to_list())
    oracle_rows = expanded.filter(
        pl.col("candidate_family") == CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V3
    )
    assert CANDIDATE_FAMILY_DEGRADATION_SWEEP_V3 in families
    assert CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V3 in families
    assert set(oracle_rows["split_name"].unique().to_list()) == {"train_selection"}
    assert set(expanded["not_market_execution"].unique().to_list()) == {True}


def test_v3_candidate_library_bounds_expensive_train_generation() -> None:
    base_library = _candidate_library(
        include_v3_value_family=False,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
        value_train_regret=20.0,
        value_final_regret=120.0,
        tenant_ids=(TENANTS[0],),
        source_model_names=(OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS[0],),
        final_anchor_count=2,
    )

    expanded = build_dfl_schedule_candidate_library_v3_frame(
        base_library,
        max_train_generation_anchor_count_per_tenant=1,
    )

    generated = expanded.filter(pl.col("candidate_library_version") == "v3_generated")
    generated_train_anchor_count = generated.filter(
        pl.col("split_name") == "train_selection"
    ).select(["tenant_id", "source_model_name", "anchor_timestamp"]).unique().height
    generated_final_anchor_count = generated.filter(
        pl.col("split_name") == "final_holdout"
    ).select(["tenant_id", "source_model_name", "anchor_timestamp"]).unique().height
    oracle_rows = generated.filter(
        pl.col("candidate_family") == CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V3
    )

    assert generated_train_anchor_count == 1
    assert generated_final_anchor_count == 2
    assert oracle_rows.height == 1


def test_v3_candidate_library_adds_prior_only_template_schedules() -> None:
    base_library = _candidate_library(
        include_v3_value_family=False,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
        value_train_regret=20.0,
        value_final_regret=120.0,
        tenant_ids=(TENANTS[0],),
        source_model_names=(OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS[0],),
        train_anchor_count=3,
        final_anchor_count=2,
    )

    expanded = build_dfl_schedule_candidate_library_v3_frame(
        base_library,
        min_prior_template_anchor_count=2,
    )

    generated_final = expanded.filter(
        (pl.col("split_name") == "final_holdout")
        & (
            pl.col("candidate_family").is_in(
                [
                    CANDIDATE_FAMILY_PRIOR_BEST_TEMPLATE_V3,
                    CANDIDATE_FAMILY_PRIOR_ORACLE_RESIDUAL_V3,
                ]
            )
        )
    )
    payloads = generated_final["evaluation_payload"].to_list()

    assert generated_final.height == 4
    assert set(generated_final["candidate_family"].unique().to_list()) == {
        CANDIDATE_FAMILY_PRIOR_BEST_TEMPLATE_V3,
        CANDIDATE_FAMILY_PRIOR_ORACLE_RESIDUAL_V3,
    }
    assert {
        payload["prior_template_anchor_count"]
        for payload in payloads
        if isinstance(payload, dict)
    } == {3}
    assert all(
        bool(payload.get("no_final_holdout_actuals_used_for_generation"))
        for payload in payloads
        if isinstance(payload, dict)
    )


def test_v3_value_label_panel_separates_prior_features_from_actual_labels() -> None:
    base_library = _candidate_library(
        include_v3_value_family=True,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
        value_train_regret=20.0,
        value_final_regret=120.0,
        tenant_ids=(TENANTS[0],),
        source_model_names=(OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS[0],),
        train_anchor_count=3,
        final_anchor_count=2,
    )
    expanded = build_dfl_schedule_candidate_library_v3_frame(base_library)
    mutated = _mutate_final_all_candidate_regret(expanded, regret_delta=55.0)

    original_labels = build_dfl_candidate_value_label_panel_v3_frame(expanded)
    mutated_labels = build_dfl_candidate_value_label_panel_v3_frame(mutated)
    feature_columns = sorted(
        column for column in original_labels.columns if column.startswith("selector_feature_")
    )
    label_columns = sorted(
        column for column in original_labels.columns if column.startswith("label_")
    )

    assert feature_columns
    assert label_columns
    assert original_labels.select(feature_columns).to_dicts() == mutated_labels.select(
        feature_columns
    ).to_dicts()
    assert original_labels.select(label_columns).to_dicts() != mutated_labels.select(
        label_columns
    ).to_dicts()


def test_candidate_value_dfl_v3_can_beat_v2_plus_when_prior_value_signal_exists() -> None:
    base_library = _candidate_library(
        include_v3_value_family=False,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
        value_train_regret=20.0,
        value_final_regret=120.0,
    )
    full_library = _candidate_library(
        include_v3_value_family=True,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
        value_train_regret=20.0,
        value_final_regret=120.0,
    )
    v2_model, v2_plus_model, v2_plus_strict = _v2_plus_reference(base_library)
    label_panel = build_dfl_candidate_value_label_panel_v3_frame(full_library)

    v3_model = build_dfl_candidate_value_dfl_v3_frame(
        full_library,
        v2_plus_model,
        label_panel,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    strict_frame = build_dfl_candidate_value_dfl_v3_strict_lp_benchmark_frame(
        full_library,
        v3_model,
        v2_plus_strict,
        generated_at=GENERATED_AT,
    )
    evidence = validate_dfl_candidate_value_dfl_v3_evidence(
        strict_frame,
        source_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
        min_validation_tenant_anchor_count=90,
    )
    gate = evaluate_dfl_candidate_value_dfl_v3_gate(
        strict_frame,
        source_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
        min_validation_tenant_anchor_count=90,
    )

    assert v2_model.height == 10
    assert set(v3_model["fallback_to_v2_plus"].to_list()) == {False}
    assert set(v3_model["selected_scorer_type"].to_list()) == {
        "learned_linear_candidate_value_v3"
    }
    assert all(
        "selector_feature_prior_family_mean_regret_uah" in weights
        for weights in v3_model["selected_feature_weights"].to_list()
    )
    assert set(v3_model["selected_final_family_counts"].to_list()[0]) == {
        "candidate_value_good_v3"
    }
    assert strict_frame.height == 5 * 2 * 18 * 4
    assert {
        "evaluation_id",
        "market_venue",
        "horizon_hours",
        "starting_soc_fraction",
        "starting_soc_source",
        "forecast_objective_value_uah",
        "regret_ratio",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "committed_action",
        "committed_power_mw",
        "rank_by_regret",
    }.issubset(set(strict_frame.columns))
    assert strict_frame["evaluation_id"].n_unique() == strict_frame.height
    assert {
        row["evaluation_payload"].get("safety_violation_count")
        for row in strict_frame.iter_rows(named=True)
    } == {0}
    assert strict_frame["strategy_kind"].unique().to_list() == [
        CANDIDATE_VALUE_DFL_V3_STRICT_LP_STRATEGY_KIND
    ]
    assert set(strict_frame["selection_role"].unique().to_list()) == {
        "candidate_value_dfl_v3",
        "raw_reference",
        "schedule_value_learner_v2_plus_reference",
        "strict_reference",
    }
    assert (
        candidate_value_dfl_v3_model_name("nbeatsx_official_global_panel_v1")
        in strict_frame["forecast_model_name"].unique().to_list()
    )
    assert evidence.passed is True
    assert gate.passed is True
    assert gate.metrics["mean_regret_improvement_ratio_vs_v2_plus"] > 0.0
    assert gate.metrics["market_execution_enabled"] is False


def test_candidate_value_dfl_v3_falls_back_when_prior_signal_is_weak() -> None:
    base_library = _candidate_library(
        include_v3_value_family=False,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
        value_train_regret=45.0,
        value_final_regret=90.0,
    )
    full_library = _candidate_library(
        include_v3_value_family=True,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
        value_train_regret=45.0,
        value_final_regret=90.0,
    )
    _, v2_plus_model, v2_plus_strict = _v2_plus_reference(base_library)
    label_panel = build_dfl_candidate_value_label_panel_v3_frame(full_library)

    v3_model = build_dfl_candidate_value_dfl_v3_frame(
        full_library,
        v2_plus_model,
        label_panel,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    strict_frame = build_dfl_candidate_value_dfl_v3_strict_lp_benchmark_frame(
        full_library,
        v3_model,
        v2_plus_strict,
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_candidate_value_dfl_v3_gate(
        strict_frame,
        source_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
        min_validation_tenant_anchor_count=90,
    )

    assert set(v3_model["fallback_to_v2_plus"].to_list()) == {True}
    assert set(v3_model["selected_final_mean_regret_uah"].to_list()) == {180.0}
    assert gate.passed is False
    assert gate.decision == "diagnostic_pass_replacement_blocked"


def test_candidate_value_dfl_v3_final_mutation_changes_scores_not_selected_profile() -> None:
    base_library = _candidate_library(
        include_v3_value_family=False,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
        value_train_regret=20.0,
        value_final_regret=120.0,
    )
    full_library = _candidate_library(
        include_v3_value_family=True,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
        value_train_regret=20.0,
        value_final_regret=120.0,
    )
    mutated_library = _mutate_final_value_family_regret(full_library, regret=70.0)
    _, v2_plus_model, _ = _v2_plus_reference(base_library)
    label_panel = build_dfl_candidate_value_label_panel_v3_frame(full_library)
    mutated_label_panel = _mutate_final_label_panel_regret(label_panel, regret=70.0)

    original = build_dfl_candidate_value_dfl_v3_frame(
        full_library,
        v2_plus_model,
        label_panel,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    mutated = build_dfl_candidate_value_dfl_v3_frame(
        mutated_library,
        v2_plus_model,
        mutated_label_panel,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )

    prior_columns = [
        "tenant_id",
        "source_model_name",
        "selected_value_profile_name",
        "selected_scorer_type",
        "selected_feature_weights",
        "fallback_to_v2_plus",
    ]
    assert original.select(prior_columns).to_dicts() == mutated.select(
        prior_columns
    ).to_dicts()
    assert (
        original["selected_final_mean_regret_uah"].to_list()
        != mutated["selected_final_mean_regret_uah"].to_list()
    )


def test_candidate_value_dfl_v3_learned_scorer_uses_train_label_panel() -> None:
    base_library = _candidate_library(
        include_v3_value_family=False,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
        value_train_regret=20.0,
        value_final_regret=120.0,
    )
    full_library = _candidate_library(
        include_v3_value_family=True,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
        value_train_regret=20.0,
        value_final_regret=120.0,
    )
    _, v2_plus_model, _ = _v2_plus_reference(base_library)
    label_panel = build_dfl_candidate_value_label_panel_v3_frame(full_library)
    poisoned_train_labels = _mutate_train_label_panel_family_regret(
        label_panel,
        family="candidate_value_good_v3",
        regret=900.0,
    )

    original = build_dfl_candidate_value_dfl_v3_frame(
        full_library,
        v2_plus_model,
        label_panel,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    poisoned = build_dfl_candidate_value_dfl_v3_frame(
        full_library,
        v2_plus_model,
        poisoned_train_labels,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )

    assert set(original["fallback_to_v2_plus"].to_list()) == {False}
    assert set(poisoned["fallback_to_v2_plus"].to_list()) == {True}
    assert (
        original["selected_feature_weights"].to_list()
        != poisoned["selected_feature_weights"].to_list()
    )


def test_candidate_value_dfl_v3_failure_audit_explains_prior_template_gap() -> None:
    base_library = _candidate_library(
        include_v3_value_family=False,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
        value_train_regret=20.0,
        value_final_regret=120.0,
        tenant_ids=(TENANTS[0],),
        source_model_names=(OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS[0],),
        train_anchor_count=3,
        final_anchor_count=2,
    )
    expanded = build_dfl_schedule_candidate_library_v3_frame(
        base_library,
        min_prior_template_anchor_count=2,
    )
    label_panel = build_dfl_candidate_value_label_panel_v3_frame(expanded)
    _, v2_plus_model, v2_plus_strict = _v2_plus_reference(
        base_library,
        tenant_ids=(TENANTS[0],),
        source_model_names=(OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS[0],),
        final_validation_anchor_count_per_tenant=2,
    )
    v3_model = build_dfl_candidate_value_dfl_v3_frame(
        expanded,
        v2_plus_model,
        label_panel,
        tenant_ids=(TENANTS[0],),
        forecast_model_names=(OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS[0],),
        final_validation_anchor_count_per_tenant=2,
    )
    strict_frame = build_dfl_candidate_value_dfl_v3_strict_lp_benchmark_frame(
        expanded,
        v3_model,
        v2_plus_strict,
        generated_at=GENERATED_AT,
    )

    audit = build_dfl_candidate_value_dfl_v3_failure_audit_frame(
        label_panel,
        v3_model,
        strict_frame,
    )
    prior_rows = audit.filter(
        pl.col("candidate_family").is_in(
            [
                CANDIDATE_FAMILY_PRIOR_BEST_TEMPLATE_V3,
                CANDIDATE_FAMILY_PRIOR_ORACLE_RESIDUAL_V3,
            ]
        )
    )

    assert prior_rows.height == 2
    assert set(prior_rows["audit_grain"].to_list()) == {"candidate_family"}
    assert set(prior_rows["market_execution_enabled"].to_list()) == {False}
    assert all(
        diagnosis
        in {
            "template_not_competitive_vs_v2_plus",
            "template_competitive_but_not_selected",
            "template_beats_v2_plus_candidate",
        }
        for diagnosis in prior_rows["diagnosis"].to_list()
    )


def _v2_plus_reference(
    library: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...] = TENANTS,
    source_model_names: tuple[str, ...] = OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    final_validation_anchor_count_per_tenant: int = 18,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    v2_model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=tenant_ids,
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
    )
    v2_plus_model = build_dfl_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        tenant_ids=tenant_ids,
        forecast_model_names=source_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
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
    include_v3_value_family: bool,
    v2_plus_train_regret: float,
    v2_plus_final_regret: float,
    value_train_regret: float,
    value_final_regret: float,
    tenant_ids: tuple[str, ...] = TENANTS,
    source_model_names: tuple[str, ...] = OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    train_anchor_count: int = 3,
    final_anchor_count: int = 18,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in tenant_ids:
        for source_model_name in source_model_names:
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
                if include_v3_value_family:
                    rows.append(
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="candidate_value_good_v3",
                            candidate_model_name=f"candidate_value_good_{source_model_name}",
                            anchor=anchor,
                            split_name=split_name,
                            regret=(
                                value_final_regret
                                if split_name == "final_holdout"
                                else value_train_regret
                            ),
                            forecast_prices=(800.0, 7200.0),
                            dispatch=(1.0, -1.0),
                            prior_family_mean_regret=value_train_regret,
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
            "horizon": [
                {
                    "interval_start": anchor + timedelta(hours=index + 1),
                    "forecast_price_uah_mwh": forecast_prices[index],
                    "actual_price_uah_mwh": actual_prices[index],
                    "net_power_mw": dispatch[index],
                    "soc_fraction": 0.5,
                    "degradation_penalty_uah": 0.0,
                }
                for index in range(len(forecast_prices))
            ],
            "safety_violation_count": 0,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        },
    }


def _mutate_final_value_family_regret(
    frame: pl.DataFrame, *, regret: float
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in frame.iter_rows(named=True):
        copied = dict(row)
        if (
            str(row["split_name"]) == "final_holdout"
            and str(row["candidate_family"]) == "candidate_value_good_v3"
        ):
            copied["regret_uah"] = regret
            copied["decision_value_uah"] = 1000.0 - regret
            copied["actual_price_uah_mwh_vector"] = [1200.0, 4500.0]
        rows.append(copied)
    return pl.DataFrame(rows)


def _mutate_final_all_candidate_regret(
    frame: pl.DataFrame, *, regret_delta: float
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in frame.iter_rows(named=True):
        copied = dict(row)
        if str(row["split_name"]) == "final_holdout":
            copied["regret_uah"] = float(row["regret_uah"]) + regret_delta
            copied["decision_value_uah"] = float(row["decision_value_uah"]) - regret_delta
            copied["actual_price_uah_mwh_vector"] = [1300.0, 4200.0]
        rows.append(copied)
    return pl.DataFrame(rows)


def _mutate_final_label_panel_regret(frame: pl.DataFrame, *, regret: float) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in frame.iter_rows(named=True):
        copied = dict(row)
        if str(row["split_name"]) == "final_holdout":
            copied["label_regret_uah"] = regret
            copied["label_decision_value_uah"] = float(row["label_oracle_value_uah"]) - regret
        rows.append(copied)
    return pl.DataFrame(rows)


def _mutate_train_label_panel_family_regret(
    frame: pl.DataFrame,
    *,
    family: str,
    regret: float,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in frame.iter_rows(named=True):
        copied = dict(row)
        if str(row["split_name"]) == "train_selection" and str(row["candidate_family"]) == family:
            copied["label_regret_uah"] = regret
            copied["label_decision_value_uah"] = float(row["label_oracle_value_uah"]) - regret
        rows.append(copied)
    return pl.DataFrame(rows)
