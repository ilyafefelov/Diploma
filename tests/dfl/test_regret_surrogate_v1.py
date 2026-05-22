from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.regret_surrogate_v1 import (
    REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE,
    REGRET_SURROGATE_CANDIDATE_VALUE_V7_SELECTION_ROLE,
    REGRET_SURROGATE_SPARSE_SAFE_SWITCH_SELECTION_ROLE,
    REGRET_SURROGATE_SELECTION_ROLE,
    STRICT_REFERENCE_ROLE,
    V2_PLUS_REFERENCE_ROLE,
    build_dfl_expanded_schedule_value_teacher_label_panel_v1_frame,
    build_dfl_regret_surrogate_contextual_candidate_value_v2_frame,
    build_dfl_regret_surrogate_contextual_rolling_robustness_frame,
    build_dfl_regret_surrogate_contextual_strict_lp_benchmark_frame,
    build_dfl_regret_surrogate_safe_switch_context_audit_frame,
    build_dfl_regret_surrogate_candidate_value_v1_frame,
    build_dfl_regret_surrogate_forecast_correction_v1_frame,
    build_dfl_regret_surrogate_rolling_robustness_frame,
    build_dfl_regret_surrogate_strict_lp_benchmark_frame,
    build_dfl_regret_surrogate_teacher_label_panel_v2_frame,
    build_dfl_v2_plus_learning_limit_audit_frame,
    build_dfl_v2_plus_opportunity_backfill_requirements_frame,
    build_dfl_backfilled_context_feature_panel_v7_frame,
    build_dfl_feasible_schedule_candidate_library_v7_frame,
    build_dfl_candidate_value_teacher_label_panel_v7_frame,
    build_dfl_candidate_value_regret_surrogate_v7_frame,
    build_dfl_candidate_value_v7_rolling_robustness_frame,
    build_dfl_candidate_value_v7_strict_lp_benchmark_frame,
    build_dfl_sparse_safe_switch_abstention_model_v6_frame,
    build_dfl_sparse_safe_switch_candidate_library_v6_frame,
    build_dfl_sparse_safe_switch_feature_contract_audit_frame,
    build_dfl_sparse_safe_switch_opportunity_audit_frame,
    build_dfl_sparse_safe_switch_rolling_robustness_frame,
    build_dfl_sparse_safe_switch_strict_lp_benchmark_frame,
    build_dfl_sparse_safe_switch_teacher_label_panel_v6_frame,
    evaluate_dfl_regret_surrogate_gate,
)

TENANTS = ("tenant_a", "tenant_b")
SOURCE = "nbeatsx_official_global_panel_horizon_calibrated_v1"
GENERATED_AT = datetime(2026, 5, 22, 12)
FIRST_ANCHOR = datetime(2026, 1, 1, 12)


def test_learning_limit_audit_reports_candidate_ceiling() -> None:
    panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=80.0)

    audit = build_dfl_v2_plus_learning_limit_audit_frame(panel)

    assert audit.height == len(TENANTS) * 5
    assert set(audit["learning_limit_failure_mode"].to_list()) == {
        "selector_too_conservative"
    }
    assert set(audit["candidate_universe_can_beat_v2_plus_gate"].to_list()) == {True}
    assert set(audit["recommended_next_branch"].to_list()) == {"regret_surrogate_dfl"}
    assert set(audit["market_execution_enabled"].to_list()) == {False}


def test_learning_limit_audit_stops_when_oracle_switch_cannot_clear_gate() -> None:
    panel = _candidate_panel(train_alt_regret=117.0, final_alt_regret=117.0)

    audit = build_dfl_v2_plus_learning_limit_audit_frame(panel)

    assert set(audit["candidate_universe_can_beat_v2_plus_gate"].to_list()) == {False}
    assert set(audit["recommended_next_branch"].to_list()) == {
        "data_or_candidate_backfill"
    }


def test_teacher_panel_keeps_final_features_prior_only_when_labels_mutate() -> None:
    base_panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    mutated_panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=360.0)

    base = build_dfl_expanded_schedule_value_teacher_label_panel_v1_frame(
        base_panel,
        build_dfl_v2_plus_learning_limit_audit_frame(base_panel),
    )
    mutated = build_dfl_expanded_schedule_value_teacher_label_panel_v1_frame(
        mutated_panel,
        build_dfl_v2_plus_learning_limit_audit_frame(mutated_panel),
    )
    feature_columns = sorted(
        column for column in base.columns if column.startswith("selector_feature_")
    )

    assert feature_columns
    assert (
        base.select(feature_columns).to_dicts()
        == mutated.select(feature_columns).to_dicts()
    )
    assert base.select("label_regret_delta_vs_v2_plus_uah").to_dicts() != (
        mutated.select("label_regret_delta_vs_v2_plus_uah").to_dicts()
    )
    assert (
        base.filter(pl.col("split_name") == "final_holdout")
        .select(pl.col("is_training_row").any())
        .item()
        is False
    )
    assert set(base["raw_hourly_action_imitation"].unique().to_list()) == {False}


def test_regret_surrogate_can_select_learnable_safe_candidate() -> None:
    panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    teacher = _teacher_panel(panel)
    correction = build_dfl_regret_surrogate_forecast_correction_v1_frame(
        teacher,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        min_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_predicted_tail_risk_probability=0.30,
    )
    candidate_value = build_dfl_regret_surrogate_candidate_value_v1_frame(
        teacher,
        correction,
    )
    strict = build_dfl_regret_surrogate_strict_lp_benchmark_frame(
        teacher,
        candidate_value,
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_regret_surrogate_gate(
        strict,
        min_validation_tenant_anchor_count=len(TENANTS) * 2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    assert set(correction["fallback_to_v2_plus"].to_list()) == {False}
    assert set(candidate_value["selected_final_candidate_count"].to_list()) == {2}
    assert {
        STRICT_REFERENCE_ROLE,
        V2_PLUS_REFERENCE_ROLE,
        REGRET_SURROGATE_SELECTION_ROLE,
    }.issubset(set(strict["selection_role"].unique().to_list()))
    assert {
        "starting_soc_fraction",
        "starting_soc_source",
        "committed_action",
        "committed_power_mw",
        "rank_by_regret",
    }.issubset(set(strict.columns))
    assert strict.filter(pl.col("selection_role") == REGRET_SURROGATE_SELECTION_ROLE)[
        "regret_uah"
    ].to_list() == [80.0] * (len(TENANTS) * 2)
    assert gate.passed is True
    assert gate.metrics["market_execution_enabled"] is False


def test_regret_surrogate_falls_back_on_tail_risk_even_if_final_would_win() -> None:
    panel = _candidate_panel(train_alt_regret=320.0, final_alt_regret=20.0)
    teacher = _teacher_panel(panel)
    correction = build_dfl_regret_surrogate_forecast_correction_v1_frame(
        teacher,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        min_prior_safe_win_count=1,
        max_predicted_tail_risk_probability=0.25,
    )
    candidate_value = build_dfl_regret_surrogate_candidate_value_v1_frame(
        teacher,
        correction,
    )
    strict = build_dfl_regret_surrogate_strict_lp_benchmark_frame(
        teacher,
        candidate_value,
        generated_at=GENERATED_AT,
    )

    assert set(correction["fallback_to_v2_plus"].to_list()) == {True}
    assert set(candidate_value["selected_final_candidate_count"].to_list()) == {0}
    assert strict.filter(pl.col("selection_role") == REGRET_SURROGATE_SELECTION_ROLE)[
        "regret_uah"
    ].to_list() == [120.0] * (len(TENANTS) * 2)


def test_regret_surrogate_rolling_uses_prior_windows_only() -> None:
    panel = _candidate_panel(
        train_alt_regret=80.0,
        final_alt_regret=80.0,
        train_anchor_count=4,
        final_anchor_count=4,
    )
    teacher = _teacher_panel(panel)

    rolling = build_dfl_regret_surrogate_rolling_robustness_frame(
        teacher,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        validation_window_count=2,
        validation_anchor_count=2,
        min_prior_anchors_before_window=2,
        min_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_predicted_tail_risk_probability=0.30,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    assert rolling.height == 2
    assert set(rolling["rolling_window_passed"].to_list()) == {True}
    assert rolling["minimum_prior_anchor_count_before_window"].min() >= 2
    assert set(rolling["market_execution_enabled"].to_list()) == {False}


def test_safe_switch_context_audit_reports_missing_prior_context() -> None:
    panel = _final_context_shifted_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    teacher = _teacher_panel(panel)

    audit = build_dfl_regret_surrogate_safe_switch_context_audit_frame(teacher)

    final = audit.filter(pl.col("split_name") == "final_holdout")
    assert set(final["safe_switch_context_failure_mode"].to_list()) == {
        "context_without_prior_support"
    }
    assert set(final["recommended_next_branch"].to_list()) == {"data_context_backfill"}
    assert set(final["material_safe_switch_available"].to_list()) == {True}
    assert set(final["market_execution_enabled"].to_list()) == {False}


def test_teacher_panel_v2_keeps_final_context_features_prior_only() -> None:
    base_panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    mutated_panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=360.0)

    base_v2 = _teacher_panel_v2(base_panel)
    mutated_v2 = _teacher_panel_v2(mutated_panel)
    feature_columns = sorted(
        column for column in base_v2.columns if column.startswith("selector_feature_")
    )

    assert feature_columns
    assert (
        base_v2.select(feature_columns).to_dicts()
        == mutated_v2.select(feature_columns).to_dicts()
    )
    assert base_v2.select("label_context_material_safe_switch").to_dicts() != (
        mutated_v2.select("label_context_material_safe_switch").to_dicts()
    )
    assert (
        base_v2.filter(pl.col("split_name") == "final_holdout")
        .select(pl.col("is_training_row").any())
        .item()
        is False
    )


def test_contextual_regret_surrogate_selects_prior_supported_safe_candidate() -> None:
    panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    teacher_v2 = _teacher_panel_v2(panel)

    candidate_value = build_dfl_regret_surrogate_contextual_candidate_value_v2_frame(
        teacher_v2,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        min_context_prior_support_count=1,
        min_context_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_context_tail_risk_probability=0.30,
    )
    strict = build_dfl_regret_surrogate_contextual_strict_lp_benchmark_frame(
        teacher_v2,
        candidate_value,
        generated_at=GENERATED_AT,
    )

    assert set(candidate_value["selected_final_candidate_count"].to_list()) == {2}
    assert strict.filter(
        pl.col("selection_role") == REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE
    )["regret_uah"].to_list() == [80.0] * (len(TENANTS) * 2)
    assert (
        strict.filter(
            pl.col("selection_role") == REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE
        )["regret_uah"].mean()
        < strict.filter(pl.col("selection_role") == V2_PLUS_REFERENCE_ROLE)[
            "regret_uah"
        ].mean()
    )


def test_contextual_regret_surrogate_falls_back_without_prior_context_support() -> None:
    panel = _final_context_shifted_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    teacher_v2 = _teacher_panel_v2(panel)

    candidate_value = build_dfl_regret_surrogate_contextual_candidate_value_v2_frame(
        teacher_v2,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        min_context_prior_support_count=1,
        min_context_prior_safe_win_count=1,
    )
    strict = build_dfl_regret_surrogate_contextual_strict_lp_benchmark_frame(
        teacher_v2,
        candidate_value,
        generated_at=GENERATED_AT,
    )

    assert set(candidate_value["selected_final_candidate_count"].to_list()) == {0}
    assert strict.filter(
        pl.col("selection_role") == REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE
    )["regret_uah"].to_list() == [120.0] * (len(TENANTS) * 2)


def test_contextual_regret_surrogate_rolling_uses_prior_context_only() -> None:
    panel = _candidate_panel(
        train_alt_regret=80.0,
        final_alt_regret=80.0,
        train_anchor_count=4,
        final_anchor_count=4,
    )
    teacher = _teacher_panel(panel)

    rolling = build_dfl_regret_surrogate_contextual_rolling_robustness_frame(
        teacher,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        validation_window_count=2,
        validation_anchor_count=2,
        min_prior_anchors_before_window=2,
        min_context_prior_support_count=1,
        min_context_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_context_tail_risk_probability=0.30,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    assert rolling.height == 2
    assert set(rolling["rolling_window_passed"].to_list()) == {True}
    assert rolling["minimum_prior_anchor_count_before_window"].min() >= 2
    assert set(rolling["market_execution_enabled"].to_list()) == {False}


def test_sparse_safe_switch_feature_contract_blocks_label_leakage() -> None:
    teacher_v2 = _teacher_panel_v2(
        _candidate_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    )
    leaked = teacher_v2.with_columns(
        pl.col("label_regret_delta_vs_v2_plus_uah").alias(
            "selector_feature_final_regret_uah"
        )
    )

    audit = build_dfl_sparse_safe_switch_feature_contract_audit_frame(leaked)

    assert audit.height == 1
    assert audit["feature_contract_passed"].item() is False
    assert (
        "selector_feature_final_regret_uah"
        in audit["blocked_selector_feature_names"].item()
    )
    assert audit["market_execution_enabled"].item() is False


def test_sparse_safe_switch_audit_uses_distance_not_exact_context_profile() -> None:
    teacher_v2 = _teacher_panel_v2(
        _final_context_shifted_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    )
    library = build_dfl_sparse_safe_switch_candidate_library_v6_frame(teacher_v2)

    audit = build_dfl_sparse_safe_switch_opportunity_audit_frame(
        library,
        max_prior_neighbor_distance=2.0,
    )

    final = audit.filter(pl.col("split_name") == "final_holdout")
    assert set(final["sparse_opportunity_class"].to_list()) == {
        "material_candidate_prior_supported"
    }
    assert final["nearest_prior_safe_switch_distance"].max() > 0.0
    assert set(final["market_execution_enabled"].to_list()) == {False}


def test_sparse_safe_switch_v6_selects_distance_supported_candidate() -> None:
    teacher_v2 = _teacher_panel_v2(
        _final_context_shifted_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    )
    library = build_dfl_sparse_safe_switch_candidate_library_v6_frame(teacher_v2)
    audit = build_dfl_sparse_safe_switch_opportunity_audit_frame(
        library,
        max_prior_neighbor_distance=2.0,
    )
    teacher_v6 = build_dfl_sparse_safe_switch_teacher_label_panel_v6_frame(
        library,
        audit,
        max_prior_neighbor_distance=2.0,
    )

    model = build_dfl_sparse_safe_switch_abstention_model_v6_frame(
        teacher_v6,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        max_prior_neighbor_distance=2.0,
        min_neighbor_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_neighbor_tail_risk_probability=0.30,
    )
    strict = build_dfl_sparse_safe_switch_strict_lp_benchmark_frame(
        teacher_v6,
        model,
        generated_at=GENERATED_AT,
    )

    assert set(model["selected_final_candidate_count"].to_list()) == {2}
    assert strict.filter(
        pl.col("selection_role") == REGRET_SURROGATE_SPARSE_SAFE_SWITCH_SELECTION_ROLE
    )["regret_uah"].to_list() == [80.0] * (len(TENANTS) * 2)


def test_sparse_safe_switch_v6_abstains_without_prior_neighbor() -> None:
    teacher_v2 = _teacher_panel_v2(
        _final_context_shifted_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    )
    library = build_dfl_sparse_safe_switch_candidate_library_v6_frame(teacher_v2)
    audit = build_dfl_sparse_safe_switch_opportunity_audit_frame(
        library,
        max_prior_neighbor_distance=0.20,
    )
    teacher_v6 = build_dfl_sparse_safe_switch_teacher_label_panel_v6_frame(
        library,
        audit,
        max_prior_neighbor_distance=0.20,
    )

    model = build_dfl_sparse_safe_switch_abstention_model_v6_frame(
        teacher_v6,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        max_prior_neighbor_distance=0.20,
        min_neighbor_safe_win_count=1,
    )
    strict = build_dfl_sparse_safe_switch_strict_lp_benchmark_frame(
        teacher_v6,
        model,
        generated_at=GENERATED_AT,
    )

    assert set(model["selected_final_candidate_count"].to_list()) == {0}
    assert set(model["abstention_reason"].to_list()) == {"no_prior_neighbor_support"}
    assert strict.filter(
        pl.col("selection_role") == REGRET_SURROGATE_SPARSE_SAFE_SWITCH_SELECTION_ROLE
    )["regret_uah"].to_list() == [120.0] * (len(TENANTS) * 2)


def test_sparse_safe_switch_final_label_mutation_changes_scores_not_selection() -> None:
    base_v2 = _teacher_panel_v2(
        _candidate_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    )
    mutated_v2 = _teacher_panel_v2(
        _candidate_panel(train_alt_regret=80.0, final_alt_regret=360.0)
    )

    base_model = _sparse_safe_switch_model(base_v2)
    mutated_model = _sparse_safe_switch_model(mutated_v2)
    base_strict = build_dfl_sparse_safe_switch_strict_lp_benchmark_frame(
        build_dfl_sparse_safe_switch_teacher_label_panel_v6_frame(
            build_dfl_sparse_safe_switch_candidate_library_v6_frame(base_v2),
            build_dfl_sparse_safe_switch_opportunity_audit_frame(
                build_dfl_sparse_safe_switch_candidate_library_v6_frame(base_v2),
                max_prior_neighbor_distance=2.0,
            ),
            max_prior_neighbor_distance=2.0,
        ),
        base_model,
        generated_at=GENERATED_AT,
    )
    mutated_strict = build_dfl_sparse_safe_switch_strict_lp_benchmark_frame(
        build_dfl_sparse_safe_switch_teacher_label_panel_v6_frame(
            build_dfl_sparse_safe_switch_candidate_library_v6_frame(mutated_v2),
            build_dfl_sparse_safe_switch_opportunity_audit_frame(
                build_dfl_sparse_safe_switch_candidate_library_v6_frame(mutated_v2),
                max_prior_neighbor_distance=2.0,
            ),
            max_prior_neighbor_distance=2.0,
        ),
        mutated_model,
        generated_at=GENERATED_AT,
    )

    assert base_model["selected_final_candidate_keys"].to_list() == (
        mutated_model["selected_final_candidate_keys"].to_list()
    )
    base_selected = base_strict.filter(
        pl.col("selection_role") == REGRET_SURROGATE_SPARSE_SAFE_SWITCH_SELECTION_ROLE
    )
    mutated_selected = mutated_strict.filter(
        pl.col("selection_role") == REGRET_SURROGATE_SPARSE_SAFE_SWITCH_SELECTION_ROLE
    )
    assert (
        base_selected["regret_uah"].to_list()
        != mutated_selected["regret_uah"].to_list()
    )


def test_sparse_safe_switch_rolling_uses_prior_neighbors_only() -> None:
    teacher = _teacher_panel_v2(
        _candidate_panel(
            train_alt_regret=80.0,
            final_alt_regret=80.0,
            train_anchor_count=4,
            final_anchor_count=4,
        )
    )
    library = build_dfl_sparse_safe_switch_candidate_library_v6_frame(teacher)

    rolling = build_dfl_sparse_safe_switch_rolling_robustness_frame(
        library,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        validation_window_count=2,
        validation_anchor_count=2,
        min_prior_anchors_before_window=2,
        max_prior_neighbor_distance=2.0,
        min_neighbor_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_neighbor_tail_risk_probability=0.30,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    assert rolling.height == 2
    assert set(rolling["rolling_window_passed"].to_list()) == {True}
    assert rolling["minimum_prior_anchor_count_before_window"].min() >= 2
    assert set(rolling["market_execution_enabled"].to_list()) == {False}


def test_v2_plus_backfill_requirements_find_strict_guarded_rescue_need() -> None:
    library, audit = _v6_library_and_audit(
        _teacher_panel_v2(
            _candidate_panel(
                train_alt_regret=130.0,
                final_alt_regret=130.0,
                train_strict_regret=80.0,
                final_strict_regret=80.0,
            )
        )
    )

    requirements = build_dfl_v2_plus_opportunity_backfill_requirements_frame(
        library,
        audit,
        material_switch_delta_uah=25.0,
    )

    final = requirements.filter(pl.col("split_name") == "final_holdout")
    assert set(final["opportunity_backfill_decision"].to_list()) == {
        "candidate_generation_needed"
    }
    assert set(final["diagnostic_strict_control_material_local_win"].to_list()) == {
        True
    }
    assert set(final["candidate_family_gap"].to_list()) == {True}
    assert set(final["market_execution_enabled"].to_list()) == {False}


def test_v7_candidate_library_adds_feasible_strict_guarded_rescue_variants() -> None:
    library, audit = _v6_library_and_audit(
        _teacher_panel_v2(
            _candidate_panel(
                train_alt_regret=130.0,
                final_alt_regret=130.0,
                train_strict_regret=80.0,
                final_strict_regret=80.0,
            )
        )
    )
    requirements = build_dfl_v2_plus_opportunity_backfill_requirements_frame(
        library,
        audit,
    )
    context_panel = build_dfl_backfilled_context_feature_panel_v7_frame(
        library,
        requirements,
    )

    v7_library = build_dfl_feasible_schedule_candidate_library_v7_frame(
        context_panel,
        requirements,
    )

    rescue = v7_library.filter(
        (pl.col("candidate_source") == "v7_generated_candidate")
        & (pl.col("candidate_family") == "strict_guarded_rescue_v7")
    )
    assert rescue.height == len(TENANTS) * 5
    assert set(rescue["eligible_for_final_selection_v7"].to_list()) == {True}
    assert set(rescue["oracle_neighborhood_train_only"].to_list()) == {False}
    assert set(rescue["candidate_schedule_class"].to_list()) == {
        "strict_guarded_rescue"
    }
    assert (
        v7_library.filter(pl.col("candidate_source") == "v2_plus_default").height
        == len(TENANTS) * 5
    )
    assert set(v7_library["market_execution_enabled"].unique().to_list()) == {False}


def test_v7_regret_surrogate_selects_prior_supported_strict_rescue() -> None:
    v7_library, teacher_v7 = _v7_library_and_teacher(
        _candidate_panel(
            train_alt_regret=130.0,
            final_alt_regret=130.0,
            train_strict_regret=80.0,
            final_strict_regret=80.0,
        )
    )

    model = build_dfl_candidate_value_regret_surrogate_v7_frame(
        teacher_v7,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        max_prior_neighbor_distance=2.0,
        min_neighbor_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_neighbor_tail_risk_probability=0.30,
        allowed_candidate_sources=("v7_generated_candidate",),
        min_prior_material_safe_switch_examples_for_dt=1,
    )
    strict = build_dfl_candidate_value_v7_strict_lp_benchmark_frame(
        teacher_v7,
        model,
        generated_at=GENERATED_AT,
    )

    assert set(model["selected_final_candidate_count"].to_list()) == {2}
    assert strict.filter(
        pl.col("selection_role") == REGRET_SURROGATE_CANDIDATE_VALUE_V7_SELECTION_ROLE
    )["regret_uah"].to_list() == [80.0] * (len(TENANTS) * 2)
    assert set(strict["market_execution_enabled"].unique().to_list()) == {False}


def test_v7_final_label_mutation_changes_scores_not_selected_candidates() -> None:
    _, base_teacher = _v7_library_and_teacher(
        _candidate_panel(
            train_alt_regret=130.0,
            final_alt_regret=130.0,
            train_strict_regret=80.0,
            final_strict_regret=80.0,
        )
    )
    _, mutated_teacher = _v7_library_and_teacher(
        _candidate_panel(
            train_alt_regret=130.0,
            final_alt_regret=130.0,
            train_strict_regret=80.0,
            final_strict_regret=360.0,
        )
    )

    base_model = build_dfl_candidate_value_regret_surrogate_v7_frame(
        base_teacher,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        max_prior_neighbor_distance=2.0,
        min_neighbor_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_neighbor_tail_risk_probability=0.30,
        allowed_candidate_sources=("v7_generated_candidate",),
        min_prior_material_safe_switch_examples_for_dt=1,
    )
    mutated_model = build_dfl_candidate_value_regret_surrogate_v7_frame(
        mutated_teacher,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        max_prior_neighbor_distance=2.0,
        min_neighbor_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_neighbor_tail_risk_probability=0.30,
        allowed_candidate_sources=("v7_generated_candidate",),
        min_prior_material_safe_switch_examples_for_dt=1,
    )
    base_strict = build_dfl_candidate_value_v7_strict_lp_benchmark_frame(
        base_teacher,
        base_model,
        generated_at=GENERATED_AT,
    )
    mutated_strict = build_dfl_candidate_value_v7_strict_lp_benchmark_frame(
        mutated_teacher,
        mutated_model,
        generated_at=GENERATED_AT,
    )

    assert base_model["selected_final_candidate_keys"].to_list() == (
        mutated_model["selected_final_candidate_keys"].to_list()
    )
    base_selected = base_strict.filter(
        pl.col("selection_role") == REGRET_SURROGATE_CANDIDATE_VALUE_V7_SELECTION_ROLE
    )
    mutated_selected = mutated_strict.filter(
        pl.col("selection_role") == REGRET_SURROGATE_CANDIDATE_VALUE_V7_SELECTION_ROLE
    )
    assert (
        base_selected["regret_uah"].to_list()
        != mutated_selected["regret_uah"].to_list()
    )
    selector_features = sorted(
        column
        for column in base_teacher.columns
        if column.startswith("selector_feature_")
    )
    assert base_teacher.select(selector_features).to_dicts() == (
        mutated_teacher.select(selector_features).to_dicts()
    )


def test_v7_rolling_uses_prior_backfilled_neighbors_only() -> None:
    v7_library, _ = _v7_library_and_teacher(
        _candidate_panel(
            train_alt_regret=130.0,
            final_alt_regret=130.0,
            train_anchor_count=4,
            final_anchor_count=4,
            train_strict_regret=80.0,
            final_strict_regret=80.0,
        )
    )

    rolling = build_dfl_candidate_value_v7_rolling_robustness_frame(
        v7_library,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        validation_window_count=2,
        validation_anchor_count=2,
        min_prior_anchors_before_window=2,
        max_prior_neighbor_distance=2.0,
        min_neighbor_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_neighbor_tail_risk_probability=0.30,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
        allowed_candidate_sources=("v7_generated_candidate",),
        min_prior_material_safe_switch_examples_for_dt=1,
    )

    assert rolling.height == 2
    assert set(rolling["rolling_window_passed"].to_list()) == {True}
    assert rolling["minimum_prior_anchor_count_before_window"].min() >= 2
    assert set(rolling["market_execution_enabled"].to_list()) == {False}


def _teacher_panel(panel: pl.DataFrame) -> pl.DataFrame:
    return build_dfl_expanded_schedule_value_teacher_label_panel_v1_frame(
        panel,
        build_dfl_v2_plus_learning_limit_audit_frame(panel),
    )


def _teacher_panel_v2(panel: pl.DataFrame) -> pl.DataFrame:
    teacher = _teacher_panel(panel)
    audit = build_dfl_regret_surrogate_safe_switch_context_audit_frame(teacher)
    return build_dfl_regret_surrogate_teacher_label_panel_v2_frame(teacher, audit)


def _sparse_safe_switch_model(teacher_v2: pl.DataFrame) -> pl.DataFrame:
    library = build_dfl_sparse_safe_switch_candidate_library_v6_frame(teacher_v2)
    audit = build_dfl_sparse_safe_switch_opportunity_audit_frame(
        library,
        max_prior_neighbor_distance=2.0,
    )
    teacher_v6 = build_dfl_sparse_safe_switch_teacher_label_panel_v6_frame(
        library,
        audit,
        max_prior_neighbor_distance=2.0,
    )
    return build_dfl_sparse_safe_switch_abstention_model_v6_frame(
        teacher_v6,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        max_prior_neighbor_distance=2.0,
        min_neighbor_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_neighbor_tail_risk_probability=0.30,
    )


def _v6_library_and_audit(
    teacher_v2: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    library = build_dfl_sparse_safe_switch_candidate_library_v6_frame(teacher_v2)
    audit = build_dfl_sparse_safe_switch_opportunity_audit_frame(
        library,
        max_prior_neighbor_distance=2.0,
    )
    return library, audit


def _v7_library_and_teacher(panel: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    v6_library, v6_audit = _v6_library_and_audit(_teacher_panel_v2(panel))
    requirements = build_dfl_v2_plus_opportunity_backfill_requirements_frame(
        v6_library,
        v6_audit,
    )
    context_panel = build_dfl_backfilled_context_feature_panel_v7_frame(
        v6_library,
        requirements,
    )
    v7_library = build_dfl_feasible_schedule_candidate_library_v7_frame(
        context_panel,
        requirements,
    )
    teacher_v7 = build_dfl_candidate_value_teacher_label_panel_v7_frame(
        v7_library,
        requirements,
        max_prior_neighbor_distance=2.0,
    )
    return v7_library, teacher_v7


def _final_context_shifted_panel(
    *,
    train_alt_regret: float,
    final_alt_regret: float,
) -> pl.DataFrame:
    return _candidate_panel(
        train_alt_regret=train_alt_regret,
        final_alt_regret=final_alt_regret,
    ).with_columns(
        pl.when(pl.col("split_name") == "final_holdout")
        .then(0.0)
        .otherwise(pl.col("selector_feature_grid_event_context_ready"))
        .alias("selector_feature_grid_event_context_ready")
    )


def _candidate_panel(
    *,
    train_alt_regret: float,
    final_alt_regret: float,
    train_anchor_count: int = 3,
    final_anchor_count: int = 2,
    train_strict_regret: float = 300.0,
    final_strict_regret: float = 300.0,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant in TENANTS:
        anchors = [
            (FIRST_ANCHOR + timedelta(hours=index), "train_selection")
            for index in range(train_anchor_count)
        ] + [
            (
                FIRST_ANCHOR + timedelta(hours=train_anchor_count + index),
                "final_holdout",
            )
            for index in range(final_anchor_count)
        ]
        for anchor, split_name in anchors:
            alt_regret = (
                train_alt_regret
                if split_name == "train_selection"
                else final_alt_regret
            )
            strict_regret = (
                train_strict_regret
                if split_name == "train_selection"
                else final_strict_regret
            )
            for source, family, model_name, regret, dispatch in (
                (
                    "strict_fallback",
                    "strict_control",
                    "strict_similar_day",
                    strict_regret,
                    [0.0, 0.0, 0.0, 0.0],
                ),
                (
                    "v2_plus_default",
                    "frozen_v2_plus_fallback",
                    "schedule_value_learner_v2_plus",
                    120.0,
                    [0.1, -0.1, 0.0, 0.0],
                ),
                (
                    "tft_shadow_candidate",
                    "tft_quantile_risk",
                    "tft_safe_candidate",
                    alt_regret,
                    [0.2, -0.2, 0.0, 0.0],
                ),
                (
                    "poland_shadow_candidate",
                    "poland_lag24_tail_risk",
                    "poland_tail_candidate",
                    360.0,
                    [0.4, -0.4, 0.0, 0.0],
                ),
            ):
                rows.append(
                    _candidate_row(
                        tenant=tenant,
                        candidate_source=source,
                        family=family,
                        model_name=model_name,
                        anchor=anchor,
                        split_name=split_name,
                        regret=regret,
                        dispatch=dispatch,
                    )
                )
    return pl.DataFrame(rows, infer_schema_length=None)


def _candidate_row(
    *,
    tenant: str,
    candidate_source: str,
    family: str,
    model_name: str,
    anchor: datetime,
    split_name: str,
    regret: float,
    dispatch: list[float],
) -> dict[str, object]:
    forecast = [1000.0, 3000.0, 1500.0, 4000.0]
    actual = [1100.0, 2800.0, 1600.0, 3900.0]
    soc = [0.5 + sum(dispatch[: index + 1]) * 0.05 for index in range(len(dispatch))]
    baseline_regret = 120.0
    delta = regret - baseline_regret
    return {
        "tenant_id": tenant,
        "source_model_name": SOURCE,
        "candidate_source": candidate_source,
        "candidate_family": family,
        "candidate_model_name": model_name,
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "split_name": split_name,
        "horizon_hours": 4,
        "eligible_for_final_selection": candidate_source != "strict_fallback",
        "forecast_price_uah_mwh_vector": forecast,
        "actual_price_uah_mwh_vector": actual,
        "dispatch_mw_vector": dispatch,
        "soc_fraction_vector": soc,
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0 - regret,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "v2_plus_baseline_regret_uah": baseline_regret,
        "label_regret_delta_vs_v2_plus_uah": delta,
        "label_safe_switch_win": candidate_source
        not in {
            "v2_plus_default",
            "strict_fallback",
        }
        and delta < 0.0,
        "label_tail_risk_loss": candidate_source
        not in {
            "v2_plus_default",
            "strict_fallback",
        }
        and delta >= 150.0,
        "label_best_candidate_family": family if delta < 0.0 else "frozen_v2_plus",
        "label_best_candidate_model_name": (
            model_name if delta < 0.0 else "schedule_value_learner_v2_plus"
        ),
        "label_is_anchor_best_candidate": delta < 0.0,
        "selector_feature_schedule_distance_from_v2_plus": (
            0.0 if candidate_source == "v2_plus_default" else abs(dispatch[0] - 0.1)
        ),
        "selector_feature_total_throughput_delta_mwh": sum(
            abs(value) for value in dispatch
        )
        - 0.2,
        "selector_feature_terminal_soc_delta_fraction": soc[-1] - 0.5,
        "selector_feature_forecast_spread_uah_mwh": max(forecast) - min(forecast),
        "selector_feature_total_degradation_penalty_uah": abs(sum(dispatch)) * 10.0,
        "selector_feature_poland_shadow_candidate": float(
            candidate_source == "poland_shadow_candidate"
        ),
        "selector_feature_tft_shadow_candidate": float(
            candidate_source == "tft_shadow_candidate"
        ),
        "selector_feature_weather_load_context_ready": 1.0,
        "selector_feature_calendar_publication_context_ready": 1.0,
        "selector_feature_grid_event_context_ready": 1.0,
        "selector_feature_hour_of_day": float(anchor.hour),
        "selector_feature_weekend": float(anchor.weekday() >= 5),
        "total_degradation_penalty_uah": abs(sum(dispatch)) * 10.0,
        "total_throughput_mwh": sum(abs(value) for value in dispatch),
        "safety_violation_count": 0,
        "target_label_space": "schedule_candidate_index",
        "raw_hourly_action_imitation": False,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "evaluation_payload": {
            "candidate_source": candidate_source,
            "candidate_family": family,
            "candidate_model_name": model_name,
            "dispatch_mw": dispatch,
            "soc_fraction": soc,
        },
    }
