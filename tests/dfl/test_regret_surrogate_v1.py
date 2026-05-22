from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.regret_surrogate_v1 import (
    REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE,
    REGRET_SURROGATE_CANDIDATE_VALUE_V7_SELECTION_ROLE,
    REGRET_SURROGATE_CANDIDATE_VALUE_V8_SELECTION_ROLE,
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
    build_dfl_candidate_value_regret_surrogate_v8_frame,
    build_dfl_candidate_value_v7_rolling_robustness_frame,
    build_dfl_candidate_value_v7_strict_lp_benchmark_frame,
    build_dfl_candidate_value_v8_rolling_robustness_frame,
    build_dfl_candidate_value_v8_strict_lp_benchmark_frame,
    build_dfl_v8_false_positive_tail_risk_audit_frame,
    build_dfl_v8_pruned_candidate_library_frame,
    build_dfl_v8_pruned_candidate_family_plan_frame,
    build_dfl_ua_context_backfilled_feature_panel_v8_frame,
    build_dfl_ua_context_candidate_v8_strict_rescore_frame,
    build_dfl_ua_context_candidate_value_teacher_label_panel_v8_frame,
    build_dfl_ua_context_feasible_schedule_candidate_library_v8_frame,
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


def test_v2_plus_backfill_requirements_tolerate_missing_anchor_strict_row() -> None:
    library, audit = _v6_library_and_audit(
        _teacher_panel_v2(_candidate_panel(train_alt_regret=130.0, final_alt_regret=130.0))
    )
    target_anchor = FIRST_ANCHOR + timedelta(hours=3)
    library = library.filter(
        ~(
            (pl.col("tenant_id") == TENANTS[0])
            & (pl.col("anchor_timestamp") == target_anchor)
            & (pl.col("candidate_source") == "strict_fallback")
        )
    )

    requirements = build_dfl_v2_plus_opportunity_backfill_requirements_frame(
        library,
        audit,
        material_switch_delta_uah=25.0,
    )

    target = requirements.filter(
        (pl.col("tenant_id") == TENANTS[0])
        & (pl.col("anchor_timestamp") == target_anchor)
    ).row(0, named=True)
    assert target["strict_control_best_regret_uah"] == target["v2_plus_regret_uah"]
    assert target["strict_control_reference_available"] is False
    assert target["diagnostic_strict_control_material_local_win"] is False


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


def test_v7_rolling_keeps_train_only_oracle_diagnostics_out_of_validation() -> None:
    v7_library, _ = _v7_library_and_teacher(
        _candidate_panel(
            train_alt_regret=130.0,
            final_alt_regret=130.0,
            train_anchor_count=8,
            final_anchor_count=0,
            train_strict_regret=80.0,
        )
    )
    v7_library = v7_library.with_columns(
        pl.when(pl.col("candidate_source") == "tft_shadow_candidate")
        .then(pl.lit("oracle_gap_candidate"))
        .otherwise(pl.col("candidate_source"))
        .alias("candidate_source"),
        pl.when(pl.col("candidate_source") == "tft_shadow_candidate")
        .then(True)
        .otherwise(pl.col("oracle_neighborhood_train_only"))
        .alias("oracle_neighborhood_train_only"),
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
    assert set(rolling["market_execution_enabled"].to_list()) == {False}


def test_v8_context_backfill_merges_source_backed_ua_features() -> None:
    v7_library, _ = _v7_library_and_teacher(
        _candidate_panel(train_alt_regret=130.0, final_alt_regret=130.0)
    )
    requirements = _v7_requirements(v7_library)

    panel = build_dfl_ua_context_backfilled_feature_panel_v8_frame(
        v7_library,
        _ua_context_panel(v7_library),
        requirements,
    )

    assert panel.height == v7_library.height
    assert "selector_feature_ua_context_ready" in panel.columns
    assert "selector_feature_ua_morning_evening_spread_skew" in panel.columns
    assert "diagnostic_ua_context_blockers" in panel.columns
    assert set(panel["selector_feature_ua_context_ready"].to_list()) == {1.0}
    assert set(panel["training_source_scope"].to_list()) == {
        "ukrainian_only_oree_open_meteo_tenant_grid"
    }
    assert set(panel["market_execution_enabled"].to_list()) == {False}


def test_v8_candidate_library_adds_new_feasible_ua_context_schedules() -> None:
    v7_library, _ = _v7_library_and_teacher(
        _candidate_panel(train_alt_regret=130.0, final_alt_regret=130.0)
    )
    requirements = _v7_requirements(v7_library)
    panel = build_dfl_ua_context_backfilled_feature_panel_v8_frame(
        v7_library,
        _ua_context_panel(v7_library),
        requirements,
    )

    v8_library = build_dfl_ua_context_feasible_schedule_candidate_library_v8_frame(
        panel,
        requirements,
    )

    generated = v8_library.filter(
        pl.col("candidate_source") == "ua_context_v8_generated_candidate"
    )
    assert generated.height == len(TENANTS) * 5 * 5
    assert {
        "ua_peak_trough_shift_v8",
        "ua_terminal_reserve_v8",
        "ua_morning_evening_block_v8",
        "ua_tail_risk_clipped_v8",
        "ua_strict_blend_rescue_v8",
    }.issubset(set(generated["candidate_family"].to_list()))
    assert set(generated["candidate_value_label_status"].to_list()) == {
        "pending_strict_rescore"
    }
    assert set(generated["eligible_for_final_selection_v8"].to_list()) == {True}
    assert set(generated["diagnostic_requires_strict_rescore"].to_list()) == {True}
    assert set(generated["market_execution_enabled"].to_list()) == {False}
    first_generated = generated.row(0, named=True)
    baseline = v8_library.filter(
        (pl.col("tenant_id") == first_generated["tenant_id"])
        & (pl.col("source_model_name") == first_generated["source_model_name"])
        & (pl.col("anchor_timestamp") == first_generated["anchor_timestamp"])
        & (pl.col("candidate_source") == "v2_plus_default")
    ).row(0, named=True)
    assert first_generated["dispatch_mw_vector"] != baseline["dispatch_mw_vector"]


def test_v8_strict_rescore_rebuilds_generated_candidate_regret_labels() -> None:
    v8_library, requirements = _v8_library(
        _candidate_panel(train_alt_regret=130.0, final_alt_regret=130.0)
    )

    rescored = build_dfl_ua_context_candidate_v8_strict_rescore_frame(v8_library)

    generated = rescored.filter(
        pl.col("candidate_source") == "ua_context_v8_generated_candidate"
    )
    assert generated.height == len(TENANTS) * 5 * 5
    assert set(generated["candidate_value_label_status"].to_list()) == {
        "strict_rescored_v8_candidate"
    }
    assert set(generated["diagnostic_requires_strict_rescore"].to_list()) == {False}
    assert set(generated["market_execution_enabled"].to_list()) == {False}
    assert generated["regret_uah"].min() >= 0.0
    for row in generated.iter_rows(named=True):
        assert row["label_regret_delta_vs_v2_plus_uah"] == (
            row["regret_uah"] - row["v2_plus_baseline_regret_uah"]
        )
    assert requirements.height == len(TENANTS) * 5


def test_v8_final_actual_mutation_changes_rescore_labels_not_candidate_features() -> None:
    base_library, _ = _v8_library(
        _candidate_panel(train_alt_regret=130.0, final_alt_regret=130.0)
    )
    mutated_library = base_library.with_columns(
        pl.when(pl.col("split_name") == "final_holdout")
        .then(pl.lit([5000.0, 1000.0, 4500.0, 900.0]))
        .otherwise(pl.col("actual_price_uah_mwh_vector"))
        .alias("actual_price_uah_mwh_vector")
    )

    base_rescore = build_dfl_ua_context_candidate_v8_strict_rescore_frame(base_library)
    mutated_rescore = build_dfl_ua_context_candidate_v8_strict_rescore_frame(
        mutated_library
    )

    selector_columns = sorted(
        column
        for column in base_rescore.columns
        if column.startswith("selector_feature_")
    )
    assert base_rescore.select(selector_columns).to_dicts() == (
        mutated_rescore.select(selector_columns).to_dicts()
    )
    generated_filter = pl.col("candidate_source") == "ua_context_v8_generated_candidate"
    assert base_rescore.filter(generated_filter).select(
        ["candidate_model_name", "dispatch_mw_vector"]
    ).to_dicts() == mutated_rescore.filter(generated_filter).select(
        ["candidate_model_name", "dispatch_mw_vector"]
    ).to_dicts()
    assert base_rescore.filter(generated_filter)["regret_uah"].to_list() != (
        mutated_rescore.filter(generated_filter)["regret_uah"].to_list()
    )


def test_v8_teacher_label_panel_uses_rescored_candidates() -> None:
    v8_library, requirements = _v8_library(
        _candidate_panel(train_alt_regret=130.0, final_alt_regret=130.0)
    )
    rescored = build_dfl_ua_context_candidate_v8_strict_rescore_frame(v8_library)

    teacher_v8 = build_dfl_ua_context_candidate_value_teacher_label_panel_v8_frame(
        rescored,
        requirements,
        material_switch_delta_uah=25.0,
        max_prior_neighbor_distance=2.0,
        nearest_neighbor_count=3,
    )

    generated = teacher_v8.filter(
        pl.col("candidate_source") == "ua_context_v8_generated_candidate"
    )
    assert generated.height == len(TENANTS) * 5 * 5
    assert "label_v8_material_safe_switch" in teacher_v8.columns
    assert "selector_feature_v8_neighbor_safe_win_count" in teacher_v8.columns
    assert set(generated["candidate_value_label_status"].to_list()) == {
        "strict_rescored_v8_candidate"
    }
    assert set(generated["teacher_panel_version"].to_list()) == {
        "candidate_value_teacher_v8"
    }
    assert set(generated["market_execution_enabled"].to_list()) == {False}


def test_v8_selector_can_use_prior_supported_strict_rescored_candidates() -> None:
    teacher_v8 = _v8_teacher(
        _candidate_panel(
            train_alt_regret=130.0,
            final_alt_regret=130.0,
            train_strict_regret=300.0,
            final_strict_regret=300.0,
        ).with_columns(pl.lit(300.0).alias("oracle_value_uah"))
    )

    model = build_dfl_candidate_value_regret_surrogate_v8_frame(
        teacher_v8,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        max_prior_neighbor_distance=2.0,
        min_neighbor_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_neighbor_tail_risk_probability=0.30,
        allowed_candidate_sources=("ua_context_v8_generated_candidate",),
        min_prior_material_safe_switch_examples_for_dt=1,
    )
    strict = build_dfl_candidate_value_v8_strict_lp_benchmark_frame(
        teacher_v8,
        model,
        generated_at=GENERATED_AT,
    )

    assert set(model["selected_final_candidate_count"].to_list()) == {2}
    selected = strict.filter(
        pl.col("selection_role") == REGRET_SURROGATE_CANDIDATE_VALUE_V8_SELECTION_ROLE
    )
    assert selected["regret_uah"].to_list() == [20.0] * (len(TENANTS) * 2)
    assert set(selected["market_execution_enabled"].to_list()) == {False}


def test_v8_final_label_mutation_changes_scores_not_selected_candidates() -> None:
    base_teacher = _v8_teacher(
        _candidate_panel(
            train_alt_regret=130.0,
            final_alt_regret=130.0,
        ).with_columns(pl.lit(300.0).alias("oracle_value_uah"))
    )
    mutated_teacher = base_teacher.with_columns(
        pl.when(
            (pl.col("split_name") == "final_holdout")
            & (pl.col("candidate_source") == "ua_context_v8_generated_candidate")
        )
        .then(pl.col("regret_uah") + 250.0)
        .otherwise(pl.col("regret_uah"))
        .alias("regret_uah"),
        pl.when(
            (pl.col("split_name") == "final_holdout")
            & (pl.col("candidate_source") == "ua_context_v8_generated_candidate")
        )
        .then(pl.col("label_regret_delta_vs_v2_plus_uah") + 250.0)
        .otherwise(pl.col("label_regret_delta_vs_v2_plus_uah"))
        .alias("label_regret_delta_vs_v2_plus_uah"),
    )

    base_model = build_dfl_candidate_value_regret_surrogate_v8_frame(
        base_teacher,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        max_prior_neighbor_distance=2.0,
        min_neighbor_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_neighbor_tail_risk_probability=0.30,
        allowed_candidate_sources=("ua_context_v8_generated_candidate",),
        min_prior_material_safe_switch_examples_for_dt=1,
    )
    mutated_model = build_dfl_candidate_value_regret_surrogate_v8_frame(
        mutated_teacher,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        max_prior_neighbor_distance=2.0,
        min_neighbor_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_neighbor_tail_risk_probability=0.30,
        allowed_candidate_sources=("ua_context_v8_generated_candidate",),
        min_prior_material_safe_switch_examples_for_dt=1,
    )
    base_strict = build_dfl_candidate_value_v8_strict_lp_benchmark_frame(
        base_teacher,
        base_model,
        generated_at=GENERATED_AT,
    )
    mutated_strict = build_dfl_candidate_value_v8_strict_lp_benchmark_frame(
        mutated_teacher,
        mutated_model,
        generated_at=GENERATED_AT,
    )

    assert base_model["selected_final_candidate_keys"].to_list() == (
        mutated_model["selected_final_candidate_keys"].to_list()
    )
    selected_role = pl.col("selection_role") == (
        REGRET_SURROGATE_CANDIDATE_VALUE_V8_SELECTION_ROLE
    )
    assert base_strict.filter(selected_role)["regret_uah"].to_list() != (
        mutated_strict.filter(selected_role)["regret_uah"].to_list()
    )


def test_v8_false_positive_tail_risk_audit_separates_final_loss_from_prior_risk() -> None:
    base_teacher = _v8_teacher(
        _candidate_panel(
            train_alt_regret=130.0,
            final_alt_regret=130.0,
        ).with_columns(pl.lit(300.0).alias("oracle_value_uah"))
    )
    model = build_dfl_candidate_value_regret_surrogate_v8_frame(
        base_teacher,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        max_prior_neighbor_distance=2.0,
        min_neighbor_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_neighbor_tail_risk_probability=0.30,
        allowed_candidate_sources=("ua_context_v8_generated_candidate",),
        min_prior_material_safe_switch_examples_for_dt=1,
    )
    selected_keys = {
        key
        for keys in model["selected_final_candidate_keys"].to_list()
        for key in keys
    }
    mutated_teacher = base_teacher.with_columns(
        pl.when(
            pl.struct(
                [
                    "tenant_id",
                    "source_model_name",
                    "anchor_timestamp",
                    "candidate_source",
                    "candidate_family",
                    "candidate_model_name",
                ]
            )
            .map_elements(
                lambda row: (
                    f"{row['tenant_id']}|{row['source_model_name']}|"
                    f"{row['anchor_timestamp'].isoformat()}|"
                    f"{row['candidate_source']}|"
                    f"{row['candidate_family']}|"
                    f"{row['candidate_model_name']}"
                )
                in selected_keys,
                return_dtype=pl.Boolean,
            )
        )
        .then(pl.col("regret_uah") + 250.0)
        .otherwise(pl.col("regret_uah"))
        .alias("regret_uah"),
        pl.when(
            pl.struct(
                [
                    "tenant_id",
                    "source_model_name",
                    "anchor_timestamp",
                    "candidate_source",
                    "candidate_family",
                    "candidate_model_name",
                ]
            )
            .map_elements(
                lambda row: (
                    f"{row['tenant_id']}|{row['source_model_name']}|"
                    f"{row['anchor_timestamp'].isoformat()}|"
                    f"{row['candidate_source']}|"
                    f"{row['candidate_family']}|"
                    f"{row['candidate_model_name']}"
                )
                in selected_keys,
                return_dtype=pl.Boolean,
            )
        )
        .then(pl.col("label_regret_delta_vs_v2_plus_uah") + 250.0)
        .otherwise(pl.col("label_regret_delta_vs_v2_plus_uah"))
        .alias("label_regret_delta_vs_v2_plus_uah"),
    )

    base_audit = build_dfl_v8_false_positive_tail_risk_audit_frame(
        base_teacher,
        model,
        tail_risk_delta_uah=150.0,
    )
    mutated_audit = build_dfl_v8_false_positive_tail_risk_audit_frame(
        mutated_teacher,
        model,
        tail_risk_delta_uah=150.0,
    )

    selected = mutated_audit.filter(pl.col("audit_row_type") == "selected_switch")
    assert selected.height == len(selected_keys)
    assert set(selected["false_positive_class"].to_list()) == {
        "v8_false_positive_tail_risk_loss"
    }
    assert set(selected["recommended_next_action"].to_list()) == {
        "backfill_ukrainian_prior_context"
    }
    assert set(selected["market_execution_enabled"].to_list()) == {False}

    prior_columns = [
        "candidate_source",
        "candidate_family",
        "prior_candidate_count",
        "prior_safe_win_count",
        "prior_tail_risk_loss_count",
        "prior_tail_risk_probability",
        "prior_pruned_for_next_training",
    ]
    base_family = base_audit.filter(pl.col("audit_row_type") == "candidate_family")
    mutated_family = mutated_audit.filter(pl.col("audit_row_type") == "candidate_family")
    assert base_family.select(prior_columns).sort(prior_columns[:2]).to_dicts() == (
        mutated_family.select(prior_columns).sort(prior_columns[:2]).to_dicts()
    )


def test_v8_pruned_candidate_family_plan_blocks_prior_tail_risk_families() -> None:
    teacher_v8 = _v8_teacher(
        _candidate_panel(
            train_alt_regret=130.0,
            final_alt_regret=130.0,
        ).with_columns(pl.lit(300.0).alias("oracle_value_uah"))
    ).with_columns(
        pl.when(
            (pl.col("split_name") != "final_holdout")
            & (pl.col("candidate_source") == "ua_context_v8_generated_candidate")
        )
        .then(pl.lit(240.0))
        .otherwise(pl.col("label_regret_delta_vs_v2_plus_uah"))
        .alias("label_regret_delta_vs_v2_plus_uah"),
        pl.when(
            (pl.col("split_name") != "final_holdout")
            & (pl.col("candidate_source") == "ua_context_v8_generated_candidate")
        )
        .then(pl.lit(True))
        .otherwise(pl.col("label_v8_tail_risk_loss"))
        .alias("label_v8_tail_risk_loss"),
    )
    model = build_dfl_candidate_value_regret_surrogate_v8_frame(
        teacher_v8,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        max_prior_neighbor_distance=2.0,
        min_neighbor_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_neighbor_tail_risk_probability=0.30,
        allowed_candidate_sources=("ua_context_v8_generated_candidate",),
        min_prior_material_safe_switch_examples_for_dt=1,
    )

    audit = build_dfl_v8_false_positive_tail_risk_audit_frame(
        teacher_v8,
        model,
        prune_tail_risk_probability_threshold=0.50,
    )
    plan = build_dfl_v8_pruned_candidate_family_plan_frame(audit)

    generated_plan = plan.filter(
        pl.col("candidate_source") == "ua_context_v8_generated_candidate"
    )
    assert generated_plan.height > 0
    assert set(generated_plan["allowed_for_next_selector_training"].to_list()) == {False}
    assert set(generated_plan["recommended_next_action"].to_list()) == {
        "prune_candidate_family"
    }
    assert set(generated_plan["market_execution_enabled"].to_list()) == {False}


def test_v8_pruned_candidate_library_removes_prior_risk_families_but_keeps_fallbacks() -> None:
    teacher_v8 = _v8_teacher(
        _candidate_panel(
            train_alt_regret=130.0,
            final_alt_regret=130.0,
        ).with_columns(pl.lit(300.0).alias("oracle_value_uah"))
    ).with_columns(
        pl.when(
            (pl.col("split_name") != "final_holdout")
            & (pl.col("candidate_source") == "ua_context_v8_generated_candidate")
        )
        .then(pl.lit(240.0))
        .otherwise(pl.col("label_regret_delta_vs_v2_plus_uah"))
        .alias("label_regret_delta_vs_v2_plus_uah"),
        pl.when(
            (pl.col("split_name") != "final_holdout")
            & (pl.col("candidate_source") == "ua_context_v8_generated_candidate")
        )
        .then(pl.lit(True))
        .otherwise(pl.col("label_v8_tail_risk_loss"))
        .alias("label_v8_tail_risk_loss"),
    )
    model = build_dfl_candidate_value_regret_surrogate_v8_frame(
        teacher_v8,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        max_prior_neighbor_distance=2.0,
        min_neighbor_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_neighbor_tail_risk_probability=0.30,
        allowed_candidate_sources=("ua_context_v8_generated_candidate",),
        min_prior_material_safe_switch_examples_for_dt=1,
    )
    plan = build_dfl_v8_pruned_candidate_family_plan_frame(
        build_dfl_v8_false_positive_tail_risk_audit_frame(teacher_v8, model)
    )

    pruned = build_dfl_v8_pruned_candidate_library_frame(teacher_v8, plan)

    assert "ua_context_v8_generated_candidate" not in set(
        pruned["candidate_source"].to_list()
    )
    assert {"v2_plus_default", "strict_fallback"}.issubset(
        set(pruned["candidate_source"].to_list())
    )
    assert set(pruned["candidate_family_pruned_for_next_selector"].to_list()) == {
        False
    }
    assert set(pruned["market_execution_enabled"].to_list()) == {False}


def test_v8_rolling_uses_prior_rescored_neighbors_only() -> None:
    v8_library, requirements = _v8_library(
        _candidate_panel(
            train_alt_regret=130.0,
            final_alt_regret=130.0,
            train_anchor_count=4,
            final_anchor_count=4,
        ).with_columns(pl.lit(300.0).alias("oracle_value_uah"))
    )
    rescored = build_dfl_ua_context_candidate_v8_strict_rescore_frame(v8_library)

    rolling = build_dfl_candidate_value_v8_rolling_robustness_frame(
        rescored,
        requirements,
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
        allowed_candidate_sources=("ua_context_v8_generated_candidate",),
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


def _v8_library(candidate_panel: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    v7_library, _ = _v7_library_and_teacher(candidate_panel)
    requirements = _v7_requirements(v7_library)
    context_panel = build_dfl_ua_context_backfilled_feature_panel_v8_frame(
        v7_library,
        _ua_context_panel(v7_library),
        requirements,
    )
    return (
        build_dfl_ua_context_feasible_schedule_candidate_library_v8_frame(
            context_panel,
            requirements,
        ),
        requirements,
    )


def _v8_teacher(candidate_panel: pl.DataFrame) -> pl.DataFrame:
    v8_library, requirements = _v8_library(candidate_panel)
    return build_dfl_ua_context_candidate_value_teacher_label_panel_v8_frame(
        build_dfl_ua_context_candidate_v8_strict_rescore_frame(v8_library),
        requirements,
        material_switch_delta_uah=25.0,
        max_prior_neighbor_distance=2.0,
        nearest_neighbor_count=3,
    )


def _v7_requirements(v7_library: pl.DataFrame) -> pl.DataFrame:
    v6_like = v7_library.filter(
        pl.col("candidate_source") != "v7_generated_candidate"
    )
    audit = build_dfl_sparse_safe_switch_opportunity_audit_frame(
        v6_like,
        max_prior_neighbor_distance=2.0,
    )
    return build_dfl_v2_plus_opportunity_backfill_requirements_frame(v6_like, audit)


def _ua_context_panel(candidate_library: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in candidate_library.select(
        ["tenant_id", "source_model_name", "anchor_timestamp"]
    ).unique().iter_rows(named=True):
        anchor = row["anchor_timestamp"]
        rows.append(
            {
                "tenant_id": row["tenant_id"],
                "source_model_name": row["source_model_name"],
                "anchor_timestamp": anchor,
                "selector_feature_publication_time_ready": 1.0,
                "selector_feature_weather_load_context_ready": 1.0,
                "selector_feature_grid_event_context_ready": 1.0,
                "selector_feature_anchor_hour": float(anchor.hour),
                "selector_feature_anchor_is_weekend": float(anchor.weekday() >= 5),
                "selector_feature_weather_temperature_c": 18.0,
                "selector_feature_net_load_mw": 2.5,
                "selector_feature_national_grid_risk_score": 0.2,
                "diagnostic_context_blockers": [],
                "context_source": "ukrainian_only_oree_open_meteo_tenant_grid",
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None)


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
