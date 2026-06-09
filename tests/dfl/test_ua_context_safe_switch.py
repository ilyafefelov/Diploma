from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl import schedule_value_learner_v2_plus as v2_plus
from smart_arbitrage.dfl.oracle_gap_safe_switch import (
    build_dfl_oracle_gap_safe_switch_feature_panel_frame,
    build_dfl_oracle_gap_safe_switch_label_frame,
)
from smart_arbitrage.dfl.ua_context_safe_switch import (
    UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_SKLEARN,
    UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_TORCH,
    build_dfl_ua_calendar_publication_context_frame,
    build_dfl_ua_context_oracle_gap_feature_panel_frame,
    build_dfl_ua_context_safe_switch_rolling_robustness_frame,
    build_dfl_ua_context_safe_switch_scorer_frame,
    build_dfl_ua_context_safe_switch_separability_audit_frame,
    build_dfl_ua_context_safe_switch_strict_lp_benchmark_frame,
    build_dfl_ua_grid_event_context_frame,
    build_dfl_ua_weather_load_context_frame,
    evaluate_dfl_ua_context_safe_switch_gate,
)
from smart_arbitrage.dfl.ua_context_lava_dt import (
    UA_CONTEXT_LAVA_BEHAVIOR_CLONING_SELECTION_ROLE,
    UA_CONTEXT_LAVA_SELECTION_ROLE,
    UA_CONTEXT_LAVA_STRICT_LP_STRATEGY_KIND,
    build_dfl_ua_context_lava_candidate_policy_frame,
    build_dfl_ua_context_lava_rolling_robustness_frame,
    build_dfl_ua_context_lava_sequence_training_frame,
    build_dfl_ua_context_lava_strict_lp_benchmark_frame,
    build_dfl_ua_context_lava_teacher_frame,
    evaluate_dfl_ua_context_lava_gate,
)

TENANTS: tuple[str, ...] = ("tenant_a", "tenant_b")
SOURCE = "nbeatsx_official_global_panel_horizon_calibrated_v1"
GENERATED_AT = datetime(2026, 5, 22, 10)
FIRST_ANCHOR = datetime(2026, 1, 1, 12)


def test_ua_context_lanes_emit_prior_features_and_blockers() -> None:
    panel = _oracle_gap_panel(train_alt_regret=70.0, final_alt_regret=80.0)
    benchmark_context = _benchmark_context(panel)
    weather_load = build_dfl_ua_weather_load_context_frame(
        panel,
        benchmark_context,
        _net_load_context(panel),
    )
    calendar = build_dfl_ua_calendar_publication_context_frame(panel, benchmark_context)
    grid = build_dfl_ua_grid_event_context_frame(panel, _grid_context(panel))

    assert set(calendar["calendar_publication_context_blocker"].unique().to_list()) == {
        "context_ready"
    }
    assert set(weather_load["weather_load_context_blocker"].unique().to_list()) == {
        "context_ready"
    }
    assert set(grid["grid_event_context_blocker"].unique().to_list()) == {
        "context_ready"
    }
    assert weather_load.select(
        (pl.col("feature_available_timestamp") < pl.col("anchor_timestamp")).all()
    ).item()
    assert grid.select(
        (pl.col("feature_available_timestamp") <= pl.col("anchor_timestamp")).all()
    ).item()
    assert set(calendar["market_execution_enabled"].unique().to_list()) == {False}


def test_ua_context_feature_panel_keeps_features_prior_only_when_labels_mutate() -> None:
    base = _ua_feature_panel(train_alt_regret=70.0, final_alt_regret=80.0)
    mutated = _ua_feature_panel(train_alt_regret=70.0, final_alt_regret=450.0)
    feature_columns = sorted(
        column for column in base.columns if column.startswith("selector_feature_")
    )

    assert feature_columns
    assert base.select(feature_columns).to_dicts() == mutated.select(
        feature_columns
    ).to_dicts()
    assert base.select("label_regret_delta_vs_v2_plus_uah").to_dicts() != (
        mutated.select("label_regret_delta_vs_v2_plus_uah").to_dicts()
    )


def test_ua_context_safe_switch_models_can_select_safe_candidate() -> None:
    panel = _ua_feature_panel(train_alt_regret=70.0, final_alt_regret=80.0)
    audit = build_dfl_ua_context_safe_switch_separability_audit_frame(panel)
    scorer = build_dfl_ua_context_safe_switch_scorer_frame(
        panel,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        scorer_kinds=("sklearn", "torch"),
        min_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_predicted_tail_risk_probability=0.30,
        torch_max_epochs=8,
    )
    strict = build_dfl_ua_context_safe_switch_strict_lp_benchmark_frame(
        panel,
        scorer,
        _oracle_gap_inputs(train_alt_regret=70.0, final_alt_regret=80.0)[
            "schedule_value_v2_plus_strict_frame"
        ],
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_ua_context_safe_switch_gate(
        strict,
        selection_role=UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_SKLEARN,
        min_validation_tenant_anchor_count=len(TENANTS) * 2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    assert audit["missed_safe_switch_opportunity_count"].sum() >= 0
    assert set(scorer["fallback_to_v2_plus"].to_list()) == {False}
    assert {
        UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_SKLEARN,
        UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_TORCH,
    }.issubset(set(strict["selection_role"].unique().to_list()))
    assert gate.passed is True
    assert gate.metrics["market_execution_enabled"] is False


def test_ua_context_safe_switch_falls_back_on_tail_risk() -> None:
    panel = _ua_feature_panel(train_alt_regret=290.0, final_alt_regret=20.0)
    scorer = build_dfl_ua_context_safe_switch_scorer_frame(
        panel,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        scorer_kinds=("sklearn", "torch"),
        min_prior_safe_win_count=1,
        max_predicted_tail_risk_probability=0.25,
        torch_max_epochs=4,
    )
    strict = build_dfl_ua_context_safe_switch_strict_lp_benchmark_frame(
        panel,
        scorer,
        _oracle_gap_inputs(train_alt_regret=290.0, final_alt_regret=20.0)[
            "schedule_value_v2_plus_strict_frame"
        ],
        generated_at=GENERATED_AT,
    )

    assert set(scorer["fallback_to_v2_plus"].to_list()) == {True}
    for role in (
        UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_SKLEARN,
        UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_TORCH,
    ):
        assert strict.filter(pl.col("selection_role") == role)[
            "regret_uah"
        ].to_list() == [120.0] * (len(TENANTS) * 2)


def test_ua_context_rolling_robustness_uses_prior_windows() -> None:
    panel = _ua_feature_panel(
        train_alt_regret=70.0,
        final_alt_regret=80.0,
        train_anchor_count=4,
        final_anchor_count=4,
    )

    rolling = build_dfl_ua_context_safe_switch_rolling_robustness_frame(
        panel,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        validation_window_count=2,
        validation_anchor_count=2,
        min_prior_anchors_before_window=2,
        scorer_kinds=("sklearn",),
        min_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_predicted_tail_risk_probability=0.30,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    assert rolling.height == 2
    assert set(rolling["rolling_window_passed"].to_list()) == {True}
    assert rolling["minimum_prior_anchor_count_before_window"].min() >= 2
    assert set(rolling["market_execution_enabled"].to_list()) == {False}


def test_ua_context_lava_teacher_uses_candidate_index_not_raw_actions() -> None:
    panel = _ua_feature_panel(train_alt_regret=70.0, final_alt_regret=80.0)

    teacher = build_dfl_ua_context_lava_teacher_frame(panel)
    training = build_dfl_ua_context_lava_sequence_training_frame(teacher)

    assert "teacher_candidate_index" in teacher.columns
    assert set(teacher["target_label_space"].unique().to_list()) == {
        "ua_context_schedule_candidate_index"
    }
    assert set(teacher["raw_hourly_action_imitation"].unique().to_list()) == {False}
    assert "safe_schedule_candidate" in set(
        teacher["teacher_schedule_candidate_class"].to_list()
    )
    assert training.filter(pl.col("split_name") == "final_holdout").select(
        pl.col("is_training_row").any()
    ).item() is False
    assert set(training["market_execution_enabled"].unique().to_list()) == {False}


def test_ua_context_lava_policy_can_select_safe_candidate_and_emit_bc_baseline() -> None:
    panel = _ua_feature_panel(train_alt_regret=70.0, final_alt_regret=80.0)
    training = build_dfl_ua_context_lava_sequence_training_frame(
        build_dfl_ua_context_lava_teacher_frame(panel)
    )
    policy = build_dfl_ua_context_lava_candidate_policy_frame(
        training,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        min_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_predicted_tail_risk_probability=0.30,
        torch_max_epochs=16,
        use_cuda_if_available=False,
    )
    strict = build_dfl_ua_context_lava_strict_lp_benchmark_frame(
        training,
        policy,
        _oracle_gap_inputs(train_alt_regret=70.0, final_alt_regret=80.0)[
            "schedule_value_v2_plus_strict_frame"
        ],
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_ua_context_lava_gate(
        strict,
        min_validation_tenant_anchor_count=len(TENANTS) * 2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    selected = strict.filter(pl.col("selection_role") == UA_CONTEXT_LAVA_SELECTION_ROLE)
    assert set(policy["fallback_to_v2_plus"].to_list()) == {False}
    assert selected["regret_uah"].to_list() == [80.0] * (len(TENANTS) * 2)
    assert {
        UA_CONTEXT_LAVA_SELECTION_ROLE,
        UA_CONTEXT_LAVA_BEHAVIOR_CLONING_SELECTION_ROLE,
    }.issubset(set(strict["selection_role"].unique().to_list()))
    for role in (
        UA_CONTEXT_LAVA_SELECTION_ROLE,
        UA_CONTEXT_LAVA_BEHAVIOR_CLONING_SELECTION_ROLE,
    ):
        role_payloads = (
            strict.filter(pl.col("selection_role") == role)["evaluation_payload"]
            .head(1)
            .to_list()
        )
        assert role_payloads[0]["selection_role"] == role
        assert role_payloads[0]["ua_context_lava_role"] == role
    assert set(strict["strategy_kind"].unique().to_list()) == {
        UA_CONTEXT_LAVA_STRICT_LP_STRATEGY_KIND
    }
    assert gate.passed is True
    assert gate.metrics["market_execution_enabled"] is False


def test_ua_context_lava_policy_falls_back_on_tail_risk_or_weak_signal() -> None:
    panel = _ua_feature_panel(train_alt_regret=290.0, final_alt_regret=20.0)
    training = build_dfl_ua_context_lava_sequence_training_frame(
        build_dfl_ua_context_lava_teacher_frame(panel)
    )
    policy = build_dfl_ua_context_lava_candidate_policy_frame(
        training,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        min_prior_safe_win_count=1,
        max_predicted_tail_risk_probability=0.25,
        torch_max_epochs=8,
        use_cuda_if_available=False,
    )
    strict = build_dfl_ua_context_lava_strict_lp_benchmark_frame(
        training,
        policy,
        _oracle_gap_inputs(train_alt_regret=290.0, final_alt_regret=20.0)[
            "schedule_value_v2_plus_strict_frame"
        ],
        generated_at=GENERATED_AT,
    )

    selected = strict.filter(pl.col("selection_role") == UA_CONTEXT_LAVA_SELECTION_ROLE)
    assert set(policy["fallback_to_v2_plus"].to_list()) == {True}
    assert selected["regret_uah"].to_list() == [120.0] * (len(TENANTS) * 2)


def test_ua_context_lava_rolling_uses_prior_windows_only() -> None:
    panel = _ua_feature_panel(
        train_alt_regret=70.0,
        final_alt_regret=80.0,
        train_anchor_count=4,
        final_anchor_count=4,
    )
    training = build_dfl_ua_context_lava_sequence_training_frame(
        build_dfl_ua_context_lava_teacher_frame(panel)
    )

    rolling = build_dfl_ua_context_lava_rolling_robustness_frame(
        training,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        validation_window_count=2,
        validation_anchor_count=2,
        min_prior_anchors_before_window=2,
        min_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_predicted_tail_risk_probability=0.30,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
        torch_max_epochs=8,
        use_cuda_if_available=False,
    )

    assert rolling.height == 2
    assert set(rolling["rolling_window_passed"].to_list()) == {True}
    assert rolling["minimum_prior_anchor_count_before_window"].min() >= 2
    assert set(rolling["market_execution_enabled"].to_list()) == {False}


def _ua_feature_panel(
    *,
    train_alt_regret: float,
    final_alt_regret: float,
    train_anchor_count: int = 3,
    final_anchor_count: int = 2,
) -> pl.DataFrame:
    panel = _oracle_gap_panel(
        train_alt_regret=train_alt_regret,
        final_alt_regret=final_alt_regret,
        train_anchor_count=train_anchor_count,
        final_anchor_count=final_anchor_count,
    )
    benchmark_context = _benchmark_context(panel)
    return build_dfl_ua_context_oracle_gap_feature_panel_frame(
        panel,
        build_dfl_ua_calendar_publication_context_frame(panel, benchmark_context),
        build_dfl_ua_weather_load_context_frame(
            panel,
            benchmark_context,
            _net_load_context(panel),
        ),
        build_dfl_ua_grid_event_context_frame(panel, _grid_context(panel)),
    )


def _oracle_gap_panel(
    *,
    train_alt_regret: float,
    final_alt_regret: float,
    train_anchor_count: int = 3,
    final_anchor_count: int = 2,
) -> pl.DataFrame:
    frames = _oracle_gap_inputs(
        train_alt_regret=train_alt_regret,
        final_alt_regret=final_alt_regret,
        train_anchor_count=train_anchor_count,
        final_anchor_count=final_anchor_count,
    )
    return build_dfl_oracle_gap_safe_switch_feature_panel_frame(
        build_dfl_oracle_gap_safe_switch_label_frame(**frames)
    )


def _oracle_gap_inputs(
    *,
    train_alt_regret: float,
    final_alt_regret: float,
    tail_alt_regret: float = 290.0,
    train_anchor_count: int = 3,
    final_anchor_count: int = 2,
) -> dict[str, pl.DataFrame]:
    library = _candidate_library(
        train_alt_regret=train_alt_regret,
        final_alt_regret=final_alt_regret,
        tail_alt_regret=tail_alt_regret,
        train_anchor_count=train_anchor_count,
        final_anchor_count=final_anchor_count,
    )
    v2_model = v2.build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        final_validation_anchor_count_per_tenant=final_anchor_count,
    )
    v2_plus_model = v2_plus.build_dfl_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        final_validation_anchor_count_per_tenant=final_anchor_count,
        min_prior_mean_improvement_ratio_vs_v2=0.01,
    )
    strict = v2_plus.build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        library,
        v2_plus_model,
        v2_model,
        generated_at=GENERATED_AT,
    )
    audit = v2_plus.build_dfl_schedule_value_learner_v2_plus_oracle_gap_audit_frame(
        library,
        strict,
        source_model_names=(SOURCE,),
    )
    return {
        "schedule_candidate_library_frame": library,
        "schedule_value_v2_plus_frame": v2_plus_model,
        "schedule_value_v2_frame": v2_model,
        "schedule_value_v2_plus_strict_frame": strict,
        "oracle_gap_audit_frame": audit,
    }


def _candidate_library(
    *,
    train_alt_regret: float,
    final_alt_regret: float,
    tail_alt_regret: float,
    train_anchor_count: int,
    final_anchor_count: int,
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
            for family, model_name, regret, prior_mean, dispatch in (
                (
                    "strict_control",
                    "strict_similar_day",
                    300.0,
                    130.0,
                    [0.0, 0.0, 0.0, 0.0],
                ),
                (
                    v2.CANDIDATE_FAMILY_RAW,
                    "raw_nbeatsx",
                    500.0,
                    140.0,
                    [0.1, 0.0, -0.1, 0.0],
                ),
                (
                    "rank_extrema_perturbation_v2_plus",
                    "rank_extrema_safe",
                    120.0,
                    60.0,
                    [0.1, -0.1, 0.0, 0.0],
                ),
                (
                    "robust_spread_penalty_v2_plus",
                    "ua_context_safe_switch",
                    alt_regret,
                    80.0,
                    [0.2, -0.2, 0.0, 0.0],
                ),
                (
                    "strict_neighborhood_shift_v2_plus",
                    "tail_risk_shift",
                    tail_alt_regret,
                    90.0,
                    [0.4, -0.4, 0.0, 0.0],
                ),
            ):
                rows.append(
                    _candidate_row(
                        tenant=tenant,
                        anchor=anchor,
                        split_name=split_name,
                        family=family,
                        model_name=model_name,
                        regret=regret,
                        prior_family_mean_regret=prior_mean,
                        dispatch=dispatch,
                    )
                )
    return pl.DataFrame(rows)


def _candidate_row(
    *,
    tenant: str,
    anchor: datetime,
    split_name: str,
    family: str,
    model_name: str,
    regret: float,
    prior_family_mean_regret: float,
    dispatch: list[float],
) -> dict[str, object]:
    forecast = [1000.0, 3000.0, 1500.0, 4000.0]
    actual = [1100.0, 2800.0, 1600.0, 3900.0]
    soc = [0.5 + sum(dispatch[: index + 1]) * 0.05 for index in range(len(dispatch))]
    return {
        "tenant_id": tenant,
        "source_model_name": SOURCE,
        "candidate_family": family,
        "candidate_model_name": model_name,
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "split_name": split_name,
        "horizon_hours": 4,
        "forecast_price_uah_mwh_vector": forecast,
        "actual_price_uah_mwh_vector": actual,
        "dispatch_mw_vector": dispatch,
        "soc_fraction_vector": soc,
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0 - regret,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "prior_family_mean_regret_uah": prior_family_mean_regret,
        "forecast_spread_uah_mwh": max(forecast) - min(forecast),
        "total_degradation_penalty_uah": abs(sum(dispatch)) * 10.0,
        "total_throughput_mwh": sum(abs(value) for value in dispatch),
        "soc_min_slack_fraction": min(soc),
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": 0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evaluation_payload": {
            "dispatch_mw": dispatch,
            "soc_fraction": soc,
            "candidate_family": family,
            "candidate_model_name": model_name,
        },
    }


def _benchmark_context(panel: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in panel.unique(["tenant_id", "anchor_timestamp"]).iter_rows(named=True):
        anchor = row["anchor_timestamp"]
        if not isinstance(anchor, datetime):
            raise TypeError("anchor_timestamp must be datetime.")
        rows.append(
            {
                "tenant_id": row["tenant_id"],
                "timestamp": anchor - timedelta(hours=1),
                "price_uah_mwh": 1200.0,
                "weather_temperature": 18.0,
                "weather_wind_speed": 4.5,
                "weather_precipitation": 0.0,
                "weather_effective_solar": 0.42,
                "weather_source_kind": "observed_open_meteo",
                "publication_timestamp": anchor - timedelta(hours=24),
                "source_kind": "observed_oree_dam",
            }
        )
    return pl.DataFrame(rows)


def _net_load_context(panel: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in panel.unique(["tenant_id", "anchor_timestamp"]).iter_rows(named=True):
        anchor = row["anchor_timestamp"]
        if not isinstance(anchor, datetime):
            raise TypeError("anchor_timestamp must be datetime.")
        rows.append(
            {
                "tenant_id": row["tenant_id"],
                "timestamp": anchor - timedelta(hours=1),
                "net_load_mw": 0.55,
                "pv_estimate_mw": 0.12,
                "source_kind": "configured_proxy",
            }
        )
    return pl.DataFrame(rows)


def _grid_context(panel: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in panel.unique(["tenant_id", "anchor_timestamp"]).iter_rows(named=True):
        anchor = row["anchor_timestamp"]
        if not isinstance(anchor, datetime):
            raise TypeError("anchor_timestamp must be datetime.")
        rows.append(
            {
                "tenant_id": row["tenant_id"],
                "timestamp": anchor - timedelta(hours=1),
                "grid_event_count_24h": 1.0,
                "tenant_region_affected": 1.0,
                "national_grid_risk_score": 0.25,
                "days_since_grid_event": 0.1,
                "outage_flag": 0.0,
                "saving_request_flag": 1.0,
                "solar_shift_hint": 0.0,
                "event_source_freshness_hours": 2.0,
            }
        )
    return pl.DataFrame(rows)
