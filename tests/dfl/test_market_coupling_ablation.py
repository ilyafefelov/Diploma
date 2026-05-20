from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.market_coupling_ablation import (
    build_dfl_market_coupling_v2_plus_ablation_frame,
    validate_dfl_market_coupling_v2_plus_ablation_evidence,
)
from smart_arbitrage.dfl.market_coupling_ablation_export import (
    build_dfl_market_coupling_v2_plus_ablation_packet,
    write_dfl_market_coupling_v2_plus_ablation_packet,
)
from smart_arbitrage.dfl.market_coupled_v2_plus import (
    DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND,
    build_dfl_market_coupled_schedule_value_learner_v2_plus_frame,
    build_dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.schedule_value_learner import (
    build_dfl_schedule_value_learner_v2_frame,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_CLAIM_SCOPE,
    DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND,
    build_dfl_schedule_value_learner_v2_plus_frame,
    build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
    schedule_value_learner_v2_plus_model_name,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus_robustness import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_CLAIM_SCOPE,
)
from smart_arbitrage.forecasting.market_coupling_features import (
    MARKET_COUPLING_FEATURE_ROUTE_CLAIM_SCOPE,
)

SOURCE = "nbeatsx_official_global_panel_horizon_calibrated_v1"
TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)


def test_market_coupling_ablation_blocks_without_approved_features() -> None:
    frame = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0]),
        _robustness_frame(passed_windows=4),
        _feature_route(approved=False),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )

    row = frame.row(0, named=True)
    assert row["ablation_status"] == "blocked_by_governance"
    assert row["did_train_market_coupled_variant"] is False
    assert row["ablation_passed"] is False
    assert row["ablation_blocker"] == "no_approved_external_features"
    assert row["baseline_mean_regret_uah"] == 100.0
    assert row["market_execution_enabled"] is False

    outcome = validate_dfl_market_coupling_v2_plus_ablation_evidence(
        frame,
        source_model_names=(SOURCE,),
    )
    assert outcome.passed is True


def test_market_coupling_ablation_can_wait_for_experimental_route_materialization() -> None:
    frame = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0]),
        _robustness_frame(passed_windows=4),
        _feature_route(approved=False, experimental=True),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )

    row = frame.row(0, named=True)
    assert row["ablation_status"] == "approved_route_pending_materialization"
    assert row["did_train_market_coupled_variant"] is False
    assert row["approved_external_feature_columns_csv"] == (
        "entsoe_neighbor_day_ahead_price_context"
    )
    assert row["ablation_blocker"] == "missing_market_coupled_v2_plus_evidence"
    assert row["market_execution_enabled"] is False


def test_market_coupled_b_variant_emits_strict_rows_from_experimental_poland_route() -> None:
    library = _candidate_library_for_market_coupled_variant()
    v2_model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        final_validation_anchor_count_per_tenant=18,
    )
    v2_plus_model = build_dfl_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        final_validation_anchor_count_per_tenant=18,
    )

    b_model = build_dfl_market_coupled_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        v2_plus_model,
        _feature_route(approved=False, experimental=True),
        _lagged_poland_feature_frame(library),
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        final_validation_anchor_count_per_tenant=18,
    )
    strict_frame = build_dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        library,
        v2_model,
        v2_plus_model,
        b_model,
        generated_at=datetime(2026, 5, 20, 12),
    )

    assert b_model.height == len(TENANTS)
    assert set(b_model["approved_external_feature_columns_csv"].to_list()) == {
        "entsoe_neighbor_day_ahead_price_context"
    }
    assert set(strict_frame["strategy_kind"].unique().to_list()) == {
        DFL_MARKET_COUPLED_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND
    }
    assert set(strict_frame["selection_role"].unique().to_list()) == {
        "raw_reference",
        "schedule_value_learner_v2_plus",
        "schedule_value_learner_v2_reference",
        "strict_reference",
    }
    selected_payload = strict_frame.filter(
        pl.col("selection_role") == "schedule_value_learner_v2_plus"
    ).row(0, named=True)["evaluation_payload"]
    assert selected_payload["market_coupled_variant"] is True
    assert selected_payload["approved_external_feature_columns"] == [
        "entsoe_neighbor_day_ahead_price_context"
    ]
    assert strict_frame["not_market_execution"].all()


def test_market_coupled_b_variant_uses_rich_poland_regime_features_when_prior_safe() -> None:
    library = _regime_sensitive_candidate_library_for_market_coupled_variant()
    v2_model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        final_validation_anchor_count_per_tenant=18,
    )
    v2_plus_model = build_dfl_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        final_validation_anchor_count_per_tenant=18,
    )

    b_model = build_dfl_market_coupled_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        v2_plus_model,
        _feature_route(approved=False, experimental=True),
        _rich_lagged_poland_feature_frame(library),
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        final_validation_anchor_count_per_tenant=18,
        min_prior_mean_improvement_ratio_vs_ukrainian_v2_plus=0.0,
    )

    assert b_model.height == len(TENANTS)
    assert b_model["fallback_to_ukrainian_v2_plus"].unique().to_list() == [False]
    assert set(b_model["selected_weight_profile_name"].to_list()) == {
        "poland_lag24_rich_regime_selector"
    }
    selected_features = b_model.row(0, named=True)["selected_feature_names"]
    assert "entsoe_pl_lag24_delta_1h_uah_mwh" in selected_features
    assert "entsoe_pl_lag24_daily_spread_uah_mwh" in selected_features
    assert b_model["selected_train_mean_regret_uah"].max() < b_model[
        "ukrainian_v2_plus_train_mean_regret_uah"
    ].min()
    assert b_model["selected_final_mean_regret_uah"].max() < b_model[
        "ukrainian_v2_plus_final_mean_regret_uah"
    ].min()


def test_market_coupling_ablation_compares_materialized_b_variant() -> None:
    library = _candidate_library_for_market_coupled_variant()
    v2_model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        final_validation_anchor_count_per_tenant=18,
    )
    v2_plus_model = build_dfl_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        final_validation_anchor_count_per_tenant=18,
    )
    baseline_strict = build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        library,
        v2_plus_model,
        v2_model,
        generated_at=datetime(2026, 5, 20, 12),
    )
    b_model = build_dfl_market_coupled_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        v2_plus_model,
        _feature_route(approved=False, experimental=True),
        _lagged_poland_feature_frame(library),
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        final_validation_anchor_count_per_tenant=18,
    )
    market_coupled_strict = build_dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        library,
        v2_model,
        v2_plus_model,
        b_model,
        generated_at=datetime(2026, 5, 20, 12),
    )

    frame = build_dfl_market_coupling_v2_plus_ablation_frame(
        baseline_strict,
        _robustness_frame(passed_windows=4),
        _feature_route(approved=False, experimental=True),
        market_coupled_strict_frame=market_coupled_strict,
        market_coupled_robustness_frame=_robustness_frame(passed_windows=4),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )

    row = frame.row(0, named=True)
    assert row["ablation_status"] == "comparison_complete"
    assert row["did_train_market_coupled_variant"] is True
    assert row["market_execution_enabled"] is False


def test_market_coupling_ablation_passes_only_when_market_coupled_variant_beats_v2_plus() -> None:
    frame = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0]),
        _robustness_frame(passed_windows=4),
        _feature_route(approved=True),
        market_coupled_strict_frame=_strict_frame(
            baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0],
            selected_regrets=[80.0, 80.0, 80.0, 80.0],
        ),
        market_coupled_robustness_frame=_robustness_frame(passed_windows=4),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )

    row = frame.row(0, named=True)
    assert row["ablation_status"] == "comparison_complete"
    assert row["did_train_market_coupled_variant"] is True
    assert row["ablation_passed"] is True
    assert row["approved_external_feature_columns_csv"] == (
        "entsoe_neighbor_day_ahead_price_context"
    )
    assert row["mean_regret_improvement_ratio_vs_ukrainian_v2_plus"] == 0.2
    assert row["rolling_robustness_preserved"] is True


def test_market_coupling_ablation_blocks_median_degradation() -> None:
    frame = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[20.0, 20.0, 20.0, 220.0]),
        _robustness_frame(passed_windows=4),
        _feature_route(approved=True),
        market_coupled_strict_frame=_strict_frame(
            baseline_selected_regrets=[20.0, 20.0, 20.0, 220.0],
            selected_regrets=[0.0, 0.0, 100.0, 100.0],
        ),
        market_coupled_robustness_frame=_robustness_frame(passed_windows=4),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )

    row = frame.row(0, named=True)
    assert row["ablation_passed"] is False
    assert "median_degraded" in row["ablation_blocker"]


def test_market_coupling_ablation_scoring_changes_do_not_change_approved_route() -> None:
    route = _feature_route(approved=True)
    base = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0]),
        _robustness_frame(passed_windows=4),
        route,
        market_coupled_strict_frame=_strict_frame(
            baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0],
            selected_regrets=[80.0, 80.0, 80.0, 80.0],
        ),
        market_coupled_robustness_frame=_robustness_frame(passed_windows=4),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )
    mutated = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0]),
        _robustness_frame(passed_windows=4),
        route,
        market_coupled_strict_frame=_strict_frame(
            baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0],
            selected_regrets=[140.0, 140.0, 140.0, 140.0],
        ),
        market_coupled_robustness_frame=_robustness_frame(passed_windows=4),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )

    base_row = base.row(0, named=True)
    mutated_row = mutated.row(0, named=True)
    assert base_row["approved_external_feature_columns_csv"] == mutated_row[
        "approved_external_feature_columns_csv"
    ]
    assert base_row["ablation_passed"] is True
    assert mutated_row["ablation_passed"] is False


def test_market_coupling_ablation_validation_rejects_false_claim_flags() -> None:
    frame = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0]),
        _robustness_frame(passed_windows=4),
        _feature_route(approved=False),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    ).with_columns(pl.lit(False).alias("not_market_execution"))

    outcome = validate_dfl_market_coupling_v2_plus_ablation_evidence(
        frame,
        source_model_names=(SOURCE,),
    )

    assert outcome.passed is False
    assert "research-only claim flags" in outcome.description


def test_market_coupling_ablation_packet_writes_blocked_governance_artifacts(
    tmp_path,
) -> None:
    frame = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0]),
        _robustness_frame(passed_windows=4),
        _feature_route(approved=False),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )

    packet = build_dfl_market_coupling_v2_plus_ablation_packet(
        run_slug="market-coupling-ablation-test",
        ablation_frame=frame,
        dagster_run_id="run-abc",
        materialization_command="dagster asset materialize ...",
    )
    export_dir = write_dfl_market_coupling_v2_plus_ablation_packet(
        packet,
        output_root=tmp_path,
        ablation_frame=frame,
    )

    assert packet["baseline_comparator"]["calibrated_v2_plus_mean_regret_uah"] == 174.77
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert packet["ablation_summary"]["status_counts"] == {"blocked_by_governance": 1}
    assert packet["ablation_summary"]["trained_market_coupled_variant_count"] == 0
    assert (
        export_dir / "dfl_market_coupling_v2_plus_ablation_summary.json"
    ).exists()
    assert (
        export_dir / "dfl_market_coupling_v2_plus_ablation_summary.md"
    ).exists()
    assert (export_dir / "dfl_market_coupling_v2_plus_ablation_rows.csv").exists()
    markdown = (
        export_dir / "dfl_market_coupling_v2_plus_ablation_summary.md"
    ).read_text(encoding="utf-8")
    assert "blocked_by_governance" in markdown
    assert "No market-coupled training run was executed" in markdown


def test_market_coupling_ablation_packet_refuses_failed_evidence() -> None:
    frame = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0]),
        _robustness_frame(passed_windows=4),
        _feature_route(approved=False),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    ).drop("claim_scope")

    try:
        build_dfl_market_coupling_v2_plus_ablation_packet(
            run_slug="market-coupling-ablation-test",
            ablation_frame=frame,
        )
    except ValueError as exc:
        assert "ablation evidence check failed" in str(exc)
    else:
        raise AssertionError("Expected failed evidence to block export.")


def _feature_route(*, approved: bool, experimental: bool = False) -> pl.DataFrame:
    status = "approved_for_training" if approved else "blocked_by_governance"
    blockers = "" if approved else ("domain_shift" if experimental else "publication_time,prior_fx,licensing")
    point_in_time_ready = approved or experimental
    return pl.DataFrame(
        [
            {
                "feature_name": "entsoe_neighbor_day_ahead_price_context",
                "source_name": "ENTSO_E",
                "source_kind": "neighbor_market_api",
                "approved_feature_column": "entsoe_neighbor_day_ahead_price_context",
                "feature_route_status": status,
                "source_backed_row_count": 24 if point_in_time_ready else 0,
                "training_use_allowed": approved,
                "feature_use_allowed": approved,
                "approved_for_official_training": approved,
                "experimental_feature_route_status": (
                    "approved_for_experimental_ablation"
                    if experimental
                    else "blocked_for_experimental_ablation"
                ),
                "approved_for_experimental_ablation": experimental,
                "training_blockers_csv": blockers,
                "readiness_status": "training_ready" if approved else "blocked",
                "licensing_status": "ready" if point_in_time_ready else "blocked",
                "timezone_status": "ready" if point_in_time_ready else "blocked",
                "currency_status": "ready" if point_in_time_ready else "blocked",
                "market_rules_status": "ready" if point_in_time_ready else "blocked",
                "temporal_availability_status": "ready" if point_in_time_ready else "blocked",
                "domain_shift_status": "ready" if approved else "blocked",
                "publication_time_policy": "must_be_published_before_ua_anchor",
                "decision_cutoff_policy": "ua_dam_day_ahead_cutoff",
                "external_feature_role": "future_market_coupling_covariate",
                "claim_scope": MARKET_COUPLING_FEATURE_ROUTE_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        ]
    )


def _candidate_library_for_market_coupled_variant() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    first_anchor = datetime(2026, 1, 1, 23)
    for tenant_id in TENANTS:
        for anchor_index in range(40):
            anchor = first_anchor + timedelta(days=anchor_index)
            split_name = "final_holdout" if anchor_index >= 22 else "train_selection"
            family_regrets = {
                "strict_control": 300.0,
                "raw_source": 700.0,
                "forecast_perturbation": 220.0,
                "rank_extrema_perturbation_v2_plus": 180.0,
            }
            for family, regret in family_regrets.items():
                forecast = [1000.0, 5000.0, 1500.0, 4000.0]
                rows.append(
                    {
                        "tenant_id": tenant_id,
                        "source_model_name": SOURCE,
                        "candidate_family": family,
                        "candidate_model_name": f"{family}_{SOURCE}",
                        "anchor_timestamp": anchor,
                        "generated_at": datetime(2026, 5, 20, 12),
                        "split_name": split_name,
                        "horizon_hours": 4,
                        "forecast_price_uah_mwh_vector": forecast,
                        "actual_price_uah_mwh_vector": [900.0, 5200.0, 1400.0, 4200.0],
                        "dispatch_mw_vector": [0.1, -0.1, 0.0, 0.1],
                        "soc_fraction_vector": [0.5, 0.6, 0.5, 0.5],
                        "decision_value_uah": 1000.0 - regret,
                        "forecast_objective_value_uah": 1000.0 - regret,
                        "oracle_value_uah": 1000.0,
                        "regret_uah": regret,
                        "regret_ratio": regret / 1000.0,
                        "total_degradation_penalty_uah": 0.0,
                        "total_throughput_mwh": 0.2,
                        "forecast_spread_uah_mwh": 4000.0,
                        "actual_spread_uah_mwh": 4300.0,
                        "forecast_top_k_actual_overlap": 1.0,
                        "forecast_bottom_k_actual_overlap": 1.0,
                        "peak_index_abs_error": 0.0,
                        "trough_index_abs_error": 0.0,
                        "soc_min_slack_fraction": 0.4,
                        "prior_family_mean_regret_uah": regret,
                        "safety_violation_count": 0,
                        "data_quality_tier": "thesis_grade",
                        "observed_coverage_ratio": 1.0,
                        "not_full_dfl": True,
                        "not_market_execution": True,
                        "claim_scope": "dfl_schedule_candidate_library_v2_plus_not_full_dfl",
                        "evaluation_payload": {
                            "selector_row_candidate_family": family,
                            "not_full_dfl": True,
                            "not_market_execution": True,
                        },
                        "candidate_library_version": "test",
                    }
                )
    return pl.DataFrame(rows)


def _regime_sensitive_candidate_library_for_market_coupled_variant() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    first_anchor = datetime(2026, 1, 1, 23)
    for tenant_id in TENANTS:
        for anchor_index in range(40):
            anchor = first_anchor + timedelta(days=anchor_index)
            split_name = "final_holdout" if anchor_index >= 22 else "train_selection"
            high_poland_regime = anchor_index % 2 == 1
            family_regrets = {
                "strict_control": 300.0,
                "raw_source": 700.0,
                "forecast_perturbation": 220.0,
                "rank_extrema_perturbation_v2_plus": 260.0
                if high_poland_regime
                else 120.0,
                "robust_spread_penalty_v2_plus": 120.0
                if high_poland_regime
                else 260.0,
            }
            family_prior_means = {
                "strict_control": 300.0,
                "raw_source": 700.0,
                "forecast_perturbation": 220.0,
                "rank_extrema_perturbation_v2_plus": 170.0,
                "robust_spread_penalty_v2_plus": 190.0,
            }
            for family, regret in family_regrets.items():
                forecast = [1000.0, 5000.0, 1500.0, 4000.0]
                rows.append(
                    {
                        "tenant_id": tenant_id,
                        "source_model_name": SOURCE,
                        "candidate_family": family,
                        "candidate_model_name": f"{family}_{SOURCE}",
                        "anchor_timestamp": anchor,
                        "generated_at": datetime(2026, 5, 20, 12),
                        "split_name": split_name,
                        "horizon_hours": 4,
                        "forecast_price_uah_mwh_vector": forecast,
                        "actual_price_uah_mwh_vector": [900.0, 5200.0, 1400.0, 4200.0],
                        "dispatch_mw_vector": [0.1, -0.1, 0.0, 0.1],
                        "soc_fraction_vector": [0.5, 0.6, 0.5, 0.5],
                        "decision_value_uah": 1000.0 - regret,
                        "forecast_objective_value_uah": 1000.0 - regret,
                        "oracle_value_uah": 1000.0,
                        "regret_uah": regret,
                        "regret_ratio": regret / 1000.0,
                        "total_degradation_penalty_uah": 0.0,
                        "total_throughput_mwh": 0.2,
                        "forecast_spread_uah_mwh": 4000.0,
                        "actual_spread_uah_mwh": 4300.0,
                        "forecast_top_k_actual_overlap": 1.0,
                        "forecast_bottom_k_actual_overlap": 1.0,
                        "peak_index_abs_error": 0.0,
                        "trough_index_abs_error": 0.0,
                        "soc_min_slack_fraction": 0.4,
                        "prior_family_mean_regret_uah": family_prior_means[family],
                        "safety_violation_count": 0,
                        "data_quality_tier": "thesis_grade",
                        "observed_coverage_ratio": 1.0,
                        "not_full_dfl": True,
                        "not_market_execution": True,
                        "claim_scope": "dfl_schedule_candidate_library_v2_plus_not_full_dfl",
                        "evaluation_payload": {
                            "selector_row_candidate_family": family,
                            "not_full_dfl": True,
                            "not_market_execution": True,
                        },
                        "candidate_library_version": "test",
                    }
                )
    return pl.DataFrame(rows)


def _lagged_poland_feature_frame(library: pl.DataFrame) -> pl.DataFrame:
    anchors = [
        row["anchor_timestamp"]
        for row in library.select("anchor_timestamp").unique().iter_rows(named=True)
    ]
    timestamps = {
        anchor + timedelta(hours=offset)
        for anchor in anchors
        for offset in range(4)
    }
    return pl.DataFrame(
        [
            {
                "delivery_timestamp_utc": timestamp,
                "feature_column": "entsoe_neighbor_day_ahead_price_context",
                "neighbor_market_price_uah_mwh": 2500.0 + index,
                "training_use_allowed": False,
                "feature_use_allowed": True,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
            for index, timestamp in enumerate(sorted(timestamps))
        ]
    )


def _rich_lagged_poland_feature_frame(library: pl.DataFrame) -> pl.DataFrame:
    anchors = [
        row["anchor_timestamp"]
        for row in library.select("anchor_timestamp").unique().iter_rows(named=True)
    ]
    timestamps = {
        anchor + timedelta(hours=offset)
        for anchor in anchors
        for offset in range(4)
    }
    rows: list[dict[str, object]] = []
    for timestamp in sorted(timestamps):
        high_regime = timestamp.date().toordinal() % 2 == 0
        price = 4200.0 if high_regime else 1800.0
        delta = 600.0 if high_regime else -600.0
        daily_spread = 1800.0 if high_regime else 300.0
        rows.append(
            {
                "delivery_timestamp_utc": timestamp,
                "feature_column": "entsoe_pl_lag24_day_ahead_price_uah_mwh",
                "neighbor_market_price_uah_mwh": price,
                "entsoe_pl_lag24_day_ahead_price_uah_mwh": price,
                "entsoe_pl_lag24_delta_1h_uah_mwh": delta,
                "entsoe_pl_lag24_delta_24h_uah_mwh": delta,
                "entsoe_pl_lag24_daily_spread_uah_mwh": daily_spread,
                "entsoe_pl_lag24_daily_price_rank": 0.8 if high_regime else 0.2,
                "entsoe_pl_lag24_daily_peak_hour_utc": 20 if high_regime else 8,
                "entsoe_pl_lag24_daily_trough_hour_utc": 3,
                "training_use_allowed": False,
                "feature_use_allowed": True,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(rows)


def _strict_frame(
    *,
    baseline_selected_regrets: list[float],
    selected_regrets: list[float] | None = None,
) -> pl.DataFrame:
    selected_values = selected_regrets or baseline_selected_regrets
    rows: list[dict[str, object]] = []
    tenants = ["client_001_kyiv_mall", "client_002_lviv_office"]
    first_anchor = datetime(2026, 4, 1, 23)
    role_regrets = {
        "strict_reference": [200.0] * len(selected_values),
        "raw_reference": [240.0] * len(selected_values),
        "schedule_value_learner_v2_reference": baseline_selected_regrets,
        "schedule_value_learner_v2_plus": selected_values,
    }
    for tenant_index, tenant_id in enumerate(tenants):
        for anchor_index, anchor_offset in enumerate(range(2)):
            anchor = first_anchor + timedelta(days=anchor_offset)
            value_index = tenant_index * 2 + anchor_index
            for role, regrets in role_regrets.items():
                regret = regrets[value_index]
                forecast_model_name = (
                    schedule_value_learner_v2_plus_model_name(SOURCE)
                    if role == "schedule_value_learner_v2_plus"
                    else f"{role}_{SOURCE}"
                )
                payload = {
                    "data_quality_tier": "thesis_grade",
                    "observed_coverage_ratio": 1.0,
                    "safety_violation_count": 0,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                    "selection_role": role,
                }
                rows.append(
                    {
                        "evaluation_id": f"{tenant_id}:{anchor_index}:{role}",
                        "tenant_id": tenant_id,
                        "source_model_name": SOURCE,
                        "forecast_model_name": forecast_model_name,
                        "strategy_kind": (
                            DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND
                        ),
                        "market_venue": "DAM",
                        "anchor_timestamp": anchor,
                        "generated_at": datetime(2026, 5, 15),
                        "horizon_hours": 24,
                        "starting_soc_fraction": 0.5,
                        "starting_soc_source": "test_fixture",
                        "decision_value_uah": 1000.0 - regret,
                        "forecast_objective_value_uah": 1000.0 - regret,
                        "oracle_value_uah": 1000.0,
                        "regret_uah": regret,
                        "regret_ratio": regret / 1000.0,
                        "total_degradation_penalty_uah": 0.0,
                        "total_throughput_mwh": 0.0,
                        "committed_action": "HOLD",
                        "committed_power_mw": 0.0,
                        "rank_by_regret": 1,
                        "data_quality_tier": "thesis_grade",
                        "observed_coverage_ratio": 1.0,
                        "safety_violation_count": 0,
                        "selection_role": role,
                        "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_CLAIM_SCOPE,
                        "not_full_dfl": True,
                        "not_market_execution": True,
                        "evaluation_payload": payload,
                    }
                )
    return pl.DataFrame(rows)


def _robustness_frame(*, passed_windows: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for window_index in range(1, 5):
        passed = window_index <= passed_windows
        rows.append(
            {
                "source_model_name": SOURCE,
                "window_index": window_index,
                "tenant_count": 2,
                "validation_anchor_count_per_tenant": 2,
                "validation_tenant_anchor_count": 4,
                "minimum_prior_anchor_count_before_window": 30,
                "strict_mean_regret_uah": 200.0,
                "raw_mean_regret_uah": 240.0,
                "v2_mean_regret_uah": 100.0,
                "selected_mean_regret_uah": 80.0 if passed else 210.0,
                "strict_median_regret_uah": 200.0,
                "v2_median_regret_uah": 100.0,
                "selected_median_regret_uah": 80.0 if passed else 210.0,
                "mean_regret_improvement_ratio_vs_raw": (
                    (240.0 - (80.0 if passed else 210.0)) / 240.0
                ),
                "mean_regret_improvement_ratio_vs_strict": (
                    (200.0 - (80.0 if passed else 210.0)) / 200.0
                ),
                "mean_regret_improvement_ratio_vs_v2": (
                    (100.0 - (80.0 if passed else 210.0)) / 100.0
                ),
                "development_passed": passed,
                "source_specific_strict_passed": passed,
                "v2_non_degradation_passed": passed,
                "v2_plus_window_passed": passed,
                "passing_window_count_for_source": passed_windows,
                "robust_research_challenger": passed_windows >= 3,
                "production_promote": False,
                "gate_label": "v2_plus_window_pass" if passed else "blocked",
                "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(rows)
