from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.action_targeting import action_target_model_name
from smart_arbitrage.dfl.decision_targeting import decision_target_model_name, panel_v2_model_name
from smart_arbitrage.dfl.trajectory_value import (
    TRAJECTORY_VALUE_SELECTOR_STRICT_LP_STRATEGY_KIND,
    build_dfl_trajectory_value_candidate_panel_frame,
    build_dfl_trajectory_value_selector_frame,
    build_dfl_trajectory_value_selector_strict_lp_benchmark_frame,
    evaluate_dfl_trajectory_value_selector_gate,
    trajectory_value_selector_model_name,
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
GENERATED_AT = datetime(2026, 5, 8, 10)


def test_candidate_panel_preserves_strict_final_holdout_schedules_and_prior_scores() -> None:
    benchmark = _benchmark_frame(anchor_count_per_tenant=20)
    panel = _panel_frame(anchor_count_per_tenant=20, final_validation_anchor_count=18)
    decision_panel = _decision_panel_frame(anchor_count_per_tenant=20, final_validation_anchor_count=18)
    action_panel = _action_panel_frame(anchor_count_per_tenant=20, final_validation_anchor_count=18)
    action_strict = _strict_frame(anchor_count_per_tenant=18)

    result = build_dfl_trajectory_value_candidate_panel_frame(
        benchmark,
        _strict_frame(anchor_count_per_tenant=18, include_families=("strict_control", "raw_source", "panel_v2")),
        _strict_frame(
            anchor_count_per_tenant=18,
            include_families=("strict_control", "raw_source", "panel_v2", "decision_target_v3"),
        ),
        action_strict,
        panel,
        decision_panel,
        action_panel,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=18,
        max_train_anchors_per_tenant=2,
    )

    assert result.height == 5 * 2 * 18 * 5
    assert result.select("tenant_id").n_unique() == 5
    assert result.select("source_model_name").n_unique() == 2
    assert set(result["candidate_family"].unique().to_list()) == {
        "strict_control",
        "raw_source",
        "panel_v2",
        "decision_target_v3",
        "action_target_v4",
    }
    assert result.select("split_name").to_series().unique().to_list() == ["final_holdout"]

    v4_rows = result.filter(
        (pl.col("source_model_name") == "tft_silver_v0")
        & (pl.col("candidate_family") == "action_target_v4")
    )
    assert v4_rows.height == 90
    assert v4_rows.select("prior_selection_mean_regret_uah").to_series().unique().to_list() == [60.0]

    sample = v4_rows.row(0, named=True)
    assert sample["dispatch_mw_vector"] == [0.0, 1.0]
    assert sample["forecast_price_uah_mwh_vector"] == [1400.0, 5400.0]
    assert sample["actual_price_uah_mwh_vector"] == [1000.0, 5000.0]
    assert sample["not_full_dfl"] is True
    assert sample["not_market_execution"] is True


def test_selector_uses_prior_scores_only_and_final_scoring_remains_diagnostic() -> None:
    candidate_panel = _candidate_panel_from_final_regrets(
        raw_regret=500.0,
        v2_regret=200.0,
        v3_regret=100.0,
        v4_regret=800.0,
        v4_prior_regret=60.0,
    )
    mutated_final_panel = _candidate_panel_from_final_regrets(
        raw_regret=500.0,
        v2_regret=200.0,
        v3_regret=100.0,
        v4_regret=5.0,
        v4_prior_regret=60.0,
    )

    selected = build_dfl_trajectory_value_selector_frame(
        candidate_panel,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )
    mutated = build_dfl_trajectory_value_selector_frame(
        mutated_final_panel,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )

    assert selected.select("selected_candidate_family").to_series().unique().to_list() == [
        "action_target_v4"
    ]
    assert mutated.select("selected_candidate_family").to_series().unique().to_list() == [
        "action_target_v4"
    ]
    assert selected["selected_final_mean_regret_uah"].to_list() == [800.0] * 10
    assert mutated["selected_final_mean_regret_uah"].to_list() == [5.0] * 10


def test_selector_strict_benchmark_emits_selected_rows_with_selector_model_name() -> None:
    candidate_panel = _candidate_panel_from_final_regrets(
        raw_regret=500.0,
        v2_regret=200.0,
        v3_regret=100.0,
        v4_regret=800.0,
        v4_prior_regret=60.0,
    )
    selector = build_dfl_trajectory_value_selector_frame(
        candidate_panel,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )

    result = build_dfl_trajectory_value_selector_strict_lp_benchmark_frame(
        candidate_panel,
        selector,
        generated_at=GENERATED_AT,
    )

    assert result.height == 5 * 2 * 18 * 3
    assert result.select("strategy_kind").to_series().unique().to_list() == [
        TRAJECTORY_VALUE_SELECTOR_STRICT_LP_STRATEGY_KIND
    ]
    assert {
        "strict_similar_day",
        "tft_silver_v0",
        "nbeatsx_silver_v0",
        trajectory_value_selector_model_name("tft_silver_v0"),
        trajectory_value_selector_model_name("nbeatsx_silver_v0"),
    }.issubset(set(result["forecast_model_name"].unique().to_list()))
    selector_rows = result.filter(pl.col("forecast_model_name").str.starts_with("dfl_trajectory_value_selector_v1_"))
    assert selector_rows.height == 180
    payload = selector_rows.row(0, named=True)["evaluation_payload"]
    assert payload["trajectory_value_selected_candidate_family"] == "action_target_v4"
    assert payload["not_full_dfl"] is True
    assert payload["not_market_execution"] is True


def test_selector_strict_benchmark_keeps_control_reference_when_control_is_selected() -> None:
    candidate_panel = _candidate_panel_from_final_regrets(
        strict_regret=100.0,
        raw_regret=500.0,
        v2_regret=450.0,
        v3_regret=400.0,
        v4_regret=300.0,
        v4_prior_regret=600.0,
        strict_prior_regret=10.0,
    )
    selector = build_dfl_trajectory_value_selector_frame(
        candidate_panel,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )

    result = build_dfl_trajectory_value_selector_strict_lp_benchmark_frame(candidate_panel, selector)

    assert selector.select("selected_candidate_family").to_series().unique().to_list() == ["strict_control"]
    assert result.filter(pl.col("forecast_model_name") == "strict_similar_day").height == 180
    assert result.filter(pl.col("forecast_model_name").str.starts_with("dfl_trajectory_value_selector_v1_")).height == 180


def test_selector_gate_allows_development_diagnostic_but_blocks_production_when_strict_wins() -> None:
    candidate_panel = _candidate_panel_from_final_regrets(
        strict_regret=100.0,
        raw_regret=500.0,
        v2_regret=450.0,
        v3_regret=400.0,
        v4_regret=300.0,
        v4_prior_regret=60.0,
    )
    selector = build_dfl_trajectory_value_selector_frame(
        candidate_panel,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )
    strict_frame = build_dfl_trajectory_value_selector_strict_lp_benchmark_frame(candidate_panel, selector)

    result = evaluate_dfl_trajectory_value_selector_gate(strict_frame, source_model_names=SOURCE_MODELS)

    assert result.passed is False
    assert result.decision == "diagnostic_pass_production_blocked"
    assert result.metrics["development_gate_passed"] is True
    assert "strict_similar_day" in result.description


def test_selector_gate_promotes_only_when_selector_beats_strict_control() -> None:
    candidate_panel = _candidate_panel_from_final_regrets(
        strict_regret=100.0,
        raw_regret=500.0,
        v2_regret=450.0,
        v3_regret=400.0,
        v4_regret=90.0,
        v4_prior_regret=60.0,
    )
    selector = build_dfl_trajectory_value_selector_frame(
        candidate_panel,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )
    strict_frame = build_dfl_trajectory_value_selector_strict_lp_benchmark_frame(candidate_panel, selector)

    result = evaluate_dfl_trajectory_value_selector_gate(strict_frame, source_model_names=SOURCE_MODELS)

    assert result.passed is True
    assert result.decision == "promote"
    assert result.metrics["mean_regret_improvement_ratio_vs_strict"] == 0.1


def test_selector_gate_blocks_under_coverage_and_non_thesis_rows() -> None:
    under_coverage_panel = _candidate_panel_from_final_regrets(
        raw_regret=500.0,
        v2_regret=450.0,
        v3_regret=400.0,
        v4_regret=90.0,
        v4_prior_regret=60.0,
        anchor_count_per_tenant=17,
    )
    under_selector = build_dfl_trajectory_value_selector_frame(
        under_coverage_panel,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        min_final_holdout_tenant_anchor_count_per_source_model=85,
    )
    under_strict = build_dfl_trajectory_value_selector_strict_lp_benchmark_frame(
        under_coverage_panel,
        under_selector,
    )
    under_result = evaluate_dfl_trajectory_value_selector_gate(under_strict, source_model_names=SOURCE_MODELS)

    thesis_panel = _candidate_panel_from_final_regrets(
        raw_regret=500.0,
        v2_regret=450.0,
        v3_regret=400.0,
        v4_regret=90.0,
        v4_prior_regret=60.0,
    )
    non_thesis_selector = build_dfl_trajectory_value_selector_frame(
        thesis_panel,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )
    non_thesis_strict = build_dfl_trajectory_value_selector_strict_lp_benchmark_frame(
        thesis_panel,
        non_thesis_selector,
    ).with_columns(
        pl.Series(
            "evaluation_payload",
            [
                {**payload, "data_quality_tier": "demo_grade"}
                for payload in build_dfl_trajectory_value_selector_strict_lp_benchmark_frame(
                    thesis_panel,
                    non_thesis_selector,
                )
                .select("evaluation_payload")
                .to_series()
                .to_list()
            ],
        )
    )
    non_thesis_result = evaluate_dfl_trajectory_value_selector_gate(
        non_thesis_strict,
        source_model_names=SOURCE_MODELS,
    )

    assert under_result.passed is False
    assert "validation tenant-anchor count" in under_result.description
    assert non_thesis_result.passed is False
    assert "thesis_grade" in non_thesis_result.description


def test_candidate_panel_rejects_bad_vectors_and_non_thesis_inputs() -> None:
    benchmark = _benchmark_frame(anchor_count_per_tenant=20)
    panel = _panel_frame(anchor_count_per_tenant=20, final_validation_anchor_count=18)
    decision_panel = _decision_panel_frame(anchor_count_per_tenant=20, final_validation_anchor_count=18)
    action_panel = _action_panel_frame(anchor_count_per_tenant=20, final_validation_anchor_count=18)
    strict_frame = _strict_frame(anchor_count_per_tenant=18)

    bad_vectors = strict_frame.with_columns(
        pl.Series(
            "evaluation_payload",
            [
                {**payload, "horizon": payload["horizon"][:1]}
                for payload in strict_frame.select("evaluation_payload").to_series().to_list()
            ],
        )
    )
    non_thesis = strict_frame.with_columns(
        pl.Series(
            "evaluation_payload",
            [
                {**payload, "data_quality_tier": "demo_grade"}
                for payload in strict_frame.select("evaluation_payload").to_series().to_list()
            ],
        )
    )

    for invalid_frame, message in [(bad_vectors, "vector length"), (non_thesis, "thesis_grade")]:
        try:
            build_dfl_trajectory_value_candidate_panel_frame(
                benchmark,
                invalid_frame,
                invalid_frame,
                invalid_frame,
                panel,
                decision_panel,
                action_panel,
                tenant_ids=CANONICAL_TENANTS,
                forecast_model_names=SOURCE_MODELS,
                final_validation_anchor_count_per_tenant=18,
                max_train_anchors_per_tenant=2,
            )
        except ValueError as exc:
            assert message in str(exc)
        else:
            raise AssertionError("expected invalid trajectory/value evidence to fail")


def _candidate_panel_from_final_regrets(
    *,
    raw_regret: float,
    v2_regret: float,
    v3_regret: float,
    v4_regret: float,
    v4_prior_regret: float,
    strict_regret: float = 100.0,
    strict_prior_regret: float | None = None,
    anchor_count_per_tenant: int = 18,
    data_quality_tier: str = "thesis_grade",
) -> pl.DataFrame:
    benchmark = _benchmark_frame(
        anchor_count_per_tenant=anchor_count_per_tenant + 2,
        strict_regret=strict_regret,
        raw_regret=raw_regret,
        strict_prior_regret=strict_prior_regret,
    )
    panel = _panel_frame(
        anchor_count_per_tenant=anchor_count_per_tenant + 2,
        final_validation_anchor_count=anchor_count_per_tenant,
        v2_prior_regret=90.0,
    )
    decision_panel = _decision_panel_frame(
        anchor_count_per_tenant=anchor_count_per_tenant + 2,
        final_validation_anchor_count=anchor_count_per_tenant,
        v3_prior_regret=80.0,
    )
    action_panel = _action_panel_frame(
        anchor_count_per_tenant=anchor_count_per_tenant + 2,
        final_validation_anchor_count=anchor_count_per_tenant,
        v4_prior_regret=v4_prior_regret,
    )
    action_strict = _strict_frame(
        anchor_count_per_tenant=anchor_count_per_tenant,
        strict_regret=strict_regret,
        raw_regret=raw_regret,
        v2_regret=v2_regret,
        v3_regret=v3_regret,
        v4_regret=v4_regret,
        data_quality_tier=data_quality_tier,
    )
    return build_dfl_trajectory_value_candidate_panel_frame(
        benchmark,
        _strict_frame(
            anchor_count_per_tenant=anchor_count_per_tenant,
            strict_regret=strict_regret,
            raw_regret=raw_regret,
            v2_regret=v2_regret,
            data_quality_tier=data_quality_tier,
            include_families=("strict_control", "raw_source", "panel_v2"),
        ),
        _strict_frame(
            anchor_count_per_tenant=anchor_count_per_tenant,
            strict_regret=strict_regret,
            raw_regret=raw_regret,
            v2_regret=v2_regret,
            v3_regret=v3_regret,
            data_quality_tier=data_quality_tier,
            include_families=("strict_control", "raw_source", "panel_v2", "decision_target_v3"),
        ),
        action_strict,
        panel,
        decision_panel,
        action_panel,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        final_validation_anchor_count_per_tenant=anchor_count_per_tenant,
        max_train_anchors_per_tenant=2,
    )


def _benchmark_frame(
    *,
    anchor_count_per_tenant: int,
    strict_regret: float = 100.0,
    raw_regret: float = 500.0,
    strict_prior_regret: float | None = None,
) -> pl.DataFrame:
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
                        regret=float(strict_prior_regret if strict_prior_regret is not None else strict_regret),
                        forecast_prices=(1000.0, 5000.0),
                    )
                )
                rows.append(
                    _evaluation_row(
                        tenant_id=tenant_id,
                        source_model_name=source_model_name,
                        model_name=source_model_name,
                        anchor=anchor,
                        regret=raw_regret,
                        forecast_prices=(5000.0, 1000.0),
                    )
                )
    return pl.DataFrame(rows)


def _strict_frame(
    *,
    anchor_count_per_tenant: int,
    strict_regret: float = 100.0,
    raw_regret: float = 500.0,
    v2_regret: float = 400.0,
    v3_regret: float = 300.0,
    v4_regret: float = 200.0,
    data_quality_tier: str = "thesis_grade",
    include_families: tuple[str, ...] = (
        "strict_control",
        "raw_source",
        "panel_v2",
        "decision_target_v3",
        "action_target_v4",
    ),
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in CANONICAL_TENANTS:
        for source_model_name in SOURCE_MODELS:
            for anchor_index in range(anchor_count_per_tenant):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index + 2)
                family_specs = {
                    "strict_control": ("strict_similar_day", strict_regret, (1000.0, 5000.0)),
                    "raw_source": (source_model_name, raw_regret, (5000.0, 1000.0)),
                    "panel_v2": (panel_v2_model_name(source_model_name), v2_regret, (1200.0, 5200.0)),
                    "decision_target_v3": (
                        decision_target_model_name(source_model_name),
                        v3_regret,
                        (1300.0, 5300.0),
                    ),
                    "action_target_v4": (
                        action_target_model_name(source_model_name),
                        v4_regret,
                        (1400.0, 5400.0),
                    ),
                }
                for family in include_families:
                    model_name, regret, forecast_prices = family_specs[family]
                    rows.append(
                        _evaluation_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            model_name=model_name,
                            anchor=anchor,
                            regret=regret,
                            forecast_prices=forecast_prices,
                            data_quality_tier=data_quality_tier,
                        )
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
    data_quality_tier: str = "thesis_grade",
) -> dict[str, object]:
    return {
        "evaluation_id": f"{tenant_id}:{source_model_name}:{model_name}:{anchor.isoformat()}",
        "tenant_id": tenant_id,
        "forecast_model_name": model_name,
        "strategy_kind": "strict_lp_test",
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
        "committed_action": "DISCHARGE" if forecast_prices[1] > forecast_prices[0] else "CHARGE",
        "committed_power_mw": 1.0,
        "rank_by_regret": 1,
        "evaluation_payload": {
            "data_quality_tier": data_quality_tier,
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


def _panel_frame(
    *,
    anchor_count_per_tenant: int,
    final_validation_anchor_count: int,
    v2_prior_regret: float = 70.0,
) -> pl.DataFrame:
    first_holdout_anchor = FIRST_ANCHOR + timedelta(days=anchor_count_per_tenant - final_validation_anchor_count)
    last_holdout_anchor = FIRST_ANCHOR + timedelta(days=anchor_count_per_tenant - 1)
    return pl.DataFrame(
        [
            {
                "tenant_id": tenant_id,
                "forecast_model_name": source_model_name,
                "final_validation_anchor_count": final_validation_anchor_count,
                "horizon_hours": 2,
                "first_final_holdout_anchor_timestamp": first_holdout_anchor,
                "last_final_holdout_anchor_timestamp": last_holdout_anchor,
                "v2_checkpoint_horizon_biases_uah_mwh": [100.0, -100.0],
                "v2_checkpoint_epoch": 1,
                "v2_inner_selection_relaxed_regret_uah": v2_prior_regret,
                "data_quality_tier": "thesis_grade",
                "observed_coverage_ratio": 1.0,
                "claim_scope": "offline_dfl_panel_experiment_not_full_dfl",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
            for tenant_id in CANONICAL_TENANTS
            for source_model_name in SOURCE_MODELS
        ]
    )


def _decision_panel_frame(
    *,
    anchor_count_per_tenant: int,
    final_validation_anchor_count: int,
    v3_prior_regret: float = 80.0,
) -> pl.DataFrame:
    first_holdout_anchor = FIRST_ANCHOR + timedelta(days=anchor_count_per_tenant - final_validation_anchor_count)
    last_holdout_anchor = FIRST_ANCHOR + timedelta(days=anchor_count_per_tenant - 1)
    return pl.DataFrame(
        [
            {
                "tenant_id": tenant_id,
                "forecast_model_name": source_model_name,
                "final_validation_anchor_count": final_validation_anchor_count,
                "horizon_hours": 2,
                "first_final_holdout_anchor_timestamp": first_holdout_anchor,
                "last_final_holdout_anchor_timestamp": last_holdout_anchor,
                "inner_selection_mean_regret_uah": v3_prior_regret,
                "panel_v2_horizon_biases_uah_mwh": [100.0, -100.0],
                "data_quality_tier": "thesis_grade",
                "observed_coverage_ratio": 1.0,
                "claim_scope": "offline_dfl_decision_target_v3_not_full_dfl",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
            for tenant_id in CANONICAL_TENANTS
            for source_model_name in SOURCE_MODELS
        ]
    )


def _action_panel_frame(
    *,
    anchor_count_per_tenant: int,
    final_validation_anchor_count: int,
    v4_prior_regret: float = 60.0,
) -> pl.DataFrame:
    first_holdout_anchor = FIRST_ANCHOR + timedelta(days=anchor_count_per_tenant - final_validation_anchor_count)
    last_holdout_anchor = FIRST_ANCHOR + timedelta(days=anchor_count_per_tenant - 1)
    return pl.DataFrame(
        [
            {
                "tenant_id": tenant_id,
                "forecast_model_name": source_model_name,
                "final_validation_anchor_count": final_validation_anchor_count,
                "horizon_hours": 2,
                "first_final_holdout_anchor_timestamp": first_holdout_anchor,
                "last_final_holdout_anchor_timestamp": last_holdout_anchor,
                "inner_selection_mean_regret_uah": v4_prior_regret,
                "data_quality_tier": "thesis_grade",
                "observed_coverage_ratio": 1.0,
                "claim_scope": "offline_dfl_action_target_v4_not_full_dfl",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
            for tenant_id in CANONICAL_TENANTS
            for source_model_name in SOURCE_MODELS
        ]
    )
