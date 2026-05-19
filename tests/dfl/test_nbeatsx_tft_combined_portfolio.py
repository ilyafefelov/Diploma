from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.nbeatsx_tft_combined_portfolio import (
    DFL_NBEATSX_TFT_META_SELECTOR_STRICT_LP_STRATEGY_KIND,
    DFL_NBEATSX_TFT_META_SELECTOR_ROLLING_STRICT_LP_STRATEGY_KIND,
    build_dfl_nbeatsx_tft_candidate_portfolio_v1_frame,
    build_dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame,
    build_dfl_nbeatsx_tft_complementarity_audit_frame,
    build_dfl_nbeatsx_tft_meta_selector_robustness_frame,
    build_dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame,
    build_dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame,
    evaluate_dfl_nbeatsx_tft_meta_selector_gate,
)

TENANTS: tuple[str, ...] = ("client_001_kyiv_mall", "client_002_lviv_office")
BASELINE_SOURCE = "nbeatsx_official_global_panel_horizon_calibrated_v1"
TFT_SOURCE = "tft_official_global_panel_v1_horizon_quantile_calibrated_v1"
COMBINED_SOURCE = "nbeatsx_tft_candidate_portfolio_meta_selector_v1"
GENERATED_AT = datetime(2026, 5, 19, 9)
FIRST_ANCHOR = datetime(2026, 1, 1, 23)


def test_complementarity_audit_separates_labels_from_prior_features() -> None:
    baseline = _v2_plus_strict_frame(final_regret=200.0)
    tft = _tft_candidate_library(train_regret=100.0, final_regret=120.0)

    audit = build_dfl_nbeatsx_tft_complementarity_audit_frame(
        baseline,
        tft,
        baseline_source_model_name=BASELINE_SOURCE,
        tft_source_model_names=(TFT_SOURCE,),
        final_validation_anchor_count_per_tenant=1,
    )
    mutated = build_dfl_nbeatsx_tft_complementarity_audit_frame(
        baseline,
        _mutate_final_labels(tft, final_regret=999.0),
        baseline_source_model_name=BASELINE_SOURCE,
        tft_source_model_names=(TFT_SOURCE,),
        final_validation_anchor_count_per_tenant=1,
    )

    assert audit.height == len(TENANTS)
    assert set(audit["complementarity_class"].to_list()) == {
        "candidate_available_but_not_selected"
    }
    feature_columns = sorted(
        column for column in audit.columns if column.startswith("selector_feature_")
    )
    assert feature_columns
    assert audit.select(feature_columns).rows() == mutated.select(feature_columns).rows()
    assert audit["best_tft_regret_uah"].to_list() != mutated[
        "best_tft_regret_uah"
    ].to_list()


def test_candidate_portfolio_includes_fallbacks_and_cross_model_candidates() -> None:
    baseline = _v2_plus_strict_frame(final_regret=200.0)
    nbeatsx_library = _nbeatsx_v2_plus_library(train_regret=200.0, final_regret=200.0)
    tft = _tft_candidate_library(train_regret=100.0, final_regret=120.0)
    audit = build_dfl_nbeatsx_tft_complementarity_audit_frame(
        baseline,
        tft,
        baseline_source_model_name=BASELINE_SOURCE,
        tft_source_model_names=(TFT_SOURCE,),
        final_validation_anchor_count_per_tenant=1,
    )

    portfolio = build_dfl_nbeatsx_tft_candidate_portfolio_v1_frame(
        nbeatsx_library,
        tft,
        baseline,
        audit,
        baseline_source_model_name=BASELINE_SOURCE,
        tft_source_model_names=(TFT_SOURCE,),
        final_validation_anchor_count_per_tenant=1,
    )

    assert {
        "nbeatsx_v2_plus",
        "strict_fallback",
        "tft_quantile",
        "cross_model",
    }.issubset(set(portfolio["portfolio_source"].unique().to_list()))
    assert portfolio.filter(
        (pl.col("candidate_family") == "frozen_v2_plus_fallback")
        & (pl.col("split_name") == "final_holdout")
    ).height == len(TENANTS)
    assert portfolio.filter(
        (pl.col("candidate_family") == "strict_control")
        & (pl.col("split_name") == "final_holdout")
    ).height == len(TENANTS)
    assert set(portfolio["market_execution_enabled"].unique().to_list()) == {False}


def test_candidate_portfolio_bounds_tft_candidates_by_prior_objective_not_label() -> None:
    baseline = _v2_plus_strict_frame(final_regret=200.0)
    nbeatsx_library = _nbeatsx_v2_plus_library(train_regret=200.0, final_regret=200.0)
    tft = _wide_tft_candidate_library()
    audit = build_dfl_nbeatsx_tft_complementarity_audit_frame(
        baseline,
        tft,
        baseline_source_model_name=BASELINE_SOURCE,
        tft_source_model_names=(TFT_SOURCE,),
        final_validation_anchor_count_per_tenant=1,
    )

    portfolio = build_dfl_nbeatsx_tft_candidate_portfolio_v1_frame(
        nbeatsx_library,
        tft,
        baseline,
        audit,
        baseline_source_model_name=BASELINE_SOURCE,
        tft_source_model_names=(TFT_SOURCE,),
        final_validation_anchor_count_per_tenant=1,
        max_tft_candidates_per_anchor_source_family=2,
    )
    mutated_portfolio = build_dfl_nbeatsx_tft_candidate_portfolio_v1_frame(
        nbeatsx_library,
        _mutate_final_labels(tft, final_regret=1.0),
        baseline,
        audit,
        baseline_source_model_name=BASELINE_SOURCE,
        tft_source_model_names=(TFT_SOURCE,),
        final_validation_anchor_count_per_tenant=1,
        max_tft_candidates_per_anchor_source_family=2,
    )

    chosen = _final_tft_quantile_candidate_names(portfolio)
    mutated_chosen = _final_tft_quantile_candidate_names(mutated_portfolio)
    assert chosen == mutated_chosen
    assert set(chosen.values()) == {
        (
            f"{TFT_SOURCE}:forecast_objective_rank_3",
            f"{TFT_SOURCE}:forecast_objective_rank_4",
        )
    }


def test_meta_selector_can_pass_only_when_candidate_beats_v2_plus() -> None:
    baseline = _v2_plus_strict_frame(final_regret=200.0)
    portfolio, audit = _portfolio(train_regret=100.0, final_regret=120.0)

    selector = build_dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame(
        portfolio,
        audit,
        tenant_ids=TENANTS,
        baseline_source_model_name=BASELINE_SOURCE,
        combined_source_model_name=COMBINED_SOURCE,
        min_prior_mean_improvement_ratio_vs_v2_plus=0.01,
    )
    strict_frame = build_dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame(
        portfolio,
        selector,
        baseline,
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_nbeatsx_tft_meta_selector_gate(
        strict_frame,
        baseline_source_model_name=BASELINE_SOURCE,
        combined_source_model_name=COMBINED_SOURCE,
        min_validation_tenant_anchor_count=2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    selected = strict_frame.filter(
        pl.col("selection_role") == "nbeatsx_tft_meta_selector_v1"
    )
    assert set(selector["fallback_to_v2_plus"].to_list()) == {False}
    assert set(selected["fallback_to_v2_plus"].to_list()) == {False}
    assert set(strict_frame["strategy_kind"].unique().to_list()) == {
        DFL_NBEATSX_TFT_META_SELECTOR_STRICT_LP_STRATEGY_KIND
    }
    assert gate.passed is True
    assert gate.metrics["selected_mean_regret_uah"] == 120.0
    assert gate.metrics["v2_plus_mean_regret_uah"] == 200.0
    assert gate.metrics["market_execution_enabled"] is False
    assert selected["rank_by_regret"].null_count() == 0


def test_meta_selector_benchmark_uses_only_configured_baseline_source() -> None:
    baseline = _v2_plus_strict_frame(final_regret=200.0)
    raw_source_baseline = baseline.with_columns(
        [
            pl.lit("nbeatsx_official_global_panel_v1").alias("source_model_name"),
            pl.when(pl.col("forecast_model_name") != "strict_similar_day")
            .then(
                pl.lit(
                    "dfl_schedule_value_learner_v2_plus_nbeatsx_official_global_panel_v1"
                )
            )
            .otherwise(pl.col("forecast_model_name"))
            .alias("forecast_model_name"),
        ]
    )
    portfolio, audit = _portfolio(train_regret=220.0, final_regret=80.0)
    selector = build_dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame(
        portfolio,
        audit,
        tenant_ids=TENANTS,
        baseline_source_model_name=BASELINE_SOURCE,
        combined_source_model_name=COMBINED_SOURCE,
        min_prior_mean_improvement_ratio_vs_v2_plus=0.01,
    )

    strict_frame = build_dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame(
        portfolio,
        selector,
        pl.concat([baseline, raw_source_baseline], how="diagonal_relaxed"),
        baseline_source_model_name=BASELINE_SOURCE,
        generated_at=GENERATED_AT,
    )

    selected = strict_frame.filter(
        pl.col("selection_role") == "nbeatsx_tft_meta_selector_v1"
    )
    assert selected.height == len(TENANTS)
    assert selected.select("evaluation_id").n_unique() == selected.height
    assert set(selected["selected_source_model_name"].to_list()) == {BASELINE_SOURCE}
    assert selected["regret_uah"].to_list() == [200.0, 200.0]


def test_meta_selector_falls_back_to_v2_plus_when_prior_signal_is_weak() -> None:
    baseline = _v2_plus_strict_frame(final_regret=200.0)
    portfolio, audit = _portfolio(train_regret=220.0, final_regret=80.0)

    selector = build_dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame(
        portfolio,
        audit,
        tenant_ids=TENANTS,
        baseline_source_model_name=BASELINE_SOURCE,
        combined_source_model_name=COMBINED_SOURCE,
        min_prior_mean_improvement_ratio_vs_v2_plus=0.01,
    )
    strict_frame = build_dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame(
        portfolio,
        selector,
        baseline,
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_nbeatsx_tft_meta_selector_gate(
        strict_frame,
        baseline_source_model_name=BASELINE_SOURCE,
        combined_source_model_name=COMBINED_SOURCE,
        min_validation_tenant_anchor_count=2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    selected = strict_frame.filter(
        pl.col("selection_role") == "nbeatsx_tft_meta_selector_v1"
    )
    assert set(selector["fallback_to_v2_plus"].to_list()) == {True}
    assert set(selected["fallback_to_v2_plus"].to_list()) == {True}
    assert selected["regret_uah"].to_list() == [200.0, 200.0]
    assert gate.passed is False
    assert gate.metrics["selected_mean_regret_uah"] == 200.0


def test_meta_selector_robustness_requires_each_window_to_beat_v2_plus() -> None:
    baseline = _v2_plus_strict_frame(final_regret=200.0, anchor_count=2)
    portfolio, audit = _portfolio(
        train_regret=100.0,
        final_regret=120.0,
        anchor_count=2,
    )
    selector = build_dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame(
        portfolio,
        audit,
        tenant_ids=TENANTS,
        baseline_source_model_name=BASELINE_SOURCE,
        combined_source_model_name=COMBINED_SOURCE,
        min_prior_mean_improvement_ratio_vs_v2_plus=0.01,
    )
    strict_frame = build_dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame(
        portfolio,
        selector,
        baseline,
        generated_at=GENERATED_AT,
    )

    robustness = build_dfl_nbeatsx_tft_meta_selector_robustness_frame(
        strict_frame,
        baseline_source_model_name=BASELINE_SOURCE,
        combined_source_model_name=COMBINED_SOURCE,
        validation_window_count=2,
        validation_anchor_count=1,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    assert robustness.height == 2
    assert set(robustness["rolling_pass"].to_list()) == {True}
    assert set(robustness["market_execution_enabled"].to_list()) == {False}


def test_rolling_strict_frame_replays_prior_windows_with_v2_plus_baseline() -> None:
    nbeatsx_library = _rolling_nbeatsx_v2_plus_library(anchor_count=5)
    tft_library = _rolling_tft_candidate_library(anchor_count=5, validation_regret=120.0)

    strict_frame = build_dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame(
        nbeatsx_library,
        tft_library,
        tenant_ids=TENANTS,
        baseline_source_model_name=BASELINE_SOURCE,
        tft_source_model_names=(TFT_SOURCE,),
        combined_source_model_name=COMBINED_SOURCE,
        validation_window_count=2,
        validation_anchor_count=1,
        min_prior_anchors_before_window=1,
        min_prior_mean_improvement_ratio_vs_v2_plus=0.01,
        generated_at=GENERATED_AT,
    )
    robustness = build_dfl_nbeatsx_tft_meta_selector_robustness_frame(
        strict_frame,
        baseline_source_model_name=BASELINE_SOURCE,
        combined_source_model_name=COMBINED_SOURCE,
        validation_window_count=2,
        validation_anchor_count=1,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    selected = strict_frame.filter(pl.col("selection_role") == "nbeatsx_tft_meta_selector_v1")
    baseline = strict_frame.filter(pl.col("selection_role") == "schedule_value_learner_v2_plus")
    assert set(strict_frame["strategy_kind"].unique().to_list()) == {
        DFL_NBEATSX_TFT_META_SELECTOR_ROLLING_STRICT_LP_STRATEGY_KIND
    }
    assert selected.height == len(TENANTS) * 2
    assert baseline.height == len(TENANTS) * 2
    assert set(selected["fallback_to_v2_plus"].to_list()) == {False}
    assert set(selected["selected_source_model_name"].to_list()) == {TFT_SOURCE}
    assert set(robustness["rolling_pass"].to_list()) == {True}


def test_rolling_strict_selection_is_stable_when_validation_labels_mutate() -> None:
    nbeatsx_library = _rolling_nbeatsx_v2_plus_library(anchor_count=5)
    tft_library = _rolling_tft_candidate_library(anchor_count=5, validation_regret=120.0)
    mutated_tft_library = _mutate_anchor_labels(
        tft_library,
        anchors={FIRST_ANCHOR + timedelta(days=4)},
        regret=999.0,
    )

    strict_frame = build_dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame(
        nbeatsx_library,
        tft_library,
        tenant_ids=TENANTS,
        baseline_source_model_name=BASELINE_SOURCE,
        tft_source_model_names=(TFT_SOURCE,),
        combined_source_model_name=COMBINED_SOURCE,
        validation_window_count=2,
        validation_anchor_count=1,
        min_prior_anchors_before_window=1,
        min_prior_mean_improvement_ratio_vs_v2_plus=0.01,
        generated_at=GENERATED_AT,
    )
    mutated = build_dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame(
        nbeatsx_library,
        mutated_tft_library,
        tenant_ids=TENANTS,
        baseline_source_model_name=BASELINE_SOURCE,
        tft_source_model_names=(TFT_SOURCE,),
        combined_source_model_name=COMBINED_SOURCE,
        validation_window_count=2,
        validation_anchor_count=1,
        min_prior_anchors_before_window=1,
        min_prior_mean_improvement_ratio_vs_v2_plus=0.01,
        generated_at=GENERATED_AT,
    )

    key_columns = [
        "tenant_id",
        "anchor_timestamp",
        "selected_source_model_name",
        "selected_candidate_family",
        "selected_candidate_model_name",
        "fallback_to_v2_plus",
    ]
    selected = strict_frame.filter(pl.col("selection_role") == "nbeatsx_tft_meta_selector_v1")
    mutated_selected = mutated.filter(
        pl.col("selection_role") == "nbeatsx_tft_meta_selector_v1"
    )
    assert selected.select(key_columns).rows() == mutated_selected.select(key_columns).rows()
    assert selected["regret_uah"].to_list() != mutated_selected["regret_uah"].to_list()


def test_rolling_strict_uses_configured_prior_window_not_all_history() -> None:
    nbeatsx_library = _rolling_nbeatsx_v2_plus_library(anchor_count=6)
    tft_library = _rolling_tft_candidate_library(anchor_count=6, validation_regret=120.0)

    strict_frame = build_dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame(
        nbeatsx_library,
        tft_library,
        tenant_ids=TENANTS,
        baseline_source_model_name=BASELINE_SOURCE,
        tft_source_model_names=(TFT_SOURCE,),
        combined_source_model_name=COMBINED_SOURCE,
        validation_window_count=1,
        validation_anchor_count=1,
        min_prior_anchors_before_window=2,
        min_prior_mean_improvement_ratio_vs_v2_plus=0.01,
        generated_at=GENERATED_AT,
    )

    assert set(strict_frame["used_prior_anchor_count"].to_list()) == {2}
    assert set(strict_frame["available_prior_anchor_count_before_window"].to_list()) == {5}


def _portfolio(
    *,
    train_regret: float,
    final_regret: float,
    anchor_count: int = 1,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    baseline = _v2_plus_strict_frame(final_regret=200.0, anchor_count=anchor_count)
    nbeatsx_library = _nbeatsx_v2_plus_library(
        train_regret=200.0,
        final_regret=200.0,
        anchor_count=anchor_count,
    )
    tft = _tft_candidate_library(
        train_regret=train_regret,
        final_regret=final_regret,
        anchor_count=anchor_count,
    )
    audit = build_dfl_nbeatsx_tft_complementarity_audit_frame(
        baseline,
        tft,
        baseline_source_model_name=BASELINE_SOURCE,
        tft_source_model_names=(TFT_SOURCE,),
        final_validation_anchor_count_per_tenant=anchor_count,
    )
    portfolio = build_dfl_nbeatsx_tft_candidate_portfolio_v1_frame(
        nbeatsx_library,
        tft,
        baseline,
        audit,
        baseline_source_model_name=BASELINE_SOURCE,
        tft_source_model_names=(TFT_SOURCE,),
        final_validation_anchor_count_per_tenant=anchor_count,
    )
    return portfolio, audit


def _nbeatsx_v2_plus_library(
    *,
    train_regret: float,
    final_regret: float,
    anchor_count: int = 1,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_index, tenant_id in enumerate(TENANTS):
        rows.append(
            _candidate_row(
                tenant_id=tenant_id,
                source_model_name=BASELINE_SOURCE,
                candidate_family="frozen_v2_plus_fallback",
                candidate_model_name=f"dfl_schedule_value_learner_v2_plus_{BASELINE_SOURCE}",
                anchor=FIRST_ANCHOR - timedelta(days=tenant_index + 1),
                split_name="train_selection",
                regret=train_regret,
                forecast_prices=(1000.0, 5200.0),
                dispatch=(0.0, 1.0),
            )
        )
        rows.append(
            _candidate_row(
                tenant_id=tenant_id,
                source_model_name=BASELINE_SOURCE,
                candidate_family="strict_control",
                candidate_model_name="strict_similar_day",
                anchor=FIRST_ANCHOR - timedelta(days=tenant_index + 1),
                split_name="train_selection",
                regret=train_regret + 100.0,
                forecast_prices=(1100.0, 4800.0),
                dispatch=(0.0, 0.0),
            )
        )
        for anchor_index in range(anchor_count):
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            rows.append(
                _candidate_row(
                    tenant_id=tenant_id,
                    source_model_name=BASELINE_SOURCE,
                    candidate_family="frozen_v2_plus_fallback",
                    candidate_model_name=f"dfl_schedule_value_learner_v2_plus_{BASELINE_SOURCE}",
                    anchor=anchor,
                    split_name="final_holdout",
                    regret=final_regret,
                    forecast_prices=(1000.0, 5200.0),
                    dispatch=(0.0, 1.0),
                )
            )
            rows.append(
                _candidate_row(
                    tenant_id=tenant_id,
                    source_model_name=BASELINE_SOURCE,
                    candidate_family="strict_control",
                    candidate_model_name="strict_similar_day",
                    anchor=anchor,
                    split_name="final_holdout",
                    regret=final_regret + 100.0,
                    forecast_prices=(1100.0, 4800.0),
                    dispatch=(0.0, 0.0),
                )
            )
    return pl.DataFrame(rows)


def _rolling_nbeatsx_v2_plus_library(*, anchor_count: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index in range(anchor_count):
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            for family, model_name, regret, forecast_prices, dispatch in [
                (
                    "strict_control",
                    "strict_similar_day",
                    300.0,
                    (1100.0, 4800.0),
                    (0.0, 0.0),
                ),
                (
                    "raw_source",
                    BASELINE_SOURCE,
                    200.0,
                    (1000.0, 5200.0),
                    (0.0, 1.0),
                ),
            ]:
                rows.append(
                    _candidate_row(
                        tenant_id=tenant_id,
                        source_model_name=BASELINE_SOURCE,
                        candidate_family=family,
                        candidate_model_name=model_name,
                        anchor=anchor,
                        split_name="train_selection",
                        regret=regret,
                        forecast_prices=forecast_prices,
                        dispatch=dispatch,
                    )
                )
    return pl.DataFrame(rows)


def _rolling_tft_candidate_library(
    *,
    anchor_count: int,
    validation_regret: float,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index in range(anchor_count):
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            is_validation_tail = anchor_index >= anchor_count - 2
            rows.append(
                _candidate_row(
                    tenant_id=tenant_id,
                    source_model_name=TFT_SOURCE,
                    candidate_family="tft_quantile_peak_timing",
                    candidate_model_name=TFT_SOURCE,
                    anchor=anchor,
                    split_name="train_selection",
                    regret=validation_regret if is_validation_tail else 100.0,
                    forecast_prices=(5300.0, 900.0),
                    dispatch=(1.0, 0.0),
                )
            )
    return pl.DataFrame(rows).with_columns(
        [
            pl.lit("p50").alias("source_quantile"),
            pl.lit(True).alias("quantile_candidate_lane"),
        ]
    )


def _mutate_anchor_labels(
    frame: pl.DataFrame,
    *,
    anchors: set[datetime],
    regret: float,
) -> pl.DataFrame:
    return frame.with_columns(
        [
            pl.when(pl.col("anchor_timestamp").is_in(sorted(anchors)))
            .then(pl.lit(regret))
            .otherwise(pl.col("regret_uah"))
            .alias("regret_uah"),
            pl.when(pl.col("anchor_timestamp").is_in(sorted(anchors)))
            .then(pl.lit([9900.0, 100.0]))
            .otherwise(pl.col("actual_price_uah_mwh_vector"))
            .alias("actual_price_uah_mwh_vector"),
        ]
    )


def _tft_candidate_library(
    *,
    train_regret: float,
    final_regret: float,
    anchor_count: int = 1,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_index, tenant_id in enumerate(TENANTS):
        rows.append(
            _candidate_row(
                tenant_id=tenant_id,
                source_model_name=TFT_SOURCE,
                candidate_family="tft_quantile_peak_timing",
                candidate_model_name=TFT_SOURCE,
                anchor=FIRST_ANCHOR - timedelta(days=tenant_index + 1),
                split_name="train_selection",
                regret=train_regret,
                forecast_prices=(5300.0, 900.0),
                dispatch=(1.0, 0.0),
            )
        )
        for anchor_index in range(anchor_count):
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            rows.append(
                _candidate_row(
                    tenant_id=tenant_id,
                    source_model_name=TFT_SOURCE,
                    candidate_family="tft_quantile_peak_timing",
                    candidate_model_name=TFT_SOURCE,
                    anchor=anchor,
                    split_name="final_holdout",
                    regret=final_regret,
                    forecast_prices=(5300.0, 900.0),
                    dispatch=(1.0, 0.0),
                )
            )
    return pl.DataFrame(rows).with_columns(
        [
            pl.lit("p50").alias("source_quantile"),
            pl.lit(True).alias("quantile_candidate_lane"),
        ]
    )


def _wide_tft_candidate_library() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for rank in range(5):
            row = _candidate_row(
                tenant_id=tenant_id,
                source_model_name=TFT_SOURCE,
                candidate_family="tft_quantile_peak_timing",
                candidate_model_name=f"{TFT_SOURCE}:forecast_objective_rank_{rank}",
                anchor=FIRST_ANCHOR,
                split_name="final_holdout",
                regret=500.0 - (rank * 25.0),
                forecast_prices=(5000.0 + rank, 900.0),
                dispatch=(1.0, 0.0),
            )
            row["forecast_objective_value_uah"] = 1000.0 + rank
            rows.append(row)
    return pl.DataFrame(rows).with_columns(
        [
            pl.lit("p50").alias("source_quantile"),
            pl.lit(True).alias("quantile_candidate_lane"),
        ]
    )


def _final_tft_quantile_candidate_names(
    portfolio: pl.DataFrame,
) -> dict[str, tuple[str, ...]]:
    frame = portfolio.filter(
        (pl.col("portfolio_source") == "tft_quantile")
        & (pl.col("split_name") == "final_holdout")
    )
    return {
        str(tenant_id): tuple(sorted(str(value) for value in group["candidate_model_name"]))
        for tenant_id, group in frame.group_by("tenant_id", maintain_order=True)
    }


def _v2_plus_strict_frame(*, final_regret: float, anchor_count: int = 1) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index in range(anchor_count):
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            for role, model_name, regret in [
                ("strict_reference", "strict_similar_day", final_regret + 100.0),
                (
                    "schedule_value_learner_v2_plus",
                    f"dfl_schedule_value_learner_v2_plus_{BASELINE_SOURCE}",
                    final_regret,
                ),
            ]:
                row = _evaluation_row(
                    tenant_id=tenant_id,
                    source_model_name=BASELINE_SOURCE,
                    model_name=model_name,
                    anchor=anchor,
                    regret=regret,
                )
                row["selection_role"] = role
                rows.append(row)
    return pl.DataFrame(rows)


def _mutate_final_labels(frame: pl.DataFrame, *, final_regret: float) -> pl.DataFrame:
    return frame.with_columns(
        [
            pl.when(pl.col("split_name") == "final_holdout")
            .then(pl.lit(final_regret))
            .otherwise(pl.col("regret_uah"))
            .alias("regret_uah"),
            pl.when(pl.col("split_name") == "final_holdout")
            .then(pl.lit([9999.0, 100.0]))
            .otherwise(pl.col("actual_price_uah_mwh_vector"))
            .alias("actual_price_uah_mwh_vector"),
        ]
    )


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
) -> dict[str, object]:
    actual_prices = [1100.0, 5100.0][: len(forecast_prices)]
    soc = [0.5, 0.4][: len(forecast_prices)]
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
        "dispatch_mw_vector": list(dispatch),
        "soc_fraction_vector": soc,
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": sum(abs(value) for value in dispatch),
        "forecast_spread_uah_mwh": max(forecast_prices) - min(forecast_prices),
        "actual_spread_uah_mwh": max(actual_prices) - min(actual_prices),
        "forecast_top_k_actual_overlap": 0.5,
        "forecast_bottom_k_actual_overlap": 0.5,
        "peak_index_abs_error": 1.0,
        "trough_index_abs_error": 1.0,
        "soc_min_slack_fraction": 0.4,
        "prior_family_mean_regret_uah": regret,
        "safety_violation_count": 0,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "claim_scope": "unit",
        "evaluation_payload": {
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "source_forecast_model_name": source_model_name,
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
    return {
        "evaluation_id": f"{tenant_id}:{model_name}:{anchor:%Y%m%dT%H%M}",
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "forecast_model_name": model_name,
        "strategy_kind": DFL_NBEATSX_TFT_META_SELECTOR_STRICT_LP_STRATEGY_KIND,
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
        "total_throughput_mwh": 0.0,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "selection_role": "raw_reference",
        "forecast_price_uah_mwh_vector": [1000.0, 5200.0],
        "actual_price_uah_mwh_vector": [1100.0, 5100.0],
        "dispatch_mw_vector": [0.0, 1.0],
        "soc_fraction_vector": [0.5, 0.4],
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "evaluation_payload": {
            "claim_scope": "unit",
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "source_forecast_model_name": source_model_name,
        },
    }
