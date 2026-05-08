from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.strict_failure_selector import (
    DFL_STRICT_FAILURE_SELECTOR_STRICT_LP_STRATEGY_KIND,
    build_dfl_strict_failure_selector_frame,
    build_dfl_strict_failure_selector_strict_lp_benchmark_frame,
    evaluate_dfl_strict_failure_selector_gate,
    strict_failure_selector_model_name,
    validate_dfl_strict_failure_selector_evidence,
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
GENERATED_AT = datetime(2026, 5, 8, 18)


def test_strict_failure_selector_learns_threshold_from_train_selection_only() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=500.0,
        challenger_final_regret=150.0,
    )
    mutated_final_library = _candidate_library_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=500.0,
        challenger_final_regret=600.0,
    )

    selector = build_dfl_strict_failure_selector_frame(
        library,
        _autopsy_frame(library),
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        switch_threshold_grid_uah=(0.0, 100.0, 250.0),
        min_final_holdout_tenant_anchor_count_per_source_model=90,
    )
    mutated_selector = build_dfl_strict_failure_selector_frame(
        mutated_final_library,
        _autopsy_frame(mutated_final_library),
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        switch_threshold_grid_uah=(0.0, 100.0, 250.0),
        min_final_holdout_tenant_anchor_count_per_source_model=90,
    )

    assert selector.height == 10
    assert selector["selected_switch_threshold_uah"].to_list() == [0.0] * 10
    assert mutated_selector["selected_switch_threshold_uah"].to_list() == [0.0] * 10
    assert selector["selected_train_mean_regret_uah"].to_list() == [100.0] * 10
    assert selector["selected_final_mean_regret_uah"].to_list() == [150.0] * 10
    assert mutated_selector["selected_final_mean_regret_uah"].to_list() == [600.0] * 10
    assert set(selector["claim_scope"].unique().to_list()) == {
        "dfl_strict_failure_selector_v1_not_full_dfl"
    }


def test_strict_failure_selector_strict_benchmark_and_gate_can_promote_when_it_beats_strict() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=500.0,
        challenger_final_regret=150.0,
    )
    selector = build_dfl_strict_failure_selector_frame(
        library,
        _autopsy_frame(library),
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        min_final_holdout_tenant_anchor_count_per_source_model=90,
    )

    strict_frame = build_dfl_strict_failure_selector_strict_lp_benchmark_frame(
        library,
        selector,
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_strict_failure_selector_gate(strict_frame, source_model_names=SOURCE_MODELS)
    evidence = validate_dfl_strict_failure_selector_evidence(strict_frame)

    assert strict_frame.height == 5 * 2 * 18 * 4
    assert strict_frame.select("strategy_kind").to_series().unique().to_list() == [
        DFL_STRICT_FAILURE_SELECTOR_STRICT_LP_STRATEGY_KIND
    ]
    assert strict_failure_selector_model_name("tft_silver_v0") in strict_frame[
        "forecast_model_name"
    ].unique().to_list()
    assert evidence.passed is True
    assert gate.passed is True
    assert gate.decision == "promote"
    assert gate.metrics["production_gate_passed"] is True
    assert gate.metrics["mean_regret_improvement_ratio_vs_strict"] == 0.5


def test_strict_failure_selector_gate_blocks_when_selector_loses_to_strict() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=500.0,
        challenger_final_regret=450.0,
    )
    selector = build_dfl_strict_failure_selector_frame(
        library,
        _autopsy_frame(library),
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        min_final_holdout_tenant_anchor_count_per_source_model=90,
    )
    strict_frame = build_dfl_strict_failure_selector_strict_lp_benchmark_frame(library, selector)

    gate = evaluate_dfl_strict_failure_selector_gate(strict_frame, source_model_names=SOURCE_MODELS)

    assert gate.passed is False
    assert gate.decision == "diagnostic_pass_production_blocked"
    assert gate.metrics["development_gate_passed"] is True
    assert gate.metrics["production_gate_passed"] is False
    assert "strict_similar_day" in gate.description


def test_strict_failure_selector_rejects_under_coverage_non_thesis_and_split_overlap() -> None:
    under_coverage = _candidate_library_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=500.0,
        challenger_final_regret=150.0,
        final_anchor_count=17,
    )
    non_thesis = _candidate_library_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=500.0,
        challenger_final_regret=150.0,
        data_quality_tier="demo_grade",
    )
    split_overlap = _candidate_library_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=500.0,
        challenger_final_regret=150.0,
    ).with_columns(
        pl.when(
            (pl.col("split_name") == "final_holdout")
            & (pl.col("anchor_timestamp") == FIRST_ANCHOR + timedelta(days=2))
        )
        .then(pl.lit("train_selection"))
        .otherwise(pl.col("split_name"))
        .alias("split_name")
    )

    for frame, expected_message in [
        (under_coverage, "final-holdout tenant-anchor count"),
        (non_thesis, "thesis_grade"),
        (split_overlap, "final-holdout tenant-anchor count"),
    ]:
        try:
            build_dfl_strict_failure_selector_frame(
                frame,
                _autopsy_frame(frame),
                tenant_ids=CANONICAL_TENANTS,
                forecast_model_names=SOURCE_MODELS,
            )
        except ValueError as exc:
            assert expected_message in str(exc)
        else:
            raise AssertionError("expected invalid selector evidence to fail")


def test_strict_failure_selector_evidence_blocks_missing_selector_rows() -> None:
    library = _candidate_library_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=500.0,
        challenger_final_regret=150.0,
    )
    selector = build_dfl_strict_failure_selector_frame(
        library,
        _autopsy_frame(library),
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        min_final_holdout_tenant_anchor_count_per_source_model=90,
    )
    strict_frame = build_dfl_strict_failure_selector_strict_lp_benchmark_frame(library, selector)
    missing_selector = strict_frame.filter(
        ~pl.col("forecast_model_name").str.starts_with("dfl_strict_failure_selector_v1_")
    )

    evidence = validate_dfl_strict_failure_selector_evidence(missing_selector)

    assert evidence.passed is False
    assert "strict/raw/selector rows must cover matching tenant-anchor sets" in evidence.description


def _candidate_library_from_regrets(
    *,
    strict_final_regret: float,
    raw_final_regret: float,
    challenger_final_regret: float,
    final_anchor_count: int = 18,
    data_quality_tier: str = "thesis_grade",
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    train_anchor_count = 2
    total_anchor_count = train_anchor_count + final_anchor_count
    for tenant_id in CANONICAL_TENANTS:
        for source_model_name in SOURCE_MODELS:
            for anchor_index in range(total_anchor_count):
                split_name = "final_holdout" if anchor_index >= train_anchor_count else "train_selection"
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                rows.extend(
                    [
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="strict_control",
                            candidate_model_name="strict_similar_day",
                            anchor=anchor,
                            split_name=split_name,
                            regret=strict_final_regret if split_name == "final_holdout" else 300.0,
                            forecast_prices=(1000.0, 5000.0),
                            data_quality_tier=data_quality_tier,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="raw_source",
                            candidate_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            regret=raw_final_regret if split_name == "final_holdout" else 500.0,
                            forecast_prices=(5000.0, 1000.0),
                            data_quality_tier=data_quality_tier,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="strict_raw_blend_v2",
                            candidate_model_name=f"dfl_strict_raw_blend_v2_{source_model_name}_w050",
                            anchor=anchor,
                            split_name=split_name,
                            regret=challenger_final_regret if split_name == "final_holdout" else 100.0,
                            forecast_prices=(900.0, 6500.0),
                            data_quality_tier=data_quality_tier,
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
    forecast_prices: tuple[float, float],
    data_quality_tier: str,
) -> dict[str, object]:
    horizon = [
        {
            "forecast_timestamp": (anchor + timedelta(hours=index + 1)).isoformat(),
            "forecast_price_uah_mwh": forecast_prices[index],
            "actual_price_uah_mwh": (1000.0, 5000.0)[index],
            "net_power_mw": (1.0, -1.0)[index],
            "soc_fraction": (0.55, 0.45)[index],
        }
        for index in range(2)
    ]
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "candidate_family": candidate_family,
        "candidate_model_name": candidate_model_name,
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "split_name": split_name,
        "horizon_hours": 2,
        "forecast_price_uah_mwh_vector": list(forecast_prices),
        "actual_price_uah_mwh_vector": [1000.0, 5000.0],
        "dispatch_mw_vector": [1.0, -1.0],
        "soc_fraction_vector": [0.55, 0.45],
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 10.0,
        "total_throughput_mwh": 2.0,
        "forecast_spread_uah_mwh": max(forecast_prices) - min(forecast_prices),
        "actual_spread_uah_mwh": 4000.0,
        "forecast_top_k_actual_overlap": 1.0,
        "forecast_bottom_k_actual_overlap": 1.0,
        "peak_index_abs_error": 0.0,
        "trough_index_abs_error": 0.0,
        "soc_min_slack_fraction": 0.45,
        "prior_family_mean_regret_uah": regret,
        "safety_violation_count": 0,
        "data_quality_tier": data_quality_tier,
        "observed_coverage_ratio": 1.0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "claim_scope": "dfl_schedule_candidate_library_v2_not_full_dfl",
        "candidate_library_version": "v2_test",
        "evaluation_payload": {
            "horizon": horizon,
            "data_quality_tier": data_quality_tier,
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": 0,
            "not_full_dfl": True,
            "not_market_execution": True,
        },
    }


def _autopsy_frame(library: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for anchor_frame in library.filter(pl.col("split_name") == "final_holdout").partition_by(
        ["tenant_id", "source_model_name", "anchor_timestamp"]
    ):
        strict = anchor_frame.filter(pl.col("candidate_family") == "strict_control").row(0, named=True)
        non_strict = (
            anchor_frame.filter(pl.col("candidate_family") != "strict_control")
            .sort("regret_uah")
            .row(0, named=True)
        )
        gap = float(strict["regret_uah"]) - float(non_strict["regret_uah"])
        rows.append(
            {
                "tenant_id": strict["tenant_id"],
                "source_model_name": strict["source_model_name"],
                "anchor_timestamp": strict["anchor_timestamp"],
                "strict_regret_uah": float(strict["regret_uah"]),
                "best_non_strict_regret_uah": float(non_strict["regret_uah"]),
                "strict_gap_to_best_non_strict_uah": gap,
                "strict_high_regret_flag": True,
                "selected_candidate_family": non_strict["candidate_family"],
                "selected_candidate_model_name": non_strict["candidate_model_name"],
                "candidate_family_count": 2,
                "recommended_next_action": (
                    "train_selector_to_detect_strict_failure"
                    if gap > 0.0
                    else "expand_data_or_candidate_library"
                ),
                "claim_scope": "dfl_strict_baseline_autopsy_not_full_dfl",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(rows)
