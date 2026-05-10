from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.regime_gated_tft_selector import (
    build_dfl_regime_gated_tft_selector_v2_frame,
    build_dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame,
    validate_dfl_regime_gated_tft_selector_v2_evidence,
)

TENANTS: tuple[str, ...] = ("client_001_kyiv_mall", "client_002_lviv_office")
SOURCE_MODELS: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0")
FIRST_ANCHOR = datetime(2026, 4, 1, 23)


def test_regime_gated_selector_allows_tft_only_in_prior_strict_failure_regime() -> None:
    panel = _prior_feature_panel(
        final_cluster="strict_failure_captured",
        training_cluster="strict_failure_captured",
        final_best_non_strict_regret=20.0,
    )
    audit = _feature_audit_frame(
        final_cluster="strict_failure_captured",
        training_cluster="strict_failure_captured",
        training_selected_regret=70.0,
    )

    selector = build_dfl_regime_gated_tft_selector_v2_frame(
        panel,
        audit,
        tenant_ids=TENANTS,
        source_model_names=SOURCE_MODELS,
        min_training_window_count=1,
    )
    tft = _selector_row(selector, "tft_silver_v0", 1, "strict_failure_captured")
    nbeatsx = _selector_row(selector, "nbeatsx_silver_v0", 1, "strict_failure_captured")

    assert tft["selected_fallback_strategy"] == "tft_challenger"
    assert tft["allow_challenger"] is True
    assert nbeatsx["selected_fallback_strategy"] == "strict_similar_day"
    assert nbeatsx["allow_challenger"] is False
    assert nbeatsx["promotion_blocker"] == "source_not_tft"


def test_regime_gated_selector_defaults_to_strict_in_stable_region() -> None:
    panel = _prior_feature_panel(
        final_cluster="strict_stable_region",
        training_cluster="strict_failure_captured",
        final_best_non_strict_regret=10.0,
    )
    audit = _feature_audit_frame(
        final_cluster="strict_stable_region",
        training_cluster="strict_failure_captured",
        training_selected_regret=70.0,
    )

    selector = build_dfl_regime_gated_tft_selector_v2_frame(
        panel,
        audit,
        tenant_ids=TENANTS,
        source_model_names=("tft_silver_v0",),
        min_training_window_count=1,
    )

    tft = _selector_row(selector, "tft_silver_v0", 1, "strict_stable_region")
    assert tft["selected_fallback_strategy"] == "strict_similar_day"
    assert tft["allow_challenger"] is False
    assert tft["promotion_blocker"] == "strict_stable_region"


def test_regime_gated_selector_final_actual_mutation_changes_score_not_rule() -> None:
    panel = _prior_feature_panel(
        final_cluster="strict_failure_captured",
        training_cluster="strict_failure_captured",
        final_best_non_strict_regret=20.0,
    )
    mutated_panel = _prior_feature_panel(
        final_cluster="strict_failure_captured",
        training_cluster="strict_failure_captured",
        final_best_non_strict_regret=90.0,
    )
    audit = _feature_audit_frame(
        final_cluster="strict_failure_captured",
        training_cluster="strict_failure_captured",
        training_selected_regret=70.0,
    )

    selector = build_dfl_regime_gated_tft_selector_v2_frame(
        panel,
        audit,
        tenant_ids=TENANTS,
        source_model_names=("tft_silver_v0",),
        min_training_window_count=1,
    )
    mutated_selector = build_dfl_regime_gated_tft_selector_v2_frame(
        mutated_panel,
        audit,
        tenant_ids=TENANTS,
        source_model_names=("tft_silver_v0",),
        min_training_window_count=1,
    )
    strict_frame = build_dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame(
        _schedule_library(best_non_strict_regret=20.0),
        selector,
        panel,
    )
    mutated_strict_frame = build_dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame(
        _schedule_library(best_non_strict_regret=90.0),
        mutated_selector,
        mutated_panel,
    )

    assert selector.select("selected_rule_name").to_series().to_list() == mutated_selector.select(
        "selected_rule_name"
    ).to_series().to_list()
    assert _selector_mean_regret(strict_frame, "tft_silver_v0") == 20.0
    assert _selector_mean_regret(mutated_strict_frame, "tft_silver_v0") == 90.0


def test_regime_gated_strict_frame_matches_utc_feature_anchors_to_naive_library() -> None:
    panel = _prior_feature_panel(
        final_cluster="strict_failure_captured",
        training_cluster="strict_failure_captured",
        final_best_non_strict_regret=20.0,
    ).with_columns(
        pl.col("anchor_timestamp").dt.replace_time_zone("UTC"),
        pl.col("prior_cutoff_timestamp").dt.replace_time_zone("UTC"),
    )
    audit = _feature_audit_frame(
        final_cluster="strict_failure_captured",
        training_cluster="strict_failure_captured",
        training_selected_regret=70.0,
    )
    selector = build_dfl_regime_gated_tft_selector_v2_frame(
        panel,
        audit,
        tenant_ids=TENANTS,
        source_model_names=("tft_silver_v0",),
        min_training_window_count=1,
    )

    strict_frame = build_dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame(
        _schedule_library(best_non_strict_regret=20.0),
        selector,
        panel,
    )

    assert strict_frame.height > 0
    assert _selector_mean_regret(strict_frame, "tft_silver_v0") == 20.0


def test_regime_gated_selector_evidence_validates_coverage_and_claim_flags() -> None:
    panel = _prior_feature_panel(
        final_cluster="strict_failure_captured",
        training_cluster="strict_failure_captured",
        final_best_non_strict_regret=20.0,
    )
    audit = _feature_audit_frame(
        final_cluster="strict_failure_captured",
        training_cluster="strict_failure_captured",
        training_selected_regret=70.0,
    )
    selector = build_dfl_regime_gated_tft_selector_v2_frame(
        panel,
        audit,
        tenant_ids=TENANTS,
        source_model_names=("tft_silver_v0",),
        min_training_window_count=1,
    )
    strict_frame = build_dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame(
        _schedule_library(best_non_strict_regret=20.0),
        selector,
        panel,
    )
    bad_flags = strict_frame.with_columns(
        evaluation_payload=pl.Series(
            "evaluation_payload",
            [
                {**payload, "not_market_execution": False}
                for payload in strict_frame["evaluation_payload"].to_list()
            ],
        )
    )

    evidence = validate_dfl_regime_gated_tft_selector_v2_evidence(
        strict_frame,
        source_model_names=("tft_silver_v0",),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )
    bad_evidence = validate_dfl_regime_gated_tft_selector_v2_evidence(
        bad_flags,
        source_model_names=("tft_silver_v0",),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )

    assert evidence.passed is True
    assert evidence.metadata["model_summaries"][0]["production_gate_passed"] is True
    assert bad_evidence.passed is False
    assert "not_market_execution" in bad_evidence.description


def test_regime_gated_strict_frame_empty_schema_keeps_string_columns() -> None:
    panel = _prior_feature_panel(
        final_cluster="strict_failure_captured",
        training_cluster="strict_failure_captured",
        final_best_non_strict_regret=20.0,
    )
    audit = _feature_audit_frame(
        final_cluster="strict_failure_captured",
        training_cluster="strict_failure_captured",
        training_selected_regret=70.0,
    )
    selector = build_dfl_regime_gated_tft_selector_v2_frame(
        panel,
        audit,
        tenant_ids=TENANTS,
        source_model_names=("tft_silver_v0",),
        min_training_window_count=1,
    )
    unmatched_library = _schedule_library(best_non_strict_regret=20.0).with_columns(
        anchor_timestamp=pl.col("anchor_timestamp") + timedelta(days=90)
    )

    strict_frame = build_dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame(
        unmatched_library,
        selector,
        panel,
    )

    assert strict_frame.height == 0
    assert strict_frame.schema["forecast_model_name"] == pl.String
    assert strict_frame.filter(
        pl.col("forecast_model_name").str.starts_with("dfl_regime_gated_tft_selector_v2_")
    ).height == 0


def _selector_row(
    frame: pl.DataFrame,
    source_model_name: str,
    window_index: int,
    regime_label: str,
) -> dict[str, object]:
    rows = frame.filter(
        (pl.col("source_model_name") == source_model_name)
        & (pl.col("window_index") == window_index)
        & (pl.col("regime_label") == regime_label)
    ).to_dicts()
    assert len(rows) == 1
    return rows[0]


def _selector_mean_regret(strict_frame: pl.DataFrame, source_model_name: str) -> float:
    return (
        strict_frame.filter(
            (pl.col("source_model_name") == source_model_name)
            & (pl.col("evaluation_payload").struct.field("selector_row_role") == "selector")
        )
        .select("regret_uah")
        .mean()
        .item()
    )


def _prior_feature_panel(
    *,
    final_cluster: str,
    training_cluster: str,
    final_best_non_strict_regret: float,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
        for window_index, cluster in [(1, final_cluster), (2, training_cluster)]:
            for tenant_id in TENANTS:
                for anchor_index in range(2):
                    anchor = FIRST_ANCHOR + timedelta(days=(window_index - 1) * 10 + anchor_index)
                    best_regret = final_best_non_strict_regret if window_index == 1 else 70.0
                    rows.append(
                        {
                            "tenant_id": tenant_id,
                            "source_model_name": source_model_name,
                            "window_index": window_index,
                            "anchor_timestamp": anchor,
                            "prior_cutoff_timestamp": anchor - timedelta(days=1),
                            "selector_feature_prior_anchor_count": 30,
                            "selector_feature_prior_strict_mean_regret_uah": 100.0,
                            "selector_feature_prior_raw_mean_regret_uah": 130.0,
                            "selector_feature_prior_best_non_strict_mean_regret_uah": 70.0,
                            "selector_feature_prior_strict_minus_best_non_strict_uah": 30.0,
                            "selector_feature_prior_top_rank_overlap_mean": 0.8,
                            "selector_feature_prior_bottom_rank_overlap_mean": 0.8,
                            "selector_feature_prior_price_regime": "high_spread",
                            "selector_feature_prior_spread_volatility_regime": "stable",
                            "selector_feature_prior_price_spread_std_uah_mwh": 100.0,
                            "selector_feature_prior_net_load_mean_mw": 1.0,
                            "analysis_only_strict_regret_uah": 100.0,
                            "analysis_only_raw_regret_uah": 130.0,
                            "analysis_only_best_non_strict_regret_uah": best_regret,
                            "analysis_only_selected_regret_uah": best_regret,
                            "analysis_only_selected_candidate_family": "strict_raw_blend_v2",
                            "analysis_only_selector_beats_strict": best_regret < 100.0,
                            "analysis_only_failure_cluster": cluster,
                            "claim_scope": "dfl_strict_failure_prior_feature_panel_not_full_dfl",
                            "not_full_dfl": True,
                            "not_market_execution": True,
                        }
                    )
    return pl.DataFrame(rows)


def _feature_audit_frame(
    *,
    final_cluster: str,
    training_cluster: str,
    training_selected_regret: float,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
        for window_index, cluster in [(1, final_cluster), (2, training_cluster)]:
            selected = 20.0 if window_index == 1 else training_selected_regret
            for tenant_id in TENANTS:
                rows.append(
                    {
                        "tenant_id": tenant_id,
                        "source_model_name": source_model_name,
                        "window_index": window_index,
                        "validation_anchor_count": 2,
                        "strict_mean_regret_uah": 100.0,
                        "raw_mean_regret_uah": 130.0,
                        "selected_mean_regret_uah": selected,
                        "strict_median_regret_uah": 100.0,
                        "selected_median_regret_uah": selected,
                        "mean_regret_improvement_ratio_vs_strict": (100.0 - selected) / 100.0,
                        "mean_regret_improvement_ratio_vs_raw": (130.0 - selected) / 130.0,
                        "failure_cluster": cluster,
                        "claim_scope": "dfl_strict_failure_feature_audit_not_full_dfl",
                        "not_full_dfl": True,
                        "not_market_execution": True,
                    }
                )
    return pl.DataFrame(rows)


def _schedule_library(*, best_non_strict_regret: float) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
        for tenant_id in TENANTS:
            for anchor_index in range(2):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                rows.extend(
                    [
                        _library_row(
                            tenant_id,
                            source_model_name,
                            anchor,
                            candidate_family="strict_control",
                            candidate_model_name="strict_similar_day",
                            regret=100.0,
                        ),
                        _library_row(
                            tenant_id,
                            source_model_name,
                            anchor,
                            candidate_family="raw_source",
                            candidate_model_name=source_model_name,
                            regret=130.0,
                        ),
                        _library_row(
                            tenant_id,
                            source_model_name,
                            anchor,
                            candidate_family="strict_raw_blend_v2",
                            candidate_model_name=f"strict_raw_blend_v2_{source_model_name}",
                            regret=best_non_strict_regret,
                        ),
                    ]
                )
    return pl.DataFrame(rows)


def _library_row(
    tenant_id: str,
    source_model_name: str,
    anchor: datetime,
    *,
    candidate_family: str,
    candidate_model_name: str,
    regret: float,
) -> dict[str, object]:
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "candidate_family": candidate_family,
        "candidate_model_name": candidate_model_name,
        "anchor_timestamp": anchor,
        "split_name": "final_holdout",
        "horizon_hours": 2,
        "forecast_price_uah_mwh_vector": [1000.0, 1200.0],
        "actual_price_uah_mwh_vector": [1100.0, 1300.0],
        "dispatch_mw_vector": [0.0, 0.0],
        "soc_fraction_vector": [0.5, 0.5],
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 990.0 - regret,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": 1.0,
        "forecast_spread_uah_mwh": 200.0,
        "safety_violation_count": 0,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evaluation_payload": {
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": 0,
            "not_full_dfl": True,
            "not_market_execution": True,
        },
    }
