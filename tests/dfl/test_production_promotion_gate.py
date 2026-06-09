from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.production_promotion_gate import (
    build_dfl_production_promotion_gate_frame,
    validate_dfl_production_promotion_gate_evidence,
)
from smart_arbitrage.dfl.source_specific_challenger import (
    build_dfl_source_specific_research_challenger_frame,
)

TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
SOURCE_MODELS: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0")
FIRST_FINAL_ANCHOR = datetime(2026, 4, 12, 23)
GENERATED_AT = datetime(2026, 5, 10, 12)


def test_production_gate_promotes_only_regime_specific_robust_tft() -> None:
    gate = build_dfl_production_promotion_gate_frame(
        _source_specific_frame(
            tft_fallback_regret=80.0,
            nbeatsx_fallback_regret=102.0,
            tft_strict_pass_window_count=3,
            tft_cluster="rank_instability",
        ),
        _robustness_frame(tft_strict_pass_window_count=3),
        _feature_audit_frame(tft_cluster="rank_instability"),
        _coverage_audit_frame(eligible_anchor_count=104),
        source_model_names=SOURCE_MODELS,
    )

    tft = _row(gate, "tft_silver_v0", "rank_instability")
    nbeatsx = _row(gate, "nbeatsx_silver_v0", "strict_stable_region")
    evidence = validate_dfl_production_promotion_gate_evidence(
        gate,
        source_model_names=SOURCE_MODELS,
    )

    assert evidence.passed is True
    assert tft["production_promote"] is True
    assert tft["fallback_strategy"] == "promoted_tft_silver_v0_regime_gate"
    assert tft["promotion_blocker"] == "none"
    assert tft["market_execution_enabled"] is False
    assert nbeatsx["production_promote"] is False
    assert nbeatsx["promotion_blocker"] == "latest_signal_missing"
    assert evidence.metadata["promoted_source_model_names"] == ["tft_silver_v0"]


def test_production_gate_blocks_latest_tft_signal_when_rolling_windows_are_not_robust() -> None:
    gate = build_dfl_production_promotion_gate_frame(
        _source_specific_frame(
            tft_fallback_regret=80.0,
            nbeatsx_fallback_regret=102.0,
            tft_strict_pass_window_count=2,
            tft_cluster="rank_instability",
        ),
        _robustness_frame(tft_strict_pass_window_count=2),
        _feature_audit_frame(tft_cluster="rank_instability"),
        _coverage_audit_frame(eligible_anchor_count=104),
        source_model_names=SOURCE_MODELS,
    )

    tft = _row(gate, "tft_silver_v0", "rank_instability")

    assert tft["latest_source_signal"] is True
    assert tft["rolling_strict_pass_window_count"] == 2
    assert tft["production_promote"] is False
    assert tft["promotion_blocker"] == "rolling_not_robust"
    assert tft["fallback_strategy"] == "strict_similar_day"


def test_production_gate_keeps_strict_default_in_strict_stable_regime() -> None:
    gate = build_dfl_production_promotion_gate_frame(
        _source_specific_frame(
            tft_fallback_regret=80.0,
            nbeatsx_fallback_regret=102.0,
            tft_strict_pass_window_count=3,
            tft_cluster="strict_stable_region",
        ),
        _robustness_frame(tft_strict_pass_window_count=3),
        _feature_audit_frame(tft_cluster="strict_stable_region"),
        _coverage_audit_frame(eligible_anchor_count=104),
        source_model_names=SOURCE_MODELS,
    )

    tft = _row(gate, "tft_silver_v0", "strict_stable_region")

    assert tft["robust_research_challenger"] is True
    assert tft["production_promote"] is False
    assert tft["promotion_blocker"] == "strict_stable_region"
    assert tft["fallback_strategy"] == "strict_similar_day"


def test_production_gate_final_holdout_mutation_changes_score_not_prior_regime_context() -> None:
    original = build_dfl_production_promotion_gate_frame(
        _source_specific_frame(
            tft_fallback_regret=80.0,
            nbeatsx_fallback_regret=102.0,
            tft_strict_pass_window_count=3,
            tft_cluster="rank_instability",
        ),
        _robustness_frame(tft_strict_pass_window_count=3),
        _feature_audit_frame(tft_cluster="rank_instability"),
        _coverage_audit_frame(eligible_anchor_count=104),
        source_model_names=SOURCE_MODELS,
    )
    mutated = build_dfl_production_promotion_gate_frame(
        _source_specific_frame(
            tft_fallback_regret=70.0,
            nbeatsx_fallback_regret=102.0,
            tft_strict_pass_window_count=3,
            tft_cluster="rank_instability",
        ),
        _robustness_frame(tft_strict_pass_window_count=3),
        _feature_audit_frame(tft_cluster="rank_instability"),
        _coverage_audit_frame(eligible_anchor_count=104),
        source_model_names=SOURCE_MODELS,
    )

    original_tft = _row(original, "tft_silver_v0", "rank_instability")
    mutated_tft = _row(mutated, "tft_silver_v0", "rank_instability")

    assert original_tft["latest_fallback_mean_regret_uah"] == 80.0
    assert mutated_tft["latest_fallback_mean_regret_uah"] == 70.0
    assert original_tft["regime_label"] == mutated_tft["regime_label"]
    assert original_tft["rolling_strict_pass_window_count"] == mutated_tft[
        "rolling_strict_pass_window_count"
    ]


def test_production_gate_evidence_fails_on_undercoverage_and_bad_claim_flags() -> None:
    undercoverage = build_dfl_production_promotion_gate_frame(
        _source_specific_frame(
            tft_fallback_regret=80.0,
            nbeatsx_fallback_regret=102.0,
            tft_strict_pass_window_count=3,
            tft_cluster="rank_instability",
            final_anchor_count_per_tenant=17,
        ),
        _robustness_frame(tft_strict_pass_window_count=3),
        _feature_audit_frame(tft_cluster="rank_instability"),
        _coverage_audit_frame(eligible_anchor_count=104),
        source_model_names=SOURCE_MODELS,
    )
    bad_flags = undercoverage.with_columns(
        not_market_execution=pl.lit(False),
        market_execution_enabled=pl.lit(True),
    )

    undercoverage_evidence = validate_dfl_production_promotion_gate_evidence(
        undercoverage,
        source_model_names=SOURCE_MODELS,
    )
    bad_flags_evidence = validate_dfl_production_promotion_gate_evidence(
        bad_flags,
        source_model_names=SOURCE_MODELS,
    )

    assert undercoverage_evidence.passed is False
    assert "validation tenant-anchor count" in undercoverage_evidence.description
    assert bad_flags_evidence.passed is False
    assert "not_market_execution=true" in bad_flags_evidence.description
    assert "market_execution_enabled=false" in bad_flags_evidence.description


def _source_specific_frame(
    *,
    tft_fallback_regret: float,
    nbeatsx_fallback_regret: float,
    tft_strict_pass_window_count: int,
    tft_cluster: str,
    final_anchor_count_per_tenant: int = 18,
) -> pl.DataFrame:
    return build_dfl_source_specific_research_challenger_frame(
        _fallback_strict_frame(
            tft_fallback_regret=tft_fallback_regret,
            nbeatsx_fallback_regret=nbeatsx_fallback_regret,
            final_anchor_count_per_tenant=final_anchor_count_per_tenant,
        ),
        _feature_aware_strict_frame(tft_selector_regret=95.0, nbeatsx_selector_regret=104.0),
        _robustness_frame(tft_strict_pass_window_count=tft_strict_pass_window_count),
        _feature_audit_frame(tft_cluster=tft_cluster),
        source_model_names=SOURCE_MODELS,
    )


def _row(frame: pl.DataFrame, source_model_name: str, regime_label: str) -> dict[str, object]:
    rows = frame.filter(
        (pl.col("source_model_name") == source_model_name)
        & (pl.col("regime_label") == regime_label)
    ).to_dicts()
    assert len(rows) == 1
    return rows[0]


def _fallback_strict_frame(
    *,
    tft_fallback_regret: float,
    nbeatsx_fallback_regret: float,
    final_anchor_count_per_tenant: int = 18,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
        fallback_regret = (
            tft_fallback_regret
            if source_model_name == "tft_silver_v0"
            else nbeatsx_fallback_regret
        )
        for tenant_id in TENANTS:
            for anchor_index in range(final_anchor_count_per_tenant):
                anchor = FIRST_FINAL_ANCHOR + timedelta(days=anchor_index)
                rows.extend(
                    [
                        _strict_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            forecast_model_name="strict_similar_day",
                            anchor=anchor,
                            regret=100.0,
                            selection_role="strict_reference",
                        ),
                        _strict_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            forecast_model_name=f"dfl_residual_dt_fallback_v1_{source_model_name}",
                            anchor=anchor,
                            regret=fallback_regret,
                            selection_role="fallback_strategy",
                        ),
                    ]
                )
    return pl.DataFrame(rows)


def _feature_aware_strict_frame(
    *,
    tft_selector_regret: float,
    nbeatsx_selector_regret: float,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
        selector_regret = (
            tft_selector_regret
            if source_model_name == "tft_silver_v0"
            else nbeatsx_selector_regret
        )
        for tenant_id in TENANTS:
            for anchor_index in range(18):
                anchor = FIRST_FINAL_ANCHOR + timedelta(days=anchor_index)
                rows.extend(
                    [
                        _strict_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            forecast_model_name="strict_similar_day",
                            anchor=anchor,
                            regret=100.0,
                            selection_role="strict_reference",
                            selector_row_role="strict_reference",
                        ),
                        _strict_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            forecast_model_name=source_model_name,
                            anchor=anchor,
                            regret=130.0,
                            selection_role="raw_reference",
                            selector_row_role="raw_reference",
                        ),
                        _strict_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            forecast_model_name=(
                                f"dfl_feature_aware_strict_failure_selector_v2_{source_model_name}"
                            ),
                            anchor=anchor,
                            regret=selector_regret,
                            selection_role="selector",
                            selector_row_role="selector",
                        ),
                    ]
                )
    return pl.DataFrame(rows)


def _strict_row(
    *,
    tenant_id: str,
    source_model_name: str,
    forecast_model_name: str,
    anchor: datetime,
    regret: float,
    selection_role: str,
    selector_row_role: str | None = None,
) -> dict[str, object]:
    payload = {
        "source_forecast_model_name": source_model_name,
        "selection_role": selection_role,
        "selector_row_role": selector_row_role or selection_role,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": 0,
        "not_full_dfl": True,
        "not_market_execution": True,
    }
    return {
        "evaluation_id": f"{tenant_id}:{source_model_name}:{forecast_model_name}:{anchor:%Y%m%d}",
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": "synthetic_strict_gate",
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "horizon_hours": 24,
        "starting_soc_fraction": 0.5,
        "starting_soc_source": "test",
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 990.0 - regret,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": 1.0,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "selection_role": selection_role,
        "selected_strategy_source": "test",
        "evaluation_payload": payload,
    }


def _robustness_frame(*, tft_strict_pass_window_count: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
        for window_index in range(1, 5):
            strict_passed = (
                source_model_name == "tft_silver_v0"
                and window_index <= tft_strict_pass_window_count
            )
            rows.append(
                {
                    "source_model_name": source_model_name,
                    "selector_model_name": f"dfl_strict_failure_selector_v1_{source_model_name}",
                    "window_index": window_index,
                    "tenant_count": 5,
                    "validation_anchor_count_per_tenant": 18,
                    "validation_tenant_anchor_count": 90,
                    "minimum_prior_anchor_count_before_window": 30,
                    "strict_mean_regret_uah": 100.0,
                    "raw_mean_regret_uah": 130.0,
                    "selected_mean_regret_uah": 80.0 if strict_passed else 99.0,
                    "strict_median_regret_uah": 100.0,
                    "selected_median_regret_uah": 90.0 if strict_passed else 100.0,
                    "mean_regret_improvement_ratio_vs_raw": 0.2,
                    "mean_regret_improvement_ratio_vs_strict": 0.2 if strict_passed else 0.01,
                    "development_passed": True,
                    "source_specific_strict_passed": strict_passed,
                    "robust_research_challenger": False,
                    "production_promote": False,
                    "claim_scope": "dfl_strict_failure_selector_robustness_not_full_dfl",
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows)


def _feature_audit_frame(*, tft_cluster: str) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
        cluster = tft_cluster if source_model_name == "tft_silver_v0" else "strict_stable_region"
        for window_index in range(1, 5):
            for tenant_id in TENANTS:
                rows.append(
                    {
                        "tenant_id": tenant_id,
                        "source_model_name": source_model_name,
                        "window_index": window_index,
                        "validation_anchor_count": 18,
                        "strict_mean_regret_uah": 100.0,
                        "raw_mean_regret_uah": 130.0,
                        "selected_mean_regret_uah": 80.0
                        if cluster != "strict_stable_region"
                        else 100.0,
                        "mean_regret_improvement_ratio_vs_strict": 0.2
                        if cluster != "strict_stable_region"
                        else 0.0,
                        "mean_regret_improvement_ratio_vs_raw": 0.25,
                        "failure_cluster": cluster,
                        "claim_scope": "dfl_strict_failure_feature_audit_not_full_dfl",
                        "not_full_dfl": True,
                        "not_market_execution": True,
                    }
                )
    return pl.DataFrame(rows)


def _coverage_audit_frame(*, eligible_anchor_count: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        rows.append(
            {
                "tenant_id": tenant_id,
                "first_timestamp": datetime(2026, 1, 1),
                "last_timestamp": datetime(2026, 4, 30, 23),
                "price_row_count": 2880,
                "weather_observed_row_count": 2880,
                "expected_hour_count": 2880,
                "missing_price_hours": 0,
                "missing_weather_hours": 0,
                "eligible_anchor_count": eligible_anchor_count,
                "target_anchor_count_per_tenant": 90,
                "meets_target_anchor_count": eligible_anchor_count >= 90,
                "first_eligible_anchor_timestamp": datetime(2026, 1, 8, 23),
                "last_eligible_anchor_timestamp": datetime(2026, 4, 29, 23),
                "latest_benchmark_generated_at": GENERATED_AT,
                "latest_benchmark_anchor_count": 104,
                "latest_benchmark_model_count": 3,
                "price_observed_coverage_ratio": 1.0,
                "weather_observed_coverage_ratio": 1.0,
                "data_quality_tier": "thesis_grade",
                "claim_scope": "ua_observed_dfl_data_coverage_audit",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(rows)
