from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.source_specific_challenger import (
    build_dfl_source_specific_research_challenger_frame,
    validate_dfl_source_specific_research_challenger_evidence,
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


def test_source_specific_challenger_marks_tft_latest_signal_and_blocks_non_robust_sources() -> None:
    frame = build_dfl_source_specific_research_challenger_frame(
        _fallback_strict_frame(tft_fallback_regret=80.0, nbeatsx_fallback_regret=102.0),
        _feature_aware_strict_frame(tft_selector_regret=95.0, nbeatsx_selector_regret=104.0),
        _robustness_frame(tft_strict_pass_window_count=1),
        _feature_audit_frame(),
        source_model_names=SOURCE_MODELS,
    )

    tft = _row(frame, "tft_silver_v0")
    nbeatsx = _row(frame, "nbeatsx_silver_v0")
    evidence = validate_dfl_source_specific_research_challenger_evidence(
        frame,
        source_model_names=SOURCE_MODELS,
    )

    assert evidence.passed is True
    assert tft["latest_source_signal"] is True
    assert tft["latest_mean_regret_improvement_ratio_vs_strict"] == 0.2
    assert tft["rolling_strict_pass_window_count"] == 1
    assert tft["robust_research_challenger"] is False
    assert tft["production_promote"] is False
    assert tft["gate_label"] == "latest_signal_not_robust"
    assert nbeatsx["latest_source_signal"] is False
    assert nbeatsx["robust_research_challenger"] is False
    assert evidence.metadata["latest_signal_source_model_names"] == ["tft_silver_v0"]
    assert evidence.metadata["robust_source_model_names"] == []


def test_source_specific_challenger_requires_three_of_four_rolling_strict_passes_for_robust_label() -> None:
    weak = build_dfl_source_specific_research_challenger_frame(
        _fallback_strict_frame(tft_fallback_regret=80.0, nbeatsx_fallback_regret=102.0),
        _feature_aware_strict_frame(tft_selector_regret=95.0, nbeatsx_selector_regret=104.0),
        _robustness_frame(tft_strict_pass_window_count=2),
        _feature_audit_frame(),
        source_model_names=SOURCE_MODELS,
    )
    robust = build_dfl_source_specific_research_challenger_frame(
        _fallback_strict_frame(tft_fallback_regret=80.0, nbeatsx_fallback_regret=102.0),
        _feature_aware_strict_frame(tft_selector_regret=95.0, nbeatsx_selector_regret=104.0),
        _robustness_frame(tft_strict_pass_window_count=3),
        _feature_audit_frame(),
        source_model_names=SOURCE_MODELS,
    )

    assert _row(weak, "tft_silver_v0")["robust_research_challenger"] is False
    assert _row(weak, "tft_silver_v0")["gate_label"] == "latest_signal_not_robust"
    assert _row(robust, "tft_silver_v0")["robust_research_challenger"] is True
    assert _row(robust, "tft_silver_v0")["production_promote"] is False
    assert _row(robust, "tft_silver_v0")["gate_label"] == "robust_research_challenger"


def test_source_specific_challenger_final_holdout_mutation_changes_latest_scoring_not_rolling_context() -> None:
    original = build_dfl_source_specific_research_challenger_frame(
        _fallback_strict_frame(tft_fallback_regret=80.0, nbeatsx_fallback_regret=102.0),
        _feature_aware_strict_frame(tft_selector_regret=95.0, nbeatsx_selector_regret=104.0),
        _robustness_frame(tft_strict_pass_window_count=1),
        _feature_audit_frame(),
        source_model_names=SOURCE_MODELS,
    )
    mutated = build_dfl_source_specific_research_challenger_frame(
        _fallback_strict_frame(tft_fallback_regret=70.0, nbeatsx_fallback_regret=102.0),
        _feature_aware_strict_frame(tft_selector_regret=95.0, nbeatsx_selector_regret=104.0),
        _robustness_frame(tft_strict_pass_window_count=1),
        _feature_audit_frame(),
        source_model_names=SOURCE_MODELS,
    )

    original_tft = _row(original, "tft_silver_v0")
    mutated_tft = _row(mutated, "tft_silver_v0")

    assert original_tft["latest_fallback_mean_regret_uah"] == 80.0
    assert mutated_tft["latest_fallback_mean_regret_uah"] == 70.0
    assert original_tft["rolling_strict_pass_window_count"] == mutated_tft[
        "rolling_strict_pass_window_count"
    ]
    assert original_tft["dominant_failure_cluster"] == mutated_tft["dominant_failure_cluster"]


def test_source_specific_challenger_evidence_fails_on_missing_source_undercoverage_and_bad_claim_flags() -> None:
    missing_source = build_dfl_source_specific_research_challenger_frame(
        _fallback_strict_frame(tft_fallback_regret=80.0, nbeatsx_fallback_regret=102.0).filter(
            pl.col("source_model_name") != "nbeatsx_silver_v0"
        ),
        _feature_aware_strict_frame(tft_selector_regret=95.0, nbeatsx_selector_regret=104.0),
        _robustness_frame(tft_strict_pass_window_count=1),
        _feature_audit_frame(),
        source_model_names=SOURCE_MODELS,
    )
    undercoverage = build_dfl_source_specific_research_challenger_frame(
        _fallback_strict_frame(
            tft_fallback_regret=80.0,
            nbeatsx_fallback_regret=102.0,
            final_anchor_count_per_tenant=17,
        ),
        _feature_aware_strict_frame(tft_selector_regret=95.0, nbeatsx_selector_regret=104.0),
        _robustness_frame(tft_strict_pass_window_count=1),
        _feature_audit_frame(),
        source_model_names=SOURCE_MODELS,
    )
    bad_flags = build_dfl_source_specific_research_challenger_frame(
        _fallback_strict_frame(
            tft_fallback_regret=80.0,
            nbeatsx_fallback_regret=102.0,
            data_quality_tier="demo_grade",
        ),
        _feature_aware_strict_frame(tft_selector_regret=95.0, nbeatsx_selector_regret=104.0),
        _robustness_frame(tft_strict_pass_window_count=1),
        _feature_audit_frame(),
        source_model_names=SOURCE_MODELS,
    )

    missing_evidence = validate_dfl_source_specific_research_challenger_evidence(
        missing_source,
        source_model_names=SOURCE_MODELS,
    )
    undercoverage_evidence = validate_dfl_source_specific_research_challenger_evidence(
        undercoverage,
        source_model_names=SOURCE_MODELS,
    )
    bad_flags_evidence = validate_dfl_source_specific_research_challenger_evidence(
        bad_flags,
        source_model_names=SOURCE_MODELS,
    )

    assert missing_evidence.passed is False
    assert "nbeatsx_silver_v0" in missing_evidence.description
    assert undercoverage_evidence.passed is False
    assert "validation tenant-anchor count" in undercoverage_evidence.description
    assert bad_flags_evidence.passed is False
    assert "thesis_grade" in bad_flags_evidence.description


def _row(frame: pl.DataFrame, source_model_name: str) -> dict[str, object]:
    rows = frame.filter(pl.col("source_model_name") == source_model_name).to_dicts()
    assert len(rows) == 1
    return rows[0]


def _fallback_strict_frame(
    *,
    tft_fallback_regret: float,
    nbeatsx_fallback_regret: float,
    final_anchor_count_per_tenant: int = 18,
    data_quality_tier: str = "thesis_grade",
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
                            data_quality_tier=data_quality_tier,
                        ),
                        _strict_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            forecast_model_name=f"dfl_residual_dt_fallback_v1_{source_model_name}",
                            anchor=anchor,
                            regret=fallback_regret,
                            selection_role="fallback_strategy",
                            data_quality_tier=data_quality_tier,
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
    data_quality_tier: str = "thesis_grade",
    selector_row_role: str | None = None,
) -> dict[str, object]:
    payload = {
        "source_forecast_model_name": source_model_name,
        "selection_role": selection_role,
        "selector_row_role": selector_row_role or selection_role,
        "data_quality_tier": data_quality_tier,
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


def _feature_audit_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
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
                        "selected_mean_regret_uah": 95.0,
                        "mean_regret_improvement_ratio_vs_strict": 0.05,
                        "mean_regret_improvement_ratio_vs_raw": 0.25,
                        "failure_cluster": (
                            "rank_instability"
                            if source_model_name == "tft_silver_v0"
                            else "strict_stable_region"
                        ),
                        "claim_scope": "dfl_strict_failure_feature_audit_not_full_dfl",
                        "not_full_dfl": True,
                        "not_market_execution": True,
                    }
                )
    return pl.DataFrame(rows)
