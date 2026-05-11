from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.schedule_value_promotion_gate import (
    DFL_SCHEDULE_VALUE_PRODUCTION_GATE_CLAIM_SCOPE,
    build_dfl_schedule_value_production_gate_frame,
    validate_dfl_schedule_value_production_gate_evidence,
)
from smart_arbitrage.dfl.schedule_value_learner import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_CLAIM_SCOPE,
    DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_LP_STRATEGY_KIND,
    schedule_value_learner_v2_model_name,
)
from smart_arbitrage.dfl.schedule_value_learner_robustness import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_ROBUSTNESS_CLAIM_SCOPE,
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
GENERATED_AT = datetime(2026, 5, 11, 12)


def test_schedule_value_production_gate_promotes_offline_when_latest_and_rolling_pass() -> None:
    gate = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(selected_regrets={"tft_silver_v0": 80.0, "nbeatsx_silver_v0": 85.0}),
        _robustness_frame(strict_pass_counts={"tft_silver_v0": 3, "nbeatsx_silver_v0": 4}),
        source_model_names=SOURCE_MODELS,
    )
    evidence = validate_dfl_schedule_value_production_gate_evidence(
        gate,
        source_model_names=SOURCE_MODELS,
    )

    assert evidence.passed is True
    assert set(gate["production_promote"]) == {True}
    assert set(gate["market_execution_enabled"]) == {False}
    assert set(gate["claim_scope"]) == {DFL_SCHEDULE_VALUE_PRODUCTION_GATE_CLAIM_SCOPE}
    assert _row(gate, "tft_silver_v0")["allowed_challenger"] == (
        "dfl_schedule_value_learner_v2_tft_silver_v0"
    )
    assert _row(gate, "nbeatsx_silver_v0")["fallback_strategy"] == (
        "strict_similar_day_default_fallback"
    )
    assert evidence.metadata["promoted_source_model_names"] == [
        "nbeatsx_silver_v0",
        "tft_silver_v0",
    ]


def test_schedule_value_production_gate_blocks_median_degradation() -> None:
    gate = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(
            selected_regrets={"tft_silver_v0": 40.0, "nbeatsx_silver_v0": 85.0},
            selected_median_regrets={"tft_silver_v0": 120.0, "nbeatsx_silver_v0": 85.0},
        ),
        _robustness_frame(strict_pass_counts={"tft_silver_v0": 3, "nbeatsx_silver_v0": 4}),
        source_model_names=SOURCE_MODELS,
    )

    tft = _row(gate, "tft_silver_v0")
    nbeatsx = _row(gate, "nbeatsx_silver_v0")

    assert tft["latest_source_signal"] is False
    assert tft["production_promote"] is False
    assert tft["promotion_blocker"] == "median_degraded"
    assert nbeatsx["production_promote"] is True


def test_schedule_value_production_gate_blocks_rolling_failure() -> None:
    gate = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(selected_regrets={"tft_silver_v0": 80.0, "nbeatsx_silver_v0": 85.0}),
        _robustness_frame(strict_pass_counts={"tft_silver_v0": 2, "nbeatsx_silver_v0": 4}),
        source_model_names=SOURCE_MODELS,
    )

    tft = _row(gate, "tft_silver_v0")

    assert tft["latest_source_signal"] is True
    assert tft["rolling_strict_pass_window_count"] == 2
    assert tft["production_promote"] is False
    assert tft["promotion_blocker"] == "rolling_not_robust"


def test_schedule_value_production_gate_final_score_mutation_does_not_change_robustness_context() -> None:
    robust = _robustness_frame(strict_pass_counts={"tft_silver_v0": 3, "nbeatsx_silver_v0": 4})
    original = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(selected_regrets={"tft_silver_v0": 80.0, "nbeatsx_silver_v0": 85.0}),
        robust,
        source_model_names=SOURCE_MODELS,
    )
    mutated = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(selected_regrets={"tft_silver_v0": 140.0, "nbeatsx_silver_v0": 85.0}),
        robust,
        source_model_names=SOURCE_MODELS,
    )

    original_tft = _row(original, "tft_silver_v0")
    mutated_tft = _row(mutated, "tft_silver_v0")

    assert original_tft["latest_selected_mean_regret_uah"] == 80.0
    assert mutated_tft["latest_selected_mean_regret_uah"] == 140.0
    assert original_tft["rolling_strict_pass_window_count"] == mutated_tft[
        "rolling_strict_pass_window_count"
    ]
    assert original_tft["robust_research_challenger"] == mutated_tft[
        "robust_research_challenger"
    ]


def test_schedule_value_production_gate_evidence_fails_on_bad_flags_and_market_execution() -> None:
    gate = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(selected_regrets={"tft_silver_v0": 80.0, "nbeatsx_silver_v0": 85.0}),
        _robustness_frame(strict_pass_counts={"tft_silver_v0": 3, "nbeatsx_silver_v0": 4}),
        source_model_names=SOURCE_MODELS,
    )
    bad_gate = gate.with_columns(
        not_market_execution=pl.lit(False),
        market_execution_enabled=pl.lit(True),
    )

    evidence = validate_dfl_schedule_value_production_gate_evidence(
        bad_gate,
        source_model_names=SOURCE_MODELS,
    )

    assert evidence.passed is False
    assert "not_market_execution=true" in evidence.description
    assert "market_execution_enabled=false" in evidence.description


def _row(frame: pl.DataFrame, source_model_name: str) -> dict[str, object]:
    rows = frame.filter(pl.col("source_model_name") == source_model_name).to_dicts()
    assert len(rows) == 1
    return rows[0]


def _strict_frame(
    *,
    selected_regrets: dict[str, float],
    selected_median_regrets: dict[str, float] | None = None,
    final_anchor_count_per_tenant: int = 18,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
        for tenant_id in TENANTS:
            for anchor_index in range(final_anchor_count_per_tenant):
                anchor = FIRST_FINAL_ANCHOR + timedelta(days=anchor_index)
                selected_regret = selected_regrets[source_model_name]
                if selected_median_regrets and anchor_index <= final_anchor_count_per_tenant // 2:
                    selected_regret = selected_median_regrets[source_model_name]
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
                            forecast_model_name=source_model_name,
                            anchor=anchor,
                            regret=500.0,
                            selection_role="raw_reference",
                        ),
                        _strict_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            forecast_model_name=schedule_value_learner_v2_model_name(
                                source_model_name
                            ),
                            anchor=anchor,
                            regret=selected_regret,
                            selection_role="schedule_value_learner",
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
) -> dict[str, object]:
    payload = {
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": 0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "selection_role": selection_role,
    }
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "regret_uah": regret,
        "selection_role": selection_role,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": 0,
        "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evaluation_payload": payload,
    }


def _robustness_frame(*, strict_pass_counts: dict[str, int]) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
        pass_count = strict_pass_counts[source_model_name]
        for window_index in range(1, 5):
            strict_passed = window_index <= pass_count
            rows.append(
                {
                    "source_model_name": source_model_name,
                    "window_index": window_index,
                    "tenant_count": len(TENANTS),
                    "validation_anchor_count_per_tenant": 18,
                    "validation_tenant_anchor_count": 90,
                    "minimum_prior_anchor_count_before_window": 86 - ((window_index - 1) * 18),
                    "strict_mean_regret_uah": 100.0,
                    "raw_mean_regret_uah": 500.0,
                    "selected_mean_regret_uah": 80.0 if strict_passed else 99.0,
                    "strict_median_regret_uah": 100.0,
                    "selected_median_regret_uah": 80.0 if strict_passed else 101.0,
                    "development_passed": True,
                    "source_specific_strict_passed": strict_passed,
                    "passing_window_count_for_source": pass_count,
                    "robust_research_challenger": pass_count >= 3 and window_index == 1,
                    "production_promote": False,
                    "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_ROBUSTNESS_CLAIM_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows)
