from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.promotion_gate import (
    evaluate_offline_dfl_promotion_gate,
    evaluate_strategy_promotion_gate,
)


def test_strategy_promotion_gate_passes_when_candidate_beats_frozen_control() -> None:
    frame = _strategy_frame(candidate_regrets=[90.0] * 90)

    result = evaluate_strategy_promotion_gate(frame, candidate_model_name="candidate_dfl_v1")

    assert result.passed is True
    assert result.decision == "promote"
    assert result.metrics["mean_regret_improvement_ratio"] == 0.1


def test_strategy_promotion_gate_blocks_weak_mean_improvement() -> None:
    frame = _strategy_frame(candidate_regrets=[97.0] * 90)

    result = evaluate_strategy_promotion_gate(frame, candidate_model_name="candidate_dfl_v1")

    assert result.passed is False
    assert result.decision == "block"
    assert "mean regret improvement" in result.description


def test_strategy_promotion_gate_blocks_worse_median_even_when_mean_improves() -> None:
    frame = _strategy_frame(candidate_regrets=([0.0] * 20) + ([101.0] * 70))

    result = evaluate_strategy_promotion_gate(frame, candidate_model_name="candidate_dfl_v1")

    assert result.passed is False
    assert result.metrics["candidate_median_regret_uah"] == 101.0
    assert "median regret" in result.description


def test_strategy_promotion_gate_blocks_non_thesis_or_safety_violating_rows() -> None:
    frame = _strategy_frame(
        candidate_regrets=[90.0] * 90,
        data_quality_tier="demo_grade",
        observed_coverage_ratio=0.95,
        safety_violation_count=1,
    )

    result = evaluate_strategy_promotion_gate(frame, candidate_model_name="candidate_dfl_v1")

    assert result.passed is False
    assert "thesis_grade" in result.description
    assert "observed coverage" in result.description
    assert "safety violations" in result.description


def test_offline_dfl_promotion_gate_blocks_current_negative_summary_shape() -> None:
    frame = pl.DataFrame(
        [
            {
                "experiment_name": "offline_horizon_bias_dfl_v0",
                "tenant_id": "client_003_dnipro_factory",
                "forecast_model_name": "tft_silver_v0",
                "validation_anchor_count": 18,
                "baseline_validation_relaxed_regret_uah": 1974.55,
                "dfl_validation_relaxed_regret_uah": 2460.07,
                "improved_over_baseline": False,
                "data_quality_tier": "thesis_grade",
                "claim_scope": "offline_dfl_experiment_not_full_dfl",
                "not_market_execution": True,
            }
        ]
    )

    result = evaluate_offline_dfl_promotion_gate(frame)

    assert result.passed is False
    assert result.decision == "block"
    assert "validation_anchor_count" in result.description
    assert "does not improve" in result.description


def _strategy_frame(
    *,
    candidate_regrets: list[float],
    data_quality_tier: str = "thesis_grade",
    observed_coverage_ratio: float = 1.0,
    safety_violation_count: int = 0,
) -> pl.DataFrame:
    generated_at = datetime(2026, 5, 7, 10)
    first_anchor = datetime(2026, 1, 1, 23)
    rows: list[dict[str, object]] = []
    for index, candidate_regret in enumerate(candidate_regrets):
        anchor = first_anchor + timedelta(days=index)
        rows.append(
            _row(
                anchor=anchor,
                generated_at=generated_at,
                model_name="strict_similar_day",
                regret=100.0,
                data_quality_tier="thesis_grade",
                observed_coverage_ratio=1.0,
                safety_violation_count=0,
            )
        )
        rows.append(
            _row(
                anchor=anchor,
                generated_at=generated_at,
                model_name="candidate_dfl_v1",
                regret=candidate_regret,
                data_quality_tier=data_quality_tier,
                observed_coverage_ratio=observed_coverage_ratio,
                safety_violation_count=safety_violation_count,
            )
        )
    return pl.DataFrame(rows)


def _row(
    *,
    anchor: datetime,
    generated_at: datetime,
    model_name: str,
    regret: float,
    data_quality_tier: str,
    observed_coverage_ratio: float,
    safety_violation_count: int,
) -> dict[str, object]:
    return {
        "tenant_id": "client_003_dnipro_factory",
        "forecast_model_name": model_name,
        "strategy_kind": "real_data_rolling_origin_benchmark",
        "anchor_timestamp": anchor,
        "generated_at": generated_at,
        "regret_uah": regret,
        "evaluation_payload": {
            "data_quality_tier": data_quality_tier,
            "observed_coverage_ratio": observed_coverage_ratio,
            "safety_violation_count": safety_violation_count,
            "not_market_execution": True,
        },
    }
