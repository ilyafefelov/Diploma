from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.evidence_registry import (
    HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
    REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND,
    build_dfl_vector_evidence_registry,
    summarize_dfl_training_example_vectors,
)


def test_training_vector_summary_uses_latest_tenant_batch() -> None:
    older_generated_at = datetime(2026, 5, 6, 10)
    latest_generated_at = datetime(2026, 5, 7, 10)
    frame = pl.DataFrame(
        [
            _training_example_row(
                anchor=datetime(2026, 1, 1, 23),
                model_name="strict_similar_day",
                generated_at=older_generated_at,
            ),
            *[
                _training_example_row(
                    anchor=datetime(2026, 1, 1, 23) + timedelta(days=index),
                    model_name=model_name,
                    generated_at=latest_generated_at,
                )
                for index in range(90)
                for model_name in ("strict_similar_day", "tft_silver_v0", "nbeatsx_silver_v0")
            ],
        ]
    )

    summary = summarize_dfl_training_example_vectors(frame)

    assert summary["passed"] is True
    assert summary["training_example_row_count"] == 270
    assert summary["anchor_count"] == 90
    assert summary["forecast_model_count"] == 3
    assert summary["latest_generated_at"] == "2026-05-07T10:00:00"


def test_registry_blocks_candidate_when_latest_batch_lacks_strict_baseline() -> None:
    registry = build_dfl_vector_evidence_registry(
        run_slug="unit",
        training_example_frame=pl.DataFrame([_training_example_row(anchor=datetime(2026, 1, 1, 23))]),
        evaluation_frames_by_strategy_kind={
            REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND: pl.DataFrame(
                [
                    _evaluation_row(
                        anchor=datetime(2026, 1, 1, 23) + timedelta(days=index),
                        model_name="tft_silver_v0",
                        regret=90.0,
                    )
                    for index in range(90)
                ]
            )
        },
        candidate_model_names_by_strategy_kind={
            REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND: ("tft_silver_v0",),
        },
    )

    gate = registry["promotion_gate_results"][0]
    assert gate["decision"] == "block"
    assert "strict_similar_day anchor_count" in gate["description"]


def test_registry_blocks_candidate_below_required_anchor_count() -> None:
    rows = [
        _evaluation_row(
            anchor=datetime(2026, 1, 1, 23) + timedelta(days=index),
            model_name="strict_similar_day",
            regret=100.0,
        )
        for index in range(90)
    ]
    rows.extend(
        _evaluation_row(
            anchor=datetime(2026, 1, 1, 23) + timedelta(days=index),
            model_name="tft_silver_v0",
            regret=90.0,
        )
        for index in range(30)
    )

    registry = build_dfl_vector_evidence_registry(
        run_slug="unit",
        training_example_frame=pl.DataFrame([_training_example_row(anchor=datetime(2026, 1, 1, 23))]),
        evaluation_frames_by_strategy_kind={
            REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND: pl.DataFrame(rows),
        },
        candidate_model_names_by_strategy_kind={
            REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND: ("tft_silver_v0",),
        },
    )

    gate = registry["promotion_gate_results"][0]
    assert gate["decision"] == "block"
    assert "tft_silver_v0 anchor_count must be at least 90; observed 30" in gate["description"]


def test_registry_only_promotes_candidate_that_clears_mean_and_median_gate() -> None:
    rows = [
        item
        for index in range(90)
        for item in (
            _evaluation_row(
                anchor=datetime(2026, 1, 1, 23) + timedelta(days=index),
                model_name="strict_similar_day",
                regret=100.0,
                strategy_kind=HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
            ),
            _evaluation_row(
                anchor=datetime(2026, 1, 1, 23) + timedelta(days=index),
                model_name="tft_horizon_regret_weighted_calibrated_v0",
                regret=90.0,
                strategy_kind=HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND,
            ),
        )
    ]

    registry = build_dfl_vector_evidence_registry(
        run_slug="unit",
        training_example_frame=pl.DataFrame([_training_example_row(anchor=datetime(2026, 1, 1, 23))]),
        evaluation_frames_by_strategy_kind={
            HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND: pl.DataFrame(rows),
        },
        candidate_model_names_by_strategy_kind={
            HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND: (
                "tft_horizon_regret_weighted_calibrated_v0",
            ),
        },
    )

    gate = registry["promotion_gate_results"][0]
    assert gate["decision"] == "promote"
    assert gate["metrics"]["mean_regret_improvement_ratio"] == 0.1
    assert registry["overall_promotion_decision"] == "candidate_promoted_for_research_review"


def test_registry_blocks_current_negative_offline_dfl_shape() -> None:
    registry = build_dfl_vector_evidence_registry(
        run_slug="unit",
        training_example_frame=pl.DataFrame([_training_example_row(anchor=datetime(2026, 1, 1, 23))]),
        evaluation_frames_by_strategy_kind={},
        candidate_model_names_by_strategy_kind={},
        offline_dfl_experiment_frame=pl.DataFrame(
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
        ),
    )

    offline_gate = registry["offline_dfl_gate_result"]
    assert offline_gate["decision"] == "block"
    assert "validation_anchor_count" in offline_gate["description"]
    assert "does not improve" in offline_gate["description"]


def _training_example_row(
    *,
    anchor: datetime,
    model_name: str = "strict_similar_day",
    generated_at: datetime = datetime(2026, 5, 7, 10),
) -> dict[str, object]:
    return {
        "training_example_id": f"client_003_dnipro_factory:{model_name}:{anchor.strftime('%Y%m%dT%H%M')}:v2",
        "evaluation_id": f"{model_name}:eval",
        "baseline_evaluation_id": "strict:eval",
        "tenant_id": "client_003_dnipro_factory",
        "anchor_timestamp": anchor,
        "horizon_start": anchor + timedelta(hours=1),
        "horizon_end": anchor + timedelta(hours=2),
        "horizon_hours": 2,
        "market_venue": "DAM",
        "currency": "UAH",
        "forecast_model_name": model_name,
        "strategy_kind": REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND,
        "baseline_strategy_name": "strict_similar_day",
        "baseline_forecast_model_name": "strict_similar_day",
        "forecast_price_vector_uah_mwh": [1000.0, 1100.0],
        "actual_price_vector_uah_mwh": [1050.0, 1150.0],
        "candidate_dispatch_vector_mw": [0.0, 0.0],
        "baseline_dispatch_vector_mw": [0.0, 0.0],
        "candidate_degradation_penalty_vector_uah": [0.0, 0.0],
        "baseline_degradation_penalty_vector_uah": [0.0, 0.0],
        "candidate_net_value_uah": 900.0,
        "baseline_net_value_uah": 900.0,
        "oracle_net_value_uah": 1000.0,
        "candidate_regret_uah": 100.0,
        "baseline_regret_uah": 100.0,
        "regret_delta_vs_baseline_uah": 0.0,
        "total_throughput_mwh": 0.0,
        "total_degradation_penalty_uah": 0.0,
        "candidate_feasible": True,
        "baseline_feasible": True,
        "safety_violation_count": 0,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "claim_scope": "dfl_training_examples_not_full_dfl",
        "not_full_dfl": True,
        "not_market_execution": True,
        "generated_at": generated_at,
    }


def _evaluation_row(
    *,
    anchor: datetime,
    model_name: str,
    regret: float,
    strategy_kind: str = REAL_DATA_ROLLING_ORIGIN_STRATEGY_KIND,
) -> dict[str, object]:
    return {
        "evaluation_id": f"{model_name}:{anchor.strftime('%Y%m%dT%H%M')}",
        "tenant_id": "client_003_dnipro_factory",
        "forecast_model_name": model_name,
        "strategy_kind": strategy_kind,
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": datetime(2026, 5, 7, 10),
        "horizon_hours": 24,
        "starting_soc_fraction": 0.5,
        "starting_soc_source": "tenant_default",
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 1000.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": 0.0,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "evaluation_payload": {
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": 0,
            "not_market_execution": True,
        },
    }
