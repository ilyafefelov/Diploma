from datetime import datetime, timedelta

import polars as pl
import pytest
from pydantic import ValidationError

from smart_arbitrage.dfl.schemas import DFLTrainingExampleV2
from smart_arbitrage.dfl.training_examples import build_dfl_training_example_frame


def _evaluation_frame(*, include_strict: bool = True, data_quality_tier: str = "thesis_grade") -> pl.DataFrame:
    anchor = datetime(2026, 4, 1, 23)
    rows: list[dict[str, object]] = []
    if include_strict:
        rows.append(_row(anchor=anchor, model_name="strict_similar_day", regret=100.0, net_power=[0.2, -0.1]))
    rows.append(
        _row(
            anchor=anchor,
            model_name="tft_silver_v0",
            regret=80.0,
            net_power=[0.1, 0.0],
            data_quality_tier=data_quality_tier,
        )
    )
    return pl.DataFrame(rows)


def _row(
    *,
    anchor: datetime,
    model_name: str,
    regret: float,
    net_power: list[float],
    data_quality_tier: str = "thesis_grade",
) -> dict[str, object]:
    horizon = [
        {
            "step_index": index,
            "interval_start": (anchor + timedelta(hours=index + 1)).isoformat(),
            "forecast_price_uah_mwh": 1000.0 + (index * 100.0),
            "actual_price_uah_mwh": 1100.0 + (index * 50.0),
            "net_power_mw": net_power[index],
            "degradation_penalty_uah": 5.0 + index,
        }
        for index in range(len(net_power))
    ]
    return {
        "evaluation_id": f"{model_name}:eval",
        "tenant_id": "client_003_dnipro_factory",
        "forecast_model_name": model_name,
        "strategy_kind": "real_data_rolling_origin_benchmark",
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": datetime(2026, 5, 7, 10),
        "horizon_hours": len(net_power),
        "starting_soc_fraction": 0.52,
        "starting_soc_source": "tenant_default",
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 11.0,
        "total_throughput_mwh": 0.3,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "evaluation_payload": {
            "data_quality_tier": data_quality_tier,
            "observed_coverage_ratio": 1.0,
            "academic_scope": "Gold-layer forecast strategy evaluation.",
            "committed_dispatch_preview": {"action": "HOLD", "power_mw": 0.0},
            "forecast_diagnostics": {"mae_uah_mwh": 75.0},
            "horizon": horizon,
        },
    }


def test_dfl_training_example_v2_rejects_string_boolean_flags() -> None:
    valid = _valid_example_payload()
    valid["not_market_execution"] = "true"

    with pytest.raises(ValidationError):
        DFLTrainingExampleV2.model_validate(valid)


def test_dfl_training_example_frame_extracts_vectors_and_strict_baseline() -> None:
    frame = build_dfl_training_example_frame(_evaluation_frame())

    row = frame.filter(pl.col("forecast_model_name") == "tft_silver_v0").row(0, named=True)
    assert row["training_example_id"] == "client_003_dnipro_factory:tft_silver_v0:20260401T2300:v2"
    assert row["currency"] == "UAH"
    assert row["baseline_forecast_model_name"] == "strict_similar_day"
    assert row["forecast_price_vector_uah_mwh"] == [1000.0, 1100.0]
    assert row["actual_price_vector_uah_mwh"] == [1100.0, 1150.0]
    assert row["candidate_dispatch_vector_mw"] == [0.1, 0.0]
    assert row["baseline_dispatch_vector_mw"] == [0.2, -0.1]
    assert row["candidate_regret_uah"] == 80.0
    assert row["baseline_regret_uah"] == 100.0
    assert row["regret_delta_vs_baseline_uah"] == -20.0
    assert row["candidate_feasible"] is True
    assert row["baseline_feasible"] is True
    assert row["not_full_dfl"] is True
    assert row["not_market_execution"] is True
    assert row["claim_scope"] == "dfl_training_examples_not_full_dfl"


def test_dfl_training_example_frame_rejects_non_thesis_rows_by_default() -> None:
    with pytest.raises(ValueError, match="thesis_grade"):
        build_dfl_training_example_frame(_evaluation_frame(data_quality_tier="demo_grade"))


def test_dfl_training_example_frame_requires_strict_baseline_per_anchor() -> None:
    with pytest.raises(ValueError, match="strict_similar_day"):
        build_dfl_training_example_frame(_evaluation_frame(include_strict=False))


def test_dfl_training_example_frame_requires_vector_lengths_to_match_horizon() -> None:
    frame = _evaluation_frame()
    payload = dict(frame.row(1, named=True)["evaluation_payload"])
    payload["horizon"] = payload["horizon"][:1]
    broken = frame.with_columns(pl.Series("evaluation_payload", [frame.row(0, named=True)["evaluation_payload"], payload]))

    with pytest.raises(ValueError, match="horizon length"):
        build_dfl_training_example_frame(broken)


def _valid_example_payload() -> dict[str, object]:
    anchor = datetime(2026, 4, 1, 23)
    return {
        "training_example_id": "example:v2",
        "evaluation_id": "candidate-eval",
        "baseline_evaluation_id": "strict-eval",
        "tenant_id": "client_003_dnipro_factory",
        "anchor_timestamp": anchor,
        "horizon_start": anchor + timedelta(hours=1),
        "horizon_end": anchor + timedelta(hours=2),
        "horizon_hours": 2,
        "market_venue": "DAM",
        "currency": "UAH",
        "forecast_model_name": "tft_silver_v0",
        "strategy_kind": "real_data_rolling_origin_benchmark",
        "baseline_strategy_name": "strict_similar_day",
        "baseline_forecast_model_name": "strict_similar_day",
        "forecast_price_vector_uah_mwh": [1000.0, 1100.0],
        "actual_price_vector_uah_mwh": [1100.0, 1150.0],
        "candidate_dispatch_vector_mw": [0.1, 0.0],
        "baseline_dispatch_vector_mw": [0.2, -0.1],
        "candidate_degradation_penalty_vector_uah": [5.0, 6.0],
        "baseline_degradation_penalty_vector_uah": [5.0, 6.0],
        "candidate_net_value_uah": 920.0,
        "baseline_net_value_uah": 900.0,
        "oracle_net_value_uah": 1000.0,
        "candidate_regret_uah": 80.0,
        "baseline_regret_uah": 100.0,
        "regret_delta_vs_baseline_uah": -20.0,
        "total_throughput_mwh": 0.3,
        "total_degradation_penalty_uah": 11.0,
        "candidate_feasible": True,
        "baseline_feasible": True,
        "safety_violation_count": 0,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "claim_scope": "dfl_training_examples_not_full_dfl",
        "not_full_dfl": True,
        "not_market_execution": True,
        "generated_at": datetime(2026, 5, 7, 10),
    }
