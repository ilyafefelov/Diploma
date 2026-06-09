from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.strategy.dispatch_sensitivity import (
    build_forecast_dispatch_sensitivity_frame,
)


def test_dispatch_sensitivity_extracts_forecast_and_dispatch_diagnostics() -> None:
    anchor = datetime(2026, 5, 1, 23)
    evaluation_frame = pl.DataFrame(
        [
            _evaluation_row(
                anchor=anchor,
                model_name="tft_horizon_regret_weighted_calibrated_v0",
                regret=50.0,
                horizon=[
                    _horizon_row(anchor, step_index=0, forecast_price=100.0, actual_price=120.0, net_power=-1.0),
                    _horizon_row(anchor, step_index=1, forecast_price=300.0, actual_price=260.0, net_power=1.0),
                    _horizon_row(anchor, step_index=2, forecast_price=180.0, actual_price=170.0, net_power=0.0),
                ],
            )
        ]
    )

    sensitivity = build_forecast_dispatch_sensitivity_frame(evaluation_frame)
    row = sensitivity.row(0, named=True)

    assert sensitivity.height == 1
    assert row["diagnostic_id"] == "eval-001:sensitivity"
    assert row["forecast_model_name"] == "tft_horizon_regret_weighted_calibrated_v0"
    assert row["forecast_mae_uah_mwh"] == pytest.approx(33.3)
    assert row["mean_forecast_error_uah_mwh"] == pytest.approx(-10.0)
    assert row["charge_energy_mwh"] == pytest.approx(1.0)
    assert row["discharge_energy_mwh"] == pytest.approx(1.0)
    assert row["forecast_dispatch_spread_uah_mwh"] == pytest.approx(200.0)
    assert row["realized_dispatch_spread_uah_mwh"] == pytest.approx(140.0)
    assert row["dispatch_spread_error_uah_mwh"] == pytest.approx(60.0)
    assert row["diagnostic_bucket"] == "low_regret"
    assert row["data_quality_tier"] == "thesis_grade"


def test_dispatch_sensitivity_marks_lp_sensitivity_when_forecast_error_is_small() -> None:
    anchor = datetime(2026, 5, 1, 23)
    evaluation_frame = pl.DataFrame(
        [
            _evaluation_row(
                anchor=anchor,
                model_name="strict_similar_day",
                regret=900.0,
                horizon=[
                    _horizon_row(anchor, step_index=0, forecast_price=100.0, actual_price=101.0, net_power=-1.0),
                    _horizon_row(anchor, step_index=1, forecast_price=110.0, actual_price=109.0, net_power=1.0),
                ],
            )
        ]
    )

    sensitivity = build_forecast_dispatch_sensitivity_frame(evaluation_frame)

    assert sensitivity.row(0, named=True)["diagnostic_bucket"] == "lp_dispatch_sensitivity"


def _evaluation_row(
    *,
    anchor: datetime,
    model_name: str,
    regret: float,
    horizon: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "evaluation_id": "eval-001",
        "tenant_id": "client_003_dnipro_factory",
        "forecast_model_name": model_name,
        "strategy_kind": "horizon_regret_weighted_forecast_calibration_benchmark",
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": datetime(2026, 5, 5),
        "horizon_hours": len(horizon),
        "starting_soc_fraction": 0.5,
        "starting_soc_source": "tenant_default",
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 1000.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 1.0,
        "total_throughput_mwh": 2.0,
        "committed_action": "DISCHARGE",
        "committed_power_mw": 1.0,
        "rank_by_regret": 1,
        "evaluation_payload": {
            "data_quality_tier": "thesis_grade",
            "forecast_diagnostics": {
                "mae_uah_mwh": 33.3,
                "rmse_uah_mwh": 35.0,
                "directional_accuracy": 0.75,
                "spread_ranking_quality": 0.8,
                "top_k_price_recall": 0.67,
                "price_cap_violation_count": 0.0,
                "mean_forecast_price_uah_mwh": 193.33,
                "mean_actual_price_uah_mwh": 183.33,
            },
            "horizon": horizon,
        },
    }


def _horizon_row(
    anchor: datetime,
    *,
    step_index: int,
    forecast_price: float,
    actual_price: float,
    net_power: float,
) -> dict[str, object]:
    return {
        "step_index": step_index,
        "interval_start": (anchor + timedelta(hours=step_index + 1)).isoformat(),
        "forecast_price_uah_mwh": forecast_price,
        "actual_price_uah_mwh": actual_price,
        "net_power_mw": net_power,
        "degradation_penalty_uah": 0.0,
    }
