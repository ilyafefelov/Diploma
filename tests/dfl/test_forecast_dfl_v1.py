from datetime import datetime, timedelta

import polars as pl
import torch

import smart_arbitrage.dfl.forecast_dfl_v1 as forecast_dfl_v1
import smart_arbitrage.dfl.relaxed_dispatch as relaxed_dispatch
from smart_arbitrage.dfl.decision_loss import DecisionLossResult
from smart_arbitrage.dfl.forecast_dfl_v1 import (
    DFL_FORECAST_DFL_V1_STRICT_LP_STRATEGY_KIND,
    build_dfl_forecast_dfl_v1_panel_frame,
    build_dfl_forecast_dfl_v1_strict_lp_benchmark_frame,
    dfl_forecast_dfl_v1_model_name,
)

TENANT_ID = "client_003_dnipro_factory"
SOURCE_MODEL = "tft_silver_v0"
DFL_MODEL = dfl_forecast_dfl_v1_model_name(SOURCE_MODEL)
FIRST_ANCHOR = datetime(2026, 1, 1, 23)
GENERATED_AT = datetime(2026, 5, 9, 12)


def test_dfl_forecast_v1_panel_learns_only_from_prior_anchors() -> None:
    base = build_dfl_forecast_dfl_v1_panel_frame(
        _benchmark_frame(anchor_count=8, final_actual_prices=(1000.0, 5000.0)),
        tenant_ids=(TENANT_ID,),
        forecast_model_names=(SOURCE_MODEL,),
        final_validation_anchor_count_per_tenant=2,
        max_train_anchors_per_tenant=6,
        inner_validation_fraction=0.33,
        epoch_count=2,
        learning_rate=10.0,
    )
    mutated = build_dfl_forecast_dfl_v1_panel_frame(
        _benchmark_frame(anchor_count=8, final_actual_prices=(5000.0, 1000.0)),
        tenant_ids=(TENANT_ID,),
        forecast_model_names=(SOURCE_MODEL,),
        final_validation_anchor_count_per_tenant=2,
        max_train_anchors_per_tenant=6,
        inner_validation_fraction=0.33,
        epoch_count=2,
        learning_rate=10.0,
    )

    base_row = base.row(0, named=True)
    mutated_row = mutated.row(0, named=True)

    assert base_row["dfl_v1_checkpoint_horizon_biases_uah_mwh"] == mutated_row["dfl_v1_checkpoint_horizon_biases_uah_mwh"]
    assert base_row["first_final_holdout_anchor_timestamp"] == FIRST_ANCHOR + timedelta(days=6)
    assert base_row["not_full_dfl"] is True
    assert base_row["not_market_execution"] is True


def test_dfl_forecast_v1_strict_benchmark_emits_raw_control_and_dfl_rows() -> None:
    benchmark = _benchmark_frame(anchor_count=8)
    panel = build_dfl_forecast_dfl_v1_panel_frame(
        benchmark,
        tenant_ids=(TENANT_ID,),
        forecast_model_names=(SOURCE_MODEL,),
        final_validation_anchor_count_per_tenant=2,
        max_train_anchors_per_tenant=6,
        inner_validation_fraction=0.33,
        epoch_count=2,
        learning_rate=10.0,
    )

    result = build_dfl_forecast_dfl_v1_strict_lp_benchmark_frame(
        benchmark,
        panel,
        tenant_ids=(TENANT_ID,),
        forecast_model_names=(SOURCE_MODEL,),
        final_validation_anchor_count_per_tenant=2,
        generated_at=GENERATED_AT,
    )

    assert result.height == 2 * 3
    assert set(result.select("forecast_model_name").to_series().to_list()) == {
        "strict_similar_day",
        SOURCE_MODEL,
        DFL_MODEL,
    }
    assert result.select("strategy_kind").to_series().unique().to_list() == [
        DFL_FORECAST_DFL_V1_STRICT_LP_STRATEGY_KIND
    ]
    dfl_row = result.filter(pl.col("forecast_model_name") == DFL_MODEL).row(0, named=True)
    assert dfl_row["evaluation_payload"]["source_forecast_model_name"] == SOURCE_MODEL
    assert dfl_row["evaluation_payload"]["not_full_dfl"] is True
    assert dfl_row["evaluation_payload"]["not_market_execution"] is True


def test_dfl_forecast_v1_panel_rejects_non_thesis_rows() -> None:
    benchmark = _benchmark_frame(anchor_count=8)
    non_thesis = benchmark.with_columns(
        pl.Series(
            "evaluation_payload",
            [
                {**payload, "data_quality_tier": "demo_grade"}
                for payload in benchmark.select("evaluation_payload").to_series().to_list()
            ],
        )
    )

    try:
        build_dfl_forecast_dfl_v1_panel_frame(
            non_thesis,
            tenant_ids=(TENANT_ID,),
            forecast_model_names=(SOURCE_MODEL,),
            final_validation_anchor_count_per_tenant=2,
        )
    except ValueError as exc:
        assert "thesis_grade" in str(exc)
    else:
        raise AssertionError("expected non-thesis rows to fail")


def test_dfl_forecast_v1_panel_marks_relaxed_solver_fallback(monkeypatch) -> None:
    def failing_score_examples(**kwargs: object) -> object:
        raise RuntimeError("synthetic relaxed solver failure")

    monkeypatch.setattr(forecast_dfl_v1, "_score_examples", failing_score_examples)

    result = build_dfl_forecast_dfl_v1_panel_frame(
        _benchmark_frame(anchor_count=8),
        tenant_ids=(TENANT_ID,),
        forecast_model_names=(SOURCE_MODEL,),
        final_validation_anchor_count_per_tenant=2,
        max_train_anchors_per_tenant=6,
        inner_validation_fraction=0.33,
        epoch_count=1,
        learning_rate=10.0,
    )

    row = result.row(0, named=True)
    assert row["relaxed_solver_status"].startswith("fallback")
    assert row["dfl_v1_checkpoint_epoch"] == 0
    assert row["dfl_v1_inner_selection_relaxed_regret_uah"] >= 1_000_000_000.0


def test_dfl_forecast_v1_panel_uses_bounded_surrogate_without_catastrophic_fallback(monkeypatch) -> None:
    def failing_layer(*args: object, **kwargs: object) -> object:
        raise RuntimeError("synthetic cvxpylayer failure")

    monkeypatch.setattr(relaxed_dispatch, "_relaxed_dispatch_layer", failing_layer)

    result = build_dfl_forecast_dfl_v1_panel_frame(
        _benchmark_frame(anchor_count=8),
        tenant_ids=(TENANT_ID,),
        forecast_model_names=(SOURCE_MODEL,),
        final_validation_anchor_count_per_tenant=2,
        max_train_anchors_per_tenant=6,
        inner_validation_fraction=0.33,
        epoch_count=1,
        learning_rate=10.0,
    )

    row = result.row(0, named=True)
    assert "surrogate_bounded" in row["relaxed_solver_status"]
    assert "fallback" not in row["relaxed_solver_status"]
    assert row["dfl_v1_inner_selection_relaxed_regret_uah"] < 1_000_000_000.0


def test_dfl_forecast_v1_panel_guards_non_finite_training_loss(monkeypatch) -> None:
    def non_finite_loss(**kwargs: object) -> DecisionLossResult:
        value = torch.tensor(float("nan"), dtype=torch.float64, requires_grad=True)
        return DecisionLossResult(
            total_loss=value,
            relaxed_realized_value_uah=value,
            relaxed_regret_uah=value,
            spread_shape_loss=value,
            rank_shape_loss=value,
            mae_loss=value,
            throughput_regularizer=value,
        )

    monkeypatch.setattr(forecast_dfl_v1, "compute_decision_loss_v1", non_finite_loss)

    result = build_dfl_forecast_dfl_v1_panel_frame(
        _benchmark_frame(anchor_count=8),
        tenant_ids=(TENANT_ID,),
        forecast_model_names=(SOURCE_MODEL,),
        final_validation_anchor_count_per_tenant=2,
        max_train_anchors_per_tenant=6,
        inner_validation_fraction=0.33,
        epoch_count=1,
        learning_rate=10.0,
    )

    row = result.row(0, named=True)
    assert "training_guard:non_finite_loss" in row["relaxed_solver_status"]
    assert "fallback:score:non_finite" not in row["relaxed_solver_status"]
    assert row["dfl_v1_checkpoint_epoch"] == 0
    assert row["dfl_v1_inner_selection_relaxed_regret_uah"] < 1_000_000_000.0


def _benchmark_frame(
    *,
    anchor_count: int,
    final_actual_prices: tuple[float, float] = (1000.0, 5000.0),
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for anchor_index in range(anchor_count):
        anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
        actual_prices = final_actual_prices if anchor_index >= anchor_count - 2 else (1000.0, 5000.0)
        rows.append(
            _benchmark_row(
                anchor=anchor,
                model_name="strict_similar_day",
                forecast_prices=(1000.0, 5000.0),
                actual_prices=actual_prices,
            )
        )
        rows.append(
            _benchmark_row(
                anchor=anchor,
                model_name=SOURCE_MODEL,
                forecast_prices=(5000.0, 1000.0),
                actual_prices=actual_prices,
            )
        )
    return pl.DataFrame(rows)


def _benchmark_row(
    *,
    anchor: datetime,
    model_name: str,
    forecast_prices: tuple[float, float],
    actual_prices: tuple[float, float],
) -> dict[str, object]:
    return {
        "evaluation_id": f"{TENANT_ID}:{model_name}:{anchor.isoformat()}",
        "tenant_id": TENANT_ID,
        "forecast_model_name": model_name,
        "strategy_kind": "real_data_rolling_origin_benchmark",
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "horizon_hours": 2,
        "starting_soc_fraction": 0.5,
        "starting_soc_source": "tenant_default",
        "decision_value_uah": 0.0,
        "forecast_objective_value_uah": 0.0,
        "oracle_value_uah": 0.0,
        "regret_uah": 0.0,
        "regret_ratio": 0.0,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": 0.0,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "evaluation_payload": {
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "not_full_dfl": True,
            "not_market_execution": True,
            "horizon": [
                {
                    "step_index": step_index,
                    "interval_start": (anchor + timedelta(hours=step_index + 1)).isoformat(),
                    "forecast_price_uah_mwh": forecast_prices[step_index],
                    "actual_price_uah_mwh": actual_prices[step_index],
                    "net_power_mw": 0.0,
                    "degradation_penalty_uah": 0.0,
                }
                for step_index in range(2)
            ],
        },
    }
