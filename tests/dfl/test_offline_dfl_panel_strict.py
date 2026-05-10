from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.dfl.panel_strict import (
    OFFLINE_DFL_PANEL_STRICT_LP_STRATEGY_KIND,
    build_offline_dfl_panel_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.promotion_gate import evaluate_offline_dfl_panel_strict_promotion_gate

CANONICAL_TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
SOURCE_MODEL = "tft_silver_v0"
V2_MODEL = f"offline_dfl_panel_v2_{SOURCE_MODEL}"
FIRST_ANCHOR = datetime(2026, 1, 1, 23)
GENERATED_AT = datetime(2026, 5, 7, 10)


def test_strict_panel_builder_emits_final_holdout_rows_for_all_tenants() -> None:
    benchmark = _benchmark_frame(anchor_count_per_tenant=20)
    panel = _panel_frame(anchor_count_per_tenant=20)

    result = build_offline_dfl_panel_strict_lp_benchmark_frame(
        benchmark,
        panel,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=(SOURCE_MODEL,),
        final_validation_anchor_count_per_tenant=18,
        generated_at=GENERATED_AT,
    )

    assert result.height == 5 * 18 * 3
    assert result.select("tenant_id").n_unique() == 5
    assert result.filter(pl.col("forecast_model_name") == V2_MODEL).height == 90
    assert result.select("strategy_kind").to_series().unique().to_list() == [
        OFFLINE_DFL_PANEL_STRICT_LP_STRATEGY_KIND
    ]
    assert set(result.select("forecast_model_name").to_series().unique().to_list()) == {
        "strict_similar_day",
        SOURCE_MODEL,
        V2_MODEL,
    }

    v2_row = result.filter(pl.col("forecast_model_name") == V2_MODEL).row(0, named=True)
    payload = v2_row["evaluation_payload"]
    assert payload["source_forecast_model_name"] == SOURCE_MODEL
    assert payload["v2_checkpoint_horizon_biases_uah_mwh"] == [-4000.0, 4000.0]
    assert [point["forecast_price_uah_mwh"] for point in payload["horizon"]] == [1000.0, 5000.0]


def test_strict_panel_builder_keeps_checkpoint_metadata_fixed_when_actuals_change() -> None:
    panel = _panel_frame(anchor_count_per_tenant=20)
    base = build_offline_dfl_panel_strict_lp_benchmark_frame(
        _benchmark_frame(anchor_count_per_tenant=20, final_actual_prices=(1000.0, 5000.0)),
        panel,
        tenant_ids=CANONICAL_TENANTS[:1],
        forecast_model_names=(SOURCE_MODEL,),
        final_validation_anchor_count_per_tenant=18,
        generated_at=GENERATED_AT,
    )
    mutated = build_offline_dfl_panel_strict_lp_benchmark_frame(
        _benchmark_frame(anchor_count_per_tenant=20, final_actual_prices=(5000.0, 1000.0)),
        panel,
        tenant_ids=CANONICAL_TENANTS[:1],
        forecast_model_names=(SOURCE_MODEL,),
        final_validation_anchor_count_per_tenant=18,
        generated_at=GENERATED_AT,
    )

    base_v2 = base.filter(pl.col("forecast_model_name") == V2_MODEL)
    mutated_v2 = mutated.filter(pl.col("forecast_model_name") == V2_MODEL)
    base_payload = base_v2.row(0, named=True)["evaluation_payload"]
    mutated_payload = mutated_v2.row(0, named=True)["evaluation_payload"]

    assert base_payload["v2_checkpoint_epoch"] == mutated_payload["v2_checkpoint_epoch"]
    assert (
        base_payload["v2_checkpoint_horizon_biases_uah_mwh"]
        == mutated_payload["v2_checkpoint_horizon_biases_uah_mwh"]
    )
    assert base_v2.select(pl.sum("regret_uah")).item() != mutated_v2.select(pl.sum("regret_uah")).item()


def test_strict_panel_builder_fails_when_panel_or_holdout_rows_are_missing() -> None:
    benchmark = _benchmark_frame(anchor_count_per_tenant=20)
    panel = _panel_frame(anchor_count_per_tenant=20)

    with pytest.raises(ValueError, match="missing offline DFL panel row"):
        build_offline_dfl_panel_strict_lp_benchmark_frame(
            benchmark,
            panel.filter(pl.col("tenant_id") != CANONICAL_TENANTS[0]),
            tenant_ids=CANONICAL_TENANTS[:1],
            forecast_model_names=(SOURCE_MODEL,),
            final_validation_anchor_count_per_tenant=18,
        )

    missing_anchor = FIRST_ANCHOR + timedelta(days=19)
    incomplete_benchmark = benchmark.filter(
        ~(
            (pl.col("tenant_id") == CANONICAL_TENANTS[0])
            & (pl.col("forecast_model_name") == SOURCE_MODEL)
            & (pl.col("anchor_timestamp") == missing_anchor)
        )
    )
    with pytest.raises(ValueError, match="missing final-holdout source rows"):
        build_offline_dfl_panel_strict_lp_benchmark_frame(
            incomplete_benchmark,
            panel,
            tenant_ids=CANONICAL_TENANTS[:1],
            forecast_model_names=(SOURCE_MODEL,),
            final_validation_anchor_count_per_tenant=18,
        )


def test_strict_panel_builder_rejects_mismatched_bias_lengths() -> None:
    benchmark = _benchmark_frame(anchor_count_per_tenant=20)
    panel = _panel_frame(anchor_count_per_tenant=20).with_columns(
        pl.Series("v2_checkpoint_horizon_biases_uah_mwh", [[-200.0] for _ in CANONICAL_TENANTS])
    )

    with pytest.raises(ValueError, match="bias length"):
        build_offline_dfl_panel_strict_lp_benchmark_frame(
            benchmark,
            panel,
            tenant_ids=CANONICAL_TENANTS[:1],
            forecast_model_names=(SOURCE_MODEL,),
            final_validation_anchor_count_per_tenant=18,
        )


def test_strict_panel_promotion_gate_passes_only_when_v2_beats_strict_control() -> None:
    result = evaluate_offline_dfl_panel_strict_promotion_gate(_strict_gate_frame(v2_regret_uah=90.0))

    assert result.passed is True
    assert result.decision == "promote"
    assert result.metrics["best_source_model_name"] == SOURCE_MODEL
    assert result.metrics["mean_regret_improvement_ratio_vs_strict"] == 0.1


def test_strict_panel_promotion_gate_blocks_under_90_or_non_thesis_evidence() -> None:
    under_90 = evaluate_offline_dfl_panel_strict_promotion_gate(
        _strict_gate_frame(anchor_count_per_tenant=17, v2_regret_uah=90.0)
    )
    non_thesis = evaluate_offline_dfl_panel_strict_promotion_gate(
        _strict_gate_frame(v2_regret_uah=90.0, data_quality_tier="demo_grade")
    )

    assert under_90.passed is False
    assert "validation tenant-anchor count" in under_90.description
    assert non_thesis.passed is False
    assert "thesis_grade" in non_thesis.description


def test_strict_panel_promotion_gate_blocks_median_degradation_and_weak_mean() -> None:
    worse_median = evaluate_offline_dfl_panel_strict_promotion_gate(
        _strict_gate_frame(v2_regrets=([0.0] * 20) + ([101.0] * 70))
    )
    weak_mean = evaluate_offline_dfl_panel_strict_promotion_gate(
        _strict_gate_frame(v2_regret_uah=97.0)
    )

    assert worse_median.passed is False
    assert "median regret" in worse_median.description
    assert weak_mean.passed is False
    assert "mean regret improvement" in weak_mean.description


def _benchmark_frame(
    *,
    anchor_count_per_tenant: int,
    final_actual_prices: tuple[float, float] = (1000.0, 5000.0),
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in CANONICAL_TENANTS:
        for anchor_index in range(anchor_count_per_tenant):
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            actual_prices = final_actual_prices if anchor_index >= anchor_count_per_tenant - 18 else (1500.0, 2500.0)
            rows.append(
                _benchmark_row(
                    tenant_id=tenant_id,
                    anchor=anchor,
                    model_name="strict_similar_day",
                    forecast_prices=(1000.0, 5000.0),
                    actual_prices=actual_prices,
                )
            )
            rows.append(
                _benchmark_row(
                    tenant_id=tenant_id,
                    anchor=anchor,
                    model_name=SOURCE_MODEL,
                    forecast_prices=(5000.0, 1000.0),
                    actual_prices=actual_prices,
                )
            )
    return pl.DataFrame(rows)


def _benchmark_row(
    *,
    tenant_id: str,
    anchor: datetime,
    model_name: str,
    forecast_prices: tuple[float, float],
    actual_prices: tuple[float, float],
) -> dict[str, object]:
    return {
        "evaluation_id": f"{tenant_id}:{model_name}:{anchor.isoformat()}",
        "tenant_id": tenant_id,
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


def _panel_frame(*, anchor_count_per_tenant: int) -> pl.DataFrame:
    first_holdout_anchor = FIRST_ANCHOR + timedelta(days=anchor_count_per_tenant - 18)
    last_holdout_anchor = FIRST_ANCHOR + timedelta(days=anchor_count_per_tenant - 1)
    return pl.DataFrame(
        [
            {
                "experiment_name": "offline_horizon_bias_panel_dfl_v2",
                "tenant_id": tenant_id,
                "forecast_model_name": SOURCE_MODEL,
                "fit_anchor_count": 1,
                "inner_selection_anchor_count": 1,
                "final_validation_anchor_count": 18,
                "horizon_hours": 2,
                "epoch_count": 2,
                "learning_rate": 10.0,
                "last_fit_anchor_timestamp": first_holdout_anchor - timedelta(days=2),
                "first_inner_selection_anchor_timestamp": first_holdout_anchor - timedelta(days=1),
                "last_inner_selection_anchor_timestamp": first_holdout_anchor - timedelta(days=1),
                "first_final_holdout_anchor_timestamp": first_holdout_anchor,
                "last_final_holdout_anchor_timestamp": last_holdout_anchor,
                "v2_checkpoint_horizon_biases_uah_mwh": [-4000.0, 4000.0],
                "v2_checkpoint_epoch": 1,
                "v2_inner_selection_relaxed_regret_uah": 10.0,
                "data_quality_tier": "thesis_grade",
                "observed_coverage_ratio": 1.0,
                "claim_scope": "offline_dfl_panel_experiment_not_full_dfl",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
            for tenant_id in CANONICAL_TENANTS
        ]
    )


def _strict_gate_frame(
    *,
    anchor_count_per_tenant: int = 18,
    v2_regret_uah: float | None = None,
    v2_regrets: list[float] | None = None,
    data_quality_tier: str = "thesis_grade",
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    tenant_anchors = [
        (tenant_id, FIRST_ANCHOR + timedelta(days=anchor_index))
        for tenant_id in CANONICAL_TENANTS
        for anchor_index in range(anchor_count_per_tenant)
    ]
    resolved_v2_regrets = v2_regrets or [float(v2_regret_uah if v2_regret_uah is not None else 90.0)] * len(
        tenant_anchors
    )
    for index, (tenant_id, anchor) in enumerate(tenant_anchors):
        rows.append(_strict_gate_row(tenant_id=tenant_id, anchor=anchor, model_name="strict_similar_day", regret=100.0))
        rows.append(_strict_gate_row(tenant_id=tenant_id, anchor=anchor, model_name=SOURCE_MODEL, regret=120.0))
        rows.append(
            _strict_gate_row(
                tenant_id=tenant_id,
                anchor=anchor,
                model_name=V2_MODEL,
                regret=resolved_v2_regrets[index],
                data_quality_tier=data_quality_tier,
            )
        )
    return pl.DataFrame(rows)


def _strict_gate_row(
    *,
    tenant_id: str,
    anchor: datetime,
    model_name: str,
    regret: float,
    data_quality_tier: str = "thesis_grade",
) -> dict[str, object]:
    return {
        "tenant_id": tenant_id,
        "forecast_model_name": model_name,
        "strategy_kind": OFFLINE_DFL_PANEL_STRICT_LP_STRATEGY_KIND,
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "regret_uah": regret,
        "evaluation_payload": {
            "data_quality_tier": data_quality_tier,
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": 0,
            "not_full_dfl": True,
            "not_market_execution": True,
            "source_forecast_model_name": SOURCE_MODEL,
        },
    }
