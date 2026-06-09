from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest

from smart_arbitrage.dfl.poland_lag24_experimental_export import (
    build_poland_lag24_experimental_schedule_value_packet,
    write_poland_lag24_experimental_schedule_value_packet,
)


def test_poland_lag24_packet_exports_near_miss_negative_evidence(tmp_path) -> None:
    comparison_frame = _comparison_frame()
    raw_strict_frame = _raw_strict_frame()

    packet = build_poland_lag24_experimental_schedule_value_packet(
        run_slug="week3_poland_lag24_experimental_schedule_value",
        comparison_frame=comparison_frame,
        raw_strict_frame=raw_strict_frame,
        dagster_run_id="run-polish-lag24",
    )
    export_dir = write_poland_lag24_experimental_schedule_value_packet(
        packet,
        output_root=tmp_path,
        comparison_frame=comparison_frame,
        raw_strict_frame=raw_strict_frame,
    )

    assert packet["gate"]["promotes_over_frozen_v2_plus"] is False
    assert packet["gate"]["blocker"] == "mean_not_improved_vs_frozen_v2_plus"
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert packet["claim_boundary"]["offline_strategy_promotion_only"] is True
    assert packet["summary"]["best_experimental_model_name"] == (
        "dfl_schedule_value_learner_v2_plus_"
        "nbeatsx_official_global_panel_poland_lag24_experimental_v1"
    )
    assert packet["summary"]["best_experimental_mean_regret_uah"] == pytest.approx(
        184.66,
        abs=0.01,
    )
    assert packet["summary"]["mean_regret_delta_vs_frozen_v2_plus_uah"] == pytest.approx(
        9.89,
        abs=0.01,
    )
    assert (export_dir / "poland_lag24_experimental_schedule_value_summary.json").exists()
    assert (export_dir / "poland_lag24_experimental_schedule_value_summary.md").exists()
    assert (export_dir / "poland_lag24_experimental_schedule_value_comparison.csv").exists()
    assert (export_dir / "poland_lag24_experimental_raw_strict_rows.csv").exists()
    markdown = (
        export_dir / "poland_lag24_experimental_schedule_value_summary.md"
    ).read_text(encoding="utf-8")
    assert "near-miss negative evidence" in markdown
    assert "market_execution_enabled=false" in markdown


def test_poland_lag24_packet_refuses_market_execution_claim() -> None:
    comparison_frame = _comparison_frame().with_columns(
        pl.lit(True).alias("market_execution_enabled")
    )

    with pytest.raises(ValueError, match="market execution"):
        build_poland_lag24_experimental_schedule_value_packet(
            run_slug="invalid",
            comparison_frame=comparison_frame,
        )


def test_poland_lag24_packet_refuses_missing_frozen_comparator() -> None:
    comparison_frame = _comparison_frame().filter(
        pl.col("comparison_group") != "frozen_ukrainian_v2_plus"
    )

    with pytest.raises(ValueError, match="frozen Ukrainian-only V2\\+"):
        build_poland_lag24_experimental_schedule_value_packet(
            run_slug="invalid",
            comparison_frame=comparison_frame,
        )


def _comparison_frame() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "comparison_group": "frozen_ukrainian_v2_plus",
                "forecast_model_name": (
                    "dfl_schedule_value_learner_v2_plus_"
                    "nbeatsx_official_global_panel_horizon_calibrated_v1"
                ),
                "row_count": 90,
                "tenant_count": 5,
                "anchor_count": 18,
                "mean_regret_uah": 174.768398,
                "median_regret_uah": 67.300273,
                "best_frozen_v2_plus_model_name": (
                    "dfl_schedule_value_learner_v2_plus_"
                    "nbeatsx_official_global_panel_horizon_calibrated_v1"
                ),
                "best_frozen_v2_plus_mean_regret_uah": 174.768398,
                "mean_regret_delta_vs_best_frozen_v2_plus_uah": 0.0,
                "mean_regret_improvement_ratio_vs_best_frozen_v2_plus": 0.0,
                "market_execution_enabled": False,
                "not_full_dfl": True,
                "not_market_execution": True,
            },
            {
                "comparison_group": "poland_lag24_experimental",
                "forecast_model_name": (
                    "dfl_schedule_value_learner_v2_plus_"
                    "nbeatsx_official_global_panel_poland_lag24_experimental_v1"
                ),
                "row_count": 90,
                "tenant_count": 5,
                "anchor_count": 18,
                "mean_regret_uah": 184.663136,
                "median_regret_uah": 65.164745,
                "best_frozen_v2_plus_model_name": (
                    "dfl_schedule_value_learner_v2_plus_"
                    "nbeatsx_official_global_panel_horizon_calibrated_v1"
                ),
                "best_frozen_v2_plus_mean_regret_uah": 174.768398,
                "mean_regret_delta_vs_best_frozen_v2_plus_uah": 9.894738,
                "mean_regret_improvement_ratio_vs_best_frozen_v2_plus": -0.056617,
                "market_execution_enabled": False,
                "not_full_dfl": True,
                "not_market_execution": True,
            },
            {
                "comparison_group": "poland_lag24_experimental",
                "forecast_model_name": (
                    "dfl_schedule_value_learner_v2_plus_"
                    "tft_official_global_panel_poland_lag24_experimental_v1"
                ),
                "row_count": 90,
                "tenant_count": 5,
                "anchor_count": 18,
                "mean_regret_uah": 218.123023,
                "median_regret_uah": 105.504734,
                "best_frozen_v2_plus_model_name": (
                    "dfl_schedule_value_learner_v2_plus_"
                    "nbeatsx_official_global_panel_horizon_calibrated_v1"
                ),
                "best_frozen_v2_plus_mean_regret_uah": 174.768398,
                "mean_regret_delta_vs_best_frozen_v2_plus_uah": 43.354625,
                "mean_regret_improvement_ratio_vs_best_frozen_v2_plus": -0.248065,
                "market_execution_enabled": False,
                "not_full_dfl": True,
                "not_market_execution": True,
            },
        ]
    )


def _raw_strict_frame() -> pl.DataFrame:
    generated_at = datetime(2026, 5, 20, 12, 10, 6)
    rows: list[dict[str, object]] = []
    raw_regrets = {
        "nbeatsx_official_global_panel_poland_lag24_experimental_v1": 751.841333,
        "tft_official_global_panel_poland_lag24_experimental_v1": 2621.633670,
        "strict_similar_day": 310.582808,
    }
    for model_name, regret in raw_regrets.items():
        for index in range(2):
            rows.append(
                {
                    "tenant_id": f"tenant-{index}",
                    "forecast_model_name": model_name,
                    "strategy_kind": (
                        "official_global_panel_poland_lag24_experimental_"
                        "rolling_strict_lp_benchmark"
                    ),
                    "anchor_timestamp": generated_at,
                    "generated_at": generated_at,
                    "regret_uah": regret,
                    "market_execution_enabled": False,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows)
