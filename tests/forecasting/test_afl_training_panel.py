from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.forecasting.afl import (
    build_afl_training_panel_frame,
    build_forecast_candidate_forensics_frame,
)


def _benchmark_frame(*, tenant_count: int = 1, anchor_count: int = 4) -> pl.DataFrame:
    first_anchor = datetime(2026, 4, 1, 23)
    tenant_ids = [f"client_{index:03d}" for index in range(tenant_count)]
    rows: list[dict[str, object]] = []
    for tenant_id in tenant_ids:
        for anchor_index in range(anchor_count):
            anchor = first_anchor + timedelta(days=anchor_index)
            for model_name, regret_offset in [
                ("strict_similar_day", 100.0),
                ("nbeatsx_silver_v0", 130.0),
                ("tft_silver_v0", 120.0),
            ]:
                regret = regret_offset + anchor_index
                rows.append(
                    {
                        "tenant_id": tenant_id,
                        "forecast_model_name": model_name,
                        "strategy_kind": "real_data_rolling_origin_benchmark",
                        "market_venue": "DAM",
                        "anchor_timestamp": anchor,
                        "generated_at": datetime(2026, 5, 8, 12),
                        "horizon_hours": 3,
                        "decision_value_uah": 1000.0 - regret,
                        "oracle_value_uah": 1000.0,
                        "regret_uah": regret,
                        "regret_ratio": regret / 1000.0,
                        "total_degradation_penalty_uah": 5.0,
                        "total_throughput_mwh": 0.1,
                        "evaluation_payload": {
                            "data_quality_tier": "thesis_grade",
                            "observed_coverage_ratio": 1.0,
                            "forecast_diagnostics": {
                                "mae_uah_mwh": regret / 2.0,
                                "rmse_uah_mwh": regret / 1.5,
                                "smape": 0.1,
                                "directional_accuracy": 0.66,
                                "spread_ranking_quality": 0.5,
                                "top_k_price_recall": 0.33,
                            },
                            "horizon": [
                                {
                                    "step_index": 0,
                                    "interval_start": (anchor + timedelta(hours=1)).isoformat(),
                                    "forecast_price_uah_mwh": 1000.0,
                                    "actual_price_uah_mwh": 1050.0,
                                    "net_power_mw": 0.0,
                                    "degradation_penalty_uah": 0.0,
                                },
                                {
                                    "step_index": 1,
                                    "interval_start": (anchor + timedelta(hours=2)).isoformat(),
                                    "forecast_price_uah_mwh": 1500.0,
                                    "actual_price_uah_mwh": 1700.0,
                                    "net_power_mw": 0.2,
                                    "degradation_penalty_uah": 2.0,
                                },
                                {
                                    "step_index": 2,
                                    "interval_start": (anchor + timedelta(hours=3)).isoformat(),
                                    "forecast_price_uah_mwh": 900.0,
                                    "actual_price_uah_mwh": 800.0,
                                    "net_power_mw": -0.2,
                                    "degradation_penalty_uah": 3.0,
                                },
                            ],
                        },
                    }
                )
    return pl.DataFrame(rows)


def test_forecast_candidate_forensics_labels_compact_silver_candidates() -> None:
    forensics = build_forecast_candidate_forensics_frame(_benchmark_frame())

    rows = {
        row["forecast_model_name"]: row
        for row in forensics.iter_rows(named=True)
    }

    assert rows["strict_similar_day"]["candidate_kind"] == "frozen_control_comparator"
    assert rows["nbeatsx_silver_v0"]["candidate_kind"] == "compact_silver_candidate"
    assert rows["tft_silver_v0"]["candidate_kind"] == "compact_silver_candidate"
    assert rows["nbeatsx_silver_v0"]["model_family"] == "NBEATSx"
    assert rows["tft_silver_v0"]["model_family"] == "TFT"
    assert rows["tft_silver_v0"]["not_full_dfl"] is True
    assert rows["tft_silver_v0"]["not_market_execution"] is True


def test_afl_training_panel_splits_latest_anchors_and_uses_prior_features_only() -> None:
    panel = build_afl_training_panel_frame(
        _benchmark_frame(),
        final_holdout_anchor_count_per_tenant=1,
    )

    assert panel.height == 12
    assert panel.filter(pl.col("split") == "final_holdout").height == 3
    assert panel.filter(pl.col("split") == "train_selection").height == 9
    assert panel.select("claim_scope").to_series().unique().to_list() == [
        "arbitrage_focused_learning_panel_not_full_dfl"
    ]

    tft_final = panel.filter(
        (pl.col("forecast_model_name") == "tft_silver_v0")
        & (pl.col("split") == "final_holdout")
    ).row(0, named=True)
    assert tft_final["feature_prior_model_anchor_count"] == 3
    assert tft_final["feature_prior_mean_model_regret_uah"] == pytest.approx(121.0)
    assert tft_final["feature_prior_mean_strict_regret_uah"] == pytest.approx(101.0)
    assert tft_final["label_regret_uah"] == pytest.approx(123.0)
    assert tft_final["label_actual_price_spread_uah_mwh"] == pytest.approx(900.0)
    assert "feature_forecast_top3_bottom3_rank_overlap" not in panel.columns
    assert "diagnostic_forecast_top3_bottom3_rank_overlap" in panel.columns
    assert tft_final["label_decision_weight_uah"] > 0.0


def test_afl_training_panel_final_holdout_actual_mutation_does_not_change_features() -> None:
    base = _benchmark_frame()
    latest_anchor = base.select("anchor_timestamp").to_series().max()
    mutated_rows: list[dict[str, object]] = []
    for row in base.iter_rows(named=True):
        mutated_row = dict(row)
        if mutated_row["anchor_timestamp"] == latest_anchor:
            mutated_row["regret_uah"] = float(mutated_row["regret_uah"]) + 5000.0
            mutated_row["evaluation_payload"] = _mutate_actual_horizon_prices(
                mutated_row["evaluation_payload"]
            )
        mutated_rows.append(mutated_row)
    mutated = pl.DataFrame(mutated_rows)

    base_panel = build_afl_training_panel_frame(base, final_holdout_anchor_count_per_tenant=1)
    mutated_panel = build_afl_training_panel_frame(mutated, final_holdout_anchor_count_per_tenant=1)
    feature_columns = [
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "split",
        *sorted(column for column in base_panel.columns if column.startswith("feature_")),
    ]

    assert base_panel.select(feature_columns).equals(mutated_panel.select(feature_columns))
    assert not base_panel.select("label_regret_uah").equals(mutated_panel.select("label_regret_uah"))
    assert not base_panel.select("label_actual_price_spread_uah_mwh").equals(
        mutated_panel.select("label_actual_price_spread_uah_mwh")
    )


def test_afl_training_panel_adds_prior_weather_load_context_without_future_leakage() -> None:
    benchmark = _benchmark_frame(anchor_count=4)
    latest_anchor = benchmark.select("anchor_timestamp").to_series().max()
    assert isinstance(latest_anchor, datetime)

    def utc_timestamp(offset_hours: int) -> datetime:
        return (latest_anchor + timedelta(hours=offset_hours)).replace(tzinfo=UTC)

    context_rows = [
        {
            "tenant_id": "client_000",
            "timestamp": utc_timestamp(-2),
            "weather_temperature": 10.0,
            "weather_wind_speed": 4.0,
            "weather_cloudcover": 30.0,
            "weather_effective_solar": 100.0,
        },
        {
            "tenant_id": "client_000",
            "timestamp": utc_timestamp(-1),
            "weather_temperature": 14.0,
            "weather_wind_speed": 6.0,
            "weather_cloudcover": 50.0,
            "weather_effective_solar": 300.0,
        },
        {
            "tenant_id": "client_000",
            "timestamp": utc_timestamp(1),
            "weather_temperature": 1000.0,
            "weather_wind_speed": 1000.0,
            "weather_cloudcover": 1000.0,
            "weather_effective_solar": 1000.0,
        },
    ]
    weather_context = pl.DataFrame(context_rows)
    load_context = pl.DataFrame(
        [
            {
                "tenant_id": "client_000",
                "timestamp": utc_timestamp(-2),
                "load_mw": 0.5,
                "pv_estimate_mw": 0.1,
                "net_load_mw": 0.4,
                "btm_battery_power_mw": 0.04,
                "claim_scope": "tenant_historical_net_load_configured_proxy",
                "not_full_dfl": True,
                "not_market_execution": True,
            },
            {
                "tenant_id": "client_000",
                "timestamp": utc_timestamp(-1),
                "load_mw": 0.7,
                "pv_estimate_mw": 0.2,
                "net_load_mw": 0.5,
                "btm_battery_power_mw": 0.05,
                "claim_scope": "tenant_historical_net_load_configured_proxy",
                "not_full_dfl": True,
                "not_market_execution": True,
            },
            {
                "tenant_id": "client_000",
                "timestamp": utc_timestamp(1),
                "load_mw": 50.0,
                "pv_estimate_mw": 50.0,
                "net_load_mw": 50.0,
                "btm_battery_power_mw": 50.0,
                "claim_scope": "tenant_historical_net_load_configured_proxy",
                "not_full_dfl": True,
                "not_market_execution": True,
            },
        ]
    )

    panel = build_afl_training_panel_frame(
        benchmark,
        final_holdout_anchor_count_per_tenant=1,
        weather_context_frame=weather_context,
        tenant_historical_net_load_frame=load_context,
    )

    final_row = panel.filter(
        (pl.col("forecast_model_name") == "tft_silver_v0")
        & (pl.col("split") == "final_holdout")
    ).row(0, named=True)
    assert final_row["feature_prior_weather_context_row_count"] == 2
    assert final_row["feature_prior_weather_temperature_mean"] == pytest.approx(12.0)
    assert final_row["feature_prior_weather_wind_speed_mean"] == pytest.approx(5.0)
    assert final_row["feature_prior_net_load_context_row_count"] == 2
    assert final_row["feature_prior_net_load_mean_mw"] == pytest.approx(0.45)
    assert final_row["feature_prior_load_context_configured_proxy"] == pytest.approx(1.0)


def test_afl_training_panel_rejects_non_thesis_rows() -> None:
    bad = _benchmark_frame().with_columns(
        pl.col("evaluation_payload").map_elements(
            lambda payload: {**payload, "data_quality_tier": "demo_grade"},
            return_dtype=pl.Object,
        )
    )

    with pytest.raises(ValueError, match="thesis_grade"):
        build_afl_training_panel_frame(bad)


def _mutate_actual_horizon_prices(payload: object) -> object:
    if not isinstance(payload, dict):
        return payload
    horizon = payload.get("horizon")
    if not isinstance(horizon, list):
        return payload
    mutated_horizon = []
    for index, item in enumerate(horizon):
        if not isinstance(item, dict):
            mutated_horizon.append(item)
            continue
        mutated_item = dict(item)
        mutated_item["actual_price_uah_mwh"] = 5000.0 - (index * 1000.0)
        mutated_horizon.append(mutated_item)
    return {**payload, "horizon": mutated_horizon}
