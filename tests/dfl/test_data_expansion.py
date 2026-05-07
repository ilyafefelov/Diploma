from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.dfl.data_expansion import (
    build_dfl_action_label_panel_frame,
    build_dfl_data_coverage_audit_frame,
    european_dataset_bridge_registry_frame,
)

TENANTS: tuple[str, ...] = ("client_003_dnipro_factory", "client_004_kharkiv_hospital")
SOURCE_MODEL = "tft_silver_v0"
FIRST_TIMESTAMP = datetime(2026, 1, 1)
GENERATED_AT = datetime(2026, 5, 7, 10)


def test_data_coverage_audit_counts_tenant_specific_eligible_anchors() -> None:
    feature_frame = _feature_frame(
        tenant_ids=TENANTS,
        hour_count=217,
        missing_by_tenant={"client_004_kharkiv_hospital": {FIRST_TIMESTAMP + timedelta(hours=192)}},
    )

    result = build_dfl_data_coverage_audit_frame(
        feature_frame,
        tenant_ids=TENANTS,
        target_anchor_count_per_tenant=2,
        required_past_hours=168,
        horizon_hours=24,
    )

    dnipro = result.filter(pl.col("tenant_id") == "client_003_dnipro_factory").row(0, named=True)
    kharkiv = result.filter(pl.col("tenant_id") == "client_004_kharkiv_hospital").row(0, named=True)

    assert dnipro["eligible_anchor_count"] == 2
    assert dnipro["meets_target_anchor_count"] is True
    assert dnipro["missing_price_hours"] == 0
    assert dnipro["missing_weather_hours"] == 0
    assert dnipro["data_quality_tier"] == "thesis_grade"
    assert kharkiv["eligible_anchor_count"] == 0
    assert kharkiv["meets_target_anchor_count"] is False
    assert kharkiv["missing_price_hours"] == 1
    assert kharkiv["data_quality_tier"] == "coverage_gap"


def test_data_coverage_audit_rejects_non_observed_price_or_weather_rows() -> None:
    synthetic_price = _feature_frame(tenant_ids=TENANTS[:1], hour_count=193).with_columns(
        pl.when(pl.col("timestamp") == FIRST_TIMESTAMP)
        .then(pl.lit("synthetic"))
        .otherwise(pl.col("source_kind"))
        .alias("source_kind")
    )
    synthetic_weather = _feature_frame(tenant_ids=TENANTS[:1], hour_count=193).with_columns(
        pl.when(pl.col("timestamp") == FIRST_TIMESTAMP)
        .then(pl.lit("synthetic"))
        .otherwise(pl.col("weather_source_kind"))
        .alias("weather_source_kind")
    )

    with pytest.raises(ValueError, match="observed OREE"):
        build_dfl_data_coverage_audit_frame(
            synthetic_price,
            tenant_ids=TENANTS[:1],
            target_anchor_count_per_tenant=1,
        )

    with pytest.raises(ValueError, match="observed Open-Meteo"):
        build_dfl_data_coverage_audit_frame(
            synthetic_weather,
            tenant_ids=TENANTS[:1],
            target_anchor_count_per_tenant=1,
        )


def test_action_label_panel_uses_latest_final_holdout_and_oracle_targets() -> None:
    benchmark = _benchmark_frame(tenant_ids=TENANTS[:1], anchor_count=5)

    result = build_dfl_action_label_panel_frame(
        benchmark,
        tenant_ids=TENANTS[:1],
        forecast_model_names=(SOURCE_MODEL,),
        final_holdout_anchor_count_per_tenant=2,
    )

    assert result.height == 5
    assert result.filter(pl.col("split_name") == "final_holdout").height == 2
    assert result.filter(pl.col("split_name") == "train_selection").height == 3

    final_anchors = result.filter(pl.col("split_name") == "final_holdout")["anchor_timestamp"].to_list()
    assert final_anchors == [
        FIRST_TIMESTAMP + timedelta(days=3, hours=23),
        FIRST_TIMESTAMP + timedelta(days=4, hours=23),
    ]

    row = result.row(0, named=True)
    assert row["forecast_model_name"] == SOURCE_MODEL
    assert row["strict_baseline_forecast_model_name"] == "strict_similar_day"
    assert row["target_strategy_name"] == "oracle_lp"
    assert len(row["forecast_price_vector_uah_mwh"]) == row["horizon_hours"]
    assert len(row["actual_price_vector_uah_mwh"]) == row["horizon_hours"]
    assert len(row["oracle_signed_dispatch_vector_mw"]) == row["horizon_hours"]
    assert len(row["oracle_soc_after_mwh_vector"]) == row["horizon_hours"]
    assert len(row["target_charge_mask"]) == row["horizon_hours"]
    assert len(row["target_discharge_mask"]) == row["horizon_hours"]
    assert row["data_quality_tier"] == "thesis_grade"
    assert row["not_full_dfl"] is True
    assert row["not_market_execution"] is True


def test_action_label_panel_rejects_non_thesis_missing_baseline_and_bad_vectors() -> None:
    benchmark = _benchmark_frame(tenant_ids=TENANTS[:1], anchor_count=5)
    non_thesis = benchmark.with_columns(
        pl.Series(
            "evaluation_payload",
            [
                {**payload, "data_quality_tier": "demo_grade"}
                for payload in benchmark["evaluation_payload"].to_list()
            ],
        )
    )
    missing_baseline = benchmark.filter(pl.col("forecast_model_name") != "strict_similar_day")
    payloads = benchmark["evaluation_payload"].to_list()
    payloads[1] = {**payloads[1], "horizon": payloads[1]["horizon"][:1]}
    bad_vector = benchmark.with_columns(pl.Series("evaluation_payload", payloads))

    with pytest.raises(ValueError, match="thesis_grade"):
        build_dfl_action_label_panel_frame(
            non_thesis,
            tenant_ids=TENANTS[:1],
            forecast_model_names=(SOURCE_MODEL,),
            final_holdout_anchor_count_per_tenant=2,
        )
    with pytest.raises(ValueError, match="strict_similar_day"):
        build_dfl_action_label_panel_frame(
            missing_baseline,
            tenant_ids=TENANTS[:1],
            forecast_model_names=(SOURCE_MODEL,),
            final_holdout_anchor_count_per_tenant=2,
        )
    with pytest.raises(ValueError, match="horizon length"):
        build_dfl_action_label_panel_frame(
            bad_vector,
            tenant_ids=TENANTS[:1],
            forecast_model_names=(SOURCE_MODEL,),
            final_holdout_anchor_count_per_tenant=2,
        )


def test_european_dataset_bridge_is_research_only_not_training_input() -> None:
    registry = european_dataset_bridge_registry_frame()

    assert registry.height == 4
    assert set(registry["source_name"].to_list()) == {
        "ENTSO-E Transparency Platform",
        "Open Power System Data",
        "Ember API",
        "Nord Pool Data Portal",
    }
    assert registry["training_use_allowed"].to_list() == [False, False, False, False]
    assert registry["not_ingested"].to_list() == [True, True, True, True]
    assert registry["claim_scope"].unique().to_list() == ["external_validation_roadmap_only"]


def _feature_frame(
    *,
    tenant_ids: tuple[str, ...],
    hour_count: int,
    missing_by_tenant: dict[str, set[datetime]] | None = None,
) -> pl.DataFrame:
    missing = missing_by_tenant or {}
    rows: list[dict[str, object]] = []
    for tenant_id in tenant_ids:
        missing_timestamps = missing.get(tenant_id, set())
        for hour_index in range(hour_count):
            timestamp = FIRST_TIMESTAMP + timedelta(hours=hour_index)
            if timestamp in missing_timestamps:
                continue
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "timestamp": timestamp,
                    "price_uah_mwh": 2000.0 + hour_index,
                    "source_kind": "observed",
                    "weather_source_kind": "observed",
                }
            )
    return pl.DataFrame(rows)


def _benchmark_frame(*, tenant_ids: tuple[str, ...], anchor_count: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in tenant_ids:
        for anchor_index in range(anchor_count):
            anchor = FIRST_TIMESTAMP + timedelta(days=anchor_index, hours=23)
            for model_name, regret, net_power in [
                ("strict_similar_day", 100.0, [0.1, -0.1]),
                (SOURCE_MODEL, 80.0, [0.0, 0.1]),
            ]:
                rows.append(
                    _benchmark_row(
                        tenant_id=tenant_id,
                        anchor=anchor,
                        model_name=model_name,
                        regret=regret,
                        net_power=net_power,
                    )
                )
    return pl.DataFrame(rows)


def _benchmark_row(
    *,
    tenant_id: str,
    anchor: datetime,
    model_name: str,
    regret: float,
    net_power: list[float],
) -> dict[str, object]:
    horizon = [
        {
            "step_index": index,
            "interval_start": (anchor + timedelta(hours=index + 1)).isoformat(),
            "forecast_price_uah_mwh": 1000.0 + index * 300.0,
            "actual_price_uah_mwh": 900.0 + index * 500.0,
            "net_power_mw": net_power[index],
            "degradation_penalty_uah": 2.0 + index,
        }
        for index in range(len(net_power))
    ]
    return {
        "evaluation_id": f"{tenant_id}:{anchor:%Y%m%d}:{model_name}",
        "tenant_id": tenant_id,
        "forecast_model_name": model_name,
        "strategy_kind": "real_data_rolling_origin_benchmark",
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "horizon_hours": len(net_power),
        "starting_soc_fraction": 0.52,
        "starting_soc_source": "tenant_default",
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 3.0,
        "total_throughput_mwh": 0.2,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "evaluation_payload": {
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "horizon": horizon,
        },
    }
