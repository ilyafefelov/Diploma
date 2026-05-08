from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import polars as pl
import pytest

from smart_arbitrage.dfl.strict_failure_features import (
    DFL_STRICT_FAILURE_FEATURE_AUDIT_CLAIM_SCOPE,
    DFL_STRICT_FAILURE_PRIOR_FEATURE_PANEL_CLAIM_SCOPE,
    build_dfl_strict_failure_feature_audit_frame,
    build_dfl_strict_failure_prior_feature_panel_frame,
    validate_dfl_strict_failure_feature_audit_evidence,
)
from smart_arbitrage.dfl.strict_failure_robustness import (
    build_dfl_strict_failure_selector_robustness_frame,
)


CANONICAL_TENANTS = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
SOURCE_MODELS = ("tft_silver_v0", "nbeatsx_silver_v0")
FIRST_ANCHOR = datetime(2026, 1, 1, 23, tzinfo=UTC)


def test_prior_feature_panel_uses_prior_windows_and_keeps_validation_outcomes_as_labels() -> None:
    library = _candidate_library_104()
    robustness = _build_robustness(library)
    panel = _build_feature_panel(library, robustness)

    assert panel.height == 720
    assert panel.select("tenant_id").n_unique() == 5
    assert panel.select("source_model_name").n_unique() == 2
    assert panel.select("window_index").n_unique() == 4
    assert set(panel["claim_scope"]) == {DFL_STRICT_FAILURE_PRIOR_FEATURE_PANEL_CLAIM_SCOPE}
    assert "selector_feature_prior_strict_mean_regret_uah" in panel.columns
    assert "selector_feature_prior_price_spread_std_uah_mwh" in panel.columns
    assert "selector_feature_prior_net_load_mean_mw" in panel.columns
    assert "analysis_only_selected_regret_uah" in panel.columns
    assert "analysis_only_selector_beats_strict" in panel.columns

    latest_anchor = FIRST_ANCHOR + timedelta(days=86)
    latest_row = panel.filter(
        (pl.col("source_model_name") == "tft_silver_v0")
        & (pl.col("tenant_id") == "client_003_dnipro_factory")
        & (pl.col("window_index") == 1)
        & (pl.col("anchor_timestamp") == latest_anchor)
    ).row(0, named=True)
    assert latest_row["prior_cutoff_timestamp"] == latest_anchor
    assert latest_row["selector_feature_prior_anchor_count"] == 86
    assert latest_row["selector_feature_prior_strict_mean_regret_uah"] == pytest.approx(300.0)
    assert latest_row["analysis_only_selected_regret_uah"] == pytest.approx(150.0)


def test_mutating_validation_outcomes_changes_labels_not_prior_features() -> None:
    library = _candidate_library_104()
    mutated = _mutate_latest_challenger_regret(library, regret_uah=700.0)

    panel = _build_feature_panel(library, _build_robustness(library))
    mutated_panel = _build_feature_panel(mutated, _build_robustness(mutated))

    row_filter = (
        (pl.col("source_model_name") == "tft_silver_v0")
        & (pl.col("tenant_id") == "client_003_dnipro_factory")
        & (pl.col("window_index") == 1)
        & (pl.col("anchor_timestamp") == FIRST_ANCHOR + timedelta(days=86))
    )
    base_row = panel.filter(row_filter).row(0, named=True)
    mutated_row = mutated_panel.filter(row_filter).row(0, named=True)
    selector_feature_columns = [
        column for column in panel.columns if column.startswith("selector_feature_")
    ]

    assert {column: base_row[column] for column in selector_feature_columns} == {
        column: mutated_row[column] for column in selector_feature_columns
    }
    assert mutated_row["analysis_only_selected_regret_uah"] > base_row["analysis_only_selected_regret_uah"]
    assert mutated_row["analysis_only_selector_beats_strict"] is False


def test_feature_audit_summarizes_windows_and_validates_claim_boundary() -> None:
    library = _candidate_library_104()
    panel = _build_feature_panel(library, _build_robustness(library))

    audit = build_dfl_strict_failure_feature_audit_frame(panel)
    evidence = validate_dfl_strict_failure_feature_audit_evidence(audit)

    assert audit.height == 40
    assert audit.select("tenant_id").n_unique() == 5
    assert audit.select("source_model_name").n_unique() == 2
    assert audit.select("window_index").n_unique() == 4
    assert set(audit["claim_scope"]) == {DFL_STRICT_FAILURE_FEATURE_AUDIT_CLAIM_SCOPE}
    assert set(audit["not_full_dfl"]) == {True}
    assert set(audit["not_market_execution"]) == {True}
    assert evidence.passed is True
    assert evidence.metadata["row_count"] == 40
    assert evidence.metadata["tenant_count"] == 5
    assert evidence.metadata["source_model_count"] == 2
    assert set(audit["failure_cluster"]).issubset(
        {
            "strict_failure_captured",
            "strict_stable_region",
            "high_spread_volatility",
            "rank_instability",
            "load_weather_stress",
            "tenant_specific_outlier",
        }
    )


@pytest.mark.parametrize(
    ("case_name", "message"),
    [
        ("demo_grade", "thesis_grade"),
        ("not_full_dfl_false", "not_full_dfl"),
        ("safety_violation", "zero safety"),
        ("missing_coverage", "coverage"),
    ],
)
def test_feature_panel_blocks_invalid_evidence(
    case_name: str, message: str
) -> None:
    if case_name == "demo_grade":
        bad_frame = _candidate_library_104(data_quality_tier="demo_grade")
    elif case_name == "not_full_dfl_false":
        bad_frame = _candidate_library_104(not_full_dfl=False)
    elif case_name == "safety_violation":
        bad_frame = _candidate_library_104(safety_violation_count=1)
    else:
        bad_frame = _candidate_library_104().filter(
            ~(
                (pl.col("tenant_id") == "client_005_odesa_hotel")
                & (pl.col("source_model_name") == "nbeatsx_silver_v0")
            )
        )
    with pytest.raises(ValueError, match=message):
        _build_feature_panel(bad_frame, _build_robustness(bad_frame))


def _build_feature_panel(
    library: pl.DataFrame,
    robustness: pl.DataFrame,
) -> pl.DataFrame:
    return build_dfl_strict_failure_prior_feature_panel_frame(
        library,
        robustness,
        _benchmark_feature_frame(),
        _tenant_historical_load_frame(),
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        validation_window_count=4,
        validation_anchor_count=18,
        min_prior_anchors_before_window=30,
        min_prior_anchor_count=3,
    )


def _build_robustness(frame: pl.DataFrame) -> pl.DataFrame:
    return build_dfl_strict_failure_selector_robustness_frame(
        frame,
        tenant_ids=CANONICAL_TENANTS,
        forecast_model_names=SOURCE_MODELS,
        validation_window_count=4,
        validation_anchor_count=18,
        min_prior_anchors_before_window=30,
        min_prior_anchor_count=3,
        min_robust_passing_windows=3,
    )


def _candidate_library_104(
    *,
    data_quality_tier: str = "thesis_grade",
    not_full_dfl: bool = True,
    safety_violation_count: int = 0,
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for tenant_id in CANONICAL_TENANTS:
        for source_model_name in SOURCE_MODELS:
            for anchor_index in range(104):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                split_name = "final_holdout" if anchor_index >= 86 else "train_selection"
                if anchor_index < 32:
                    challenger_regret = 100.0
                elif source_model_name == "tft_silver_v0":
                    challenger_regret = 150.0
                else:
                    challenger_regret = 290.0
                rows.extend(
                    [
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            candidate_family="strict_control",
                            candidate_model_name="strict_similar_day",
                            regret_uah=300.0,
                            data_quality_tier=data_quality_tier,
                            not_full_dfl=not_full_dfl,
                            safety_violation_count=safety_violation_count,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            candidate_family="raw_source",
                            candidate_model_name=source_model_name,
                            regret_uah=500.0,
                            data_quality_tier=data_quality_tier,
                            not_full_dfl=not_full_dfl,
                            safety_violation_count=safety_violation_count,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            candidate_family="strict_raw_blend_v2",
                            candidate_model_name=f"strict_raw_blend_v2_{source_model_name}",
                            regret_uah=challenger_regret,
                            data_quality_tier=data_quality_tier,
                            not_full_dfl=not_full_dfl,
                            safety_violation_count=safety_violation_count,
                        ),
                    ]
                )
    return pl.DataFrame(rows)


def _candidate_row(
    *,
    tenant_id: str,
    source_model_name: str,
    anchor: datetime,
    split_name: str,
    candidate_family: str,
    candidate_model_name: str,
    regret_uah: float,
    data_quality_tier: str,
    not_full_dfl: bool,
    safety_violation_count: int,
) -> dict[str, Any]:
    day_index = (anchor - FIRST_ANCHOR).days
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "anchor_timestamp": anchor,
        "generated_at": datetime(2026, 5, 8, tzinfo=UTC),
        "split_name": split_name,
        "candidate_family": candidate_family,
        "candidate_model_name": candidate_model_name,
        "horizon_hours": 2,
        "forecast_price_uah_mwh_vector": [1_000.0 + day_index, 1_250.0 + day_index],
        "actual_price_uah_mwh_vector": [1_100.0 + day_index, 1_300.0 + day_index],
        "dispatch_mw_vector": [0.0, 1.0],
        "soc_fraction_vector": [0.5, 0.45],
        "decision_value_uah": 1_000.0 - regret_uah,
        "forecast_objective_value_uah": 950.0,
        "oracle_value_uah": 1_000.0,
        "regret_uah": regret_uah,
        "regret_ratio": regret_uah / 1_000.0,
        "total_degradation_penalty_uah": 10.0,
        "total_throughput_mwh": 1.0,
        "forecast_spread_uah_mwh": 250.0,
        "actual_spread_uah_mwh": 200.0 + (day_index % 7) * 25.0,
        "forecast_top_k_actual_overlap": 1.0 if day_index % 5 else 0.0,
        "forecast_bottom_k_actual_overlap": 1.0 if day_index % 6 else 0.0,
        "peak_index_abs_error": float(day_index % 4),
        "trough_index_abs_error": float(day_index % 3),
        "soc_min_slack_fraction": 0.45,
        "prior_family_mean_regret_uah": regret_uah,
        "safety_violation_count": safety_violation_count,
        "data_quality_tier": data_quality_tier,
        "observed_coverage_ratio": 1.0,
        "not_full_dfl": not_full_dfl,
        "not_market_execution": True,
        "claim_scope": "schedule_candidate_library_v2_not_full_dfl",
        "candidate_library_version": "v2_test",
        "evaluation_payload": {
            "data_quality_tier": data_quality_tier,
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": safety_violation_count,
            "not_full_dfl": not_full_dfl,
            "not_market_execution": True,
        },
    }


def _mutate_latest_challenger_regret(frame: pl.DataFrame, regret_uah: float) -> pl.DataFrame:
    latest_window_start = FIRST_ANCHOR + timedelta(days=86)
    latest_challenger = (
        (pl.col("anchor_timestamp") >= latest_window_start)
        & (pl.col("candidate_family") == "strict_raw_blend_v2")
    )
    return frame.with_columns(
        pl.when(latest_challenger)
        .then(pl.lit(regret_uah))
        .otherwise(pl.col("regret_uah"))
        .alias("regret_uah"),
        pl.when(latest_challenger)
        .then(pl.lit(1_000.0 - regret_uah))
        .otherwise(pl.col("decision_value_uah"))
        .alias("decision_value_uah"),
        pl.when(latest_challenger)
        .then(pl.lit(regret_uah / 1_000.0))
        .otherwise(pl.col("regret_ratio"))
        .alias("regret_ratio"),
    )


def _benchmark_feature_frame() -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for tenant_index, tenant_id in enumerate(CANONICAL_TENANTS):
        for anchor_index in range(104):
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "timestamp": anchor,
                    "price_uah_mwh": 1_000.0 + anchor_index * 4.0 + tenant_index,
                    "weather_temperature": 5.0 + tenant_index + (anchor_index % 8),
                    "weather_wind_speed": 3.0 + (anchor_index % 5),
                    "weather_cloudcover": 40.0 + (anchor_index % 20),
                    "weather_precipitation": float(anchor_index % 3),
                    "weather_effective_solar": 200.0 + (anchor_index % 6) * 50.0,
                    "source_kind": "observed",
                    "weather_source_kind": "historical_open_meteo",
                }
            )
    return pl.DataFrame(rows)


def _tenant_historical_load_frame() -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for tenant_index, tenant_id in enumerate(CANONICAL_TENANTS):
        for anchor_index in range(104):
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "timestamp": anchor,
                    "timezone": "Europe/Kyiv",
                    "profile_label": "test_profile",
                    "load_mw": 0.1 + tenant_index * 0.02,
                    "pv_estimate_mw": 0.01 * (anchor_index % 5),
                    "net_load_mw": 0.09 + tenant_index * 0.02 + 0.002 * (anchor_index % 7),
                    "btm_battery_power_mw": 0.01 + tenant_index * 0.001,
                    "source_kind": "configured_proxy",
                    "weather_source_kind": "historical_open_meteo",
                    "reason_code": "test",
                    "claim_scope": "tenant_historical_net_load_configured_proxy",
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows)
