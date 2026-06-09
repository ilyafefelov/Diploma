from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.poland_lag24_candidate_value_ranker import (
    POLAND_LAG24_CANDIDATE_VALUE_RANKER_SELECTION_ROLE,
    build_poland_lag24_candidate_value_label_panel_frame,
    build_poland_lag24_candidate_value_ranker_frame,
    build_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame,
)

TENANT = "client_001_kyiv_mall"
BASELINE_SOURCE = "nbeatsx_official_global_panel_horizon_calibrated_v1"
POLAND_SOURCE = "tft_official_global_panel_poland_lag24_horizon_quantile_calibrated_v1"


def test_poland_candidate_value_ranker_selects_safe_candidate_from_prior_labels() -> None:
    library = _candidate_library()
    lagged_features = _lagged_feature_frame()
    frozen = _frozen_v2_plus_strict_frame()

    label_panel = build_poland_lag24_candidate_value_label_panel_frame(
        library,
        lagged_features,
    )
    ranker = build_poland_lag24_candidate_value_ranker_frame(
        label_panel,
        tenant_ids=(TENANT,),
        forecast_model_names=(POLAND_SOURCE,),
        min_prior_mean_improvement_ratio_vs_frozen_proxy=0.01,
    )
    strict = build_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame(
        library,
        ranker,
        frozen,
    )

    selected = strict.filter(
        pl.col("selection_role") == POLAND_LAG24_CANDIDATE_VALUE_RANKER_SELECTION_ROLE
    )

    assert ranker.row(0, named=True)["fallback_to_frozen_v2_plus"] is False
    assert selected.height == 2
    assert set(selected["candidate_family"].to_list()) == {"poland_value_good"}
    assert selected["regret_uah"].mean() < 120.0
    assert selected.select(pl.col("market_execution_enabled").any()).item() is False


def test_poland_candidate_value_ranker_features_are_stable_when_final_labels_mutate() -> None:
    library = _candidate_library()
    mutated = library.with_columns(
        pl.when(pl.col("split_name") == "final_holdout")
        .then(pl.col("regret_uah") + 500.0)
        .otherwise(pl.col("regret_uah"))
        .alias("regret_uah")
    )
    lagged_features = _lagged_feature_frame()

    base_panel = build_poland_lag24_candidate_value_label_panel_frame(
        library,
        lagged_features,
    )
    mutated_panel = build_poland_lag24_candidate_value_label_panel_frame(
        mutated,
        lagged_features,
    )
    selector_columns = [
        column for column in base_panel.columns if column.startswith("selector_feature_")
    ]

    assert selector_columns
    assert base_panel.select(selector_columns).to_dicts() == mutated_panel.select(
        selector_columns
    ).to_dicts()
    assert base_panel.select("label_regret_uah").to_dicts() != mutated_panel.select(
        "label_regret_uah"
    ).to_dicts()


def _candidate_library() -> pl.DataFrame:
    start = datetime(2026, 1, 1, 23)
    rows: list[dict[str, object]] = []
    for anchor_index in range(5):
        anchor = start + timedelta(days=anchor_index)
        split_name = "train_selection" if anchor_index < 3 else "final_holdout"
        for family, regret, spread, throughput in (
            ("strict_control", 150.0, 500.0, 0.2),
            ("poland_value_good", 80.0, 1500.0, 0.4),
            ("poland_value_bad", 260.0, 250.0, 0.1),
        ):
            rows.append(
                {
                    "tenant_id": TENANT,
                    "source_model_name": POLAND_SOURCE,
                    "candidate_family": family,
                    "candidate_model_name": f"{family}_{anchor_index}",
                    "anchor_timestamp": anchor,
                    "generated_at": datetime(2026, 5, 21, 15),
                    "split_name": split_name,
                    "horizon_hours": 24,
                    "forecast_price_uah_mwh_vector": [1000.0, 1000.0 + spread],
                    "actual_price_uah_mwh_vector": [900.0, 2600.0],
                    "dispatch_mw_vector": [0.2, -0.2],
                    "soc_fraction_vector": [0.55, 0.50],
                    "decision_value_uah": 1000.0 - regret,
                    "forecast_objective_value_uah": 900.0,
                    "oracle_value_uah": 1000.0,
                    "regret_uah": regret,
                    "regret_ratio": regret / 1000.0,
                    "total_degradation_penalty_uah": 10.0,
                    "total_throughput_mwh": throughput,
                    "evaluation_payload": {
                        "claim_scope": "test",
                        "market_execution_enabled": False,
                    },
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows)


def _lagged_feature_frame() -> pl.DataFrame:
    start = datetime(2026, 1, 1, 23)
    rows: list[dict[str, object]] = []
    for anchor_index in range(5):
        anchor = start + timedelta(days=anchor_index)
        rows.append(
            {
                "delivery_timestamp_utc": anchor.replace(tzinfo=None).isoformat()
                + "+00:00",
                "entsoe_pl_lag24_day_ahead_price_uah_mwh": 3200.0 + anchor_index * 25.0,
                "entsoe_pl_lag24_delta_24h_uah_mwh": float(anchor_index) * 10.0,
                "entsoe_pl_lag24_daily_spread_uah_mwh": 700.0,
                "entsoe_pl_lag24_daily_price_rank": 0.8,
                "entsoe_pl_lag24_ua_spread_uah_mwh": 200.0 + anchor_index * 10.0,
                "entsoe_pl_lag24_ua_rank_disagreement": 0.25,
                "entsoe_pl_lag24_ua_peak_hour_delta": 2.0,
                "entsoe_pl_lag24_ua_trough_hour_delta": 1.0,
                "entsoe_pl_lag24_morning_block_mean_uah_mwh": 2900.0,
                "entsoe_pl_lag24_evening_block_mean_uah_mwh": 3600.0,
                "entsoe_pl_lag24_evening_morning_spread_uah_mwh": 700.0,
            }
        )
    return pl.DataFrame(rows)


def _frozen_v2_plus_strict_frame() -> pl.DataFrame:
    start = datetime(2026, 1, 4, 23)
    rows: list[dict[str, object]] = []
    for anchor_index in range(2):
        anchor = start + timedelta(days=anchor_index)
        rows.append(
            {
                "tenant_id": TENANT,
                "source_model_name": BASELINE_SOURCE,
                "forecast_model_name": f"dfl_schedule_value_learner_v2_plus_{BASELINE_SOURCE}",
                "selection_role": "schedule_value_learner_v2_plus",
                "anchor_timestamp": anchor,
                "generated_at": datetime(2026, 5, 21, 15),
                "regret_uah": 120.0,
                "decision_value_uah": 880.0,
                "forecast_objective_value_uah": 900.0,
                "oracle_value_uah": 1000.0,
                "evaluation_payload": {"market_execution_enabled": False},
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(rows)
