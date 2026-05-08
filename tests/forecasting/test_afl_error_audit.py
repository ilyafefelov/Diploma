from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.forecasting.afl_error_audit import (
    build_afl_forecast_error_audit_frame,
    validate_afl_forecast_error_audit_evidence,
)


def test_afl_forecast_error_audit_classifies_shape_rank_and_lp_value_failures() -> None:
    audit = build_afl_forecast_error_audit_frame(
        _forensics_frame(),
        _afl_panel_frame(),
    )

    nbeatsx = audit.filter(
        (pl.col("forecast_model_name") == "nbeatsx_silver_v0")
        & (pl.col("split") == "final_holdout")
    ).row(0, named=True)

    assert nbeatsx["row_count"] == 2
    assert nbeatsx["spread_shape_failure_rate"] == pytest.approx(1.0)
    assert nbeatsx["rank_extrema_failure_rate"] == pytest.approx(1.0)
    assert nbeatsx["lp_value_failure_rate"] == pytest.approx(1.0)
    assert nbeatsx["strict_control_high_regret_overlap_rate"] == pytest.approx(0.5)
    assert nbeatsx["weather_load_regime_status"] == "context_unavailable"
    assert "label_" not in nbeatsx["selector_feature_columns_csv"]
    assert nbeatsx["dominant_failure_mode"] == "lp_value_failure"
    assert nbeatsx["not_full_dfl"] is True
    assert nbeatsx["not_market_execution"] is True

    outcome = validate_afl_forecast_error_audit_evidence(audit)

    assert outcome.passed is True
    assert outcome.metadata["model_count"] == 2
    assert outcome.metadata["claim_flag_failure_rows"] == 0


def test_afl_forecast_error_audit_rejects_bad_claim_flags_and_missing_strict_rows() -> None:
    bad_claim_panel = _afl_panel_frame().with_columns(
        pl.when(pl.col("forecast_model_name") == "nbeatsx_silver_v0")
        .then(False)
        .otherwise(pl.col("not_full_dfl"))
        .alias("not_full_dfl")
    )
    missing_strict_panel = _afl_panel_frame().filter(
        pl.col("forecast_model_name") != "strict_similar_day"
    )

    with pytest.raises(ValueError, match="research-only"):
        build_afl_forecast_error_audit_frame(_forensics_frame(), bad_claim_panel)

    with pytest.raises(ValueError, match="strict_similar_day"):
        build_afl_forecast_error_audit_frame(_forensics_frame(), missing_strict_panel)


def test_afl_forecast_error_audit_validation_blocks_label_columns_as_selector_features() -> None:
    audit = build_afl_forecast_error_audit_frame(
        _forensics_frame(),
        _afl_panel_frame(),
    ).with_columns(
        (
            pl.col("selector_feature_columns_csv") + ",label_regret_uah"
        ).alias("selector_feature_columns_csv")
    )

    outcome = validate_afl_forecast_error_audit_evidence(audit)

    assert outcome.passed is False
    assert outcome.metadata["selector_label_column_rows"] == audit.height


def _forensics_frame() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "forecast_model_name": "strict_similar_day",
                "model_family": "Strict Similar-Day",
                "candidate_kind": "frozen_control_comparator",
                "implementation_scope": "level_1_baseline_forecast_control",
                "row_count": 2,
                "tenant_count": 1,
                "anchor_count": 2,
                "claim_scope": "forecast_candidate_forensics_not_full_dfl",
                "not_full_dfl": True,
                "not_market_execution": True,
            },
            {
                "forecast_model_name": "nbeatsx_silver_v0",
                "model_family": "NBEATSx",
                "candidate_kind": "compact_silver_candidate",
                "implementation_scope": "compact_in_repo_nbeatsx_style_candidate",
                "row_count": 2,
                "tenant_count": 1,
                "anchor_count": 2,
                "claim_scope": "forecast_candidate_forensics_not_full_dfl",
                "not_full_dfl": True,
                "not_market_execution": True,
            },
            {
                "forecast_model_name": "tft_silver_v0",
                "model_family": "TFT",
                "candidate_kind": "compact_silver_candidate",
                "implementation_scope": "compact_in_repo_tft_style_candidate",
                "row_count": 2,
                "tenant_count": 1,
                "anchor_count": 2,
                "claim_scope": "forecast_candidate_forensics_not_full_dfl",
                "not_full_dfl": True,
                "not_market_execution": True,
            },
        ]
    )


def _afl_panel_frame() -> pl.DataFrame:
    first_anchor = datetime(2026, 4, 28, 23)
    rows: list[dict[str, object]] = []
    for anchor_index in range(2):
        anchor = first_anchor + timedelta(days=anchor_index)
        strict_regret = 100.0 if anchor_index == 0 else 250.0
        for model_name, regret, forecast_spread, rank_overlap, top_recall in [
            ("strict_similar_day", strict_regret, 900.0, 1.0, 1.0),
            ("nbeatsx_silver_v0", 300.0 + anchor_index, 100.0, 0.0, 0.0),
            ("tft_silver_v0", 90.0 + anchor_index, 880.0, 1.0, 1.0),
        ]:
            rows.append(
                {
                    "tenant_id": "client_001_kyiv_mall",
                    "forecast_model_name": model_name,
                    "model_family": "NBEATSx" if "nbeatsx" in model_name else "TFT",
                    "candidate_kind": (
                        "frozen_control_comparator"
                        if model_name == "strict_similar_day"
                        else "compact_silver_candidate"
                    ),
                    "market_venue": "DAM",
                    "anchor_timestamp": anchor,
                    "split": "final_holdout",
                    "horizon_hours": 24,
                    "feature_anchor_hour": 23.0,
                    "feature_anchor_weekday": 2.0,
                    "feature_prior_model_anchor_count": 86,
                    "feature_prior_strict_anchor_count": 86,
                    "feature_prior_mean_model_regret_uah": regret,
                    "feature_prior_mean_strict_regret_uah": strict_regret,
                    "feature_prior_regret_advantage_vs_strict_uah": strict_regret - regret,
                    "feature_forecast_price_spread_uah_mwh": forecast_spread,
                    "feature_forecast_active_hour_count": 4,
                    "feature_forecast_top3_bottom3_rank_overlap": rank_overlap,
                    "label_regret_uah": regret,
                    "label_regret_ratio": regret / 1000.0,
                    "label_decision_value_uah": 1000.0 - regret,
                    "label_oracle_value_uah": 1000.0,
                    "label_total_degradation_penalty_uah": 3.0,
                    "label_total_throughput_mwh": 0.2,
                    "label_actual_price_spread_uah_mwh": 1000.0,
                    "label_decision_weight_uah": regret + 1000.0,
                    "diagnostic_mae_uah_mwh": 50.0,
                    "diagnostic_rmse_uah_mwh": 60.0,
                    "diagnostic_top_k_price_recall": top_recall,
                    "diagnostic_spread_ranking_quality": rank_overlap,
                    "claim_scope": "arbitrage_focused_learning_panel_not_full_dfl",
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows)
