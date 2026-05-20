from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.poland_lag24_prior_veto import (
    PolandLag24PriorVetoConfig,
    build_poland_lag24_prior_veto_frame,
    build_poland_lag24_prior_veto_packet,
    write_poland_lag24_prior_veto_packet,
)


def test_poland_lag24_prior_veto_uses_prior_rows_and_fallback() -> None:
    audit_frame = _audit_frame()

    veto_frame = build_poland_lag24_prior_veto_frame(
        audit_frame,
        config=PolandLag24PriorVetoConfig(
            min_prior_rows=4,
            ridge_alpha=100.0,
            threshold_candidates=(-100.0, -10.0, 0.0, 10.0, 50.0),
            max_prior_selected_loss_delta_uah=60.0,
        ),
    )

    assert veto_frame.height == audit_frame.height
    first_anchor = veto_frame["anchor_timestamp"].min()
    first_rows = veto_frame.filter(pl.col("anchor_timestamp") == first_anchor)
    assert first_rows.select(pl.col("selected_uses_challenger").any()).item() is False
    assert first_rows.select(pl.col("selection_reason").unique()).to_series().to_list() == [
        "fallback_insufficient_prior"
    ]
    assert veto_frame.select(pl.col("selected_uses_challenger").sum()).item() > 0
    assert veto_frame.select(pl.col("market_execution_enabled").any()).item() is False
    assert veto_frame.select(pl.col("not_market_execution").all()).item() is True


def test_poland_lag24_prior_veto_current_labels_do_not_change_selection() -> None:
    audit_frame = _audit_frame()
    config = PolandLag24PriorVetoConfig(
        min_prior_rows=4,
        ridge_alpha=100.0,
        threshold_candidates=(-100.0, -10.0, 0.0, 10.0, 50.0),
        max_prior_selected_loss_delta_uah=60.0,
    )

    original = build_poland_lag24_prior_veto_frame(audit_frame, config=config)
    last_anchor = audit_frame["anchor_timestamp"].max()
    mutated = audit_frame.with_columns(
        pl.when(pl.col("anchor_timestamp") == last_anchor)
        .then(pl.lit(9999.0))
        .otherwise(pl.col("challenger_regret_uah"))
        .alias("challenger_regret_uah"),
        pl.when(pl.col("anchor_timestamp") == last_anchor)
        .then(pl.lit(9999.0) - pl.col("baseline_regret_uah"))
        .otherwise(pl.col("delta_regret_uah"))
        .alias("delta_regret_uah"),
    )
    changed = build_poland_lag24_prior_veto_frame(mutated, config=config)

    key_columns = [
        "tenant_id",
        "anchor_timestamp",
        "selected_uses_challenger",
        "predicted_delta_regret_uah",
        "selected_threshold_uah",
    ]
    assert original.select(key_columns).rows() == changed.select(key_columns).rows()
    assert original["selected_regret_uah"].to_list() != changed["selected_regret_uah"].to_list()


def test_poland_lag24_prior_veto_packet_exports_improvement_not_promotion(
    tmp_path,
) -> None:
    audit_frame = _audit_frame()
    config = PolandLag24PriorVetoConfig(
        min_prior_rows=4,
        ridge_alpha=100.0,
        threshold_candidates=(-100.0, -10.0, 0.0, 10.0, 50.0),
        max_prior_selected_loss_delta_uah=60.0,
        promotion_min_improvement_ratio=0.5,
    )
    veto_frame = build_poland_lag24_prior_veto_frame(
        audit_frame,
        config=config,
    )

    packet = build_poland_lag24_prior_veto_packet(
        run_slug="week3_poland_prior_veto",
        veto_frame=veto_frame,
        dagster_run_id="prior-veto-run",
        config=config,
    )
    export_dir = write_poland_lag24_prior_veto_packet(
        packet,
        output_root=tmp_path,
        veto_frame=veto_frame,
    )

    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert packet["summary"]["selected_challenger_rows"] > 0
    assert packet["gate"]["beats_frozen_v2_plus_mean"] is True
    assert packet["gate"]["promotes_over_frozen_v2_plus"] is False
    assert packet["gate"]["blocker"] == "improvement_below_5_percent"
    assert (export_dir / "poland_lag24_prior_veto_summary.json").exists()
    assert (export_dir / "poland_lag24_prior_veto_rows.csv").exists()
    markdown = (export_dir / "poland_lag24_prior_veto_summary.md").read_text(
        encoding="utf-8"
    )
    assert "prior-only" in markdown
    assert "market_execution_enabled=false" in markdown


def _audit_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    start = datetime(2026, 4, 1, 23)
    for anchor_index in range(6):
        for tenant_id in ("safe", "risky"):
            is_safe = tenant_id == "safe"
            baseline_regret = 100.0
            if anchor_index < 2:
                challenger_regret = 70.0 if is_safe else 150.0
            else:
                challenger_regret = 75.0 if is_safe else 180.0
            if is_safe and anchor_index >= 2:
                challenger_regret = 60.0
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "anchor_timestamp": (start + timedelta(days=anchor_index)).isoformat(),
                    "baseline_model_name": "v2_plus",
                    "challenger_model_name": "poland_tft",
                    "baseline_regret_uah": baseline_regret,
                    "challenger_regret_uah": challenger_regret,
                    "delta_regret_uah": challenger_regret - baseline_regret,
                    "outcome_class": (
                        "poland_improved"
                        if challenger_regret < baseline_regret
                        else "poland_worse"
                    ),
                    "baseline_decision_value_uah": 1000.0 - baseline_regret,
                    "challenger_decision_value_uah": 1000.0 - challenger_regret,
                    "oracle_value_uah": 1000.0,
                    "baseline_committed_action": "HOLD",
                    "challenger_committed_action": "DISCHARGE" if is_safe else "CHARGE",
                    "baseline_committed_power_mw": 0.0,
                    "challenger_committed_power_mw": 0.1 if is_safe else -0.1,
                    "action_changed": True,
                    "baseline_total_throughput_mwh": 0.4,
                    "challenger_total_throughput_mwh": 0.7 if is_safe else 1.2,
                    "throughput_delta_mwh": 0.3 if is_safe else 0.8,
                    "baseline_total_degradation_penalty_uah": 10.0,
                    "challenger_total_degradation_penalty_uah": 15.0 if is_safe else 50.0,
                    "degradation_delta_uah": 5.0 if is_safe else 40.0,
                    "baseline_candidate_family": "v2_plus_default",
                    "challenger_candidate_family": "safe_family" if is_safe else "risky_family",
                    "baseline_weight_profile": "balanced",
                    "challenger_weight_profile": "balanced",
                    "challenger_source_quantile": "p50",
                    "challenger_quantile_spread_scale": 1.0 if is_safe else 3.0,
                    "baseline_forecast_peak_step": 19,
                    "challenger_forecast_peak_step": 20 if is_safe else 5,
                    "baseline_forecast_trough_step": 12,
                    "challenger_forecast_trough_step": 12 if is_safe else 2,
                    "baseline_forecast_spread_uah_mwh": 2000.0,
                    "challenger_forecast_spread_uah_mwh": 2100.0 if is_safe else 9000.0,
                    "forecast_spread_delta_uah_mwh": 100.0 if is_safe else 7000.0,
                    "baseline_absolute_dispatch_mwh": 0.4,
                    "challenger_absolute_dispatch_mwh": 0.7 if is_safe else 1.2,
                    "tail_loss_threshold_uah": 60.0,
                    "tail_risk_class": "not_loss" if is_safe else "tail_loss",
                }
            )
    return pl.DataFrame(rows)
