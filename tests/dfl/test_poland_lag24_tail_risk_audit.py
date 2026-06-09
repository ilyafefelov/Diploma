from __future__ import annotations

from datetime import datetime
import json

import polars as pl
import pytest

from smart_arbitrage.dfl.poland_lag24_tail_risk_audit import (
    build_poland_lag24_tail_risk_audit_frame,
    build_poland_lag24_tail_risk_packet,
    write_poland_lag24_tail_risk_packet,
)


BASELINE_MODEL = (
    "dfl_schedule_value_learner_v2_plus_"
    "nbeatsx_official_global_panel_horizon_calibrated_v1"
)
CHALLENGER_MODEL = (
    "dfl_schedule_value_learner_v2_plus_"
    "tft_official_global_panel_poland_lag24_horizon_quantile_calibrated_v1"
)


def test_poland_lag24_tail_risk_audit_splits_wins_losses_and_tail_failures() -> None:
    audit_frame = build_poland_lag24_tail_risk_audit_frame(
        baseline_frame=_strict_rows(BASELINE_MODEL, [100.0, 200.0, 40.0, 80.0]),
        challenger_frame=_strict_rows(CHALLENGER_MODEL, [50.0, 360.0, 20.0, 120.0]),
        baseline_model_name=BASELINE_MODEL,
        challenger_model_name=CHALLENGER_MODEL,
        tail_loss_quantile=0.5,
    )

    assert audit_frame.height == 4
    assert audit_frame.select((pl.col("outcome_class") == "poland_improved").sum()).item() == 2
    assert audit_frame.select((pl.col("outcome_class") == "poland_worse").sum()).item() == 2
    tail_rows = audit_frame.filter(pl.col("tail_risk_class") == "tail_loss")
    assert tail_rows.height == 1
    assert tail_rows["delta_regret_uah"].to_list() == [160.0]
    assert tail_rows["challenger_candidate_family"].to_list() == ["tft_quantile_risk"]
    assert tail_rows["challenger_peak_step_error"].to_list() == [1]


def test_poland_lag24_tail_risk_packet_reports_oracle_diagnostic_not_promotion(
    tmp_path,
) -> None:
    audit_frame = build_poland_lag24_tail_risk_audit_frame(
        baseline_frame=_strict_rows(BASELINE_MODEL, [100.0, 200.0, 40.0, 80.0]),
        challenger_frame=_strict_rows(CHALLENGER_MODEL, [50.0, 360.0, 20.0, 120.0]),
        baseline_model_name=BASELINE_MODEL,
        challenger_model_name=CHALLENGER_MODEL,
        tail_loss_quantile=0.5,
    )

    packet = build_poland_lag24_tail_risk_packet(
        run_slug="week3_poland_tail_audit",
        audit_frame=audit_frame,
        dagster_run_id="tail-risk-run",
    )
    export_dir = write_poland_lag24_tail_risk_packet(
        packet,
        output_root=tmp_path,
        audit_frame=audit_frame,
    )

    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert packet["summary"]["wins"] == 2
    assert packet["summary"]["losses"] == 2
    assert packet["summary"]["mean_delta_regret_uah"] == pytest.approx(32.5)
    assert packet["summary"]["oracle_loss_avoidance_mean_regret_uah"] == pytest.approx(
        87.5
    )
    assert packet["summary"]["oracle_loss_avoidance_is_diagnostic_only"] is True
    assert (export_dir / "poland_lag24_tail_risk_summary.json").exists()
    assert (export_dir / "poland_lag24_tail_risk_summary.md").exists()
    assert (export_dir / "poland_lag24_tail_risk_rows.csv").exists()
    assert (export_dir / "poland_lag24_tail_risk_by_tenant.csv").exists()
    markdown = (export_dir / "poland_lag24_tail_risk_summary.md").read_text(
        encoding="utf-8"
    )
    assert "oracle-only diagnostic" in markdown
    assert "market_execution_enabled=false" in markdown


def test_poland_lag24_tail_risk_audit_refuses_market_execution_claim() -> None:
    baseline = _strict_rows(BASELINE_MODEL, [10.0]).with_columns(
        pl.lit(True).alias("market_execution_enabled")
    )

    with pytest.raises(ValueError, match="market execution"):
        build_poland_lag24_tail_risk_audit_frame(
            baseline_frame=baseline,
            challenger_frame=_strict_rows(CHALLENGER_MODEL, [9.0]),
            baseline_model_name=BASELINE_MODEL,
            challenger_model_name=CHALLENGER_MODEL,
        )


def _strict_rows(model_name: str, regrets: list[float]) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for index, regret in enumerate(regrets):
        anchor = datetime(2026, 4, 13 + index, 23, 0)
        rows.append(
            {
                "tenant_id": f"client_{index % 2:03d}",
                "anchor_timestamp": anchor.isoformat(),
                "forecast_model_name": model_name,
                "strategy_kind": "dfl_schedule_value_learner_v2_plus_strict_lp_benchmark",
                "market_venue": "DAM",
                "regret_uah": regret,
                "decision_value_uah": 1000.0 - regret,
                "oracle_value_uah": 1000.0,
                "committed_action": "DISCHARGE" if index % 2 else "HOLD",
                "committed_power_mw": 0.1 * index,
                "total_throughput_mwh": 0.5 + (0.1 * index),
                "total_degradation_penalty_uah": 20.0 + index,
                "rank_by_regret": index + 1,
                "evaluation_payload": json.dumps(
                    {
                        "candidate_family": (
                            "tft_quantile_risk"
                            if "poland_lag24" in model_name
                            else "v2_plus_default"
                        ),
                        "selected_weight_profile_name": "balanced",
                        "source_quantile": "p50",
                        "quantile_spread_scale": 1.2,
                        "claim_scope": "offline_strategy_promotion_only",
                        "not_market_execution": True,
                        "not_full_dfl": True,
                        "horizon": [
                            {
                                "step_index": 0,
                                "actual_price_uah_mwh": 10.0,
                                "forecast_price_uah_mwh": 12.0,
                                "net_power_mw": 0.0,
                            },
                            {
                                "step_index": 1,
                                "actual_price_uah_mwh": 30.0,
                                "forecast_price_uah_mwh": (
                                    18.0 if "poland_lag24" in model_name else 28.0
                                ),
                                "net_power_mw": 0.1,
                            },
                            {
                                "step_index": 2,
                                "actual_price_uah_mwh": 20.0,
                                "forecast_price_uah_mwh": (
                                    35.0 if "poland_lag24" in model_name else 21.0
                                ),
                                "net_power_mw": -0.1,
                            },
                        ],
                    }
                ),
            }
        )
    return pl.DataFrame(rows)
