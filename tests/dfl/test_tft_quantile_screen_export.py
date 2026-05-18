from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.tft_quantile_schedule_value import (
    DFL_TFT_AUGMENTED_V2_PLUS_STRICT_LP_STRATEGY_KIND,
    TFT_QUANTILE_SOURCE_MODELS,
    build_dfl_tft_quantile_schedule_candidate_library_frame,
)
from smart_arbitrage.dfl.tft_quantile_screen_export import (
    build_dfl_tft_quantile_screen_packet,
    write_dfl_tft_quantile_screen_packet,
)

TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
GENERATED_AT = datetime(2026, 5, 18, 14)
FIRST_ANCHOR = datetime(2026, 4, 12, 23)


def test_tft_quantile_screen_packet_exports_blocked_gate(tmp_path) -> None:
    raw_strict = _raw_tft_strict_frame(anchor_count=1)
    candidates = build_dfl_tft_quantile_schedule_candidate_library_frame(
        raw_strict,
        tenant_ids=TENANTS,
        final_validation_anchor_count_per_tenant=1,
    )
    augmented = _v2_plus_augmented_blocked_frame()

    packet = build_dfl_tft_quantile_screen_packet(
        run_slug="unit_tft_screen",
        raw_strict_frame=raw_strict,
        candidate_library_frame=candidates,
        augmented_gate_frame=augmented,
        dagster_run_id="unit-run",
    )
    export_dir = write_dfl_tft_quantile_screen_packet(
        packet,
        output_root=tmp_path,
        raw_strict_frame=raw_strict,
        candidate_library_frame=candidates,
        augmented_gate_frame=augmented,
    )

    assert packet["gate"]["passed"] is False
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert packet["gate_blockers"] == ["missing_tft_train_rows"]
    assert (export_dir / "dfl_tft_quantile_screen_summary.json").exists()
    assert (export_dir / "dfl_tft_quantile_screen_summary.md").exists()
    assert (export_dir / "tft_raw_strict_rows.csv").exists()
    assert (export_dir / "tft_candidate_library_rows.csv").exists()
    assert (export_dir / "tft_augmented_gate_rows.csv").exists()


def test_tft_quantile_screen_packet_refuses_market_execution_rows() -> None:
    raw_strict = _raw_tft_strict_frame(anchor_count=1).with_columns(
        pl.lit(True).alias("market_execution_enabled")
    )
    candidates = build_dfl_tft_quantile_schedule_candidate_library_frame(
        _raw_tft_strict_frame(anchor_count=1),
        tenant_ids=TENANTS,
        final_validation_anchor_count_per_tenant=1,
    )

    try:
        build_dfl_tft_quantile_screen_packet(
            run_slug="bad_tft_screen",
            raw_strict_frame=raw_strict,
            candidate_library_frame=candidates,
            augmented_gate_frame=_v2_plus_augmented_blocked_frame(),
        )
    except ValueError as error:
        assert "market execution disabled" in str(error)
    else:  # pragma: no cover
        raise AssertionError("expected market-execution guard to fail")


def _raw_tft_strict_frame(*, anchor_count: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index in range(anchor_count):
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            rows.append(
                _evaluation_row(
                    tenant_id=tenant_id,
                    forecast_model_name="strict_similar_day",
                    anchor=anchor,
                    regret=310.0,
                )
            )
            for source_model_name in TFT_QUANTILE_SOURCE_MODELS:
                rows.append(
                    _evaluation_row(
                        tenant_id=tenant_id,
                        forecast_model_name=source_model_name,
                        anchor=anchor,
                        regret=2500.0,
                    )
                )
    return pl.DataFrame(rows)


def _v2_plus_augmented_blocked_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        anchor = FIRST_ANCHOR
        for role, regret in [
            ("strict_reference", 310.0),
            ("raw_reference", 620.0),
            ("schedule_value_learner_v2_reference", 206.0),
            ("schedule_value_learner_v2_plus", 174.0),
        ]:
            row = _evaluation_row(
                tenant_id=tenant_id,
                forecast_model_name="nbeatsx_official_global_panel_horizon_calibrated_v1",
                anchor=anchor,
                regret=regret,
            )
            row["source_model_name"] = (
                "nbeatsx_official_global_panel_horizon_calibrated_v1"
            )
            row["selection_role"] = role
            row["strategy_kind"] = DFL_TFT_AUGMENTED_V2_PLUS_STRICT_LP_STRATEGY_KIND
            row["market_execution_enabled"] = False
            row["tft_gate_blocker"] = "missing_tft_train_rows"
            rows.append(row)
    return pl.DataFrame(rows)


def _evaluation_row(
    *,
    tenant_id: str,
    forecast_model_name: str,
    anchor: datetime,
    regret: float,
) -> dict[str, object]:
    return {
        "evaluation_id": f"{tenant_id}:{forecast_model_name}:{anchor:%Y%m%dT%H%M}",
        "tenant_id": tenant_id,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": "unit_tft_screen",
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "horizon_hours": 2,
        "starting_soc_fraction": 0.5,
        "starting_soc_source": "tenant_default",
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": 0.1,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "selection_role": "raw_reference",
        "evaluation_payload": {
            "not_market_execution": True,
            "market_execution_enabled": False,
            "not_full_dfl": True,
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "source_forecast_model_name": forecast_model_name,
            "horizon": [
                {
                    "step_index": 0,
                    "forecast_price_uah_mwh": 1000.0,
                    "actual_price_uah_mwh": 1000.0,
                    "net_power_mw": 0.0,
                    "soc_fraction": 0.5,
                    "degradation_penalty_uah": 0.0,
                },
                {
                    "step_index": 1,
                    "forecast_price_uah_mwh": 5200.0,
                    "actual_price_uah_mwh": 5200.0,
                    "net_power_mw": 0.0,
                    "soc_fraction": 0.5,
                    "degradation_penalty_uah": 0.0,
                },
            ],
        },
    }
