from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.dfl.schedule_value_learner import (
    build_dfl_schedule_value_learner_v2_frame,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus import (
    build_dfl_schedule_value_regret_decomposition_frame,
    build_dfl_schedule_value_learner_v2_plus_frame,
    build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus_export import (
    build_dfl_schedule_value_learner_v2_plus_comparison_packet,
    write_dfl_schedule_value_learner_v2_plus_comparison_packet,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus_robustness import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_CLAIM_SCOPE,
)

TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
SOURCE_MODELS: tuple[str, ...] = (
    "nbeatsx_official_global_panel_v1",
    "nbeatsx_official_global_panel_horizon_calibrated_v1",
)
FIRST_ANCHOR = datetime(2026, 1, 1, 23)
GENERATED_AT = datetime(2026, 5, 15, 12)


def test_v2_plus_comparison_packet_writes_artifacts_after_gate_pass(
    tmp_path,
) -> None:
    strict_frame, learner_frame, decomposition_frame = _frames_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=700.0,
        v2_final_regret=220.0,
        v2_plus_final_regret=180.0,
        v2_plus_train_regret=30.0,
    )

    packet = build_dfl_schedule_value_learner_v2_plus_comparison_packet(
        run_slug="v2-plus-test",
        strict_frame=strict_frame,
        learner_frame=learner_frame,
        regret_decomposition_frame=decomposition_frame,
        dagster_run_id="run-123",
        materialization_command="dagster asset materialize ...",
    )
    export_dir = write_dfl_schedule_value_learner_v2_plus_comparison_packet(
        packet,
        output_root=tmp_path,
        strict_frame=strict_frame,
    )

    assert packet["gate"]["passed"] is True
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert packet["gate"]["metrics"]["mean_regret_improvement_ratio_vs_v2"] > 0.0
    assert (
        export_dir / "dfl_schedule_value_learner_v2_plus_comparison.json"
    ).exists()
    assert (
        export_dir / "dfl_schedule_value_learner_v2_plus_comparison.md"
    ).exists()
    assert (
        export_dir / "dfl_schedule_value_learner_v2_plus_strict_rows.csv"
    ).exists()
    assert "Offline Strategy Promotion evidence only" in (
        export_dir / "dfl_schedule_value_learner_v2_plus_comparison.md"
    ).read_text(encoding="utf-8")


def test_v2_plus_comparison_packet_attaches_rolling_robustness_summary(
    tmp_path,
) -> None:
    strict_frame, learner_frame, decomposition_frame = _frames_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=700.0,
        v2_final_regret=220.0,
        v2_plus_final_regret=180.0,
        v2_plus_train_regret=30.0,
    )
    robustness_frame = _robustness_frame(robust=True)

    packet = build_dfl_schedule_value_learner_v2_plus_comparison_packet(
        run_slug="v2-plus-test",
        strict_frame=strict_frame,
        learner_frame=learner_frame,
        regret_decomposition_frame=decomposition_frame,
        rolling_robustness_frame=robustness_frame,
    )
    export_dir = write_dfl_schedule_value_learner_v2_plus_comparison_packet(
        packet,
        output_root=tmp_path,
        strict_frame=strict_frame,
        rolling_robustness_frame=robustness_frame,
    )

    assert packet["rolling_robustness"]["gate"]["decision"] == (
        "v2_plus_robust_research_challenger"
    )
    assert packet["rolling_robustness"]["gate"]["metrics"][
        "market_execution_enabled"
    ] is False
    assert packet["attached_artifacts"]["rolling_robustness_csv"] == (
        "dfl_schedule_value_learner_v2_plus_rolling_robustness.csv"
    )
    assert (
        export_dir / "dfl_schedule_value_learner_v2_plus_rolling_robustness.csv"
    ).exists()
    markdown = (
        export_dir / "dfl_schedule_value_learner_v2_plus_comparison.md"
    ).read_text(encoding="utf-8")
    assert "## Rolling Robustness" in markdown
    assert "4 / 4" in markdown


def test_v2_plus_comparison_packet_refuses_non_robust_attachment() -> None:
    strict_frame, learner_frame, decomposition_frame = _frames_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=700.0,
        v2_final_regret=220.0,
        v2_plus_final_regret=180.0,
        v2_plus_train_regret=30.0,
    )

    with pytest.raises(ValueError, match="robustness gate failed"):
        build_dfl_schedule_value_learner_v2_plus_comparison_packet(
            run_slug="v2-plus-test",
            strict_frame=strict_frame,
            learner_frame=learner_frame,
            regret_decomposition_frame=decomposition_frame,
            rolling_robustness_frame=_robustness_frame(robust=False),
        )


def test_v2_plus_comparison_export_refuses_failed_evidence() -> None:
    strict_frame, learner_frame, decomposition_frame = _frames_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=700.0,
        v2_final_regret=220.0,
        v2_plus_final_regret=180.0,
        v2_plus_train_regret=30.0,
    )

    with pytest.raises(ValueError, match="evidence check failed"):
        build_dfl_schedule_value_learner_v2_plus_comparison_packet(
            run_slug="v2-plus-test",
            strict_frame=strict_frame.drop("selection_role"),
            learner_frame=learner_frame,
            regret_decomposition_frame=decomposition_frame,
        )


def test_v2_plus_comparison_export_refuses_failed_gate() -> None:
    strict_frame, learner_frame, decomposition_frame = _frames_from_regrets(
        strict_final_regret=300.0,
        raw_final_regret=700.0,
        v2_final_regret=220.0,
        v2_plus_final_regret=240.0,
        v2_plus_train_regret=30.0,
    )

    with pytest.raises(ValueError, match="strict gate failed"):
        build_dfl_schedule_value_learner_v2_plus_comparison_packet(
            run_slug="v2-plus-test",
            strict_frame=strict_frame,
            learner_frame=learner_frame,
            regret_decomposition_frame=decomposition_frame,
        )


def _frames_from_regrets(
    *,
    strict_final_regret: float,
    raw_final_regret: float,
    v2_final_regret: float,
    v2_plus_final_regret: float,
    v2_plus_train_regret: float,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    library = _candidate_library_from_regrets(
        strict_final_regret=strict_final_regret,
        raw_final_regret=raw_final_regret,
        v2_final_regret=v2_final_regret,
        v2_plus_final_regret=v2_plus_final_regret,
        v2_plus_train_regret=v2_plus_train_regret,
    )
    v2_model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )
    v2_plus_model = build_dfl_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        tenant_ids=TENANTS,
        forecast_model_names=SOURCE_MODELS,
    )
    strict_frame = build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        library,
        v2_plus_model,
        v2_model,
        generated_at=GENERATED_AT,
    )
    decomposition_frame = build_dfl_schedule_value_regret_decomposition_frame(
        library,
        v2_model,
    )
    return strict_frame, v2_plus_model, decomposition_frame


def _candidate_library_from_regrets(
    *,
    strict_final_regret: float,
    raw_final_regret: float,
    v2_final_regret: float,
    v2_plus_final_regret: float,
    v2_plus_train_regret: float,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    train_anchor_count = 3
    final_anchor_count = 18
    for tenant_id in TENANTS:
        for source_model_name in SOURCE_MODELS:
            for anchor_index in range(train_anchor_count + final_anchor_count):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                split_name = (
                    "final_holdout"
                    if anchor_index >= train_anchor_count
                    else "train_selection"
                )
                rows.extend(
                    [
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="strict_control",
                            candidate_model_name="strict_similar_day",
                            anchor=anchor,
                            split_name=split_name,
                            regret=(
                                strict_final_regret
                                if split_name == "final_holdout"
                                else 300.0
                            ),
                            forecast_prices=(1000.0, 5000.0),
                            prior_family_mean_regret=300.0,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="raw_source",
                            candidate_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            regret=(
                                raw_final_regret
                                if split_name == "final_holdout"
                                else 700.0
                            ),
                            forecast_prices=(5000.0, 1000.0),
                            prior_family_mean_regret=700.0,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="strict_prior_residual_v2",
                            candidate_model_name=(
                                "dfl_schedule_library_v2_prior_residual_"
                                f"{source_model_name}"
                            ),
                            anchor=anchor,
                            split_name=split_name,
                            regret=(
                                v2_final_regret
                                if split_name == "final_holdout"
                                else 80.0
                            ),
                            forecast_prices=(900.0, 6500.0),
                            prior_family_mean_regret=80.0,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="rank_extrema_perturbation_v2_plus",
                            candidate_model_name=(
                                "dfl_schedule_library_v2_plus_rank_extrema_"
                                f"{source_model_name}"
                            ),
                            anchor=anchor,
                            split_name=split_name,
                            regret=(
                                v2_plus_final_regret
                                if split_name == "final_holdout"
                                else v2_plus_train_regret
                            ),
                            forecast_prices=(700.0, 7000.0),
                            prior_family_mean_regret=v2_plus_train_regret,
                        ),
                    ]
                )
    return pl.DataFrame(rows)


def _candidate_row(
    *,
    tenant_id: str,
    source_model_name: str,
    candidate_family: str,
    candidate_model_name: str,
    anchor: datetime,
    split_name: str,
    regret: float,
    forecast_prices: tuple[float, ...],
    prior_family_mean_regret: float,
) -> dict[str, object]:
    actual_prices = [1000.0, 5000.0, 1500.0, 4000.0][: len(forecast_prices)]
    dispatch = [0.0, 1.0, 0.0, -1.0][: len(forecast_prices)]
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "candidate_family": candidate_family,
        "candidate_model_name": candidate_model_name,
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "split_name": split_name,
        "horizon_hours": len(forecast_prices),
        "forecast_price_uah_mwh_vector": list(forecast_prices),
        "actual_price_uah_mwh_vector": actual_prices,
        "dispatch_mw_vector": dispatch,
        "soc_fraction_vector": [0.5, 0.4, 0.6, 0.5][: len(forecast_prices)],
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 5.0,
        "total_throughput_mwh": 0.2,
        "forecast_spread_uah_mwh": max(forecast_prices) - min(forecast_prices),
        "actual_spread_uah_mwh": max(actual_prices) - min(actual_prices),
        "forecast_top_k_actual_overlap": 1.0,
        "forecast_bottom_k_actual_overlap": 1.0,
        "peak_index_abs_error": 0.0,
        "trough_index_abs_error": 0.0,
        "soc_min_slack_fraction": 0.4,
        "prior_family_mean_regret_uah": prior_family_mean_regret,
        "safety_violation_count": 0,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "claim_scope": "dfl_schedule_candidate_library_v2_plus_not_full_dfl",
        "evaluation_payload": {
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "not_full_dfl": True,
            "not_market_execution": True,
            "safety_violation_count": 0,
            "source_forecast_model_name": source_model_name,
        },
    }


def _robustness_frame(*, robust: bool) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
        passing_count = 4 if robust else 2
        for window_index in range(1, 5):
            passed = window_index <= passing_count
            rows.append(
                {
                    "source_model_name": source_model_name,
                    "window_index": window_index,
                    "validation_start_anchor_timestamp": (
                        FIRST_ANCHOR + timedelta(days=window_index)
                    ),
                    "validation_end_anchor_timestamp": (
                        FIRST_ANCHOR + timedelta(days=window_index + 17)
                    ),
                    "tenant_count": len(TENANTS),
                    "validation_anchor_count_per_tenant": 18,
                    "validation_tenant_anchor_count": 90,
                    "minimum_prior_anchor_count_before_window": 30,
                    "fallback_to_v2_by_tenant": {},
                    "selected_family_counts": {"strict_raw_blend_v2": 90},
                    "strict_mean_regret_uah": 300.0,
                    "raw_mean_regret_uah": 700.0,
                    "v2_mean_regret_uah": 220.0,
                    "selected_mean_regret_uah": 180.0 if passed else 240.0,
                    "strict_median_regret_uah": 200.0,
                    "v2_median_regret_uah": 120.0,
                    "selected_median_regret_uah": 100.0 if passed else 130.0,
                    "mean_regret_improvement_ratio_vs_raw": 0.7 if passed else 0.6,
                    "mean_regret_improvement_ratio_vs_strict": 0.4 if passed else 0.2,
                    "mean_regret_improvement_ratio_vs_v2": 0.1 if passed else -0.1,
                    "development_passed": passed,
                    "source_specific_strict_passed": passed,
                    "v2_non_degradation_passed": passed,
                    "v2_plus_window_passed": passed,
                    "passing_window_count_for_source": passing_count,
                    "robust_research_challenger": robust,
                    "production_promote": False,
                    "claim_scope": (
                        DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_CLAIM_SCOPE
                    ),
                    "academic_scope": "test robustness evidence",
                    "not_full_dfl": True,
                    "not_market_execution": True,
                    "gate_label": (
                        "robust_research_challenger" if robust else "blocked"
                    ),
                }
            )
    return pl.DataFrame(rows)
