from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.dfl.schedule_value_promotion_gate import (
    DFL_SCHEDULE_VALUE_PRODUCTION_GATE_CLAIM_SCOPE,
    build_dfl_schedule_value_learner_v2_trace_summary,
    build_dfl_schedule_value_production_gate_frame,
    build_dfl_schedule_value_production_gate_registry,
    validate_dfl_schedule_value_production_gate_evidence,
    write_dfl_schedule_value_production_gate_registry,
)
from smart_arbitrage.dfl.schedule_value_learner import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_CLAIM_SCOPE,
    DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_LP_STRATEGY_KIND,
    schedule_value_learner_v2_model_name,
)
from smart_arbitrage.dfl.schedule_value_learner_robustness import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_ROBUSTNESS_CLAIM_SCOPE,
)

TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
SOURCE_MODELS: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0")
FIRST_FINAL_ANCHOR = datetime(2026, 4, 12, 23)
GENERATED_AT = datetime(2026, 5, 11, 12)


def test_schedule_value_production_gate_promotes_offline_when_latest_and_rolling_pass() -> (
    None
):
    gate = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(
            selected_regrets={"tft_silver_v0": 80.0, "nbeatsx_silver_v0": 85.0}
        ),
        _robustness_frame(
            strict_pass_counts={"tft_silver_v0": 3, "nbeatsx_silver_v0": 4}
        ),
        source_model_names=SOURCE_MODELS,
    )
    evidence = validate_dfl_schedule_value_production_gate_evidence(
        gate,
        source_model_names=SOURCE_MODELS,
    )

    assert evidence.passed is True
    assert set(gate["production_promote"]) == {True}
    assert set(gate["market_execution_enabled"]) == {False}
    assert set(gate["claim_scope"]) == {DFL_SCHEDULE_VALUE_PRODUCTION_GATE_CLAIM_SCOPE}
    assert _row(gate, "tft_silver_v0")["allowed_challenger"] == (
        "dfl_schedule_value_learner_v2_tft_silver_v0"
    )
    assert _row(gate, "nbeatsx_silver_v0")["fallback_strategy"] == (
        "strict_similar_day_default_fallback"
    )
    assert evidence.metadata["promoted_source_model_names"] == [
        "nbeatsx_silver_v0",
        "tft_silver_v0",
    ]


def test_schedule_value_production_gate_blocks_median_degradation() -> None:
    gate = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(
            selected_regrets={"tft_silver_v0": 40.0, "nbeatsx_silver_v0": 85.0},
            selected_median_regrets={"tft_silver_v0": 120.0, "nbeatsx_silver_v0": 85.0},
        ),
        _robustness_frame(
            strict_pass_counts={"tft_silver_v0": 3, "nbeatsx_silver_v0": 4}
        ),
        source_model_names=SOURCE_MODELS,
    )

    tft = _row(gate, "tft_silver_v0")
    nbeatsx = _row(gate, "nbeatsx_silver_v0")

    assert tft["latest_source_signal"] is False
    assert tft["production_promote"] is False
    assert tft["promotion_blocker"] == "median_degraded"
    assert nbeatsx["production_promote"] is True


def test_schedule_value_production_gate_blocks_rolling_failure() -> None:
    gate = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(
            selected_regrets={"tft_silver_v0": 80.0, "nbeatsx_silver_v0": 85.0}
        ),
        _robustness_frame(
            strict_pass_counts={"tft_silver_v0": 2, "nbeatsx_silver_v0": 4}
        ),
        source_model_names=SOURCE_MODELS,
    )

    tft = _row(gate, "tft_silver_v0")

    assert tft["latest_source_signal"] is True
    assert tft["rolling_strict_pass_window_count"] == 2
    assert tft["production_promote"] is False
    assert tft["promotion_blocker"] == "rolling_not_robust"


def test_schedule_value_production_gate_final_score_mutation_does_not_change_robustness_context() -> (
    None
):
    robust = _robustness_frame(
        strict_pass_counts={"tft_silver_v0": 3, "nbeatsx_silver_v0": 4}
    )
    original = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(
            selected_regrets={"tft_silver_v0": 80.0, "nbeatsx_silver_v0": 85.0}
        ),
        robust,
        source_model_names=SOURCE_MODELS,
    )
    mutated = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(
            selected_regrets={"tft_silver_v0": 140.0, "nbeatsx_silver_v0": 85.0}
        ),
        robust,
        source_model_names=SOURCE_MODELS,
    )

    original_tft = _row(original, "tft_silver_v0")
    mutated_tft = _row(mutated, "tft_silver_v0")

    assert original_tft["latest_selected_mean_regret_uah"] == 80.0
    assert mutated_tft["latest_selected_mean_regret_uah"] == 140.0
    assert (
        original_tft["rolling_strict_pass_window_count"]
        == mutated_tft["rolling_strict_pass_window_count"]
    )
    assert (
        original_tft["robust_research_challenger"]
        == mutated_tft["robust_research_challenger"]
    )


def test_schedule_value_production_gate_evidence_fails_on_bad_flags_and_market_execution() -> (
    None
):
    gate = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(
            selected_regrets={"tft_silver_v0": 80.0, "nbeatsx_silver_v0": 85.0}
        ),
        _robustness_frame(
            strict_pass_counts={"tft_silver_v0": 3, "nbeatsx_silver_v0": 4}
        ),
        source_model_names=SOURCE_MODELS,
    )
    bad_gate = gate.with_columns(
        not_market_execution=pl.lit(False),
        market_execution_enabled=pl.lit(True),
    )

    evidence = validate_dfl_schedule_value_production_gate_evidence(
        bad_gate,
        source_model_names=SOURCE_MODELS,
    )

    assert evidence.passed is False
    assert "not_market_execution=true" in evidence.description
    assert "market_execution_enabled=false" in evidence.description


def test_schedule_value_production_gate_registry_writes_concise_artifacts(
    tmp_path,
) -> None:
    gate = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(
            selected_regrets={"tft_silver_v0": 80.0, "nbeatsx_silver_v0": 85.0}
        ),
        _robustness_frame(
            strict_pass_counts={"tft_silver_v0": 3, "nbeatsx_silver_v0": 4}
        ),
        source_model_names=SOURCE_MODELS,
    )

    registry = build_dfl_schedule_value_production_gate_registry(
        run_slug="unit_schedule_value_gate",
        gate_frame=gate,
        dagster_run_id="unit-run",
    )
    export_dir = write_dfl_schedule_value_production_gate_registry(
        registry,
        output_root=tmp_path,
        run_slug="unit_schedule_value_gate",
    )

    assert registry["claim_boundary"]["not_market_execution"] is True
    assert registry["summary"]["production_promote_count"] == 2
    assert registry["summary"]["market_execution_enabled"] is False
    assert registry["source_model_rows"][0]["fallback_strategy"] == (
        "strict_similar_day_default_fallback"
    )
    assert (export_dir / "dfl_schedule_value_production_gate_registry.json").exists()
    markdown = (
        export_dir / "dfl_schedule_value_production_gate_registry.md"
    ).read_text(encoding="utf-8")
    assert "NBEATSx" in markdown
    assert "market execution remains disabled" in markdown


def test_schedule_value_production_gate_registry_attaches_attempt_evidence(
    tmp_path,
) -> None:
    gate = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(
            selected_regrets={"tft_silver_v0": 80.0, "nbeatsx_silver_v0": 85.0}
        ),
        _robustness_frame(
            strict_pass_counts={"tft_silver_v0": 3, "nbeatsx_silver_v0": 4}
        ),
        source_model_names=SOURCE_MODELS,
    )
    attempt_manifest = tmp_path / "source-attempt.json"
    monitor_snapshot = tmp_path / "source-monitor.json"
    attempt_manifest.write_text(
        '{"attempt_kind": "official_global_panel_backfill"}', encoding="utf-8"
    )
    monitor_snapshot.write_text(
        '{"status": "complete", "next_anchor_index": 365}', encoding="utf-8"
    )

    registry = build_dfl_schedule_value_production_gate_registry(
        run_slug="unit_schedule_value_gate",
        gate_frame=gate,
    )
    export_dir = write_dfl_schedule_value_production_gate_registry(
        registry,
        output_root=tmp_path / "exports",
        run_slug="unit_schedule_value_gate",
        attempt_manifest_path=attempt_manifest,
        monitor_snapshot_path=monitor_snapshot,
    )

    assert (export_dir / "attempt_manifest.json").read_text(encoding="utf-8") == (
        '{"attempt_kind": "official_global_panel_backfill"}'
    )
    assert (export_dir / "resume-summary.json").read_text(encoding="utf-8") == (
        '{"status": "complete", "next_anchor_index": 365}'
    )
    registry_json = (
        export_dir / "dfl_schedule_value_production_gate_registry.json"
    ).read_text(encoding="utf-8")
    assert '"attempt_manifest": "attempt_manifest.json"' in registry_json
    assert '"monitor_snapshot": "resume-summary.json"' in registry_json
    markdown = (
        export_dir / "dfl_schedule_value_production_gate_registry.md"
    ).read_text(encoding="utf-8")
    assert "## Attached Evidence" in markdown
    assert "`attempt_manifest.json`" in markdown
    assert "`resume-summary.json`" in markdown


def test_schedule_value_learner_trace_summary_records_profile_and_candidate_counts() -> (
    None
):
    summary = build_dfl_schedule_value_learner_v2_trace_summary(
        _learner_trace_frame(),
        early_train_candidate_schedules_per_anchor=9,
        max_candidate_schedules_per_anchor=10,
    )

    assert summary["summary"]["tenant_count"] == 5
    assert summary["summary"]["source_model_count"] == 2
    assert summary["summary"]["early_train_candidate_schedules_per_anchor"] == 9
    assert summary["summary"]["max_candidate_schedules_per_anchor"] == 10
    assert summary["summary"]["final_holdout_candidate_count_per_source_model"] == {
        "nbeatsx_silver_v0": 900,
        "tft_silver_v0": 900,
    }
    assert summary["source_model_rows"][0]["selected_weight_profile_names"] == [
        "prior_regret_value"
    ]
    assert summary["source_model_rows"][0]["selected_final_family_counts"] == {
        "strict_guarded_prior_value": 35,
        "strict_similar_day": 30,
        "strict_raw_blend_v2": 25,
    }
    assert (
        summary["tenant_source_rows"][0]["selected_train_family_counts"][
            "forecast_perturbation"
        ]
        == 0
    )
    assert summary["tenant_source_rows"][0]["final_holdout_candidate_count"] == 180


def test_schedule_value_production_gate_registry_attaches_learner_trace_summary(
    tmp_path,
) -> None:
    gate = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(
            selected_regrets={"tft_silver_v0": 80.0, "nbeatsx_silver_v0": 85.0}
        ),
        _robustness_frame(
            strict_pass_counts={"tft_silver_v0": 3, "nbeatsx_silver_v0": 4}
        ),
        source_model_names=SOURCE_MODELS,
    )
    registry = build_dfl_schedule_value_production_gate_registry(
        run_slug="unit_schedule_value_gate",
        gate_frame=gate,
    )
    export_dir = write_dfl_schedule_value_production_gate_registry(
        registry,
        output_root=tmp_path / "exports",
        run_slug="unit_schedule_value_gate",
        learner_trace_frame=_learner_trace_frame(),
    )

    assert (export_dir / "dfl_schedule_value_learner_v2_trace_summary.json").exists()
    assert (export_dir / "dfl_schedule_value_learner_v2_trace_summary.md").exists()
    registry_json = (
        export_dir / "dfl_schedule_value_production_gate_registry.json"
    ).read_text(encoding="utf-8")
    assert '"production_promote_count": 2' in registry_json
    assert (
        '"learner_trace_summary": "dfl_schedule_value_learner_v2_trace_summary.json"'
        in registry_json
    )
    markdown = (
        export_dir / "dfl_schedule_value_production_gate_registry.md"
    ).read_text(encoding="utf-8")
    assert "Learner V2 trace summary" in markdown
    assert "`dfl_schedule_value_learner_v2_trace_summary.md`" in markdown


def test_schedule_value_production_gate_registry_fails_on_missing_attachment(
    tmp_path,
) -> None:
    gate = build_dfl_schedule_value_production_gate_frame(
        _strict_frame(
            selected_regrets={"tft_silver_v0": 80.0, "nbeatsx_silver_v0": 85.0}
        ),
        _robustness_frame(
            strict_pass_counts={"tft_silver_v0": 3, "nbeatsx_silver_v0": 4}
        ),
        source_model_names=SOURCE_MODELS,
    )
    registry = build_dfl_schedule_value_production_gate_registry(
        run_slug="unit_schedule_value_gate",
        gate_frame=gate,
    )

    with pytest.raises(FileNotFoundError, match="Registry attachment does not exist"):
        write_dfl_schedule_value_production_gate_registry(
            registry,
            output_root=tmp_path / "exports",
            run_slug="unit_schedule_value_gate",
            attempt_manifest_path=tmp_path / "missing-attempt-manifest.json",
        )


def test_schedule_value_production_gate_registry_keeps_official_source_names(
    tmp_path,
) -> None:
    gate = pl.DataFrame(
        {
            "source_model_name": [
                "nbeatsx_official_global_panel_horizon_calibrated_v1",
                "nbeatsx_official_global_panel_v1",
            ],
            "tenant_count": [5, 5],
            "latest_validation_tenant_anchor_count": [90, 90],
            "latest_strict_mean_regret_uah": [310.58, 310.58],
            "latest_selected_mean_regret_uah": [206.37, 225.44],
            "latest_strict_median_regret_uah": [198.39, 198.39],
            "latest_selected_median_regret_uah": [96.02, 109.69],
            "latest_mean_regret_improvement_ratio_vs_strict": [0.3355, 0.2741],
            "latest_median_not_worse": [True, True],
            "latest_source_signal": [True, True],
            "rolling_window_count": [4, 4],
            "rolling_strict_pass_window_count": [4, 4],
            "rolling_development_pass_window_count": [4, 3],
            "robust_research_challenger": [True, True],
            "allowed_challenger": [
                "dfl_schedule_value_learner_v2_nbeatsx_official_global_panel_horizon_calibrated_v1",
                "dfl_schedule_value_learner_v2_nbeatsx_official_global_panel_v1",
            ],
            "fallback_strategy": [
                "strict_similar_day_default_fallback",
                "strict_similar_day_default_fallback",
            ],
            "promotion_blocker": ["none", "none"],
            "production_promote": [True, True],
            "market_execution_enabled": [False, False],
            "claim_scope": [
                "dfl_schedule_value_production_gate_offline_strategy_not_market_execution",
                "dfl_schedule_value_production_gate_offline_strategy_not_market_execution",
            ],
            "not_full_dfl": [True, True],
            "not_market_execution": [True, True],
        }
    )

    registry = build_dfl_schedule_value_production_gate_registry(
        run_slug="unit_official_global_panel_gate",
        gate_frame=gate,
    )
    export_dir = write_dfl_schedule_value_production_gate_registry(
        registry,
        output_root=tmp_path,
        run_slug="unit_official_global_panel_gate",
    )

    markdown = (
        export_dir / "dfl_schedule_value_production_gate_registry.md"
    ).read_text(encoding="utf-8")
    assert "`nbeatsx_official_global_panel_horizon_calibrated_v1`" in markdown
    assert "`nbeatsx_official_global_panel_v1`" in markdown
    assert "`nbeatsx_silver_v0`" not in markdown


def _row(frame: pl.DataFrame, source_model_name: str) -> dict[str, object]:
    rows = frame.filter(pl.col("source_model_name") == source_model_name).to_dicts()
    assert len(rows) == 1
    return rows[0]


def _strict_frame(
    *,
    selected_regrets: dict[str, float],
    selected_median_regrets: dict[str, float] | None = None,
    final_anchor_count_per_tenant: int = 18,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
        for tenant_id in TENANTS:
            for anchor_index in range(final_anchor_count_per_tenant):
                anchor = FIRST_FINAL_ANCHOR + timedelta(days=anchor_index)
                selected_regret = selected_regrets[source_model_name]
                if (
                    selected_median_regrets
                    and anchor_index <= final_anchor_count_per_tenant // 2
                ):
                    selected_regret = selected_median_regrets[source_model_name]
                rows.extend(
                    [
                        _strict_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            forecast_model_name="strict_similar_day",
                            anchor=anchor,
                            regret=100.0,
                            selection_role="strict_reference",
                        ),
                        _strict_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            forecast_model_name=source_model_name,
                            anchor=anchor,
                            regret=500.0,
                            selection_role="raw_reference",
                        ),
                        _strict_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            forecast_model_name=schedule_value_learner_v2_model_name(
                                source_model_name
                            ),
                            anchor=anchor,
                            regret=selected_regret,
                            selection_role="schedule_value_learner",
                        ),
                    ]
                )
    return pl.DataFrame(rows)


def _strict_row(
    *,
    tenant_id: str,
    source_model_name: str,
    forecast_model_name: str,
    anchor: datetime,
    regret: float,
    selection_role: str,
) -> dict[str, object]:
    payload = {
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": 0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "selection_role": selection_role,
    }
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "regret_uah": regret,
        "selection_role": selection_role,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": 0,
        "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evaluation_payload": payload,
    }


def _robustness_frame(*, strict_pass_counts: dict[str, int]) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
        pass_count = strict_pass_counts[source_model_name]
        for window_index in range(1, 5):
            strict_passed = window_index <= pass_count
            rows.append(
                {
                    "source_model_name": source_model_name,
                    "window_index": window_index,
                    "tenant_count": len(TENANTS),
                    "validation_anchor_count_per_tenant": 18,
                    "validation_tenant_anchor_count": 90,
                    "minimum_prior_anchor_count_before_window": 86
                    - ((window_index - 1) * 18),
                    "strict_mean_regret_uah": 100.0,
                    "raw_mean_regret_uah": 500.0,
                    "selected_mean_regret_uah": 80.0 if strict_passed else 99.0,
                    "strict_median_regret_uah": 100.0,
                    "selected_median_regret_uah": 80.0 if strict_passed else 101.0,
                    "development_passed": True,
                    "source_specific_strict_passed": strict_passed,
                    "passing_window_count_for_source": pass_count,
                    "robust_research_challenger": pass_count >= 3 and window_index == 1,
                    "production_promote": False,
                    "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_ROBUSTNESS_CLAIM_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows)


def _learner_trace_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for source_model_name in SOURCE_MODELS:
        for tenant_id in TENANTS:
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "selected_weight_profile_name": "prior_regret_value",
                    "selected_train_family_counts": {
                        "forecast_perturbation": None,
                        "raw_source": 40,
                        "strict_control": 30,
                        "strict_raw_blend_v2": 16,
                    },
                    "selected_final_family_counts": {
                        "strict_guarded_prior_value": 7,
                        "strict_similar_day": 6,
                        "strict_raw_blend_v2": 5,
                    },
                    "train_anchor_count": 347,
                    "final_holdout_anchor_count": 18,
                    "final_holdout_tenant_anchor_count": 90,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows)
