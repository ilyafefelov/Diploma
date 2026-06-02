from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json

import polars as pl
import pytest
import torch
from transformers import DecisionTransformerConfig, DecisionTransformerModel

from smart_arbitrage.assets.gold.baseline_solver import BaselineForecastPoint
from smart_arbitrage.dfl import hf_safe_switch_scorer
from smart_arbitrage.dfl.hf_live_safe_switch_preview import (
    LiveSafeSwitchTemplateSpec,
    build_hf_live_safe_switch_candidate_rows,
)
from smart_arbitrage.dfl.hf_safe_switch_scorer import (
    build_hf_safe_switch_scorer_packet,
    load_hf_safe_switch_inference_bundle,
    score_hf_safe_switch_candidate_rows,
    select_hf_safe_switch_candidate,
    summarize_hf_safe_switch_guard,
    write_hf_safe_switch_scorer_packet,
)
from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics


def test_hf_safe_switch_inference_bundle_round_trips_checkpoint_metadata(tmp_path) -> None:
    packet = build_hf_safe_switch_scorer_packet(
        teacher_rows_frame=_teacher_rows(),
        run_slug="hf-live-bundle-test",
        thresholds_uah=(100.0,),
        max_epochs=1,
        hidden_dim=8,
        num_layers=1,
        num_heads=1,
        output_dir=tmp_path,
        save_checkpoint=True,
    )
    write_hf_safe_switch_scorer_packet(output_dir=tmp_path, packet=packet)

    bundle = load_hf_safe_switch_inference_bundle(
        tmp_path / "hf_safe_switch_scorer_model_checkpoint"
    )

    assert bundle.metadata["load_smoke_passed"] is True
    assert bundle.threshold_uah == 100.0
    assert bundle.feature_names
    assert bundle.candidate_families == (
        "raw_reference",
        "schedule_value_learner_v2_plus",
        "schedule_value_learner_v2_plus_reference",
        "strict_reference",
    )
    assert bundle.feature_means.shape == bundle.feature_scales.shape
    assert bundle.metadata["market_execution_enabled"] is False
    assert bundle.metadata["promotion_gate_passed"] is False


def test_hf_safe_switch_inference_bundle_materializes_meta_checkpoint_on_cpu(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    packet = build_hf_safe_switch_scorer_packet(
        teacher_rows_frame=_teacher_rows(),
        run_slug="hf-live-meta-bundle-test",
        thresholds_uah=(100.0,),
        max_epochs=1,
        hidden_dim=8,
        num_layers=1,
        num_heads=1,
        output_dir=tmp_path,
        save_checkpoint=True,
    )
    write_hf_safe_switch_scorer_packet(output_dir=tmp_path, packet=packet)
    checkpoint_dir = tmp_path / "hf_safe_switch_scorer_model_checkpoint"

    def _meta_model_from_pretrained(checkpoint_path, **kwargs):
        del kwargs
        config = DecisionTransformerConfig.from_pretrained(checkpoint_path)
        with torch.device("meta"):
            return DecisionTransformerModel(config)

    monkeypatch.setattr(
        hf_safe_switch_scorer.DecisionTransformerModel,
        "from_pretrained",
        _meta_model_from_pretrained,
    )

    bundle = load_hf_safe_switch_inference_bundle(checkpoint_dir)

    assert not any(parameter.is_meta for parameter in bundle.model.parameters())
    assert {parameter.device.type for parameter in bundle.model.parameters()} == {"cpu"}
    assert not any(buffer.is_meta for buffer in bundle.model.buffers())


def test_hf_safe_switch_inference_bundle_loads_checkpoint_without_from_pretrained(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    packet = build_hf_safe_switch_scorer_packet(
        teacher_rows_frame=_teacher_rows(),
        run_slug="hf-live-direct-state-dict-bundle-test",
        thresholds_uah=(100.0,),
        max_epochs=1,
        hidden_dim=8,
        num_layers=1,
        num_heads=1,
        output_dir=tmp_path,
        save_checkpoint=True,
    )
    write_hf_safe_switch_scorer_packet(output_dir=tmp_path, packet=packet)
    checkpoint_dir = tmp_path / "hf_safe_switch_scorer_model_checkpoint"

    def _forbidden_from_pretrained(*args, **kwargs):
        del args, kwargs
        raise AssertionError("live inference bundles must load from CPU state dict directly")

    monkeypatch.setattr(
        hf_safe_switch_scorer.DecisionTransformerModel,
        "from_pretrained",
        _forbidden_from_pretrained,
    )

    bundle = load_hf_safe_switch_inference_bundle(checkpoint_dir)

    assert not any(parameter.is_meta for parameter in bundle.model.parameters())
    assert {parameter.device.type for parameter in bundle.model.parameters()} == {"cpu"}
    assert not any(buffer.is_meta for buffer in bundle.model.buffers())


def test_hf_safe_switch_inference_bundle_materializes_meta_buffers_on_cpu(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    packet = build_hf_safe_switch_scorer_packet(
        teacher_rows_frame=_teacher_rows(),
        run_slug="hf-live-meta-buffer-bundle-test",
        thresholds_uah=(100.0,),
        max_epochs=1,
        output_dir=tmp_path,
        save_checkpoint=True,
    )
    write_hf_safe_switch_scorer_packet(output_dir=tmp_path, packet=packet)
    checkpoint_dir = tmp_path / "hf_safe_switch_scorer_model_checkpoint"
    original_from_pretrained = DecisionTransformerModel.from_pretrained

    def _meta_buffer_model_from_pretrained(checkpoint_path, **kwargs):
        model = original_from_pretrained(
            checkpoint_path,
            **kwargs,
        )
        model.register_buffer("_codex_meta_probe", torch.empty(1, device="meta"))
        return model

    monkeypatch.setattr(
        hf_safe_switch_scorer.DecisionTransformerModel,
        "from_pretrained",
        _meta_buffer_model_from_pretrained,
    )

    bundle = load_hf_safe_switch_inference_bundle(checkpoint_dir)

    assert not any(parameter.is_meta for parameter in bundle.model.parameters())
    assert not any(buffer.is_meta for buffer in bundle.model.buffers())


def test_hf_safe_switch_inference_bundle_refuses_executable_metadata(tmp_path) -> None:
    packet = build_hf_safe_switch_scorer_packet(
        teacher_rows_frame=_teacher_rows(),
        run_slug="hf-live-bad-bundle-test",
        thresholds_uah=(100.0,),
        max_epochs=1,
        hidden_dim=8,
        num_layers=1,
        num_heads=1,
        output_dir=tmp_path,
        save_checkpoint=True,
    )
    write_hf_safe_switch_scorer_packet(output_dir=tmp_path, packet=packet)
    metadata_path = (
        tmp_path
        / "hf_safe_switch_scorer_model_checkpoint"
        / "checkpoint_metadata.json"
    )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["market_execution_enabled"] = True
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    with pytest.raises(ValueError, match="market_execution_enabled"):
        load_hf_safe_switch_inference_bundle(
            tmp_path / "hf_safe_switch_scorer_model_checkpoint"
        )


def test_live_candidate_selection_uses_guard_and_abstains_on_tail_risk() -> None:
    selected = select_hf_safe_switch_candidate(
        [
            _scored_candidate("schedule_value_learner_v2_plus", 0.0, 0.0),
            _scored_candidate("strict_reference", -140.0, 0.2),
        ],
        threshold_uah=100.0,
        max_predicted_tail_risk_probability=0.5,
    )
    blocked = select_hf_safe_switch_candidate(
        [
            _scored_candidate("schedule_value_learner_v2_plus", 0.0, 0.0),
            _scored_candidate("strict_reference", -140.0, 0.9),
        ],
        threshold_uah=100.0,
        max_predicted_tail_risk_probability=0.5,
    )

    assert selected["selected_schedule_family"] == "strict_reference"
    assert selected["abstained_to_v2_plus"] is False
    assert blocked["selected_schedule_family"] == "schedule_value_learner_v2_plus"
    assert blocked["abstained_to_v2_plus"] is True


def test_live_guard_diagnostics_classify_failures_and_normalize_fallback_delta() -> None:
    scored_candidates = [
        _scored_candidate("schedule_value_learner_v2_plus", -77.0, 0.3, schedule_value=0.0),
        _scored_candidate("strict_reference", -93.0, 0.2, schedule_value=900.0),
        _scored_candidate("raw_reference", -140.0, 0.8, schedule_value=2400.0),
    ]
    selection = select_hf_safe_switch_candidate(
        scored_candidates,
        threshold_uah=100.0,
        max_predicted_tail_risk_probability=0.5,
    )

    diagnostics = summarize_hf_safe_switch_guard(
        scored_candidates,
        selected_candidate=selection["selected_candidate"],
        threshold_uah=100.0,
        max_predicted_tail_risk_probability=0.5,
        max_family_tail_risk_probability=1.0,
    )

    assert selection["abstained_to_v2_plus"] is True
    assert diagnostics["reported_selected_predicted_regret_delta_vs_v2_plus_uah"] == 0.0
    assert diagnostics["best_nonfallback_schedule_family"] == "raw_reference"
    assert diagnostics["best_safe_nonfallback_schedule_family"] == "strict_reference"
    assert diagnostics["best_safe_nonfallback_threshold_margin_to_switch_uah"] == 7.0
    assert diagnostics["threshold_guard_failed_count"] == 1
    assert diagnostics["predicted_tail_guard_failed_count"] == 1
    assert diagnostics["safety_guard_failed_count"] == 0
    assert diagnostics["selected_vs_best_template_value_gap_uah"] == 2400.0


def test_live_candidate_builder_accepts_template_specs_without_changing_default() -> None:
    default_rows = build_hf_live_safe_switch_candidate_rows(
        tenant_id="client_003_dnipro_factory",
        source_model_name="official_oree_dam_live",
        anchor_timestamp=datetime(2026, 5, 15, 23, tzinfo=UTC),
        forecast=_forecast_points(),
        battery_metrics=_battery_metrics(),
        starting_soc_fraction=0.5,
    )
    tuned_rows = build_hf_live_safe_switch_candidate_rows(
        tenant_id="client_003_dnipro_factory",
        source_model_name="official_oree_dam_live",
        anchor_timestamp=datetime(2026, 5, 15, 23, tzinfo=UTC),
        forecast=_forecast_points(),
        battery_metrics=_battery_metrics(),
        starting_soc_fraction=0.5,
        template_specs={
            "strict_reference": LiveSafeSwitchTemplateSpec(
                active_hour_count=4,
                power_fraction=0.4,
            ),
        },
    )

    default_strict = next(
        row for row in default_rows if row["dt_schedule_family_target"] == "strict_reference"
    )
    tuned_strict = next(
        row for row in tuned_rows if row["dt_schedule_family_target"] == "strict_reference"
    )
    default_fallback = next(
        row
        for row in default_rows
        if row["dt_schedule_family_target"] == "schedule_value_learner_v2_plus"
    )
    tuned_fallback = next(
        row
        for row in tuned_rows
        if row["dt_schedule_family_target"] == "schedule_value_learner_v2_plus"
    )

    assert tuned_strict["total_throughput_mwh"] > default_strict["total_throughput_mwh"]
    assert tuned_fallback["dispatch_mw_vector"] == default_fallback["dispatch_mw_vector"]


def test_live_scoring_does_not_require_actual_regret_labels(tmp_path) -> None:
    packet = build_hf_safe_switch_scorer_packet(
        teacher_rows_frame=_teacher_rows(),
        run_slug="hf-live-score-test",
        thresholds_uah=(100.0,),
        max_epochs=1,
        hidden_dim=8,
        num_layers=1,
        num_heads=1,
        output_dir=tmp_path,
        save_checkpoint=True,
    )
    write_hf_safe_switch_scorer_packet(output_dir=tmp_path, packet=packet)
    bundle = load_hf_safe_switch_inference_bundle(
        tmp_path / "hf_safe_switch_scorer_model_checkpoint"
    )
    candidate_rows = build_hf_live_safe_switch_candidate_rows(
        tenant_id="client_003_dnipro_factory",
        source_model_name="official_oree_dam_live",
        anchor_timestamp=datetime(2026, 5, 15, 23, tzinfo=UTC),
        forecast=_forecast_points(),
        battery_metrics=_battery_metrics(),
        starting_soc_fraction=0.5,
    )

    result = score_hf_safe_switch_candidate_rows(
        bundle=bundle,
        candidate_rows=candidate_rows,
    )

    assert len(candidate_rows) == 4
    assert all("regret_uah" not in row for row in candidate_rows)
    assert result["selected_candidate_id"]
    assert result["live_actual_regret_available"] is False
    assert len(result["scored_candidates"]) == 4


def _scored_candidate(
    family: str,
    predicted_delta: float,
    predicted_tail_risk: float,
    *,
    schedule_value: float = 1000.0,
    family_tail_risk: float = 0.0,
    safety_violation_count: int = 0,
) -> dict[str, object]:
    return {
        "dt_schedule_family_target": family,
        "dt_candidate_id_target": family,
        "dt_candidate_index_target": 1 if family == "schedule_value_learner_v2_plus" else 3,
        "schedule_value_uah": schedule_value,
        "predicted_regret_delta_vs_v2_plus_uah": predicted_delta,
        "predicted_tail_risk_probability": predicted_tail_risk,
        "family_tail_risk_probability": family_tail_risk,
        "safety_violation_count": safety_violation_count,
    }


def _forecast_points() -> list[BaselineForecastPoint]:
    anchor = datetime(2026, 5, 15, 23, tzinfo=UTC)
    return [
        BaselineForecastPoint(
            forecast_timestamp=anchor + timedelta(hours=hour + 1),
            source_timestamp=anchor,
            predicted_price_uah_mwh=1800.0
            + hour * 35.0
            + (900.0 if 18 <= hour <= 21 else 0.0)
            - (250.0 if hour <= 5 else 0.0),
        )
        for hour in range(24)
    ]


def _battery_metrics() -> BatteryPhysicalMetrics:
    return BatteryPhysicalMetrics(
        capacity_mwh=2.0,
        max_power_mw=0.5,
        soc_min_fraction=0.1,
        soc_max_fraction=0.9,
        round_trip_efficiency=0.92,
        degradation_cost_per_cycle_uah=80.0,
    )


def _teacher_rows() -> pl.DataFrame:
    start = datetime(2026, 4, 1, 23, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    for split_name, day_offset in (
        ("train_selection", 0),
        ("train_selection", 1),
        ("final_holdout", 2),
        ("final_holdout", 3),
    ):
        anchor = start + timedelta(days=day_offset)
        for candidate_index, family, regret_delta in (
            (0, "raw_reference", -60.0),
            (1, "schedule_value_learner_v2_plus", 0.0),
            (2, "schedule_value_learner_v2_plus_reference", 45.0),
            (3, "strict_reference", -120.0),
        ):
            rows.append(
                {
                    "tenant_id": "client_003_dnipro_factory",
                    "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
                    "anchor_timestamp": anchor,
                    "split_name": split_name,
                    "forecast_price_uah_mwh_vector": [1000.0, 1600.0, 4200.0],
                    "dispatch_mw_vector": [0.0, -0.1, 0.1],
                    "soc_fraction_vector": [0.5, 0.55, 0.45, 0.5],
                    "forecast_spread_uah_mwh": 3200.0,
                    "soc_min_slack_fraction": 0.35,
                    "total_throughput_mwh": 0.2,
                    "total_degradation_penalty_uah": 4.0,
                    "forecast_objective_value_uah": 260.0,
                    "safety_violation_count": 0,
                    "dt_candidate_index_target": candidate_index,
                    "dt_candidate_id_target": f"{anchor.isoformat()}|{family}",
                    "dt_schedule_family_target": family,
                    "teacher_anchor_candidate_count": 4,
                    "regret_delta_vs_v2_plus_uah": regret_delta,
                    "regret_uah": 100.0 + regret_delta,
                    "schedule_value_uah": 1000.0 - 100.0 - regret_delta,
                    "market_execution_enabled": False,
                    "promotion_gate_passed": False,
                    "market_execution_gate_passed": False,
                    "raw_hourly_action_imitation": False,
                    "permits_model_training": False,
                    "not_market_execution": True,
                    "research_shadow_not_promotable": True,
                }
            )
    return pl.DataFrame(rows)
