from __future__ import annotations

from datetime import datetime, timedelta
import json

import polars as pl

from smart_arbitrage.dfl.regret_aware_v2_plus_selector import (
    FEATURE_SET_EXPANDED,
    MODEL_KIND_HIST_GRADIENT_BOOSTING,
    MODEL_KIND_RANDOM_FOREST,
    build_regret_aware_v2_plus_selector_packet,
    parse_selector_vector,
)
from scripts.materialize_regret_aware_v2_plus_selector_packet import (
    main as materialize_regret_aware_v2_plus_selector_packet,
)


def test_regret_aware_v2_plus_selector_learns_weighted_switch_with_abstention() -> None:
    frame = _teacher_rows_for_regret_aware_selector()

    result = build_regret_aware_v2_plus_selector_packet(
        frame,
        run_slug="regret-aware-selector-test",
        min_predicted_improvement_uah=5.0,
        ridge_l2=0.01,
    )

    selected = result["selected_rows"].sort("anchor_timestamp")
    summary = result["summary"]

    assert summary["claim_scope"] == (
        "regret_aware_v2_plus_selector_shadow_not_promotable_not_market_execution"
    )
    assert summary["loss_function"] == "weighted_ridge_regret_delta_vs_v2_plus"
    assert summary["sample_weight_formula"] == "1 + abs(regret_delta_vs_v2_plus_uah) / 100"
    assert summary["feature_leakage_guard"]["uses_realized_regret_as_feature"] is False
    assert summary["boundary"]["abstains_to_v2_plus_when_signal_is_weak"] is True
    assert summary["boundary"]["market_execution_enabled"] is False
    assert summary["boundary"]["dt_lava_ready"] is False
    assert summary["training"]["max_sample_weight"] > summary["training"]["min_sample_weight"]

    high_signal = selected.row(0, named=True)
    weak_signal = selected.row(1, named=True)
    assert high_signal["selected_schedule_family"] == "safe_challenger"
    assert high_signal["abstained_to_v2_plus"] is False
    assert high_signal["selected_regret_uah"] == 60.0
    assert high_signal["v2_plus_regret_uah"] == 100.0
    assert high_signal["predicted_improvement_vs_v2_plus_uah"] >= 5.0

    assert weak_signal["selected_schedule_family"] == "schedule_value_learner_v2_plus"
    assert weak_signal["abstained_to_v2_plus"] is True
    assert weak_signal["abstention_reason"] == "predicted_improvement_below_threshold"
    assert weak_signal["selected_regret_uah"] == 100.0

    assert summary["evaluation"]["final_holdout_anchor_count"] == 2
    assert summary["evaluation"]["non_v2_plus_switch_count"] == 1
    assert summary["evaluation"]["abstention_count"] == 1
    assert summary["evaluation"]["selector_mean_regret_uah"] == 80.0
    assert summary["evaluation"]["v2_plus_mean_regret_uah"] == 100.0
    assert summary["evaluation"]["selector_minus_v2_plus_mean_regret_uah"] == -20.0


def test_regret_aware_v2_plus_selector_refuses_market_execution_rows() -> None:
    frame = _teacher_rows_for_regret_aware_selector().with_columns(
        pl.lit(True).alias("market_execution_enabled")
    )

    try:
        build_regret_aware_v2_plus_selector_packet(frame, run_slug="bad")
    except ValueError as exc:
        assert "market execution" in str(exc)
    else:  # pragma: no cover - defensive test branch.
        raise AssertionError("expected market execution rows to be rejected")


def test_regret_aware_v2_plus_selector_supports_expanded_hgb_safe_switch_model() -> None:
    frame = _teacher_rows_for_regret_aware_selector()

    result = build_regret_aware_v2_plus_selector_packet(
        frame,
        run_slug="regret-aware-hgb-test",
        min_predicted_improvement_uah=0.0,
        ridge_l2=1.0,
        model_kind=MODEL_KIND_HIST_GRADIENT_BOOSTING,
        feature_set=FEATURE_SET_EXPANDED,
    )

    summary = result["summary"]

    assert summary["model_kind"] == MODEL_KIND_HIST_GRADIENT_BOOSTING
    assert summary["feature_set"] == FEATURE_SET_EXPANDED
    assert summary["loss_function"] == "hist_gradient_boosting_regret_delta_vs_v2_plus"
    assert summary["feature_leakage_guard"]["uses_realized_regret_as_feature"] is False
    assert "selector_feature_anchor_day_of_year" in summary["feature_names"]
    assert summary["boundary"]["market_execution_enabled"] is False


def test_regret_aware_v2_plus_selector_supports_random_forest_safe_switch_model() -> None:
    frame = _teacher_rows_for_regret_aware_selector()

    result = build_regret_aware_v2_plus_selector_packet(
        frame,
        run_slug="regret-aware-rf-test",
        min_predicted_improvement_uah=0.0,
        model_kind=MODEL_KIND_RANDOM_FOREST,
        feature_set=FEATURE_SET_EXPANDED,
    )

    summary = result["summary"]

    assert summary["model_kind"] == MODEL_KIND_RANDOM_FOREST
    assert summary["feature_set"] == FEATURE_SET_EXPANDED
    assert summary["loss_function"] == "random_forest_regret_delta_vs_v2_plus"
    assert summary["feature_leakage_guard"]["uses_realized_regret_as_feature"] is False
    assert summary["boundary"]["market_execution_enabled"] is False


def test_regret_aware_v2_plus_selector_detects_exact_content_mirror() -> None:
    result = build_regret_aware_v2_plus_selector_packet(
        _exact_mirror_teacher_rows_for_regret_aware_selector(),
        run_slug="regret-aware-exact-mirror-test",
        min_predicted_improvement_uah=0.0,
        model_kind=MODEL_KIND_RANDOM_FOREST,
        feature_set=FEATURE_SET_EXPANDED,
    )

    independence = result["summary"]["evaluation_independence"]

    assert independence == {
        "train_candidate_row_count": 4,
        "evaluation_candidate_row_count": 4,
        "content_overlap_candidate_row_count": 4,
        "content_overlap_ratio": 1.0,
        "exact_content_mirror": True,
        "independent_holdout": False,
    }


def test_regret_aware_v2_plus_selector_cli_writes_research_shadow_packet(tmp_path) -> None:
    teacher_rows_csv = tmp_path / "teacher_rows.csv"
    output_dir = tmp_path / "selector_packet"
    _csv_ready(_teacher_rows_for_regret_aware_selector()).write_csv(teacher_rows_csv)

    exit_code = materialize_regret_aware_v2_plus_selector_packet(
        [
            "--teacher-rows-csv",
            str(teacher_rows_csv),
            "--output-dir",
            str(output_dir),
            "--run-slug",
            "regret-aware-cli-test",
            "--min-predicted-improvement-uah",
            "5",
            "--ridge-l2",
            "0.01",
        ]
    )

    assert exit_code == 0
    summary = (output_dir / "regret_aware_v2_plus_selector_summary.json").read_text(
        encoding="utf-8"
    )
    assert "regret-aware-cli-test" in summary
    assert (output_dir / "regret_aware_v2_plus_selector_selected_rows.csv").exists()
    assert (output_dir / "regret_aware_v2_plus_selector_summary.md").exists()
    assert (output_dir / "regret_aware_v2_plus_selector_teacher_rows.csv").exists()


def test_regret_aware_v2_plus_selector_reads_double_encoded_csv_vectors(
    tmp_path,
) -> None:
    teacher_rows_csv = tmp_path / "teacher_rows.csv"
    _csv_ready(_vector_only_signal_rows_for_regret_aware_selector()).write_csv(
        teacher_rows_csv
    )
    frame = pl.read_csv(teacher_rows_csv, infer_schema_length=1000)

    row = frame.row(0, named=True)
    parsed_forecast = parse_selector_vector(row["forecast_price_uah_mwh_vector"])
    parsed_dispatch = parse_selector_vector(row["dispatch_mw_vector"])
    parsed_soc = parse_selector_vector(row["soc_fraction_vector"])

    assert parsed_forecast == [1000.0, 1520.0]
    assert parsed_dispatch == [0.1, -0.1]
    assert parsed_soc == [0.52, 0.58]
    assert parse_selector_vector('"not-a-list"') == []


def test_regret_aware_v2_plus_selector_uses_csv_vector_features(tmp_path) -> None:
    teacher_rows_csv = tmp_path / "teacher_rows.csv"
    _csv_ready(_teacher_rows_for_regret_aware_selector()).write_csv(teacher_rows_csv)
    frame = pl.read_csv(teacher_rows_csv, infer_schema_length=1000)

    result = build_regret_aware_v2_plus_selector_packet(
        frame,
        run_slug="regret-aware-csv-vector-feature-test",
        min_predicted_improvement_uah=5.0,
        ridge_l2=0.01,
    )

    selected = result["selected_rows"].sort("anchor_timestamp")
    high_signal = selected.row(0, named=True)
    assert high_signal["selected_schedule_family"] == "safe_challenger"
    assert result["summary"]["evaluation"]["selector_mean_regret_uah"] == 80.0


def _vector_only_signal_rows_for_regret_aware_selector() -> pl.DataFrame:
    return _teacher_rows_for_regret_aware_selector().with_columns(
        pl.lit(0.0).alias("forecast_spread_uah_mwh"),
        pl.lit(0.0).alias("selector_feature_forecast_spread_uah_mwh"),
    )


def _teacher_rows_for_regret_aware_selector() -> pl.DataFrame:
    start = datetime(2026, 4, 1, 23)
    rows: list[dict[str, object]] = []
    scenarios = [
        ("train_selection", 0, 520.0, -40.0),
        ("train_selection", 1, 45.0, 120.0),
        ("final_holdout", 2, 500.0, -40.0),
        ("final_holdout", 3, 40.0, 110.0),
    ]
    for split_name, day_offset, spread, challenger_delta in scenarios:
        anchor = start + timedelta(days=day_offset)
        rows.append(
            _row(
                anchor=anchor,
                split_name=split_name,
                family="schedule_value_learner_v2_plus",
                candidate_index=0,
                regret=100.0,
                regret_delta=0.0,
                forecast_spread=spread,
            )
        )
        rows.append(
            _row(
                anchor=anchor,
                split_name=split_name,
                family="safe_challenger",
                candidate_index=1,
                regret=100.0 + challenger_delta,
                regret_delta=challenger_delta,
                forecast_spread=spread,
            )
        )
    return pl.DataFrame(rows)


def _exact_mirror_teacher_rows_for_regret_aware_selector() -> pl.DataFrame:
    train_rows = _teacher_rows_for_regret_aware_selector().filter(
        pl.col("split_name") == "train_selection"
    )
    final_rows: list[dict[str, object]] = []
    for row in train_rows.iter_rows(named=True):
        mirrored = dict(row)
        anchor = mirrored["anchor_timestamp"]
        if not isinstance(anchor, datetime):
            raise TypeError("test fixture anchor_timestamp must be datetime")
        mirrored["anchor_timestamp"] = anchor.replace(year=anchor.year + 1)
        mirrored["split_name"] = "final_holdout"
        final_rows.append(mirrored)
    return pl.concat(
        [train_rows, pl.DataFrame(final_rows, infer_schema_length=None)],
        how="diagonal_relaxed",
    )


def _csv_ready(frame: pl.DataFrame) -> pl.DataFrame:
    vector_columns = [
        "forecast_price_uah_mwh_vector",
        "dispatch_mw_vector",
        "soc_fraction_vector",
    ]
    return frame.with_columns(
        [
            pl.col(column)
            .map_elements(_json_vector, return_dtype=pl.String)
            .alias(column)
            for column in vector_columns
        ]
    )


def _json_vector(value: object) -> str:
    if isinstance(value, pl.Series):
        return json.dumps(value.to_list())
    return json.dumps(value)


def _row(
    *,
    anchor: datetime,
    split_name: str,
    family: str,
    candidate_index: int,
    regret: float,
    regret_delta: float,
    forecast_spread: float,
) -> dict[str, object]:
    return {
        "tenant_id": "client_003_dnipro_factory",
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "anchor_timestamp": anchor,
        "split_name": split_name,
        "horizon_hours": 24,
        "forecast_price_uah_mwh_vector": [1000.0, 1000.0 + forecast_spread],
        "dispatch_mw_vector": [0.1, -0.1],
        "soc_fraction_vector": [0.52, 0.58],
        "decision_value_uah": 1000.0 - regret,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "forecast_spread_uah_mwh": forecast_spread,
        "forecast_top_k_actual_overlap": 0.5,
        "forecast_bottom_k_actual_overlap": 0.5,
        "soc_min_slack_fraction": 0.2,
        "safety_violation_count": 0,
        "selector_feature_forecast_spread_uah_mwh": forecast_spread,
        "selector_feature_terminal_soc_delta_fraction": 0.06,
        "selector_feature_total_throughput_delta_mwh": 0.4,
        "selector_feature_total_degradation_penalty_uah": 2.0,
        "market_execution_enabled": False,
        "not_full_dfl": True,
        "not_market_execution": True,
        "raw_hourly_action_imitation": False,
        "dt_candidate_index_target": candidate_index,
        "dt_candidate_id_target": f"{anchor.isoformat()}|{family}",
        "dt_schedule_family_target": family,
        "return_to_go_regret_target_uah": -regret_delta,
        "regret_delta_vs_v2_plus_uah": regret_delta,
        "schedule_value_uah": 1000.0 - regret,
        "dfl_input_contract": (
            "calibrated_forecasts_tenant_soc_context_feasible_candidate_schedules"
        ),
        "dfl_target_contract": "best_candidate_schedule_value_regret_delta_vs_v2_plus",
        "dt_input_contract": (
            "research_shadow_sequence_forecast_battery_tenant_candidate_value_return_to_go"
        ),
        "dt_action_target_contract": "candidate_id_or_schedule_family",
        "v2_plus_role": "real_schedule_value_learner_v2_plus_comparator",
        "v13_training_permission_gate_passed": False,
        "v13_blocking_context_families": "explicit_dam_publication_receipts",
        "permitted_model_training_row": False,
        "permits_model_training": False,
        "training_blocker": "apples_to_apples_research_shadow_not_v13_training",
        "promotion_gate_passed": False,
        "market_execution_gate_passed": False,
        "not_deployed_dt_control": True,
        "research_shadow_source_kind": (
            "v2_plus_strict_rows_mirrored_training_adapter"
        ),
        "research_shadow_reward_reference": "real_v2_plus_strict_rows_comparator",
        "publication_receipt_verified": False,
        "source_publication_timestamp_available": False,
        "market_availability_claim": False,
        "research_shadow_not_promotable": True,
    }
