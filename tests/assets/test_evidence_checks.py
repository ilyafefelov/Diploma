from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from smart_arbitrage.assets.checks.dfl_readiness import DFL_EVIDENCE_ASSET_CHECKS
from smart_arbitrage.defs import defs
from smart_arbitrage.evidence.quality_checks import (
    DFL_ACTION_LABEL_MODEL_NAMES,
    DFL_ACTION_LABEL_TENANT_IDS,
    RAW_FORECAST_MODEL_NAMES,
    validate_dfl_action_label_panel_evidence,
    validate_dfl_training_evidence,
    validate_horizon_calibration_evidence,
    validate_real_data_benchmark_evidence,
    validate_selector_evidence,
)


TENANT_ID = "client_003_dnipro_factory"


def test_dfl_evidence_asset_checks_are_registered() -> None:
    expected_check_keys = {
        ("real_data_rolling_origin_benchmark_frame", "dnipro_thesis_grade_90_anchor_evidence"),
        ("dfl_training_frame", "dfl_training_readiness_evidence"),
        (
            "horizon_regret_weighted_forecast_strategy_benchmark_frame",
            "horizon_calibration_no_leakage_evidence",
        ),
        ("calibrated_value_aware_ensemble_frame", "calibrated_selector_cardinality_evidence"),
        ("risk_adjusted_value_gate_frame", "risk_adjusted_selector_cardinality_evidence"),
        ("dfl_action_label_panel_frame", "dfl_action_label_panel_readiness_evidence"),
        (
            "dfl_action_classifier_failure_analysis_frame",
            "dfl_action_classifier_failure_analysis_evidence",
        ),
        (
            "dfl_non_strict_oracle_upper_bound_frame",
            "dfl_non_strict_oracle_upper_bound_evidence",
        ),
        (
            "dfl_strict_failure_selector_strict_lp_benchmark_frame",
            "dfl_strict_failure_selector_evidence",
        ),
        (
            "dfl_strict_failure_selector_robustness_frame",
            "dfl_strict_failure_selector_robustness_evidence",
        ),
        (
            "dfl_strict_failure_feature_audit_frame",
            "dfl_strict_failure_feature_audit_evidence",
        ),
        (
            "dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame",
            "dfl_feature_aware_strict_failure_selector_evidence",
        ),
    }

    check_keys = {
        (check_key.asset_key.to_user_string(), check_key.name)
        for check_def in DFL_EVIDENCE_ASSET_CHECKS
        for check_key in check_def.check_keys
    }
    registered_check_keys = {
        (check_key.asset_key.to_user_string(), check_key.name)
        for check_def in defs.asset_checks or []
        for check_key in check_def.check_keys
    }

    assert expected_check_keys.issubset(check_keys)
    assert check_keys.issubset(registered_check_keys)


def test_real_data_benchmark_evidence_requires_dnipro_thesis_grade_anchors_and_models() -> None:
    outcome = validate_real_data_benchmark_evidence(_benchmark_frame(anchor_count=90))

    assert outcome.passed is True
    assert outcome.metadata["tenant_id"] == TENANT_ID
    assert outcome.metadata["anchor_count"] == 90
    assert outcome.metadata["model_count"] == 3
    assert outcome.metadata["data_quality_tiers"] == ["thesis_grade"]


def test_real_data_benchmark_evidence_fails_when_anchor_coverage_is_short() -> None:
    outcome = validate_real_data_benchmark_evidence(_benchmark_frame(anchor_count=89))

    assert outcome.passed is False
    assert "anchor_count" in outcome.description
    assert outcome.metadata["anchor_count"] == 89


def test_real_data_benchmark_evidence_fails_for_non_thesis_rows() -> None:
    frame = _benchmark_frame(anchor_count=90, data_quality_tier="demo_grade")

    outcome = validate_real_data_benchmark_evidence(frame)

    assert outcome.passed is False
    assert "thesis_grade" in outcome.description
    assert outcome.metadata["data_quality_tiers"] == ["demo_grade"]


def test_real_data_benchmark_evidence_fails_when_raw_candidate_is_missing() -> None:
    frame = _benchmark_frame(anchor_count=90).filter(pl.col("forecast_model_name") != "tft_silver_v0")

    outcome = validate_real_data_benchmark_evidence(frame)

    assert outcome.passed is False
    assert outcome.metadata["missing_models"] == ["tft_silver_v0"]


def test_dfl_training_evidence_requires_raw_and_selector_rows() -> None:
    outcome = validate_dfl_training_evidence(_dfl_training_frame(anchor_count=90))

    assert outcome.passed is True
    assert outcome.metadata["anchor_count"] == 90
    assert outcome.metadata["model_count"] == 4
    assert outcome.metadata["strategy_kinds"] == [
        "real_data_rolling_origin_benchmark",
        "value_aware_ensemble_gate",
    ]


def test_dfl_training_evidence_fails_without_selector_rows() -> None:
    frame = _dfl_training_frame(anchor_count=90).filter(
        pl.col("strategy_kind") != "value_aware_ensemble_gate"
    )

    outcome = validate_dfl_training_evidence(frame)

    assert outcome.passed is False
    assert outcome.metadata["missing_strategy_kinds"] == ["value_aware_ensemble_gate"]


def test_horizon_calibration_evidence_rejects_future_prior_anchor_metadata() -> None:
    rows = _horizon_calibration_frame(anchor_count=90).iter_rows(named=True)
    leaky_rows: list[dict[str, object]] = []
    for row in rows:
        if (
            row["anchor_timestamp"] == _first_anchor()
            and row["forecast_model_name"] == "tft_horizon_regret_weighted_calibrated_v0"
        ):
            row = {
                **row,
                "evaluation_payload": {
                    "data_quality_tier": "thesis_grade",
                    "source_forecast_model_name": "tft_silver_v0",
                    "prior_anchor_count": 99,
                    "calibration_window_anchor_count": 28,
                    "calibration_status": "calibrated",
                    "horizon": _horizon_payload(_first_anchor()),
                },
            }
        leaky_rows.append(row)
    frame = pl.DataFrame(leaky_rows)

    outcome = validate_horizon_calibration_evidence(frame)

    assert outcome.passed is False
    assert "future" in outcome.description
    assert outcome.metadata["leaky_rows"] == 1


def test_selector_evidence_requires_one_row_per_anchor() -> None:
    frame = pl.concat(
        [
            _selector_frame(
                anchor_count=90,
                strategy_kind="calibrated_value_aware_ensemble_gate",
                model_name="calibrated_value_aware_ensemble_v0",
            ),
            _selector_frame(
                anchor_count=1,
                strategy_kind="calibrated_value_aware_ensemble_gate",
                model_name="calibrated_value_aware_ensemble_v0",
            ),
        ],
        how="diagonal_relaxed",
    )

    outcome = validate_selector_evidence(
        frame,
        expected_strategy_kind="calibrated_value_aware_ensemble_gate",
        expected_model_name="calibrated_value_aware_ensemble_v0",
    )

    assert outcome.passed is False
    assert outcome.metadata["row_count"] == 91
    assert outcome.metadata["anchor_count"] == 90


def test_dfl_action_label_panel_evidence_accepts_104_anchor_all_tenant_panel() -> None:
    outcome = validate_dfl_action_label_panel_evidence(_action_label_frame(anchor_count=104))

    assert outcome.passed is True
    assert outcome.metadata["row_count"] == 1040
    assert outcome.metadata["tenant_count"] == 5
    assert outcome.metadata["model_count"] == 2
    assert outcome.metadata["train_selection_rows"] == 860
    assert outcome.metadata["final_holdout_rows"] == 180
    assert outcome.metadata["min_anchor_count_per_tenant_model"] == 104
    assert outcome.metadata["final_holdout_anchor_count_per_tenant_model"] == 18
    assert outcome.metadata["vector_length_failures"] == 0
    assert outcome.metadata["one_hot_mask_failures"] == 0
    assert outcome.metadata["train_final_overlap_count"] == 0


def test_dfl_action_label_panel_evidence_blocks_missing_tenant_or_model_and_under_90() -> None:
    missing_tenant = _action_label_frame(anchor_count=104).filter(
        pl.col("tenant_id") != "client_005_odesa_hotel"
    )
    missing_model = _action_label_frame(anchor_count=104).filter(
        pl.col("forecast_model_name") != "nbeatsx_silver_v0"
    )
    under_90 = _action_label_frame(anchor_count=89)

    missing_tenant_outcome = validate_dfl_action_label_panel_evidence(missing_tenant)
    missing_model_outcome = validate_dfl_action_label_panel_evidence(missing_model)
    under_90_outcome = validate_dfl_action_label_panel_evidence(under_90)

    assert missing_tenant_outcome.passed is False
    assert missing_tenant_outcome.metadata["missing_tenants"] == ["client_005_odesa_hotel"]
    assert missing_model_outcome.passed is False
    assert missing_model_outcome.metadata["missing_models"] == ["nbeatsx_silver_v0"]
    assert under_90_outcome.passed is False
    assert under_90_outcome.metadata["min_anchor_count_per_tenant_model"] == 89


def test_dfl_action_label_panel_evidence_blocks_bad_vectors_claims_and_masks() -> None:
    good_frame = _action_label_frame(anchor_count=104)
    bad_vectors = _replace_first_action_label_row(
        good_frame,
        forecast_price_vector_uah_mwh=[1000.0],
    )
    bad_claim = _replace_first_action_label_row(good_frame, not_full_dfl=False)
    bad_mask = _replace_first_action_label_row(
        good_frame,
        target_charge_mask=[1, *([0] * 23)],
        target_discharge_mask=[1, *([0] * 23)],
        target_hold_mask=[0, *([1] * 23)],
    )

    vector_outcome = validate_dfl_action_label_panel_evidence(bad_vectors)
    claim_outcome = validate_dfl_action_label_panel_evidence(bad_claim)
    mask_outcome = validate_dfl_action_label_panel_evidence(bad_mask)

    assert vector_outcome.passed is False
    assert vector_outcome.metadata["vector_length_failures"] == 1
    assert claim_outcome.passed is False
    assert claim_outcome.metadata["claim_flag_failure_rows"] == 1
    assert mask_outcome.passed is False
    assert mask_outcome.metadata["one_hot_mask_failures"] == 1


def test_dfl_action_label_panel_evidence_blocks_non_latest_holdout_and_overlap() -> None:
    good_frame = _action_label_frame(anchor_count=104)
    non_latest_holdout = _replace_first_action_label_row(
        good_frame,
        split_name="final_holdout",
        is_final_holdout=True,
    )
    overlap = pl.concat(
        [
            good_frame,
            good_frame.head(1).with_columns(
                pl.lit("final_holdout").alias("split_name"),
                pl.lit(True).alias("is_final_holdout"),
            ),
        ],
        how="diagonal_relaxed",
    )

    non_latest_outcome = validate_dfl_action_label_panel_evidence(non_latest_holdout)
    overlap_outcome = validate_dfl_action_label_panel_evidence(overlap)

    assert non_latest_outcome.passed is False
    assert non_latest_outcome.metadata["non_latest_holdout_groups"] == 1
    assert overlap_outcome.passed is False
    assert overlap_outcome.metadata["train_final_overlap_count"] == 1


def _first_anchor() -> datetime:
    return datetime(2026, 1, 22, 23, tzinfo=UTC)


def _anchors(anchor_count: int) -> list[datetime]:
    return [_first_anchor() + timedelta(days=index) for index in range(anchor_count)]


def _benchmark_frame(
    *,
    anchor_count: int,
    data_quality_tier: str = "thesis_grade",
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    generated_at = datetime(2026, 5, 6, 22, 57, 36, tzinfo=UTC)
    for anchor in _anchors(anchor_count):
        for rank, model_name in enumerate(RAW_FORECAST_MODEL_NAMES, start=1):
            rows.append(
                {
                    "evaluation_id": f"{TENANT_ID}:{anchor.isoformat()}:{model_name}",
                    "tenant_id": TENANT_ID,
                    "forecast_model_name": model_name,
                    "strategy_kind": "real_data_rolling_origin_benchmark",
                    "market_venue": "DAM",
                    "anchor_timestamp": anchor,
                    "generated_at": generated_at,
                    "horizon_hours": 24,
                    "starting_soc_fraction": 0.52,
                    "starting_soc_source": "tenant_default",
                    "decision_value_uah": 1000.0 - rank,
                    "forecast_objective_value_uah": 900.0,
                    "oracle_value_uah": 1000.0,
                    "regret_uah": float(rank),
                    "regret_ratio": float(rank) / 1000.0,
                    "total_degradation_penalty_uah": 10.0,
                    "total_throughput_mwh": 0.1,
                    "committed_action": "HOLD",
                    "committed_power_mw": 0.0,
                    "rank_by_regret": rank,
                    "evaluation_payload": {
                        "data_quality_tier": data_quality_tier,
                        "observed_coverage_ratio": 1.0 if data_quality_tier == "thesis_grade" else 0.5,
                        "academic_scope": "Real-data rolling-origin DAM benchmark.",
                        "horizon": _horizon_payload(anchor),
                    },
                }
            )
    return pl.DataFrame(rows)


def _dfl_training_frame(*, anchor_count: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for anchor in _anchors(anchor_count):
        for model_name in [*RAW_FORECAST_MODEL_NAMES, "value_aware_ensemble_v0"]:
            rows.append(
                {
                    "training_example_id": f"{TENANT_ID}:{anchor.isoformat()}:{model_name}",
                    "tenant_id": TENANT_ID,
                    "anchor_timestamp": anchor,
                    "forecast_model_name": model_name,
                    "strategy_kind": (
                        "value_aware_ensemble_gate"
                        if model_name == "value_aware_ensemble_v0"
                        else "real_data_rolling_origin_benchmark"
                    ),
                    "data_quality_tier": "thesis_grade",
                    "observed_coverage_ratio": 1.0,
                    "regret_uah": 100.0,
                }
            )
    return pl.DataFrame(rows)


def _horizon_calibration_frame(*, anchor_count: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    generated_at = datetime(2026, 5, 6, 22, 57, 36, tzinfo=UTC)
    model_names = [
        "strict_similar_day",
        "tft_silver_v0",
        "nbeatsx_silver_v0",
        "tft_horizon_regret_weighted_calibrated_v0",
        "nbeatsx_horizon_regret_weighted_calibrated_v0",
    ]
    for prior_count, anchor in enumerate(_anchors(anchor_count)):
        for model_name in model_names:
            payload: dict[str, object] = {
                "data_quality_tier": "thesis_grade",
                "observed_coverage_ratio": 1.0,
                "academic_scope": "Horizon-aware regret-weighted forecast calibration benchmark; not full differentiable DFL.",
                "horizon": _horizon_payload(anchor),
            }
            if model_name.endswith("_horizon_regret_weighted_calibrated_v0"):
                payload.update(
                    {
                        "source_forecast_model_name": (
                            "tft_silver_v0"
                            if model_name.startswith("tft")
                            else "nbeatsx_silver_v0"
                        ),
                        "prior_anchor_count": prior_count,
                        "calibration_window_anchor_count": min(prior_count, 28),
                        "calibration_status": (
                            "calibrated"
                            if prior_count >= 14
                            else "insufficient_prior_history"
                        ),
                    }
                )
            else:
                payload.update(
                    {
                        "source_forecast_model_name": model_name,
                        "prior_anchor_count": None,
                        "calibration_status": "comparator_source_row",
                    }
                )
            rows.append(
                {
                    "evaluation_id": f"{TENANT_ID}:horizon:{anchor.isoformat()}:{model_name}",
                    "tenant_id": TENANT_ID,
                    "forecast_model_name": model_name,
                    "strategy_kind": "horizon_regret_weighted_forecast_calibration_benchmark",
                    "market_venue": "DAM",
                    "anchor_timestamp": anchor,
                    "generated_at": generated_at,
                    "horizon_hours": 24,
                    "evaluation_payload": payload,
                    "regret_uah": 100.0,
                }
            )
    return pl.DataFrame(rows)


def _selector_frame(
    *,
    anchor_count: int,
    strategy_kind: str,
    model_name: str,
) -> pl.DataFrame:
    generated_at = datetime(2026, 5, 6, 22, 57, 36, tzinfo=UTC)
    return pl.DataFrame(
        [
            {
                "evaluation_id": f"{TENANT_ID}:{strategy_kind}:{anchor.isoformat()}",
                "tenant_id": TENANT_ID,
                "forecast_model_name": model_name,
                "strategy_kind": strategy_kind,
                "anchor_timestamp": anchor,
                "generated_at": generated_at,
                "evaluation_payload": {
                    "data_quality_tier": "thesis_grade",
                    "academic_scope": "Selector diagnostic, not full DFL.",
                    "selected_model_name": "strict_similar_day",
                    "horizon": _horizon_payload(anchor),
                },
            }
            for anchor in _anchors(anchor_count)
        ]
    )


def _horizon_payload(anchor: datetime) -> list[dict[str, object]]:
    return [
        {
            "step_index": step_index,
            "interval_start": (anchor + timedelta(hours=step_index + 1)).isoformat(),
            "forecast_price_uah_mwh": 1000.0 + step_index,
            "actual_price_uah_mwh": 1005.0 + step_index,
            "net_power_mw": 0.0,
            "degradation_penalty_uah": 0.0,
        }
        for step_index in range(24)
    ]


def _action_label_frame(*, anchor_count: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    generated_at = datetime(2026, 5, 7, 20, 8, 52, tzinfo=UTC)
    for tenant_id in DFL_ACTION_LABEL_TENANT_IDS:
        for model_name in DFL_ACTION_LABEL_MODEL_NAMES:
            for anchor_index, anchor in enumerate(_anchors(anchor_count)):
                is_final_holdout = anchor_index >= anchor_count - 18
                masks = _action_masks(anchor_index)
                rows.append(
                    {
                        "action_label_id": f"{tenant_id}:{model_name}:{anchor:%Y%m%dT%H%M}:action-label-v1",
                        "evaluation_id": f"{tenant_id}:{model_name}:{anchor.isoformat()}",
                        "strict_baseline_evaluation_id": f"{tenant_id}:strict:{anchor.isoformat()}",
                        "tenant_id": tenant_id,
                        "anchor_timestamp": anchor,
                        "split_name": "final_holdout" if is_final_holdout else "train_selection",
                        "is_final_holdout": is_final_holdout,
                        "horizon_start": anchor + timedelta(hours=1),
                        "horizon_end": anchor + timedelta(hours=24),
                        "horizon_hours": 24,
                        "market_venue": "DAM",
                        "currency": "UAH",
                        "forecast_model_name": model_name,
                        "source_strategy_kind": "real_data_rolling_origin_benchmark",
                        "strict_baseline_forecast_model_name": "strict_similar_day",
                        "target_strategy_name": "oracle_lp",
                        "forecast_price_vector_uah_mwh": _float_vector(1000.0 + anchor_index),
                        "actual_price_vector_uah_mwh": _float_vector(1100.0 + anchor_index),
                        "candidate_signed_dispatch_vector_mw": _dispatch_vector(),
                        "strict_baseline_signed_dispatch_vector_mw": _dispatch_vector(),
                        "oracle_signed_dispatch_vector_mw": _oracle_dispatch_vector(anchor_index),
                        "oracle_charge_mw_vector": [0.25 if value < 0 else 0.0 for value in _oracle_dispatch_vector(anchor_index)],
                        "oracle_discharge_mw_vector": [0.25 if value > 0 else 0.0 for value in _oracle_dispatch_vector(anchor_index)],
                        "oracle_soc_before_mwh_vector": _float_vector(0.5),
                        "oracle_soc_after_mwh_vector": _float_vector(0.5),
                        "oracle_degradation_penalty_vector_uah": _float_vector(1.0),
                        "target_charge_mask": masks["charge"],
                        "target_discharge_mask": masks["discharge"],
                        "target_hold_mask": masks["hold"],
                        "candidate_net_value_uah": 900.0,
                        "strict_baseline_net_value_uah": 950.0,
                        "oracle_net_value_uah": 1000.0,
                        "candidate_regret_uah": 100.0,
                        "strict_baseline_regret_uah": 50.0,
                        "regret_delta_vs_strict_baseline_uah": 50.0,
                        "candidate_total_throughput_mwh": 0.2,
                        "strict_baseline_total_throughput_mwh": 0.2,
                        "candidate_total_degradation_penalty_uah": 3.0,
                        "strict_baseline_total_degradation_penalty_uah": 2.0,
                        "candidate_safety_violation_count": 0,
                        "strict_baseline_safety_violation_count": 0,
                        "data_quality_tier": "thesis_grade",
                        "observed_coverage_ratio": 1.0,
                        "claim_scope": "dfl_action_label_panel_not_full_dfl",
                        "not_full_dfl": True,
                        "not_market_execution": True,
                        "generated_at": generated_at,
                    }
                )
    return pl.DataFrame(rows)


def _replace_first_action_label_row(frame: pl.DataFrame, **updates: object) -> pl.DataFrame:
    rows = frame.iter_rows(named=True)
    updated_rows: list[dict[str, object]] = []
    for row_index, row in enumerate(rows):
        updated_rows.append({**row, **updates} if row_index == 0 else row)
    return pl.DataFrame(updated_rows)


def _float_vector(start: float) -> list[float]:
    return [start + float(index) for index in range(24)]


def _dispatch_vector() -> list[float]:
    return [0.0 for _ in range(24)]


def _oracle_dispatch_vector(anchor_index: int) -> list[float]:
    if anchor_index % 3 == 0:
        return [-0.25, *([0.0] * 23)]
    if anchor_index % 3 == 1:
        return [0.25, *([0.0] * 23)]
    return [0.0 for _ in range(24)]


def _action_masks(anchor_index: int) -> dict[str, list[int]]:
    dispatch = _oracle_dispatch_vector(anchor_index)
    charge = [1 if value < 0 else 0 for value in dispatch]
    discharge = [1 if value > 0 else 0 for value in dispatch]
    hold = [1 if value == 0 else 0 for value in dispatch]
    return {"charge": charge, "discharge": discharge, "hold": hold}
