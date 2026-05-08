from __future__ import annotations

from datetime import datetime, timedelta
import json

import polars as pl
import pytest

from smart_arbitrage.dfl.failure_analysis import (
    FAILURE_ANALYSIS_CLAIM_SCOPE,
    build_dfl_action_classifier_failure_analysis_frame,
    validate_dfl_action_classifier_failure_analysis_evidence,
)


TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
MODELS: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0")
FIRST_ANCHOR = datetime(2026, 1, 1, 23)
GENERATED_AT = datetime(2026, 5, 8, 9)


def test_failure_analysis_counts_regret_weighted_confusion_and_blocked_comparison() -> None:
    action_labels = _action_label_frame(tenant_ids=TENANTS[:1], model_names=MODELS[:1], anchor_count=5)

    result = build_dfl_action_classifier_failure_analysis_frame(
        action_labels,
        _strict_projection_frame(
            action_labels,
            variant_prefix="dfl_action_classifier_v0_",
            strategy_kind="dfl_action_classifier_strict_lp_projection",
            candidate_regret_uah=1000.0,
            predicted_labels=["hold", "hold", "hold", "hold"],
        ),
        _strict_projection_frame(
            action_labels,
            variant_prefix="dfl_value_aware_action_classifier_v1_",
            strategy_kind="dfl_value_aware_action_classifier_strict_lp_projection",
            candidate_regret_uah=1250.0,
            predicted_labels=["charge", "discharge", "discharge", "hold"],
        ),
    )

    plain = _analysis_row(
        result,
        tenant_id=TENANTS[0],
        source_model_name=MODELS[0],
        classifier_variant="plain_majority",
    )
    value_aware = _analysis_row(
        result,
        tenant_id=TENANTS[0],
        source_model_name=MODELS[0],
        classifier_variant="value_aware_weighted_majority",
    )
    confusion_counts = json.loads(str(plain["confusion_counts_json"]))

    assert result.height == 2
    assert sum(confusion_counts.values()) == plain["label_hour_count"]
    assert plain["missed_charge_hours"] > 0
    assert plain["missed_discharge_hours"] > 0
    assert plain["active_recall"] < 1.0
    assert value_aware["false_active_hours"] > plain["false_active_hours"]
    assert value_aware["regret_delta_vs_plain_uah"] > 0.0
    assert plain["claim_scope"] == FAILURE_ANALYSIS_CLAIM_SCOPE
    assert plain["not_full_dfl"] is True
    assert plain["not_market_execution"] is True


def test_failure_analysis_final_holdout_actual_mutation_changes_price_rank_diagnostics_only() -> None:
    action_labels = _action_label_frame(tenant_ids=TENANTS[:1], model_names=MODELS[:1], anchor_count=5)
    plain_strict = _strict_projection_frame(
        action_labels,
        variant_prefix="dfl_action_classifier_v0_",
        strategy_kind="dfl_action_classifier_strict_lp_projection",
        candidate_regret_uah=1000.0,
        predicted_labels=["charge", "hold", "discharge", "hold"],
    )
    value_strict = _strict_projection_frame(
        action_labels,
        variant_prefix="dfl_value_aware_action_classifier_v1_",
        strategy_kind="dfl_value_aware_action_classifier_strict_lp_projection",
        candidate_regret_uah=1250.0,
        predicted_labels=["charge", "discharge", "discharge", "hold"],
    )
    mutated_labels = action_labels.with_columns(
        pl.when(pl.col("split_name") == "final_holdout")
        .then(pl.lit([400.0, 300.0, 200.0, 100.0]))
        .otherwise(pl.col("actual_price_vector_uah_mwh"))
        .alias("actual_price_vector_uah_mwh")
    )

    original = build_dfl_action_classifier_failure_analysis_frame(
        action_labels,
        plain_strict,
        value_strict,
    )
    mutated = build_dfl_action_classifier_failure_analysis_frame(
        mutated_labels,
        plain_strict,
        value_strict,
    )

    original_plain = _analysis_row(
        original,
        tenant_id=TENANTS[0],
        source_model_name=MODELS[0],
        classifier_variant="plain_majority",
    )
    mutated_plain = _analysis_row(
        mutated,
        tenant_id=TENANTS[0],
        source_model_name=MODELS[0],
        classifier_variant="plain_majority",
    )

    assert original_plain["confusion_counts_json"] == mutated_plain["confusion_counts_json"]
    assert original_plain["mean_candidate_regret_uah"] == mutated_plain["mean_candidate_regret_uah"]
    assert original_plain["bottom_price_rank_miss_count"] != mutated_plain["bottom_price_rank_miss_count"]


def test_failure_analysis_evidence_check_passes_on_all_tenant_90_holdout_panel() -> None:
    action_labels = _action_label_frame(tenant_ids=TENANTS, model_names=MODELS, anchor_count=20)
    analysis = build_dfl_action_classifier_failure_analysis_frame(
        action_labels,
        _strict_projection_frame(
            action_labels,
            variant_prefix="dfl_action_classifier_v0_",
            strategy_kind="dfl_action_classifier_strict_lp_projection",
            candidate_regret_uah=1000.0,
            predicted_labels=["hold", "hold", "hold", "hold"],
        ),
        _strict_projection_frame(
            action_labels,
            variant_prefix="dfl_value_aware_action_classifier_v1_",
            strategy_kind="dfl_value_aware_action_classifier_strict_lp_projection",
            candidate_regret_uah=1250.0,
            predicted_labels=["charge", "discharge", "discharge", "hold"],
        ),
    )

    outcome = validate_dfl_action_classifier_failure_analysis_evidence(analysis)

    assert outcome.passed is True
    assert outcome.metadata["tenant_count"] == 5
    assert outcome.metadata["source_model_count"] == 2
    assert outcome.metadata["variant_count"] == 2
    assert outcome.metadata["final_holdout_tenant_anchors_by_source_model"] == {
        "nbeatsx_silver_v0": 90,
        "tft_silver_v0": 90,
    }
    assert outcome.metadata["claim_flag_failure_rows"] == 0


def test_failure_analysis_evidence_check_blocks_missing_or_short_evidence() -> None:
    action_labels = _action_label_frame(tenant_ids=TENANTS, model_names=MODELS, anchor_count=20)
    analysis = build_dfl_action_classifier_failure_analysis_frame(
        action_labels,
        _strict_projection_frame(
            action_labels,
            variant_prefix="dfl_action_classifier_v0_",
            strategy_kind="dfl_action_classifier_strict_lp_projection",
            candidate_regret_uah=1000.0,
            predicted_labels=["hold", "hold", "hold", "hold"],
        ),
        _strict_projection_frame(
            action_labels,
            variant_prefix="dfl_value_aware_action_classifier_v1_",
            strategy_kind="dfl_value_aware_action_classifier_strict_lp_projection",
            candidate_regret_uah=1250.0,
            predicted_labels=["charge", "discharge", "discharge", "hold"],
        ),
    )
    missing_tenant = analysis.filter(pl.col("tenant_id") != TENANTS[-1])
    under_90 = analysis.with_columns(
        pl.when(
            (pl.col("tenant_id") == TENANTS[0])
            & (pl.col("source_model_name") == MODELS[0])
            & (pl.col("classifier_variant") == "plain_majority")
        )
        .then(pl.lit(17))
        .otherwise(pl.col("final_holdout_anchor_count"))
        .alias("final_holdout_anchor_count")
    )
    bad_claim = analysis.with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit(False))
        .otherwise(pl.col("not_full_dfl"))
        .alias("not_full_dfl")
    )

    missing_tenant_outcome = validate_dfl_action_classifier_failure_analysis_evidence(missing_tenant)
    under_90_outcome = validate_dfl_action_classifier_failure_analysis_evidence(under_90)
    bad_claim_outcome = validate_dfl_action_classifier_failure_analysis_evidence(bad_claim)

    assert missing_tenant_outcome.passed is False
    assert missing_tenant_outcome.metadata["missing_tenants"] == [TENANTS[-1]]
    assert under_90_outcome.passed is False
    assert under_90_outcome.metadata["final_holdout_tenant_anchors_by_source_model"][MODELS[0]] == 89
    assert bad_claim_outcome.passed is False
    assert bad_claim_outcome.metadata["claim_flag_failure_rows"] == 1


def test_failure_analysis_helper_rejects_bad_inputs() -> None:
    action_labels = _action_label_frame(tenant_ids=TENANTS[:1], model_names=MODELS[:1], anchor_count=5)
    plain_strict = _strict_projection_frame(
        action_labels,
        variant_prefix="dfl_action_classifier_v0_",
        strategy_kind="dfl_action_classifier_strict_lp_projection",
        candidate_regret_uah=1000.0,
        predicted_labels=["hold", "hold", "hold", "hold"],
    )
    value_strict = _strict_projection_frame(
        action_labels,
        variant_prefix="dfl_value_aware_action_classifier_v1_",
        strategy_kind="dfl_value_aware_action_classifier_strict_lp_projection",
        candidate_regret_uah=1250.0,
        predicted_labels=["charge", "discharge", "discharge", "hold"],
    )
    non_thesis = action_labels.with_columns(pl.lit("demo_grade").alias("data_quality_tier"))
    overlap = pl.concat(
            [
                action_labels,
                action_labels.head(1).with_columns(
                    pl.lit("train_selection").alias("split_name"),
                    pl.lit(False).alias("is_final_holdout"),
                ),
            ],
        how="diagonal_relaxed",
    )
    missing_strict = plain_strict.filter(pl.col("forecast_model_name") != "strict_similar_day")

    with pytest.raises(ValueError, match="thesis_grade"):
        build_dfl_action_classifier_failure_analysis_frame(non_thesis, plain_strict, value_strict)
    with pytest.raises(ValueError, match="overlap"):
        build_dfl_action_classifier_failure_analysis_frame(overlap, plain_strict, value_strict)
    with pytest.raises(ValueError, match="missing strict_similar_day"):
        build_dfl_action_classifier_failure_analysis_frame(action_labels, missing_strict, value_strict)


def _analysis_row(
    frame: pl.DataFrame,
    *,
    tenant_id: str,
    source_model_name: str,
    classifier_variant: str,
) -> dict[str, object]:
    return frame.filter(
        (pl.col("tenant_id") == tenant_id)
        & (pl.col("source_model_name") == source_model_name)
        & (pl.col("classifier_variant") == classifier_variant)
    ).row(0, named=True)


def _action_label_frame(
    *,
    tenant_ids: tuple[str, ...],
    model_names: tuple[str, ...],
    anchor_count: int,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in tenant_ids:
        for model_name in model_names:
            for anchor_index in range(anchor_count):
                is_final = anchor_index >= anchor_count - 18
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                rows.append(
                    {
                        "tenant_id": tenant_id,
                        "forecast_model_name": model_name,
                        "anchor_timestamp": anchor,
                        "split_name": "final_holdout" if is_final else "train_selection",
                        "is_final_holdout": is_final,
                        "horizon_hours": 4,
                        "forecast_price_vector_uah_mwh": [110.0, 210.0, 390.0, 300.0],
                        "actual_price_vector_uah_mwh": [100.0, 200.0, 400.0, 300.0],
                        "target_charge_mask": [1, 0, 0, 0],
                        "target_discharge_mask": [0, 0, 1, 0],
                        "target_hold_mask": [0, 1, 0, 1],
                        "candidate_signed_dispatch_vector_mw": [-0.5, 0.0, 0.5, 0.0],
                        "strict_baseline_signed_dispatch_vector_mw": [-0.5, 0.0, 0.5, 0.0],
                        "oracle_signed_dispatch_vector_mw": [-0.5, 0.0, 0.5, 0.0],
                        "candidate_regret_uah": 900.0,
                        "strict_baseline_regret_uah": 200.0,
                        "candidate_net_value_uah": 2600.0,
                        "strict_baseline_net_value_uah": 3300.0,
                        "oracle_net_value_uah": 3500.0,
                        "candidate_safety_violation_count": 0,
                        "strict_baseline_safety_violation_count": 0,
                        "data_quality_tier": "thesis_grade",
                        "observed_coverage_ratio": 1.0,
                        "not_full_dfl": True,
                        "not_market_execution": True,
                    }
                )
    return pl.DataFrame(rows)


def _strict_projection_frame(
    action_label_frame: pl.DataFrame,
    *,
    variant_prefix: str,
    strategy_kind: str,
    candidate_regret_uah: float,
    predicted_labels: list[str],
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for action_row in action_label_frame.filter(pl.col("split_name") == "final_holdout").iter_rows(named=True):
        tenant_id = str(action_row["tenant_id"])
        source_model_name = str(action_row["forecast_model_name"])
        anchor = action_row["anchor_timestamp"]
        strict_regret = 200.0
        strict_value = 3300.0
        oracle_value = 3500.0
        rows.append(
            _projection_row(
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                forecast_model_name="strict_similar_day",
                anchor=anchor,
                strategy_kind=strategy_kind,
                decision_value_uah=strict_value,
                oracle_value_uah=oracle_value,
                regret_uah=strict_regret,
                predicted_labels=None,
            )
        )
        rows.append(
            _projection_row(
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                forecast_model_name=f"{variant_prefix}{source_model_name}",
                anchor=anchor,
                strategy_kind=strategy_kind,
                decision_value_uah=oracle_value - candidate_regret_uah,
                oracle_value_uah=oracle_value,
                regret_uah=candidate_regret_uah,
                predicted_labels=predicted_labels,
            )
        )
    return pl.DataFrame(rows)


def _projection_row(
    *,
    tenant_id: str,
    source_model_name: str,
    forecast_model_name: str,
    anchor: object,
    strategy_kind: str,
    decision_value_uah: float,
    oracle_value_uah: float,
    regret_uah: float,
    predicted_labels: list[str] | None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "source_forecast_model_name": source_model_name,
        "split_name": "final_holdout",
        "uses_final_holdout_for_training": False,
        "not_full_dfl": True,
        "not_market_execution": True,
        "horizon": [
            {
                "step_index": step_index,
                "interval_start": FIRST_ANCHOR + timedelta(hours=step_index + 1),
                "actual_price_uah_mwh": price,
                "net_power_mw": 0.0,
            }
            for step_index, price in enumerate([100.0, 200.0, 400.0, 300.0])
        ],
    }
    if predicted_labels is not None:
        payload["predicted_action_labels"] = predicted_labels
        payload["projected_signed_dispatch_vector_mw"] = [0.0, 0.0, 0.0, 0.0]
    return {
        "evaluation_id": f"{tenant_id}:{source_model_name}:{forecast_model_name}:{anchor}",
        "tenant_id": tenant_id,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": strategy_kind,
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "horizon_hours": 4,
        "decision_value_uah": decision_value_uah,
        "oracle_value_uah": oracle_value_uah,
        "regret_uah": regret_uah,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": 0.0,
        "evaluation_payload": payload,
    }
