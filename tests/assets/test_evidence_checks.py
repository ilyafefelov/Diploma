from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from smart_arbitrage.assets.checks.dfl_readiness import DFL_EVIDENCE_ASSET_CHECKS
from smart_arbitrage.defs import defs
from smart_arbitrage.evidence.quality_checks import (
    RAW_FORECAST_MODEL_NAMES,
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
