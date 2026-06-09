"""Candidate-value DFL v3 challenger anchored to frozen V2+ evidence."""

from __future__ import annotations

from datetime import datetime
from statistics import mean
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl import schedule_value_learner_v2_plus as v2_plus
from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

CANDIDATE_VALUE_DFL_V3_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_dfl_v3_not_full_dfl"
)
CANDIDATE_VALUE_DFL_V3_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_dfl_v3_strict_lp_gate_not_full_dfl"
)
CANDIDATE_VALUE_DFL_V3_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_candidate_value_dfl_v3_strict_lp_benchmark"
)
CANDIDATE_VALUE_DFL_V3_PREFIX: Final[str] = "dfl_candidate_value_dfl_v3_"
CANDIDATE_VALUE_DFL_V3_ACADEMIC_SCOPE: Final[str] = (
    "Candidate-level value scorer over expanded feasible LP-scored schedules. "
    "The scorer is selected on train/prior anchors with a regret-weighted "
    "pairwise/listwise objective and falls back to frozen V2+ unless prior "
    "evidence predicts improvement. This is not full DFL and not market execution."
)
CANDIDATE_VALUE_DFL_V3_FAILURE_AUDIT_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_dfl_v3_failure_audit_not_full_dfl"
)

DFL_SCHEDULE_CANDIDATE_LIBRARY_V3_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_candidate_library_v3_not_full_dfl"
)
CANDIDATE_FAMILY_STRICT_NEIGHBORHOOD_V3: Final[str] = "strict_neighborhood_v3"
CANDIDATE_FAMILY_SOC_TERMINAL_V3: Final[str] = "soc_terminal_target_v3"
CANDIDATE_FAMILY_PEAK_TROUGH_SHIFT_V3: Final[str] = "peak_trough_timing_shift_v3"
CANDIDATE_FAMILY_UNCERTAINTY_RISK_V3: Final[str] = "uncertainty_risk_schedule_v3"
CANDIDATE_FAMILY_DEGRADATION_SWEEP_V3: Final[str] = "degradation_price_sweep_v3"
CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V3: Final[str] = (
    "oracle_neighborhood_diagnostic_v3"
)
CANDIDATE_FAMILY_PRIOR_BEST_TEMPLATE_V3: Final[str] = "prior_best_family_template_v3"
CANDIDATE_FAMILY_PRIOR_ORACLE_RESIDUAL_V3: Final[str] = (
    "prior_oracle_residual_template_v3"
)

V3_GENERATED_CANDIDATE_FAMILIES: Final[frozenset[str]] = frozenset(
    {
        CANDIDATE_FAMILY_STRICT_NEIGHBORHOOD_V3,
        CANDIDATE_FAMILY_SOC_TERMINAL_V3,
        CANDIDATE_FAMILY_PEAK_TROUGH_SHIFT_V3,
        CANDIDATE_FAMILY_UNCERTAINTY_RISK_V3,
        CANDIDATE_FAMILY_DEGRADATION_SWEEP_V3,
        CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V3,
        CANDIDATE_FAMILY_PRIOR_BEST_TEMPLATE_V3,
        CANDIDATE_FAMILY_PRIOR_ORACLE_RESIDUAL_V3,
    }
)
DFL_CANDIDATE_VALUE_LABEL_PANEL_V3_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_label_panel_v3_not_full_dfl"
)
REQUIRED_MODEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "learner_model_name",
        "selected_value_profile_name",
        "selected_scorer_type",
        "selected_feature_weights",
        "fallback_to_v2_plus",
        "train_anchor_count",
        "final_holdout_anchor_count",
        "selected_train_mean_regret_uah",
        "selected_final_mean_regret_uah",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
LEARNED_SCORER_TYPE: Final[str] = "learned_linear_candidate_value_v3"
LEARNED_SCORER_PROFILE_NAME: Final[str] = "learned_candidate_value_ridge_v3"
LEARNED_SCORER_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "selector_feature_prior_family_mean_regret_uah",
    "selector_feature_forecast_spread_uah_mwh",
    "selector_feature_forecast_objective_value_uah",
    "selector_feature_total_throughput_mwh",
    "selector_feature_total_degradation_penalty_uah",
    "selector_feature_soc_min_slack_fraction",
)
REQUIRED_FAILURE_AUDIT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source_model_name",
        "audit_grain",
        "candidate_family",
        "row_count",
        "anchor_count",
        "mean_regret_uah",
        "v2_plus_mean_regret_uah",
        "mean_delta_vs_v2_plus_uah",
        "win_rate_vs_v2_plus",
        "diagnosis",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
        "market_execution_enabled",
    }
)
REQUIRED_STRICT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "evaluation_id",
        "tenant_id",
        "source_model_name",
        "forecast_model_name",
        "strategy_kind",
        "market_venue",
        "anchor_timestamp",
        "generated_at",
        "horizon_hours",
        "starting_soc_fraction",
        "starting_soc_source",
        "forecast_objective_value_uah",
        "regret_ratio",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "committed_action",
        "committed_power_mw",
        "rank_by_regret",
        "regret_uah",
        "selection_role",
        "evaluation_payload",
    }
)
REQUIRED_LABEL_PANEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "candidate_family",
        "candidate_model_name",
        "anchor_timestamp",
        "split_name",
        "selector_feature_prior_family_mean_regret_uah",
        "selector_feature_forecast_spread_uah_mwh",
        "selector_feature_total_throughput_mwh",
        "selector_feature_total_degradation_penalty_uah",
        "selector_feature_soc_min_slack_fraction",
        "label_regret_uah",
        "label_decision_value_uah",
        "label_oracle_value_uah",
        "label_is_anchor_best_candidate",
        "label_regret_margin_to_anchor_best_uah",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)

_VALUE_PROFILES: Final[tuple[dict[str, float | str], ...]] = (
    {
        "name": "prior_regret_value",
        "prior": 1.0,
        "spread": 0.0,
        "throughput": 0.0,
        "degradation": 1.0,
        "soc_slack": 0.0,
        "teacher_bonus": 0.0,
    },
    {
        "name": "spread_value_ranker",
        "prior": 1.0,
        "spread": -0.02,
        "throughput": 5.0,
        "degradation": 1.0,
        "soc_slack": -25.0,
        "teacher_bonus": -50.0,
    },
    {
        "name": "risk_adjusted_value_ranker",
        "prior": 1.0,
        "spread": 0.01,
        "throughput": 15.0,
        "degradation": 2.0,
        "soc_slack": -10.0,
        "teacher_bonus": -25.0,
    },
    {
        "name": "teacher_weighted_value_ranker",
        "prior": 0.8,
        "spread": -0.01,
        "throughput": 2.0,
        "degradation": 1.0,
        "soc_slack": -10.0,
        "teacher_bonus": -150.0,
    },
)


def candidate_value_dfl_v3_model_name(source_model_name: str) -> str:
    """Return the stable candidate-value DFL v3 model name."""

    return f"{CANDIDATE_VALUE_DFL_V3_PREFIX}{source_model_name}"


def build_dfl_schedule_candidate_library_v3_frame(
    schedule_candidate_library_v2_plus_frame: pl.DataFrame,
    *,
    strict_neighborhood_shift_hours: tuple[int, ...] = (-2, -1, 1, 2),
    terminal_target_shift_uah_mwh: tuple[float, ...] = (-150.0, 150.0),
    peak_trough_delta_uah_mwh: float = 350.0,
    uncertainty_spread_scales: tuple[float, ...] = (0.7, 1.1),
    degradation_spread_scales: tuple[float, ...] = (0.6, 0.85),
    include_train_oracle_neighborhood: bool = True,
    max_train_generation_anchor_count_per_tenant: int | None = 60,
    min_prior_template_anchor_count: int = 3,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Expand V2+ schedules with V3 failure-mode candidate families.

    The oracle-neighborhood family is explicitly train-only diagnostic evidence.
    It may inform objective design but is never available on final holdout.
    """

    v2._validate_library_frame(schedule_candidate_library_v2_plus_frame)
    if not strict_neighborhood_shift_hours:
        raise ValueError("strict_neighborhood_shift_hours must not be empty.")
    if any(scale <= 0.0 for scale in uncertainty_spread_scales):
        raise ValueError("uncertainty_spread_scales must contain positive values.")
    if any(scale <= 0.0 for scale in degradation_spread_scales):
        raise ValueError("degradation_spread_scales must contain positive values.")
    if (
        max_train_generation_anchor_count_per_tenant is not None
        and max_train_generation_anchor_count_per_tenant < 0
    ):
        raise ValueError(
            "max_train_generation_anchor_count_per_tenant must be non-negative or null."
        )
    if min_prior_template_anchor_count < 0:
        raise ValueError("min_prior_template_anchor_count must be non-negative.")
    resolved_generated_at = generated_at or v2._latest_generated_at(
        schedule_candidate_library_v2_plus_frame
    )
    rows = [
        _with_v3_library_claim(row, version="v3_source")
        for row in schedule_candidate_library_v2_plus_frame.iter_rows(named=True)
    ]
    grouped = v2_plus._rows_by_tenant_source_anchor(schedule_candidate_library_v2_plus_frame)
    generation_keys = _v3_generation_anchor_keys(
        grouped,
        max_train_generation_anchor_count_per_tenant=(
            max_train_generation_anchor_count_per_tenant
        ),
    )
    for key in sorted(grouped, key=lambda item: (item[0], item[1], item[2])):
        if key not in generation_keys:
            continue
        tenant_id, source_model_name, anchor_timestamp = key
        anchor_rows = grouped[key]
        strict_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_STRICT)
        raw_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_RAW)
        strict_forecast = v2._float_list(
            strict_row["forecast_price_uah_mwh_vector"],
            field_name="strict forecast",
        )
        raw_forecast = v2._float_list(
            raw_row["forecast_price_uah_mwh_vector"],
            field_name="raw forecast",
        )
        prior_template_rows = _prior_template_rows(
            grouped,
            tenant_id=tenant_id,
            source_model_name=source_model_name,
            anchor_timestamp=anchor_timestamp,
        )
        prior_template_anchor_count = len(v2._anchor_set(prior_template_rows))
        if prior_template_anchor_count >= min_prior_template_anchor_count:
            rows.extend(
                _prior_template_candidates(
                    strict_row,
                    raw_forecast=raw_forecast,
                    prior_rows=prior_template_rows,
                    source_model_name=source_model_name,
                    generated_at=resolved_generated_at,
                    prior_template_anchor_count=prior_template_anchor_count,
                )
            )
        for shift_hours in strict_neighborhood_shift_hours:
            if shift_hours == 0:
                continue
            rows.append(
                _generated_v3_candidate(
                    strict_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_STRICT_NEIGHBORHOOD_V3,
                    candidate_model_name=(
                        f"dfl_candidate_library_v3_strict_shift_{source_model_name}_"
                        f"{shift_hours:+d}"
                    ),
                    forecast_prices=v2_plus._shift_vector(strict_forecast, shift_hours),
                    generated_at=resolved_generated_at,
                    metadata={"strict_neighborhood_shift_hours": shift_hours},
                )
            )
        for shift in terminal_target_shift_uah_mwh:
            rows.append(
                _generated_v3_candidate(
                    strict_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_SOC_TERMINAL_V3,
                    candidate_model_name=(
                        f"dfl_candidate_library_v3_soc_terminal_{source_model_name}_"
                        f"{shift:+.0f}"
                    ),
                    forecast_prices=v2_plus._terminal_target_adjustment(
                        raw_forecast,
                        shift_uah_mwh=shift,
                    ),
                    generated_at=resolved_generated_at,
                    metadata={"terminal_target_shift_uah_mwh": shift},
                )
            )
        rows.append(
            _generated_v3_candidate(
                strict_row,
                source_model_name=source_model_name,
                candidate_family=CANDIDATE_FAMILY_PEAK_TROUGH_SHIFT_V3,
                candidate_model_name=f"dfl_candidate_library_v3_peak_trough_{source_model_name}",
                forecast_prices=v2_plus._rank_extrema_perturbation(
                    raw_forecast,
                    delta=peak_trough_delta_uah_mwh,
                ),
                generated_at=resolved_generated_at,
                metadata={"peak_trough_delta_uah_mwh": peak_trough_delta_uah_mwh},
            )
        )
        for scale in uncertainty_spread_scales:
            rows.append(
                _generated_v3_candidate(
                    strict_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_UNCERTAINTY_RISK_V3,
                    candidate_model_name=(
                        f"dfl_candidate_library_v3_uncertainty_{source_model_name}_"
                        f"{scale:.2f}"
                    ),
                    forecast_prices=v2_plus._scale_spread(raw_forecast, scale=scale),
                    generated_at=resolved_generated_at,
                    metadata={"uncertainty_spread_scale": scale},
                )
            )
        for scale in degradation_spread_scales:
            rows.append(
                _generated_v3_candidate(
                    strict_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_DEGRADATION_SWEEP_V3,
                    candidate_model_name=(
                        f"dfl_candidate_library_v3_degradation_{source_model_name}_"
                        f"{scale:.2f}"
                    ),
                    forecast_prices=v2_plus._scale_spread(raw_forecast, scale=scale),
                    generated_at=resolved_generated_at,
                    metadata={"degradation_spread_scale": scale},
                )
            )
        if include_train_oracle_neighborhood and str(strict_row["split_name"]) != "final_holdout":
            rows.append(
                _generated_v3_candidate(
                    strict_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V3,
                    candidate_model_name=(
                        f"dfl_candidate_library_v3_oracle_train_only_{source_model_name}"
                    ),
                    forecast_prices=v2._float_list(
                        strict_row["actual_price_uah_mwh_vector"],
                        field_name="actual oracle-neighborhood prices",
                    ),
                    generated_at=resolved_generated_at,
                    metadata={
                        "analysis_only_oracle_neighborhood": True,
                        "train_only": True,
                    },
                )
            )
    return pl.DataFrame(rows).sort(
        [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "candidate_family",
            "candidate_model_name",
        ]
    )


def _v3_generation_anchor_keys(
    grouped: dict[tuple[str, str, datetime], list[dict[str, Any]]],
    *,
    max_train_generation_anchor_count_per_tenant: int | None,
) -> set[tuple[str, str, datetime]]:
    generation_keys: set[tuple[str, str, datetime]] = set()
    train_keys_by_tenant_source: dict[tuple[str, str], list[tuple[str, str, datetime]]] = {}
    for key, rows in grouped.items():
        if not rows:
            continue
        split_name = str(rows[0]["split_name"])
        if split_name == "final_holdout":
            generation_keys.add(key)
        elif split_name == "train_selection":
            tenant_id, source_model_name, _ = key
            train_keys_by_tenant_source.setdefault(
                (tenant_id, source_model_name),
                [],
            ).append(key)
    for keys in train_keys_by_tenant_source.values():
        sorted_keys = sorted(keys, key=lambda item: item[2])
        if max_train_generation_anchor_count_per_tenant is None:
            selected_keys = sorted_keys
        elif max_train_generation_anchor_count_per_tenant == 0:
            selected_keys = []
        else:
            selected_keys = sorted_keys[-max_train_generation_anchor_count_per_tenant:]
        generation_keys.update(selected_keys)
    return generation_keys


def _prior_template_rows(
    grouped: dict[tuple[str, str, datetime], list[dict[str, Any]]],
    *,
    tenant_id: str,
    source_model_name: str,
    anchor_timestamp: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (group_tenant_id, group_source_model_name, group_anchor), group_rows in grouped.items():
        if group_tenant_id != tenant_id or group_source_model_name != source_model_name:
            continue
        if group_anchor >= anchor_timestamp:
            continue
        if not group_rows or str(group_rows[0]["split_name"]) != "train_selection":
            continue
        rows.extend(group_rows)
    return rows


def _prior_template_candidates(
    reference_row: dict[str, Any],
    *,
    raw_forecast: list[float],
    prior_rows: list[dict[str, Any]],
    source_model_name: str,
    generated_at: datetime,
    prior_template_anchor_count: int,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    oracle_residual = _mean_prior_oracle_residual(prior_rows)
    if oracle_residual:
        candidates.append(
            _generated_v3_candidate(
                reference_row,
                source_model_name=source_model_name,
                candidate_family=CANDIDATE_FAMILY_PRIOR_ORACLE_RESIDUAL_V3,
                candidate_model_name=(
                    f"dfl_candidate_library_v3_prior_oracle_residual_{source_model_name}"
                ),
                forecast_prices=_bounded_prices(
                    [price + residual for price, residual in zip(raw_forecast, oracle_residual)]
                ),
                generated_at=generated_at,
                metadata={
                    "prior_template_kind": "oracle_residual",
                    "prior_template_anchor_count": prior_template_anchor_count,
                    "no_final_holdout_actuals_used_for_generation": True,
                },
            )
        )
    best_template_delta = _mean_prior_best_template_delta(prior_rows)
    if best_template_delta:
        candidates.append(
            _generated_v3_candidate(
                reference_row,
                source_model_name=source_model_name,
                candidate_family=CANDIDATE_FAMILY_PRIOR_BEST_TEMPLATE_V3,
                candidate_model_name=(
                    f"dfl_candidate_library_v3_prior_best_template_{source_model_name}"
                ),
                forecast_prices=_bounded_prices(
                    [
                        price + delta
                        for price, delta in zip(raw_forecast, best_template_delta)
                    ]
                ),
                generated_at=generated_at,
                metadata={
                    "prior_template_kind": "best_family_delta",
                    "prior_template_anchor_count": prior_template_anchor_count,
                    "no_final_holdout_actuals_used_for_generation": True,
                },
            )
        )
    return candidates


def _mean_prior_oracle_residual(prior_rows: list[dict[str, Any]]) -> list[float]:
    residuals: list[list[float]] = []
    for anchor_rows in v2._rows_by_anchor(prior_rows).values():
        try:
            raw_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_RAW)
        except ValueError:
            continue
        raw_forecast = v2._float_list(
            raw_row["forecast_price_uah_mwh_vector"],
            field_name="raw forecast",
        )
        actual_prices = v2._float_list(
            raw_row["actual_price_uah_mwh_vector"],
            field_name="actual prices",
        )
        if len(raw_forecast) != len(actual_prices):
            continue
        residuals.append(
            [actual - forecast for actual, forecast in zip(actual_prices, raw_forecast)]
        )
    return _mean_vectors(residuals)


def _mean_prior_best_template_delta(prior_rows: list[dict[str, Any]]) -> list[float]:
    deltas: list[list[float]] = []
    for anchor_rows in v2._rows_by_anchor(prior_rows).values():
        try:
            raw_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_RAW)
        except ValueError:
            continue
        raw_forecast = v2._float_list(
            raw_row["forecast_price_uah_mwh_vector"],
            field_name="raw forecast",
        )
        candidate_rows = [
            row
            for row in anchor_rows
            if str(row["candidate_family"]) != CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V3
        ]
        if not candidate_rows:
            continue
        best_row = min(
            candidate_rows,
            key=lambda row: (
                float(row["regret_uah"]),
                v2._family_sort_index(str(row["candidate_family"])),
                str(row["candidate_model_name"]),
            ),
        )
        best_forecast = v2._float_list(
            best_row["forecast_price_uah_mwh_vector"],
            field_name="best forecast",
        )
        if len(raw_forecast) != len(best_forecast):
            continue
        deltas.append([best - raw for best, raw in zip(best_forecast, raw_forecast)])
    return _mean_vectors(deltas)


def _mean_vectors(vectors: list[list[float]]) -> list[float]:
    if not vectors:
        return []
    horizon = len(vectors[0])
    if horizon == 0 or any(len(vector) != horizon for vector in vectors):
        return []
    return [mean(vector[index] for vector in vectors) for index in range(horizon)]


def _bounded_prices(
    prices: list[float],
    *,
    floor_uah_mwh: float = 0.0,
    cap_uah_mwh: float = 16_000.0,
) -> list[float]:
    return [min(cap_uah_mwh, max(floor_uah_mwh, float(price))) for price in prices]


def build_dfl_candidate_value_label_panel_v3_frame(
    schedule_candidate_library_v3_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build prior-safe candidate features plus realized value labels for V3."""

    v2._validate_library_frame(schedule_candidate_library_v3_frame)
    rows: list[dict[str, Any]] = []
    grouped = v2_plus._rows_by_tenant_source_anchor(schedule_candidate_library_v3_frame)
    for key in sorted(grouped, key=lambda item: (item[0], item[1], item[2])):
        anchor_rows = grouped[key]
        best_regret = min(float(row["regret_uah"]) for row in anchor_rows)
        strict_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_STRICT)
        strict_regret = float(strict_row["regret_uah"])
        for row in sorted(
            anchor_rows,
            key=lambda item: (
                str(item["candidate_family"]),
                str(item["candidate_model_name"]),
            ),
        ):
            regret = float(row["regret_uah"])
            payload = dict(v2._payload(row))
            rows.append(
                {
                    "tenant_id": str(row["tenant_id"]),
                    "source_model_name": str(row["source_model_name"]),
                    "candidate_family": str(row["candidate_family"]),
                    "candidate_model_name": str(row["candidate_model_name"]),
                    "anchor_timestamp": v2._datetime_value(
                        row["anchor_timestamp"],
                        field_name="anchor_timestamp",
                    ),
                    "split_name": str(row["split_name"]),
                    "horizon_hours": int(row["horizon_hours"]),
                    "selector_feature_prior_family_mean_regret_uah": float(
                        row.get("prior_family_mean_regret_uah", regret)
                    ),
                    "selector_feature_forecast_spread_uah_mwh": float(
                        row.get("forecast_spread_uah_mwh", 0.0)
                    ),
                    "selector_feature_forecast_objective_value_uah": float(
                        row["forecast_objective_value_uah"]
                    ),
                    "selector_feature_total_throughput_mwh": float(
                        row.get("total_throughput_mwh", 0.0)
                    ),
                    "selector_feature_total_degradation_penalty_uah": float(
                        row.get("total_degradation_penalty_uah", 0.0)
                    ),
                    "selector_feature_soc_min_slack_fraction": float(
                        row.get("soc_min_slack_fraction", 0.0)
                    ),
                    "selector_feature_candidate_library_version": str(
                        row.get("candidate_library_version", "unknown")
                    ),
                    "label_regret_uah": regret,
                    "label_decision_value_uah": float(row["decision_value_uah"]),
                    "label_oracle_value_uah": float(row["oracle_value_uah"]),
                    "label_is_anchor_best_candidate": abs(regret - best_regret) <= 1e-9,
                    "label_regret_margin_to_anchor_best_uah": regret - best_regret,
                    "label_value_margin_vs_strict_uah": strict_regret - regret,
                    "label_value_tier": _value_tier(
                        regret=regret,
                        best_regret=best_regret,
                    ),
                    "claim_scope": DFL_CANDIDATE_VALUE_LABEL_PANEL_V3_CLAIM_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                    "market_execution_enabled": False,
                    "evaluation_payload": {
                        "claim_scope": DFL_CANDIDATE_VALUE_LABEL_PANEL_V3_CLAIM_SCOPE,
                        "source_candidate_claim_scope": payload.get("claim_scope"),
                        "selector_features_prior_only": True,
                        "labels_are_realized_scoring_outcomes": True,
                        "not_full_dfl": True,
                        "not_market_execution": True,
                        "market_execution_enabled": False,
                    },
                }
            )
    return pl.DataFrame(rows).sort(
        [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "candidate_family",
            "candidate_model_name",
        ]
    )


def validate_dfl_candidate_value_label_panel_v3_evidence(
    label_panel_frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate V3 candidate value-label panel structure and claim boundaries."""

    missing_columns = sorted(REQUIRED_LABEL_PANEL_COLUMNS.difference(label_panel_frame.columns))
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"candidate-value DFL v3 label panel is missing required columns: {missing_columns}",
            {"row_count": label_panel_frame.height},
        )
    selector_columns = [
        column for column in label_panel_frame.columns if column.startswith("selector_feature_")
    ]
    label_columns = [
        column for column in label_panel_frame.columns if column.startswith("label_")
    ]
    failures: list[str] = []
    if not selector_columns:
        failures.append("label panel must expose selector_feature_* columns")
    if not label_columns:
        failures.append("label panel must expose label_* columns")
    for row in label_panel_frame.iter_rows(named=True):
        if str(row["claim_scope"]) != DFL_CANDIDATE_VALUE_LABEL_PANEL_V3_CLAIM_SCOPE:
            failures.append("unexpected claim_scope")
            break
        if not bool(row["not_full_dfl"]):
            failures.append("not_full_dfl must be true")
            break
        if not bool(row["not_market_execution"]):
            failures.append("not_market_execution must be true")
            break
        if bool(row.get("market_execution_enabled", False)):
            failures.append("market_execution_enabled must be false")
            break
    return EvidenceCheckOutcome(
        not failures,
        "Candidate-value DFL v3 label panel keeps prior features separate from realized labels."
        if not failures
        else "; ".join(failures),
        {
            "row_count": label_panel_frame.height,
            "selector_feature_columns": selector_columns,
            "label_columns": label_columns,
            "market_execution_enabled": False,
        },
    )


def _value_tier(*, regret: float, best_regret: float) -> str:
    margin = regret - best_regret
    if margin <= 1e-9:
        return "best"
    if margin <= 50.0:
        return "competitive"
    return "dominated"


def build_dfl_candidate_value_dfl_v3_frame(
    schedule_candidate_library_v3_frame: pl.DataFrame,
    learner_v2_plus_frame: pl.DataFrame,
    candidate_value_label_panel_v3_frame: pl.DataFrame | None = None,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int = 18,
    min_prior_mean_improvement_ratio_vs_v2_plus: float = 0.01,
    pairwise_loss_weight: float = 0.05,
) -> pl.DataFrame:
    """Train/select a candidate-level value scorer with V2+ fallback."""

    _validate_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        min_prior_mean_improvement_ratio_vs_v2_plus=(
            min_prior_mean_improvement_ratio_vs_v2_plus
        ),
    )
    v2._validate_library_frame(schedule_candidate_library_v3_frame)
    _validate_v2_plus_model_frame(learner_v2_plus_frame)
    label_panel_frame = (
        candidate_value_label_panel_v3_frame
        if candidate_value_label_panel_v3_frame is not None
        else build_dfl_candidate_value_label_panel_v3_frame(
            schedule_candidate_library_v3_frame
        )
    )
    _validate_label_panel_frame(label_panel_frame)
    label_rows_by_key = _label_rows_by_key(label_panel_frame)
    v2_plus_rows = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in learner_v2_plus_frame.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = v2._library_rows(
                schedule_candidate_library_v3_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
            )
            train_rows = [
                row for row in source_rows if str(row["split_name"]) == "train_selection"
            ]
            final_rows = [
                row for row in source_rows if str(row["split_name"]) == "final_holdout"
            ]
            final_anchor_count = len(v2._anchor_set(final_rows))
            if final_anchor_count != final_validation_anchor_count_per_tenant:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} final-holdout tenant-anchor count must be "
                    f"{final_validation_anchor_count_per_tenant}; observed {final_anchor_count}"
                )
            if not train_rows:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} candidate-value DFL v3 needs train rows"
                )
            v2_plus_row = v2_plus_rows.get((tenant_id, source_model_name))
            if v2_plus_row is None:
                raise ValueError(f"missing V2+ learner row for {tenant_id}/{source_model_name}")
            eligible_families = _eligible_candidate_families(
                train_rows,
                final_rows,
                required_final_anchor_count=final_anchor_count,
            )
            train_label_rows = _label_rows_for_library_rows(
                train_rows,
                label_rows_by_key=label_rows_by_key,
                candidate_families=eligible_families,
            )
            final_label_rows = _label_rows_for_library_rows(
                final_rows,
                label_rows_by_key=label_rows_by_key,
                candidate_families=eligible_families,
            )
            teacher_family_scores = _teacher_family_scores(
                train_rows,
                candidate_families=eligible_families,
            )
            learned_scorer = _fit_learned_candidate_value_scorer(
                train_label_rows,
                candidate_families=eligible_families,
            )
            selected_train_rows = _select_rows_by_learned_scorer(
                train_rows,
                label_rows_by_key=label_rows_by_key,
                scorer=learned_scorer,
                candidate_families=eligible_families,
            )
            selected_final_rows = _select_rows_by_learned_scorer(
                final_rows,
                label_rows_by_key=label_rows_by_key,
                scorer=learned_scorer,
                candidate_families=eligible_families,
            )
            v2_plus_train_mean = float(v2_plus_row["selected_train_mean_regret_uah"])
            v2_plus_final_mean = float(v2_plus_row["selected_final_mean_regret_uah"])
            selected_train_mean = v2._mean_regret(selected_train_rows)
            pairwise_loss = _pairwise_regret_weighted_loss_by_scorer(
                train_label_rows,
                scorer=learned_scorer,
                candidate_families=eligible_families,
            )
            fallback_to_v2_plus = (
                v2._improvement_ratio(v2_plus_train_mean, selected_train_mean)
                < min_prior_mean_improvement_ratio_vs_v2_plus
            )
            effective_train_rows = [] if fallback_to_v2_plus else selected_train_rows
            effective_final_rows = [] if fallback_to_v2_plus else selected_final_rows
            strict_final_rows = v2._selected_family_rows(
                final_rows, v2.CANDIDATE_FAMILY_STRICT
            )
            raw_final_rows = v2._selected_family_rows(final_rows, v2.CANDIDATE_FAMILY_RAW)
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "learner_model_name": candidate_value_dfl_v3_model_name(
                        source_model_name
                    ),
                    "selected_value_profile_name": str(learned_scorer["name"]),
                    "selected_scorer_type": LEARNED_SCORER_TYPE,
                    "selected_objective_name": (
                        "candidate_value_train_label_ridge_pairwise_ranking"
                    ),
                    "selected_feature_names": list(LEARNED_SCORER_FEATURE_COLUMNS),
                    "selected_feature_weights": dict(learned_scorer["weights"]),
                    "selected_feature_means": dict(learned_scorer["feature_means"]),
                    "selected_feature_scales": dict(learned_scorer["feature_scales"]),
                    "eligible_candidate_families": sorted(eligible_families),
                    "teacher_family_scores": teacher_family_scores,
                    "pairwise_loss_weight": pairwise_loss_weight,
                    "fallback_to_v2_plus": fallback_to_v2_plus,
                    "train_anchor_count": len(v2._anchor_set(train_rows)),
                    "final_holdout_anchor_count": final_anchor_count,
                    "final_holdout_tenant_anchor_count": final_anchor_count
                    * len(tenant_ids),
                    "strict_final_mean_regret_uah": v2._mean_regret(strict_final_rows),
                    "raw_final_mean_regret_uah": v2._mean_regret(raw_final_rows),
                    "v2_plus_train_mean_regret_uah": v2_plus_train_mean,
                    "v2_plus_final_mean_regret_uah": v2_plus_final_mean,
                    "candidate_train_mean_regret_uah": selected_train_mean,
                    "candidate_train_pairwise_loss_uah": pairwise_loss,
                    "candidate_train_label_row_count": len(train_label_rows),
                    "candidate_final_label_row_count": len(final_label_rows),
                    "candidate_final_mean_regret_uah": v2._mean_regret(
                        selected_final_rows
                    ),
                    "selected_train_mean_regret_uah": (
                        v2_plus_train_mean
                        if fallback_to_v2_plus
                        else v2._mean_regret(effective_train_rows)
                    ),
                    "selected_final_mean_regret_uah": (
                        v2_plus_final_mean
                        if fallback_to_v2_plus
                        else v2._mean_regret(effective_final_rows)
                    ),
                    "selected_train_median_regret_uah": (
                        float(v2_plus_row["selected_train_median_regret_uah"])
                        if fallback_to_v2_plus
                        else v2._median_regret(effective_train_rows)
                    ),
                    "selected_final_median_regret_uah": (
                        float(v2_plus_row["selected_final_median_regret_uah"])
                        if fallback_to_v2_plus
                        else v2._median_regret(effective_final_rows)
                    ),
                    "candidate_train_family_counts": v2._family_counts(
                        selected_train_rows
                    ),
                    "candidate_final_family_counts": v2._family_counts(
                        selected_final_rows
                    ),
                    "selected_train_family_counts": (
                        dict(v2_plus_row["selected_train_family_counts"])
                        if fallback_to_v2_plus
                        else v2._family_counts(effective_train_rows)
                    ),
                    "selected_final_family_counts": (
                        dict(v2_plus_row["selected_final_family_counts"])
                        if fallback_to_v2_plus
                        else v2._family_counts(effective_final_rows)
                    ),
                    "train_mean_regret_improvement_ratio_vs_v2_plus": v2._improvement_ratio(
                        v2_plus_train_mean,
                        v2_plus_train_mean
                        if fallback_to_v2_plus
                        else v2._mean_regret(effective_train_rows),
                    ),
                    "final_mean_regret_improvement_ratio_vs_v2_plus": v2._improvement_ratio(
                        v2_plus_final_mean,
                        v2_plus_final_mean
                        if fallback_to_v2_plus
                        else v2._mean_regret(effective_final_rows),
                    ),
                    "final_mean_regret_improvement_ratio_vs_strict": v2._improvement_ratio(
                        v2._mean_regret(strict_final_rows),
                        v2_plus_final_mean
                        if fallback_to_v2_plus
                        else v2._mean_regret(effective_final_rows),
                    ),
                    "claim_scope": CANDIDATE_VALUE_DFL_V3_CLAIM_SCOPE,
                    "academic_scope": CANDIDATE_VALUE_DFL_V3_ACADEMIC_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id"])


def build_dfl_candidate_value_dfl_v3_strict_lp_benchmark_frame(
    schedule_candidate_library_v3_frame: pl.DataFrame,
    candidate_value_dfl_v3_frame: pl.DataFrame,
    v2_plus_strict_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict/raw/V2+/candidate-value rows for the V2+-anchored gate."""

    v2._validate_library_frame(schedule_candidate_library_v3_frame)
    _validate_candidate_value_model_frame(candidate_value_dfl_v3_frame)
    resolved_generated_at = generated_at or v2._latest_generated_at(v2_plus_strict_frame)
    library_rows = list(schedule_candidate_library_v3_frame.iter_rows(named=True))
    rows: list[dict[str, Any]] = []
    for learner_row in candidate_value_dfl_v3_frame.iter_rows(named=True):
        tenant_id = str(learner_row["tenant_id"])
        source_model_name = str(learner_row["source_model_name"])
        final_rows = [
            row
            for row in library_rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
            and str(row["split_name"]) == "final_holdout"
        ]
        final_anchors = sorted(v2._anchor_set(final_rows))
        v2_plus_by_anchor = _v2_plus_reference_rows(
            v2_plus_strict_frame,
            tenant_id=tenant_id,
            source_model_name=source_model_name,
        )
        candidate_rows = _select_rows_by_model_row(
            final_rows,
            learner_row=learner_row,
            candidate_families=frozenset(
                str(family) for family in learner_row["eligible_candidate_families"]
            ),
        )
        candidate_by_anchor = {
            v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
            for row in candidate_rows
        }
        for anchor_timestamp in final_anchors:
            anchor_rows = [
                row
                for row in final_rows
                if v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
                == anchor_timestamp
            ]
            strict_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_STRICT)
            raw_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_RAW)
            v2_plus_row = v2_plus_by_anchor[anchor_timestamp]
            selected_row = (
                v2_plus_row
                if bool(learner_row["fallback_to_v2_plus"])
                else candidate_by_anchor[anchor_timestamp]
            )
            rows.extend(
                [
                    _strict_benchmark_row(
                        strict_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="strict_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row(
                        raw_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="raw_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row(
                        v2_plus_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="schedule_value_learner_v2_plus_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row(
                        selected_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="candidate_value_dfl_v3",
                        generated_at=resolved_generated_at,
                    ),
                ]
            )
    return pl.DataFrame(rows).sort(
        ["tenant_id", "source_model_name", "anchor_timestamp", "selection_role"]
    )


def validate_dfl_candidate_value_dfl_v3_evidence(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> EvidenceCheckOutcome:
    """Validate structural V3 evidence without requiring headline replacement."""

    missing_columns = sorted(REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"candidate-value DFL v3 evidence is missing required columns: {missing_columns}",
            {"row_count": strict_frame.height},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(
            False, "candidate-value DFL v3 evidence has no rows", {"row_count": 0}
        )
    source_names = source_model_names or tuple(
        sorted({_source_model_name(row) for row in rows})
    )
    failures: list[str] = []
    summaries: list[dict[str, Any]] = []
    for source_model_name in source_names:
        summary, summary_failures = _gate_summary(
            rows,
            source_model_name=source_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio_vs_v2_plus=0.0,
            min_mean_regret_improvement_ratio_vs_strict=(
                DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
            ),
            include_promotion_failures=False,
        )
        summaries.append(summary)
        failures.extend(summary_failures)
    return EvidenceCheckOutcome(
        not failures,
        "Candidate-value DFL v3 evidence has valid coverage and claim boundaries."
        if not failures
        else "; ".join(failures),
        {
            "row_count": strict_frame.height,
            "source_model_count": len(source_names),
            "source_model_names": list(source_names),
            "model_summaries": summaries,
        },
    )


def build_dfl_candidate_value_dfl_v3_failure_audit_frame(
    candidate_value_label_panel_v3_frame: pl.DataFrame,
    candidate_value_dfl_v3_frame: pl.DataFrame,
    candidate_value_dfl_v3_strict_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Explain why V3 candidate schedules did or did not improve over V2+."""

    _validate_label_panel_frame(candidate_value_label_panel_v3_frame)
    _validate_candidate_value_model_frame(candidate_value_dfl_v3_frame)
    missing_columns = sorted(
        REQUIRED_STRICT_COLUMNS.difference(candidate_value_dfl_v3_strict_frame.columns)
    )
    if missing_columns:
        raise ValueError(f"candidate-value DFL v3 strict frame is missing columns: {missing_columns}")
    strict_rows = list(candidate_value_dfl_v3_strict_frame.iter_rows(named=True))
    v2_plus_regret_by_anchor = _strict_reference_regret_by_anchor(
        strict_rows,
        role="schedule_value_learner_v2_plus_reference",
    )
    selected_regret_by_anchor = _strict_reference_regret_by_anchor(
        strict_rows,
        role="candidate_value_dfl_v3",
    )
    model_rows_by_source = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in candidate_value_dfl_v3_frame.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    label_rows = [
        row
        for row in candidate_value_label_panel_v3_frame.iter_rows(named=True)
        if str(row["split_name"]) == "final_holdout"
    ]
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in label_rows:
        grouped.setdefault(
            (str(row["source_model_name"]), str(row["candidate_family"])),
            [],
        ).append(row)
    for (source_model_name, candidate_family), family_rows in sorted(grouped.items()):
        matched_rows = [
            row
            for row in family_rows
            if _tenant_source_anchor_key(row) in v2_plus_regret_by_anchor
        ]
        if not matched_rows:
            continue
        candidate_regrets = [float(row["label_regret_uah"]) for row in matched_rows]
        v2_plus_regrets = [
            v2_plus_regret_by_anchor[_tenant_source_anchor_key(row)]
            for row in matched_rows
        ]
        selected_regrets = [
            selected_regret_by_anchor.get(_tenant_source_anchor_key(row))
            for row in matched_rows
        ]
        selected_count = sum(
            1
            for candidate, selected in zip(candidate_regrets, selected_regrets, strict=True)
            if selected is not None and abs(candidate - selected) <= 1e-9
        )
        win_count = sum(
            1
            for candidate, baseline in zip(candidate_regrets, v2_plus_regrets, strict=True)
            if candidate < baseline
        )
        mean_regret = mean(candidate_regrets)
        v2_plus_mean = mean(v2_plus_regrets)
        win_rate = win_count / len(matched_rows)
        delta = mean_regret - v2_plus_mean
        fallback_count = _fallback_count_for_source(
            model_rows_by_source,
            source_model_name=source_model_name,
        )
        rows.append(
            {
                "source_model_name": source_model_name,
                "audit_grain": "candidate_family",
                "candidate_family": candidate_family,
                "split_name": "final_holdout",
                "row_count": len(matched_rows),
                "anchor_count": len({_tenant_source_anchor_key(row) for row in matched_rows}),
                "mean_regret_uah": mean_regret,
                "v2_plus_mean_regret_uah": v2_plus_mean,
                "mean_delta_vs_v2_plus_uah": delta,
                "win_rate_vs_v2_plus": win_rate,
                "selected_count": selected_count,
                "fallback_model_count": fallback_count,
                "diagnosis": _candidate_family_failure_diagnosis(
                    candidate_family=candidate_family,
                    mean_delta_vs_v2_plus=delta,
                    win_rate_vs_v2_plus=win_rate,
                    selected_count=selected_count,
                ),
                "claim_scope": CANDIDATE_VALUE_DFL_V3_FAILURE_AUDIT_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(rows).sort(["source_model_name", "audit_grain", "candidate_family"])


def validate_dfl_candidate_value_dfl_v3_failure_audit_evidence(
    failure_audit_frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate analysis-only V3 failure audit rows."""

    missing_columns = sorted(REQUIRED_FAILURE_AUDIT_COLUMNS.difference(failure_audit_frame.columns))
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"candidate-value DFL v3 failure audit is missing required columns: {missing_columns}",
            {"row_count": failure_audit_frame.height},
        )
    failures: list[str] = []
    for row in failure_audit_frame.iter_rows(named=True):
        if str(row["claim_scope"]) != CANDIDATE_VALUE_DFL_V3_FAILURE_AUDIT_CLAIM_SCOPE:
            failures.append("unexpected claim_scope")
            break
        if not bool(row["not_full_dfl"]) or not bool(row["not_market_execution"]):
            failures.append("failure audit claim boundary violation")
            break
        if bool(row["market_execution_enabled"]):
            failures.append("market_execution_enabled must be false")
            break
    return EvidenceCheckOutcome(
        not failures,
        "Candidate-value DFL v3 failure audit is valid analysis-only evidence."
        if not failures
        else "; ".join(failures),
        {
            "row_count": failure_audit_frame.height,
            "market_execution_enabled": False,
        },
    )


def evaluate_dfl_candidate_value_dfl_v3_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.0,
    min_mean_regret_improvement_ratio_vs_strict: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
) -> PromotionGateResult:
    """Evaluate whether candidate-value DFL v3 can replace frozen V2+."""

    missing_columns = sorted(REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing_columns:
        return PromotionGateResult(
            False,
            "blocked",
            f"candidate-value DFL v3 strict frame is missing required columns: {missing_columns}",
            {},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return PromotionGateResult(
            False,
            "blocked",
            "candidate-value DFL v3 strict frame has no rows",
            {},
        )
    source_names = source_model_names or tuple(
        sorted({_source_model_name(row) for row in rows})
    )
    summaries: list[dict[str, Any]] = []
    failures: list[str] = []
    passing_sources: list[str] = []
    for source_model_name in source_names:
        summary, summary_failures = _gate_summary(
            rows,
            source_model_name=source_model_name,
            min_tenant_count=1,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio_vs_v2_plus=(
                min_mean_regret_improvement_ratio_vs_v2_plus
            ),
            min_mean_regret_improvement_ratio_vs_strict=(
                min_mean_regret_improvement_ratio_vs_strict
            ),
            include_promotion_failures=True,
        )
        summaries.append(summary)
        failures.extend(summary_failures)
        if summary["offline_strategy_replacement_passed"]:
            passing_sources.append(source_model_name)
    best_summary = min(summaries, key=lambda item: item["selected_mean_regret_uah"])
    metrics = {
        "best_source_model_name": best_summary["source_model_name"],
        "tenant_count": best_summary["tenant_count"],
        "validation_tenant_anchor_count": best_summary["validation_tenant_anchor_count"],
        "strict_mean_regret_uah": best_summary["strict_mean_regret_uah"],
        "raw_mean_regret_uah": best_summary["raw_mean_regret_uah"],
        "v2_plus_mean_regret_uah": best_summary["v2_plus_mean_regret_uah"],
        "selected_mean_regret_uah": best_summary["selected_mean_regret_uah"],
        "strict_median_regret_uah": best_summary["strict_median_regret_uah"],
        "v2_plus_median_regret_uah": best_summary["v2_plus_median_regret_uah"],
        "selected_median_regret_uah": best_summary["selected_median_regret_uah"],
        "mean_regret_improvement_ratio_vs_v2_plus": best_summary[
            "mean_regret_improvement_ratio_vs_v2_plus"
        ],
        "mean_regret_improvement_ratio_vs_strict": best_summary[
            "mean_regret_improvement_ratio_vs_strict"
        ],
        "mean_regret_improvement_ratio_vs_raw": best_summary[
            "mean_regret_improvement_ratio_vs_raw"
        ],
        "development_gate_passed": any(
            bool(summary["development_gate_passed"]) for summary in summaries
        ),
        "offline_strategy_replacement_passed": bool(passing_sources),
        "market_execution_enabled": False,
        "passing_source_model_names": passing_sources,
        "model_summaries": summaries,
    }
    if passing_sources:
        return PromotionGateResult(
            True,
            "offline_strategy_replacement_passed",
            "candidate-value DFL v3 passes the V2+-anchored strict LP/oracle gate",
            metrics,
        )
    if metrics["development_gate_passed"]:
        return PromotionGateResult(
            False,
            "diagnostic_pass_replacement_blocked",
            "candidate-value DFL v3 improves over raw neural schedules but remains "
            f"blocked versus V2+ or {CONTROL_MODEL_NAME}: "
            + "; ".join(failures),
            metrics,
        )
    return PromotionGateResult(
        False,
        "blocked",
        "; ".join(failures)
        if failures
        else "candidate-value DFL v3 has no development improvement",
        metrics,
    )


def _validate_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int,
    min_prior_mean_improvement_ratio_vs_v2_plus: float,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if final_validation_anchor_count_per_tenant <= 0:
        raise ValueError("final_validation_anchor_count_per_tenant must be positive.")
    if min_prior_mean_improvement_ratio_vs_v2_plus < 0.0:
        raise ValueError(
            "min_prior_mean_improvement_ratio_vs_v2_plus must not be negative."
        )


def _with_v3_library_claim(row: dict[str, Any], *, version: str) -> dict[str, Any]:
    copied = dict(row)
    payload = dict(v2._payload(row))
    payload.update(
        {
            "claim_scope": DFL_SCHEDULE_CANDIDATE_LIBRARY_V3_CLAIM_SCOPE,
            "candidate_library_version": version,
            "no_leakage_prior_only": True,
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    copied["claim_scope"] = DFL_SCHEDULE_CANDIDATE_LIBRARY_V3_CLAIM_SCOPE
    copied["candidate_library_version"] = version
    copied["evaluation_payload"] = payload
    copied["not_full_dfl"] = True
    copied["not_market_execution"] = True
    return copied


def _generated_v3_candidate(
    reference_row: dict[str, Any],
    *,
    source_model_name: str,
    candidate_family: str,
    candidate_model_name: str,
    forecast_prices: list[float],
    generated_at: datetime,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    row = v2_plus._evaluated_candidate_row(
        reference_row,
        source_model_name=source_model_name,
        candidate_family=candidate_family,
        candidate_model_name=candidate_model_name,
        forecast_prices=forecast_prices,
        generated_at=generated_at,
        metadata={
            **metadata,
            "candidate_library_version": "v3_generated",
            "no_leakage_prior_only": True,
        },
    )
    return _with_v3_library_claim(row, version="v3_generated")


def _validate_v2_plus_model_frame(frame: pl.DataFrame) -> None:
    v2._require_columns(
        frame,
        v2_plus.REQUIRED_MODEL_COLUMNS,
        frame_name="schedule_value_learner_v2_plus_frame",
    )
    for row in frame.iter_rows(named=True):
        if not bool(row["not_full_dfl"]):
            raise ValueError("V2+ rows must keep not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError("V2+ rows must keep not_market_execution=true")


def _validate_candidate_value_model_frame(frame: pl.DataFrame) -> None:
    v2._require_columns(
        frame,
        REQUIRED_MODEL_COLUMNS,
        frame_name="candidate_value_dfl_v3_frame",
    )
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != CANDIDATE_VALUE_DFL_V3_CLAIM_SCOPE:
            raise ValueError("candidate-value DFL v3 frame has unexpected claim_scope")
        if not bool(row["not_full_dfl"]):
            raise ValueError("candidate-value DFL v3 rows must keep not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError(
                "candidate-value DFL v3 rows must keep not_market_execution=true"
            )


def _validate_label_panel_frame(frame: pl.DataFrame) -> None:
    v2._require_columns(
        frame,
        REQUIRED_LABEL_PANEL_COLUMNS,
        frame_name="candidate_value_label_panel_v3_frame",
    )
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != DFL_CANDIDATE_VALUE_LABEL_PANEL_V3_CLAIM_SCOPE:
            raise ValueError("candidate-value DFL v3 label panel has unexpected claim_scope")
        if not bool(row["not_full_dfl"]):
            raise ValueError("candidate-value DFL v3 label rows must keep not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError(
                "candidate-value DFL v3 label rows must keep not_market_execution=true"
            )


def _eligible_candidate_families(
    train_rows: list[dict[str, Any]],
    final_rows: list[dict[str, Any]],
    *,
    required_final_anchor_count: int,
) -> frozenset[str]:
    train_families = {
        str(row["candidate_family"])
        for row in train_rows
        if str(row["candidate_family"]) != CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V3
    }
    final_family_anchors: dict[str, set[datetime]] = {}
    for row in final_rows:
        family = str(row["candidate_family"])
        if family == CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V3:
            continue
        final_family_anchors.setdefault(family, set()).add(
            v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        )
    eligible = frozenset(
        family
        for family in sorted(train_families)
        if len(final_family_anchors.get(family, set())) == required_final_anchor_count
    )
    if not eligible:
        raise ValueError(
            "candidate-value DFL v3 needs at least one candidate family with full final coverage"
        )
    return eligible


def _teacher_family_scores(
    train_rows: list[dict[str, Any]],
    *,
    candidate_families: frozenset[str],
) -> dict[str, float]:
    by_anchor = v2._rows_by_anchor(train_rows)
    wins = {family: 0.0 for family in candidate_families}
    for anchor_rows in by_anchor.values():
        eligible_rows = [
            row
            for row in anchor_rows
            if str(row["candidate_family"]) in candidate_families
        ]
        if not eligible_rows:
            continue
        best_row = min(
            eligible_rows,
            key=lambda row: (
                float(row["regret_uah"]),
                v2._family_sort_index(str(row["candidate_family"])),
                str(row["candidate_model_name"]),
            ),
        )
        wins[str(best_row["candidate_family"])] += 1.0
    anchor_count = max(len(by_anchor), 1)
    return {family: wins[family] / anchor_count for family in sorted(candidate_families)}


def _label_rows_by_key(frame: pl.DataFrame) -> dict[tuple[str, str, datetime, str, str], dict[str, Any]]:
    rows: dict[tuple[str, str, datetime, str, str], dict[str, Any]] = {}
    for row in frame.iter_rows(named=True):
        rows[_candidate_identity_key(row)] = row
    return rows


def _candidate_identity_key(row: dict[str, Any]) -> tuple[str, str, datetime, str, str]:
    return (
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        str(row["candidate_family"]),
        str(row["candidate_model_name"]),
    )


def _label_rows_for_library_rows(
    rows: list[dict[str, Any]],
    *,
    label_rows_by_key: dict[tuple[str, str, datetime, str, str], dict[str, Any]],
    candidate_families: frozenset[str],
) -> list[dict[str, Any]]:
    label_rows: list[dict[str, Any]] = []
    for row in rows:
        if str(row["candidate_family"]) not in candidate_families:
            continue
        label_row = label_rows_by_key.get(_candidate_identity_key(row))
        if label_row is not None:
            label_rows.append(label_row)
    return label_rows


def _fit_learned_candidate_value_scorer(
    train_label_rows: list[dict[str, Any]],
    *,
    candidate_families: frozenset[str],
    ridge_l2: float = 1.0,
) -> dict[str, Any]:
    eligible_rows = [
        row
        for row in train_label_rows
        if str(row["candidate_family"]) in candidate_families
        and str(row["split_name"]) == "train_selection"
    ]
    if not eligible_rows:
        raise ValueError("candidate-value DFL v3 learned scorer needs train label rows")
    feature_means: dict[str, float] = {}
    feature_scales: dict[str, float] = {}
    for column in LEARNED_SCORER_FEATURE_COLUMNS:
        values = [float(row[column]) for row in eligible_rows]
        feature_means[column] = mean(values)
        span = max(values) - min(values)
        feature_scales[column] = span if span > 1e-9 else 1.0
    family_columns = tuple(f"family::{family}" for family in sorted(candidate_families))
    feature_matrix = [
        _learned_feature_vector(
            row,
            feature_means=feature_means,
            feature_scales=feature_scales,
            family_columns=family_columns,
        )
        for row in eligible_rows
    ]
    targets = [float(row["label_regret_uah"]) for row in eligible_rows]
    coefficients = _fit_ridge_coefficients(
        feature_matrix,
        targets,
        ridge_l2=ridge_l2,
    )
    feature_names = [*LEARNED_SCORER_FEATURE_COLUMNS, *family_columns]
    weights = {"intercept": coefficients[0]}
    weights.update(
        {
            feature_name: coefficients[index + 1]
            for index, feature_name in enumerate(feature_names)
        }
    )
    return {
        "name": LEARNED_SCORER_PROFILE_NAME,
        "scorer_type": LEARNED_SCORER_TYPE,
        "weights": weights,
        "feature_means": feature_means,
        "feature_scales": feature_scales,
        "family_columns": family_columns,
    }


def _fit_ridge_coefficients(
    feature_matrix: list[list[float]],
    targets: list[float],
    *,
    ridge_l2: float,
) -> list[float]:
    if not feature_matrix:
        raise ValueError("feature_matrix must not be empty")
    width = len(feature_matrix[0]) + 1
    xtx = [[0.0 for _ in range(width)] for _ in range(width)]
    xty = [0.0 for _ in range(width)]
    for features, target in zip(feature_matrix, targets, strict=True):
        row = [1.0, *features]
        for left_index, left_value in enumerate(row):
            xty[left_index] += left_value * target
            for right_index, right_value in enumerate(row):
                xtx[left_index][right_index] += left_value * right_value
    for index in range(1, width):
        xtx[index][index] += ridge_l2
    return _solve_linear_system(xtx, xty)


def _solve_linear_system(matrix: list[list[float]], vector: list[float]) -> list[float]:
    size = len(vector)
    augmented = [list(row) + [vector[index]] for index, row in enumerate(matrix)]
    for column in range(size):
        pivot_row = max(
            range(column, size),
            key=lambda row_index: abs(augmented[row_index][column]),
        )
        if abs(augmented[pivot_row][column]) < 1e-10:
            augmented[column][column] += 1e-6
            pivot_row = column
        if pivot_row != column:
            augmented[column], augmented[pivot_row] = (
                augmented[pivot_row],
                augmented[column],
            )
        pivot = augmented[column][column]
        if abs(pivot) < 1e-12:
            continue
        for item_index in range(column, size + 1):
            augmented[column][item_index] /= pivot
        for row_index in range(size):
            if row_index == column:
                continue
            factor = augmented[row_index][column]
            if abs(factor) <= 1e-12:
                continue
            for item_index in range(column, size + 1):
                augmented[row_index][item_index] -= factor * augmented[column][item_index]
    return [augmented[index][size] for index in range(size)]


def _learned_feature_vector(
    row: dict[str, Any],
    *,
    feature_means: dict[str, float],
    feature_scales: dict[str, float],
    family_columns: tuple[str, ...],
) -> list[float]:
    features = _selector_feature_values(row)
    numeric = [
        (features[column] - feature_means[column]) / feature_scales[column]
        for column in LEARNED_SCORER_FEATURE_COLUMNS
    ]
    family = str(row["candidate_family"])
    one_hot = [1.0 if column == f"family::{family}" else 0.0 for column in family_columns]
    return [*numeric, *one_hot]


def _selector_feature_values(row: dict[str, Any]) -> dict[str, float]:
    if "selector_feature_prior_family_mean_regret_uah" in row:
        return {
            column: float(row[column])
            for column in LEARNED_SCORER_FEATURE_COLUMNS
        }
    return {
        "selector_feature_prior_family_mean_regret_uah": float(
            row.get("prior_family_mean_regret_uah", row["regret_uah"])
        ),
        "selector_feature_forecast_spread_uah_mwh": float(
            row.get("forecast_spread_uah_mwh", 0.0)
        ),
        "selector_feature_forecast_objective_value_uah": float(
            row["forecast_objective_value_uah"]
        ),
        "selector_feature_total_throughput_mwh": float(
            row.get("total_throughput_mwh", 0.0)
        ),
        "selector_feature_total_degradation_penalty_uah": float(
            row.get("total_degradation_penalty_uah", 0.0)
        ),
        "selector_feature_soc_min_slack_fraction": float(
            row.get("soc_min_slack_fraction", 0.0)
        ),
    }


def _select_rows_by_learned_scorer(
    rows: list[dict[str, Any]],
    *,
    label_rows_by_key: dict[tuple[str, str, datetime, str, str], dict[str, Any]],
    scorer: dict[str, Any],
    candidate_families: frozenset[str],
) -> list[dict[str, Any]]:
    selected_rows: list[dict[str, Any]] = []
    for _, anchor_rows in sorted(v2._rows_by_anchor(rows).items()):
        candidates = [
            row
            for row in anchor_rows
            if str(row["candidate_family"]) in candidate_families
        ]
        if not candidates:
            continue
        selected_rows.append(
            min(
                candidates,
                key=lambda row: (
                    _predict_learned_candidate_regret(
                        _label_row_or_candidate_row(
                            row, label_rows_by_key=label_rows_by_key
                        ),
                        scorer=scorer,
                    ),
                    v2._family_sort_index(str(row["candidate_family"])),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected_rows


def _label_row_or_candidate_row(
    row: dict[str, Any],
    *,
    label_rows_by_key: dict[tuple[str, str, datetime, str, str], dict[str, Any]],
) -> dict[str, Any]:
    label_row = label_rows_by_key.get(_candidate_identity_key(row))
    if label_row is None:
        return row
    return label_row


def _select_rows_by_model_row(
    rows: list[dict[str, Any]],
    *,
    learner_row: dict[str, Any],
    candidate_families: frozenset[str],
) -> list[dict[str, Any]]:
    if str(learner_row.get("selected_scorer_type", "")) == LEARNED_SCORER_TYPE:
        scorer = _learned_scorer_from_model_row(learner_row)
        selected_rows: list[dict[str, Any]] = []
        for _, anchor_rows in sorted(v2._rows_by_anchor(rows).items()):
            candidates = [
                row
                for row in anchor_rows
                if str(row["candidate_family"]) in candidate_families
            ]
            if not candidates:
                continue
            selected_rows.append(
                min(
                    candidates,
                    key=lambda row: (
                        _predict_learned_candidate_regret(row, scorer=scorer),
                        v2._family_sort_index(str(row["candidate_family"])),
                        str(row["candidate_model_name"]),
                    ),
                )
            )
        return selected_rows
    return _select_rows_by_value_score(
        rows,
        profile=_profile_from_model_row(learner_row),
        candidate_families=candidate_families,
        teacher_family_scores=dict(learner_row["teacher_family_scores"]),
    )


def _learned_scorer_from_model_row(row: dict[str, Any]) -> dict[str, Any]:
    family_columns = tuple(
        sorted(
            key
            for key in dict(row["selected_feature_weights"])
            if key.startswith("family::")
        )
    )
    return {
        "name": str(row["selected_value_profile_name"]),
        "scorer_type": LEARNED_SCORER_TYPE,
        "weights": dict(row["selected_feature_weights"]),
        "feature_means": dict(row.get("selected_feature_means", {})),
        "feature_scales": dict(row.get("selected_feature_scales", {})),
        "family_columns": family_columns,
    }


def _predict_learned_candidate_regret(row: dict[str, Any], *, scorer: dict[str, Any]) -> float:
    weights = dict(scorer["weights"])
    feature_means = dict(scorer["feature_means"])
    feature_scales = dict(scorer["feature_scales"])
    family_columns = tuple(str(column) for column in scorer["family_columns"])
    features = _learned_feature_vector(
        row,
        feature_means=feature_means,
        feature_scales=feature_scales,
        family_columns=family_columns,
    )
    feature_names = [*LEARNED_SCORER_FEATURE_COLUMNS, *family_columns]
    score = float(weights.get("intercept", 0.0))
    for feature_name, feature_value in zip(feature_names, features, strict=True):
        score += float(weights.get(feature_name, 0.0)) * feature_value
    return score


def _pairwise_regret_weighted_loss_by_scorer(
    train_label_rows: list[dict[str, Any]],
    *,
    scorer: dict[str, Any],
    candidate_families: frozenset[str],
) -> float:
    losses: list[float] = []
    rows_by_anchor: dict[datetime, list[dict[str, Any]]] = {}
    for row in train_label_rows:
        if str(row["candidate_family"]) not in candidate_families:
            continue
        anchor = v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        rows_by_anchor.setdefault(anchor, []).append(row)
    for anchor_rows in rows_by_anchor.values():
        for left_index, left in enumerate(anchor_rows):
            for right in anchor_rows[left_index + 1 :]:
                left_regret = float(left["label_regret_uah"])
                right_regret = float(right["label_regret_uah"])
                if abs(left_regret - right_regret) <= 1e-9:
                    continue
                better, worse = (left, right) if left_regret < right_regret else (right, left)
                if _predict_learned_candidate_regret(
                    better,
                    scorer=scorer,
                ) > _predict_learned_candidate_regret(worse, scorer=scorer):
                    losses.append(abs(left_regret - right_regret))
                else:
                    losses.append(0.0)
    return mean(losses) if losses else 0.0


def _select_value_profile(
    train_rows: list[dict[str, Any]],
    *,
    candidate_families: frozenset[str],
    teacher_family_scores: dict[str, float],
    pairwise_loss_weight: float,
) -> dict[str, float | str]:
    candidates: list[dict[str, float | str]] = []
    for profile in _VALUE_PROFILES:
        selected_rows = _select_rows_by_value_score(
            train_rows,
            profile=profile,
            candidate_families=candidate_families,
            teacher_family_scores=teacher_family_scores,
        )
        pairwise_loss = _pairwise_regret_weighted_loss(
            train_rows,
            profile=profile,
            candidate_families=candidate_families,
            teacher_family_scores=teacher_family_scores,
        )
        train_mean = v2._mean_regret(selected_rows)
        train_median = v2._median_regret(selected_rows)
        candidate = dict(profile)
        candidate["train_mean_regret_uah"] = train_mean
        candidate["train_median_regret_uah"] = train_median
        candidate["pairwise_loss_uah"] = pairwise_loss
        candidate["profile_objective_uah"] = train_mean + pairwise_loss_weight * pairwise_loss
        candidates.append(candidate)
    return min(
        candidates,
        key=lambda item: (
            float(item["profile_objective_uah"]),
            float(item["train_mean_regret_uah"]),
            str(item["name"]),
        ),
    )


def _select_rows_by_value_score(
    rows: list[dict[str, Any]],
    *,
    profile: dict[str, float | str],
    candidate_families: frozenset[str],
    teacher_family_scores: dict[str, float],
) -> list[dict[str, Any]]:
    selected_rows: list[dict[str, Any]] = []
    for _, anchor_rows in sorted(v2._rows_by_anchor(rows).items()):
        candidates = [
            row
            for row in anchor_rows
            if str(row["candidate_family"]) in candidate_families
        ]
        if not candidates:
            continue
        selected_rows.append(
            min(
                candidates,
                key=lambda row: (
                    _value_score(
                        row,
                        profile=profile,
                        teacher_family_scores=teacher_family_scores,
                    ),
                    v2._family_sort_index(str(row["candidate_family"])),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected_rows


def _value_score(
    row: dict[str, Any],
    *,
    profile: dict[str, float | str],
    teacher_family_scores: dict[str, float],
) -> float:
    family = str(row["candidate_family"])
    prior_regret = float(row.get("prior_family_mean_regret_uah", row["regret_uah"]))
    spread = float(row.get("forecast_spread_uah_mwh", 0.0))
    throughput = float(row.get("total_throughput_mwh", 0.0))
    degradation = float(row.get("total_degradation_penalty_uah", 0.0))
    soc_slack = float(row.get("soc_min_slack_fraction", 0.0))
    teacher_bonus = teacher_family_scores.get(family, 0.0)
    return (
        float(profile["prior"]) * prior_regret
        + float(profile["spread"]) * spread
        + float(profile["throughput"]) * throughput
        + float(profile["degradation"]) * degradation
        + float(profile["soc_slack"]) * soc_slack
        + float(profile["teacher_bonus"]) * teacher_bonus
    )


def _pairwise_regret_weighted_loss(
    train_rows: list[dict[str, Any]],
    *,
    profile: dict[str, float | str],
    candidate_families: frozenset[str],
    teacher_family_scores: dict[str, float],
) -> float:
    losses: list[float] = []
    for anchor_rows in v2._rows_by_anchor(train_rows).values():
        candidates = [
            row
            for row in anchor_rows
            if str(row["candidate_family"]) in candidate_families
        ]
        for left_index, left in enumerate(candidates):
            for right in candidates[left_index + 1 :]:
                left_regret = float(left["regret_uah"])
                right_regret = float(right["regret_uah"])
                if abs(left_regret - right_regret) <= 1e-9:
                    continue
                better, worse = (left, right) if left_regret < right_regret else (right, left)
                if _value_score(
                    better,
                    profile=profile,
                    teacher_family_scores=teacher_family_scores,
                ) > _value_score(
                    worse,
                    profile=profile,
                    teacher_family_scores=teacher_family_scores,
                ):
                    losses.append(abs(left_regret - right_regret))
                else:
                    losses.append(0.0)
    return mean(losses) if losses else 0.0


def _profile_from_model_row(row: dict[str, Any]) -> dict[str, float | str]:
    weights = dict(row["selected_feature_weights"])
    weights["name"] = str(row["selected_value_profile_name"])
    return weights


def _v2_plus_reference_rows(
    strict_frame: pl.DataFrame,
    *,
    tenant_id: str,
    source_model_name: str,
) -> dict[datetime, dict[str, Any]]:
    rows = [
        row
        for row in strict_frame.iter_rows(named=True)
        if str(row["tenant_id"]) == tenant_id
        and _source_model_name(row) == source_model_name
        and _selection_role(row)
        in {"schedule_value_learner_v2_plus", "schedule_value_learner_v2_plus_reference"}
    ]
    if not rows:
        raise ValueError(f"missing V2+ strict rows for {tenant_id}/{source_model_name}")
    return {
        v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
        for row in rows
    }


def _strict_benchmark_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    learner_row: dict[str, Any],
    role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(v2._payload(row))
    forecast_model_name = _forecast_model_name_for_role(
        row,
        source_model_name=source_model_name,
        role=role,
    )
    anchor_timestamp = v2._datetime_value(
        row["anchor_timestamp"],
        field_name="anchor_timestamp",
    )
    candidate_family = str(
        row.get("candidate_family", payload.get("selector_row_candidate_family", ""))
    )
    candidate_model_name = str(
        row.get(
            "candidate_model_name",
            payload.get("selector_row_candidate_model_name", forecast_model_name),
        )
    )
    payload.update(
        {
            "strict_gate_kind": "candidate_value_dfl_v3_strict_lp",
            "source_forecast_model_name": source_model_name,
            "learner_model_name": candidate_value_dfl_v3_model_name(source_model_name),
            "selected_value_profile_name": str(learner_row["selected_value_profile_name"]),
            "selected_feature_weights": dict(learner_row["selected_feature_weights"]),
            "teacher_family_scores": dict(learner_row["teacher_family_scores"]),
            "fallback_to_v2_plus": bool(learner_row["fallback_to_v2_plus"]),
            "selector_row_candidate_family": candidate_family,
            "selector_row_candidate_model_name": candidate_model_name,
            "selection_role": role,
            "claim_scope": CANDIDATE_VALUE_DFL_V3_STRICT_CLAIM_SCOPE,
            "academic_scope": CANDIDATE_VALUE_DFL_V3_ACADEMIC_SCOPE,
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": _safety_violation_count(row),
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:candidate-value-dfl-v3:{source_model_name}:"
            f"{role}:{candidate_family}:{candidate_model_name}:"
            f"{anchor_timestamp:%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": CANDIDATE_VALUE_DFL_V3_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": _starting_soc_fraction(row),
        "starting_soc_source": "schedule_candidate_library_v3",
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": _committed_action(row),
        "committed_power_mw": _committed_power_mw(row),
        "rank_by_regret": 1,
        "selection_role": role,
        "claim_scope": CANDIDATE_VALUE_DFL_V3_STRICT_CLAIM_SCOPE,
        "academic_scope": CANDIDATE_VALUE_DFL_V3_ACADEMIC_SCOPE,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": _safety_violation_count(row),
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "evaluation_payload": payload,
    }


def _starting_soc_fraction(row: dict[str, Any]) -> float:
    if "soc_fraction_vector" in row:
        return v2._first_or_default(row["soc_fraction_vector"], default=0.5)
    return float(row.get("starting_soc_fraction", 0.5))


def _safety_violation_count(row: dict[str, Any]) -> int:
    value = row.get("safety_violation_count", 0)
    return 0 if value is None else int(value)


def _committed_action(row: dict[str, Any]) -> str:
    if "dispatch_mw_vector" in row:
        return v2._committed_action(row)
    return str(row.get("committed_action", "HOLD"))


def _committed_power_mw(row: dict[str, Any]) -> float:
    if "dispatch_mw_vector" in row:
        return abs(v2._first_or_default(row["dispatch_mw_vector"], default=0.0))
    return abs(float(row.get("committed_power_mw", 0.0)))


def _forecast_model_name_for_role(
    row: dict[str, Any],
    *,
    source_model_name: str,
    role: str,
) -> str:
    if role == "strict_reference":
        return CONTROL_MODEL_NAME
    if role == "raw_reference":
        return source_model_name
    if role == "schedule_value_learner_v2_plus_reference":
        return str(row["forecast_model_name"]) if "forecast_model_name" in row else (
            v2_plus.schedule_value_learner_v2_plus_model_name(source_model_name)
        )
    if role == "candidate_value_dfl_v3":
        return candidate_value_dfl_v3_model_name(source_model_name)
    return str(row.get("forecast_model_name", source_model_name))


def _gate_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio_vs_v2_plus: float,
    min_mean_regret_improvement_ratio_vs_strict: float,
    include_promotion_failures: bool,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
    strict_rows = [
        row for row in source_rows if _selection_role(row) == "strict_reference"
    ]
    raw_rows = [row for row in source_rows if _selection_role(row) == "raw_reference"]
    v2_plus_rows = [
        row
        for row in source_rows
        if _selection_role(row) == "schedule_value_learner_v2_plus_reference"
    ]
    selected_rows = [
        row for row in source_rows if _selection_role(row) == "candidate_value_dfl_v3"
    ]
    strict_anchors = v2._tenant_anchor_set(strict_rows)
    raw_anchors = v2._tenant_anchor_set(raw_rows)
    v2_plus_anchors = v2._tenant_anchor_set(v2_plus_rows)
    selected_anchors = v2._tenant_anchor_set(selected_rows)
    if (
        strict_anchors != raw_anchors
        or strict_anchors != v2_plus_anchors
        or strict_anchors != selected_anchors
    ):
        failures.append(
            f"{source_model_name} strict/raw/V2+/V3 rows must cover matching tenant-anchor sets"
        )
    tenant_count = len({tenant_id for tenant_id, _ in selected_anchors})
    validation_count = len(selected_anchors)
    if tenant_count < min_tenant_count:
        failures.append(
            f"{source_model_name} tenant_count must be at least {min_tenant_count}; observed {tenant_count}"
        )
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {validation_count}"
        )
    failures.extend(
        v2._provenance_failures(
            [*strict_rows, *raw_rows, *v2_plus_rows, *selected_rows]
        )
    )
    strict_mean = v2._mean_regret(strict_rows)
    raw_mean = v2._mean_regret(raw_rows)
    v2_plus_mean = v2._mean_regret(v2_plus_rows)
    selected_mean = v2._mean_regret(selected_rows)
    strict_median = v2._median_regret(strict_rows)
    v2_plus_median = v2._median_regret(v2_plus_rows)
    selected_median = v2._median_regret(selected_rows)
    improvement_vs_raw = v2._improvement_ratio(raw_mean, selected_mean)
    improvement_vs_strict = v2._improvement_ratio(strict_mean, selected_mean)
    improvement_vs_v2_plus = v2._improvement_ratio(v2_plus_mean, selected_mean)
    development_passed = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_raw > 0.0
    )
    replacement_passed = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_strict >= min_mean_regret_improvement_ratio_vs_strict
        and improvement_vs_v2_plus > min_mean_regret_improvement_ratio_vs_v2_plus
        and selected_median <= v2_plus_median
        and not failures
    )
    if include_promotion_failures:
        if (
            selected_rows
            and v2_plus_rows
            and improvement_vs_v2_plus
            <= min_mean_regret_improvement_ratio_vs_v2_plus
        ):
            failures.append(
                f"{source_model_name} candidate-value DFL v3 must improve over frozen V2+ "
                f"by more than {min_mean_regret_improvement_ratio_vs_v2_plus:.1%}; "
                f"observed {improvement_vs_v2_plus:.1%}"
            )
        if (
            selected_rows
            and strict_rows
            and improvement_vs_strict < min_mean_regret_improvement_ratio_vs_strict
        ):
            failures.append(
                f"{source_model_name} mean regret improvement vs {CONTROL_MODEL_NAME} "
                f"must be at least {min_mean_regret_improvement_ratio_vs_strict:.1%}; "
                f"observed {improvement_vs_strict:.1%}"
            )
        if selected_rows and v2_plus_rows and selected_median > v2_plus_median:
            failures.append(
                f"{source_model_name} median regret must not be worse than frozen V2+; "
                f"observed learner={selected_median:.2f}, v2_plus={v2_plus_median:.2f}"
            )
    return {
        "source_model_name": source_model_name,
        "learner_model_name": candidate_value_dfl_v3_model_name(source_model_name),
        "tenant_count": tenant_count,
        "validation_tenant_anchor_count": validation_count,
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "v2_plus_mean_regret_uah": v2_plus_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "v2_plus_median_regret_uah": v2_plus_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "mean_regret_improvement_ratio_vs_v2_plus": improvement_vs_v2_plus,
        "development_gate_passed": development_passed,
        "offline_strategy_replacement_passed": replacement_passed,
        "market_execution_enabled": False,
        "failures": failures,
    }, failures


def _source_model_name(row: dict[str, Any]) -> str:
    if "source_model_name" in row:
        return str(row["source_model_name"])
    payload = v2._payload(row)
    return str(payload.get("source_forecast_model_name", ""))


def _tenant_source_anchor_key(row: dict[str, Any]) -> tuple[str, str, datetime]:
    return (
        str(row["tenant_id"]),
        _source_model_name(row),
        v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
    )


def _strict_reference_regret_by_anchor(
    rows: list[dict[str, Any]],
    *,
    role: str,
) -> dict[tuple[str, str, datetime], float]:
    return {
        _tenant_source_anchor_key(row): float(row["regret_uah"])
        for row in rows
        if _selection_role(row) == role
    }


def _fallback_count_for_source(
    model_rows_by_source: dict[tuple[str, str], dict[str, Any]],
    *,
    source_model_name: str,
) -> int:
    return sum(
        1
        for (_, model_source_name), row in model_rows_by_source.items()
        if model_source_name == source_model_name and bool(row["fallback_to_v2_plus"])
    )


def _candidate_family_failure_diagnosis(
    *,
    candidate_family: str,
    mean_delta_vs_v2_plus: float,
    win_rate_vs_v2_plus: float,
    selected_count: int,
) -> str:
    if mean_delta_vs_v2_plus < 0.0:
        return "template_beats_v2_plus_candidate" if _is_prior_template(candidate_family) else (
            "candidate_family_beats_v2_plus"
        )
    if _is_prior_template(candidate_family):
        if win_rate_vs_v2_plus >= 0.25 and selected_count == 0:
            return "template_competitive_but_not_selected"
        return "template_not_competitive_vs_v2_plus"
    if selected_count > 0:
        return "selected_candidate_worse_than_v2_plus"
    return "candidate_not_competitive_vs_v2_plus"


def _is_prior_template(candidate_family: str) -> bool:
    return candidate_family in {
        CANDIDATE_FAMILY_PRIOR_BEST_TEMPLATE_V3,
        CANDIDATE_FAMILY_PRIOR_ORACLE_RESIDUAL_V3,
    }


def _selection_role(row: dict[str, Any]) -> str:
    if row.get("selection_role"):
        return str(row["selection_role"])
    payload = v2._payload(row)
    return str(payload.get("selection_role", payload.get("selector_row_role", "")))


__all__ = [
    "CANDIDATE_FAMILY_DEGRADATION_SWEEP_V3",
    "CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V3",
    "CANDIDATE_FAMILY_PRIOR_BEST_TEMPLATE_V3",
    "CANDIDATE_FAMILY_PRIOR_ORACLE_RESIDUAL_V3",
    "CANDIDATE_VALUE_DFL_V3_FAILURE_AUDIT_CLAIM_SCOPE",
    "CANDIDATE_VALUE_DFL_V3_STRICT_LP_STRATEGY_KIND",
    "build_dfl_candidate_value_dfl_v3_failure_audit_frame",
    "build_dfl_candidate_value_dfl_v3_frame",
    "build_dfl_candidate_value_dfl_v3_strict_lp_benchmark_frame",
    "build_dfl_candidate_value_label_panel_v3_frame",
    "build_dfl_schedule_candidate_library_v3_frame",
    "candidate_value_dfl_v3_model_name",
    "evaluate_dfl_candidate_value_dfl_v3_gate",
    "validate_dfl_candidate_value_dfl_v3_evidence",
    "validate_dfl_candidate_value_dfl_v3_failure_audit_evidence",
    "validate_dfl_candidate_value_label_panel_v3_evidence",
]
