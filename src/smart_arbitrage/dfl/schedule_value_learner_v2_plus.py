"""V2+ schedule/value regret autopsy and fallback selector evidence."""

from __future__ import annotations

from datetime import datetime
from statistics import mean
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl import strict_challenger
from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_SCHEDULE_VALUE_REGRET_DECOMPOSITION_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_value_regret_decomposition_not_full_dfl"
)
DFL_SCHEDULE_CANDIDATE_LIBRARY_V2_PLUS_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_candidate_library_v2_plus_not_full_dfl"
)
DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_value_learner_v2_plus_not_full_dfl"
)
DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_value_learner_v2_plus_strict_lp_gate_not_full_dfl"
)
DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_schedule_value_learner_v2_plus_strict_lp_benchmark"
)
DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_PREFIX: Final[str] = (
    "dfl_schedule_value_learner_v2_plus_"
)
DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_PROFILE_NAME: Final[str] = (
    "v2_plus_prior_best_with_v2_fallback"
)
DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ACADEMIC_SCOPE: Final[str] = (
    "Prior-only V2+ schedule/value selector over expanded feasible LP-scored "
    "schedule candidates. It keeps frozen V2 as fallback and is Offline Strategy "
    "Promotion evidence only: not full DFL, not Decision Transformer control, "
    "and not market execution."
)

CANDIDATE_FAMILY_RANK_EXTREMA_V2_PLUS: Final[str] = (
    "rank_extrema_perturbation_v2_plus"
)
CANDIDATE_FAMILY_ROBUST_SPREAD_V2_PLUS: Final[str] = (
    "robust_spread_penalty_v2_plus"
)
CANDIDATE_FAMILY_STRICT_NEIGHBORHOOD_V2_PLUS: Final[str] = (
    "strict_neighborhood_shift_v2_plus"
)
CANDIDATE_FAMILY_TEMPORAL_BLOCK_V2_PLUS: Final[str] = (
    "temporal_block_reconciled_v2_plus"
)
CANDIDATE_FAMILY_SOC_TERMINAL_V2_PLUS: Final[str] = (
    "soc_terminal_target_v2_plus"
)
V2_PLUS_CANDIDATE_FAMILIES: Final[frozenset[str]] = frozenset(
    {
        CANDIDATE_FAMILY_RANK_EXTREMA_V2_PLUS,
        CANDIDATE_FAMILY_ROBUST_SPREAD_V2_PLUS,
        CANDIDATE_FAMILY_STRICT_NEIGHBORHOOD_V2_PLUS,
        CANDIDATE_FAMILY_TEMPORAL_BLOCK_V2_PLUS,
        CANDIDATE_FAMILY_SOC_TERMINAL_V2_PLUS,
    }
)

REQUIRED_DECOMPOSITION_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "split_name",
        "strict_regret_uah",
        "v2_regret_uah",
        "best_candidate_regret_uah",
        "failure_mode",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_MODEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "learner_model_name",
        "selected_weight_profile_name",
        "train_anchor_count",
        "final_holdout_anchor_count",
        "selected_train_mean_regret_uah",
        "selected_final_mean_regret_uah",
        "fallback_to_v2",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_STRICT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "forecast_model_name",
        "strategy_kind",
        "anchor_timestamp",
        "generated_at",
        "regret_uah",
        "selection_role",
        "evaluation_payload",
    }
)


def schedule_value_learner_v2_plus_model_name(source_model_name: str) -> str:
    """Return the stable DFL v2+ schedule/value learner model name."""

    return f"{DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_PREFIX}{source_model_name}"


def build_dfl_schedule_value_regret_decomposition_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    learner_v2_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Explain where frozen V2 still loses value on final holdout anchors."""

    v2._validate_library_frame(schedule_candidate_library_frame)
    v2._validate_learner_frame(learner_v2_frame)
    learner_rows = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in learner_v2_frame.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    grouped = _rows_by_tenant_source_anchor(schedule_candidate_library_frame)
    for key in sorted(grouped, key=lambda item: (item[1], item[0], item[2])):
        tenant_id, source_model_name, anchor_timestamp = key
        anchor_rows = grouped[key]
        if str(anchor_rows[0]["split_name"]) != "final_holdout":
            continue
        learner_row = learner_rows.get((tenant_id, source_model_name))
        if learner_row is None:
            raise ValueError(f"missing v2 learner row for {tenant_id}/{source_model_name}")
        strict_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_STRICT)
        raw_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_RAW)
        v2_row = v2._select_rows_by_score(
            _base_candidate_rows(anchor_rows),
            profile=v2._profile_by_name(str(learner_row["selected_weight_profile_name"])),
        )[0]
        best_row = min(
            anchor_rows,
            key=lambda row: (
                float(row["regret_uah"]),
                v2._family_sort_index(str(row["candidate_family"])),
                str(row["candidate_model_name"]),
            ),
        )
        rows.append(
            {
                "tenant_id": tenant_id,
                "source_model_name": source_model_name,
                "anchor_timestamp": anchor_timestamp,
                "split_name": "final_holdout",
                "strict_candidate_family": str(strict_row["candidate_family"]),
                "raw_candidate_family": str(raw_row["candidate_family"]),
                "v2_selected_candidate_family": str(v2_row["candidate_family"]),
                "v2_selected_candidate_model_name": str(v2_row["candidate_model_name"]),
                "best_candidate_family": str(best_row["candidate_family"]),
                "best_candidate_model_name": str(best_row["candidate_model_name"]),
                "strict_regret_uah": float(strict_row["regret_uah"]),
                "raw_regret_uah": float(raw_row["regret_uah"]),
                "v2_regret_uah": float(v2_row["regret_uah"]),
                "best_candidate_regret_uah": float(best_row["regret_uah"]),
                "regret_gap_v2_to_best_candidate_uah": max(
                    0.0,
                    float(v2_row["regret_uah"]) - float(best_row["regret_uah"]),
                ),
                "strict_minus_v2_regret_uah": (
                    float(strict_row["regret_uah"]) - float(v2_row["regret_uah"])
                ),
                "candidate_family_count": len(
                    {str(row["candidate_family"]) for row in anchor_rows}
                ),
                "forecast_spread_uah_mwh": float(v2_row["forecast_spread_uah_mwh"]),
                "actual_spread_uah_mwh": float(v2_row.get("actual_spread_uah_mwh", 0.0)),
                "forecast_top_k_actual_overlap": float(
                    v2_row.get("forecast_top_k_actual_overlap", 1.0)
                ),
                "forecast_bottom_k_actual_overlap": float(
                    v2_row.get("forecast_bottom_k_actual_overlap", 1.0)
                ),
                "peak_index_abs_error": float(v2_row.get("peak_index_abs_error", 0.0)),
                "trough_index_abs_error": float(v2_row.get("trough_index_abs_error", 0.0)),
                "soc_min_slack_fraction": float(v2_row.get("soc_min_slack_fraction", 1.0)),
                "total_throughput_mwh": float(v2_row["total_throughput_mwh"]),
                "total_degradation_penalty_uah": float(
                    v2_row["total_degradation_penalty_uah"]
                ),
                "failure_mode": _failure_mode(
                    strict_row=strict_row,
                    v2_row=v2_row,
                    best_row=best_row,
                ),
                "claim_scope": DFL_SCHEDULE_VALUE_REGRET_DECOMPOSITION_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id", "anchor_timestamp"])


def build_dfl_schedule_candidate_library_v2_plus_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    *,
    rank_perturbation_delta_uah_mwh: float = 250.0,
    robust_spread_scales: tuple[float, ...] = (0.8, 0.9),
    strict_neighborhood_shift_hours: tuple[int, ...] = (-1, 1),
    block_reconcile_hours: tuple[int, ...] = (3, 6),
    terminal_target_shift_uah_mwh: float = 100.0,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Expand a V2 schedule library with deterministic prior-safe families."""

    v2._validate_library_frame(schedule_candidate_library_frame)
    if rank_perturbation_delta_uah_mwh < 0.0:
        raise ValueError("rank_perturbation_delta_uah_mwh must not be negative.")
    if any(scale <= 0.0 for scale in robust_spread_scales):
        raise ValueError("robust_spread_scales must contain positive values.")
    if any(block_size <= 0 for block_size in block_reconcile_hours):
        raise ValueError("block_reconcile_hours must contain positive values.")
    if not strict_neighborhood_shift_hours:
        raise ValueError("strict_neighborhood_shift_hours must contain at least one shift.")
    resolved_generated_at = generated_at or v2._latest_generated_at(
        schedule_candidate_library_frame
    )
    rows = [_source_library_row(row) for row in schedule_candidate_library_frame.iter_rows(named=True)]
    grouped = _rows_by_tenant_source_anchor(schedule_candidate_library_frame)
    for key in sorted(grouped, key=lambda item: (item[0], item[1], item[2])):
        _, source_model_name, _ = key
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
        rows.append(
            _evaluated_candidate_row(
                strict_row,
                source_model_name=source_model_name,
                candidate_family=CANDIDATE_FAMILY_RANK_EXTREMA_V2_PLUS,
                candidate_model_name=(
                    f"dfl_schedule_library_v2_plus_rank_extrema_{source_model_name}"
                ),
                forecast_prices=_rank_extrema_perturbation(
                    raw_forecast,
                    delta=rank_perturbation_delta_uah_mwh,
                ),
                generated_at=resolved_generated_at,
                metadata={"rank_perturbation_delta_uah_mwh": rank_perturbation_delta_uah_mwh},
            )
        )
        for scale in robust_spread_scales:
            rows.append(
                _evaluated_candidate_row(
                    strict_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_ROBUST_SPREAD_V2_PLUS,
                    candidate_model_name=(
                        "dfl_schedule_library_v2_plus_robust_spread_"
                        f"{source_model_name}_{scale:.2f}"
                    ),
                    forecast_prices=_scale_spread(raw_forecast, scale=scale),
                    generated_at=resolved_generated_at,
                    metadata={"robust_spread_scale": scale},
                )
            )
        for shift_hours in strict_neighborhood_shift_hours:
            if shift_hours == 0:
                continue
            rows.append(
                _evaluated_candidate_row(
                    strict_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_STRICT_NEIGHBORHOOD_V2_PLUS,
                    candidate_model_name=(
                        "dfl_schedule_library_v2_plus_strict_shift_"
                        f"{source_model_name}_{shift_hours:+d}"
                    ),
                    forecast_prices=_shift_vector(strict_forecast, shift_hours),
                    generated_at=resolved_generated_at,
                    metadata={"strict_neighborhood_shift_hours": shift_hours},
                )
            )
        for block_size in block_reconcile_hours:
            rows.append(
                _evaluated_candidate_row(
                    strict_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_TEMPORAL_BLOCK_V2_PLUS,
                    candidate_model_name=(
                        "dfl_schedule_library_v2_plus_block_reconciled_"
                        f"{source_model_name}_{block_size}h"
                    ),
                    forecast_prices=_block_reconciled(raw_forecast, block_size=block_size),
                    generated_at=resolved_generated_at,
                    metadata={"block_reconcile_hours": block_size},
                )
            )
        rows.append(
            _evaluated_candidate_row(
                strict_row,
                source_model_name=source_model_name,
                candidate_family=CANDIDATE_FAMILY_SOC_TERMINAL_V2_PLUS,
                candidate_model_name=(
                    f"dfl_schedule_library_v2_plus_soc_terminal_{source_model_name}"
                ),
                forecast_prices=_terminal_target_adjustment(
                    raw_forecast,
                    shift_uah_mwh=terminal_target_shift_uah_mwh,
                ),
                generated_at=resolved_generated_at,
                metadata={"terminal_target_shift_uah_mwh": terminal_target_shift_uah_mwh},
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


def build_dfl_schedule_value_learner_v2_plus_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    learner_v2_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int = 18,
    min_prior_mean_improvement_ratio_vs_v2: float = 0.01,
) -> pl.DataFrame:
    """Select V2+ candidates with a non-degradation fallback to frozen V2."""

    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if final_validation_anchor_count_per_tenant <= 0:
        raise ValueError("final_validation_anchor_count_per_tenant must be positive.")
    if min_prior_mean_improvement_ratio_vs_v2 < 0.0:
        raise ValueError("min_prior_mean_improvement_ratio_vs_v2 must not be negative.")
    v2._validate_library_frame(schedule_candidate_library_frame)
    v2._validate_learner_frame(learner_v2_frame)
    v2_rows = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in learner_v2_frame.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = v2._library_rows(
                schedule_candidate_library_frame,
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
                    f"{tenant_id}/{source_model_name} schedule/value learner v2+ needs train rows"
                )
            v2_learner_row = v2_rows.get((tenant_id, source_model_name))
            if v2_learner_row is None:
                raise ValueError(f"missing v2 learner row for {tenant_id}/{source_model_name}")
            v2_profile = v2._profile_by_name(str(v2_learner_row["selected_weight_profile_name"]))
            selected_v2_train_rows = v2._select_rows_by_score(
                _base_candidate_rows(train_rows), profile=v2_profile
            )
            selected_v2_final_rows = v2._select_rows_by_score(
                _base_candidate_rows(final_rows), profile=v2_profile
            )
            selected_plus_train_rows = _best_plus_or_v2_rows(
                train_rows,
                v2_rows_by_anchor=selected_v2_train_rows,
            )
            selected_plus_final_rows = _best_plus_or_v2_rows(
                final_rows,
                v2_rows_by_anchor=selected_v2_final_rows,
            )
            v2_train_mean = v2._mean_regret(selected_v2_train_rows)
            plus_train_mean = v2._mean_regret(selected_plus_train_rows)
            fallback_to_v2 = (
                v2._improvement_ratio(v2_train_mean, plus_train_mean)
                < min_prior_mean_improvement_ratio_vs_v2
            )
            selected_train_rows = selected_v2_train_rows if fallback_to_v2 else selected_plus_train_rows
            selected_final_rows = selected_v2_final_rows if fallback_to_v2 else selected_plus_final_rows
            strict_train_rows = v2._selected_family_rows(
                train_rows, v2.CANDIDATE_FAMILY_STRICT
            )
            raw_train_rows = v2._selected_family_rows(
                train_rows, v2.CANDIDATE_FAMILY_RAW
            )
            strict_final_rows = v2._selected_family_rows(
                final_rows, v2.CANDIDATE_FAMILY_STRICT
            )
            raw_final_rows = v2._selected_family_rows(
                final_rows, v2.CANDIDATE_FAMILY_RAW
            )
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "learner_model_name": schedule_value_learner_v2_plus_model_name(
                        source_model_name
                    ),
                    "selected_weight_profile_name": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_PROFILE_NAME,
                    "selected_feature_names": [
                        "prior_family_mean_regret_uah",
                        "regret_uah",
                        "candidate_family",
                    ],
                    "selected_feature_weights": {
                        "selection_rule": "lowest_prior_regret_candidate_with_v2_fallback",
                        "min_prior_mean_improvement_ratio_vs_v2": (
                            min_prior_mean_improvement_ratio_vs_v2
                        ),
                    },
                    "fallback_to_v2": fallback_to_v2,
                    "train_anchor_count": len(v2._anchor_set(train_rows)),
                    "final_holdout_anchor_count": final_anchor_count,
                    "final_holdout_tenant_anchor_count": final_anchor_count * len(tenant_ids),
                    "strict_train_mean_regret_uah": v2._mean_regret(strict_train_rows),
                    "raw_train_mean_regret_uah": v2._mean_regret(raw_train_rows),
                    "v2_train_mean_regret_uah": v2_train_mean,
                    "v2_plus_candidate_train_mean_regret_uah": plus_train_mean,
                    "selected_train_mean_regret_uah": v2._mean_regret(selected_train_rows),
                    "strict_train_median_regret_uah": v2._median_regret(strict_train_rows),
                    "selected_train_median_regret_uah": v2._median_regret(selected_train_rows),
                    "strict_final_mean_regret_uah": v2._mean_regret(strict_final_rows),
                    "raw_final_mean_regret_uah": v2._mean_regret(raw_final_rows),
                    "v2_final_mean_regret_uah": v2._mean_regret(selected_v2_final_rows),
                    "v2_plus_candidate_final_mean_regret_uah": v2._mean_regret(
                        selected_plus_final_rows
                    ),
                    "selected_final_mean_regret_uah": v2._mean_regret(selected_final_rows),
                    "strict_final_median_regret_uah": v2._median_regret(strict_final_rows),
                    "selected_final_median_regret_uah": v2._median_regret(selected_final_rows),
                    "selected_train_family_counts": v2._family_counts(selected_train_rows),
                    "selected_final_family_counts": v2._family_counts(selected_final_rows),
                    "train_mean_regret_improvement_ratio_vs_v2": v2._improvement_ratio(
                        v2_train_mean,
                        v2._mean_regret(selected_train_rows),
                    ),
                    "final_mean_regret_improvement_ratio_vs_v2": v2._improvement_ratio(
                        v2._mean_regret(selected_v2_final_rows),
                        v2._mean_regret(selected_final_rows),
                    ),
                    "final_mean_regret_improvement_ratio_vs_strict": v2._improvement_ratio(
                        v2._mean_regret(strict_final_rows),
                        v2._mean_regret(selected_final_rows),
                    ),
                    "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_CLAIM_SCOPE,
                    "academic_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ACADEMIC_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id"])


def build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    learner_v2_plus_frame: pl.DataFrame,
    learner_v2_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict/raw/V2/V2+ rows for the V2+ strict LP/oracle gate."""

    v2._validate_library_frame(schedule_candidate_library_frame)
    _validate_learner_v2_plus_frame(learner_v2_plus_frame)
    v2._validate_learner_frame(learner_v2_frame)
    resolved_generated_at = generated_at or v2._latest_generated_at(
        schedule_candidate_library_frame
    )
    rows: list[dict[str, Any]] = []
    library_rows = list(schedule_candidate_library_frame.iter_rows(named=True))
    v2_rows = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in learner_v2_frame.iter_rows(named=True)
    }
    for learner_row in learner_v2_plus_frame.iter_rows(named=True):
        tenant_id = str(learner_row["tenant_id"])
        source_model_name = str(learner_row["source_model_name"])
        v2_learner_row = v2_rows.get((tenant_id, source_model_name))
        if v2_learner_row is None:
            raise ValueError(f"missing v2 learner row for {tenant_id}/{source_model_name}")
        final_rows = [
            row
            for row in library_rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
            and str(row["split_name"]) == "final_holdout"
        ]
        selected_v2_by_anchor = {
            v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
            for row in v2._select_rows_by_score(
                _base_candidate_rows(final_rows),
                profile=v2._profile_by_name(str(v2_learner_row["selected_weight_profile_name"])),
            )
        }
        selected_plus_by_anchor = {
            v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
            for row in _selected_rows_from_learner_row(
                final_rows,
                learner_row=learner_row,
                selected_v2_rows=list(selected_v2_by_anchor.values()),
            )
        }
        for anchor_timestamp in sorted(selected_plus_by_anchor):
            anchor_rows = [
                row
                for row in final_rows
                if v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
                == anchor_timestamp
            ]
            strict_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_STRICT)
            raw_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_RAW)
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
                        selected_v2_by_anchor[anchor_timestamp],
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="schedule_value_learner_v2_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row(
                        selected_plus_by_anchor[anchor_timestamp],
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="schedule_value_learner_v2_plus",
                        generated_at=resolved_generated_at,
                    ),
                ]
            )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(
        ["tenant_id", "source_model_name", "anchor_timestamp", "selection_role"]
    )


def validate_dfl_schedule_value_learner_v2_plus_evidence(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> EvidenceCheckOutcome:
    """Validate V2+ strict evidence without requiring promotion."""

    missing_columns = sorted(REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"schedule/value learner v2+ evidence is missing required columns: {missing_columns}",
            {"row_count": strict_frame.height},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(
            False, "schedule/value learner v2+ evidence has no rows", {"row_count": 0}
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
            min_mean_regret_improvement_ratio=DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
            include_promotion_failures=False,
        )
        summaries.append(summary)
        failures.extend(summary_failures)
    return EvidenceCheckOutcome(
        not failures,
        "Schedule/value learner v2+ evidence has valid coverage and claim boundaries."
        if not failures
        else "; ".join(failures),
        {
            "row_count": strict_frame.height,
            "source_model_count": len(source_names),
            "source_model_names": list(source_names),
            "model_summaries": summaries,
        },
    )


def evaluate_dfl_schedule_value_learner_v2_plus_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> PromotionGateResult:
    """Evaluate V2+ against strict and frozen V2."""

    missing_columns = sorted(REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing_columns:
        return PromotionGateResult(
            False,
            "blocked",
            f"schedule/value learner v2+ strict frame is missing required columns: {missing_columns}",
            {},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return PromotionGateResult(False, "blocked", "schedule/value learner v2+ strict frame has no rows", {})
    source_names = source_model_names or tuple(
        sorted({_source_model_name(row) for row in rows})
    )
    summaries: list[dict[str, Any]] = []
    failures: list[str] = []
    for source_model_name in source_names:
        summary, summary_failures = _gate_summary(
            rows,
            source_model_name=source_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
            include_promotion_failures=True,
        )
        summaries.append(summary)
        failures.extend(summary_failures)
    production_passing = [
        summary for summary in summaries if summary["production_gate_passed"]
    ]
    development_passing = [
        summary for summary in summaries if summary["development_gate_passed"]
    ]
    best = max(
        summaries,
        key=lambda summary: float(summary["mean_regret_improvement_ratio_vs_v2"]),
    )
    metrics = {
        "best_source_model_name": best["source_model_name"],
        "tenant_count": best["tenant_count"],
        "validation_tenant_anchor_count": best["validation_tenant_anchor_count"],
        "strict_mean_regret_uah": best["strict_mean_regret_uah"],
        "raw_mean_regret_uah": best["raw_mean_regret_uah"],
        "v2_mean_regret_uah": best["v2_mean_regret_uah"],
        "selected_mean_regret_uah": best["selected_mean_regret_uah"],
        "strict_median_regret_uah": best["strict_median_regret_uah"],
        "v2_median_regret_uah": best["v2_median_regret_uah"],
        "selected_median_regret_uah": best["selected_median_regret_uah"],
        "mean_regret_improvement_ratio_vs_strict": best["mean_regret_improvement_ratio_vs_strict"],
        "mean_regret_improvement_ratio_vs_raw": best["mean_regret_improvement_ratio_vs_raw"],
        "mean_regret_improvement_ratio_vs_v2": best["mean_regret_improvement_ratio_vs_v2"],
        "development_gate_passed": bool(development_passing),
        "production_gate_passed": bool(production_passing),
        "market_execution_enabled": False,
        "passing_source_model_names": [
            str(summary["source_model_name"]) for summary in production_passing
        ],
        "model_summaries": summaries,
    }
    if production_passing and not failures:
        return PromotionGateResult(
            True,
            "promote",
            "schedule/value learner v2+ passes strict LP/oracle and frozen V2 gate",
            metrics,
        )
    if development_passing:
        return PromotionGateResult(
            False,
            "diagnostic_pass_production_blocked",
            "schedule/value learner v2+ improves over raw neural schedules but remains "
            f"blocked versus {CONTROL_MODEL_NAME} or frozen V2 evidence: "
            + "; ".join(failures),
            metrics,
        )
    return PromotionGateResult(
        False,
        "blocked",
        "; ".join(failures)
        if failures
        else "schedule/value learner v2+ has no development improvement",
        metrics,
    )


def _failure_mode(
    *,
    strict_row: dict[str, Any],
    v2_row: dict[str, Any],
    best_row: dict[str, Any],
) -> str:
    strict_regret = float(strict_row["regret_uah"])
    v2_regret = float(v2_row["regret_uah"])
    best_regret = float(best_row["regret_uah"])
    if max(strict_regret, v2_regret) <= 25.0:
        return "strict_already_near_oracle"
    if v2_regret - best_regret >= max(25.0, 0.05 * max(v2_regret, 1.0)):
        return "selector_chose_wrong_family"
    top_overlap = float(v2_row.get("forecast_top_k_actual_overlap", 1.0))
    bottom_overlap = float(v2_row.get("forecast_bottom_k_actual_overlap", 1.0))
    if min(top_overlap, bottom_overlap) < 0.5 or float(v2_row.get("peak_index_abs_error", 0.0)) >= 3.0:
        return "forecast_extrema_rank_wrong"
    if float(v2_row.get("soc_min_slack_fraction", 1.0)) <= 0.05:
        return "soc_path_constrained"
    if best_regret >= 0.95 * v2_regret:
        return "candidate_library_missing_good_schedule"
    return "decision_value_gap_remaining"


def _source_library_row(row: dict[str, Any]) -> dict[str, Any]:
    copied = dict(row)
    payload = dict(v2._payload(row))
    payload.update(
        {
            "claim_scope": DFL_SCHEDULE_CANDIDATE_LIBRARY_V2_PLUS_CLAIM_SCOPE,
            "candidate_library_version": "v2_plus_source",
            "no_leakage_prior_only": True,
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    copied["claim_scope"] = DFL_SCHEDULE_CANDIDATE_LIBRARY_V2_PLUS_CLAIM_SCOPE
    copied["candidate_library_version"] = "v2_plus_source"
    copied["evaluation_payload"] = payload
    return copied


def _evaluated_candidate_row(
    reference_row: dict[str, Any],
    *,
    source_model_name: str,
    candidate_family: str,
    candidate_model_name: str,
    forecast_prices: list[float],
    generated_at: datetime,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    enriched_metadata = {
        **metadata,
        "candidate_library_version": "v2_plus_generated",
        "no_leakage_prior_only": True,
    }
    row = strict_challenger._evaluated_candidate_row(
        reference_row,
        source_model_name=source_model_name,
        candidate_family=candidate_family,
        candidate_model_name=candidate_model_name,
        forecast_prices=forecast_prices,
        generated_at=generated_at,
        metadata=enriched_metadata,
    )
    payload = dict(v2._payload(row))
    payload.update(
        {
            "claim_scope": DFL_SCHEDULE_CANDIDATE_LIBRARY_V2_PLUS_CLAIM_SCOPE,
            "candidate_library_version": "v2_plus_generated",
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    row["claim_scope"] = DFL_SCHEDULE_CANDIDATE_LIBRARY_V2_PLUS_CLAIM_SCOPE
    row["candidate_library_version"] = "v2_plus_generated"
    row["evaluation_payload"] = payload
    return row


def _rank_extrema_perturbation(values: list[float], *, delta: float) -> list[float]:
    if not values:
        return []
    k = max(1, len(values) // 6)
    high_indices = _rank_indices(values, largest=True)[:k]
    low_indices = _rank_indices(values, largest=False)[:k]
    adjusted = values.copy()
    for index in high_indices:
        adjusted[index] += delta
    for index in low_indices:
        adjusted[index] -= delta
    return adjusted


def _scale_spread(values: list[float], *, scale: float) -> list[float]:
    if not values:
        return []
    center = mean(values)
    return [center + (value - center) * scale for value in values]


def _shift_vector(values: list[float], shift_hours: int) -> list[float]:
    if not values:
        return []
    width = len(values)
    shift = shift_hours % width
    return values[-shift:] + values[:-shift] if shift else values.copy()


def _block_reconciled(values: list[float], *, block_size: int) -> list[float]:
    reconciled: list[float] = []
    for start in range(0, len(values), block_size):
        block = values[start : start + block_size]
        block_mean = mean(block)
        reconciled.extend([0.5 * value + 0.5 * block_mean for value in block])
    return reconciled


def _terminal_target_adjustment(values: list[float], *, shift_uah_mwh: float) -> list[float]:
    midpoint = len(values) // 2
    return [
        value + (shift_uah_mwh if index >= midpoint else 0.0)
        for index, value in enumerate(values)
    ]


def _rank_indices(values: list[float], *, largest: bool) -> list[int]:
    return sorted(
        range(len(values)),
        key=lambda index: (values[index], -index if largest else index),
        reverse=largest,
    )


def _best_plus_or_v2_rows(
    rows: list[dict[str, Any]],
    *,
    v2_rows_by_anchor: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    v2_by_anchor = {
        v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
        for row in v2_rows_by_anchor
    }
    selected_rows: list[dict[str, Any]] = []
    for anchor_timestamp, anchor_rows in sorted(v2._rows_by_anchor(rows).items()):
        plus_rows = [
            row
            for row in anchor_rows
            if str(row["candidate_family"]) in V2_PLUS_CANDIDATE_FAMILIES
        ]
        fallback_row = v2_by_anchor[anchor_timestamp]
        if not plus_rows:
            selected_rows.append(fallback_row)
            continue
        selected_rows.append(
            min(
                [fallback_row, *plus_rows],
                key=lambda row: (
                    float(row["prior_family_mean_regret_uah"]),
                    float(row["regret_uah"]),
                    str(row["candidate_family"]),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected_rows


def _base_candidate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if str(row["candidate_family"]) not in V2_PLUS_CANDIDATE_FAMILIES
    ]


def _selected_rows_from_learner_row(
    rows: list[dict[str, Any]],
    *,
    learner_row: dict[str, Any],
    selected_v2_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if bool(learner_row["fallback_to_v2"]):
        return selected_v2_rows
    return _best_plus_or_v2_rows(rows, v2_rows_by_anchor=selected_v2_rows)


def _validate_learner_v2_plus_frame(frame: pl.DataFrame) -> None:
    v2._require_columns(
        frame, REQUIRED_MODEL_COLUMNS, frame_name="schedule_value_learner_v2_plus_frame"
    )
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_CLAIM_SCOPE:
            raise ValueError("schedule/value learner v2+ frame has an unexpected claim_scope")
        if not bool(row["not_full_dfl"]):
            raise ValueError("schedule/value learner v2+ rows must keep not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError(
                "schedule/value learner v2+ rows must keep not_market_execution=true"
            )


def _strict_benchmark_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    learner_row: dict[str, Any],
    role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(v2._payload(row))
    learner_model_name = schedule_value_learner_v2_plus_model_name(source_model_name)
    forecast_model_name = _forecast_model_name_for_role(
        row,
        source_model_name=source_model_name,
        role=role,
    )
    anchor_timestamp = v2._datetime_value(
        row["anchor_timestamp"], field_name="anchor_timestamp"
    )
    payload.update(
        {
            "strict_gate_kind": "dfl_schedule_value_learner_v2_plus_strict_lp",
            "source_forecast_model_name": source_model_name,
            "learner_model_name": learner_model_name,
            "selected_weight_profile_name": str(learner_row["selected_weight_profile_name"]),
            "selected_feature_names": list(learner_row["selected_feature_names"]),
            "selected_feature_weights": dict(learner_row["selected_feature_weights"]),
            "fallback_to_v2": bool(learner_row["fallback_to_v2"]),
            "selector_row_candidate_family": str(row["candidate_family"]),
            "selector_row_candidate_model_name": str(row["candidate_model_name"]),
            "selector_row_role": role,
            "selection_role": role,
            "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_CLAIM_SCOPE,
            "academic_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ACADEMIC_SCOPE,
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": int(row["safety_violation_count"]),
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:schedule-value-learner-v2-plus:{source_model_name}:"
            f"{role}:{row['candidate_family']}:{anchor_timestamp:%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": v2._first_or_default(
            row["soc_fraction_vector"], default=0.5
        ),
        "starting_soc_source": "schedule_candidate_library_v2_plus",
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": v2._committed_action(row),
        "committed_power_mw": abs(
            v2._first_or_default(row["dispatch_mw_vector"], default=0.0)
        ),
        "rank_by_regret": 1,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": int(row["safety_violation_count"]),
        "selection_role": role,
        "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evaluation_payload": payload,
    }


def _forecast_model_name_for_role(
    row: dict[str, Any],
    *,
    source_model_name: str,
    role: str,
) -> str:
    if role == "schedule_value_learner_v2_plus":
        return schedule_value_learner_v2_plus_model_name(source_model_name)
    if role == "schedule_value_learner_v2_reference":
        return v2.schedule_value_learner_v2_model_name(source_model_name)
    return str(row["candidate_model_name"])


def _gate_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
    include_promotion_failures: bool,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
    strict_rows = [
        row for row in source_rows if _selection_role(row) == "strict_reference"
    ]
    raw_rows = [row for row in source_rows if _selection_role(row) == "raw_reference"]
    v2_rows = [
        row
        for row in source_rows
        if _selection_role(row) == "schedule_value_learner_v2_reference"
    ]
    selected_rows = [
        row
        for row in source_rows
        if _selection_role(row) == "schedule_value_learner_v2_plus"
    ]
    strict_anchors = v2._tenant_anchor_set(strict_rows)
    raw_anchors = v2._tenant_anchor_set(raw_rows)
    v2_anchors = v2._tenant_anchor_set(v2_rows)
    selected_anchors = v2._tenant_anchor_set(selected_rows)
    if (
        strict_anchors != raw_anchors
        or strict_anchors != v2_anchors
        or strict_anchors != selected_anchors
    ):
        failures.append(
            f"{source_model_name} strict/raw/v2/v2+ rows must cover matching tenant-anchor sets"
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
        _provenance_failures([*strict_rows, *raw_rows, *v2_rows, *selected_rows])
    )
    strict_mean = v2._mean_regret(strict_rows)
    raw_mean = v2._mean_regret(raw_rows)
    v2_mean = v2._mean_regret(v2_rows)
    selected_mean = v2._mean_regret(selected_rows)
    strict_median = v2._median_regret(strict_rows)
    v2_median = v2._median_regret(v2_rows)
    selected_median = v2._median_regret(selected_rows)
    improvement_vs_raw = v2._improvement_ratio(raw_mean, selected_mean)
    improvement_vs_strict = v2._improvement_ratio(strict_mean, selected_mean)
    improvement_vs_v2 = v2._improvement_ratio(v2_mean, selected_mean)
    development_passed = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_raw > 0.0
    )
    production_passed = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_strict >= min_mean_regret_improvement_ratio
        and improvement_vs_v2 > 0.0
        and selected_median <= strict_median
        and selected_median <= v2_median
        and not failures
    )
    if include_promotion_failures:
        if selected_rows and strict_rows and improvement_vs_strict < min_mean_regret_improvement_ratio:
            failures.append(
                f"{source_model_name} mean regret improvement vs {CONTROL_MODEL_NAME} must be at least "
                f"{min_mean_regret_improvement_ratio:.1%}; observed {improvement_vs_strict:.1%}"
            )
        if selected_rows and v2_rows and improvement_vs_v2 <= 0.0:
            failures.append(
                f"{source_model_name} v2+ must improve frozen Schedule/Value Learner V2; "
                f"observed improvement_vs_v2={improvement_vs_v2:.1%}"
            )
        if selected_rows and strict_rows and selected_median > strict_median:
            failures.append(
                f"{source_model_name} median regret must not be worse than {CONTROL_MODEL_NAME}; "
                f"observed learner={selected_median:.2f}, strict={strict_median:.2f}"
            )
        if selected_rows and v2_rows and selected_median > v2_median:
            failures.append(
                f"{source_model_name} v2+ median regret must not degrade V2; "
                f"observed learner={selected_median:.2f}, v2={v2_median:.2f}"
            )
    return {
        "source_model_name": source_model_name,
        "learner_model_name": schedule_value_learner_v2_plus_model_name(source_model_name),
        "tenant_count": tenant_count,
        "validation_tenant_anchor_count": validation_count,
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "v2_mean_regret_uah": v2_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "v2_median_regret_uah": v2_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "mean_regret_improvement_ratio_vs_v2": improvement_vs_v2,
        "development_gate_passed": development_passed,
        "production_gate_passed": production_passed,
        "market_execution_enabled": False,
        "failures": failures,
    }, failures


def _provenance_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    for row in rows:
        payload = v2._payload(row)
        if str(row.get("data_quality_tier", payload.get("data_quality_tier"))) != "thesis_grade":
            failures.append("schedule/value learner v2+ rows must be thesis_grade")
            break
        if float(row.get("observed_coverage_ratio", payload.get("observed_coverage_ratio", 0.0))) < 1.0:
            failures.append("schedule/value learner v2+ rows must have observed coverage")
            break
        if int(row.get("safety_violation_count", payload.get("safety_violation_count", 1))):
            failures.append("schedule/value learner v2+ rows must have zero safety violations")
            break
        if not bool(row.get("not_full_dfl", payload.get("not_full_dfl", False))):
            failures.append("schedule/value learner v2+ rows must keep not_full_dfl=true")
            break
        if not bool(row.get("not_market_execution", payload.get("not_market_execution", False))):
            failures.append("schedule/value learner v2+ rows must keep not_market_execution=true")
            break
    return failures


def _rows_by_tenant_source_anchor(
    frame: pl.DataFrame,
) -> dict[tuple[str, str, datetime], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, datetime], list[dict[str, Any]]] = {}
    for row in frame.iter_rows(named=True):
        key = (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        )
        grouped.setdefault(key, []).append(row)
    return grouped


def _source_model_name(row: dict[str, Any]) -> str:
    if "source_model_name" in row:
        return str(row["source_model_name"])
    payload = v2._payload(row)
    return str(payload.get("source_forecast_model_name", ""))


def _selection_role(row: dict[str, Any]) -> str:
    if "selection_role" in row:
        return str(row["selection_role"])
    payload = v2._payload(row)
    return str(payload.get("selection_role", ""))


__all__ = [
    "DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND",
    "build_dfl_schedule_candidate_library_v2_plus_frame",
    "build_dfl_schedule_value_learner_v2_plus_frame",
    "build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    "build_dfl_schedule_value_regret_decomposition_frame",
    "evaluate_dfl_schedule_value_learner_v2_plus_gate",
    "schedule_value_learner_v2_plus_model_name",
    "validate_dfl_schedule_value_learner_v2_plus_evidence",
]
