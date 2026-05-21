"""Tail-risk-aware LAVA/DT target redesign.

This module consumes the LAVA schedule-neighbor bridge as diagnostic evidence.
It does not imitate raw hourly actions. Instead, it prepares a conservative
candidate-index target that blocks prior-observed tail-risk perturbation
families and keeps frozen V2+ as the default comparator/fallback.
"""

from __future__ import annotations

from datetime import UTC, datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.lava_schedule_neighbor_bridge import (
    STRICT_REFERENCE_ROLE,
    V2_PLUS_REFERENCE_ROLE,
)
from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.dfl.tft_quantile_schedule_value import (
    FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
)

DFL_LAVA_TAIL_RISK_DIAGNOSTIC_CLAIM_SCOPE: Final[str] = (
    "dfl_lava_tail_risk_diagnostic_not_full_dfl"
)
DFL_LAVA_TAIL_RISK_AWARE_TARGET_CLAIM_SCOPE: Final[str] = (
    "dfl_lava_tail_risk_aware_target_not_full_dfl"
)
DFL_LAVA_TAIL_RISK_AWARE_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_lava_tail_risk_aware_strict_lp_gate_not_full_dfl"
)
DFL_LAVA_TAIL_RISK_AWARE_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_lava_tail_risk_aware_strict_lp_benchmark"
)
DFL_LAVA_TAIL_RISK_AWARE_MODEL_NAME: Final[str] = (
    "dfl_lava_tail_risk_aware_candidate_target_v1"
)
DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE: Final[str] = (
    "lava_tail_risk_aware_target"
)
DFL_LAVA_SAFE_SWITCH_CLAIM_SCOPE: Final[str] = (
    "dfl_lava_tail_risk_safe_switch_not_full_dfl"
)
DFL_LAVA_SAFE_SWITCH_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_lava_tail_risk_safe_switch_strict_lp_gate_not_full_dfl"
)
DFL_LAVA_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_lava_tail_risk_safe_switch_strict_lp_benchmark"
)
DFL_LAVA_SAFE_SWITCH_MODEL_NAME: Final[str] = (
    "dfl_lava_tail_risk_safe_switch_v1"
)
DFL_LAVA_SAFE_SWITCH_SELECTION_ROLE: Final[str] = (
    "lava_tail_risk_safe_switch"
)

_REQUIRED_CANDIDATE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "candidate_family",
        "candidate_model_name",
        "candidate_source",
        "anchor_timestamp",
        "generated_at",
        "split_name",
        "horizon_hours",
        "forecast_price_uah_mwh_vector",
        "dispatch_mw_vector",
        "soc_fraction_vector",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "regret_ratio",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "evaluation_payload",
        "eligible_for_final_selection",
        "label_regret_delta_vs_v2_plus_uah",
        "selector_feature_schedule_distance_from_v2_plus",
        "selector_feature_total_throughput_delta_mwh",
        "selector_feature_terminal_soc_delta_fraction",
        "selector_feature_forecast_spread_uah_mwh",
        "selector_feature_total_degradation_penalty_uah",
        "selector_feature_poland_shadow_candidate",
        "selector_feature_oracle_train_diagnostic",
    }
)
_REQUIRED_STRICT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "forecast_model_name",
        "selection_role",
        "anchor_timestamp",
        "regret_uah",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "evaluation_payload",
    }
)


def build_dfl_lava_tail_risk_diagnostic_frame(
    lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    lava_candidate_value_strict_lp_benchmark_frame: pl.DataFrame,
    *,
    tail_risk_delta_uah: float = 150.0,
) -> pl.DataFrame:
    """Classify bridge candidates by safe value, weak value, or tail-risk loss."""

    _require_columns(
        lava_schedule_neighbor_candidate_frame,
        _REQUIRED_CANDIDATE_COLUMNS,
        frame_name="lava_schedule_neighbor_candidate_frame",
    )
    _require_columns(
        lava_candidate_value_strict_lp_benchmark_frame,
        _REQUIRED_STRICT_COLUMNS,
        frame_name="lava_candidate_value_strict_lp_benchmark_frame",
    )
    if tail_risk_delta_uah <= 0.0:
        raise ValueError("tail_risk_delta_uah must be positive.")
    failed_selected = {
        _selected_signature(row)
        for row in lava_candidate_value_strict_lp_benchmark_frame.iter_rows(named=True)
        if str(row["selection_role"]) == "lava_candidate_value_scorer"
        and float(row["regret_uah"]) > tail_risk_delta_uah
        and _selected_signature(row) is not None
    }
    rows: list[dict[str, Any]] = []
    for row in lava_schedule_neighbor_candidate_frame.iter_rows(named=True):
        delta = float(row["label_regret_delta_vs_v2_plus_uah"])
        candidate_source = str(row["candidate_source"])
        candidate_family = str(row["candidate_family"])
        split_name = str(row["split_name"])
        is_train = split_name != "final_holdout"
        selected_by_failed_scorer = _candidate_signature(row) in failed_selected
        if candidate_source == "v2_plus_default":
            diagnostic_class = "v2_plus_default"
            recommendation = "fallback_candidate"
        elif not bool(row["eligible_for_final_selection"]):
            diagnostic_class = "oracle_only_train_diagnostic"
            recommendation = "diagnostic_only_do_not_train_target"
        elif delta >= tail_risk_delta_uah:
            diagnostic_class = "tail_risk_perturbation_loss"
            recommendation = "block_family_for_dt_target"
        elif delta < 0.0:
            diagnostic_class = "safe_neighbor_candidate"
            recommendation = "candidate_teacher_win"
        else:
            diagnostic_class = "neutral_or_weak_neighbor"
            recommendation = "fallback_candidate"
        if selected_by_failed_scorer and diagnostic_class != "tail_risk_perturbation_loss":
            recommendation = "audit_failed_scorer_selection"
        rows.append(
            {
                "tenant_id": str(row["tenant_id"]),
                "source_model_name": str(row["source_model_name"]),
                "candidate_family": candidate_family,
                "candidate_model_name": str(row["candidate_model_name"]),
                "candidate_source": candidate_source,
                "anchor_timestamp": _datetime_value(row["anchor_timestamp"]),
                "generated_at": _datetime_value(row["generated_at"]),
                "split_name": split_name,
                "is_train_or_prior_anchor": is_train,
                "eligible_for_final_selection": bool(
                    row["eligible_for_final_selection"]
                ),
                "label_regret_delta_vs_v2_plus_uah": delta,
                "tail_risk_delta_uah": tail_risk_delta_uah,
                "selected_by_failed_lava_scorer": selected_by_failed_scorer,
                "tail_risk_diagnostic_class": diagnostic_class,
                "target_recommendation": recommendation,
                "target_space": "schedule_candidate_index",
                "raw_hourly_action_imitation": False,
                "claim_scope": DFL_LAVA_TAIL_RISK_DIAGNOSTIC_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(rows).sort(
        ["tenant_id", "anchor_timestamp", "split_name", "candidate_source", "candidate_family"]
    )


def build_dfl_lava_tail_risk_aware_target_frame(
    lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    lava_tail_risk_diagnostic_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    min_prior_safe_win_count: int = 1,
    max_prior_tail_loss_count: int = 0,
    hard_blocked_candidate_families: tuple[str, ...] = (
        "rank_extrema_perturbation_v2_plus",
    ),
) -> pl.DataFrame:
    """Select schedule-candidate targets from prior diagnostics, not final labels."""

    _require_columns(
        lava_schedule_neighbor_candidate_frame,
        _REQUIRED_CANDIDATE_COLUMNS,
        frame_name="lava_schedule_neighbor_candidate_frame",
    )
    _require_columns(
        lava_tail_risk_diagnostic_frame,
        frozenset(
            {
                "tenant_id",
                "candidate_family",
                "candidate_source",
                "split_name",
                "tail_risk_diagnostic_class",
                "label_regret_delta_vs_v2_plus_uah",
                "market_execution_enabled",
            }
        ),
        frame_name="lava_tail_risk_diagnostic_frame",
    )
    if not tenant_ids:
        raise ValueError("tenant_ids must not be empty.")
    if min_prior_safe_win_count < 1:
        raise ValueError("min_prior_safe_win_count must be at least 1.")
    if max_prior_tail_loss_count < 0:
        raise ValueError("max_prior_tail_loss_count must not be negative.")
    candidate_rows = list(lava_schedule_neighbor_candidate_frame.iter_rows(named=True))
    diagnostic_rows = list(lava_tail_risk_diagnostic_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        tenant_candidates = [row for row in candidate_rows if str(row["tenant_id"]) == tenant_id]
        train_diagnostics = [
            row
            for row in diagnostic_rows
            if str(row["tenant_id"]) == tenant_id and str(row["split_name"]) != "final_holdout"
        ]
        family_stats = _family_diagnostic_stats(train_diagnostics)
        hard_blocked = set(hard_blocked_candidate_families)
        blocked_families = sorted(
            family
            for family, stats in family_stats.items()
            if (
                stats["tail_loss_count"] > max_prior_tail_loss_count
                or family in hard_blocked
            )
            and family not in {"frozen_v2_plus_fallback", "strict_control"}
        )
        allowed_families = sorted(
            family
            for family, stats in family_stats.items()
            if stats["safe_win_count"] >= min_prior_safe_win_count
            and stats["tail_loss_count"] <= max_prior_tail_loss_count
            and family not in hard_blocked
            and family not in {"frozen_v2_plus_fallback", "strict_control"}
        )
        family_priority = {
            family: stats["mean_prior_delta_uah"]
            for family, stats in family_stats.items()
            if family in allowed_families
        }
        final_rows = [
            row
            for row in tenant_candidates
            if str(row["split_name"]) == "final_holdout"
            and bool(row["eligible_for_final_selection"])
        ]
        fallback_rows = _source_rows(final_rows, "v2_plus_default")
        fallback = not allowed_families
        selected_final = (
            fallback_rows
            if fallback
            else _select_prior_safe_final_rows(final_rows, allowed_families, family_priority)
        )
        if not selected_final:
            selected_final = fallback_rows
            fallback = True
        output_rows.append(
            {
                "tenant_id": tenant_id,
                "target_model_name": DFL_LAVA_TAIL_RISK_AWARE_MODEL_NAME,
                "target_label_space": "schedule_candidate_index",
                "raw_hourly_action_imitation": False,
                "fallback_to_v2_plus": fallback,
                "selector_gate_blocker": (
                    "tail_risk_filtered_safe_family_selected"
                    if not fallback
                    else "no_prior_safe_family_after_tail_risk_filter"
                ),
                "blocked_candidate_families": blocked_families,
                "allowed_candidate_families": allowed_families,
                "hard_blocked_candidate_families": sorted(hard_blocked),
                "family_prior_mean_delta_uah": family_priority,
                "train_anchor_count": _anchor_count(
                    [
                        row
                        for row in tenant_candidates
                        if str(row["split_name"]) != "final_holdout"
                    ]
                ),
                "final_holdout_anchor_count": _anchor_count(final_rows),
                "selected_final_candidate_keys": [_candidate_key(row) for row in selected_final],
                "selected_final_family_counts": _family_counts(selected_final),
                "selected_final_candidate_source_counts": _source_counts(selected_final),
                "claim_scope": DFL_LAVA_TAIL_RISK_AWARE_TARGET_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(output_rows).sort(["tenant_id"])


def build_dfl_lava_tail_risk_aware_strict_lp_benchmark_frame(
    lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    lava_tail_risk_aware_target_frame: pl.DataFrame,
    frozen_v2_plus_strict_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Strict-score the tail-risk-aware candidate target against V2+."""

    _require_columns(
        lava_schedule_neighbor_candidate_frame,
        _REQUIRED_CANDIDATE_COLUMNS,
        frame_name="lava_schedule_neighbor_candidate_frame",
    )
    _require_columns(
        lava_tail_risk_aware_target_frame,
        frozenset(
            {
                "tenant_id",
                "selected_final_candidate_keys",
                "fallback_to_v2_plus",
                "blocked_candidate_families",
                "raw_hourly_action_imitation",
                "market_execution_enabled",
            }
        ),
        frame_name="lava_tail_risk_aware_target_frame",
    )
    _require_columns(
        frozen_v2_plus_strict_frame,
        _REQUIRED_STRICT_COLUMNS,
        frame_name="frozen_v2_plus_strict_frame",
    )
    resolved_generated_at = generated_at or _latest_generated_at(
        lava_schedule_neighbor_candidate_frame
    )
    candidate_rows = list(lava_schedule_neighbor_candidate_frame.iter_rows(named=True))
    candidate_by_key = {_candidate_key(row): row for row in candidate_rows}
    v2_reference_by_tenant: dict[str, list[dict[str, Any]]] = {}
    output_rows: list[dict[str, Any]] = []
    for row in frozen_v2_plus_strict_frame.iter_rows(named=True):
        if str(row["selection_role"]) not in {
            "strict_reference",
            "schedule_value_learner_v2_plus",
        }:
            continue
        role = (
            STRICT_REFERENCE_ROLE
            if str(row["selection_role"]) == "strict_reference"
            else V2_PLUS_REFERENCE_ROLE
        )
        if (
            role == V2_PLUS_REFERENCE_ROLE
            and str(row["source_model_name"]) == baseline_source_model_name
        ):
            v2_reference_by_tenant.setdefault(str(row["tenant_id"]), []).append(row)
        output_rows.append(
            _strict_reference_row(
                row,
                selection_role=role,
                generated_at=resolved_generated_at,
            )
        )
    for target_row in lava_tail_risk_aware_target_frame.iter_rows(named=True):
        if bool(target_row["fallback_to_v2_plus"]):
            for selected in sorted(
                v2_reference_by_tenant.get(str(target_row["tenant_id"]), []),
                key=lambda row: _datetime_value(row["anchor_timestamp"]),
            ):
                output_rows.append(
                    _strict_fallback_reference_row(
                        selected,
                        target_row=target_row,
                        generated_at=resolved_generated_at,
                    )
                )
            continue
        for key in target_row["selected_final_candidate_keys"]:
            selected = candidate_by_key[str(key)]
            output_rows.append(
                _strict_candidate_row(
                    selected,
                    target_row=target_row,
                    generated_at=resolved_generated_at,
                )
            )
    return pl.DataFrame(output_rows).sort(
        ["tenant_id", "anchor_timestamp", "selection_role", "forecast_model_name"]
    )


def build_dfl_lava_tail_risk_safe_switch_scorer_frame(
    lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    lava_tail_risk_diagnostic_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    min_prior_safe_win_count: int = 2,
    max_prior_tail_loss_count: int = 0,
    min_prior_precision: float = 0.75,
    min_prior_mean_improvement_uah: float = 1.0,
    allowed_candidate_sources: tuple[str, ...] = ("poland_shadow_candidate",),
    require_family_tail_loss_free: bool = True,
    hard_blocked_candidate_families: tuple[str, ...] = (
        "rank_extrema_perturbation_v2_plus",
    ),
) -> pl.DataFrame:
    """Train a conservative prior-only safe-switch scorer over candidate profiles."""

    _require_columns(
        lava_schedule_neighbor_candidate_frame,
        _REQUIRED_CANDIDATE_COLUMNS,
        frame_name="lava_schedule_neighbor_candidate_frame",
    )
    _require_columns(
        lava_tail_risk_diagnostic_frame,
        frozenset(
            {
                "tenant_id",
                "candidate_family",
                "candidate_model_name",
                "anchor_timestamp",
                "split_name",
                "tail_risk_diagnostic_class",
                "label_regret_delta_vs_v2_plus_uah",
                "market_execution_enabled",
            }
        ),
        frame_name="lava_tail_risk_diagnostic_frame",
    )
    if not tenant_ids:
        raise ValueError("tenant_ids must not be empty.")
    if min_prior_safe_win_count < 1:
        raise ValueError("min_prior_safe_win_count must be at least 1.")
    if max_prior_tail_loss_count < 0:
        raise ValueError("max_prior_tail_loss_count must not be negative.")
    if not 0.0 <= min_prior_precision <= 1.0:
        raise ValueError("min_prior_precision must be between 0 and 1.")
    if min_prior_mean_improvement_uah < 0.0:
        raise ValueError("min_prior_mean_improvement_uah must not be negative.")
    if not allowed_candidate_sources:
        raise ValueError("allowed_candidate_sources must not be empty.")
    hard_blocked = set(hard_blocked_candidate_families)
    allowed_sources = set(allowed_candidate_sources)
    candidate_rows = list(lava_schedule_neighbor_candidate_frame.iter_rows(named=True))
    diagnostic_by_signature = {
        _candidate_signature(row): row
        for row in lava_tail_risk_diagnostic_frame.iter_rows(named=True)
    }
    output_rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        tenant_rows = [row for row in candidate_rows if str(row["tenant_id"]) == tenant_id]
        train_rows = [
            row
            for row in tenant_rows
            if str(row["split_name"]) != "final_holdout"
            and bool(row["eligible_for_final_selection"])
        ]
        final_rows = [
            row
            for row in tenant_rows
            if str(row["split_name"]) == "final_holdout"
            and bool(row["eligible_for_final_selection"])
        ]
        if not train_rows:
            raise ValueError(f"{tenant_id} safe switch needs train rows.")
        if not final_rows:
            raise ValueError(f"{tenant_id} safe switch needs final rows.")
        profile_stats = _safe_switch_profile_stats(
            train_rows,
            diagnostic_by_signature=diagnostic_by_signature,
            hard_blocked_candidate_families=hard_blocked,
            allowed_candidate_sources=allowed_sources,
        )
        allowed_profiles = sorted(
            profile
            for profile, stats in profile_stats.items()
            if int(stats["safe_win_count"]) >= min_prior_safe_win_count
            and int(stats["tail_loss_count"]) <= max_prior_tail_loss_count
            and (
                not require_family_tail_loss_free
                or int(stats["family_tail_loss_count"]) <= max_prior_tail_loss_count
            )
            and float(stats["safe_precision"]) >= min_prior_precision
            and float(stats["mean_prior_delta_uah"]) <= -min_prior_mean_improvement_uah
            and not bool(stats["hard_blocked"])
        )
        selected_final: list[dict[str, Any]] = []
        fallback_anchor_keys: list[str] = []
        for anchor, anchor_rows in sorted(_rows_by_anchor(final_rows).items()):
            candidates = [
                row
                for row in anchor_rows
                if _safe_switch_profile_key(row) in allowed_profiles
                and str(row["candidate_family"]) not in hard_blocked
                and str(row["candidate_source"]) in allowed_sources
            ]
            if not candidates:
                fallback_anchor_keys.append(_anchor_key_from_parts(tenant_id, anchor[1]))
                continue
            selected_final.append(
                min(
                    candidates,
                    key=lambda row: (
                        float(
                            profile_stats[_safe_switch_profile_key(row)][
                                "mean_prior_delta_uah"
                            ]
                        ),
                        float(
                            profile_stats[_safe_switch_profile_key(row)][
                                "tail_loss_count"
                            ]
                        ),
                        float(row["selector_feature_schedule_distance_from_v2_plus"]),
                        float(row["selector_feature_total_throughput_delta_mwh"]),
                        str(row["candidate_family"]),
                        str(row["candidate_model_name"]),
                    ),
                )
            )
        fallback_count = len(fallback_anchor_keys)
        selected_counts = _source_counts(selected_final)
        if fallback_count:
            selected_counts["frozen_v2_plus_fallback"] = fallback_count
        selected_family_counts = _family_counts(selected_final)
        if fallback_count:
            selected_family_counts["frozen_v2_plus_fallback"] = fallback_count
        output_rows.append(
            {
                "tenant_id": tenant_id,
                "learner_model_name": DFL_LAVA_SAFE_SWITCH_MODEL_NAME,
                "target_label_space": "schedule_candidate_index",
                "raw_hourly_action_imitation": False,
                "selected_scorer_type": "prior_profile_safe_switch",
                "selected_feature_names": [
                    "candidate_family",
                    "candidate_source",
                    "selector_feature_schedule_distance_from_v2_plus",
                    "selector_feature_total_throughput_delta_mwh",
                    "selector_feature_terminal_soc_delta_fraction",
                    "selector_feature_forecast_spread_uah_mwh",
                    "selector_feature_poland_shadow_candidate",
                ],
                "allowed_risk_profiles": allowed_profiles,
                "blocked_risk_profiles": sorted(
                    profile
                    for profile, stats in profile_stats.items()
                    if profile not in allowed_profiles or bool(stats["hard_blocked"])
                ),
                "risk_profile_prior_stats": profile_stats,
                "fallback_to_v2_plus": not selected_final,
                "uses_v2_plus_anchor_fallback": bool(fallback_anchor_keys),
                "selector_gate_blocker": (
                    "safe_switch_profile_selected"
                    if selected_final
                    else "no_prior_safe_profile_after_tail_risk_filter"
                ),
                "hard_blocked_candidate_families": sorted(hard_blocked),
                "allowed_candidate_sources": sorted(allowed_sources),
                "require_family_tail_loss_free": require_family_tail_loss_free,
                "train_anchor_count": _anchor_count(train_rows),
                "final_holdout_anchor_count": _anchor_count(final_rows),
                "fallback_final_anchor_keys": fallback_anchor_keys,
                "selected_final_candidate_keys": [_candidate_key(row) for row in selected_final],
                "selected_final_profile_keys": [
                    _safe_switch_profile_key(row) for row in selected_final
                ],
                "selected_final_family_counts": selected_family_counts,
                "selected_final_candidate_source_counts": selected_counts,
                "claim_scope": DFL_LAVA_SAFE_SWITCH_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(output_rows).sort(["tenant_id"])


def build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame(
    lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    lava_tail_risk_safe_switch_scorer_frame: pl.DataFrame,
    frozen_v2_plus_strict_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Strict-score the prior-profile safe switch against frozen V2+."""

    _require_columns(
        lava_schedule_neighbor_candidate_frame,
        _REQUIRED_CANDIDATE_COLUMNS,
        frame_name="lava_schedule_neighbor_candidate_frame",
    )
    _require_columns(
        lava_tail_risk_safe_switch_scorer_frame,
        frozenset(
            {
                "tenant_id",
                "selected_final_candidate_keys",
                "fallback_final_anchor_keys",
                "selected_final_candidate_source_counts",
                "raw_hourly_action_imitation",
                "market_execution_enabled",
            }
        ),
        frame_name="lava_tail_risk_safe_switch_scorer_frame",
    )
    _require_columns(
        frozen_v2_plus_strict_frame,
        _REQUIRED_STRICT_COLUMNS,
        frame_name="frozen_v2_plus_strict_frame",
    )
    resolved_generated_at = generated_at or _latest_generated_at(
        lava_schedule_neighbor_candidate_frame
    )
    candidate_rows = list(lava_schedule_neighbor_candidate_frame.iter_rows(named=True))
    candidate_by_key = {_candidate_key(row): row for row in candidate_rows}
    v2_reference_by_anchor: dict[str, dict[str, Any]] = {}
    output_rows: list[dict[str, Any]] = []
    for row in frozen_v2_plus_strict_frame.iter_rows(named=True):
        if str(row["selection_role"]) not in {
            "strict_reference",
            "schedule_value_learner_v2_plus",
        }:
            continue
        role = (
            STRICT_REFERENCE_ROLE
            if str(row["selection_role"]) == "strict_reference"
            else V2_PLUS_REFERENCE_ROLE
        )
        if (
            role == V2_PLUS_REFERENCE_ROLE
            and str(row["source_model_name"]) == baseline_source_model_name
        ):
            v2_reference_by_anchor[_anchor_key(row)] = row
        output_rows.append(
            _safe_switch_reference_row(
                row,
                selection_role=role,
                generated_at=resolved_generated_at,
            )
        )
    for scorer_row in lava_tail_risk_safe_switch_scorer_frame.iter_rows(named=True):
        for key in scorer_row["selected_final_candidate_keys"]:
            selected = candidate_by_key[str(key)]
            output_rows.append(
                _safe_switch_candidate_row(
                    selected,
                    scorer_row=scorer_row,
                    generated_at=resolved_generated_at,
                )
            )
        for anchor_key in scorer_row["fallback_final_anchor_keys"]:
            fallback = v2_reference_by_anchor.get(str(anchor_key))
            if fallback is None:
                raise ValueError(f"Missing frozen V2+ fallback row for {anchor_key}.")
            output_rows.append(
                _safe_switch_fallback_row(
                    fallback,
                    scorer_row=scorer_row,
                    generated_at=resolved_generated_at,
                )
            )
    return pl.DataFrame(output_rows).sort(
        ["tenant_id", "anchor_timestamp", "selection_role", "forecast_model_name"]
    )


def evaluate_dfl_lava_tail_risk_aware_gate(
    strict_frame: pl.DataFrame,
    *,
    min_validation_tenant_anchor_count: int = 90,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.0,
    min_mean_regret_improvement_ratio_vs_strict: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
) -> PromotionGateResult:
    """Gate the redesigned target against V2+, not just strict control."""

    _require_columns(
        strict_frame,
        frozenset(
            {
                "tenant_id",
                "selection_role",
                "anchor_timestamp",
                "regret_uah",
                "not_market_execution",
                "market_execution_enabled",
            }
        ),
        frame_name="tail-risk-aware strict frame",
    )
    summaries = _role_summaries(strict_frame)
    selected = summaries.get(DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE)
    v2_plus = summaries.get(V2_PLUS_REFERENCE_ROLE)
    strict = summaries.get(STRICT_REFERENCE_ROLE)
    failures: list[str] = []
    if selected is None:
        failures.append("missing tail-risk-aware target rows")
    if v2_plus is None:
        failures.append("missing V2+ reference rows")
    if strict is None:
        failures.append("missing strict reference rows")
    validation_count = _tenant_anchor_count(
        strict_frame.filter(
            pl.col("selection_role") == DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE
        )
    )
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            "tail-risk-aware validation tenant-anchor count below required "
            f"{min_validation_tenant_anchor_count}"
        )
    if failures or selected is None or v2_plus is None or strict is None:
        return PromotionGateResult(
            False,
            "blocked",
            "; ".join(failures),
            {
                "role_summaries": summaries,
                "validation_tenant_anchor_count": validation_count,
                "market_execution_enabled": False,
            },
        )
    improvement_vs_v2 = _improvement_ratio(
        float(v2_plus["mean_regret_uah"]),
        float(selected["mean_regret_uah"]),
    )
    improvement_vs_strict = _improvement_ratio(
        float(strict["mean_regret_uah"]),
        float(selected["mean_regret_uah"]),
    )
    median_degraded = float(selected["median_regret_uah"]) > float(
        v2_plus["median_regret_uah"]
    )
    if improvement_vs_v2 < min_mean_regret_improvement_ratio_vs_v2_plus:
        failures.append("mean_not_improved_vs_v2_plus")
    if improvement_vs_strict < min_mean_regret_improvement_ratio_vs_strict:
        failures.append(f"mean_not_improved_vs_{CONTROL_MODEL_NAME}")
    if median_degraded:
        failures.append("median_degraded_vs_v2_plus")
    metrics = {
        "selected_mean_regret_uah": selected["mean_regret_uah"],
        "v2_plus_mean_regret_uah": v2_plus["mean_regret_uah"],
        "strict_mean_regret_uah": strict["mean_regret_uah"],
        "selected_median_regret_uah": selected["median_regret_uah"],
        "v2_plus_median_regret_uah": v2_plus["median_regret_uah"],
        "mean_regret_improvement_ratio_vs_v2_plus": improvement_vs_v2,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "validation_tenant_anchor_count": validation_count,
        "role_summaries": summaries,
        "market_execution_enabled": False,
        "offline_strategy_challenger_passed": not failures,
        "production_promote": False,
    }
    if failures:
        return PromotionGateResult(False, "blocked", "; ".join(failures), metrics)
    return PromotionGateResult(
        True,
        "offline_strategy_challenger",
        "Tail-risk-aware LAVA target beats V2+ under strict LP/oracle evidence",
        metrics,
    )


def evaluate_dfl_lava_tail_risk_safe_switch_gate(
    strict_frame: pl.DataFrame,
    *,
    min_validation_tenant_anchor_count: int = 90,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.0,
    min_mean_regret_improvement_ratio_vs_strict: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
) -> PromotionGateResult:
    """Gate the safe-switch scorer against V2+, not just strict control."""

    _require_columns(
        strict_frame,
        frozenset(
            {
                "tenant_id",
                "selection_role",
                "anchor_timestamp",
                "regret_uah",
                "not_market_execution",
                "market_execution_enabled",
            }
        ),
        frame_name="safe-switch strict frame",
    )
    return _evaluate_against_v2_plus(
        strict_frame,
        selected_role=DFL_LAVA_SAFE_SWITCH_SELECTION_ROLE,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            min_mean_regret_improvement_ratio_vs_strict
        ),
        success_description=(
            "Tail-risk safe-switch scorer beats V2+ under strict LP/oracle evidence"
        ),
    )


def _evaluate_against_v2_plus(
    strict_frame: pl.DataFrame,
    *,
    selected_role: str,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio_vs_v2_plus: float,
    min_mean_regret_improvement_ratio_vs_strict: float,
    success_description: str,
) -> PromotionGateResult:
    summaries = _role_summaries(strict_frame)
    selected = summaries.get(selected_role)
    v2_plus = summaries.get(V2_PLUS_REFERENCE_ROLE)
    strict = summaries.get(STRICT_REFERENCE_ROLE)
    failures: list[str] = []
    if selected is None:
        failures.append(f"missing {selected_role} rows")
    if v2_plus is None:
        failures.append("missing V2+ reference rows")
    if strict is None:
        failures.append("missing strict reference rows")
    validation_count = _tenant_anchor_count(
        strict_frame.filter(pl.col("selection_role") == selected_role)
    )
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            "validation tenant-anchor count below required "
            f"{min_validation_tenant_anchor_count}"
        )
    if failures or selected is None or v2_plus is None or strict is None:
        return PromotionGateResult(
            False,
            "blocked",
            "; ".join(failures),
            {
                "role_summaries": summaries,
                "validation_tenant_anchor_count": validation_count,
                "market_execution_enabled": False,
            },
        )
    improvement_vs_v2 = _improvement_ratio(
        float(v2_plus["mean_regret_uah"]),
        float(selected["mean_regret_uah"]),
    )
    improvement_vs_strict = _improvement_ratio(
        float(strict["mean_regret_uah"]),
        float(selected["mean_regret_uah"]),
    )
    median_degraded = float(selected["median_regret_uah"]) > float(
        v2_plus["median_regret_uah"]
    )
    if improvement_vs_v2 < min_mean_regret_improvement_ratio_vs_v2_plus:
        failures.append("mean_not_improved_vs_v2_plus")
    if improvement_vs_strict < min_mean_regret_improvement_ratio_vs_strict:
        failures.append(f"mean_not_improved_vs_{CONTROL_MODEL_NAME}")
    if median_degraded:
        failures.append("median_degraded_vs_v2_plus")
    metrics = {
        "selected_mean_regret_uah": selected["mean_regret_uah"],
        "v2_plus_mean_regret_uah": v2_plus["mean_regret_uah"],
        "strict_mean_regret_uah": strict["mean_regret_uah"],
        "selected_median_regret_uah": selected["median_regret_uah"],
        "v2_plus_median_regret_uah": v2_plus["median_regret_uah"],
        "mean_regret_improvement_ratio_vs_v2_plus": improvement_vs_v2,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "validation_tenant_anchor_count": validation_count,
        "role_summaries": summaries,
        "market_execution_enabled": False,
        "offline_strategy_challenger_passed": not failures,
        "production_promote": False,
    }
    if failures:
        return PromotionGateResult(False, "blocked", "; ".join(failures), metrics)
    return PromotionGateResult(
        True,
        "offline_strategy_challenger",
        success_description,
        metrics,
    )


def _family_diagnostic_stats(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not bool(row.get("eligible_for_final_selection", True)):
            continue
        family = str(row["candidate_family"])
        item = stats.setdefault(
            family,
            {
                "safe_win_count": 0,
                "tail_loss_count": 0,
                "deltas": [],
            },
        )
        diagnostic_class = str(row["tail_risk_diagnostic_class"])
        if diagnostic_class == "safe_neighbor_candidate":
            item["safe_win_count"] += 1
        if diagnostic_class == "tail_risk_perturbation_loss":
            item["tail_loss_count"] += 1
        item["deltas"].append(float(row["label_regret_delta_vs_v2_plus_uah"]))
    return {
        family: {
            "safe_win_count": int(values["safe_win_count"]),
            "tail_loss_count": int(values["tail_loss_count"]),
            "mean_prior_delta_uah": mean(values["deltas"]) if values["deltas"] else 0.0,
        }
        for family, values in stats.items()
    }


def _select_prior_safe_final_rows(
    final_rows: list[dict[str, Any]],
    allowed_families: list[str],
    family_priority: dict[str, float],
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    allowed = set(allowed_families)
    for _anchor, anchor_rows in sorted(_rows_by_anchor(final_rows).items()):
        candidates = [row for row in anchor_rows if str(row["candidate_family"]) in allowed]
        if not candidates:
            fallback = _source_rows(anchor_rows, "v2_plus_default")
            selected.extend(fallback[:1])
            continue
        selected.append(
            min(
                candidates,
                key=lambda row: (
                    family_priority.get(str(row["candidate_family"]), 0.0),
                    float(row["selector_feature_schedule_distance_from_v2_plus"]),
                    float(row["selector_feature_total_throughput_delta_mwh"]),
                    str(row["candidate_family"]),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected


def _safe_switch_profile_stats(
    train_rows: list[dict[str, Any]],
    *,
    diagnostic_by_signature: dict[str, dict[str, Any]],
    hard_blocked_candidate_families: set[str],
    allowed_candidate_sources: set[str],
) -> dict[str, dict[str, Any]]:
    raw_stats: dict[str, dict[str, Any]] = {}
    family_tail_loss_counts: dict[str, int] = {}
    for row in train_rows:
        if str(row["candidate_source"]) == "oracle_neighbor_train_diagnostic":
            continue
        if str(row["candidate_source"]) not in allowed_candidate_sources:
            continue
        profile = _safe_switch_profile_key(row)
        family = str(row["candidate_family"])
        diagnostic = diagnostic_by_signature.get(_candidate_signature(row), {})
        diagnostic_class = str(
            diagnostic.get(
                "tail_risk_diagnostic_class",
                _diagnostic_class_from_delta(
                    float(row["label_regret_delta_vs_v2_plus_uah"])
                ),
            )
        )
        item = raw_stats.setdefault(
            profile,
            {
                "candidate_family": family,
                "candidate_source": str(row["candidate_source"]),
                "safe_win_count": 0,
                "tail_loss_count": 0,
                "weak_count": 0,
                "deltas": [],
                "hard_blocked": family in hard_blocked_candidate_families,
            },
        )
        if diagnostic_class == "safe_neighbor_candidate":
            item["safe_win_count"] += 1
        elif diagnostic_class == "tail_risk_perturbation_loss":
            item["tail_loss_count"] += 1
            family_tail_loss_counts[family] = family_tail_loss_counts.get(family, 0) + 1
        else:
            item["weak_count"] += 1
        item["deltas"].append(float(row["label_regret_delta_vs_v2_plus_uah"]))
    stats: dict[str, dict[str, Any]] = {}
    for profile, values in raw_stats.items():
        denominator = (
            int(values["safe_win_count"])
            + int(values["tail_loss_count"])
            + int(values["weak_count"])
        )
        stats[profile] = {
            "candidate_family": values["candidate_family"],
            "candidate_source": values["candidate_source"],
            "safe_win_count": int(values["safe_win_count"]),
            "tail_loss_count": int(values["tail_loss_count"]),
            "weak_count": int(values["weak_count"]),
            "family_tail_loss_count": family_tail_loss_counts.get(
                str(values["candidate_family"]),
                0,
            ),
            "safe_precision": (
                int(values["safe_win_count"]) / denominator if denominator else 0.0
            ),
            "mean_prior_delta_uah": (
                mean(values["deltas"]) if values["deltas"] else 0.0
            ),
            "hard_blocked": bool(values["hard_blocked"]),
        }
    return stats


def _diagnostic_class_from_delta(delta: float) -> str:
    if delta < 0.0:
        return "safe_neighbor_candidate"
    if delta >= 150.0:
        return "tail_risk_perturbation_loss"
    return "neutral_or_weak_neighbor"


def _safe_switch_profile_key(row: dict[str, Any]) -> str:
    return "|".join(
        (
            f"family={row['candidate_family']}",
            f"source={row['candidate_source']}",
            "distance="
            + _magnitude_bucket(
                float(row["selector_feature_schedule_distance_from_v2_plus"])
            ),
            "throughput_delta="
            + _signed_bucket(float(row["selector_feature_total_throughput_delta_mwh"])),
            "terminal_soc_delta="
            + _signed_bucket(
                float(row["selector_feature_terminal_soc_delta_fraction"])
            ),
            "spread="
            + _magnitude_bucket(float(row["selector_feature_forecast_spread_uah_mwh"])),
            f"poland={int(float(row['selector_feature_poland_shadow_candidate']) > 0.0)}",
        )
    )


def _magnitude_bucket(value: float) -> str:
    absolute = abs(value)
    if absolute <= 1e-9:
        return "zero"
    if absolute <= 0.5:
        return "small"
    if absolute <= 1.0:
        return "medium"
    return "large"


def _signed_bucket(value: float) -> str:
    if abs(value) <= 1e-9:
        return "zero"
    sign = "positive" if value > 0.0 else "negative"
    return f"{sign}_{_magnitude_bucket(value)}"


def _strict_reference_row(
    row: dict[str, Any],
    *,
    selection_role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    copied = dict(row)
    copied.update(
        {
            "evaluation_id": (
                f"{row['tenant_id']}:lava_tail_risk:{selection_role}:"
                f"{_datetime_value(row['anchor_timestamp']):%Y%m%dT%H%M}"
            ),
            "strategy_kind": DFL_LAVA_TAIL_RISK_AWARE_STRICT_LP_STRATEGY_KIND,
            "selection_role": selection_role,
            "selected_strategy_source": selection_role,
            "generated_at": generated_at,
            "claim_scope": DFL_LAVA_TAIL_RISK_AWARE_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return copied


def _safe_switch_reference_row(
    row: dict[str, Any],
    *,
    selection_role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    copied = dict(row)
    copied.update(
        {
            "evaluation_id": (
                f"{row['tenant_id']}:lava_safe_switch:{selection_role}:"
                f"{_datetime_value(row['anchor_timestamp']):%Y%m%dT%H%M}"
            ),
            "strategy_kind": DFL_LAVA_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
            "selection_role": selection_role,
            "selected_strategy_source": selection_role,
            "generated_at": generated_at,
            "claim_scope": DFL_LAVA_SAFE_SWITCH_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return copied


def _safe_switch_fallback_row(
    row: dict[str, Any],
    *,
    scorer_row: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    copied = dict(row)
    payload = _payload(row)
    payload.update(
        {
            "selector_role": DFL_LAVA_SAFE_SWITCH_SELECTION_ROLE,
            "fallback_to_v2_plus": True,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "allowed_risk_profiles": list(scorer_row["allowed_risk_profiles"]),
            "market_execution_enabled": False,
        }
    )
    copied.update(
        {
            "evaluation_id": (
                f"{row['tenant_id']}:lava_safe_switch:"
                f"{DFL_LAVA_SAFE_SWITCH_SELECTION_ROLE}:"
                f"{_datetime_value(row['anchor_timestamp']):%Y%m%dT%H%M}"
            ),
            "source_model_name": "lava_tail_risk_safe_switch_bridge_v1",
            "forecast_model_name": DFL_LAVA_SAFE_SWITCH_MODEL_NAME,
            "strategy_kind": DFL_LAVA_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
            "selection_role": DFL_LAVA_SAFE_SWITCH_SELECTION_ROLE,
            "selected_strategy_source": "frozen_v2_plus_fallback",
            "selected_candidate_family": "frozen_v2_plus_fallback",
            "selected_candidate_model_name": str(row["forecast_model_name"]),
            "candidate_family": "frozen_v2_plus_fallback",
            "candidate_model_name": str(row["forecast_model_name"]),
            "generated_at": generated_at,
            "fallback_to_v2_plus": True,
            "evaluation_payload": payload,
            "claim_scope": DFL_LAVA_SAFE_SWITCH_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return copied


def _safe_switch_candidate_row(
    row: dict[str, Any],
    *,
    scorer_row: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    payload = _payload(row)
    payload.update(
        {
            "selector_role": DFL_LAVA_SAFE_SWITCH_SELECTION_ROLE,
            "fallback_to_v2_plus": False,
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "selected_risk_profile": _safe_switch_profile_key(row),
            "allowed_risk_profiles": list(scorer_row["allowed_risk_profiles"]),
            "market_execution_enabled": False,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:lava_safe_switch:"
            f"{DFL_LAVA_SAFE_SWITCH_SELECTION_ROLE}:"
            f"{_datetime_value(row['anchor_timestamp']):%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": "lava_tail_risk_safe_switch_bridge_v1",
        "forecast_model_name": DFL_LAVA_SAFE_SWITCH_MODEL_NAME,
        "strategy_kind": DFL_LAVA_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "selection_role": DFL_LAVA_SAFE_SWITCH_SELECTION_ROLE,
        "selected_strategy_source": str(row["candidate_source"]),
        "selected_candidate_family": str(row["candidate_family"]),
        "selected_candidate_model_name": str(row["candidate_model_name"]),
        "candidate_family": str(row["candidate_family"]),
        "candidate_model_name": str(row["candidate_model_name"]),
        "anchor_timestamp": _datetime_value(row["anchor_timestamp"]),
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": _first_or_default(row["soc_fraction_vector"], default=0.5),
        "starting_soc_source": "lava_tail_risk_safe_switch_bridge",
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": _committed_action(row),
        "committed_power_mw": abs(_first_or_default(row["dispatch_mw_vector"], default=0.0)),
        "rank_by_regret": 1,
        "data_quality_tier": str(row.get("data_quality_tier", "thesis_grade")),
        "observed_coverage_ratio": float(row.get("observed_coverage_ratio", 1.0)),
        "safety_violation_count": int(row.get("safety_violation_count", 0)),
        "fallback_to_v2_plus": False,
        "evaluation_payload": payload,
        "claim_scope": DFL_LAVA_SAFE_SWITCH_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _strict_fallback_reference_row(
    row: dict[str, Any],
    *,
    target_row: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    copied = dict(row)
    payload = _payload(row)
    payload.update(
        {
            "selector_role": DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE,
            "fallback_to_v2_plus": True,
            "blocked_candidate_families": list(target_row["blocked_candidate_families"]),
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
        }
    )
    copied.update(
        {
            "evaluation_id": (
                f"{row['tenant_id']}:lava_tail_risk:{DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE}:"
                f"{_datetime_value(row['anchor_timestamp']):%Y%m%dT%H%M}"
            ),
            "source_model_name": "lava_tail_risk_aware_bridge_v1",
            "forecast_model_name": DFL_LAVA_TAIL_RISK_AWARE_MODEL_NAME,
            "strategy_kind": DFL_LAVA_TAIL_RISK_AWARE_STRICT_LP_STRATEGY_KIND,
            "selection_role": DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE,
            "selected_strategy_source": "frozen_v2_plus_fallback",
            "selected_candidate_family": "frozen_v2_plus_fallback",
            "selected_candidate_model_name": str(row["forecast_model_name"]),
            "candidate_family": "frozen_v2_plus_fallback",
            "candidate_model_name": str(row["forecast_model_name"]),
            "generated_at": generated_at,
            "fallback_to_v2_plus": True,
            "evaluation_payload": payload,
            "claim_scope": DFL_LAVA_TAIL_RISK_AWARE_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return copied


def _strict_candidate_row(
    row: dict[str, Any],
    *,
    target_row: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    payload = _payload(row)
    payload.update(
        {
            "selector_role": DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE,
            "fallback_to_v2_plus": bool(target_row["fallback_to_v2_plus"]),
            "blocked_candidate_families": list(target_row["blocked_candidate_families"]),
            "target_label_space": "schedule_candidate_index",
            "raw_hourly_action_imitation": False,
            "market_execution_enabled": False,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:lava_tail_risk:{DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE}:"
            f"{_datetime_value(row['anchor_timestamp']):%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": "lava_tail_risk_aware_bridge_v1",
        "forecast_model_name": DFL_LAVA_TAIL_RISK_AWARE_MODEL_NAME,
        "strategy_kind": DFL_LAVA_TAIL_RISK_AWARE_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "selection_role": DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE,
        "selected_strategy_source": str(row["candidate_source"]),
        "selected_candidate_family": str(row["candidate_family"]),
        "selected_candidate_model_name": str(row["candidate_model_name"]),
        "candidate_family": str(row["candidate_family"]),
        "candidate_model_name": str(row["candidate_model_name"]),
        "anchor_timestamp": _datetime_value(row["anchor_timestamp"]),
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": _first_or_default(row["soc_fraction_vector"], default=0.5),
        "starting_soc_source": "lava_tail_risk_aware_bridge",
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": _committed_action(row),
        "committed_power_mw": abs(_first_or_default(row["dispatch_mw_vector"], default=0.0)),
        "rank_by_regret": 1,
        "data_quality_tier": str(row.get("data_quality_tier", "thesis_grade")),
        "observed_coverage_ratio": float(row.get("observed_coverage_ratio", 1.0)),
        "safety_violation_count": int(row.get("safety_violation_count", 0)),
        "fallback_to_v2_plus": bool(target_row["fallback_to_v2_plus"]),
        "evaluation_payload": payload,
        "claim_scope": DFL_LAVA_TAIL_RISK_AWARE_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _role_summaries(frame: pl.DataFrame) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for role in sorted(str(value) for value in frame["selection_role"].unique()):
        role_frame = frame.filter(pl.col("selection_role") == role)
        values = [float(value) for value in role_frame["regret_uah"].to_list()]
        summaries[role] = {
            "selection_role": role,
            "row_count": role_frame.height,
            "tenant_anchor_count": _tenant_anchor_count(role_frame),
            "mean_regret_uah": mean(values),
            "median_regret_uah": median(values),
        }
    return summaries


def _require_columns(frame: pl.DataFrame, columns: frozenset[str], *, frame_name: str) -> None:
    missing = sorted(columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing columns: {missing}")
    if "market_execution_enabled" in frame.columns and frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError(f"{frame_name} refuses market execution claims.")


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload", {})
    return dict(payload) if isinstance(payload, dict) else {}


def _selected_signature(row: dict[str, Any]) -> str | None:
    family = row.get("candidate_family") or row.get("selected_candidate_family")
    model = row.get("candidate_model_name") or row.get("selected_candidate_model_name")
    if family is None or model is None:
        return None
    anchor = _datetime_value(row["anchor_timestamp"]).isoformat()
    return f"{anchor}|{row['tenant_id']}|{family}|{model}"


def _candidate_signature(row: dict[str, Any]) -> str:
    anchor = _datetime_value(row["anchor_timestamp"]).isoformat()
    return f"{anchor}|{row['tenant_id']}|{row['candidate_family']}|{row['candidate_model_name']}"


def _candidate_key(row: dict[str, Any]) -> str:
    anchor = _datetime_value(row["anchor_timestamp"]).isoformat()
    return (
        f"{anchor}|{row['tenant_id']}|{row['source_model_name']}|"
        f"{row['candidate_family']}|{row['candidate_model_name']}"
    )


def _anchor_key(row: dict[str, Any]) -> str:
    return _anchor_key_from_parts(str(row["tenant_id"]), _datetime_value(row["anchor_timestamp"]))


def _anchor_key_from_parts(tenant_id: str, anchor_timestamp: datetime) -> str:
    return f"{tenant_id}|{anchor_timestamp.isoformat()}"


def _tenant_anchor_key(row: dict[str, Any]) -> tuple[str, datetime]:
    return (str(row["tenant_id"]), _datetime_value(row["anchor_timestamp"]))


def _datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)


def _rows_by_anchor(rows: list[dict[str, Any]]) -> dict[tuple[str, datetime], list[dict[str, Any]]]:
    result: dict[tuple[str, datetime], list[dict[str, Any]]] = {}
    for row in rows:
        result.setdefault(_tenant_anchor_key(row), []).append(row)
    return result


def _source_rows(rows: list[dict[str, Any]], source: str) -> list[dict[str, Any]]:
    return [row for row in rows if str(row["candidate_source"]) == source]


def _family_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        family = str(row["candidate_family"])
        counts[family] = counts.get(family, 0) + 1
    return counts


def _source_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        source = str(row["candidate_source"])
        counts[source] = counts.get(source, 0) + 1
    return counts


def _anchor_count(rows: list[dict[str, Any]]) -> int:
    return len({_tenant_anchor_key(row)[1] for row in rows})


def _tenant_anchor_count(frame: pl.DataFrame) -> int:
    if frame.is_empty():
        return 0
    return int(frame.select(["tenant_id", "anchor_timestamp"]).unique().height)


def _improvement_ratio(baseline: float, challenger: float) -> float:
    if baseline <= 0.0:
        return 0.0
    return (baseline - challenger) / baseline


def _latest_generated_at(frame: pl.DataFrame) -> datetime:
    if "generated_at" not in frame.columns or frame.is_empty():
        return datetime.now(UTC).replace(tzinfo=None)
    values = [_datetime_value(value) for value in frame["generated_at"].to_list()]
    return max(values)


def _first_or_default(value: Any, *, default: float) -> float:
    values = _float_list(value)
    return values[0] if values else default


def _float_list(value: Any) -> list[float]:
    if value is None:
        return []
    if isinstance(value, str):
        return [float(item.strip()) for item in value.split(",") if item.strip()]
    return [float(item) for item in value]


def _committed_action(row: dict[str, Any]) -> str:
    power = _first_or_default(row["dispatch_mw_vector"], default=0.0)
    if power > 1e-9:
        return "DISCHARGE"
    if power < -1e-9:
        return "CHARGE"
    return "HOLD"


__all__ = [
    "DFL_LAVA_SAFE_SWITCH_SELECTION_ROLE",
    "DFL_LAVA_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND",
    "DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE",
    "DFL_LAVA_TAIL_RISK_AWARE_STRICT_LP_STRATEGY_KIND",
    "build_dfl_lava_tail_risk_safe_switch_scorer_frame",
    "build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame",
    "build_dfl_lava_tail_risk_aware_strict_lp_benchmark_frame",
    "build_dfl_lava_tail_risk_aware_target_frame",
    "build_dfl_lava_tail_risk_diagnostic_frame",
    "evaluate_dfl_lava_tail_risk_safe_switch_gate",
    "evaluate_dfl_lava_tail_risk_aware_gate",
]
