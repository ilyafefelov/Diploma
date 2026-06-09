"""V2+-anchored LAVA-style schedule-neighbor bridge.

This module prepares teacher labels and feasible schedule-neighbor candidates
for a later DT/LAVA branch. It deliberately remains a conservative tabular
scorer: V2+ is the fallback/comparator and final promotion still depends on the
unchanged strict LP/oracle regret evidence.
"""

from __future__ import annotations

from datetime import UTC, datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl import candidate_value_dfl_v3 as v3
from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.dfl.tft_quantile_schedule_value import (
    FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
)

DFL_V2_PLUS_SCHEDULE_NEIGHBOR_TEACHER_LABEL_CLAIM_SCOPE: Final[str] = (
    "dfl_v2_plus_schedule_neighbor_teacher_labels_not_full_dfl"
)
DFL_LAVA_SCHEDULE_NEIGHBOR_CANDIDATE_CLAIM_SCOPE: Final[str] = (
    "dfl_lava_schedule_neighbor_candidates_not_full_dfl"
)
DFL_LAVA_CANDIDATE_VALUE_SCORER_CLAIM_SCOPE: Final[str] = (
    "dfl_lava_candidate_value_scorer_not_full_dfl"
)
DFL_LAVA_CANDIDATE_VALUE_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_lava_candidate_value_strict_lp_gate_not_full_dfl"
)
DFL_LAVA_CANDIDATE_VALUE_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_lava_candidate_value_strict_lp_benchmark"
)
DFL_LAVA_CANDIDATE_VALUE_MODEL_NAME: Final[str] = "dfl_lava_candidate_value_scorer_v1"
DFL_LAVA_SELECTION_ROLE: Final[str] = "lava_candidate_value_scorer"
BEHAVIOR_CLONING_SELECTION_ROLE: Final[str] = "behavior_cloning_reference"
V2_PLUS_REFERENCE_ROLE: Final[str] = "schedule_value_learner_v2_plus_reference"
STRICT_REFERENCE_ROLE: Final[str] = "strict_reference"

LAVA_SELECTOR_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "selector_feature_schedule_distance_from_v2_plus",
    "selector_feature_total_throughput_delta_mwh",
    "selector_feature_terminal_soc_delta_fraction",
    "selector_feature_forecast_spread_uah_mwh",
    "selector_feature_total_degradation_penalty_uah",
    "selector_feature_poland_shadow_candidate",
    "selector_feature_oracle_train_diagnostic",
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
_REQUIRED_LIBRARY_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "candidate_family",
        "candidate_model_name",
        "anchor_timestamp",
        "generated_at",
        "split_name",
        "horizon_hours",
        "forecast_price_uah_mwh_vector",
        "actual_price_uah_mwh_vector",
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
    }
)
_REQUIRED_CANDIDATE_COLUMNS: Final[frozenset[str]] = _REQUIRED_LIBRARY_COLUMNS | frozenset(
    {
        "candidate_source",
        "eligible_for_final_selection",
        "label_regret_delta_vs_v2_plus_uah",
        *LAVA_SELECTOR_FEATURE_COLUMNS,
    }
)


def build_dfl_v2_plus_schedule_neighbor_teacher_label_frame(
    frozen_v2_plus_strict_frame: pl.DataFrame,
    poland_v2_plus_strict_frame: pl.DataFrame,
    poland_prior_veto_frame: pl.DataFrame,
    poland_candidate_ranker_strict_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    poland_source_model_names: tuple[str, ...],
    tail_risk_delta_uah: float = 150.0,
) -> pl.DataFrame:
    """Classify current V2+/Poland evidence into teacher-label rows."""

    _require_columns(
        frozen_v2_plus_strict_frame,
        _REQUIRED_STRICT_COLUMNS,
        frame_name="frozen_v2_plus_strict_frame",
    )
    _require_columns(
        poland_v2_plus_strict_frame,
        _REQUIRED_STRICT_COLUMNS,
        frame_name="poland_v2_plus_strict_frame",
    )
    _require_columns(
        poland_candidate_ranker_strict_frame,
        _REQUIRED_STRICT_COLUMNS,
        frame_name="poland_candidate_ranker_strict_frame",
    )
    if not poland_source_model_names:
        raise ValueError("poland_source_model_names must not be empty.")
    baseline_rows = [
        row
        for row in frozen_v2_plus_strict_frame.iter_rows(named=True)
        if str(row["source_model_name"]) == baseline_source_model_name
        and str(row["selection_role"]) == "schedule_value_learner_v2_plus"
    ]
    baseline_by_key = {_tenant_anchor_key(row): row for row in baseline_rows}
    output_rows: list[dict[str, Any]] = []
    for baseline_row in baseline_rows:
        output_rows.append(
            _teacher_row(
                baseline_row,
                baseline_row=baseline_row,
                source="frozen_v2_plus",
                teacher_class="v2_plus_best",
                candidate_family="frozen_v2_plus",
                tail_risk_delta_uah=tail_risk_delta_uah,
            )
        )
    for frame, source, role_filter in (
        (poland_v2_plus_strict_frame, "poland_v2_plus", "schedule_value_learner_v2_plus"),
        (
            poland_candidate_ranker_strict_frame,
            "poland_candidate_ranker",
            "poland_lag24_candidate_value_ranker_v1",
        ),
    ):
        for row in frame.iter_rows(named=True):
            if str(row["source_model_name"]) not in poland_source_model_names:
                continue
            if str(row["selection_role"]) != role_filter:
                continue
            baseline_match = baseline_by_key.get(_tenant_anchor_key(row))
            if baseline_match is None:
                continue
            delta = float(row["regret_uah"]) - float(baseline_match["regret_uah"])
            if source == "poland_candidate_ranker" and delta > 0.0:
                teacher_class = "selector_overreach"
            elif delta < 0.0:
                teacher_class = "poland_safe_win"
            elif delta >= tail_risk_delta_uah:
                teacher_class = "poland_tail_risk_loss"
            else:
                teacher_class = "v2_plus_best"
            output_rows.append(
                _teacher_row(
                    row,
                    baseline_row=baseline_match,
                    source=source,
                    teacher_class=teacher_class,
                    candidate_family=_candidate_family(row, fallback=source),
                    tail_risk_delta_uah=tail_risk_delta_uah,
                )
            )
    if not poland_prior_veto_frame.is_empty():
        _require_columns(
            poland_prior_veto_frame,
            frozenset(
                {
                    "tenant_id",
                    "anchor_timestamp",
                    "selected_strategy_name",
                    "selected_regret_uah",
                    "baseline_regret_uah",
                    "market_execution_enabled",
                }
            ),
            frame_name="poland_prior_veto_frame",
        )
        for row in poland_prior_veto_frame.iter_rows(named=True):
            key = _tenant_anchor_key(row)
            baseline_match = baseline_by_key.get(key)
            if baseline_match is None:
                continue
            surrogate = dict(baseline_match)
            surrogate.update(
                {
                    "source_model_name": "poland_lag24_prior_tail_risk_veto",
                    "forecast_model_name": str(row["selected_strategy_name"]),
                    "selection_role": "poland_prior_tail_risk_veto_reference",
                    "regret_uah": float(row["selected_regret_uah"]),
                }
            )
            delta = float(row["selected_regret_uah"]) - float(row["baseline_regret_uah"])
            teacher_class = "poland_safe_win" if delta < 0.0 else "v2_plus_best"
            output_rows.append(
                _teacher_row(
                    surrogate,
                    baseline_row=baseline_row,
                    source="poland_prior_veto",
                    teacher_class=teacher_class,
                    candidate_family="poland_prior_tail_risk_veto",
                    tail_risk_delta_uah=tail_risk_delta_uah,
                )
            )
    return pl.DataFrame(output_rows).sort(
        ["tenant_id", "anchor_timestamp", "teacher_source", "candidate_family"]
    )


def build_dfl_lava_schedule_neighbor_candidate_frame(
    v2_plus_candidate_library_frame: pl.DataFrame,
    poland_candidate_library_frame: pl.DataFrame,
    frozen_v2_plus_strict_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
    poland_source_model_names: tuple[str, ...],
    include_oracle_train_diagnostics: bool = True,
) -> pl.DataFrame:
    """Build feasible LAVA schedule-neighbor candidates with V2+ fallback rows."""

    _require_columns(
        v2_plus_candidate_library_frame,
        _REQUIRED_LIBRARY_COLUMNS,
        frame_name="v2_plus_candidate_library_frame",
    )
    _require_columns(
        poland_candidate_library_frame,
        _REQUIRED_LIBRARY_COLUMNS,
        frame_name="poland_candidate_library_frame",
    )
    _require_columns(
        frozen_v2_plus_strict_frame,
        _REQUIRED_STRICT_COLUMNS,
        frame_name="frozen_v2_plus_strict_frame",
    )
    baseline_candidates = _baseline_candidate_by_anchor(
        v2_plus_candidate_library_frame,
        baseline_source_model_name=baseline_source_model_name,
    )
    strict_rows = [
        row
        for row in v2_plus_candidate_library_frame.iter_rows(named=True)
        if str(row["source_model_name"]) == baseline_source_model_name
        and str(row["candidate_family"]) == v2.CANDIDATE_FAMILY_STRICT
    ]
    rows: list[dict[str, Any]] = []
    for row in strict_rows:
        baseline = baseline_candidates.get(_tenant_anchor_key(row))
        rows.append(
            _candidate_output_row(
                row,
                baseline_row=baseline,
                candidate_source="strict_fallback",
                candidate_family=str(row["candidate_family"]),
                eligible_for_final_selection=True,
                analysis_only=False,
            )
        )
    for baseline in baseline_candidates.values():
        rows.append(
            _candidate_output_row(
                baseline,
                baseline_row=baseline,
                candidate_source="v2_plus_default",
                candidate_family="frozen_v2_plus_fallback",
                eligible_for_final_selection=True,
                analysis_only=False,
            )
        )
    for row in poland_candidate_library_frame.iter_rows(named=True):
        if str(row["source_model_name"]) not in poland_source_model_names:
            continue
        baseline = baseline_candidates.get(_tenant_anchor_key(row))
        if baseline is None:
            continue
        rows.append(
            _candidate_output_row(
                row,
                baseline_row=baseline,
                candidate_source="poland_shadow_candidate",
                candidate_family=str(row["candidate_family"]),
                eligible_for_final_selection=True,
                analysis_only=False,
            )
        )
    if include_oracle_train_diagnostics:
        rows.extend(_oracle_train_diagnostic_rows(rows))
    return pl.DataFrame(rows).sort(
        [
            "tenant_id",
            "anchor_timestamp",
            "split_name",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )


def build_dfl_lava_candidate_value_scorer_frame(
    lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    min_prior_mean_improvement_ratio_vs_v2_plus: float = 0.05,
    ridge_l2: float = 10.0,
) -> pl.DataFrame:
    """Train a conservative candidate-level delta scorer on train rows only."""

    _validate_candidate_frame(lava_schedule_neighbor_candidate_frame)
    if not tenant_ids:
        raise ValueError("tenant_ids must not be empty.")
    if min_prior_mean_improvement_ratio_vs_v2_plus < 0.0:
        raise ValueError("min_prior_mean_improvement_ratio_vs_v2_plus must not be negative.")
    rows = list(lava_schedule_neighbor_candidate_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        tenant_rows = [row for row in rows if str(row["tenant_id"]) == tenant_id]
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
            raise ValueError(f"{tenant_id} LAVA scorer needs train rows.")
        if not final_rows:
            raise ValueError(f"{tenant_id} LAVA scorer needs final rows.")
        scorer = _fit_delta_scorer(train_rows, ridge_l2=ridge_l2)
        selected_train = _select_rows(train_rows, scorer=scorer)
        selected_final = _select_rows(final_rows, scorer=scorer)
        baseline_train = _source_rows(train_rows, "v2_plus_default")
        baseline_mean = _mean_regret(baseline_train)
        selected_train_mean = _mean_regret(selected_train)
        improvement = _improvement_ratio(baseline_mean, selected_train_mean)
        fallback = improvement < min_prior_mean_improvement_ratio_vs_v2_plus
        output_rows.append(
            {
                "tenant_id": tenant_id,
                "learner_model_name": DFL_LAVA_CANDIDATE_VALUE_MODEL_NAME,
                "selected_scorer_type": "lava_schedule_neighbor_ridge_delta_scorer",
                "selected_feature_names": list(LAVA_SELECTOR_FEATURE_COLUMNS),
                "selected_feature_weights": dict(scorer["weights"]),
                "selected_feature_means": dict(scorer["feature_means"]),
                "selected_feature_scales": dict(scorer["feature_scales"]),
                "fallback_to_v2_plus": fallback,
                "selector_gate_blocker": (
                    "candidate_prior_improvement_selected"
                    if not fallback
                    else "weak_prior_improvement_vs_v2_plus"
                ),
                "train_anchor_count": _anchor_count(train_rows),
                "final_holdout_anchor_count": _anchor_count(final_rows),
                "v2_plus_train_mean_regret_uah": baseline_mean,
                "selected_train_mean_regret_uah": (
                    baseline_mean if fallback else selected_train_mean
                ),
                "candidate_train_mean_regret_uah": selected_train_mean,
                "candidate_final_mean_regret_uah": _mean_regret(selected_final),
                "prior_mean_improvement_ratio_vs_v2_plus": improvement,
                "selected_train_family_counts": _family_counts(
                    baseline_train if fallback else selected_train
                ),
                "selected_final_family_counts": _family_counts(
                    _source_rows(final_rows, "v2_plus_default") if fallback else selected_final
                ),
                "selected_train_candidate_source_counts": _candidate_source_counts(
                    baseline_train if fallback else selected_train
                ),
                "selected_final_candidate_source_counts": _candidate_source_counts(
                    _source_rows(final_rows, "v2_plus_default")
                    if fallback
                    else selected_final
                ),
                "selected_train_candidate_keys": [
                    _candidate_key(row)
                    for row in (baseline_train if fallback else selected_train)
                ],
                "selected_final_candidate_keys": [
                    _candidate_key(row)
                    for row in (
                        _source_rows(final_rows, "v2_plus_default")
                        if fallback
                        else selected_final
                    )
                ],
                "claim_scope": DFL_LAVA_CANDIDATE_VALUE_SCORER_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(output_rows).sort(["tenant_id"])


def build_dfl_lava_candidate_value_strict_lp_benchmark_frame(
    lava_schedule_neighbor_candidate_frame: pl.DataFrame,
    lava_candidate_value_scorer_frame: pl.DataFrame,
    frozen_v2_plus_strict_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict, V2+, behavior-cloning, and LAVA scorer comparison rows."""

    _validate_candidate_frame(lava_schedule_neighbor_candidate_frame)
    _validate_scorer_frame(lava_candidate_value_scorer_frame)
    _require_columns(
        frozen_v2_plus_strict_frame,
        _REQUIRED_STRICT_COLUMNS,
        frame_name="frozen_v2_plus_strict_frame",
    )
    resolved_generated_at = generated_at or _latest_generated_at(
        lava_schedule_neighbor_candidate_frame
    )
    candidate_rows = list(lava_schedule_neighbor_candidate_frame.iter_rows(named=True))
    baseline_strict_rows = [
        row
        for row in frozen_v2_plus_strict_frame.iter_rows(named=True)
        if str(row["selection_role"]) in {"strict_reference", "schedule_value_learner_v2_plus"}
    ]
    output_rows: list[dict[str, Any]] = []
    for row in baseline_strict_rows:
        role = (
            STRICT_REFERENCE_ROLE
            if str(row["selection_role"]) == "strict_reference"
            else V2_PLUS_REFERENCE_ROLE
        )
        output_rows.append(
            _strict_reference_row(
                row,
                selection_role=role,
                generated_at=resolved_generated_at,
            )
        )
    for scorer_row in lava_candidate_value_scorer_frame.iter_rows(named=True):
        tenant_id = str(scorer_row["tenant_id"])
        final_rows = [
            row
            for row in candidate_rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["split_name"]) == "final_holdout"
            and bool(row["eligible_for_final_selection"])
        ]
        selected_by_key = {_candidate_key(row): row for row in final_rows}
        for key in scorer_row["selected_final_candidate_keys"]:
            selected = selected_by_key[str(key)]
            output_rows.append(
                _strict_candidate_row(
                    selected,
                    scorer_row=scorer_row,
                    selection_role=DFL_LAVA_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                )
            )
        for selected in _behavior_cloning_rows(candidate_rows, tenant_id=tenant_id):
            output_rows.append(
                _strict_candidate_row(
                    selected,
                    scorer_row=scorer_row,
                    selection_role=BEHAVIOR_CLONING_SELECTION_ROLE,
                    generated_at=resolved_generated_at,
                )
            )
    return pl.DataFrame(output_rows).sort(
        ["tenant_id", "anchor_timestamp", "selection_role", "forecast_model_name"]
    )


def evaluate_dfl_lava_candidate_value_gate(
    strict_frame: pl.DataFrame,
    *,
    min_validation_tenant_anchor_count: int = 90,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.0,
    min_mean_regret_improvement_ratio_vs_strict: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
) -> PromotionGateResult:
    """Require LAVA scorer to beat V2+ before any promotion claim."""

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
        frame_name="lava strict frame",
    )
    if strict_frame.select(pl.col("market_execution_enabled").any()).item():
        return PromotionGateResult(
            False,
            "blocked",
            "LAVA strict frame refuses market execution claims",
            {"market_execution_enabled": False},
        )
    summaries = _role_summaries(strict_frame)
    selected = summaries.get(DFL_LAVA_SELECTION_ROLE)
    v2_plus = summaries.get(V2_PLUS_REFERENCE_ROLE)
    strict = summaries.get(STRICT_REFERENCE_ROLE)
    failures: list[str] = []
    if selected is None:
        failures.append("missing LAVA scorer rows")
    if v2_plus is None:
        failures.append("missing V2+ reference rows")
    if strict is None:
        failures.append("missing strict reference rows")
    validation_count = _tenant_anchor_count(
        strict_frame.filter(pl.col("selection_role") == DFL_LAVA_SELECTION_ROLE)
    )
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            "LAVA validation tenant-anchor count below required "
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
        "LAVA schedule-neighbor scorer beats V2+ under strict LP/oracle evidence",
        metrics,
    )


def _teacher_row(
    row: dict[str, Any],
    *,
    baseline_row: dict[str, Any],
    source: str,
    teacher_class: str,
    candidate_family: str,
    tail_risk_delta_uah: float,
) -> dict[str, Any]:
    baseline_regret = float(baseline_row["regret_uah"])
    candidate_regret = float(row["regret_uah"])
    delta = candidate_regret - baseline_regret
    payload = _payload(row)
    return {
        "tenant_id": str(row["tenant_id"]),
        "anchor_timestamp": _datetime_value(row["anchor_timestamp"]),
        "teacher_source": source,
        "source_model_name": str(row["source_model_name"]),
        "forecast_model_name": str(row["forecast_model_name"]),
        "candidate_family": candidate_family,
        "teacher_class": teacher_class,
        "baseline_v2_plus_regret_uah": baseline_regret,
        "candidate_regret_uah": candidate_regret,
        "label_regret_delta_vs_v2_plus_uah": delta,
        "label_beats_v2_plus": delta < 0.0,
        "label_tail_risk": delta >= tail_risk_delta_uah,
        "selector_feature_schedule_distance_from_v2_plus": _schedule_distance_payload(
            payload,
            _payload(baseline_row),
        ),
        "selector_feature_total_throughput_delta_mwh": float(
            row.get("total_throughput_mwh", 0.0)
        )
        - float(baseline_row.get("total_throughput_mwh", 0.0)),
        "selector_feature_candidate_is_poland": float(source.startswith("poland")),
        "claim_scope": DFL_V2_PLUS_SCHEDULE_NEIGHBOR_TEACHER_LABEL_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _candidate_output_row(
    row: dict[str, Any],
    *,
    baseline_row: dict[str, Any] | None,
    candidate_source: str,
    candidate_family: str,
    eligible_for_final_selection: bool,
    analysis_only: bool,
) -> dict[str, Any]:
    baseline = baseline_row or row
    dispatch = _float_list(row["dispatch_mw_vector"])
    baseline_dispatch = _float_list(baseline["dispatch_mw_vector"])
    soc = _float_list(row["soc_fraction_vector"])
    baseline_soc = _float_list(baseline["soc_fraction_vector"])
    output = dict(row)
    output.update(
        {
            "candidate_family": candidate_family,
            "candidate_source": candidate_source,
            "eligible_for_final_selection": eligible_for_final_selection,
            "analysis_only": analysis_only,
            "label_regret_delta_vs_v2_plus_uah": float(row["regret_uah"])
            - float(baseline["regret_uah"]),
            "label_beats_v2_plus": float(row["regret_uah"]) < float(baseline["regret_uah"]),
            "selector_feature_schedule_distance_from_v2_plus": _schedule_distance(
                dispatch,
                baseline_dispatch,
            ),
            "selector_feature_total_throughput_delta_mwh": float(
                row["total_throughput_mwh"]
            )
            - float(baseline["total_throughput_mwh"]),
            "selector_feature_terminal_soc_delta_fraction": (
                (soc[-1] if soc else 0.5) - (baseline_soc[-1] if baseline_soc else 0.5)
            ),
            "selector_feature_forecast_spread_uah_mwh": _spread(
                _float_list(row["forecast_price_uah_mwh_vector"])
            ),
            "selector_feature_total_degradation_penalty_uah": float(
                row["total_degradation_penalty_uah"]
            ),
            "selector_feature_poland_shadow_candidate": float(
                candidate_source == "poland_shadow_candidate"
            ),
            "selector_feature_oracle_train_diagnostic": float(
                candidate_source == "oracle_neighbor_train_diagnostic"
            ),
            "claim_scope": DFL_LAVA_SCHEDULE_NEIGHBOR_CANDIDATE_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return output


def _fit_delta_scorer(
    train_rows: list[dict[str, Any]],
    *,
    ridge_l2: float,
) -> dict[str, Any]:
    feature_means: dict[str, float] = {}
    feature_scales: dict[str, float] = {}
    for column in LAVA_SELECTOR_FEATURE_COLUMNS:
        values = [float(row[column]) for row in train_rows]
        feature_means[column] = mean(values)
        span = max(values) - min(values)
        feature_scales[column] = span if span > 1e-9 else 1.0
    family_columns = tuple(
        f"family::{family}"
        for family in sorted({str(row["candidate_family"]) for row in train_rows})
    )
    candidate_source_columns = tuple(
        f"source::{source}"
        for source in sorted({str(row["candidate_source"]) for row in train_rows})
    )
    feature_matrix = [
        _feature_vector(
            row,
            feature_means=feature_means,
            feature_scales=feature_scales,
            family_columns=family_columns,
            candidate_source_columns=candidate_source_columns,
        )
        for row in train_rows
    ]
    targets = [float(row["label_regret_delta_vs_v2_plus_uah"]) for row in train_rows]
    coefficients = v3._fit_ridge_coefficients(
        feature_matrix,
        targets,
        ridge_l2=ridge_l2,
    )
    feature_names = [
        *LAVA_SELECTOR_FEATURE_COLUMNS,
        *family_columns,
        *candidate_source_columns,
    ]
    weights = {"intercept": coefficients[0]}
    weights.update(
        {
            feature_name: coefficients[index + 1]
            for index, feature_name in enumerate(feature_names)
        }
    )
    return {
        "weights": weights,
        "feature_means": feature_means,
        "feature_scales": feature_scales,
        "family_columns": family_columns,
        "candidate_source_columns": candidate_source_columns,
    }


def _select_rows(
    rows: list[dict[str, Any]],
    *,
    scorer: dict[str, Any],
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for _anchor, anchor_rows in sorted(_rows_by_anchor(rows).items()):
        eligible = [row for row in anchor_rows if bool(row["eligible_for_final_selection"])]
        if not eligible:
            continue
        selected.append(
            min(
                eligible,
                key=lambda row: (
                    _predict_delta(row, scorer=scorer),
                    str(row["candidate_source"]),
                    str(row["candidate_family"]),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected


def _predict_delta(row: dict[str, Any], *, scorer: dict[str, Any]) -> float:
    feature_names = [
        *LAVA_SELECTOR_FEATURE_COLUMNS,
        *tuple(str(column) for column in scorer["family_columns"]),
        *tuple(str(column) for column in scorer["candidate_source_columns"]),
    ]
    values = _feature_vector(
        row,
        feature_means=dict(scorer["feature_means"]),
        feature_scales=dict(scorer["feature_scales"]),
        family_columns=tuple(str(column) for column in scorer["family_columns"]),
        candidate_source_columns=tuple(
            str(column) for column in scorer["candidate_source_columns"]
        ),
    )
    weights = dict(scorer["weights"])
    score = float(weights.get("intercept", 0.0))
    for feature_name, value in zip(feature_names, values, strict=True):
        score += float(weights.get(feature_name, 0.0)) * value
    return score


def _feature_vector(
    row: dict[str, Any],
    *,
    feature_means: dict[str, float],
    feature_scales: dict[str, float],
    family_columns: tuple[str, ...],
    candidate_source_columns: tuple[str, ...],
) -> list[float]:
    numeric = [
        (float(row[column]) - feature_means[column]) / feature_scales[column]
        for column in LAVA_SELECTOR_FEATURE_COLUMNS
    ]
    family = str(row["candidate_family"])
    source = str(row["candidate_source"])
    family_one_hot = [
        1.0 if column == f"family::{family}" else 0.0 for column in family_columns
    ]
    source_one_hot = [
        1.0 if column == f"source::{source}" else 0.0
        for column in candidate_source_columns
    ]
    return [*numeric, *family_one_hot, *source_one_hot]


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
                f"{row['tenant_id']}:lava:{selection_role}:"
                f"{_datetime_value(row['anchor_timestamp']):%Y%m%dT%H%M}"
            ),
            "strategy_kind": DFL_LAVA_CANDIDATE_VALUE_STRICT_LP_STRATEGY_KIND,
            "selection_role": selection_role,
            "selected_strategy_source": selection_role,
            "generated_at": generated_at,
            "claim_scope": DFL_LAVA_CANDIDATE_VALUE_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return copied


def _strict_candidate_row(
    row: dict[str, Any],
    *,
    scorer_row: dict[str, Any],
    selection_role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(row.get("evaluation_payload", {}))
    payload.update(
        {
            "selector_role": selection_role,
            "fallback_to_v2_plus": bool(scorer_row["fallback_to_v2_plus"]),
            "selected_feature_weights": dict(scorer_row["selected_feature_weights"]),
            "market_execution_enabled": False,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:lava:{selection_role}:"
            f"{_datetime_value(row['anchor_timestamp']):%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": "lava_schedule_neighbor_bridge_v1",
        "forecast_model_name": f"{DFL_LAVA_CANDIDATE_VALUE_MODEL_NAME}_{selection_role}",
        "strategy_kind": DFL_LAVA_CANDIDATE_VALUE_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "selection_role": selection_role,
        "selected_strategy_source": str(row["candidate_source"]),
        "selected_candidate_family": str(row["candidate_family"]),
        "selected_candidate_model_name": str(row["candidate_model_name"]),
        "candidate_family": str(row["candidate_family"]),
        "candidate_model_name": str(row["candidate_model_name"]),
        "anchor_timestamp": _datetime_value(row["anchor_timestamp"]),
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": v2._first_or_default(
            row["soc_fraction_vector"],
            default=0.5,
        ),
        "starting_soc_source": "lava_schedule_neighbor_bridge",
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": v2._committed_action(row),
        "committed_power_mw": abs(
            v2._first_or_default(row["dispatch_mw_vector"], default=0.0)
        ),
        "rank_by_regret": 1,
        "data_quality_tier": str(row.get("data_quality_tier", "thesis_grade")),
        "observed_coverage_ratio": float(row.get("observed_coverage_ratio", 1.0)),
        "safety_violation_count": int(row.get("safety_violation_count", 0)),
        "fallback_to_v2_plus": bool(scorer_row["fallback_to_v2_plus"]),
        "evaluation_payload": payload,
        "claim_scope": DFL_LAVA_CANDIDATE_VALUE_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _baseline_candidate_by_anchor(
    frame: pl.DataFrame,
    *,
    baseline_source_model_name: str,
) -> dict[tuple[str, datetime], dict[str, Any]]:
    rows = [
        row
        for row in frame.iter_rows(named=True)
        if str(row["source_model_name"]) == baseline_source_model_name
        and str(row["candidate_family"]) != v2.CANDIDATE_FAMILY_STRICT
    ]
    by_anchor = _rows_by_anchor(rows)
    return {
        anchor_key: min(
            anchor_rows,
            key=lambda row: (
                _candidate_source_priority(str(row["candidate_family"])),
                str(row["candidate_family"]),
                str(row["candidate_model_name"]),
            ),
        )
        for anchor_key, anchor_rows in by_anchor.items()
    }


def _oracle_train_diagnostic_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    train_rows = [
        row
        for row in rows
        if str(row["split_name"]) != "final_holdout"
        and bool(row["eligible_for_final_selection"])
    ]
    diagnostics: list[dict[str, Any]] = []
    for _anchor, anchor_rows in sorted(_rows_by_anchor(train_rows).items()):
        best = min(anchor_rows, key=lambda row: float(row["regret_uah"]))
        copied = dict(best)
        copied.update(
            {
                "candidate_family": "oracle_neighbor_train_diagnostic",
                "candidate_model_name": (
                    f"oracle_neighbor_train_diagnostic:{best['candidate_model_name']}"
                ),
                "candidate_source": "oracle_neighbor_train_diagnostic",
                "eligible_for_final_selection": False,
                "analysis_only": True,
                "selector_feature_oracle_train_diagnostic": 1.0,
            }
        )
        diagnostics.append(copied)
    return diagnostics


def _behavior_cloning_rows(
    rows: list[dict[str, Any]],
    *,
    tenant_id: str,
) -> list[dict[str, Any]]:
    tenant_rows = [row for row in rows if str(row["tenant_id"]) == tenant_id]
    train_winners = [
        row
        for row in tenant_rows
        if str(row["split_name"]) != "final_holdout"
        and bool(row["eligible_for_final_selection"])
        and float(row["label_regret_delta_vs_v2_plus_uah"]) < 0.0
    ]
    if train_winners:
        counts = _family_counts(train_winners)
        selected_family = min(
            counts,
            key=lambda family: (-counts[family], str(family)),
        )
    else:
        selected_family = "frozen_v2_plus_fallback"
    final_rows = [
        row
        for row in tenant_rows
        if str(row["split_name"]) == "final_holdout"
        and bool(row["eligible_for_final_selection"])
    ]
    output: list[dict[str, Any]] = []
    for _anchor, anchor_rows in sorted(_rows_by_anchor(final_rows).items()):
        matching = [row for row in anchor_rows if str(row["candidate_family"]) == selected_family]
        fallback = _source_rows(anchor_rows, "v2_plus_default")
        output.append((matching or fallback or anchor_rows)[0])
    return output


def _source_rows(rows: list[dict[str, Any]], source: str) -> list[dict[str, Any]]:
    return [row for row in rows if str(row["candidate_source"]) == source]


def _validate_candidate_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, _REQUIRED_CANDIDATE_COLUMNS, frame_name="LAVA candidate frame")
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("LAVA candidate frame refuses market execution claims.")


def _validate_scorer_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        frozenset(
            {
                "tenant_id",
                "selected_feature_weights",
                "selected_final_candidate_keys",
                "fallback_to_v2_plus",
                "claim_scope",
                "not_market_execution",
                "market_execution_enabled",
            }
        ),
        frame_name="LAVA scorer frame",
    )


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


def _candidate_family(row: dict[str, Any], *, fallback: str) -> str:
    value = row.get("candidate_family")
    return fallback if value is None else str(value)


def _tenant_anchor_key(row: dict[str, Any]) -> tuple[str, datetime]:
    return (str(row["tenant_id"]), _datetime_value(row["anchor_timestamp"]))


def _datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)


def _float_list(value: Any) -> list[float]:
    if value is None:
        return []
    if isinstance(value, str):
        return [float(item.strip()) for item in value.split(",") if item.strip()]
    return [float(item) for item in value]


def _schedule_distance(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    width = min(len(left), len(right))
    return sum(abs(left[index] - right[index]) for index in range(width))


def _schedule_distance_payload(left: dict[str, Any], right: dict[str, Any]) -> float:
    return _schedule_distance(_horizon_dispatch(left), _horizon_dispatch(right))


def _horizon_dispatch(payload: dict[str, Any]) -> list[float]:
    horizon = payload.get("horizon", [])
    if not isinstance(horizon, list):
        return []
    return [float(row.get("net_power_mw", 0.0)) for row in horizon if isinstance(row, dict)]


def _spread(values: list[float]) -> float:
    return max(values) - min(values) if values else 0.0


def _rows_by_anchor(rows: list[dict[str, Any]]) -> dict[tuple[str, datetime], list[dict[str, Any]]]:
    result: dict[tuple[str, datetime], list[dict[str, Any]]] = {}
    for row in rows:
        result.setdefault(_tenant_anchor_key(row), []).append(row)
    return result


def _candidate_key(row: dict[str, Any]) -> str:
    anchor = _datetime_value(row["anchor_timestamp"]).isoformat()
    return (
        f"{anchor}|{row['tenant_id']}|{row['source_model_name']}|"
        f"{row['candidate_family']}|{row['candidate_model_name']}"
    )


def _candidate_source_priority(family: str) -> int:
    if family == "rank_extrema_perturbation_v2_plus":
        return 0
    if family == "strict_neighborhood_v2_plus":
        return 1
    if family == "strict_control":
        return 99
    return 10


def _anchor_count(rows: list[dict[str, Any]]) -> int:
    return len({_tenant_anchor_key(row)[1] for row in rows})


def _family_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        family = str(row["candidate_family"])
        counts[family] = counts.get(family, 0) + 1
    return counts


def _candidate_source_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        source = str(row["candidate_source"])
        counts[source] = counts.get(source, 0) + 1
    return counts


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    return mean(float(row["regret_uah"]) for row in rows)


def _improvement_ratio(baseline: float, challenger: float) -> float:
    if baseline <= 0.0:
        return 0.0
    return (baseline - challenger) / baseline


def _tenant_anchor_count(frame: pl.DataFrame) -> int:
    if frame.is_empty():
        return 0
    return int(frame.select(["tenant_id", "anchor_timestamp"]).unique().height)


def _latest_generated_at(frame: pl.DataFrame) -> datetime:
    if "generated_at" not in frame.columns or frame.is_empty():
        return datetime.now(UTC).replace(tzinfo=None)
    values = [_datetime_value(value) for value in frame["generated_at"].to_list()]
    return max(values)


__all__ = [
    "DFL_LAVA_CANDIDATE_VALUE_STRICT_LP_STRATEGY_KIND",
    "build_dfl_lava_candidate_value_scorer_frame",
    "build_dfl_lava_candidate_value_strict_lp_benchmark_frame",
    "build_dfl_lava_schedule_neighbor_candidate_frame",
    "build_dfl_v2_plus_schedule_neighbor_teacher_label_frame",
    "evaluate_dfl_lava_candidate_value_gate",
]
