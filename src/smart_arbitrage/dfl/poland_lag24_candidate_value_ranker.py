"""Candidate-value ranker for Poland lag-24 shadow schedules.

This module is deliberately smaller than DT/LAVA. It trains a deterministic
tabular ridge scorer over feasible schedule candidates and prior-safe Poland
context features, then falls back to frozen Ukrainian-only V2+ unless the
selected Poland candidate is justified by prior/train rows.
"""

from __future__ import annotations

from datetime import datetime
from statistics import mean
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl import candidate_value_dfl_v3 as v3
from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl.tft_quantile_schedule_value import (
    FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
)

POLAND_LAG24_CANDIDATE_VALUE_LABEL_CLAIM_SCOPE: Final[str] = (
    "poland_lag24_candidate_value_label_panel_not_full_dfl"
)
POLAND_LAG24_CANDIDATE_VALUE_RANKER_CLAIM_SCOPE: Final[str] = (
    "poland_lag24_candidate_value_ranker_not_full_dfl"
)
POLAND_LAG24_CANDIDATE_VALUE_RANKER_STRICT_CLAIM_SCOPE: Final[str] = (
    "poland_lag24_candidate_value_ranker_strict_lp_gate_not_full_dfl"
)
POLAND_LAG24_CANDIDATE_VALUE_RANKER_SELECTION_ROLE: Final[str] = (
    "poland_lag24_candidate_value_ranker_v1"
)
POLAND_LAG24_CANDIDATE_VALUE_RANKER_MODEL_NAME: Final[str] = (
    "dfl_poland_lag24_candidate_value_ranker_v1"
)
POLAND_LAG24_CANDIDATE_VALUE_RANKER_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_poland_lag24_candidate_value_ranker_strict_lp_benchmark"
)

POLAND_LAG24_RANKER_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "selector_feature_prior_family_mean_regret_uah",
    "selector_feature_forecast_spread_uah_mwh",
    "selector_feature_forecast_objective_value_uah",
    "selector_feature_total_throughput_mwh",
    "selector_feature_total_degradation_penalty_uah",
    "selector_feature_terminal_soc_fraction",
    "selector_feature_dispatch_reversal_count",
    "selector_feature_poland_lag24_price_uah_mwh",
    "selector_feature_poland_lag24_delta_24h_uah_mwh",
    "selector_feature_poland_lag24_daily_spread_uah_mwh",
    "selector_feature_poland_lag24_daily_price_rank",
    "selector_feature_poland_lag24_ua_spread_uah_mwh",
    "selector_feature_poland_lag24_ua_rank_disagreement",
    "selector_feature_poland_lag24_ua_peak_hour_delta",
    "selector_feature_poland_lag24_ua_trough_hour_delta",
    "selector_feature_poland_lag24_evening_morning_spread_uah_mwh",
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
        "dispatch_mw_vector",
        "soc_fraction_vector",
        "decision_value_uah",
        "forecast_objective_value_uah",
        "oracle_value_uah",
        "regret_uah",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "evaluation_payload",
    }
)
_REQUIRED_BASELINE_COLUMNS: Final[frozenset[str]] = frozenset(
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


def build_poland_lag24_candidate_value_label_panel_frame(
    poland_schedule_candidate_library_frame: pl.DataFrame,
    entsoe_poland_lagged_feature_candidate_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Attach prior-safe Poland context features to candidate value labels."""

    _require_columns(
        poland_schedule_candidate_library_frame,
        _REQUIRED_LIBRARY_COLUMNS,
        frame_name="poland_schedule_candidate_library_frame",
    )
    if entsoe_poland_lagged_feature_candidate_frame.is_empty():
        raise ValueError("entsoe_poland_lagged_feature_candidate_frame must not be empty.")
    feature_rows = _poland_features_by_timestamp(
        entsoe_poland_lagged_feature_candidate_frame
    )
    prior_family_means = _prior_family_mean_regret_by_key(
        poland_schedule_candidate_library_frame
    )
    rows: list[dict[str, Any]] = []
    for row in poland_schedule_candidate_library_frame.iter_rows(named=True):
        anchor = _datetime_value(row["anchor_timestamp"])
        row_with_prior = dict(row)
        row_with_prior["prior_family_mean_regret_uah"] = prior_family_means.get(
            (
                str(row["tenant_id"]),
                str(row["source_model_name"]),
                str(row["candidate_family"]),
            ),
            0.0,
        )
        features = _selector_features(
            row_with_prior,
            poland_features=feature_rows.get(anchor, {}),
        )
        rows.append(
            {
                "tenant_id": str(row["tenant_id"]),
                "source_model_name": str(row["source_model_name"]),
                "candidate_family": str(row["candidate_family"]),
                "candidate_model_name": str(row["candidate_model_name"]),
                "anchor_timestamp": anchor,
                "split_name": str(row["split_name"]),
                "horizon_hours": int(row["horizon_hours"]),
                **features,
                "label_regret_uah": float(row["regret_uah"]),
                "label_decision_value_uah": float(row["decision_value_uah"]),
                "label_oracle_value_uah": float(row["oracle_value_uah"]),
                "claim_scope": POLAND_LAG24_CANDIDATE_VALUE_LABEL_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
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


def build_poland_lag24_candidate_value_ranker_frame(
    poland_candidate_value_label_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    min_prior_mean_improvement_ratio_vs_frozen_proxy: float = 0.01,
    ridge_l2: float = 1.0,
) -> pl.DataFrame:
    """Train a deterministic tabular candidate-value ranker on prior rows."""

    _validate_label_panel(poland_candidate_value_label_panel_frame)
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if min_prior_mean_improvement_ratio_vs_frozen_proxy < 0.0:
        raise ValueError("minimum prior improvement must not be negative.")
    rows = list(poland_candidate_value_label_panel_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = [
                row
                for row in rows
                if str(row["tenant_id"]) == tenant_id
                and str(row["source_model_name"]) == source_model_name
            ]
            train_rows = [
                row for row in source_rows if str(row["split_name"]) == "train_selection"
            ]
            final_rows = [
                row for row in source_rows if str(row["split_name"]) == "final_holdout"
            ]
            if not train_rows:
                raise ValueError(f"{tenant_id}/{source_model_name} ranker needs train rows.")
            if not final_rows:
                raise ValueError(f"{tenant_id}/{source_model_name} ranker needs final rows.")
            candidate_families = frozenset(str(row["candidate_family"]) for row in source_rows)
            scorer = _fit_scorer(
                train_rows,
                candidate_families=candidate_families,
                ridge_l2=ridge_l2,
            )
            selected_train = _select_by_scorer(
                train_rows,
                scorer=scorer,
                candidate_families=candidate_families,
            )
            selected_final = _select_by_scorer(
                final_rows,
                scorer=scorer,
                candidate_families=candidate_families,
            )
            strict_train = [
                row for row in train_rows if str(row["candidate_family"]) == "strict_control"
            ] or train_rows
            prior_baseline_mean = _mean_regret(strict_train)
            selected_train_mean = _mean_regret(selected_train)
            prior_improvement = _improvement_ratio(
                prior_baseline_mean,
                selected_train_mean,
            )
            fallback = (
                prior_improvement < min_prior_mean_improvement_ratio_vs_frozen_proxy
            )
            output_rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "learner_model_name": POLAND_LAG24_CANDIDATE_VALUE_RANKER_MODEL_NAME,
                    "selected_scorer_type": "tabular_ridge_candidate_value_ranker",
                    "selected_feature_names": list(POLAND_LAG24_RANKER_FEATURE_COLUMNS),
                    "selected_feature_weights": dict(scorer["weights"]),
                    "selected_feature_means": dict(scorer["feature_means"]),
                    "selected_feature_scales": dict(scorer["feature_scales"]),
                    "eligible_candidate_families": sorted(candidate_families),
                    "fallback_to_frozen_v2_plus": fallback,
                    "selector_gate_blocker": (
                        "candidate_prior_improvement_selected"
                        if not fallback
                        else "weak_prior_improvement_vs_strict_proxy"
                    ),
                    "train_anchor_count": _anchor_count(train_rows),
                    "final_holdout_anchor_count": _anchor_count(final_rows),
                    "prior_proxy_baseline_mean_regret_uah": prior_baseline_mean,
                    "selected_train_mean_regret_uah": (
                        prior_baseline_mean if fallback else selected_train_mean
                    ),
                    "candidate_train_mean_regret_uah": selected_train_mean,
                    "candidate_final_mean_regret_uah": _mean_regret(selected_final),
                    "prior_mean_improvement_ratio_vs_frozen_proxy": prior_improvement,
                    "selected_train_family_counts": _family_counts(selected_train),
                    "selected_final_family_counts": _family_counts(selected_final),
                    "selected_train_candidate_keys": [
                        _candidate_key(row) for row in selected_train
                    ],
                    "selected_final_candidate_keys": [
                        _candidate_key(row) for row in selected_final
                    ],
                    "claim_scope": POLAND_LAG24_CANDIDATE_VALUE_RANKER_CLAIM_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                    "market_execution_enabled": False,
                }
            )
    return pl.DataFrame(output_rows).sort(["source_model_name", "tenant_id"])


def build_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame(
    poland_schedule_candidate_library_frame: pl.DataFrame,
    poland_candidate_value_ranker_frame: pl.DataFrame,
    frozen_v2_plus_strict_frame: pl.DataFrame,
    *,
    baseline_source_model_name: str = FROZEN_V2_PLUS_BASELINE_MODEL_NAME,
) -> pl.DataFrame:
    """Emit frozen V2+ rows plus selected Poland candidate-ranker rows."""

    _require_columns(
        poland_schedule_candidate_library_frame,
        _REQUIRED_LIBRARY_COLUMNS,
        frame_name="poland_schedule_candidate_library_frame",
    )
    _require_columns(
        frozen_v2_plus_strict_frame,
        _REQUIRED_BASELINE_COLUMNS,
        frame_name="frozen_v2_plus_strict_frame",
    )
    _validate_ranker_frame(poland_candidate_value_ranker_frame)
    baseline_rows = [
        dict(row)
        for row in frozen_v2_plus_strict_frame.iter_rows(named=True)
        if str(row["source_model_name"]) == baseline_source_model_name
        and str(row["selection_role"]) == "schedule_value_learner_v2_plus"
    ]
    baseline_by_key = {
        (str(row["tenant_id"]), _datetime_value(row["anchor_timestamp"])): row
        for row in baseline_rows
    }
    library_rows = list(poland_schedule_candidate_library_frame.iter_rows(named=True))
    selected_rows: list[dict[str, Any]] = []
    for ranker_row in poland_candidate_value_ranker_frame.iter_rows(named=True):
        tenant_id = str(ranker_row["tenant_id"])
        source_model_name = str(ranker_row["source_model_name"])
        final_rows = [
            row
            for row in library_rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
            and str(row["split_name"]) == "final_holdout"
        ]
        if bool(ranker_row["fallback_to_frozen_v2_plus"]):
            for key, baseline_row in baseline_by_key.items():
                if key[0] == tenant_id:
                    selected_rows.append(
                        _strict_row_from_baseline(
                            baseline_row,
                            source_model_name=source_model_name,
                            ranker_row=ranker_row,
                        )
                    )
            continue
        selected_by_key = {(_candidate_key(row)): row for row in final_rows}
        selected_keys = [str(key) for key in ranker_row["selected_final_candidate_keys"]]
        missing_keys = [key for key in selected_keys if key not in selected_by_key]
        if missing_keys:
            raise ValueError(
                f"{tenant_id}/{source_model_name} selected candidate keys missing "
                f"from final candidate library: {missing_keys[:3]}"
            )
        for selected_key in selected_keys:
            selected = selected_by_key[selected_key]
            baseline = baseline_by_key.get(
                (tenant_id, _datetime_value(selected["anchor_timestamp"]))
            )
            selected_rows.append(
                _strict_row_from_candidate(
                    selected,
                    baseline_row=baseline,
                    ranker_row=ranker_row,
                )
            )
    baseline_frame = _baseline_frame_with_claim(baseline_rows)
    return pl.concat(
        [baseline_frame, pl.DataFrame(selected_rows)],
        how="diagonal_relaxed",
    ).sort(["tenant_id", "anchor_timestamp", "forecast_model_name"])


def _poland_features_by_timestamp(frame: pl.DataFrame) -> dict[datetime, dict[str, Any]]:
    if "delivery_timestamp_utc" not in frame.columns:
        raise ValueError("Poland lagged feature frame missing delivery_timestamp_utc.")
    result: dict[datetime, dict[str, Any]] = {}
    for row in frame.iter_rows(named=True):
        result[_datetime_from_iso(str(row["delivery_timestamp_utc"]))] = dict(row)
    return result


def _prior_family_mean_regret_by_key(frame: pl.DataFrame) -> dict[tuple[str, str, str], float]:
    rows = list(frame.iter_rows(named=True))
    grouped: dict[tuple[str, str, str], list[float]] = {}
    for row in rows:
        if str(row["split_name"]) != "train_selection":
            continue
        key = (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            str(row["candidate_family"]),
        )
        grouped.setdefault(key, []).append(float(row["regret_uah"]))
    return {key: mean(values) for key, values in grouped.items() if values}


def _selector_features(
    row: dict[str, Any],
    *,
    poland_features: dict[str, Any],
) -> dict[str, float]:
    forecast_prices = _float_list(row["forecast_price_uah_mwh_vector"])
    dispatch = _float_list(row["dispatch_mw_vector"])
    soc = _float_list(row["soc_fraction_vector"])
    return {
        "selector_feature_prior_family_mean_regret_uah": float(
            row.get("prior_family_mean_regret_uah", row["regret_uah"])
        ),
        "selector_feature_forecast_spread_uah_mwh": (
            max(forecast_prices) - min(forecast_prices) if forecast_prices else 0.0
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
        "selector_feature_terminal_soc_fraction": soc[-1] if soc else 0.5,
        "selector_feature_dispatch_reversal_count": float(
            _dispatch_reversal_count(dispatch)
        ),
        "selector_feature_poland_lag24_price_uah_mwh": _feature_value(
            poland_features,
            "entsoe_pl_lag24_day_ahead_price_uah_mwh",
        ),
        "selector_feature_poland_lag24_delta_24h_uah_mwh": _feature_value(
            poland_features,
            "entsoe_pl_lag24_delta_24h_uah_mwh",
        ),
        "selector_feature_poland_lag24_daily_spread_uah_mwh": _feature_value(
            poland_features,
            "entsoe_pl_lag24_daily_spread_uah_mwh",
        ),
        "selector_feature_poland_lag24_daily_price_rank": _feature_value(
            poland_features,
            "entsoe_pl_lag24_daily_price_rank",
        ),
        "selector_feature_poland_lag24_ua_spread_uah_mwh": _feature_value(
            poland_features,
            "entsoe_pl_lag24_ua_spread_uah_mwh",
        ),
        "selector_feature_poland_lag24_ua_rank_disagreement": _feature_value(
            poland_features,
            "entsoe_pl_lag24_ua_rank_disagreement",
        ),
        "selector_feature_poland_lag24_ua_peak_hour_delta": _feature_value(
            poland_features,
            "entsoe_pl_lag24_ua_peak_hour_delta",
        ),
        "selector_feature_poland_lag24_ua_trough_hour_delta": _feature_value(
            poland_features,
            "entsoe_pl_lag24_ua_trough_hour_delta",
        ),
        "selector_feature_poland_lag24_evening_morning_spread_uah_mwh": _feature_value(
            poland_features,
            "entsoe_pl_lag24_evening_morning_spread_uah_mwh",
        ),
    }


def _fit_scorer(
    train_rows: list[dict[str, Any]],
    *,
    candidate_families: frozenset[str],
    ridge_l2: float,
) -> dict[str, Any]:
    feature_means: dict[str, float] = {}
    feature_scales: dict[str, float] = {}
    for column in POLAND_LAG24_RANKER_FEATURE_COLUMNS:
        values = [float(row[column]) for row in train_rows]
        feature_means[column] = mean(values)
        span = max(values) - min(values)
        feature_scales[column] = span if span > 1e-9 else 1.0
    family_columns = tuple(f"family::{family}" for family in sorted(candidate_families))
    feature_matrix = [
        _feature_vector(
            row,
            feature_means=feature_means,
            feature_scales=feature_scales,
            family_columns=family_columns,
        )
        for row in train_rows
    ]
    targets = [float(row["label_regret_uah"]) for row in train_rows]
    coefficients = v3._fit_ridge_coefficients(
        feature_matrix,
        targets,
        ridge_l2=ridge_l2,
    )
    feature_names = [*POLAND_LAG24_RANKER_FEATURE_COLUMNS, *family_columns]
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
    }


def _select_by_scorer(
    rows: list[dict[str, Any]],
    *,
    scorer: dict[str, Any],
    candidate_families: frozenset[str],
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for _anchor, anchor_rows in sorted(_rows_by_anchor(rows).items()):
        candidates = [
            row
            for row in anchor_rows
            if str(row["candidate_family"]) in candidate_families
        ]
        if candidates:
            selected.append(
                min(
                    candidates,
                    key=lambda row: (
                        _predict_regret(row, scorer=scorer),
                        str(row["candidate_family"]),
                        str(row["candidate_model_name"]),
                    ),
                )
            )
    return selected


def _select_by_model_row(
    rows: list[dict[str, Any]],
    *,
    ranker_row: dict[str, Any],
) -> list[dict[str, Any]]:
    scorer = {
        "weights": dict(ranker_row["selected_feature_weights"]),
        "feature_means": dict(ranker_row["selected_feature_means"]),
        "feature_scales": dict(ranker_row["selected_feature_scales"]),
        "family_columns": tuple(
            sorted(
                key
                for key in dict(ranker_row["selected_feature_weights"])
                if key.startswith("family::")
            )
        ),
    }
    return _select_by_scorer(
        rows,
        scorer=scorer,
        candidate_families=frozenset(ranker_row["eligible_candidate_families"]),
    )


def _feature_vector(
    row: dict[str, Any],
    *,
    feature_means: dict[str, float],
    feature_scales: dict[str, float],
    family_columns: tuple[str, ...],
) -> list[float]:
    numeric = [
        (float(row[column]) - feature_means[column]) / feature_scales[column]
        for column in POLAND_LAG24_RANKER_FEATURE_COLUMNS
    ]
    family = str(row["candidate_family"])
    one_hot = [1.0 if column == f"family::{family}" else 0.0 for column in family_columns]
    return [*numeric, *one_hot]


def _predict_regret(row: dict[str, Any], *, scorer: dict[str, Any]) -> float:
    feature_names = [
        *POLAND_LAG24_RANKER_FEATURE_COLUMNS,
        *tuple(str(column) for column in scorer["family_columns"]),
    ]
    weights = dict(scorer["weights"])
    values = _feature_vector(
        row,
        feature_means=dict(scorer["feature_means"]),
        feature_scales=dict(scorer["feature_scales"]),
        family_columns=tuple(str(column) for column in scorer["family_columns"]),
    )
    score = float(weights.get("intercept", 0.0))
    for feature_name, value in zip(feature_names, values, strict=True):
        score += float(weights.get(feature_name, 0.0)) * value
    return score


def _strict_row_from_candidate(
    row: dict[str, Any],
    *,
    baseline_row: dict[str, Any] | None,
    ranker_row: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(row.get("evaluation_payload", {}))
    payload.update(
        {
            "selector_role": POLAND_LAG24_CANDIDATE_VALUE_RANKER_SELECTION_ROLE,
            "fallback_to_frozen_v2_plus": False,
            "selected_feature_weights": dict(ranker_row["selected_feature_weights"]),
            "baseline_regret_uah": None
            if baseline_row is None
            else float(baseline_row["regret_uah"]),
            "market_execution_enabled": False,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:poland-lag24-candidate-value-ranker:"
            f"{ranker_row['source_model_name']}:"
            f"{_datetime_value(row['anchor_timestamp']):%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": str(ranker_row["source_model_name"]),
        "forecast_model_name": (
            f"{POLAND_LAG24_CANDIDATE_VALUE_RANKER_MODEL_NAME}_"
            f"{ranker_row['source_model_name']}"
        ),
        "strategy_kind": POLAND_LAG24_CANDIDATE_VALUE_RANKER_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "selection_role": POLAND_LAG24_CANDIDATE_VALUE_RANKER_SELECTION_ROLE,
        "candidate_family": str(row["candidate_family"]),
        "candidate_model_name": str(row["candidate_model_name"]),
        "anchor_timestamp": _datetime_value(row["anchor_timestamp"]),
        "generated_at": row.get("generated_at"),
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": v2._first_or_default(
            row["soc_fraction_vector"],
            default=0.5,
        ),
        "starting_soc_source": "poland_lag24_candidate_value_ranker",
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(
            row.get(
                "regret_ratio",
                _safe_ratio(float(row["regret_uah"]), float(row["oracle_value_uah"])),
            )
        ),
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
        "evaluation_payload": payload,
        "claim_scope": POLAND_LAG24_CANDIDATE_VALUE_RANKER_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _strict_row_from_baseline(
    baseline_row: dict[str, Any],
    *,
    source_model_name: str,
    ranker_row: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(baseline_row.get("evaluation_payload", {}))
    payload.update(
        {
            "selector_role": POLAND_LAG24_CANDIDATE_VALUE_RANKER_SELECTION_ROLE,
            "fallback_to_frozen_v2_plus": True,
            "selected_feature_weights": dict(ranker_row["selected_feature_weights"]),
            "market_execution_enabled": False,
        }
    )
    copied = dict(baseline_row)
    copied.update(
        {
            "evaluation_id": (
                f"{baseline_row['tenant_id']}:poland-lag24-candidate-value-ranker:"
                f"{source_model_name}:frozen-v2-plus-fallback:"
                f"{_datetime_value(baseline_row['anchor_timestamp']):%Y%m%dT%H%M}"
            ),
            "source_model_name": source_model_name,
            "forecast_model_name": (
                f"{POLAND_LAG24_CANDIDATE_VALUE_RANKER_MODEL_NAME}_{source_model_name}"
            ),
            "strategy_kind": POLAND_LAG24_CANDIDATE_VALUE_RANKER_STRICT_LP_STRATEGY_KIND,
            "selection_role": POLAND_LAG24_CANDIDATE_VALUE_RANKER_SELECTION_ROLE,
            "candidate_family": "frozen_v2_plus_fallback",
            "candidate_model_name": str(baseline_row["forecast_model_name"]),
            "evaluation_payload": payload,
            "claim_scope": POLAND_LAG24_CANDIDATE_VALUE_RANKER_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return copied


def _baseline_frame_with_claim(rows: list[dict[str, Any]]) -> pl.DataFrame:
    output: list[dict[str, Any]] = []
    for row in rows:
        copied = dict(row)
        copied["claim_scope"] = "frozen_v2_plus_reference_not_market_execution"
        copied["not_full_dfl"] = True
        copied["not_market_execution"] = True
        copied["market_execution_enabled"] = False
        output.append(copied)
    return pl.DataFrame(output)


def _validate_label_panel(frame: pl.DataFrame) -> None:
    required = {
        "tenant_id",
        "source_model_name",
        "candidate_family",
        "candidate_model_name",
        "anchor_timestamp",
        "split_name",
        "label_regret_uah",
        "claim_scope",
        "not_market_execution",
        *POLAND_LAG24_RANKER_FEATURE_COLUMNS,
    }
    _require_columns(frame, frozenset(required), frame_name="poland label panel")
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != POLAND_LAG24_CANDIDATE_VALUE_LABEL_CLAIM_SCOPE:
            raise ValueError("Poland candidate-value label panel has unexpected claim_scope.")
        if not bool(row["not_market_execution"]):
            raise ValueError("Poland candidate-value label rows must keep not_market_execution.")


def _validate_ranker_frame(frame: pl.DataFrame) -> None:
    required = {
        "tenant_id",
        "source_model_name",
        "selected_feature_weights",
        "selected_feature_means",
        "selected_feature_scales",
        "selected_final_candidate_keys",
        "eligible_candidate_families",
        "fallback_to_frozen_v2_plus",
        "claim_scope",
        "not_market_execution",
    }
    _require_columns(frame, frozenset(required), frame_name="poland ranker frame")
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != POLAND_LAG24_CANDIDATE_VALUE_RANKER_CLAIM_SCOPE:
            raise ValueError("Poland candidate-value ranker frame has unexpected claim_scope.")
        if not bool(row["not_market_execution"]):
            raise ValueError("Poland candidate-value ranker rows must keep not_market_execution.")


def _require_columns(frame: pl.DataFrame, columns: frozenset[str], *, frame_name: str) -> None:
    missing = sorted(columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing columns: {missing}")
    if "market_execution_enabled" in frame.columns and frame.select(
        pl.col("market_execution_enabled").any()
    ).item():
        raise ValueError(f"{frame_name} refuses market execution claims.")


def _rows_by_anchor(rows: list[dict[str, Any]]) -> dict[datetime, list[dict[str, Any]]]:
    result: dict[datetime, list[dict[str, Any]]] = {}
    for row in rows:
        result.setdefault(_datetime_value(row["anchor_timestamp"]), []).append(row)
    return result


def _candidate_key(row: dict[str, Any]) -> str:
    anchor = _datetime_value(row["anchor_timestamp"]).isoformat()
    return (
        f"{anchor}|{row['tenant_id']}|{row['source_model_name']}|"
        f"{row['candidate_family']}|{row['candidate_model_name']}"
    )


def _anchor_count(rows: list[dict[str, Any]]) -> int:
    return len({_datetime_value(row["anchor_timestamp"]) for row in rows})


def _family_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        family = str(row["candidate_family"])
        counts[family] = counts.get(family, 0) + 1
    return counts


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    values = [
        float(row["label_regret_uah"] if "label_regret_uah" in row else row["regret_uah"])
        for row in rows
    ]
    return mean(values)


def _improvement_ratio(control_value: float, candidate_value: float) -> float:
    return (
        (control_value - candidate_value) / abs(control_value)
        if abs(control_value) > 1e-9
        else 0.0
    )


def _safe_ratio(numerator: float, denominator: float) -> float:
    return numerator / abs(denominator) if abs(denominator) > 1e-9 else 0.0


def _feature_value(row: dict[str, Any], column: str) -> float:
    value = row.get(column, 0.0)
    return 0.0 if value is None else float(value)


def _float_list(value: object) -> list[float]:
    if isinstance(value, pl.Series):
        return [float(item) for item in value.to_list()]
    if isinstance(value, (list, tuple)):
        return [float(item) for item in value]
    return []


def _dispatch_reversal_count(values: list[float]) -> int:
    signs = [1 if value > 0.0 else -1 if value < 0.0 else 0 for value in values]
    return sum(
        1
        for left, right in zip(signs, signs[1:], strict=False)
        if left != 0 and right != 0 and left != right
    )


def _datetime_value(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    return _datetime_from_iso(str(value))


def _datetime_from_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed


__all__ = [
    "POLAND_LAG24_CANDIDATE_VALUE_RANKER_SELECTION_ROLE",
    "build_poland_lag24_candidate_value_label_panel_frame",
    "build_poland_lag24_candidate_value_ranker_frame",
    "build_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame",
]
