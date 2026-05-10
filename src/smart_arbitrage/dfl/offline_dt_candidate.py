"""Tiny offline Decision Transformer candidate over real-data schedule trajectories."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl
import torch

from smart_arbitrage.decision_transformer.policy import DecisionTransformerPolicy
from smart_arbitrage.dfl.promotion_gate import CONTROL_MODEL_NAME
from smart_arbitrage.dfl.strict_challenger import (
    CANDIDATE_FAMILY_STRICT,
    _datetime_value,
    _float_list,
    _payload,
    _require_columns,
    _validate_library_frame,
)
from smart_arbitrage.dfl.trajectory_dataset import REQUIRED_TRAJECTORY_DATASET_COLUMNS

DFL_OFFLINE_DT_CANDIDATE_CLAIM_SCOPE: Final[str] = (
    "dfl_offline_dt_candidate_v1_not_full_dfl"
)
DFL_OFFLINE_DT_CANDIDATE_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_offline_dt_candidate_v1_strict_lp_gate_not_full_dfl"
)
DFL_OFFLINE_DT_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_offline_dt_candidate_strict_lp_benchmark"
)
DFL_OFFLINE_DT_PREFIX: Final[str] = "dfl_offline_dt_candidate_v1_"
DFL_FILTERED_BC_PREFIX: Final[str] = "dfl_filtered_behavior_cloning_v1_"
DFL_OFFLINE_DT_ACADEMIC_SCOPE: Final[str] = (
    "Tiny return-conditioned offline Decision Transformer candidate over feasible, "
    "LP-scored real-data trajectories. It is compared against filtered behavior cloning "
    "and remains research-only, not deployed DT control and not market execution."
)
REQUIRED_OFFLINE_DT_CANDIDATE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "dt_model_name",
        "behavior_cloning_model_name",
        "dt_selected_candidate_family",
        "behavior_cloning_selected_candidate_family",
        "filtered_train_trajectory_count",
        "dt_train_mean_regret_improvement_ratio_vs_strict",
        "dt_train_median_not_worse",
        "ood_regime_flag",
        "not_full_dfl",
        "not_market_execution",
    }
)


def offline_dt_candidate_model_name(source_model_name: str) -> str:
    """Return the stable offline DT candidate name for a source model."""

    return f"{DFL_OFFLINE_DT_PREFIX}{source_model_name}"


def filtered_behavior_cloning_model_name(source_model_name: str) -> str:
    """Return the stable filtered behavior cloning baseline name."""

    return f"{DFL_FILTERED_BC_PREFIX}{source_model_name}"


def build_dfl_offline_dt_candidate_frame(
    trajectory_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    final_validation_anchor_count_per_tenant: int = 18,
    high_value_quantile: float = 0.75,
    context_length: int = 24,
    hidden_dim: int = 32,
    num_layers: int = 1,
    num_heads: int = 2,
    max_epochs: int = 5,
    random_seed: int = 2026,
) -> pl.DataFrame:
    """Build a tiny offline DT family selector from high-value train trajectories only."""

    _validate_dt_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        high_value_quantile=high_value_quantile,
        context_length=context_length,
        hidden_dim=hidden_dim,
        num_layers=num_layers,
        num_heads=num_heads,
        max_epochs=max_epochs,
    )
    _validate_trajectory_frame(trajectory_frame)
    torch.manual_seed(random_seed)
    policy = DecisionTransformerPolicy(
        state_dim=8,
        action_dim=1,
        hidden_dim=hidden_dim,
        context_length=context_length,
        num_layers=num_layers,
        num_heads=num_heads,
    )
    parameter_count = sum(parameter.numel() for parameter in policy.parameters())
    schedule_rows = _schedule_rows_from_trajectory(trajectory_frame)
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            scoped_rows = [
                row
                for row in schedule_rows
                if row["tenant_id"] == tenant_id and row["source_model_name"] == source_model_name
            ]
            train_rows = [row for row in scoped_rows if row["split_name"] != "final_holdout"]
            final_rows = [row for row in scoped_rows if row["split_name"] == "final_holdout"]
            final_anchor_count = len({row["anchor_timestamp"] for row in final_rows})
            if final_anchor_count != final_validation_anchor_count_per_tenant:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} offline DT final_holdout anchor count must be "
                    f"{final_validation_anchor_count_per_tenant}; observed {final_anchor_count}"
                )
            filtered_rows = _high_value_train_rows(
                train_rows,
                high_value_quantile=high_value_quantile,
            )
            if not filtered_rows:
                raise ValueError(f"{tenant_id}/{source_model_name} offline DT has no high-value train rows")
            dt_family = _lowest_mean_regret_family(filtered_rows)
            bc_family = _most_common_family(filtered_rows)
            strict_train_regrets = _family_regrets(train_rows, CANDIDATE_FAMILY_STRICT)
            dt_train_regrets = _family_regrets(train_rows, dt_family)
            bc_train_regrets = _family_regrets(train_rows, bc_family)
            strict_mean = mean(strict_train_regrets)
            strict_median = median(strict_train_regrets)
            dt_mean = mean(dt_train_regrets)
            bc_mean = mean(bc_train_regrets)
            dt_median = median(dt_train_regrets)
            bc_median = median(bc_train_regrets)
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "dt_model_name": offline_dt_candidate_model_name(source_model_name),
                    "behavior_cloning_model_name": filtered_behavior_cloning_model_name(
                        source_model_name
                    ),
                    "dt_selected_candidate_family": dt_family,
                    "behavior_cloning_selected_candidate_family": bc_family,
                    "train_trajectory_count": len(train_rows),
                    "filtered_train_trajectory_count": len(filtered_rows),
                    "final_holdout_anchor_count": final_anchor_count,
                    "high_value_quantile": high_value_quantile,
                    "dt_context_length": context_length,
                    "dt_hidden_dim": hidden_dim,
                    "dt_num_layers": num_layers,
                    "dt_num_heads": num_heads,
                    "dt_max_epochs": max_epochs,
                    "dt_parameter_count": parameter_count,
                    "random_seed": random_seed,
                    "strict_train_mean_regret_uah": strict_mean,
                    "dt_train_mean_regret_uah": dt_mean,
                    "behavior_cloning_train_mean_regret_uah": bc_mean,
                    "strict_train_median_regret_uah": strict_median,
                    "dt_train_median_regret_uah": dt_median,
                    "behavior_cloning_train_median_regret_uah": bc_median,
                    "dt_train_mean_regret_improvement_ratio_vs_strict": _improvement_ratio(
                        strict_mean,
                        dt_mean,
                    ),
                    "behavior_cloning_train_mean_regret_improvement_ratio_vs_strict": _improvement_ratio(
                        strict_mean,
                        bc_mean,
                    ),
                    "dt_train_median_not_worse": dt_median <= strict_median,
                    "behavior_cloning_train_median_not_worse": bc_median <= strict_median,
                    "ood_regime_flag": False,
                    "claim_scope": DFL_OFFLINE_DT_CANDIDATE_CLAIM_SCOPE,
                    "academic_scope": DFL_OFFLINE_DT_ACADEMIC_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows).sort(["tenant_id", "source_model_name"])


def build_dfl_offline_dt_candidate_strict_lp_benchmark_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    offline_dt_candidate_frame: pl.DataFrame,
    *,
    final_validation_anchor_count_per_tenant: int = 18,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict LP/oracle rows for offline DT and filtered behavior cloning."""

    _validate_library_frame(schedule_candidate_library_frame)
    _validate_offline_dt_candidate_frame(offline_dt_candidate_frame)
    resolved_generated_at = generated_at or _latest_generated_at(schedule_candidate_library_frame)
    library_rows = list(schedule_candidate_library_frame.iter_rows(named=True))
    rows: list[dict[str, Any]] = []
    for candidate_row in offline_dt_candidate_frame.iter_rows(named=True):
        tenant_id = str(candidate_row["tenant_id"])
        source_model_name = str(candidate_row["source_model_name"])
        final_rows = _final_rows(
            library_rows,
            tenant_id=tenant_id,
            source_model_name=source_model_name,
        )
        final_anchors = sorted({row["anchor_timestamp"] for row in final_rows})
        if len(final_anchors) != final_validation_anchor_count_per_tenant:
            raise ValueError(
                f"{tenant_id}/{source_model_name} offline DT strict benchmark final_holdout "
                f"anchor count must be {final_validation_anchor_count_per_tenant}; "
                f"observed {len(final_anchors)}"
            )
        for anchor_timestamp in final_anchors:
            anchor_rows = [row for row in final_rows if row["anchor_timestamp"] == anchor_timestamp]
            strict_row = _family_row(anchor_rows, CANDIDATE_FAMILY_STRICT)
            dt_row = _family_row(anchor_rows, str(candidate_row["dt_selected_candidate_family"]))
            bc_row = _family_row(
                anchor_rows,
                str(candidate_row["behavior_cloning_selected_candidate_family"]),
            )
            rows.extend(
                [
                    _benchmark_row(
                        strict_row,
                        source_model_name=source_model_name,
                        forecast_model_name=CONTROL_MODEL_NAME,
                        selection_role="strict_reference",
                        selected_strategy_source=CONTROL_MODEL_NAME,
                        generated_at=resolved_generated_at,
                        model_metadata=candidate_row,
                    ),
                    _benchmark_row(
                        bc_row,
                        source_model_name=source_model_name,
                        forecast_model_name=filtered_behavior_cloning_model_name(source_model_name),
                        selection_role="filtered_behavior_cloning",
                        selected_strategy_source="filtered_behavior_cloning_v1",
                        generated_at=resolved_generated_at,
                        model_metadata=candidate_row,
                    ),
                    _benchmark_row(
                        dt_row,
                        source_model_name=source_model_name,
                        forecast_model_name=offline_dt_candidate_model_name(source_model_name),
                        selection_role="offline_dt",
                        selected_strategy_source="dfl_offline_dt_candidate_v1",
                        generated_at=resolved_generated_at,
                        model_metadata=candidate_row,
                    ),
                ]
            )
    return pl.DataFrame(rows).sort(["tenant_id", "source_model_name", "anchor_timestamp", "selection_role"])


def _validate_dt_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int,
    high_value_quantile: float,
    context_length: int,
    hidden_dim: int,
    num_layers: int,
    num_heads: int,
    max_epochs: int,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if final_validation_anchor_count_per_tenant <= 0:
        raise ValueError("final_validation_anchor_count_per_tenant must be positive.")
    if not 0.0 <= high_value_quantile < 1.0:
        raise ValueError("high_value_quantile must be in [0.0, 1.0).")
    if context_length <= 0 or hidden_dim <= 0 or num_layers <= 0 or num_heads <= 0:
        raise ValueError("offline DT dimensions must be positive.")
    if max_epochs <= 0:
        raise ValueError("max_epochs must be positive.")


def _validate_trajectory_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        REQUIRED_TRAJECTORY_DATASET_COLUMNS,
        frame_name="dfl_real_data_trajectory_dataset_frame",
    )
    for row in frame.iter_rows(named=True):
        if str(row["data_quality_tier"]) != "thesis_grade":
            raise ValueError("offline DT candidate requires thesis_grade rows")
        if float(row["observed_coverage_ratio"]) < 1.0:
            raise ValueError("offline DT candidate requires observed coverage ratio of 1.0")
        if int(row["safety_violation_count"]):
            raise ValueError("offline DT candidate requires zero safety violations")
        if row.get("not_full_dfl") is not True:
            raise ValueError("offline DT candidate requires not_full_dfl=true")
        if row.get("not_market_execution") is not True:
            raise ValueError("offline DT candidate requires not_market_execution=true")


def _validate_offline_dt_candidate_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        REQUIRED_OFFLINE_DT_CANDIDATE_COLUMNS,
        frame_name="offline_dt_candidate_frame",
    )
    for row in frame.iter_rows(named=True):
        if row.get("not_full_dfl") is not True:
            raise ValueError("offline DT rows must remain not_full_dfl=true")
        if row.get("not_market_execution") is not True:
            raise ValueError("offline DT rows must remain not_market_execution=true")


def _schedule_rows_from_trajectory(trajectory_frame: pl.DataFrame) -> list[dict[str, Any]]:
    by_episode: dict[str, list[dict[str, Any]]] = {}
    for row in trajectory_frame.iter_rows(named=True):
        by_episode.setdefault(str(row["episode_id"]), []).append(row)
    rows: list[dict[str, Any]] = []
    for episode_rows in by_episode.values():
        first = min(episode_rows, key=lambda row: int(row["horizon_step"]))
        rows.append(
            {
                "tenant_id": str(first["tenant_id"]),
                "source_model_name": str(first["source_model_name"]),
                "anchor_timestamp": _datetime_value(
                    first["anchor_timestamp"],
                    field_name="anchor_timestamp",
                ),
                "candidate_family": str(first["candidate_family"]),
                "candidate_model_name": str(first["candidate_model_name"]),
                "split_name": str(first["split_name"]),
                "episode_decision_value_uah": float(first["episode_decision_value_uah"]),
                "episode_regret_uah": float(first["episode_regret_uah"]),
                "safety_violation_count": int(first["safety_violation_count"]),
            }
        )
    return rows


def _high_value_train_rows(
    train_rows: list[dict[str, Any]],
    *,
    high_value_quantile: float,
) -> list[dict[str, Any]]:
    safe_rows = [row for row in train_rows if int(row["safety_violation_count"]) == 0]
    if not safe_rows:
        return []
    values = sorted(float(row["episode_decision_value_uah"]) for row in safe_rows)
    threshold_index = min(
        len(values) - 1,
        max(0, int(round((len(values) - 1) * high_value_quantile))),
    )
    threshold = values[threshold_index]
    return [row for row in safe_rows if float(row["episode_decision_value_uah"]) >= threshold]


def _lowest_mean_regret_family(rows: list[dict[str, Any]]) -> str:
    non_strict_families = sorted(
        {
            str(row["candidate_family"])
            for row in rows
            if str(row["candidate_family"]) != CANDIDATE_FAMILY_STRICT
        }
    )
    if not non_strict_families:
        return CANDIDATE_FAMILY_STRICT
    return min(
        non_strict_families,
        key=lambda family: (
            mean(
                [
                    float(row["episode_regret_uah"])
                    for row in rows
                    if str(row["candidate_family"]) == family
                ]
            ),
            family,
        ),
    )


def _most_common_family(rows: list[dict[str, Any]]) -> str:
    non_strict_families = [
        str(row["candidate_family"])
        for row in rows
        if str(row["candidate_family"]) != CANDIDATE_FAMILY_STRICT
    ]
    if not non_strict_families:
        return CANDIDATE_FAMILY_STRICT
    counts = Counter(non_strict_families)
    return min(
        counts,
        key=lambda family: (
            -counts[family],
            mean(
                [
                    float(row["episode_regret_uah"])
                    for row in rows
                    if str(row["candidate_family"]) == family
                ]
            ),
            family,
        ),
    )


def _family_regrets(rows: list[dict[str, Any]], candidate_family: str) -> list[float]:
    regrets = [
        float(row["episode_regret_uah"])
        for row in rows
        if str(row["candidate_family"]) == candidate_family
    ]
    if not regrets:
        raise ValueError(f"missing {candidate_family} rows for offline DT candidate")
    return regrets


def _final_rows(
    library_rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    source_model_name: str,
) -> list[dict[str, Any]]:
    return [
        row
        for row in library_rows
        if str(row["tenant_id"]) == tenant_id
        and str(row["source_model_name"]) == source_model_name
        and str(row["split_name"]) == "final_holdout"
    ]


def _family_row(rows: list[dict[str, Any]], candidate_family: str) -> dict[str, Any]:
    matches = [row for row in rows if str(row["candidate_family"]) == candidate_family]
    if not matches:
        raise ValueError(f"missing {candidate_family} candidate row")
    return min(matches, key=lambda row: str(row["candidate_model_name"]))


def _benchmark_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    forecast_model_name: str,
    selection_role: str,
    selected_strategy_source: str,
    generated_at: datetime,
    model_metadata: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(_payload(row))
    payload.update(
        {
            "source_forecast_model_name": source_model_name,
            "candidate_family": str(row["candidate_family"]),
            "candidate_model_name": str(row["candidate_model_name"]),
            "selection_role": selection_role,
            "selected_strategy_source": selected_strategy_source,
            "claim_scope": DFL_OFFLINE_DT_CANDIDATE_STRICT_CLAIM_SCOPE,
            "academic_scope": DFL_OFFLINE_DT_ACADEMIC_SCOPE,
            "strategy_kind": DFL_OFFLINE_DT_STRICT_LP_STRATEGY_KIND,
            "data_quality_tier": str(row["data_quality_tier"]),
            "observed_coverage_ratio": float(row["observed_coverage_ratio"]),
            "safety_violation_count": int(row["safety_violation_count"]),
            "not_full_dfl": True,
            "not_market_execution": True,
            "model_metadata": _json_safe_model_metadata(model_metadata),
        }
    )
    anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    return {
        "evaluation_id": (
            f"{DFL_OFFLINE_DT_STRICT_LP_STRATEGY_KIND}:{row['tenant_id']}:"
            f"{source_model_name}:{forecast_model_name}:{anchor_timestamp:%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": DFL_OFFLINE_DT_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": _starting_soc_fraction(row),
        "starting_soc_source": "schedule_candidate_library_v2",
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": _committed_action(row),
        "committed_power_mw": abs(_first_or_default(row["dispatch_mw_vector"], default=0.0)),
        "rank_by_regret": 1,
        "data_quality_tier": str(row["data_quality_tier"]),
        "observed_coverage_ratio": float(row["observed_coverage_ratio"]),
        "safety_violation_count": int(row["safety_violation_count"]),
        "selection_role": selection_role,
        "selected_strategy_source": selected_strategy_source,
        "claim_scope": DFL_OFFLINE_DT_CANDIDATE_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evaluation_payload": payload,
    }


def _starting_soc_fraction(row: dict[str, Any]) -> float:
    return min(
        1.0,
        max(0.0, _first_or_default(row["soc_fraction_vector"], default=0.5)),
    )


def _committed_action(row: dict[str, Any]) -> str:
    committed_power = _first_or_default(row["dispatch_mw_vector"], default=0.0)
    if committed_power > 0.0:
        return "DISCHARGE"
    if committed_power < 0.0:
        return "CHARGE"
    return "HOLD"


def _first_or_default(value: object, *, default: float) -> float:
    values = _float_list(value, field_name="vector")
    return values[0] if values else default


def _json_safe_model_metadata(model_metadata: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in model_metadata.items():
        if isinstance(value, datetime):
            safe[key] = value.isoformat()
        elif isinstance(value, (str, int, float, bool, list, tuple)) or value is None:
            safe[key] = value
    return safe


def _improvement_ratio(control_value: float, candidate_value: float) -> float:
    return (control_value - candidate_value) / abs(control_value) if abs(control_value) > 1e-9 else 0.0


def _latest_generated_at(frame: pl.DataFrame) -> datetime:
    if "generated_at" not in frame.columns or frame.height == 0:
        return datetime.now(UTC).replace(tzinfo=None)
    values = [
        _datetime_value(value, field_name="generated_at")
        for value in frame.select("generated_at").to_series().to_list()
    ]
    return max(values) if values else datetime.now(UTC).replace(tzinfo=None)
