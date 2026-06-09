"""Failure audit for the official V2+-teacher DFL/DT bridge."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Final

import polars as pl

from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_OFFICIAL_V2_PLUS_BRIDGE_FAILURE_AUDIT_CLAIM_SCOPE: Final[str] = (
    "dfl_official_v2_plus_bridge_failure_audit_not_full_dfl"
)
DFL_OFFICIAL_V2_PLUS_BRIDGE_FAILURE_AUDIT_ACADEMIC_SCOPE: Final[str] = (
    "Analysis-only audit of why official V2+-teacher residual DFL/offline DT "
    "does not beat the frozen V2+ comparator. This is not training input, not "
    "market execution, and not deployed Decision Transformer control."
)
V2_PLUS_ROLE: Final[str] = "schedule_value_learner_v2_plus_reference"
STRICT_ROLE: Final[str] = "strict_reference"
CHALLENGER_ROLES: Final[tuple[str, ...]] = (
    "residual_dfl_reference",
    "offline_dt_reference",
    "filtered_behavior_cloning_reference",
    "residual_dt_fallback_reference",
)
REQUIRED_BRIDGE_ROLES: Final[tuple[str, ...]] = (
    STRICT_ROLE,
    V2_PLUS_ROLE,
    *CHALLENGER_ROLES,
)
REQUIRED_BRIDGE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "forecast_model_name",
        "strategy_kind",
        "market_venue",
        "anchor_timestamp",
        "generated_at",
        "regret_uah",
        "decision_value_uah",
        "oracle_value_uah",
        "total_throughput_mwh",
        "data_quality_tier",
        "observed_coverage_ratio",
        "safety_violation_count",
        "selection_role",
        "not_full_dfl",
        "not_market_execution",
        "evaluation_payload",
    }
)
REQUIRED_AUDIT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "generated_at",
        "analysis_only_challenger_role",
        "analysis_only_v2_plus_regret_uah",
        "analysis_only_challenger_regret_uah",
        "analysis_only_regret_delta_vs_v2_plus_uah",
        "analysis_only_failure_mode",
        "claim_scope",
        "market_execution_enabled",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_dfl_official_v2_plus_bridge_failure_audit_frame(
    bridge_strict_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build one analysis-only failure row per source/tenant/anchor/challenger."""

    _validate_bridge_input(bridge_strict_frame)
    rows = list(bridge_strict_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for group_rows in _group_bridge_rows(rows).values():
        by_role = {str(row["selection_role"]): row for row in group_rows}
        v2_row = by_role[V2_PLUS_ROLE]
        strict_row = by_role[STRICT_ROLE]
        challenger_family_count = len(
            {
                _candidate_family(by_role[role])
                for role in (
                    "residual_dfl_reference",
                    "offline_dt_reference",
                    "residual_dt_fallback_reference",
                )
            }
        )
        candidate_family_collapse = challenger_family_count <= 1
        for challenger_role in CHALLENGER_ROLES:
            challenger_row = by_role[challenger_role]
            output_rows.append(
                _audit_row(
                    v2_row=v2_row,
                    strict_row=strict_row,
                    challenger_row=challenger_row,
                    challenger_role=challenger_role,
                    candidate_family_collapse=candidate_family_collapse,
                )
            )
    if not output_rows:
        return pl.DataFrame()
    return pl.DataFrame(output_rows).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp", "analysis_only_challenger_role"]
    )


def validate_dfl_official_v2_plus_bridge_failure_audit_evidence(
    audit_frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate that the official bridge audit is analysis-only evidence."""

    failures: list[str] = []
    missing = sorted(REQUIRED_AUDIT_COLUMNS.difference(audit_frame.columns))
    if missing:
        failures.append(f"missing audit columns: {missing}")
    if audit_frame.height == 0:
        failures.append("audit frame is empty")
    if "not_full_dfl" in audit_frame.columns and set(
        audit_frame["not_full_dfl"].unique().to_list()
    ) != {True}:
        failures.append("claim boundary violation: not_full_dfl must be true")
    if "not_market_execution" in audit_frame.columns and set(
        audit_frame["not_market_execution"].unique().to_list()
    ) != {True}:
        failures.append("claim boundary violation: not_market_execution must be true")
    if "market_execution_enabled" in audit_frame.columns and set(
        audit_frame["market_execution_enabled"].unique().to_list()
    ) != {False}:
        failures.append("market execution must remain disabled")
    return EvidenceCheckOutcome(
        not failures,
        "Official V2+-teacher bridge failure audit is valid analysis-only evidence."
        if not failures
        else "; ".join(failures),
        {
            "row_count": audit_frame.height,
            "source_model_count": _n_unique(audit_frame, "source_model_name"),
            "tenant_count": _n_unique(audit_frame, "tenant_id"),
            "failure_modes": _unique_strings(audit_frame, "analysis_only_failure_mode"),
            "market_execution_enabled": False,
        },
    )


def _validate_bridge_input(bridge_frame: pl.DataFrame) -> None:
    missing = sorted(REQUIRED_BRIDGE_COLUMNS.difference(bridge_frame.columns))
    if missing:
        raise ValueError(f"missing bridge columns: {missing}")
    if bridge_frame.height == 0:
        raise ValueError("official V2+ bridge failure audit requires non-empty rows")
    if any(str(tier) != "thesis_grade" for tier in bridge_frame["data_quality_tier"]):
        raise ValueError("official V2+ bridge failure audit found non-thesis rows")
    if bridge_frame["safety_violation_count"].sum() != 0:
        raise ValueError("official V2+ bridge failure audit found safety violations")
    if set(bridge_frame["not_full_dfl"].unique().to_list()) != {True}:
        raise ValueError("official V2+ bridge failure audit claim boundary violation")
    if set(bridge_frame["not_market_execution"].unique().to_list()) != {True}:
        raise ValueError("official V2+ bridge failure audit claim boundary violation")
    for row in bridge_frame.iter_rows(named=True):
        payload = _payload(row)
        if payload.get("market_execution_enabled") is True:
            raise ValueError("official V2+ bridge failure audit claim boundary violation")
    for key, group_rows in _group_bridge_rows(list(bridge_frame.iter_rows(named=True))).items():
        roles = {str(row["selection_role"]) for row in group_rows}
        missing_roles = sorted(set(REQUIRED_BRIDGE_ROLES).difference(roles))
        if missing_roles:
            raise ValueError(f"missing required bridge role for {key}: {missing_roles}")


def _group_bridge_rows(
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str, datetime], list[dict[str, Any]]]:
    groups: dict[tuple[str, str, datetime], list[dict[str, Any]]] = {}
    for row in rows:
        key = (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"]),
        )
        groups.setdefault(key, []).append(row)
    return groups


def _audit_row(
    *,
    v2_row: dict[str, Any],
    strict_row: dict[str, Any],
    challenger_row: dict[str, Any],
    challenger_role: str,
    candidate_family_collapse: bool,
) -> dict[str, Any]:
    v2_regret = float(v2_row["regret_uah"])
    strict_regret = float(strict_row["regret_uah"])
    challenger_regret = float(challenger_row["regret_uah"])
    v2_recall = _actual_top_discharge_recall(v2_row)
    challenger_recall = _actual_top_discharge_recall(challenger_row)
    v2_terminal_proxy = _terminal_net_energy_proxy(v2_row)
    challenger_terminal_proxy = _terminal_net_energy_proxy(challenger_row)
    v2_throughput = float(v2_row["total_throughput_mwh"])
    challenger_throughput = float(challenger_row["total_throughput_mwh"])
    return {
        "tenant_id": str(v2_row["tenant_id"]),
        "source_model_name": str(v2_row["source_model_name"]),
        "anchor_timestamp": v2_row["anchor_timestamp"],
        "generated_at": challenger_row["generated_at"],
        "market_venue": str(v2_row["market_venue"]),
        "analysis_only_challenger_role": challenger_role,
        "analysis_only_v2_plus_regret_uah": v2_regret,
        "analysis_only_challenger_regret_uah": challenger_regret,
        "analysis_only_regret_delta_vs_v2_plus_uah": challenger_regret - v2_regret,
        "analysis_only_strict_regret_uah": strict_regret,
        "analysis_only_oracle_gap_delta_uah": challenger_regret - v2_regret,
        "analysis_only_v2_plus_candidate_family": _candidate_family(v2_row),
        "analysis_only_challenger_candidate_family": _candidate_family(challenger_row),
        "analysis_only_candidate_family_collapse": candidate_family_collapse,
        "analysis_only_v2_actual_top_discharge_recall": v2_recall,
        "analysis_only_challenger_actual_top_discharge_recall": challenger_recall,
        "analysis_only_v2_top_k_price_recall": _forecast_top_k_recall(v2_row),
        "analysis_only_challenger_top_k_price_recall": _forecast_top_k_recall(
            challenger_row
        ),
        "analysis_only_v2_terminal_net_energy_proxy_mwh": v2_terminal_proxy,
        "analysis_only_challenger_terminal_net_energy_proxy_mwh": (
            challenger_terminal_proxy
        ),
        "analysis_only_terminal_net_energy_delta_mwh": (
            challenger_terminal_proxy - v2_terminal_proxy
        ),
        "analysis_only_v2_throughput_mwh": v2_throughput,
        "analysis_only_challenger_throughput_mwh": challenger_throughput,
        "analysis_only_throughput_delta_mwh": challenger_throughput - v2_throughput,
        "analysis_only_failure_mode": _failure_mode(
            challenger_role=challenger_role,
            v2_regret=v2_regret,
            strict_regret=strict_regret,
            challenger_regret=challenger_regret,
            v2_recall=v2_recall,
            challenger_recall=challenger_recall,
            throughput_delta=challenger_throughput - v2_throughput,
            terminal_delta=challenger_terminal_proxy - v2_terminal_proxy,
            candidate_family_collapse=candidate_family_collapse,
        ),
        "claim_scope": DFL_OFFICIAL_V2_PLUS_BRIDGE_FAILURE_AUDIT_CLAIM_SCOPE,
        "academic_scope": DFL_OFFICIAL_V2_PLUS_BRIDGE_FAILURE_AUDIT_ACADEMIC_SCOPE,
        "market_execution_enabled": False,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _failure_mode(
    *,
    challenger_role: str,
    v2_regret: float,
    strict_regret: float,
    challenger_regret: float,
    v2_recall: float,
    challenger_recall: float,
    throughput_delta: float,
    terminal_delta: float,
    candidate_family_collapse: bool,
) -> str:
    if v2_regret > strict_regret:
        return "bad_teacher_target"
    if challenger_role == "filtered_behavior_cloning_reference" and challenger_regret > v2_regret:
        return "dt_imitation_weaker_than_v2_selector"
    if candidate_family_collapse and challenger_regret > v2_regret:
        return "candidate_family_collapse"
    if challenger_regret > v2_regret and throughput_delta > 0.2:
        return "reward_scaling_issue"
    if challenger_regret > v2_regret and challenger_recall + 0.15 < v2_recall:
        return "horizon_credit_assignment_issue"
    if challenger_regret > v2_regret and abs(terminal_delta) > 0.4:
        return "horizon_credit_assignment_issue"
    return "weak_trajectory_objective"


def _candidate_family(row: dict[str, Any]) -> str:
    return str(_payload(row).get("candidate_family", "unknown"))


def _forecast_top_k_recall(row: dict[str, Any]) -> float:
    diagnostics = _payload(row).get("forecast_diagnostics", {})
    if not isinstance(diagnostics, dict):
        return 0.0
    return float(diagnostics.get("top_k_price_recall", 0.0))


def _actual_top_discharge_recall(row: dict[str, Any], *, top_k: int = 4) -> float:
    horizon = _horizon(row)
    if not horizon:
        return 0.0
    sorted_by_price = sorted(
        horizon,
        key=lambda step: float(step.get("actual_price_uah_mwh", 0.0)),
        reverse=True,
    )
    top_steps = {
        int(step.get("step_index", index))
        for index, step in enumerate(sorted_by_price[:top_k])
    }
    if not top_steps:
        return 0.0
    discharge_steps = {
        int(step.get("step_index", index))
        for index, step in enumerate(horizon)
        if float(step.get("net_power_mw", 0.0)) > 1e-6
    }
    return len(top_steps.intersection(discharge_steps)) / len(top_steps)


def _terminal_net_energy_proxy(row: dict[str, Any]) -> float:
    return sum(float(step.get("net_power_mw", 0.0)) for step in _horizon(row))


def _horizon(row: dict[str, Any]) -> list[dict[str, Any]]:
    horizon = _payload(row).get("horizon", [])
    if not isinstance(horizon, list):
        return []
    return [step for step in horizon if isinstance(step, dict)]


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload", {})
    if isinstance(payload, dict):
        return payload
    return {}


def _datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    raise TypeError(f"expected datetime value, got {type(value).__name__}")


def _n_unique(frame: pl.DataFrame, column: str) -> int:
    if column not in frame.columns or frame.height == 0:
        return 0
    return frame[column].n_unique()


def _unique_strings(frame: pl.DataFrame, column: str) -> list[str]:
    if column not in frame.columns or frame.height == 0:
        return []
    return sorted(str(value) for value in frame[column].unique().to_list())


__all__ = [
    "DFL_OFFICIAL_V2_PLUS_BRIDGE_FAILURE_AUDIT_CLAIM_SCOPE",
    "build_dfl_official_v2_plus_bridge_failure_audit_frame",
    "validate_dfl_official_v2_plus_bridge_failure_audit_evidence",
]
