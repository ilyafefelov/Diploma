"""Audits legacy DT artifacts before they are cited by the v1.3 evidence release."""

from __future__ import annotations

from typing import Final

from smart_arbitrage.dfl.dt_research_shadow import STATE_FEATURE_NAMES


_OUTCOME_DERIVED_STATE_FEATURES: Final[tuple[str, ...]] = (
    "forecast_top_k_actual_overlap",
    "forecast_bottom_k_actual_overlap",
    "schedule_value_scaled",
    "regret_delta_vs_v2_plus_scaled",
    "return_to_go_scaled",
)
_LEGACY_SEQUENCE_GROUP_KEYS: Final[tuple[str, ...]] = (
    "tenant_id",
    "source_model_name",
    "anchor_timestamp",
)


def audit_legacy_temporal_dt_contract() -> dict[str, object]:
    """Return the v1.3 correction for the legacy candidate-list DT packet.

    The legacy packet grouped candidate rows within one anchor and exposed
    realized-outcome fields in the state tensor. It is therefore retained only
    as a non-causal engineering diagnostic, not temporal DT policy evidence.
    """

    state_features = set(STATE_FEATURE_NAMES)
    outcome_features = [
        feature
        for feature in _OUTCOME_DERIVED_STATE_FEATURES
        if feature in state_features
    ]
    candidate_index_in_state = "candidate_index_scaled" in state_features
    candidate_list_tokens = _LEGACY_SEQUENCE_GROUP_KEYS[-1] == "anchor_timestamp"
    return {
        "legacy_sequence_group_keys": list(_LEGACY_SEQUENCE_GROUP_KEYS),
        "legacy_packet_is_time_ordered_trajectory": False,
        "legacy_packet_uses_candidate_list_tokens": candidate_list_tokens,
        "outcome_derived_state_features": outcome_features,
        "action_target_appears_in_state": candidate_index_in_state,
        "retract_temporal_dt_policy_claim": bool(
            candidate_list_tokens and (outcome_features or candidate_index_in_state)
        ),
        "corrected_claim_scope": (
            "legacy_candidate_list_diagnostic_not_causal_dt_policy_evidence"
        ),
        "market_execution_enabled": False,
    }
