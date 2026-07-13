from __future__ import annotations

import json

from smart_arbitrage.dfl.v1_3_evidence_audit import (
    audit_legacy_temporal_dt_contract,
)
from scripts.audit_v1_3_evidence import main as audit_v1_3_evidence


def test_v1_3_audit_retracts_legacy_temporal_dt_as_causal_policy_evidence() -> None:
    audit = audit_legacy_temporal_dt_contract()

    assert audit["legacy_packet_is_time_ordered_trajectory"] is False
    assert audit["legacy_packet_uses_candidate_list_tokens"] is True
    assert audit["outcome_derived_state_features"] == [
        "forecast_top_k_actual_overlap",
        "forecast_bottom_k_actual_overlap",
        "schedule_value_scaled",
        "regret_delta_vs_v2_plus_scaled",
        "return_to_go_scaled",
    ]
    assert audit["action_target_appears_in_state"] is True
    assert audit["retract_temporal_dt_policy_claim"] is True
    assert audit["corrected_claim_scope"] == (
        "legacy_candidate_list_diagnostic_not_causal_dt_policy_evidence"
    )


def test_v1_3_audit_cli_writes_machine_readable_correction(tmp_path) -> None:
    output = tmp_path / "v1_3_evidence_audit.json"

    assert audit_v1_3_evidence(["--output", str(output)]) == 0

    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["retract_temporal_dt_policy_claim"] is True
    assert written["market_execution_enabled"] is False
