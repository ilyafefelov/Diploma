from __future__ import annotations

import json

import polars as pl

from smart_arbitrage.dfl.energy_map_dam_receipt_metadata import (
    build_energy_map_dam_receipt_metadata_leads_v13_frame,
)
from smart_arbitrage.dfl.ua_context_v13_receipt_lead_audit import (
    audit_dfl_ua_context_dam_receipt_source_leads_v13_frame,
)


def test_v13_receipt_lead_audit_keeps_dataset_metadata_only_insufficient() -> None:
    frame = pl.DataFrame(
        [
            _lead(
                lead_id="energy_map_dam_dataset",
                metadata_scope="dataset_level",
                has_timestamp_column=True,
                has_source_publication_timestamp_column=False,
                dataset_last_updated_timestamp="2026-05-20T00:00:00",
            )
        ]
    )

    audit = audit_dfl_ua_context_dam_receipt_source_leads_v13_frame(frame)

    assert audit["claim_scope"] == "v13_dam_receipt_source_lead_audit_not_market_execution"
    assert audit["lead_count"] == 1
    assert audit["dataset_level_metadata_only_count"] == 1
    assert audit["candidate_receipt_source_found"] is False
    assert audit["receipt_csv_generated"] is False
    assert audit["validated_receipt_csv_ready"] is False
    assert audit["blocking_reasons"] == ["dataset_level_metadata_only"]
    assert audit["market_execution_enabled"] is False


def test_v13_receipt_lead_audit_blocks_auth_required_api_leads() -> None:
    frame = pl.DataFrame(
        [
            _lead(
                lead_id="energy_map_download_api",
                metadata_scope="row_level",
                has_timestamp_column=True,
                has_source_publication_timestamp_column=True,
                download_auth_required=True,
            )
        ]
    )

    audit = audit_dfl_ua_context_dam_receipt_source_leads_v13_frame(frame)

    assert audit["candidate_receipt_source_found"] is False
    assert audit["auth_blocked_count"] == 1
    assert audit["blocking_reasons"] == ["download_auth_required"]
    assert audit["lead_rows"][0]["lead_status"] == "blocked_auth_required"
    assert audit["market_execution_enabled"] is False


def test_v13_receipt_lead_audit_preserves_signed_ws_security_probe_status() -> None:
    frame = pl.DataFrame(
        [
            {
                **_lead(
                    lead_id="scmo_signed_probe",
                    metadata_scope="row_level",
                    has_timestamp_column=True,
                    has_source_publication_timestamp_column=False,
                    download_auth_required=True,
                    source_probe_status="credential_material_not_ready",
                ),
                "credential_mode": "preflight-gated-signed-ws-security",
                "signed_download_request_ready": False,
                "credential_material_format": "pkcs12",
                "mtls_client_cert_ready": False,
                "ws_security_signature_supported": True,
                "ws_security_signature_status": "xml_signature_builder_available",
                "ws_security_signature_applied": False,
            }
        ]
    )

    audit = audit_dfl_ua_context_dam_receipt_source_leads_v13_frame(frame)

    assert audit["candidate_receipt_source_found"] is False
    assert audit["auth_blocked_count"] == 1
    lead = audit["lead_rows"][0]
    assert lead["lead_status"] == "blocked_auth_required"
    assert lead["credential_mode"] == "preflight-gated-signed-ws-security"
    assert lead["signed_download_request_ready"] is False
    assert lead["credential_material_format"] == "pkcs12"
    assert lead["mtls_client_cert_ready"] is False
    assert lead["ws_security_signature_supported"] is True
    assert lead["ws_security_signature_status"] == "xml_signature_builder_available"
    assert lead["ws_security_signature_applied"] is False
    assert audit["market_execution_enabled"] is False


def test_v13_receipt_lead_audit_surfaces_row_level_candidate_without_csv_ready() -> None:
    frame = pl.DataFrame(
        [
            _lead(
                lead_id="manual_oree_receipt_export",
                metadata_scope="row_level",
                has_timestamp_column=True,
                has_source_publication_timestamp_column=True,
            )
        ]
    )

    audit = audit_dfl_ua_context_dam_receipt_source_leads_v13_frame(frame)

    assert audit["candidate_receipt_source_found"] is True
    assert audit["candidate_receipt_lead_count"] == 1
    assert audit["candidate_receipt_lead_ids"] == ["manual_oree_receipt_export"]
    assert audit["receipt_csv_generated"] is False
    assert audit["validated_receipt_csv_ready"] is False
    assert audit["blocking_reasons"] == ["receipt_csv_not_generated"]
    assert audit["dt_lava_ready"] is False
    assert audit["permits_model_training"] is False
    assert audit["market_execution_enabled"] is False


def test_v13_receipt_lead_audit_rejects_market_execution_claim() -> None:
    frame = pl.DataFrame(
        [
            _lead(
                lead_id="unsafe_lead",
                metadata_scope="row_level",
                has_timestamp_column=True,
                has_source_publication_timestamp_column=True,
                market_execution_enabled=True,
            )
        ]
    )

    audit = audit_dfl_ua_context_dam_receipt_source_leads_v13_frame(frame)

    assert audit["candidate_receipt_source_found"] is False
    assert audit["blocking_reasons"] == ["source_contains_market_execution_rows"]
    assert audit["lead_rows"][0]["lead_status"] == "blocked_market_execution_claim"
    assert audit["market_execution_enabled"] is False


def test_v13_receipt_lead_audit_cli_writes_summary(tmp_path) -> None:
    from scripts.audit_v13_dam_receipt_source_leads import main

    input_path = tmp_path / "receipt_leads.csv"
    output_path = tmp_path / "audit.json"
    pl.DataFrame(
        [
            _lead(
                lead_id="manual_oree_receipt_export",
                metadata_scope="row_level",
                has_timestamp_column=True,
                has_source_publication_timestamp_column=True,
            )
        ]
    ).write_csv(input_path)

    exit_code = main(["--input", str(input_path), "--output", str(output_path)])

    assert exit_code == 0
    audit = json.loads(output_path.read_text(encoding="utf-8"))
    assert audit["candidate_receipt_source_found"] is True
    assert audit["receipt_csv_generated"] is False
    assert audit["market_execution_enabled"] is False


def test_energy_map_metadata_probe_builds_file_level_leads_without_receipt_ready() -> None:
    frame = build_energy_map_dam_receipt_metadata_leads_v13_frame(
        [
            {
                "uuid": "5a616fba-fbc9-4073-9532-9161592faca8",
                "title": "DAM trading results",
                "fieldsDesc": [
                    ["date", "Day of supply", ""],
                    ["hour", "Hour", ""],
                    ["price", "Price, UAH/MWh", ""],
                ],
                "files": [
                    {
                        "filename": "2026_05_24_en_rezultaty_torhiv_rdn.csv",
                        "updated": {
                            "type": "time",
                            "time": "2026-05-24T01:46:39.298Z",
                        },
                        "rows": 83737,
                        "size": {"type": "size", "size": 8090095},
                        "format": ["EN", "CSV"],
                    }
                ],
            }
        ],
        locale="en",
    )

    assert frame.height == 1
    assert frame["metadata_scope"].to_list() == ["dataset_level"]
    assert frame["source_probe_status"].to_list() == [
        "file_level_publication_metadata_only"
    ]
    assert frame["has_timestamp_column"].to_list() == [True]
    assert frame["has_source_publication_timestamp_column"].to_list() == [False]
    assert frame["dataset_last_updated_timestamp"].to_list() == [
        "2026-05-24T01:46:39.298Z"
    ]
    assert frame["market_execution_enabled"].to_list() == [False]

    audit = audit_dfl_ua_context_dam_receipt_source_leads_v13_frame(frame)
    assert audit["candidate_receipt_source_found"] is False
    assert audit["dataset_level_metadata_only_count"] == 1
    assert audit["receipt_csv_generated"] is False
    assert audit["validated_receipt_csv_ready"] is False
    assert audit["permits_model_training"] is False
    assert audit["market_execution_enabled"] is False


def test_energy_map_metadata_probe_cli_writes_non_promotional_outputs(tmp_path) -> None:
    from scripts.probe_energy_map_dam_receipt_metadata import main

    input_path = tmp_path / "energy_map_payload.json"
    output_path = tmp_path / "energy_map_leads.csv"
    summary_path = tmp_path / "energy_map_summary.json"
    input_path.write_text(
        json.dumps(
            {
                "uuid": "5a616fba-fbc9-4073-9532-9161592faca8",
                "title": "DAM trading results",
                "fieldsDesc": [["date", "Day of supply", ""]],
                "files": [
                    {
                        "filename": "2026_05_24_en_rezultaty_torhiv_rdn.csv",
                        "updated": {
                            "type": "time",
                            "time": "2026-05-24T01:46:39.298Z",
                        },
                        "rows": 83737,
                        "size": {"type": "size", "size": 8090095},
                        "format": ["EN", "CSV"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--input-json",
            str(input_path),
            "--output-csv",
            str(output_path),
            "--summary-json",
            str(summary_path),
        ]
    )

    assert exit_code == 0
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["lead_rows"] == 1
    assert summary["candidate_receipt_source_found"] is False
    assert summary["receipt_csv_generated"] is False
    assert summary["validated_receipt_csv_ready"] is False
    assert summary["permits_model_training"] is False
    assert summary["market_execution_enabled"] is False


def _lead(
    *,
    lead_id: str,
    metadata_scope: str,
    has_timestamp_column: bool,
    has_source_publication_timestamp_column: bool,
    download_auth_required: bool = False,
    source_probe_status: str = "candidate_receipt_metadata_present",
    dataset_last_updated_timestamp: str = "",
    market_execution_enabled: bool = False,
) -> dict[str, object]:
    return {
        "lead_id": lead_id,
        "source_url": f"https://example.test/{lead_id}",
        "source_title": lead_id.replace("_", " "),
        "lead_kind": "dataset_or_api",
        "metadata_scope": metadata_scope,
        "has_timestamp_column": has_timestamp_column,
        "has_source_publication_timestamp_column": has_source_publication_timestamp_column,
        "download_auth_required": download_auth_required,
        "source_probe_status": source_probe_status,
        "dataset_last_updated_timestamp": dataset_last_updated_timestamp,
        "market_execution_enabled": market_execution_enabled,
    }
