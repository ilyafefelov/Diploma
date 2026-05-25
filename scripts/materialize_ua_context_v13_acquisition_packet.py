from __future__ import annotations

import argparse
import json
from pathlib import Path
import pickle
import sys
from typing import Any, Mapping

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_acquisition_export import (
    UA_CONTEXT_V13_ACQUISITION_INPUT_PREFLIGHT_JSON_ARTIFACT_NAME,
    UA_CONTEXT_V13_RECEIPT_SOURCE_AUDIT_JSON_ARTIFACT_NAME,
    UA_CONTEXT_V13_RECEIPT_SOURCE_LEAD_AUDIT_JSON_ARTIFACT_NAME,
    UA_CONTEXT_V13_SAFE_SWITCH_CANDIDATE_AUDITS_JSON_ARTIFACT_NAME,
    UA_CONTEXT_V13_SCMO_WS_SECURITY_PREFLIGHT_JSON_ARTIFACT_NAME,
    UA_CONTEXT_V13_SOURCE_ACQUISITION_BACKLOG_CSV_ARTIFACT_NAME,
    build_dfl_ua_context_v13_acquisition_packet,
    write_dfl_ua_context_v13_acquisition_packet,
)

DEFAULT_RUN_SLUG = "week3_dfl_ua_context_acquisition_v13"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a V13 Ukrainian context acquisition readiness packet."
    )
    parser.add_argument("--source-evidence-pickle", type=Path, default=None)
    parser.add_argument("--source-evidence-csv", type=Path, default=None)
    parser.add_argument("--source-inventory-pickle", type=Path, default=None)
    parser.add_argument("--source-inventory-csv", type=Path, default=None)
    parser.add_argument("--readiness-pickle", type=Path, default=None)
    parser.add_argument("--readiness-csv", type=Path, default=None)
    parser.add_argument("--receipt-source-audit-json", type=Path, default=None)
    parser.add_argument("--receipt-source-lead-audit-json", type=Path, default=None)
    parser.add_argument(
        "--safe-switch-candidate-audit-json",
        action="append",
        default=[],
        type=Path,
    )
    parser.add_argument("--acquisition-input-preflight-json", type=Path, default=None)
    parser.add_argument("--scmo-ws-security-preflight-json", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=Path("data") / "research_runs")
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--dagster-run-id", default=None)
    parser.add_argument("--materialization-command", default=None)
    parser.add_argument("--asset-check-status", default=None)
    args = parser.parse_args()

    source_evidence = (
        _load_optional_frame(
            pickle_path=args.source_evidence_pickle,
            csv_path=args.source_evidence_csv,
            frame_name="source evidence",
        )
    )
    source_inventory = _load_required_frame(
        pickle_path=args.source_inventory_pickle,
        csv_path=args.source_inventory_csv,
        frame_name="source inventory",
    )
    readiness = _load_required_frame(
        pickle_path=args.readiness_pickle,
        csv_path=args.readiness_csv,
        frame_name="readiness",
    )
    receipt_source_audit = _load_optional_json(args.receipt_source_audit_json)
    receipt_source_lead_audit = _load_optional_json(
        args.receipt_source_lead_audit_json
    )
    safe_switch_candidate_audits = _load_json_list(
        args.safe_switch_candidate_audit_json
    )
    acquisition_input_preflight = _load_optional_json(
        args.acquisition_input_preflight_json
    )
    scmo_ws_security_preflight = _load_optional_json(
        args.scmo_ws_security_preflight_json
    )
    packet = build_dfl_ua_context_v13_acquisition_packet(
        run_slug=args.run_slug,
        source_inventory_frame=source_inventory,
        readiness_frame=readiness,
        acquisition_source_evidence_frame=source_evidence,
        receipt_source_audit=receipt_source_audit,
        receipt_source_lead_audit=receipt_source_lead_audit,
        safe_switch_candidate_audits=safe_switch_candidate_audits,
        acquisition_input_preflight=acquisition_input_preflight,
        scmo_ws_security_preflight=scmo_ws_security_preflight,
        dagster_run_id=args.dagster_run_id,
        materialization_command=args.materialization_command,
        asset_check_status=args.asset_check_status,
    )
    export_dir = write_dfl_ua_context_v13_acquisition_packet(
        packet,
        output_root=args.output_root,
        source_inventory_frame=source_inventory,
        readiness_frame=readiness,
        acquisition_source_evidence_frame=source_evidence,
        receipt_source_audit=receipt_source_audit,
        receipt_source_lead_audit=receipt_source_lead_audit,
        safe_switch_candidate_audits=safe_switch_candidate_audits,
        acquisition_input_preflight=acquisition_input_preflight,
        scmo_ws_security_preflight=scmo_ws_security_preflight,
    )
    json.dump(
        {
            "export_dir": str(export_dir),
            "summary_json": str(
                export_dir / "dfl_ua_context_v13_acquisition_summary.json"
            ),
            "summary_markdown": str(
                export_dir / "dfl_ua_context_v13_acquisition_summary.md"
            ),
            "v13_candidate_generation_ready": packet[
                "v13_candidate_generation_ready"
            ],
            "readiness_decisions": packet["readiness_summary"][
                "readiness_decisions"
            ],
            "safe_switch_deficit_summary": packet["safe_switch_deficit_summary"],
            "safe_switch_acquisition_target_summary": packet[
                "safe_switch_acquisition_target_summary"
            ],
            "source_acquisition_backlog_summary": packet[
                "source_acquisition_backlog_summary"
            ],
            "source_acquisition_backlog_csv": str(
                export_dir / UA_CONTEXT_V13_SOURCE_ACQUISITION_BACKLOG_CSV_ARTIFACT_NAME
            ),
            "safe_switch_primary_blocking_source_families": sorted(
                {
                    str(row["primary_blocking_source_family"])
                    for row in packet["safe_switch_acquisition_target_summary"][
                        "target_rows"
                    ]
                }
            ),
            "receipt_source_audit_summary": packet["receipt_source_audit_summary"],
            "receipt_source_audit_json": str(
                export_dir / UA_CONTEXT_V13_RECEIPT_SOURCE_AUDIT_JSON_ARTIFACT_NAME
            )
            if receipt_source_audit is not None
            else None,
            "receipt_source_lead_audit_summary": packet[
                "receipt_source_lead_audit_summary"
            ],
            "receipt_source_lead_audit_json": str(
                export_dir
                / UA_CONTEXT_V13_RECEIPT_SOURCE_LEAD_AUDIT_JSON_ARTIFACT_NAME
            )
            if receipt_source_lead_audit is not None
            else None,
            "safe_switch_candidate_audit_summary": packet[
                "safe_switch_candidate_audit_summary"
            ],
            "safe_switch_candidate_audits_json": str(
                export_dir
                / UA_CONTEXT_V13_SAFE_SWITCH_CANDIDATE_AUDITS_JSON_ARTIFACT_NAME
            )
            if safe_switch_candidate_audits is not None
            else None,
            "acquisition_input_preflight_summary": packet[
                "acquisition_input_preflight_summary"
            ],
            "acquisition_input_preflight_json": str(
                export_dir
                / UA_CONTEXT_V13_ACQUISITION_INPUT_PREFLIGHT_JSON_ARTIFACT_NAME
            )
            if acquisition_input_preflight is not None
            else None,
            "scmo_ws_security_preflight_summary": packet[
                "scmo_ws_security_preflight_summary"
            ],
            "scmo_ws_security_preflight_json": str(
                export_dir
                / UA_CONTEXT_V13_SCMO_WS_SECURITY_PREFLIGHT_JSON_ARTIFACT_NAME
            )
            if scmo_ws_security_preflight is not None
            else None,
            "market_execution_enabled": packet["claim_boundary"][
                "market_execution_enabled"
            ],
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")


def _load_polars_frame(path: Path) -> pl.DataFrame:
    with path.open("rb") as file:
        value = pickle.load(file)
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"{path} must contain a pickled Polars DataFrame.")
    return value


def _load_optional_frame(
    *,
    pickle_path: Path | None,
    csv_path: Path | None,
    frame_name: str,
) -> pl.DataFrame | None:
    if pickle_path is not None and csv_path is not None:
        raise ValueError(f"Provide only one {frame_name} input: pickle or CSV.")
    if pickle_path is not None:
        return _load_polars_frame(pickle_path)
    if csv_path is not None:
        return pl.read_csv(csv_path, try_parse_dates=True)
    return None


def _load_required_frame(
    *,
    pickle_path: Path | None,
    csv_path: Path | None,
    frame_name: str,
) -> pl.DataFrame:
    frame = _load_optional_frame(
        pickle_path=pickle_path,
        csv_path=csv_path,
        frame_name=frame_name,
    )
    if frame is None:
        raise ValueError(f"Missing required {frame_name} input.")
    return frame


def _load_optional_json(path: Path | None) -> dict[str, object] | None:
    if path is None:
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain a JSON object.")
    return {str(key): item for key, item in value.items()}


def _load_json_list(paths: list[Path]) -> list[Mapping[str, Any]] | None:
    if not paths:
        return None
    return [_load_required_json(path) for path in paths]


def _load_required_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain a JSON object.")
    return {str(key): item for key, item in value.items()}


if __name__ == "__main__":
    main()
