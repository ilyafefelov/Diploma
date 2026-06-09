"""Materialize the credentialless academic MVP readiness packet."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for import_path in (PROJECT_ROOT, SRC_ROOT):
    import_path_text = str(import_path)
    if import_path_text not in sys.path:
        sys.path.insert(0, import_path_text)

from smart_arbitrage.dfl.credentialless_academic_mvp_readiness import (  # noqa: E402
    SUMMARY_JSON_NAME,
    SUMMARY_MARKDOWN_NAME,
    VALIDATION_JSON_NAME,
    write_credentialless_academic_mvp_readiness_packet,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Write a credentialless academic MVP readiness packet. This proves "
            "operator-preview and DT/LAVA prototype evidence without treating "
            "SCMO credentials as required for the diploma MVP. The packet keeps "
            "market_execution_enabled=false and does not emit ProposedBid or "
            "market-order payloads. It requires the Phase 2 teacher-packet "
            "validation JSON to pass before the V13-gated teacher-contract "
            "gate can pass, and the Phase 3 offline-challenger validation "
            "JSON to pass before the non-promotion gate can pass. The JSON artifact is "
            f"{SUMMARY_JSON_NAME}; the Markdown artifact is {SUMMARY_MARKDOWN_NAME}; "
            f"the validation artifact is {VALIDATION_JSON_NAME}."
        )
    )
    parser.add_argument(
        "--operator-preview-json",
        type=Path,
        default=None,
        help=(
            "Operator recommendation/baseline preview JSON. If omitted, use "
            "--tenant-id to build the local FastAPI read model in-process."
        ),
    )
    parser.add_argument("--tenant-id", default=None)
    parser.add_argument("--strategy-id", default="strict_similar_day")
    parser.add_argument("--v13-acquisition-summary-json", type=Path, required=True)
    parser.add_argument("--dt-lava-prototype-readiness-json", type=Path, required=True)
    parser.add_argument("--teacher-summary-json", type=Path, required=True)
    parser.add_argument("--teacher-validation-json", type=Path, required=True)
    parser.add_argument("--offline-challenger-summary-json", type=Path, required=True)
    parser.add_argument("--offline-challenger-validation-json", type=Path, required=True)
    parser.add_argument("--dt-research-shadow-sequence-summary-json", type=Path, default=None)
    parser.add_argument("--dt-research-shadow-smoke-summary-json", type=Path, default=None)
    parser.add_argument(
        "--dt-research-shadow-evaluation-validation-json",
        type=Path,
        default=None,
        help=(
            "DT research-shadow evaluation validation sidecar. If omitted while "
            "--dt-research-shadow-smoke-summary-json is provided, infer "
            "dt_research_shadow_evaluation_validation.json from the same directory."
        ),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    dt_evaluation_validation_path = _dt_research_shadow_evaluation_validation_path(args)

    packet = write_credentialless_academic_mvp_readiness_packet(
        output_dir=args.output_dir,
        operator_preview=_load_operator_preview(args),
        v13_acquisition_summary=_load_json_object(args.v13_acquisition_summary_json),
        dt_lava_prototype_readiness=_load_json_object(
            args.dt_lava_prototype_readiness_json
        ),
        teacher_summary=_load_json_object(args.teacher_summary_json),
        teacher_validation=_load_json_object(args.teacher_validation_json),
        offline_challenger_summary=_load_json_object(
            args.offline_challenger_summary_json
        ),
        offline_challenger_validation=_load_json_object(
            args.offline_challenger_validation_json
        ),
        dt_research_shadow_sequence_summary=(
            _load_json_object(args.dt_research_shadow_sequence_summary_json)
            if args.dt_research_shadow_sequence_summary_json is not None
            else None
        ),
        dt_research_shadow_smoke_summary=(
            _load_json_object(args.dt_research_shadow_smoke_summary_json)
            if args.dt_research_shadow_smoke_summary_json is not None
            else None
        ),
        dt_research_shadow_evaluation_validation=(
            _load_json_object(dt_evaluation_validation_path)
            if dt_evaluation_validation_path is not None
            else None
        ),
    )
    print(
        "Wrote credentialless academic MVP readiness packet: "
        f"{packet['summary_json']}, {packet['summary_markdown']}, "
        f"and {packet['validation_json']}"
    )
    return 0


def _dt_research_shadow_evaluation_validation_path(
    args: argparse.Namespace,
) -> Path | None:
    if args.dt_research_shadow_evaluation_validation_json is not None:
        return args.dt_research_shadow_evaluation_validation_json
    if args.dt_research_shadow_smoke_summary_json is None:
        return None
    inferred = (
        args.dt_research_shadow_smoke_summary_json.parent
        / "dt_research_shadow_evaluation_validation.json"
    )
    if not inferred.exists():
        raise ValueError(
            "DT research-shadow smoke summary was provided but evaluation "
            f"validation sidecar is missing: {inferred}"
        )
    return inferred


def _load_operator_preview(args: argparse.Namespace) -> dict[str, Any]:
    if args.operator_preview_json is not None:
        return _load_json_object(args.operator_preview_json)
    if args.tenant_id is None or not str(args.tenant_id).strip():
        raise ValueError("--operator-preview-json or --tenant-id is required.")

    from api.main import _build_operator_recommendation_response  # noqa: PLC0415

    response = _build_operator_recommendation_response(
        tenant_id=args.tenant_id,
        strategy_id=args.strategy_id,
    )
    return response.model_dump(mode="json")


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    if _contains_market_execution_enabled_true(payload):
        raise ValueError(f"Input must keep market_execution_enabled=false: {path}")
    return payload


def _contains_market_execution_enabled_true(value: object) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "market_execution_enabled" and bool(item):
                return True
            if _contains_market_execution_enabled_true(item):
                return True
        return False
    if isinstance(value, list | tuple):
        return any(_contains_market_execution_enabled_true(item) for item in value)
    return False


if __name__ == "__main__":
    raise SystemExit(main())
