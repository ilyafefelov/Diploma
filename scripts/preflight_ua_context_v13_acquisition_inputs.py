from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

import polars as pl
import yaml

from smart_arbitrage.dfl.ua_context_acquisition_v1 import (
    normalize_dfl_ua_dam_publication_receipts_frame,
)
from smart_arbitrage.dfl.ua_context_v13_acquisition import (
    normalize_dfl_ua_context_safe_switch_examples_v13_frame,
)

DAM_RECEIPT_OP_NAME = "dfl_ua_dam_publication_receipts_overlay_frame"
DAM_RECEIPT_CONFIG_KEY = "oree_dam_publication_receipts_csv_path"
SAFE_SWITCH_OP_NAME = "dfl_ua_context_safe_switch_examples_v13_frame"
SAFE_SWITCH_CONFIG_KEY = "ua_context_safe_switch_examples_csv_path"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Preflight the optional V13 DAM receipt and safe-switch acquisition "
            "input CSVs configured in a Dagster run config."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Dagster run config YAML for the V13 acquisition gate.",
    )
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    summary = validate_v13_acquisition_inputs(args.config)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    json.dump(summary, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


def validate_v13_acquisition_inputs(config_path: Path) -> dict[str, Any]:
    payload = _load_yaml_mapping(config_path)
    dam_path = _configured_csv_path(
        payload,
        op_name=DAM_RECEIPT_OP_NAME,
        config_key=DAM_RECEIPT_CONFIG_KEY,
    )
    safe_switch_path = _configured_csv_path(
        payload,
        op_name=SAFE_SWITCH_OP_NAME,
        config_key=SAFE_SWITCH_CONFIG_KEY,
    )

    dam_summary = _dam_receipt_summary(dam_path)
    safe_switch_summary = _safe_switch_summary(safe_switch_path)
    missing_required_inputs = [
        config_key
        for config_key, section in (
            (DAM_RECEIPT_CONFIG_KEY, dam_summary),
            (SAFE_SWITCH_CONFIG_KEY, safe_switch_summary),
        )
        if section["status"] == "missing_config_path"
    ]
    return {
        "claim_boundary": "v13_source_readiness_only_not_market_execution",
        "config_path": str(config_path),
        "dam_publication_receipts": dam_summary,
        "data_acquisition_needed": bool(missing_required_inputs),
        "dt_lava_ready": False,
        "full_v13_gate_evaluated": False,
        "market_execution_enabled": False,
        "missing_required_inputs": missing_required_inputs,
        "permits_model_training": False,
        "safe_switch_examples": safe_switch_summary,
        "v13_candidate_generation_ready": False,
    }


def _load_yaml_mapping(config_path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"V13 config must be a YAML mapping: {config_path}")
    return loaded


def _configured_csv_path(
    payload: dict[str, Any],
    *,
    op_name: str,
    config_key: str,
) -> Path | None:
    ops = payload.get("ops", {})
    if not isinstance(ops, dict):
        return None
    op_config = ops.get(op_name, {})
    if not isinstance(op_config, dict):
        return None
    config = op_config.get("config", {})
    if not isinstance(config, dict):
        return None
    raw_path = config.get(config_key, "")
    if raw_path is None:
        return None
    cleaned_path = str(raw_path).strip()
    if not cleaned_path:
        return None
    return Path(cleaned_path)


def _dam_receipt_summary(csv_path: Path | None) -> dict[str, Any]:
    if csv_path is None:
        return _missing_summary(
            required_columns=["timestamp", "source_publication_timestamp"]
        )
    frame = normalize_dfl_ua_dam_publication_receipts_frame(
        pl.read_csv(csv_path, try_parse_dates=True)
    )
    return {
        "configured": True,
        "first_timestamp": _iso_at(frame, "timestamp", 0),
        "last_timestamp": _iso_at(frame, "timestamp", -1),
        "path": str(csv_path),
        "receipt_rows": frame.height,
        "required_columns": ["timestamp", "source_publication_timestamp"],
        "status": "validated",
    }


def _safe_switch_summary(csv_path: Path | None) -> dict[str, Any]:
    if csv_path is None:
        return _missing_summary(
            required_columns=[
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "split_name",
                "source_evidence_timestamp",
                "label_v13_material_safe_switch",
                "label_v13_tail_risk_loss",
            ]
        )
    frame = normalize_dfl_ua_context_safe_switch_examples_v13_frame(
        pl.read_csv(csv_path, try_parse_dates=True)
    )
    tenant_source_count = (
        frame.select(["tenant_id", "source_model_name"]).unique().height
        if frame.height
        else 0
    )
    return {
        "configured": True,
        "first_anchor_timestamp": _iso_at(frame, "anchor_timestamp", 0),
        "last_anchor_timestamp": _iso_at(frame, "anchor_timestamp", -1),
        "path": str(csv_path),
        "required_columns": [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "split_name",
            "source_evidence_timestamp",
            "label_v13_material_safe_switch",
            "label_v13_tail_risk_loss",
        ],
        "safe_switch_example_rows": frame.height,
        "status": "validated",
        "tenant_source_count": tenant_source_count,
    }


def _missing_summary(*, required_columns: list[str]) -> dict[str, Any]:
    return {
        "configured": False,
        "path": None,
        "required_columns": required_columns,
        "status": "missing_config_path",
    }


def _iso_at(frame: pl.DataFrame, column_name: str, index: int) -> str | None:
    if frame.is_empty():
        return None
    value = frame[column_name].item(index)
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


if __name__ == "__main__":
    main()
