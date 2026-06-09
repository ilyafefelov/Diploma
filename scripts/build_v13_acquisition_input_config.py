from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path
import sys
from types import ModuleType
from typing import Any, Callable, Sequence

import polars as pl
import yaml

from smart_arbitrage.dfl.ua_context_acquisition_v1 import (
    normalize_dfl_ua_dam_publication_receipts_frame,
)
from smart_arbitrage.dfl.ua_context_v13_acquisition import (
    normalize_dfl_ua_context_safe_switch_examples_v13_frame,
)


def _load_preflight_module() -> ModuleType:
    try:
        return importlib.import_module(
            "scripts.preflight_ua_context_v13_acquisition_inputs"
        )
    except ModuleNotFoundError:
        return importlib.import_module("preflight_ua_context_v13_acquisition_inputs")


v13_preflight = _load_preflight_module()
DAM_RECEIPT_CONFIG_KEY = str(v13_preflight.DAM_RECEIPT_CONFIG_KEY)
DAM_RECEIPT_OP_NAME = str(v13_preflight.DAM_RECEIPT_OP_NAME)
SAFE_SWITCH_CONFIG_KEY = str(v13_preflight.SAFE_SWITCH_CONFIG_KEY)
SAFE_SWITCH_OP_NAME = str(v13_preflight.SAFE_SWITCH_OP_NAME)
validate_v13_acquisition_inputs: Callable[[Path], dict[str, Any]] = (
    v13_preflight.validate_v13_acquisition_inputs
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Write a V13 acquisition input config from already-normalized DAM "
            "receipt and safe-switch CSVs, then run the existing preflight. "
            "This does not evaluate the full V13 gate, start DT/LAVA, or "
            "enable market execution."
        )
    )
    parser.add_argument("--base-config", type=Path, required=True)
    parser.add_argument("--dam-receipts-csv", type=Path, default=None)
    parser.add_argument("--safe-switch-csv", type=Path, default=None)
    parser.add_argument("--output-config", type=Path, required=True)
    parser.add_argument("--preflight-output", type=Path, default=None)
    args = parser.parse_args(argv)

    summary = build_v13_acquisition_input_config(
        base_config_path=args.base_config,
        dam_receipts_csv_path=args.dam_receipts_csv,
        safe_switch_csv_path=args.safe_switch_csv,
        output_config_path=args.output_config,
        preflight_output_path=args.preflight_output,
    )
    json.dump(summary, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


def build_v13_acquisition_input_config(
    *,
    base_config_path: Path,
    dam_receipts_csv_path: Path | None,
    safe_switch_csv_path: Path | None,
    output_config_path: Path,
    preflight_output_path: Path | None = None,
) -> dict[str, Any]:
    if dam_receipts_csv_path is None and safe_switch_csv_path is None:
        raise ValueError(
            "At least one of dam_receipts_csv_path or safe_switch_csv_path is required."
        )
    receipt_frame = (
        normalize_dfl_ua_dam_publication_receipts_frame(
            pl.read_csv(dam_receipts_csv_path, try_parse_dates=True)
        )
        if dam_receipts_csv_path is not None
        else pl.DataFrame()
    )
    safe_switch_frame = (
        normalize_dfl_ua_context_safe_switch_examples_v13_frame(
            pl.read_csv(safe_switch_csv_path, try_parse_dates=True)
        )
        if safe_switch_csv_path is not None
        else pl.DataFrame()
    )
    config_payload = _load_yaml_mapping(base_config_path)
    if dam_receipts_csv_path is not None:
        _set_op_config_path(
            config_payload,
            op_name=DAM_RECEIPT_OP_NAME,
            config_key=DAM_RECEIPT_CONFIG_KEY,
            csv_path=dam_receipts_csv_path,
        )
    if safe_switch_csv_path is not None:
        _set_op_config_path(
            config_payload,
            op_name=SAFE_SWITCH_OP_NAME,
            config_key=SAFE_SWITCH_CONFIG_KEY,
            csv_path=safe_switch_csv_path,
        )

    output_config_path.parent.mkdir(parents=True, exist_ok=True)
    output_config_path.write_text(
        yaml.safe_dump(config_payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    preflight_summary = validate_v13_acquisition_inputs(output_config_path)
    if preflight_output_path is not None:
        preflight_output_path.parent.mkdir(parents=True, exist_ok=True)
        preflight_output_path.write_text(
            json.dumps(preflight_summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    return {
        "claim_boundary": "v13_source_readiness_only_not_market_execution",
        "base_config_path": str(base_config_path),
        "output_config_path": str(output_config_path),
        "preflight_output_path": (
            str(preflight_output_path) if preflight_output_path is not None else None
        ),
        "dam_receipts_csv_path": (
            str(dam_receipts_csv_path) if dam_receipts_csv_path is not None else None
        ),
        "safe_switch_csv_path": (
            str(safe_switch_csv_path) if safe_switch_csv_path is not None else None
        ),
        "input_config_validated": preflight_summary["missing_required_inputs"] == [],
        "data_acquisition_needed": bool(preflight_summary["data_acquisition_needed"]),
        "receipt_rows": receipt_frame.height,
        "safe_switch_example_rows": safe_switch_frame.height,
        "tenant_source_count": _tenant_source_count(safe_switch_frame),
        "full_v13_gate_evaluated": False,
        "v13_candidate_generation_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "preflight_summary": preflight_summary,
    }


def _load_yaml_mapping(config_path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"V13 base config must be a YAML mapping: {config_path}")
    return loaded


def _set_op_config_path(
    payload: dict[str, Any],
    *,
    op_name: str,
    config_key: str,
    csv_path: Path,
) -> None:
    ops = payload.setdefault("ops", {})
    if not isinstance(ops, dict):
        raise ValueError("V13 base config field 'ops' must be a mapping.")
    op_config = ops.setdefault(op_name, {})
    if not isinstance(op_config, dict):
        raise ValueError(f"V13 base config op {op_name!r} must be a mapping.")
    config = op_config.setdefault("config", {})
    if not isinstance(config, dict):
        raise ValueError(f"V13 base config op {op_name!r}.config must be a mapping.")
    config[config_key] = str(csv_path)


def _tenant_source_count(frame: pl.DataFrame) -> int:
    if frame.is_empty():
        return 0
    return frame.select(["tenant_id", "source_model_name"]).unique().height


if __name__ == "__main__":
    raise SystemExit(main())
