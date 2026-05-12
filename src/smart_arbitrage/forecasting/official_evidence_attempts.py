"""Execution metadata for resumable official forecast evidence attempts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast

OFFICIAL_EVIDENCE_ATTEMPT_CLAIM_SCOPE = "offline_strategy_promotion_evidence_attempt"

AnchorBatchOrder = Literal["chronological", "latest_first"]
OfficialEvidenceAttemptKind = Literal[
    "official_schedule_value",
    "official_global_panel_backfill",
]


@dataclass(frozen=True, slots=True)
class OfficialEvidenceAttemptConfig:
    """Configuration used to build a resumable evidence-attempt manifest."""

    attempt_kind: OfficialEvidenceAttemptKind
    generated_at_iso: str
    total_anchors: int
    batch_size: int
    asset_selection: str
    start_anchor_index: int = 0
    end_anchor_index: int = 0
    anchor_batch_order: AnchorBatchOrder = "chronological"
    enabled_official_models_csv: str = "nbeatsx_official_v0,tft_official_v0"
    nbeatsx_max_steps: int = 0
    tft_max_epochs: int = 0
    downstream_gate_enabled: bool = True
    downstream_selection: str = ""
    run_root: str = ".tmp_runtime/official_evidence_attempts"


def official_evidence_attempt_slug(
    attempt_kind: OfficialEvidenceAttemptKind,
    generated_at_iso: str,
) -> str:
    """Return the stable local/offload slug for an official evidence attempt."""

    if not generated_at_iso.strip():
        raise ValueError("generated_at_iso must not be blank.")
    normalized_kind = attempt_kind.replace("_", "-")
    normalized_timestamp = generated_at_iso.replace(":", "")
    normalized_timestamp = "".join(
        char if char.isalnum() or char in {"_", "-"} else "-"
        for char in normalized_timestamp
    )
    return f"{normalized_kind}-{normalized_timestamp}"


def build_official_evidence_attempt_manifest(
    config: OfficialEvidenceAttemptConfig,
) -> dict[str, object]:
    """Build metadata that makes official evidence attempts resumable and auditable."""

    _validate_config(config)
    end_anchor_index = _resolved_end_anchor_index(config)
    run_slug = official_evidence_attempt_slug(
        config.attempt_kind,
        config.generated_at_iso,
    )
    return {
        "manifest_version": 1,
        "attempt_kind": config.attempt_kind,
        "run_slug": run_slug,
        "resume_generated_at_iso": config.generated_at_iso,
        "anchor_batch_order": config.anchor_batch_order,
        "total_anchors": config.total_anchors,
        "start_anchor_index": config.start_anchor_index,
        "end_anchor_index": end_anchor_index,
        "batch_size": config.batch_size,
        "enabled_official_models_csv": config.enabled_official_models_csv,
        "nbeatsx_max_steps": config.nbeatsx_max_steps,
        "tft_max_epochs": config.tft_max_epochs,
        "asset_selection": config.asset_selection,
        "downstream_gate_enabled": config.downstream_gate_enabled,
        "downstream_selection": config.downstream_selection,
        "batch_plan": _build_batch_plan(
            start_anchor_index=config.start_anchor_index,
            end_anchor_index=end_anchor_index,
            batch_size=config.batch_size,
        ),
        "resume_policy": {
            "batch_identity": "anchor_batch_start_index",
            "row_identity": "strategy_kind + tenant_id + generated_at + anchor_timestamp + model_name",
            "next_anchor_index_rule": (
                "resume from the first missing or failed batch start index; "
                "keep resume_generated_at_iso unchanged"
            ),
            "completed_batch_signal": "Dagster RUN_SUCCESS plus persisted rows for the fixed generated_at",
        },
        "claim_boundary": {
            "offline_strategy_promotion_language": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "claim_scope": OFFICIAL_EVIDENCE_ATTEMPT_CLAIM_SCOPE,
        },
        "local_run_dir": f"{config.run_root.rstrip('/')}/{run_slug}",
    }


def summarize_official_evidence_attempt_resume(
    manifest: Mapping[str, object],
    *,
    persisted_anchor_count: int | None = None,
    persisted_anchor_counts_by_source: Mapping[str, int] | None = None,
) -> dict[str, object]:
    """Summarize the next resume point from a manifest and persisted coverage."""

    effective_count = _effective_persisted_anchor_count(
        persisted_anchor_count=persisted_anchor_count,
        persisted_anchor_counts_by_source=persisted_anchor_counts_by_source,
    )
    batch_plan = _manifest_batch_plan(manifest)
    planned_anchor_count = sum(
        _batch_int(batch, "anchor_batch_size") for batch in batch_plan
    )
    if effective_count > planned_anchor_count:
        raise ValueError("persisted anchor count cannot exceed planned anchor count.")

    completed_batch_start_indices: list[int] = []
    cumulative_count = 0
    next_anchor_index: int | None = None
    for batch in batch_plan:
        batch_size = _batch_int(batch, "anchor_batch_size")
        batch_start = _batch_int(batch, "anchor_batch_start_index")
        batch_end_count = cumulative_count + batch_size
        if effective_count >= batch_end_count:
            completed_batch_start_indices.append(batch_start)
        elif next_anchor_index is None:
            next_anchor_index = batch_start
        cumulative_count = batch_end_count

    status = "complete" if next_anchor_index is None else "resume_required"
    return {
        "status": status,
        "run_slug": str(manifest.get("run_slug", "")),
        "resume_generated_at_iso": str(manifest.get("resume_generated_at_iso", "")),
        "effective_persisted_anchor_count": effective_count,
        "planned_anchor_count": planned_anchor_count,
        "next_anchor_index": next_anchor_index,
        "completed_batch_start_indices": completed_batch_start_indices,
        "claim_boundary": manifest.get("claim_boundary", {}),
        "resume_policy": manifest.get("resume_policy", {}),
    }


def _validate_config(config: OfficialEvidenceAttemptConfig) -> None:
    if config.total_anchors <= 0:
        raise ValueError("total_anchors must be positive.")
    if config.batch_size <= 0:
        raise ValueError("batch_size must be positive.")
    if config.start_anchor_index < 0:
        raise ValueError("start_anchor_index must be non-negative.")
    if config.end_anchor_index < 0:
        raise ValueError("end_anchor_index must be non-negative.")
    if _resolved_end_anchor_index(config) <= config.start_anchor_index:
        raise ValueError("end_anchor_index must be greater than start_anchor_index.")
    if _resolved_end_anchor_index(config) > config.total_anchors:
        raise ValueError("end_anchor_index cannot exceed total_anchors.")
    if not config.asset_selection.strip():
        raise ValueError("asset_selection must not be blank.")
    if not config.enabled_official_models_csv.strip():
        raise ValueError("enabled_official_models_csv must not be blank.")
    if config.nbeatsx_max_steps < 0:
        raise ValueError("nbeatsx_max_steps must not be negative.")
    if config.tft_max_epochs < 0:
        raise ValueError("tft_max_epochs must not be negative.")


def _resolved_end_anchor_index(config: OfficialEvidenceAttemptConfig) -> int:
    if config.end_anchor_index > 0:
        return config.end_anchor_index
    return config.total_anchors


def _build_batch_plan(
    *,
    start_anchor_index: int,
    end_anchor_index: int,
    batch_size: int,
) -> list[dict[str, int | str]]:
    batch_plan: list[dict[str, int | str]] = []
    for anchor_index in range(start_anchor_index, end_anchor_index, batch_size):
        batch_end = min(anchor_index + batch_size, end_anchor_index)
        batch_plan.append(
            {
                "batch_id": f"anchor-{anchor_index}",
                "anchor_batch_start_index": anchor_index,
                "anchor_batch_size": batch_end - anchor_index,
                "anchor_batch_end_index_exclusive": batch_end,
            }
        )
    return batch_plan


def _effective_persisted_anchor_count(
    *,
    persisted_anchor_count: int | None,
    persisted_anchor_counts_by_source: Mapping[str, int] | None,
) -> int:
    if persisted_anchor_counts_by_source is not None:
        if not persisted_anchor_counts_by_source:
            raise ValueError("persisted_anchor_counts_by_source must not be empty.")
        count = min(persisted_anchor_counts_by_source.values())
    elif persisted_anchor_count is not None:
        count = persisted_anchor_count
    else:
        raise ValueError("persisted anchor coverage must be provided.")
    if count < 0:
        raise ValueError("persisted anchor count must be non-negative.")
    return count


def _manifest_batch_plan(
    manifest: Mapping[str, object],
) -> list[Mapping[str, Any]]:
    batch_plan = manifest.get("batch_plan")
    if not isinstance(batch_plan, list) or not batch_plan:
        raise ValueError("manifest batch_plan must be a non-empty list.")
    return [cast(Mapping[str, Any], batch) for batch in batch_plan]


def _batch_int(batch: Mapping[str, Any], key: str) -> int:
    value = batch.get(key)
    if not isinstance(value, int):
        raise ValueError(f"manifest batch_plan field {key} must be an integer.")
    return value
