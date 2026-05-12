"""Thesis-safe language helpers for offline strategy promotion evidence."""

from __future__ import annotations

from typing import Any, Final

import polars as pl

OFFLINE_STRATEGY_PROMOTION_DISPLAY_NAME: Final[str] = "Offline Strategy Promotion"
OFFLINE_STRATEGY_PROMOTION_BOUNDARY: Final[str] = (
    "offline_read_model_strategy_evidence_only_not_market_execution"
)
OFFLINE_STRATEGY_PROMOTION_ACADEMIC_SCOPE: Final[str] = (
    "Offline Strategy Promotion means offline/read-model strategy evidence only; "
    "market execution remains disabled and strict_similar_day remains the fallback."
)


def offline_strategy_promotion_academic_scope(stored_scope: str = "") -> str:
    """Return thesis-safe wording for internal production-promotion rows."""

    sanitized_scope = _sanitize_stored_scope(stored_scope)
    if not sanitized_scope:
        return OFFLINE_STRATEGY_PROMOTION_ACADEMIC_SCOPE
    return f"{OFFLINE_STRATEGY_PROMOTION_ACADEMIC_SCOPE} Source note: {sanitized_scope}"


def summarize_offline_strategy_promotion(gate_frame: pl.DataFrame) -> dict[str, Any]:
    """Summarize stable internal promotion fields with thesis-safe display language."""

    if gate_frame.height == 0:
        return {
            "display_name": OFFLINE_STRATEGY_PROMOTION_DISPLAY_NAME,
            "claim_boundary": OFFLINE_STRATEGY_PROMOTION_BOUNDARY,
            "production_promote_count": 0,
            "promoted_source_model_names": [],
            "market_execution_enabled": False,
        }
    promoted_source_model_names = sorted(
        str(row["source_model_name"])
        for row in gate_frame.iter_rows(named=True)
        if bool(row["production_promote"])
    )
    market_execution_enabled = any(
        bool(row["market_execution_enabled"]) for row in gate_frame.iter_rows(named=True)
    )
    return {
        "display_name": OFFLINE_STRATEGY_PROMOTION_DISPLAY_NAME,
        "claim_boundary": OFFLINE_STRATEGY_PROMOTION_BOUNDARY,
        "production_promote_count": len(promoted_source_model_names),
        "promoted_source_model_names": promoted_source_model_names,
        "market_execution_enabled": market_execution_enabled,
    }


def _sanitize_stored_scope(stored_scope: str) -> str:
    clean_scope = " ".join(stored_scope.split())
    if not clean_scope:
        return ""
    lower_scope = clean_scope.lower()
    if "production gate" in lower_scope or "production-promotion" in lower_scope:
        return ""
    return clean_scope
