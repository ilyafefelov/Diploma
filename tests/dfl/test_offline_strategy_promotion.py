from datetime import UTC, datetime

import polars as pl

from smart_arbitrage.dfl.offline_strategy_promotion import (
    OFFLINE_STRATEGY_PROMOTION_BOUNDARY,
    OFFLINE_STRATEGY_PROMOTION_DISPLAY_NAME,
    offline_strategy_promotion_academic_scope,
    summarize_offline_strategy_promotion,
)


def test_academic_scope_translates_internal_production_language() -> None:
    scope = offline_strategy_promotion_academic_scope(
        "Internal production gate for Schedule/Value Learner V2."
    )

    assert OFFLINE_STRATEGY_PROMOTION_DISPLAY_NAME in scope
    assert "offline/read-model strategy evidence only" in scope
    assert "market execution remains disabled" in scope
    assert "Internal production gate" not in scope


def test_summary_keeps_stable_fields_but_exposes_offline_language() -> None:
    frame = pl.DataFrame(
        {
            "source_model_name": ["nbeatsx", "tft"],
            "production_promote": [True, False],
            "market_execution_enabled": [False, False],
            "generated_at": [datetime(2026, 5, 12, tzinfo=UTC)] * 2,
        }
    )

    summary = summarize_offline_strategy_promotion(frame)

    assert summary["display_name"] == OFFLINE_STRATEGY_PROMOTION_DISPLAY_NAME
    assert summary["claim_boundary"] == OFFLINE_STRATEGY_PROMOTION_BOUNDARY
    assert summary["production_promote_count"] == 1
    assert summary["promoted_source_model_names"] == ["nbeatsx"]
    assert summary["market_execution_enabled"] is False
