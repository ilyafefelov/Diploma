from __future__ import annotations

from datetime import date, datetime, timedelta

from smart_arbitrage.publication.bess_arbitrage_index import (
    PUBLIC_FORECAST_CHALLENGE_CLAIM_BOUNDARY,
)
from smart_arbitrage.publication.forecast_challenge import (
    NBEATSX_PUBLIC_MODEL_NAME,
    STRICT_SIMILAR_DAY_MODEL_NAME,
    TFT_PUBLIC_MODEL_NAME,
    build_empty_forecast_scoreboard_payload,
    build_public_forecast_challenge_payload,
)


def test_public_forecast_challenge_publishes_baseline_and_blocks_unmaterialized_ml() -> None:
    start = datetime(2026, 6, 8)
    history = [
        {
            "timestamp": start + timedelta(hours=hour),
            "price_uah_mwh": 2400.0 + (hour % 24) * 100.0,
            "source_url": "https://www.oree.com.ua/index.php/PXS/get_pxs_hdata/example",
        }
        for hour in range(8 * 24)
    ]

    payload = build_public_forecast_challenge_payload(
        history,
        target_delivery_date=date(2026, 6, 16),
        generated_at=datetime(2026, 6, 15, 7, 0),
    )

    assert payload["claim_boundary"] == PUBLIC_FORECAST_CHALLENGE_CLAIM_BOUNDARY
    assert payload["market_execution_enabled"] is False
    models = {model["model_name"]: model for model in payload["models"]}
    assert set(models) == {
        STRICT_SIMILAR_DAY_MODEL_NAME,
        NBEATSX_PUBLIC_MODEL_NAME,
        TFT_PUBLIC_MODEL_NAME,
    }
    assert models[STRICT_SIMILAR_DAY_MODEL_NAME]["point_count"] == 24
    assert models[STRICT_SIMILAR_DAY_MODEL_NAME]["quality_boundary"] == "ranked_baseline"
    assert models[STRICT_SIMILAR_DAY_MODEL_NAME]["points"][0]["forecast_price_uah_mwh"] == 2400.0
    assert models[NBEATSX_PUBLIC_MODEL_NAME]["quality_boundary"] == "experimental_not_ranked"
    assert models[TFT_PUBLIC_MODEL_NAME]["backend_status"] == "blocked"


def test_empty_forecast_scoreboard_keeps_non_execution_boundary() -> None:
    payload = build_empty_forecast_scoreboard_payload(generated_at=datetime(2026, 6, 15, 8, 0))

    assert payload["row_count"] == 0
    assert payload["score_status"] == "pending_realized_forecast_pairs"
    assert payload["market_execution_enabled"] is False
    assert payload["proposed_bid_status"] == "not_emitted"


def test_public_forecast_challenge_blocks_baseline_when_similar_day_source_is_missing() -> None:
    history = [
        {
            "timestamp": datetime(2026, 6, 10) + timedelta(hours=hour),
            "price_uah_mwh": 2800.0,
            "source_url": "https://www.oree.com.ua/index.php/PXS/get_pxs_hdata/example",
        }
        for hour in range(24)
    ]

    payload = build_public_forecast_challenge_payload(
        history,
        target_delivery_date=date(2026, 6, 16),
        generated_at=datetime(2026, 6, 15, 7, 0),
    )

    models = {model["model_name"]: model for model in payload["models"]}
    baseline = models[STRICT_SIMILAR_DAY_MODEL_NAME]
    assert baseline["backend_status"] == "blocked"
    assert baseline["quality_boundary"] == "blocked_source_gap"
    assert baseline["point_count"] == 0
    assert payload["market_execution_enabled"] is False
