"""Public forecast-challenge payloads for the BESS index."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, Final, cast

from smart_arbitrage.publication.bess_arbitrage_index import (
    PUBLIC_FORECAST_CHALLENGE_CLAIM_BOUNDARY,
    PublicPricePoint,
)

STRICT_SIMILAR_DAY_MODEL_NAME: Final[str] = "strict_similar_day_baseline"
NBEATSX_PUBLIC_MODEL_NAME: Final[str] = "nbeatsx_official_public_challenge_v0"
TFT_PUBLIC_MODEL_NAME: Final[str] = "tft_official_public_challenge_v0"


def build_public_forecast_challenge_payload(
    history_rows: Sequence[Mapping[str, Any] | PublicPricePoint],
    *,
    target_delivery_date: date,
    generated_at: datetime | None = None,
    model_forecasts: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    history = _normalize_history(history_rows)
    generated = generated_at or datetime.now(UTC)
    model_series: list[dict[str, Any]] = []
    try:
        strict_forecast = _strict_similar_day_forecast(
            history,
            target_delivery_date=target_delivery_date,
        )
        model_series.append(
            _model_series_payload(
                model_name=STRICT_SIMILAR_DAY_MODEL_NAME,
                label="Strict similar-day baseline",
                generated_at=generated,
                target_delivery_date=target_delivery_date,
                training_cutoff=_training_cutoff(history),
                quality_boundary="ranked_baseline",
                points=strict_forecast,
            )
        )
    except Exception as error:
        model_series.append(
            _blocked_model_payload(
                model_name=STRICT_SIMILAR_DAY_MODEL_NAME,
                label="Strict similar-day baseline",
                generated_at=generated,
                target_delivery_date=target_delivery_date,
                training_cutoff=_training_cutoff(history),
                blocker=str(error),
                quality_boundary="blocked_source_gap",
            )
        )
    model_series.extend(_supplied_model_series(model_forecasts, target_delivery_date=target_delivery_date))
    present_models = {str(model["model_name"]) for model in model_series}
    for model_name, label in (
        (NBEATSX_PUBLIC_MODEL_NAME, "NBEATSx public challenge"),
        (TFT_PUBLIC_MODEL_NAME, "TFT public challenge"),
    ):
        if model_name not in present_models:
            model_series.append(
                _blocked_model_payload(
                    model_name=model_name,
                    label=label,
                    generated_at=generated,
                    target_delivery_date=target_delivery_date,
                    training_cutoff=_training_cutoff(history),
                    blocker="model_not_materialized_in_this_public_snapshot",
                )
            )

    return {
        "schema_version": "ukraine_bess_forecast_challenge.v1",
        "generated_at": _iso(generated),
        "target_delivery_date": target_delivery_date.isoformat(),
        "source": {
            "history_row_count": len(history),
            "training_cutoff": _iso(_training_cutoff(history)),
            "source_scope": "source_backed_observed_oree_history_only",
        },
        "models": model_series,
        "claim_boundary": PUBLIC_FORECAST_CHALLENGE_CLAIM_BOUNDARY,
        "market_execution_enabled": False,
        "proposed_bid_status": "not_emitted",
    }


def build_empty_forecast_scoreboard_payload(
    *,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": "ukraine_bess_forecast_scoreboard.v1",
        "generated_at": _iso(generated_at or datetime.now(UTC)),
        "rows": [],
        "row_count": 0,
        "score_status": "pending_realized_forecast_pairs",
        "metrics": [
            "mae_uah_mwh",
            "rmse_uah_mwh",
            "dispatch_regret_uah",
            "value_capture_ratio",
        ],
        "claim_boundary": PUBLIC_FORECAST_CHALLENGE_CLAIM_BOUNDARY,
        "market_execution_enabled": False,
        "proposed_bid_status": "not_emitted",
    }


def _strict_similar_day_forecast(
    history: Sequence[PublicPricePoint],
    *,
    target_delivery_date: date,
) -> list[dict[str, Any]]:
    by_timestamp = {point.timestamp: point for point in history}
    points: list[dict[str, Any]] = []
    for hour in range(24):
        forecast_timestamp = datetime.combine(target_delivery_date, datetime.min.time()) + timedelta(hours=hour)
        source_timestamp = _similar_day_source_timestamp(forecast_timestamp)
        source_point = by_timestamp.get(source_timestamp)
        if source_point is None:
            raise ValueError(
                "strict similar-day forecast needs source-backed history for "
                f"{source_timestamp.isoformat()}"
            )
        points.append(
            {
                "timestamp": forecast_timestamp.isoformat(),
                "forecast_price_uah_mwh": round(source_point.price_uah_mwh, 6),
                "source_timestamp": source_timestamp.isoformat(),
                "point_in_time_status": "generated_before_target_publication",
                "leakage_check_status": "source_timestamp_before_target",
            }
        )
    return points


def _model_series_payload(
    *,
    model_name: str,
    label: str,
    generated_at: datetime,
    target_delivery_date: date,
    training_cutoff: datetime,
    quality_boundary: str,
    points: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "model_name": model_name,
        "label": label,
        "forecast_generated_at": _iso(generated_at),
        "training_cutoff": _iso(training_cutoff),
        "target_delivery_date": target_delivery_date.isoformat(),
        "point_count": len(points),
        "point_in_time_status": "generated_before_target_publication",
        "leakage_check_status": "source_timestamp_before_target",
        "quality_boundary": quality_boundary,
        "backend_status": "materialized",
        "points": list(points),
        "claim_boundary": PUBLIC_FORECAST_CHALLENGE_CLAIM_BOUNDARY,
        "market_execution_enabled": False,
    }


def _blocked_model_payload(
    *,
    model_name: str,
    label: str,
    generated_at: datetime,
    target_delivery_date: date,
    training_cutoff: datetime,
    blocker: str,
    quality_boundary: str = "experimental_not_ranked",
) -> dict[str, Any]:
    return {
        "model_name": model_name,
        "label": label,
        "forecast_generated_at": _iso(generated_at),
        "training_cutoff": _iso(training_cutoff),
        "target_delivery_date": target_delivery_date.isoformat(),
        "point_count": 0,
        "point_in_time_status": "not_materialized",
        "leakage_check_status": "not_ranked",
        "quality_boundary": quality_boundary,
        "backend_status": "blocked",
        "blocker": blocker,
        "points": [],
        "claim_boundary": PUBLIC_FORECAST_CHALLENGE_CLAIM_BOUNDARY,
        "market_execution_enabled": False,
    }


def _supplied_model_series(
    model_forecasts: Sequence[Mapping[str, Any]],
    *,
    target_delivery_date: date,
) -> list[dict[str, Any]]:
    series: list[dict[str, Any]] = []
    for forecast in model_forecasts:
        model_name = str(forecast.get("model_name") or "").strip()
        if not model_name:
            continue
        points_value = forecast.get("points")
        points = (
            cast(Sequence[Mapping[str, Any]], points_value)
            if isinstance(points_value, list)
            else []
        )
        generated_at = _datetime_value(forecast.get("forecast_generated_at") or forecast.get("generated_at"))
        training_cutoff = _datetime_value(forecast.get("training_cutoff"))
        series.append(
            _model_series_payload(
                model_name=model_name,
                label=str(forecast.get("label") or model_name),
                generated_at=generated_at,
                target_delivery_date=target_delivery_date,
                training_cutoff=training_cutoff,
                quality_boundary=str(forecast.get("quality_boundary") or "experimental_not_ranked"),
                points=points,
            )
        )
    return series


def _normalize_history(rows: Sequence[Mapping[str, Any] | PublicPricePoint]) -> list[PublicPricePoint]:
    points: list[PublicPricePoint] = []
    for row in rows:
        if isinstance(row, PublicPricePoint):
            points.append(row)
            continue
        points.append(
            PublicPricePoint(
                timestamp=_datetime_value(row.get("timestamp")),
                price_uah_mwh=_float_value(row.get("price_uah_mwh")),
                volume_mwh=_optional_float_value(row.get("volume_mwh")),
                source_url=str(row.get("source_url") or ""),
            )
        )
    if not points:
        raise ValueError("forecast challenge requires source-backed history rows")
    return sorted(points, key=lambda point: point.timestamp)


def _training_cutoff(history: Sequence[PublicPricePoint]) -> datetime:
    return max(point.timestamp for point in history)


def _similar_day_source_timestamp(target_timestamp: datetime) -> datetime:
    if target_timestamp.weekday() in {1, 2, 3, 4}:
        return target_timestamp - timedelta(hours=24)
    return target_timestamp - timedelta(hours=168)


def _datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str) and value.strip():
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    raise TypeError("timestamp must be a datetime or ISO datetime string")


def _float_value(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise TypeError("price_uah_mwh must be numeric")
    return float(str(value).replace(",", "."))


def _optional_float_value(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return _float_value(value)


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC).isoformat()
    return value.astimezone(UTC).isoformat()
