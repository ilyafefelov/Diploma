"""Legacy-refactored Bronze ingestion for market prices and weather forecasts."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import logging
import math
import os
import re
from time import sleep
from typing import Any, Final

from bs4 import BeautifulSoup
import dagster as dg
import httpx
import polars as pl
import yaml

from smart_arbitrage.assets import taxonomy
from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_PRICE_COLUMN, DEFAULT_TIMESTAMP_COLUMN
from smart_arbitrage.resources.market_data_store import (
    get_market_data_store,
    market_price_observations_from_frame,
    weather_observations_from_frame,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WeatherLocation:
    latitude: float
    longitude: float
    timezone: str


DEFAULT_WEATHER_LOCATION: Final[WeatherLocation] = WeatherLocation(
    latitude=50.45,
    longitude=30.52,
    timezone="Europe/Kyiv",
)
WEATHER_LOCATION_CONFIG_PATH_ENV: Final[str] = "WEATHER_LOCATION_CONFIG_PATH"
WEATHER_LOCATION_TENANT_ID_ENV: Final[str] = "WEATHER_LOCATION_TENANT_ID"
UAH_PER_EUR: Final[float] = 40.0
OREE_PRICES_URL: Final[str] = "https://www.oree.com.ua/index.php/pricectr?lang=english"
OREE_DATA_VIEW_URL: Final[str] = "https://www.oree.com.ua/index.php/pricectr/data_view"
OPEN_METEO_URL: Final[str] = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_HISTORICAL_URL: Final[str] = "https://archive-api.open-meteo.com/v1/archive"
SYNTHETIC_MARKET_SOURCE_URL: Final[str] = "synthetic://smart_arbitrage/market_price_history"
SYNTHETIC_WEATHER_SOURCE_URL: Final[str] = "synthetic://smart_arbitrage/weather_forecast"
OBSERVED_SOURCE_KIND: Final[str] = "observed"
SYNTHETIC_SOURCE_KIND: Final[str] = "synthetic"
LEVEL1_MARKET_VENUE: Final[str] = "DAM"
LEVEL1_MARKET_ZONE: Final[str] = "IPS"
LEVEL1_MARKET_TIMEZONE: Final[str] = "Europe/Kyiv"
DEFAULT_MARKET_HISTORY_HOURS: Final[int] = 15 * 24
DEFAULT_MARKET_FORECAST_HOURS: Final[int] = 24
DEFAULT_WEATHER_FORECAST_HOURS: Final[int] = 7 * 24
OREE_DATA_VIEW_MONTH_REQUEST_PAUSE_SECONDS: Final[float] = 2.0
MARKET_WEATHER_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "temperature",
    "solar_radiation",
    "wind_speed",
    "cloudcover",
    "precipitation",
    "pressure",
    "humidity",
    "high_solar",
    "high_wind",
    "heavy_rain",
    "solar_elevation",
    "clear_sky_index",
    "effective_solar",
    "is_daylight",
    "season",
    "sky_condition",
    "source",
    "source_kind",
    "source_url",
    "location_latitude",
    "location_longitude",
    "location_timezone",
)


class WeatherLocationConfig(dg.Config):
    """Per-run weather location overrides over the configured default location."""

    latitude: float | None = None
    longitude: float | None = None
    timezone: str | None = None
    tenant_id: str | None = None
    location_config_path: str | None = None


class ObservedMarketBackfillConfig(dg.Config):
    """Observed-only OREE DAM backfill window."""

    start_date: str = "2026-01-01"
    end_date: str = "2026-01-31"


class TenantHistoricalWeatherConfig(dg.Config):
    """Observed-only historical Open-Meteo weather window for benchmark tenants."""

    tenant_ids_csv: str = ""
    start_date: str = "2026-01-01"
    end_date: str = "2026-01-31"
    location_config_path: str | None = None


@dg.asset(
    group_name=taxonomy.BRONZE_WEATHER,
    tags=taxonomy.asset_tags(
        medallion="bronze",
        domain="weather",
        elt_stage="extract_load",
        ml_stage="source_data",
        evidence_scope="demo",
    ),
)
def weather_forecast_bronze(context, config: WeatherLocationConfig) -> pl.DataFrame:
    """Weather forecast Bronze asset refactored from the legacy Open-Meteo ingestion flow."""

    location = _resolve_weather_location(config)
    tenant_id = _clean_optional_text(config.tenant_id)
    location_config_path = _clean_optional_text(config.location_config_path)
    weather_rows = _fetch_openmeteo_data(location.latitude, location.longitude, location.timezone)
    if weather_rows is None:
        logger.warning("Open-Meteo weather fetch failed; using synthetic fallback data.")
        weather_rows = _generate_synthetic_weather()

    weather_frame = _tag_weather_location(
        _add_solar_features(
            _validate_weather_data(pl.DataFrame(weather_rows)),
            latitude=location.latitude,
        ),
        weather_location=location,
    )
    source_values = sorted(str(value) for value in weather_frame.select("source").unique().to_series().to_list())
    metadata: dict[str, str | float | int] = {
        "rows": weather_frame.height,
        "source_values": ", ".join(source_values),
        "latitude": location.latitude,
        "longitude": location.longitude,
        "timezone": location.timezone,
        "first_timestamp": weather_frame.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(0).isoformat(),
        "last_timestamp": weather_frame.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(-1).isoformat(),
    }
    if tenant_id is not None:
        metadata["tenant_id"] = tenant_id
    if location_config_path is not None:
        metadata["location_config_path"] = location_config_path
    weather_observations = weather_observations_from_frame(weather_frame, tenant_id=tenant_id)
    get_market_data_store().upsert_weather_observations(weather_observations)
    metadata["weather_observation_rows"] = len(weather_observations)
    context.add_output_metadata(metadata)
    return weather_frame


BRONZE_INGESTION_ASSETS = [weather_forecast_bronze]


@dg.asset(
    group_name=taxonomy.BRONZE_MARKET_DATA,
    tags=taxonomy.asset_tags(
        medallion="bronze",
        domain="market_data",
        elt_stage="extract_load",
        ml_stage="source_data",
        evidence_scope="thesis_grade",
        market_venue="DAM",
    ),
)
def observed_market_price_history_bronze(
    context,
    config: ObservedMarketBackfillConfig,
) -> pl.DataFrame:
    """Observed-only OREE DAM hourly backfill for thesis benchmark runs."""

    price_history = build_observed_market_price_history(
        start_date=date.fromisoformat(config.start_date),
        end_date=date.fromisoformat(config.end_date),
    )
    market_observations = market_price_observations_from_frame(price_history)
    get_market_data_store().upsert_market_prices(market_observations)
    _add_metadata(
        context,
        {
            "rows": price_history.height,
            "source_kind": "observed",
            "market_observation_rows": len(market_observations),
            "start_timestamp": price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(0).isoformat(),
            "end_timestamp": price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(-1).isoformat(),
        },
    )
    return price_history


@dg.asset(
    group_name=taxonomy.BRONZE_WEATHER,
    tags=taxonomy.asset_tags(
        medallion="bronze",
        domain="weather",
        elt_stage="extract_load",
        ml_stage="source_data",
        evidence_scope="thesis_grade",
    ),
)
def tenant_historical_weather_bronze(
    context,
    config: TenantHistoricalWeatherConfig,
) -> pl.DataFrame:
    """Observed historical tenant weather aligned to real-data benchmark windows."""

    rows: list[pl.DataFrame] = []
    for tenant_id in _tenant_ids_from_csv(config.tenant_ids_csv):
        rows.append(
            build_observed_historical_weather_frame(
                tenant_id=tenant_id,
                start_date=date.fromisoformat(config.start_date),
                end_date=date.fromisoformat(config.end_date),
                location_config_path=config.location_config_path,
            )
        )
    weather_frame = pl.concat(rows, how="diagonal_relaxed") if rows else pl.DataFrame()
    weather_observations = weather_observations_from_frame(weather_frame, tenant_id=None) if weather_frame.height else []
    get_market_data_store().upsert_weather_observations(weather_observations)
    _add_metadata(
        context,
        {
            "rows": weather_frame.height,
            "tenant_count": weather_frame.select("tenant_id").n_unique() if weather_frame.height else 0,
            "source_kind": "observed",
            "weather_observation_rows": len(weather_observations),
        },
    )
    return weather_frame


REAL_DATA_BENCHMARK_BRONZE_ASSETS = [
    observed_market_price_history_bronze,
    tenant_historical_weather_bronze,
]


def build_demo_market_price_history(
    *,
    history_hours: int = DEFAULT_MARKET_HISTORY_HOURS,
    forecast_hours: int = DEFAULT_MARKET_FORECAST_HOURS,
    now: datetime | None = None,
) -> pl.DataFrame:
    """Build demo-ready DAM history with live OREE overlay and synthetic fallback."""

    base_history = build_synthetic_market_price_history(
        history_hours=history_hours,
        forecast_hours=forecast_hours,
        now=now,
    )
    anchor_hour = _round_to_hour(now or datetime.now())

    live_rows: list[dict[str, Any]] = []
    for day_offset in range(2):
        target_date = (anchor_hour + timedelta(days=day_offset)).date()
        fetched_rows = _fetch_oree_prices(target_date)
        if fetched_rows is not None:
            live_rows.extend(fetched_rows)

    if live_rows:
        history_frame = _overlay_market_rows(base_history, pl.DataFrame(live_rows))
    else:
        history_frame = base_history

    window_start = base_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(0)
    window_end = base_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(-1)
    return _validate_market_data(
        history_frame.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN).is_between(window_start, window_end, closed="both"))
    )


def build_synthetic_market_price_history(
    *,
    history_hours: int = DEFAULT_MARKET_HISTORY_HOURS,
    forecast_hours: int = DEFAULT_MARKET_FORECAST_HOURS,
    now: datetime | None = None,
) -> pl.DataFrame:
    """Deterministic synthetic DAM history used as the Bronze fallback and demo baseline."""

    current_hour = _round_to_hour(now or datetime.now())
    latest_timestamp = current_hour + timedelta(hours=forecast_hours)
    start_timestamp = latest_timestamp - timedelta(hours=history_hours - 1)

    rows: list[dict[str, Any]] = []
    for hour_index in range(history_hours):
        timestamp = start_timestamp + timedelta(hours=hour_index)
        price_uah = _synthetic_price_for_timestamp(timestamp=timestamp, hour_index=hour_index)
        price_eur = price_uah / UAH_PER_EUR
        volume_mwh = max(200.0, 1000.0 + 180.0 * math.sin((hour_index / 24.0) * 2.0 * math.pi))
        rows.append(
            _build_market_row(
                timestamp=timestamp,
                price_eur_mwh=price_eur,
                price_uah_mwh=price_uah,
                volume_mwh=volume_mwh,
                source="SYNTHETIC",
            )
        )
    return pl.DataFrame(rows)


def build_observed_market_price_history(
    *,
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    """Build an observed-only OREE DAM hourly history window."""

    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date.")
    observed_rows: list[dict[str, Any]] = []
    requested_dates = _date_range(start_date, end_date)
    requested_date_set = set(requested_dates)
    rows_by_date: dict[date, list[dict[str, Any]]] = {
        target_date: []
        for target_date in requested_dates
    }
    for month_index, month_date in enumerate(_month_range(start_date, end_date)):
        if month_index > 0:
            _pause_between_oree_month_requests()
        fetched_rows = _fetch_oree_data_view_month_prices(month_date)
        if fetched_rows is None:
            logger.warning(
                "OREE monthly fetch failed for %s; falling back to per-day fetch.",
                month_date.strftime("%Y-%m"),
            )
            fetched_rows = []
        for row in fetched_rows:
            timestamp = row.get(DEFAULT_TIMESTAMP_COLUMN)
            if isinstance(timestamp, datetime) and timestamp.date() in requested_date_set:
                rows_by_date[timestamp.date()].append(row)

    missing_dates: list[date] = []
    for target_date, rows in rows_by_date.items():
        if not rows:
            fallback_rows = _fetch_oree_prices(target_date)
            rows = fallback_rows or []
        if not rows:
            missing_dates.append(target_date)
            continue
        observed_rows.extend(rows)
    if missing_dates or not observed_rows:
        missing_text = ", ".join(target_date.isoformat() for target_date in missing_dates)
        raise ValueError(f"Missing observed OREE DAM rows for benchmark dates: {missing_text}.")

    price_history = _validate_market_data(pl.DataFrame(observed_rows))
    source_kinds = set(price_history.select("source_kind").to_series().to_list())
    if source_kinds != {OBSERVED_SOURCE_KIND}:
        raise ValueError("Observed market benchmark history must contain only observed source rows.")
    return price_history


def build_observed_historical_weather_frame(
    *,
    tenant_id: str,
    start_date: date,
    end_date: date,
    location_config_path: str | None = None,
) -> pl.DataFrame:
    """Build observed historical weather rows for one tenant/location."""

    location = resolve_weather_location_for_tenant(
        tenant_id=tenant_id,
        location_config_path=location_config_path,
    )
    weather_rows = _fetch_openmeteo_historical_data(
        location.latitude,
        location.longitude,
        location.timezone,
        start_date=start_date,
        end_date=end_date,
    )
    if not weather_rows:
        raise ValueError(f"Missing observed historical weather rows for tenant {tenant_id}.")
    weather_frame = _tag_weather_location(
        _add_solar_features(
            _validate_weather_data(pl.DataFrame(weather_rows)),
            latitude=location.latitude,
        ),
        weather_location=location,
    ).with_columns(pl.lit(tenant_id).alias("tenant_id"))
    source_kinds = set(weather_frame.select("source_kind").to_series().to_list())
    if source_kinds != {OBSERVED_SOURCE_KIND}:
        raise ValueError("Historical weather benchmark frame must contain only observed source rows.")
    return weather_frame


def enrich_market_price_history_with_weather(
    price_history: pl.DataFrame,
    weather_forecast: pl.DataFrame,
    *,
    weather_location: WeatherLocation | None = None,
) -> pl.DataFrame:
    """Join weather and solar features onto market history for the same hourly window."""

    if price_history.height == 0:
        return price_history

    resolved_weather_location = weather_location
    if resolved_weather_location is None:
        resolved_weather_location = _location_from_weather_frame(weather_forecast)
    if resolved_weather_location is None:
        resolved_weather_location = _resolve_weather_location(WeatherLocationConfig())

    start_timestamp = price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(0)
    base_weather_history = _build_weather_history_for_market_window(
        start_timestamp=start_timestamp,
        hours=price_history.height,
        weather_location=resolved_weather_location,
    )
    weather_history = _overlay_weather_rows(base_weather_history, weather_forecast)
    return price_history.join(
        _select_market_weather_features(weather_history),
        on=DEFAULT_TIMESTAMP_COLUMN,
        how="left",
    )


def build_weather_forecast_window(
    *,
    start_timestamp: datetime,
    hours: int,
    weather_location: WeatherLocation,
) -> pl.DataFrame:
    """Build a weather forecast window with live Open-Meteo overlay and synthetic fallback."""

    if hours <= 0:
        raise ValueError("Weather forecast window requires at least one hour.")

    base_weather_history = _build_weather_history_for_market_window(
        start_timestamp=start_timestamp,
        hours=hours,
        weather_location=weather_location,
    )
    live_weather_rows = _fetch_openmeteo_data(
        weather_location.latitude,
        weather_location.longitude,
        weather_location.timezone,
    )
    if live_weather_rows is None:
        return base_weather_history

    live_weather_history = _tag_weather_location(
        _add_solar_features(
            _validate_weather_data(pl.DataFrame(live_weather_rows)),
            latitude=weather_location.latitude,
        ),
        weather_location=weather_location,
    )
    window_end = start_timestamp + timedelta(hours=hours - 1)
    return (
        _overlay_weather_rows(base_weather_history, live_weather_history)
        .filter(pl.col(DEFAULT_TIMESTAMP_COLUMN).is_between(start_timestamp, window_end, closed="both"))
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )


def _synthetic_price_for_timestamp(*, timestamp: datetime, hour_index: int) -> float:
    hour_of_day = timestamp.hour
    weekday = timestamp.weekday()
    base_price = 2800.0
    intraday_wave = 850.0 * math.sin(((hour_of_day - 6) / 24.0) * 2.0 * math.pi)
    evening_peak = 1350.0 if 18 <= hour_of_day <= 21 else 0.0
    night_discount = -650.0 if 0 <= hour_of_day <= 5 else 0.0
    weekend_discount = -320.0 if weekday in {5, 6} else 0.0
    weekly_trend = 45.0 * (hour_index // 24)
    return max(600.0, base_price + intraday_wave + evening_peak + night_discount + weekend_discount + weekly_trend)


def _build_market_row(
    *,
    timestamp: datetime,
    price_eur_mwh: float,
    price_uah_mwh: float,
    volume_mwh: float,
    source: str,
    source_kind: str | None = None,
    source_url: str | None = None,
    market_venue: str = LEVEL1_MARKET_VENUE,
    market_zone: str = LEVEL1_MARKET_ZONE,
    market_timezone: str = LEVEL1_MARKET_TIMEZONE,
) -> dict[str, Any]:
    resolved_source_kind = source_kind or (SYNTHETIC_SOURCE_KIND if source == "SYNTHETIC" else OBSERVED_SOURCE_KIND)
    resolved_source_url = source_url or (SYNTHETIC_MARKET_SOURCE_URL if source == "SYNTHETIC" else OREE_PRICES_URL)
    return {
        DEFAULT_TIMESTAMP_COLUMN: timestamp,
        "price_eur_mwh": float(price_eur_mwh),
        DEFAULT_PRICE_COLUMN: float(price_uah_mwh),
        "volume_mwh": float(max(0.0, volume_mwh)),
        "source": source,
        "source_kind": resolved_source_kind,
        "source_url": resolved_source_url,
        "market_venue": market_venue,
        "market_zone": market_zone,
        "market_timezone": market_timezone,
        "price_spike": False,
        "low_volume": False,
        "fetched_at": datetime.now(),
    }


def _round_to_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def _overlay_market_rows(base_history: pl.DataFrame, live_history: pl.DataFrame) -> pl.DataFrame:
    required_columns = base_history.columns
    missing_columns = [column_name for column_name in required_columns if column_name not in live_history.columns]
    aligned_live_history = live_history.with_columns(
        [pl.lit(None).alias(column_name) for column_name in missing_columns]
    ).select(required_columns)
    return (
        pl.concat([base_history.select(required_columns), aligned_live_history], how="diagonal_relaxed")
        .sort(DEFAULT_TIMESTAMP_COLUMN)
        .unique(subset=[DEFAULT_TIMESTAMP_COLUMN], keep="last")
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )


def _parse_decimal(raw_value: object) -> float | None:
    if raw_value is None:
        return None
    for match in re.findall(r"\d+[\d\s,.]*", str(raw_value)):
        normalized = match.replace(" ", "").replace(",", ".")
        try:
            return float(normalized)
        except ValueError:
            continue
    return None


def _parse_hour_value(raw_value: object) -> int | None:
    if raw_value is None:
        return None
    normalized_text = str(raw_value).strip()
    if not normalized_text:
        return None
    if ":" in normalized_text:
        try:
            candidate = int(normalized_text.split(":", maxsplit=1)[0])
        except ValueError:
            candidate = None
        if candidate is not None and 0 <= candidate <= 23:
            return candidate
    for digit in re.findall(r"\d+", normalized_text):
        candidate = int(digit)
        if 0 <= candidate <= 23:
            return candidate
    return None


def _fetch_oree_prices(target_date: date) -> list[dict[str, Any]] | None:
    try:
        data_view_prices = _fetch_oree_data_view_prices(target_date)
        if data_view_prices:
            return data_view_prices

        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            response = client.get(OREE_PRICES_URL)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        parsed_rows = _extract_oree_price_rows(soup, target_date)
        if parsed_rows:
            logger.info("Parsed %s OREE rows from HTML tables for %s.", len(parsed_rows), target_date)
            return parsed_rows
        logger.warning("No OREE rows found for %s via HTML fallback.", target_date)
        return None
    except Exception as error:
        logger.warning("OREE fetch failed for %s: %s", target_date, error)
        return None


def _fetch_oree_data_view_prices(target_date: date) -> list[dict[str, Any]] | None:
    month_rows = _fetch_oree_data_view_month_prices(target_date)
    if month_rows is not None:
        target_rows = [
            row
            for row in month_rows
            if isinstance(row.get(DEFAULT_TIMESTAMP_COLUMN), datetime)
            and row[DEFAULT_TIMESTAMP_COLUMN].date() == target_date
        ]
        if target_rows:
            logger.info("Parsed %s OREE rows from data_view endpoint for %s.", len(target_rows), target_date)
            return target_rows
        return None
    return None


def _fetch_oree_data_view_month_prices(month_date: date) -> list[dict[str, Any]] | None:
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            response = client.post(
                OREE_DATA_VIEW_URL,
                data={
                    "date": month_date.strftime("%m.%Y"),
                    "market": LEVEL1_MARKET_VENUE,
                    "zone": LEVEL1_MARKET_ZONE,
                },
                headers={
                    "Referer": OREE_PRICES_URL,
                    "X-Requested-With": "XMLHttpRequest",
                },
            )
            response.raise_for_status()

        content_html = _extract_oree_data_view_content(response)
        if not content_html:
            return None
        parsed_rows = _extract_all_prices_from_data_view_content(content_html)
        if parsed_rows:
            logger.info(
                "Parsed %s OREE rows from data_view endpoint for %s.",
                len(parsed_rows),
                month_date.strftime("%m.%Y"),
            )
        return parsed_rows or None
    except Exception as error:
        logger.warning("OREE data_view fetch failed for %s: %s", month_date.strftime("%m.%Y"), error)
        return None


def _extract_oree_data_view_content(response: httpx.Response) -> str | None:
    try:
        payload = response.json()
    except ValueError:
        payload = None

    if isinstance(payload, dict):
        content_html = payload.get("content")
        if isinstance(content_html, str) and content_html.strip():
            return content_html

    content_html = response.text
    return content_html if content_html.strip() else None


def _extract_prices_from_data_view_content(content_html: str, target_date: date) -> list[dict[str, Any]]:
    return [
        row
        for row in _extract_all_prices_from_data_view_content(content_html)
        if isinstance(row.get(DEFAULT_TIMESTAMP_COLUMN), datetime)
        and row[DEFAULT_TIMESTAMP_COLUMN].date() == target_date
    ]


def _extract_all_prices_from_data_view_content(content_html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(content_html, "html.parser")
    table = soup.find("table", id="price_table") or soup.find("table")
    if table is None:
        return []

    rows = table.find_all("tr")
    if len(rows) < 2:
        return []

    parsed_rows: list[dict[str, Any]] = []
    for row in rows[1:]:
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
        if not cells:
            continue
        try:
            row_date = datetime.strptime(cells[0], "%d.%m.%Y").date()
        except ValueError:
            continue
        for hour_index, price_text in enumerate(cells[1:25], start=1):
            price_uah = _parse_decimal(price_text)
            if price_uah is None or price_uah <= 0:
                continue
            timestamp = datetime.combine(row_date, datetime.min.time().replace(hour=hour_index - 1))
            parsed_rows.append(
                _build_market_row(
                    timestamp=timestamp,
                    price_eur_mwh=price_uah / UAH_PER_EUR,
                    price_uah_mwh=price_uah,
                    volume_mwh=1000.0,
                    source="OREE_DATA_VIEW",
                    source_kind=OBSERVED_SOURCE_KIND,
                    source_url=OREE_DATA_VIEW_URL,
                )
            )
    return parsed_rows


def _extract_oree_price_rows(soup: BeautifulSoup, target_date: date) -> list[dict[str, Any]]:
    tables = soup.find_all("table", class_="price-table") or soup.find_all("table")
    best_candidate: list[dict[str, Any]] = []
    for table in tables:
        parsed_rows = _parse_table_rows(table, target_date)
        if len(parsed_rows) > len(best_candidate):
            best_candidate = parsed_rows
    return best_candidate


def _parse_table_rows(table: Any, target_date: date) -> list[dict[str, Any]]:
    rows = table.find_all("tr")
    parsed_rows: list[dict[str, Any]] = []
    seen_hours: set[int] = set()
    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        cell_text = [cell.get_text(" ", strip=True) for cell in cells]
        hour = _parse_hour_value(cell_text[0])
        if hour is None or hour in seen_hours:
            continue

        price_raw = _parse_decimal(cell_text[1])
        if price_raw is None:
            continue
        volume_raw = _parse_decimal(cell_text[2]) if len(cell_text) > 2 else 1000.0
        if volume_raw is None:
            volume_raw = 1000.0

        if price_raw > 500:
            price_uah = price_raw
            price_eur = price_raw / UAH_PER_EUR
        else:
            price_eur = price_raw
            price_uah = price_raw * UAH_PER_EUR
        if not (0 < price_eur < 500):
            continue

        parsed_rows.append(
            _build_market_row(
                timestamp=datetime.combine(target_date, datetime.min.time().replace(hour=hour)),
                price_eur_mwh=price_eur,
                price_uah_mwh=price_uah,
                volume_mwh=volume_raw,
                source="OREE_HTML",
                source_kind=OBSERVED_SOURCE_KIND,
                source_url=OREE_PRICES_URL,
            )
        )
        seen_hours.add(hour)
    return parsed_rows


def _validate_market_data(price_history: pl.DataFrame) -> pl.DataFrame:
    return (
        price_history
        .filter(
            (pl.col("price_eur_mwh") > 0)
            & (pl.col("price_eur_mwh") < 500)
            & (pl.col("volume_mwh") > 0)
        )
        .sort(DEFAULT_TIMESTAMP_COLUMN)
        .with_columns(
            [
                (pl.col("price_eur_mwh") > pl.col("price_eur_mwh").mean() * 2).alias("price_spike"),
                (pl.col("volume_mwh") < 100).alias("low_volume"),
                pl.lit(datetime.now()).alias("fetched_at"),
            ]
        )
    )


def _build_weather_history_for_market_window(
    *,
    start_timestamp: datetime,
    hours: int,
    weather_location: WeatherLocation,
) -> pl.DataFrame:
    return _tag_weather_location(
        _add_solar_features(
            _validate_weather_data(
                pl.DataFrame(
                    _generate_synthetic_weather(
                        forecast_hours=hours,
                        base_time=start_timestamp,
                    )
                )
            ),
            latitude=weather_location.latitude,
        ),
        weather_location=weather_location,
    )


def _overlay_weather_rows(base_weather: pl.DataFrame, live_weather: pl.DataFrame) -> pl.DataFrame:
    if live_weather.height == 0:
        return base_weather

    missing_columns = [column_name for column_name in base_weather.columns if column_name not in live_weather.columns]
    aligned_live_weather = live_weather.with_columns(
        [pl.lit(None).alias(column_name) for column_name in missing_columns]
    ).select(base_weather.columns)
    return (
        pl.concat([base_weather, aligned_live_weather], how="diagonal_relaxed")
        .sort(DEFAULT_TIMESTAMP_COLUMN)
        .unique(subset=[DEFAULT_TIMESTAMP_COLUMN], keep="last")
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )


def _resolve_weather_location(
    config: WeatherLocationConfig,
    *,
    default_location: WeatherLocation | None = None,
) -> WeatherLocation:
    yaml_requested = (
        _clean_optional_text(config.tenant_id) is not None
        or _clean_optional_text(config.location_config_path) is not None
    )
    yaml_location = _resolve_weather_location_from_yaml(
        tenant_id=_clean_optional_text(config.tenant_id),
        config_path=_clean_optional_text(config.location_config_path),
    )
    fallback_location = default_location or yaml_location or _resolve_default_weather_location()
    allow_env_overrides = not yaml_requested or yaml_location is None
    return WeatherLocation(
        latitude=_resolve_coordinate_value(
            configured_value=config.latitude,
            env_var_name="WEATHER_LATITUDE" if allow_env_overrides else None,
            default_value=fallback_location.latitude,
            minimum=-90.0,
            maximum=90.0,
        ),
        longitude=_resolve_coordinate_value(
            configured_value=config.longitude,
            env_var_name="WEATHER_LONGITUDE" if allow_env_overrides else None,
            default_value=fallback_location.longitude,
            minimum=-180.0,
            maximum=180.0,
        ),
        timezone=_resolve_timezone(
            config.timezone,
            default_timezone=fallback_location.timezone,
            env_var_name="WEATHER_TIMEZONE" if allow_env_overrides else None,
        ),
    )


def _resolve_default_weather_location() -> WeatherLocation:
    yaml_location = _resolve_weather_location_from_yaml(
        tenant_id=_clean_optional_text(os.getenv(WEATHER_LOCATION_TENANT_ID_ENV)),
        config_path=_clean_optional_text(os.getenv(WEATHER_LOCATION_CONFIG_PATH_ENV)),
    )
    if yaml_location is not None:
        return yaml_location

    return DEFAULT_WEATHER_LOCATION


def _resolve_weather_location_from_yaml(
    *,
    tenant_id: str | None,
    config_path: str | None,
) -> WeatherLocation | None:
    candidate_paths = _candidate_weather_location_config_paths(config_path)
    for candidate_path in candidate_paths:
        payload = _read_yaml_payload(candidate_path)
        if payload is None:
            continue

        location = _extract_weather_location_from_payload(payload, tenant_id=tenant_id)
        if location is not None:
            return location
    return None


def _candidate_weather_location_config_paths(config_path: str | None) -> list[Path]:
    explicit_path = _path_from_config_value(config_path)
    if explicit_path is not None:
        return [explicit_path]

    repo_root = _repo_root_path()
    candidate_paths = [
        repo_root / "simulations" / "tenants.yml",
        repo_root / "simulations" / "tenants.yaml",
        repo_root / "_legacy_smart-energy-ai" / "customers.yaml",
    ]
    return [candidate_path for candidate_path in candidate_paths if candidate_path.is_file()]


def _repo_root_path() -> Path:
    return Path(__file__).resolve().parents[4]


def _path_from_config_value(raw_path: str | None) -> Path | None:
    cleaned_path = _clean_optional_text(raw_path)
    if cleaned_path is None:
        return None

    candidate_path = Path(cleaned_path)
    if not candidate_path.is_absolute():
        candidate_path = (_repo_root_path() / candidate_path).resolve()
    return candidate_path if candidate_path.is_file() else None


def _read_yaml_payload(config_path: Path) -> dict[str, Any] | None:
    try:
        loaded_payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except OSError as error:
        logger.warning("Failed to read weather location config %s: %s", config_path, error)
        return None
    except yaml.YAMLError as error:
        logger.warning("Failed to parse weather location config %s: %s", config_path, error)
        return None

    if isinstance(loaded_payload, dict):
        return loaded_payload
    return None


def _extract_weather_location_from_payload(
    payload: dict[str, Any],
    *,
    tenant_id: str | None,
) -> WeatherLocation | None:
    tenant_entries = payload.get("tenants")
    if not isinstance(tenant_entries, list):
        tenant_entries = payload.get("customers")
    if not isinstance(tenant_entries, list):
        return None

    selected_entry = _select_tenant_entry(tenant_entries, tenant_id=tenant_id)
    if selected_entry is None:
        return None
    return _location_from_mapping(selected_entry.get("location"))


def list_available_weather_tenants(*, location_config_path: str | None = None) -> list[dict[str, Any]]:
    """Return tenant summaries from the canonical weather location registry."""

    candidate_paths = _candidate_weather_location_config_paths(_clean_optional_text(location_config_path))
    for candidate_path in candidate_paths:
        payload = _read_yaml_payload(candidate_path)
        if payload is None:
            continue

        tenant_entries = payload.get("tenants")
        if not isinstance(tenant_entries, list):
            tenant_entries = payload.get("customers")
        if not isinstance(tenant_entries, list):
            continue

        summaries: list[dict[str, Any]] = []
        for tenant_entry in tenant_entries:
            if not isinstance(tenant_entry, dict):
                continue
            location = _location_from_mapping(tenant_entry.get("location"))
            if location is None:
                continue
            summaries.append(
                {
                    "tenant_id": _clean_optional_text(tenant_entry.get("id")),
                    "name": _clean_optional_text(tenant_entry.get("name")),
                    "type": _clean_optional_text(tenant_entry.get("type")),
                    "latitude": location.latitude,
                    "longitude": location.longitude,
                    "timezone": location.timezone,
                }
            )
        if summaries:
            return summaries
    return []


def resolve_tenant_registry_entry(*, tenant_id: str, location_config_path: str | None = None) -> dict[str, Any]:
    """Resolve a full tenant registry entry and fail if the tenant is unknown."""

    cleaned_tenant_id = _clean_optional_text(tenant_id)
    if cleaned_tenant_id is None:
        raise ValueError("tenant_id must be provided.")

    candidate_paths = _candidate_weather_location_config_paths(_clean_optional_text(location_config_path))
    for candidate_path in candidate_paths:
        payload = _read_yaml_payload(candidate_path)
        if payload is None:
            continue

        tenant_entries = payload.get("tenants")
        if not isinstance(tenant_entries, list):
            tenant_entries = payload.get("customers")
        if not isinstance(tenant_entries, list):
            continue

        selected_entry = _select_tenant_entry(tenant_entries, tenant_id=cleaned_tenant_id)
        if selected_entry is not None:
            return selected_entry
    raise ValueError(f"Unknown tenant_id: {cleaned_tenant_id}")


def resolve_weather_location_for_tenant(*, tenant_id: str, location_config_path: str | None = None) -> WeatherLocation:
    """Resolve a tenant location explicitly and fail if the tenant is unknown."""

    cleaned_tenant_id = _clean_optional_text(tenant_id)
    if cleaned_tenant_id is None:
        raise ValueError("tenant_id must be provided.")

    location = _resolve_weather_location_from_yaml(
        tenant_id=cleaned_tenant_id,
        config_path=_clean_optional_text(location_config_path),
    )
    if location is None:
        raise ValueError(f"Unknown tenant_id: {cleaned_tenant_id}")
    return location


def build_weather_asset_run_config(*, tenant_id: str, location_config_path: str | None = None) -> dict[str, Any]:
    """Build Dagster run config for the weather Bronze asset from a tenant selection."""

    cleaned_tenant_id = _clean_optional_text(tenant_id)
    if cleaned_tenant_id is None:
        raise ValueError("tenant_id must be provided.")

    asset_config: dict[str, Any] = {
        "tenant_id": cleaned_tenant_id,
    }
    cleaned_location_config_path = _clean_optional_text(location_config_path)
    if cleaned_location_config_path is not None:
        asset_config["location_config_path"] = cleaned_location_config_path

    return {
        "ops": {
            "weather_forecast_bronze": {
                "config": asset_config,
            }
        }
    }


def _select_tenant_entry(
    tenant_entries: list[Any],
    *,
    tenant_id: str | None,
) -> dict[str, Any] | None:
    if tenant_id is not None:
        for tenant_entry in tenant_entries:
            if not isinstance(tenant_entry, dict):
                continue
            if _clean_optional_text(tenant_entry.get("id")) == tenant_id:
                return tenant_entry
        return None

    if len(tenant_entries) == 1 and isinstance(tenant_entries[0], dict):
        return tenant_entries[0]
    return None


def _location_from_mapping(raw_location: Any) -> WeatherLocation | None:
    if not isinstance(raw_location, dict):
        return None

    latitude = _extract_float_value(raw_location, keys=("lat", "latitude"))
    longitude = _extract_float_value(raw_location, keys=("lon", "lng", "longitude"))
    timezone = _extract_text_value(raw_location, keys=("timezone", "tz"))
    if latitude is None or longitude is None or timezone is None:
        return None
    return WeatherLocation(latitude=latitude, longitude=longitude, timezone=timezone)


def _extract_float_value(mapping: dict[str, Any], *, keys: tuple[str, ...]) -> float | None:
    for key in keys:
        if key not in mapping:
            continue
        try:
            return float(mapping[key])
        except (TypeError, ValueError):
            return None
    return None


def _extract_text_value(mapping: dict[str, Any], *, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        if key not in mapping:
            continue
        cleaned_value = _clean_optional_text(mapping[key])
        if cleaned_value is not None:
            return cleaned_value
    return None


def _clean_optional_text(value: object) -> str | None:
    if value is None:
        return None
    cleaned_value = str(value).strip()
    return cleaned_value or None


def _tenant_ids_from_csv(value: str) -> list[str]:
    tenant_ids = [item.strip() for item in value.split(",") if item.strip()]
    if tenant_ids:
        return tenant_ids
    return [
        str(tenant["tenant_id"])
        for tenant in list_available_weather_tenants()
        if tenant.get("tenant_id") is not None
    ]


def _date_range(start_date: date, end_date: date) -> list[date]:
    return [
        start_date + timedelta(days=day_offset)
        for day_offset in range((end_date - start_date).days + 1)
    ]


def _month_range(start_date: date, end_date: date) -> list[date]:
    months: list[date] = []
    current = date(start_date.year, start_date.month, 1)
    final = date(end_date.year, end_date.month, 1)
    while current <= final:
        months.append(current)
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


def _pause_between_oree_month_requests() -> None:
    sleep(OREE_DATA_VIEW_MONTH_REQUEST_PAUSE_SECONDS)


def _add_metadata(
    context: dg.AssetExecutionContext | None,
    metadata: dict[str, str | float | int],
) -> None:
    if context is not None:
        context.add_output_metadata(metadata)


def _location_from_weather_frame(weather_frame: pl.DataFrame) -> WeatherLocation | None:
    required_columns = {"location_latitude", "location_longitude", "location_timezone"}
    if weather_frame.height == 0 or not required_columns.issubset(set(weather_frame.columns)):
        return None

    first_row = weather_frame.select(
        ["location_latitude", "location_longitude", "location_timezone"]
    ).to_dicts()[0]
    return WeatherLocation(
        latitude=float(first_row["location_latitude"]),
        longitude=float(first_row["location_longitude"]),
        timezone=str(first_row["location_timezone"]),
    )


def _select_market_weather_features(weather_history: pl.DataFrame) -> pl.DataFrame:
    renamed_columns = {
        column_name: f"weather_{column_name}"
        for column_name in MARKET_WEATHER_FEATURE_COLUMNS
    }
    return weather_history.select(
        [DEFAULT_TIMESTAMP_COLUMN, *MARKET_WEATHER_FEATURE_COLUMNS]
    ).rename(renamed_columns)


def _tag_weather_location(
    weather_frame: pl.DataFrame,
    *,
    weather_location: WeatherLocation,
) -> pl.DataFrame:
    return weather_frame.with_columns(
        [
            pl.lit(weather_location.latitude).alias("location_latitude"),
            pl.lit(weather_location.longitude).alias("location_longitude"),
            pl.lit(weather_location.timezone).alias("location_timezone"),
        ]
    )


def _resolve_coordinate_value(
    *,
    configured_value: float | None,
    env_var_name: str | None,
    default_value: float,
    minimum: float,
    maximum: float,
) -> float:
    if configured_value is not None:
        return _clamp_numeric_value(configured_value, minimum=minimum, maximum=maximum)
    if env_var_name is None:
        return default_value
    return _clamp_coordinate(os.getenv(env_var_name), default=default_value, minimum=minimum, maximum=maximum)


def _clamp_numeric_value(value: float, *, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, float(value)))


def _clamp_coordinate(raw_value: str | None, *, default: float, minimum: float, maximum: float) -> float:
    if raw_value is None:
        return default
    try:
        numeric_value = float(raw_value)
    except ValueError:
        return default
    return max(minimum, min(maximum, numeric_value))


def _resolve_timezone(
    configured_timezone: str | None,
    *,
    default_timezone: str,
    env_var_name: str | None = "WEATHER_TIMEZONE",
) -> str:
    if configured_timezone is not None:
        stripped_timezone = configured_timezone.strip()
        if stripped_timezone:
            return stripped_timezone

    if env_var_name is None:
        return default_timezone

    env_timezone = os.getenv(env_var_name, default_timezone).strip()
    if env_timezone:
        return env_timezone
    return default_timezone


def _fetch_openmeteo_data(latitude: float, longitude: float, timezone: str) -> list[dict[str, Any]] | None:
    params: dict[str, str | int | float] = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(
            [
                "temperature_2m",
                "shortwave_radiation",
                "wind_speed_10m",
                "cloud_cover",
                "precipitation",
                "surface_pressure",
                "relative_humidity_2m",
            ]
        ),
        "forecast_days": 7,
        "timezone": timezone,
        "wind_speed_unit": "ms",
    }
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            response = client.get(OPEN_METEO_URL, params=params)
            response.raise_for_status()
        payload = response.json()
        hourly = payload.get("hourly", {})
        timestamps = hourly.get("time", [])
        weather_rows: list[dict[str, Any]] = []
        for index, timestamp_text in enumerate(timestamps):
            weather_rows.append(
                {
                    DEFAULT_TIMESTAMP_COLUMN: datetime.fromisoformat(timestamp_text),
                    "temperature": float(hourly.get("temperature_2m", [20.0])[index] or 20.0),
                    "solar_radiation": float(hourly.get("shortwave_radiation", [0.0])[index] or 0.0),
                    "wind_speed": float(hourly.get("wind_speed_10m", [5.0])[index] or 5.0),
                    "cloudcover": float(hourly.get("cloud_cover", [50.0])[index] or 50.0),
                    "precipitation": float(hourly.get("precipitation", [0.0])[index] or 0.0),
                    "pressure": float(hourly.get("surface_pressure", [1013.0])[index] or 1013.0),
                    "humidity": float(hourly.get("relative_humidity_2m", [60.0])[index] or 60.0),
                    "source": "OPEN_METEO",
                    "source_kind": OBSERVED_SOURCE_KIND,
                    "source_url": OPEN_METEO_URL,
                }
            )
        logger.info("Fetched %s Open-Meteo hourly rows.", len(weather_rows))
        return weather_rows
    except Exception as error:
        logger.warning("Open-Meteo fetch failed: %s", error)
        return None


def _fetch_openmeteo_historical_data(
    latitude: float,
    longitude: float,
    timezone: str,
    *,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]] | None:
    params: dict[str, str | float] = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(
            [
                "temperature_2m",
                "shortwave_radiation",
                "wind_speed_10m",
                "cloud_cover",
                "precipitation",
                "surface_pressure",
                "relative_humidity_2m",
            ]
        ),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "timezone": timezone,
        "wind_speed_unit": "ms",
    }
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            response = client.get(OPEN_METEO_HISTORICAL_URL, params=params)
            response.raise_for_status()
        payload = response.json()
        hourly = payload.get("hourly", {})
        timestamps = hourly.get("time", [])
        weather_rows: list[dict[str, Any]] = []
        for index, timestamp_text in enumerate(timestamps):
            weather_rows.append(
                {
                    DEFAULT_TIMESTAMP_COLUMN: datetime.fromisoformat(timestamp_text),
                    "temperature": float(hourly.get("temperature_2m", [20.0])[index] or 20.0),
                    "solar_radiation": float(hourly.get("shortwave_radiation", [0.0])[index] or 0.0),
                    "wind_speed": float(hourly.get("wind_speed_10m", [5.0])[index] or 5.0),
                    "cloudcover": float(hourly.get("cloud_cover", [50.0])[index] or 50.0),
                    "precipitation": float(hourly.get("precipitation", [0.0])[index] or 0.0),
                    "pressure": float(hourly.get("surface_pressure", [1013.0])[index] or 1013.0),
                    "humidity": float(hourly.get("relative_humidity_2m", [60.0])[index] or 60.0),
                    "source": "OPEN_METEO_HISTORICAL",
                    "source_kind": OBSERVED_SOURCE_KIND,
                    "source_url": OPEN_METEO_HISTORICAL_URL,
                }
            )
        logger.info("Fetched %s Open-Meteo historical hourly rows.", len(weather_rows))
        return weather_rows
    except Exception as error:
        logger.warning("Open-Meteo historical fetch failed: %s", error)
        return None


def _generate_synthetic_weather(
    *,
    forecast_hours: int = DEFAULT_WEATHER_FORECAST_HOURS,
    base_time: datetime | None = None,
) -> list[dict[str, Any]]:
    base_time = _round_to_hour(base_time or datetime.now())
    synthetic_rows: list[dict[str, Any]] = []
    for hour_offset in range(forecast_hours):
        timestamp = base_time + timedelta(hours=hour_offset)
        seasonal_factor = 0.55 + 0.45 * math.sin(((timestamp.timetuple().tm_yday - 80) / 365.0) * 2.0 * math.pi)
        solar_factor = max(0.0, math.sin(((timestamp.hour - 6) / 12.0) * math.pi))
        cloudcover = max(0.0, min(100.0, 55.0 + 25.0 * math.sin((hour_offset / 18.0) * math.pi)))
        synthetic_rows.append(
            {
                DEFAULT_TIMESTAMP_COLUMN: timestamp,
                "temperature": 10.0 + 12.0 * seasonal_factor + 5.0 * math.sin(((timestamp.hour - 6) / 24.0) * 2.0 * math.pi),
                "solar_radiation": 900.0 * solar_factor * (1.0 - cloudcover / 100.0),
                "wind_speed": max(0.0, 5.5 + 2.0 * math.sin((hour_offset / 9.0) * math.pi)),
                "cloudcover": cloudcover,
                "precipitation": 2.5 if cloudcover > 80.0 and timestamp.hour in {5, 6, 7, 18, 19} else 0.0,
                "pressure": 1008.0 + 7.0 * math.sin((hour_offset / 48.0) * 2.0 * math.pi),
                "humidity": max(20.0, min(100.0, 65.0 + 20.0 * math.cos((hour_offset / 12.0) * math.pi))),
                "source": "SYNTHETIC",
                "source_kind": SYNTHETIC_SOURCE_KIND,
                "source_url": SYNTHETIC_WEATHER_SOURCE_URL,
            }
        )
    return synthetic_rows


def _validate_weather_data(weather_frame: pl.DataFrame) -> pl.DataFrame:
    return (
        weather_frame
        .with_columns(
            [
                pl.col("temperature").clip(-40, 50).alias("temperature"),
                pl.col("solar_radiation").clip(0, 1200).alias("solar_radiation"),
                pl.col("wind_speed").clip(0, 50).alias("wind_speed"),
                pl.col("cloudcover").clip(0, 100).alias("cloudcover"),
                pl.col("humidity").clip(0, 100).alias("humidity"),
                pl.col("pressure").clip(950, 1050).alias("pressure"),
                pl.col("precipitation").clip(0, 100).alias("precipitation"),
            ]
        )
        .sort(DEFAULT_TIMESTAMP_COLUMN)
        .with_columns(
            [
                (pl.col("solar_radiation") > 1000).alias("high_solar"),
                (pl.col("wind_speed") > 15).alias("high_wind"),
                (pl.col("precipitation") > 10).alias("heavy_rain"),
                pl.lit(datetime.now()).alias("fetched_at"),
            ]
        )
    )


def _add_solar_features(weather_frame: pl.DataFrame, *, latitude: float) -> pl.DataFrame:
    solar_elevation_expression = _calculate_solar_elevation(
        pl.col(DEFAULT_TIMESTAMP_COLUMN),
        latitude=latitude,
    )
    return weather_frame.with_columns(
        [
            pl.col(DEFAULT_TIMESTAMP_COLUMN).dt.hour().alias("hour"),
            pl.col(DEFAULT_TIMESTAMP_COLUMN).dt.ordinal_day().alias("day_of_year"),
            solar_elevation_expression.alias("solar_elevation"),
            ((100 - pl.col("cloudcover")) / 100).alias("clear_sky_index"),
            (pl.col("solar_radiation") * (100 - pl.col("cloudcover")) / 100).alias("effective_solar"),
            (solar_elevation_expression > 0).alias("is_daylight"),
            pl.when(pl.col(DEFAULT_TIMESTAMP_COLUMN).dt.month().is_in([12, 1, 2]))
            .then(pl.lit("winter"))
            .when(pl.col(DEFAULT_TIMESTAMP_COLUMN).dt.month().is_in([3, 4, 5]))
            .then(pl.lit("spring"))
            .when(pl.col(DEFAULT_TIMESTAMP_COLUMN).dt.month().is_in([6, 7, 8]))
            .then(pl.lit("summer"))
            .otherwise(pl.lit("autumn"))
            .alias("season"),
            pl.when(pl.col("cloudcover") < 25)
            .then(pl.lit("clear"))
            .when(pl.col("cloudcover") < 75)
            .then(pl.lit("partly_cloudy"))
            .otherwise(pl.lit("cloudy"))
            .alias("sky_condition"),
        ]
    )


def _calculate_solar_elevation(timestamp_expression: pl.Expr, *, latitude: float) -> pl.Expr:
    latitude_radians = math.radians(latitude)
    day_of_year = timestamp_expression.dt.ordinal_day().cast(pl.Float64)
    hour_of_day = timestamp_expression.dt.hour().cast(pl.Float64)
    solar_declination = pl.lit(math.radians(23.44)) * (
        ((pl.lit(2.0 * math.pi) / pl.lit(365.0)) * (pl.lit(284.0) + day_of_year)).sin()
    )
    hour_angle = pl.lit(math.pi / 12.0) * (hour_of_day - pl.lit(12.0))
    solar_altitude_sine = (
        pl.lit(math.sin(latitude_radians)) * solar_declination.sin()
        + pl.lit(math.cos(latitude_radians)) * solar_declination.cos() * hour_angle.cos()
    )
    return solar_altitude_sine.clip(-1.0, 1.0).arcsin() * pl.lit(180.0 / math.pi)


__all__ = [
    "BRONZE_INGESTION_ASSETS",
    "REAL_DATA_BENCHMARK_BRONZE_ASSETS",
    "build_observed_historical_weather_frame",
    "build_observed_market_price_history",
    "build_weather_forecast_window",
    "build_weather_asset_run_config",
    "build_demo_market_price_history",
    "build_synthetic_market_price_history",
    "enrich_market_price_history_with_weather",
    "list_available_weather_tenants",
    "resolve_tenant_registry_entry",
    "resolve_weather_location_for_tenant",
    "weather_forecast_bronze",
]
