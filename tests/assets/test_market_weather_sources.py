import json
from datetime import date, datetime, timedelta
from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.assets.bronze import market_weather
from smart_arbitrage.assets.bronze.market_weather import (
    WeatherLocationConfig,
    _fetch_oree_data_view_prices,
    build_observed_historical_weather_frame,
    build_observed_market_price_history,
    build_synthetic_market_price_history,
    weather_forecast_bronze,
)
import smart_arbitrage.assets.mvp_demo as mvp_demo
from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_PRICE_COLUMN, DEFAULT_TIMESTAMP_COLUMN
from smart_arbitrage.resources.market_data_store import (
    DEFAULT_TENANT_ID,
    market_price_observations_from_frame,
    weather_observations_from_frame,
)


class _RecordingMarketDataStore:
    def __init__(self) -> None:
        self.market_observations: list[Any] = []
        self.weather_observations: list[Any] = []

    def upsert_market_prices(self, observations: list[Any]) -> None:
        self.market_observations.extend(observations)

    def upsert_weather_observations(self, observations: list[Any]) -> None:
        self.weather_observations.extend(observations)


def test_oree_data_view_parser_accepts_json_wrapped_in_html_content_type(
    monkeypatch,
) -> None:
    table_html = _build_oree_table_html(target_date="04.05.2026")
    response_text = json.dumps({"content": table_html}).replace("</", "<\\/")
    recorded_posts: list[dict[str, Any]] = []

    class FakeResponse:
        headers = {"content-type": "text/html; charset=windows-1251"}
        text = response_text

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, str]:
            return {"content": table_html}

    class FakeClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            return None

        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def post(self, url: str, *, data: dict[str, str], headers: dict[str, str]) -> FakeResponse:
            recorded_posts.append({"url": url, "data": data, "headers": headers})
            return FakeResponse()

    monkeypatch.setattr(market_weather.httpx, "Client", FakeClient)

    rows = _fetch_oree_data_view_prices(date(2026, 5, 4))

    assert rows is not None
    assert len(rows) == 24
    assert recorded_posts == [
        {
            "url": market_weather.OREE_DATA_VIEW_URL,
            "data": {"date": "05.2026", "market": "DAM", "zone": "IPS"},
            "headers": {
                "Referer": market_weather.OREE_PRICES_URL,
                "X-Requested-With": "XMLHttpRequest",
            },
        }
    ]
    assert rows[0][DEFAULT_TIMESTAMP_COLUMN] == datetime(2026, 5, 4, 0)
    assert rows[0][DEFAULT_PRICE_COLUMN] == 1000.0
    assert rows[0]["source"] == "OREE_DATA_VIEW"
    assert rows[0]["source_kind"] == "observed"
    assert rows[0]["source_url"] == market_weather.OREE_DATA_VIEW_URL
    assert rows[0]["market_venue"] == "DAM"
    assert rows[0]["market_zone"] == "IPS"


def test_weather_asset_tags_and_persists_tenant_specific_observations(
    monkeypatch,
) -> None:
    store = _RecordingMarketDataStore()

    def fake_fetch_openmeteo_data(latitude: float, longitude: float, timezone: str) -> list[dict[str, Any]]:
        assert latitude == 49.84
        assert longitude == 24.03
        assert timezone == "Europe/Kyiv"
        return [
            {
                DEFAULT_TIMESTAMP_COLUMN: datetime(2026, 5, 4, 10),
                "temperature": 19.0,
                "solar_radiation": 420.0,
                "wind_speed": 4.2,
                "cloudcover": 35.0,
                "precipitation": 0.0,
                "pressure": 1010.0,
                "humidity": 58.0,
                "source": "OPEN_METEO",
                "source_kind": "observed",
                "source_url": market_weather.OPEN_METEO_URL,
            }
        ]

    monkeypatch.setattr(market_weather, "_fetch_openmeteo_data", fake_fetch_openmeteo_data)
    monkeypatch.setattr(market_weather, "get_market_data_store", lambda: store)
    context = dg.build_asset_context()

    weather_frame = weather_forecast_bronze(
        context,
        WeatherLocationConfig(
            tenant_id="client_002_lviv_office",
            location_config_path="simulations/tenants.yml",
        ),
    )

    assert weather_frame.select("source").to_series().to_list() == ["OPEN_METEO"]
    assert weather_frame.select("source_kind").to_series().to_list() == ["observed"]
    assert weather_frame.select("source_url").to_series().to_list() == [market_weather.OPEN_METEO_URL]
    assert weather_frame.select("location_latitude").to_series().to_list() == [49.84]
    assert len(store.weather_observations) == 1
    assert store.weather_observations[0].tenant_id == "client_002_lviv_office"
    assert store.weather_observations[0].source_kind == "observed"


def test_market_price_observations_preserve_synthetic_provenance() -> None:
    price_history = build_synthetic_market_price_history(
        history_hours=2,
        forecast_hours=0,
        now=datetime(2026, 5, 4, 12),
    ).with_columns(
        pl.lit("SYNTHETIC").alias("weather_source")
    )

    observations = market_price_observations_from_frame(price_history)

    assert len(observations) == 2
    assert observations[0].source == "SYNTHETIC"
    assert observations[0].source_kind == "synthetic"
    assert observations[0].source_url == market_weather.SYNTHETIC_MARKET_SOURCE_URL
    assert observations[0].market_venue == "DAM"
    assert observations[0].market_zone == "IPS"


def test_live_market_overlay_preserves_requested_history_window(monkeypatch) -> None:
    now = datetime(2026, 5, 4, 14)

    def fake_fetch_oree_prices(target_date: date) -> list[dict[str, Any]]:
        return [
            market_weather._build_market_row(
                timestamp=datetime(target_date.year, target_date.month, target_date.day, hour_index),
                price_eur_mwh=75.0,
                price_uah_mwh=3000.0 + hour_index,
                volume_mwh=1000.0,
                source="OREE_DATA_VIEW",
                source_kind="observed",
                source_url=market_weather.OREE_DATA_VIEW_URL,
            )
            for hour_index in range(24)
        ]

    monkeypatch.setattr(market_weather, "_fetch_oree_prices", fake_fetch_oree_prices)

    price_history = market_weather.build_demo_market_price_history(
        history_hours=48,
        forecast_hours=6,
        now=now,
    )

    expected_end = datetime(2026, 5, 4, 20)
    expected_start = expected_end - timedelta(hours=47)
    assert price_history.height == 48
    assert price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(0) == expected_start
    assert price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(-1) == expected_end
    assert "OREE_DATA_VIEW" in set(price_history.select("source").to_series().to_list())


def test_observed_market_price_history_rejects_synthetic_fallback(monkeypatch) -> None:
    def fake_fetch_oree_prices(target_date: date) -> list[dict[str, Any]] | None:
        if target_date == date(2026, 5, 1):
            return [
                market_weather._build_market_row(
                    timestamp=datetime(2026, 5, 1, hour_index),
                    price_eur_mwh=50.0 + hour_index,
                    price_uah_mwh=2000.0 + hour_index,
                    volume_mwh=900.0,
                    source="OREE_DATA_VIEW",
                    source_kind="observed",
                    source_url=market_weather.OREE_DATA_VIEW_URL,
                )
                for hour_index in range(24)
            ]
        return None

    monkeypatch.setattr(market_weather, "_fetch_oree_prices", fake_fetch_oree_prices)

    price_history = build_observed_market_price_history(
        start_date=date(2026, 5, 1),
        end_date=date(2026, 5, 1),
    )

    assert price_history.height == 24
    assert set(price_history.select("source_kind").to_series().to_list()) == {"observed"}
    assert price_history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(0) == datetime(2026, 5, 1)


def test_observed_market_price_history_fails_when_required_day_missing(monkeypatch) -> None:
    monkeypatch.setattr(market_weather, "_fetch_oree_prices", lambda target_date: None)

    try:
        build_observed_market_price_history(
            start_date=date(2026, 5, 1),
            end_date=date(2026, 5, 1),
        )
    except ValueError as error:
        assert "observed OREE DAM rows" in str(error)
    else:
        raise AssertionError("Expected observed-only backfill to reject missing OREE data.")


def test_observed_historical_weather_frame_tags_tenant_and_source(monkeypatch) -> None:
    def fake_fetch_historical_weather(
        latitude: float,
        longitude: float,
        timezone: str,
        *,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        assert latitude == 49.84
        assert longitude == 24.03
        assert timezone == "Europe/Kyiv"
        assert start_date == date(2026, 5, 1)
        assert end_date == date(2026, 5, 1)
        return [
            {
                DEFAULT_TIMESTAMP_COLUMN: datetime(2026, 5, 1, 10),
                "temperature": 16.0,
                "solar_radiation": 300.0,
                "wind_speed": 5.0,
                "cloudcover": 20.0,
                "precipitation": 0.0,
                "pressure": 1012.0,
                "humidity": 55.0,
                "source": "OPEN_METEO_HISTORICAL",
                "source_kind": "observed",
                "source_url": market_weather.OPEN_METEO_HISTORICAL_URL,
            }
        ]

    monkeypatch.setattr(market_weather, "_fetch_openmeteo_historical_data", fake_fetch_historical_weather)

    weather_frame = build_observed_historical_weather_frame(
        tenant_id="client_002_lviv_office",
        start_date=date(2026, 5, 1),
        end_date=date(2026, 5, 1),
        location_config_path="simulations/tenants.yml",
    )

    assert weather_frame.height == 1
    assert weather_frame.row(0, named=True)["tenant_id"] == "client_002_lviv_office"
    assert weather_frame.row(0, named=True)["source"] == "OPEN_METEO_HISTORICAL"
    assert weather_frame.row(0, named=True)["source_kind"] == "observed"


def test_dam_price_history_asset_persists_market_observations(monkeypatch) -> None:
    store = _RecordingMarketDataStore()
    price_history = build_synthetic_market_price_history(
        history_hours=2,
        forecast_hours=0,
        now=datetime(2026, 5, 4, 12),
    ).with_columns(
        pl.lit("SYNTHETIC").alias("weather_source")
    )

    monkeypatch.setattr(
        mvp_demo,
        "build_demo_market_price_history",
        lambda *, history_hours, forecast_hours: price_history,
    )
    monkeypatch.setattr(
        mvp_demo,
        "enrich_market_price_history_with_weather",
        lambda price_history, weather_forecast_bronze: price_history,
    )
    monkeypatch.setattr(mvp_demo, "get_market_data_store", lambda: store)

    result = mvp_demo.dam_price_history(dg.build_asset_context(), pl.DataFrame())

    assert result.height == 2
    assert len(store.market_observations) == 2
    assert store.market_observations[0].source_kind == "synthetic"


def test_weather_observations_default_to_explicit_default_tenant() -> None:
    weather_frame = pl.DataFrame(
        {
            DEFAULT_TIMESTAMP_COLUMN: [datetime(2026, 5, 4, 10)],
            "temperature": [19.0],
            "solar_radiation": [420.0],
            "wind_speed": [4.2],
            "cloudcover": [35.0],
            "precipitation": [0.0],
            "pressure": [1010.0],
            "humidity": [58.0],
            "source": ["OPEN_METEO"],
            "source_kind": ["observed"],
            "source_url": [market_weather.OPEN_METEO_URL],
            "fetched_at": [datetime(2026, 5, 4, 9)],
            "location_latitude": [49.84],
            "location_longitude": [24.03],
            "location_timezone": ["Europe/Kyiv"],
        }
    )

    observations = weather_observations_from_frame(weather_frame, tenant_id=None)

    assert len(observations) == 1
    assert observations[0].tenant_id == DEFAULT_TENANT_ID


def _build_oree_table_html(*, target_date: str) -> str:
    hourly_cells = "".join(f"<td>{1000 + hour_index}.00</td>" for hour_index in range(24))
    return (
        "<table>"
        "<thead><tr><th>Date</th></tr></thead>"
        f"<tbody><tr><td>{target_date}</td>{hourly_cells}</tr></tbody>"
        "</table>"
    )
