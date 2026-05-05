from datetime import UTC, datetime

import dagster as dg

from smart_arbitrage.assets.bronze.grid_events import (
	GRID_EVENT_BRONZE_ASSETS,
	UKRENERGO_TELEGRAM_SOURCE_URL,
	UkrenergoGridEventsConfig,
	parse_ukrenergo_telegram_posts,
	ukrenergo_grid_events_bronze,
)
from smart_arbitrage.defs import defs
from smart_arbitrage.resources.grid_event_store import InMemoryGridEventStore


TELEGRAM_FIXTURE = """
<section class="tgme_channel_history">
	<div class="tgme_widget_message js-widget_message" data-post="Ukrenergo/4914">
		<div class="tgme_widget_message_text js-message_text" dir="auto">
			СТАН ЕНЕРГОСИСТЕМИ<br/>
			Споживання електроенергії відповідає сезонним показникам<br/>
			НАСЛІДКИ ОБСТРІЛІВ<br/>
			Внаслідок російських дронових ударів є нові знеструмлення на Харківщині та Одещині.<br/>
			Активне енергоспоживання сьогодні доцільно перенести на період найбільш продуктивної роботи сонячних електростанцій.
			Будь ласка, обмежте користування потужними електроприладами з 18:00 до 22:00.
		</div>
		<a class="tgme_widget_message_date" href="https://t.me/Ukrenergo/4914">
			<time datetime="2026-04-30T07:00:00+00:00" class="time">07:00</time>
		</a>
	</div>
</section>
"""


def test_ukrenergo_telegram_parser_extracts_event_tags() -> None:
	events = parse_ukrenergo_telegram_posts(
		TELEGRAM_FIXTURE,
		fetched_at=datetime(2026, 4, 30, 7, 5, tzinfo=UTC),
	)

	assert len(events) == 1
	event = events[0]
	assert event.post_id == "Ukrenergo/4914"
	assert event.post_url == "https://t.me/Ukrenergo/4914"
	assert event.source_url == UKRENERGO_TELEGRAM_SOURCE_URL
	assert event.source_kind == "observed"
	assert event.energy_system_status is True
	assert event.shelling_damage is True
	assert event.outage_or_restriction is True
	assert event.consumption_change == "stable"
	assert event.solar_shift_advice is True
	assert event.evening_saving_request is True
	assert event.affected_oblasts == ["Kharkiv", "Odesa"]


def test_ukrenergo_telegram_parser_does_not_tag_non_operational_posts() -> None:
	fixture = """
	<section class="tgme_channel_history">
		<div class="tgme_widget_message js-widget_message" data-post="Ukrenergo/4932">
			<div class="tgme_widget_message_text js-message_text" dir="auto">
				Запрошуємо на стажування Energy Hub від НЕК Укренерго.
				Кожен мегават потужності важливий для української енергосистеми.
			</div>
			<time datetime="2026-05-05T08:24:51+00:00" class="time">08:24</time>
		</div>
	</section>
	"""

	events = parse_ukrenergo_telegram_posts(
		fixture,
		fetched_at=datetime(2026, 5, 5, 8, 30, tzinfo=UTC),
	)

	assert len(events) == 1
	event = events[0]
	assert event.energy_system_status is False
	assert event.shelling_damage is False
	assert event.outage_or_restriction is False
	assert event.consumption_change == "unknown"
	assert event.solar_shift_advice is False
	assert event.evening_saving_request is False


def test_ukrenergo_grid_events_bronze_persists_observed_posts(monkeypatch) -> None:
	store = InMemoryGridEventStore()
	monkeypatch.setattr("smart_arbitrage.assets.bronze.grid_events.get_grid_event_store", lambda: store)
	monkeypatch.setattr(
		"smart_arbitrage.assets.bronze.grid_events._fetch_ukrenergo_telegram_html",
		lambda: TELEGRAM_FIXTURE,
	)

	frame = ukrenergo_grid_events_bronze(
		dg.build_asset_context(),
		UkrenergoGridEventsConfig(max_posts=10),
	)

	assert frame.height == 1
	assert store.list_grid_event_frame().height == 1
	assert frame.row(0, named=True)["source_kind"] == "observed"


def test_ukrenergo_grid_event_asset_is_registered() -> None:
	asset_keys = {asset.key.to_user_string() for asset in GRID_EVENT_BRONZE_ASSETS}
	registered_asset_keys = {asset.key.to_user_string() for asset in defs.assets or []}

	assert {"ukrenergo_grid_events_bronze"}.issubset(asset_keys)
	assert asset_keys.issubset(registered_asset_keys)
