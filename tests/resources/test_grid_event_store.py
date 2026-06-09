from datetime import UTC, datetime

from smart_arbitrage.resources.grid_event_store import (
	GridEventObservation,
	InMemoryGridEventStore,
	resolve_grid_event_store_dsn,
	grid_event_observations_to_frame,
)


def test_grid_event_store_upserts_and_lists_latest_observed_events() -> None:
	store = InMemoryGridEventStore()
	older_event = _grid_event("Ukrenergo/4913", datetime(2026, 4, 29, 7, tzinfo=UTC))
	latest_event = _grid_event("Ukrenergo/4914", datetime(2026, 4, 30, 7, tzinfo=UTC))

	store.upsert_grid_events([older_event, latest_event, latest_event])

	frame = store.list_grid_event_frame(source_kind="observed")
	assert frame.height == 2
	assert frame.select("post_id").to_series().to_list() == ["Ukrenergo/4913", "Ukrenergo/4914"]
	assert frame.row(1, named=True)["affected_oblasts"] == ["Kharkiv"]


def test_grid_event_observations_to_frame_preserves_source_metadata() -> None:
	frame = grid_event_observations_to_frame(
		[
			_grid_event(
				"Ukrenergo/4914",
				datetime(2026, 4, 30, 7, tzinfo=UTC),
			)
		]
	)

	row = frame.row(0, named=True)
	assert row["post_url"] == "https://t.me/Ukrenergo/4914"
	assert row["source"] == "UKRENERGO_TELEGRAM"
	assert row["source_kind"] == "observed"
	assert row["source_url"] == "https://t.me/s/Ukrenergo"


def test_grid_event_store_dsn_falls_back_to_market_data_dsn(monkeypatch) -> None:
	monkeypatch.delenv("SMART_ARBITRAGE_GRID_EVENT_DSN", raising=False)
	monkeypatch.setenv(
		"SMART_ARBITRAGE_MARKET_DATA_DSN",
		"postgresql://smart:arbitrage@postgres:5432/smart_arbitrage",
	)

	assert (
		resolve_grid_event_store_dsn()
		== "postgresql://smart:arbitrage@postgres:5432/smart_arbitrage"
	)

	monkeypatch.setenv("SMART_ARBITRAGE_GRID_EVENT_DSN", "postgresql://grid-events")

	assert resolve_grid_event_store_dsn() == "postgresql://grid-events"


def _grid_event(post_id: str, published_at: datetime) -> GridEventObservation:
	return GridEventObservation(
		post_id=post_id,
		post_url=f"https://t.me/{post_id}",
		published_at=published_at,
		fetched_at=datetime(2026, 4, 30, 7, 5, tzinfo=UTC),
		raw_text="СТАН ЕНЕРГОСИСТЕМИ. Є знеструмлення на Харківщині.",
		source="UKRENERGO_TELEGRAM",
		source_kind="observed",
		source_url="https://t.me/s/Ukrenergo",
		energy_system_status=True,
		shelling_damage=False,
		outage_or_restriction=True,
		consumption_change="unknown",
		solar_shift_advice=False,
		evening_saving_request=False,
		affected_oblasts=["Kharkiv"],
	)
