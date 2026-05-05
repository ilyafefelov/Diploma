"""Bronze ingestion for public Ukrenergo operational grid-event messages."""

from datetime import UTC, datetime
import html
import logging
import re
from typing import Final

from bs4 import BeautifulSoup
import dagster as dg
import httpx
import polars as pl

from smart_arbitrage.resources.grid_event_store import (
    GridEventObservation,
    get_grid_event_store,
    grid_event_observations_to_frame,
)

logger = logging.getLogger(__name__)

UKRENERGO_TELEGRAM_SOURCE_URL: Final[str] = "https://t.me/s/Ukrenergo"
UKRENERGO_TELEGRAM_SOURCE: Final[str] = "UKRENERGO_TELEGRAM"

_ENERGY_STATUS_PATTERNS: Final[tuple[str, ...]] = (
    "стан енергосистеми",
    "стан енергосистем",
)
_SHELLING_DAMAGE_PATTERNS: Final[tuple[str, ...]] = (
    "обстр",
    "дрон",
    "ракет",
    "пошкоджен",
    "ворож",
)
_OUTAGE_PATTERNS: Final[tuple[str, ...]] = (
    "знеструм",
    "відключ",
    "обмежен",
    "графік",
    "blackout",
    "outage",
    "restriction",
)
_SOLAR_SHIFT_PATTERNS: Final[tuple[str, ...]] = ("сонячн", "сес", "solar")
_EVENING_SAVING_PATTERNS: Final[tuple[str, ...]] = ("18:00", "22:00", "ощадлив", "обмежте користування")

_OBLAST_PATTERNS: Final[tuple[tuple[str, tuple[str, ...]], ...]] = (
    ("Kyiv", ("київ", "киев", "kyiv", "kiev")),
    ("Lviv", ("львів", "львов", "lviv")),
    ("Dnipropetrovsk", ("дніпропетров", "дніпро", "дніпров", "dnipro", "dnipropetrovsk")),
    ("Kharkiv", ("харків", "харьков", "kharkiv")),
    ("Odesa", ("одес", "одещ", "odessa", "odesa")),
    ("Donetsk", ("донець", "донеч", "donetsk")),
    ("Sumy", ("сумсь", "сумщ", "sumy")),
    ("Chernihiv", ("черніг", "chernihiv")),
    ("Zaporizhzhia", ("запоріж", "zaporizh")),
    ("Mykolaiv", ("микола", "mykolaiv")),
    ("Kherson", ("херсон", "kherson")),
    ("Poltava", ("полтав", "poltava")),
    ("Cherkasy", ("черкас", "cherkasy")),
)


class UkrenergoGridEventsConfig(dg.Config):
    max_posts: int = 20


@dg.asset(group_name="bronze", tags={"medallion": "bronze", "domain": "grid_events"})
def ukrenergo_grid_events_bronze(context, config: UkrenergoGridEventsConfig) -> pl.DataFrame:
    """Observed public Ukrenergo Telegram posts with transparent rule-based event tags."""

    fetched_at = datetime.now(tz=UTC)
    raw_html = _fetch_ukrenergo_telegram_html()
    observations = parse_ukrenergo_telegram_posts(raw_html or "", fetched_at=fetched_at)
    if config.max_posts > 0:
        observations = observations[-config.max_posts :]
    get_grid_event_store().upsert_grid_events(observations)
    frame = grid_event_observations_to_frame(observations)
    if context is not None:
        context.add_output_metadata(
            {
                "rows": frame.height,
                "source_url": UKRENERGO_TELEGRAM_SOURCE_URL,
                "source_kind": "observed",
                "affected_oblast_count": _affected_oblast_count(frame),
            }
        )
    return frame


def parse_ukrenergo_telegram_posts(raw_html: str, *, fetched_at: datetime) -> list[GridEventObservation]:
    soup = BeautifulSoup(raw_html, "html.parser")
    observations: list[GridEventObservation] = []
    for message_node in soup.select(".tgme_widget_message"):
        post_id = _extract_post_id(message_node)
        published_at = _extract_published_at(message_node)
        raw_text = _extract_message_text(message_node)
        if post_id is None or published_at is None or raw_text is None:
            continue
        observations.append(
            GridEventObservation(
                post_id=post_id,
                post_url=f"https://t.me/{post_id}",
                published_at=published_at,
                fetched_at=fetched_at,
                raw_text=raw_text,
                source=UKRENERGO_TELEGRAM_SOURCE,
                source_kind="observed",
                source_url=UKRENERGO_TELEGRAM_SOURCE_URL,
                energy_system_status=_has_energy_system_status(raw_text),
                shelling_damage=_contains_any(raw_text, _SHELLING_DAMAGE_PATTERNS),
                outage_or_restriction=_contains_any(raw_text, _OUTAGE_PATTERNS),
                consumption_change=_classify_consumption_change(raw_text),
                solar_shift_advice=_has_solar_shift_advice(raw_text),
                evening_saving_request=_contains_any(raw_text, _EVENING_SAVING_PATTERNS),
                affected_oblasts=_extract_affected_oblasts(raw_text),
            )
        )
    return sorted(observations, key=lambda item: (item.published_at, item.post_id))


def _fetch_ukrenergo_telegram_html() -> str | None:
    try:
        with httpx.Client(timeout=20.0, follow_redirects=True) as client:
            response = client.get(
                UKRENERGO_TELEGRAM_SOURCE_URL,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
            return response.text
    except httpx.HTTPError as error:
        logger.warning("Ukrenergo Telegram fetch failed: %s", error)
        return None


def _extract_post_id(message_node: object) -> str | None:
    raw_post_id = getattr(message_node, "get", lambda key, default=None: default)("data-post")
    if isinstance(raw_post_id, str) and raw_post_id.strip():
        return raw_post_id.strip()
    return None


def _extract_published_at(message_node: object) -> datetime | None:
    time_node = getattr(message_node, "select_one", lambda selector: None)("time[datetime]")
    if time_node is None:
        return None
    raw_datetime = time_node.get("datetime")
    if not isinstance(raw_datetime, str) or not raw_datetime.strip():
        return None
    return datetime.fromisoformat(raw_datetime.replace("Z", "+00:00"))


def _extract_message_text(message_node: object) -> str | None:
    text_node = getattr(message_node, "select_one", lambda selector: None)(".tgme_widget_message_text")
    if text_node is None:
        return None
    raw_text = html.unescape(text_node.get_text(" ", strip=True))
    normalized_text = re.sub(r"\s+", " ", raw_text).strip()
    if not normalized_text:
        return None
    return normalized_text


def _contains_any(text: str, patterns: tuple[str, ...]) -> bool:
    normalized_text = text.casefold()
    return any(pattern in normalized_text for pattern in patterns)


def _has_energy_system_status(text: str) -> bool:
    return _contains_any(text, _ENERGY_STATUS_PATTERNS)


def _classify_consumption_change(text: str) -> str:
    normalized_text = text.casefold()
    if "споживан" not in normalized_text and "consumption" not in normalized_text:
        return "unknown"
    if any(pattern in normalized_text for pattern in ("зрос", "вищ", "збільш", "increased")):
        return "increased"
    if any(pattern in normalized_text for pattern in ("зниж", "нижч", "decreased")):
        return "decreased"
    if any(pattern in normalized_text for pattern in ("відповідає сезон", "таким же", "незмін", "stable")):
        return "stable"
    return "unknown"


def _has_solar_shift_advice(text: str) -> bool:
    normalized_text = text.casefold()
    return _contains_any(normalized_text, _SOLAR_SHIFT_PATTERNS) and "перенести" in normalized_text


def _extract_affected_oblasts(text: str) -> list[str]:
    normalized_text = text.casefold()
    affected_oblasts: list[str] = []
    for oblast_name, patterns in _OBLAST_PATTERNS:
        if any(pattern in normalized_text for pattern in patterns):
            affected_oblasts.append(oblast_name)
    return affected_oblasts


def _affected_oblast_count(frame: pl.DataFrame) -> int:
    if frame.height == 0 or "affected_oblasts" not in frame.columns:
        return 0
    return len(
        {
            oblast
            for row in frame.select("affected_oblasts").to_series().to_list()
            if isinstance(row, list)
            for oblast in row
        }
    )


GRID_EVENT_BRONZE_ASSETS = [ukrenergo_grid_events_bronze]

__all__ = [
    "GRID_EVENT_BRONZE_ASSETS",
    "UKRENERGO_TELEGRAM_SOURCE_URL",
    "UkrenergoGridEventsConfig",
    "parse_ukrenergo_telegram_posts",
    "ukrenergo_grid_events_bronze",
]
