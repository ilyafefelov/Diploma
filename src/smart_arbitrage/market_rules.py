"""Date-aware Ukrainian market rule features for research benchmarks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import cache
from pathlib import Path
from typing import Any

import yaml

from smart_arbitrage.gatekeeper.schemas import MarketVenue

DEFAULT_MARKET_RULES_PATH = Path(__file__).resolve().parents[2] / "configs" / "market_rules_ua.yaml"
RECENT_REGIME_CHANGE_DAYS = 7


@dataclass(frozen=True, slots=True)
class MarketRule:
    venue: MarketVenue
    regime_id: str
    resolution: str
    effective_from: datetime
    min_price_uah_mwh: float
    max_price_uah_mwh: float


def market_rule_for_timestamp(
    *,
    venue: MarketVenue,
    timestamp: datetime,
    config_path: str | Path | None = None,
) -> MarketRule:
    """Return the active Ukrainian market rule for a venue and timestamp."""

    normalized_timestamp = timestamp.replace(tzinfo=None)
    candidates = [
        rule
        for rule in _load_market_rules(config_path)
        if rule.venue == venue and rule.effective_from <= normalized_timestamp
    ]
    if not candidates:
        raise ValueError(f"No market rule configured for {venue} at {timestamp.isoformat()}.")
    return max(candidates, key=lambda rule: rule.effective_from)


def market_rule_features(
    *,
    venue: MarketVenue,
    timestamp: datetime,
    config_path: str | Path | None = None,
) -> dict[str, float | str]:
    """Expose market-regime caps as model features."""

    rule = market_rule_for_timestamp(
        venue=venue,
        timestamp=timestamp,
        config_path=config_path,
    )
    days_since_change = max(0, (timestamp.replace(tzinfo=None).date() - rule.effective_from.date()).days)
    return {
        "market_price_cap_max": rule.max_price_uah_mwh,
        "market_price_cap_min": rule.min_price_uah_mwh,
        "market_regime_id": rule.regime_id,
        "days_since_regime_change": float(days_since_change),
        "is_price_cap_changed_recently": 1.0 if days_since_change <= RECENT_REGIME_CHANGE_DAYS else 0.0,
    }


@cache
def _load_market_rules_cached(config_path: str) -> tuple[MarketRule, ...]:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Market rules config not found: {path}")
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("Market rules config must be a mapping.")
    regimes = loaded.get("regimes")
    if not isinstance(regimes, list):
        raise ValueError("Market rules config must contain a regimes list.")
    rules: list[MarketRule] = []
    for regime in regimes:
        rules.extend(_rules_from_regime(regime))
    if not rules:
        raise ValueError("Market rules config did not define any venue rules.")
    return tuple(sorted(rules, key=lambda rule: (rule.venue, rule.effective_from)))


def _load_market_rules(config_path: str | Path | None) -> tuple[MarketRule, ...]:
    path = DEFAULT_MARKET_RULES_PATH if config_path is None else Path(config_path)
    return _load_market_rules_cached(str(path.resolve()))


def _rules_from_regime(regime: Any) -> list[MarketRule]:
    if not isinstance(regime, dict):
        raise ValueError("Each market rule regime must be a mapping.")
    regime_id = _string_field(regime, "regime_id")
    resolution = _string_field(regime, "resolution")
    effective_from = datetime.fromisoformat(_string_field(regime, "effective_from")).replace(tzinfo=None)
    venues = regime.get("venues")
    if not isinstance(venues, dict):
        raise ValueError(f"Regime {regime_id} must define venues.")
    rules: list[MarketRule] = []
    for venue, venue_payload in venues.items():
        if venue not in {"DAM", "IDM", "BALANCING"}:
            raise ValueError(f"Unsupported market venue in rules config: {venue}")
        if not isinstance(venue_payload, dict):
            raise ValueError(f"Venue payload for {venue} must be a mapping.")
        rules.append(
            MarketRule(
                venue=venue,
                regime_id=regime_id,
                resolution=resolution,
                effective_from=effective_from,
                min_price_uah_mwh=_float_field(venue_payload, "min_price_uah_mwh"),
                max_price_uah_mwh=_float_field(venue_payload, "max_price_uah_mwh"),
            )
        )
    return rules


def _string_field(mapping: dict[str, Any], field_name: str) -> str:
    value = mapping.get(field_name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string.")
    return value


def _float_field(mapping: dict[str, Any], field_name: str) -> float:
    value = mapping.get(field_name)
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric.")
    if value is None:
        raise ValueError(f"{field_name} must be numeric.")
    try:
        parsed_value = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field_name} must be numeric.") from error
    if parsed_value < 0.0:
        raise ValueError(f"{field_name} must be non-negative.")
    return parsed_value
