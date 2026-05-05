from datetime import datetime

from smart_arbitrage.gatekeeper.schemas import MARKET_PRICE_CAPS_UAH_PER_MWH
from smart_arbitrage.market_rules import market_rule_features, market_rule_for_timestamp


def test_market_rules_resolve_neurc_621_caps_by_effective_date() -> None:
    before_change = market_rule_for_timestamp(
        venue="BALANCING",
        timestamp=datetime(2026, 4, 29, 23),
    )
    after_change = market_rule_for_timestamp(
        venue="BALANCING",
        timestamp=datetime(2026, 4, 30, 0),
    )

    assert before_change.max_price_uah_mwh == 16000.0
    assert after_change.max_price_uah_mwh == 17000.0
    assert after_change.min_price_uah_mwh == 0.01
    assert after_change.regime_id == "ua_neurc_621_2026"


def test_market_rule_features_include_price_caps_and_recent_change_signal() -> None:
    features = market_rule_features(
        venue="DAM",
        timestamp=datetime(2026, 5, 3, 23),
    )

    assert features["market_price_cap_max"] == 15000.0
    assert features["market_price_cap_min"] == 10.0
    assert features["market_regime_id"] == "ua_neurc_621_2026"
    assert features["days_since_regime_change"] == 3
    assert features["is_price_cap_changed_recently"] == 1.0


def test_gatekeeper_default_balancing_cap_matches_current_policy_for_static_checks() -> None:
    assert MARKET_PRICE_CAPS_UAH_PER_MWH["BALANCING"] == 17000.0
