from smart_arbitrage.forecasting.afe import (
    build_forecast_afe_feature_catalog_frame,
    validate_forecast_afe_feature_catalog_evidence,
)
from smart_arbitrage.forecasting.grid_event_signals import GRID_EVENT_FEATURE_COLUMNS


def test_forecast_afe_catalog_marks_ukrenergo_semantic_features_as_implemented() -> None:
    catalog = build_forecast_afe_feature_catalog_frame()

    feature_groups = set(catalog["feature_group"].unique().to_list())
    assert {
        "price_history",
        "weather_forecast",
        "tenant_context",
        "semantic_grid_event",
        "external_market_context",
    }.issubset(feature_groups)

    semantic_rows = catalog.filter(catalog["feature_group"] == "semantic_grid_event")
    assert set(semantic_rows["feature_name"].to_list()) == set(GRID_EVENT_FEATURE_COLUMNS)
    assert semantic_rows["source_name"].unique().to_list() == ["UKRENERGO_TELEGRAM"]
    assert semantic_rows["semantic_source_kind"].unique().to_list() == ["official_telegram"]
    assert semantic_rows["feature_status"].unique().to_list() == ["implemented"]
    assert semantic_rows["training_use_allowed"].unique().to_list() == [True]
    assert semantic_rows["availability_scope"].unique().to_list() == ["pre_anchor_only"]
    assert semantic_rows["decision_cutoff_policy"].unique().to_list() == [
        "published_at_lte_anchor_timestamp"
    ]


def test_forecast_afe_catalog_keeps_external_market_rows_research_only() -> None:
    catalog = build_forecast_afe_feature_catalog_frame()

    external_rows = catalog.filter(catalog["feature_group"] == "external_market_context")
    assert external_rows.height >= 4
    assert external_rows["feature_status"].unique().to_list() == ["future_bridge"]
    assert external_rows["training_use_allowed"].unique().to_list() == [False]
    assert external_rows["availability_scope"].unique().to_list() == ["blocked_until_mapped"]
    assert set(external_rows["source_name"].unique().to_list()).issuperset(
        {"ENTSO_E", "OPSD", "EMBER", "NORD_POOL"}
    )


def test_forecast_afe_catalog_evidence_rejects_missing_cutoff_policy() -> None:
    catalog = build_forecast_afe_feature_catalog_frame()
    broken_rows = []
    for row_index, row in enumerate(catalog.iter_rows(named=True)):
        broken_rows.append(
            {**row, "decision_cutoff_policy": ""}
            if row_index == 0
            else row
        )
    broken = catalog.__class__(broken_rows)

    outcome = validate_forecast_afe_feature_catalog_evidence(broken)

    assert outcome.passed is False
    assert outcome.metadata["missing_cutoff_policy_rows"] == 1

