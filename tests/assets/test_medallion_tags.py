from smart_arbitrage.defs import defs


def test_all_medallion_group_assets_have_matching_medallion_tags() -> None:
    for asset in defs.assets or []:
        for asset_key, group_name in asset.group_names_by_key.items():
            if group_name not in {"bronze", "silver", "gold"}:
                continue
            tags = asset.tags_by_key.get(asset_key, {})
            assert tags.get("medallion") == group_name, asset_key.to_user_string()
