from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from smart_arbitrage.publication.oree_pxs import (
    price_rows_from_hdata_payload,
    fetch_oree_dam_price_rows,
)
from smart_arbitrage.publication.bess_arbitrage_index import (
    build_public_bess_arbitrage_index_payload,
)
from smart_arbitrage.publication.forecast_challenge import (
    build_public_forecast_challenge_payload,
)

KYIV = ZoneInfo("Europe/Kyiv")


def day_rows(day, count):
    return price_rows_from_hdata_payload(
        delivery_day=day,
        hdata_link=f"{day:%d.%m.%Y}/DAM/2",
        payload={
            "labels": list(range(1, count + 1)),
            "pricesData": list(range(100, 100 + count)),
            "amountsData": list(range(200, 200 + count)),
        },
    )


@pytest.mark.parametrize(
    "day,count",
    [(date(2026, 3, 29), 23), (date(2026, 6, 15), 24), (date(2026, 10, 25), 25)],
)
def test_ordinal_prices_cover_exact_local_delivery_day_through_lp(day, count):
    rows = day_rows(day, count)
    instants = [row["timestamp"].astimezone(UTC) for row in rows]
    assert all(row["timestamp"].tzinfo is not None for row in rows)
    assert len(set(instants)) == count
    assert all(b - a == timedelta(hours=1) for a, b in zip(instants, instants[1:]))
    assert instants[0].astimezone(KYIV) == datetime.combine(
        day, datetime.min.time(), KYIV
    )
    assert (instants[-1] + timedelta(hours=1)).astimezone(
        KYIV
    ).date() == day + timedelta(days=1)
    payload = build_public_bess_arbitrage_index_payload(list(reversed(rows)))
    assert payload["source"]["delivery_date"] == day.isoformat()
    assert payload["source"]["row_count"] == count
    for preset in payload["presets"]:
        schedule = preset["hourly_schedule"]
        assert [p["price_uah_mwh"] for p in schedule] == list(range(100, 100 + count))
        assert [
            datetime.fromisoformat(p["timestamp"]).astimezone(UTC) for p in schedule
        ] == instants
        assert preset["metrics"]["final_soc_mwh"] == pytest.approx(
            preset["battery"]["initial_soc_mwh"], abs=1e-4
        )
    assert payload["market_execution_enabled"] is False


def test_actual_october_25_payload_retains_both_repeated_hours():
    fixture = json.loads(
        Path(__file__).with_name("oree_2026_10_25.json").read_text(encoding="utf-8")
    )
    rows = price_rows_from_hdata_payload(
        delivery_day=date(2026, 10, 25), hdata_link="25.10.2026/DAM/2", payload=fixture
    )
    schedule = build_public_bess_arbitrage_index_payload(rows)["presets"][0][
        "hourly_schedule"
    ]
    assert [p["price_uah_mwh"] for p in schedule] == fixture["pricesData"]
    assert [p["volume_mwh"] for p in schedule] == fixture["amountsData"]
    repeated = [
        datetime.fromisoformat(p["timestamp"]).astimezone(KYIV) for p in schedule
    ][3:5]
    assert [p.hour for p in repeated] == [3, 3]
    assert [p.fold for p in repeated] == [0, 1]


@pytest.mark.parametrize(
    "day,count",
    [(date(2026, 3, 29), 24), (date(2026, 6, 15), 25), (date(2026, 10, 25), 24)],
)
def test_wrong_day_length_rejected(day, count):
    with pytest.raises(ValueError):
        day_rows(day, count)


@pytest.mark.parametrize("labels", [[1] * 24, list(range(2, 26)), list(range(1, 24))])
def test_malformed_labels_rejected(labels):
    with pytest.raises(ValueError):
        price_rows_from_hdata_payload(
            delivery_day=date(2026, 6, 15),
            hdata_link="test",
            payload={"labels": labels, "pricesData": [100] * 24},
        )


@pytest.mark.parametrize(
    "day,count",
    [(date(2026, 3, 29), 23), (date(2026, 6, 15), 24), (date(2026, 10, 25), 25)],
)
def test_forecast_covers_target_intervals_with_explicit_source_alignment(day, count):
    source_day = day - timedelta(days=7 if day.weekday() not in {1, 2, 3, 4} else 1)
    history = day_rows(source_day, 24)
    baseline = build_public_forecast_challenge_payload(
        history, target_delivery_date=day
    )["models"][0]
    assert baseline["point_count"] == count
    for point in baseline["points"]:
        target = datetime.fromisoformat(point["timestamp"]).astimezone(KYIV)
        source = datetime.fromisoformat(point["source_timestamp"]).astimezone(KYIV)
        assert source.date() == source_day
        assert target.hour == source.hour
        assert point["forecast_price_uah_mwh"] == 100 + source.hour


def test_forecast_history_does_not_collapse_repeated_source_hour():
    history = day_rows(date(2026, 10, 25), 25)
    payload = build_public_forecast_challenge_payload(
        history, target_delivery_date=date(2026, 11, 1)
    )
    baseline = payload["models"][0]
    assert baseline["point_count"] == 24
    # The normal-day 03:00 uses the first source occurrence; 04:00 uses ordinal 6.
    assert baseline["points"][3]["forecast_price_uah_mwh"] == 103
    assert baseline["points"][4]["forecast_price_uah_mwh"] == 105
    assert payload["source"]["history_row_count"] == 25


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "off_grid"])
def test_index_rejects_incomplete_duplicate_and_off_grid_intervals(mutation):
    rows = day_rows(date(2026, 6, 15), 24)
    if mutation == "missing":
        rows.pop()
    elif mutation == "duplicate":
        rows[-1] = rows[0]
    else:
        rows[5]["timestamp"] += timedelta(minutes=1)
    with pytest.raises(ValueError):
        build_public_bess_arbitrage_index_payload(rows)


@pytest.mark.parametrize(
    "day,count",
    [(date(2026, 3, 29), 23), (date(2026, 6, 15), 24), (date(2026, 10, 25), 25)],
)
def test_fetch_retains_every_interval_across_duplicate_links(monkeypatch, day, count):
    import httpx
    from smart_arbitrage.publication import oree_pxs

    client_type = httpx.Client

    def response(request):
        if request.url.path.endswith("get_pxs_res"):
            return httpx.Response(
                200,
                text='<input class="hdata_link" value="one"><input class="hdata_link" value="two">',
            )
        return httpx.Response(
            200,
            json={
                "pricesData": list(range(count)),
                "labels": list(range(1, count + 1)),
            },
        )

    monkeypatch.setattr(oree_pxs.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda **kwargs: client_type(transport=httpx.MockTransport(response), **kwargs),
    )
    rows = fetch_oree_dam_price_rows(day)
    assert len(rows) == count
    assert [row["price_uah_mwh"] for row in rows] == list(range(count))


def test_unordered_ordinals_retain_associated_prices():
    rows = price_rows_from_hdata_payload(
        delivery_day=date(2026, 6, 15),
        hdata_link="test",
        payload={
            "labels": list(range(24, 0, -1)),
            "pricesData": list(range(24, 0, -1)),
            "amountsData": list(range(124, 100, -1)),
        },
    )
    assert [p["price_uah_mwh"] for p in rows] == list(range(1, 25))
    assert [p["volume_mwh"] for p in rows] == list(range(101, 125))


@pytest.mark.parametrize(
    "timestamp", [datetime(2026, 3, 29, 3), datetime(2026, 10, 25, 3)]
)
def test_naive_dst_gap_or_fold_is_rejected(timestamp):
    from smart_arbitrage.publication.delivery_day import delivery_timestamp

    with pytest.raises(ValueError, match="explicit UTC offset"):
        delivery_timestamp(timestamp)


def test_local_zoneinfo_dataclass_folds_survive_index_normalization():
    from smart_arbitrage.publication.bess_arbitrage_index import PublicPricePoint

    rows = day_rows(date(2026, 10, 25), 25)
    points = [
        PublicPricePoint(row["timestamp"].astimezone(KYIV), row["price_uah_mwh"])
        for row in rows
    ]
    schedule = build_public_bess_arbitrage_index_payload(points)["presets"][0][
        "hourly_schedule"
    ]
    assert len(schedule) == 25
    assert schedule[3]["timestamp"] == "2026-10-25T03:00:00+03:00"
    assert schedule[4]["timestamp"] == "2026-10-25T03:00:00+02:00"


def test_forecast_spring_source_gap_blocks_instead_of_inventing_price():
    baseline = build_public_forecast_challenge_payload(
        day_rows(date(2026, 3, 29), 23), target_delivery_date=date(2026, 4, 5)
    )["models"][0]
    assert baseline["backend_status"] == "blocked"
    assert baseline["point_count"] == 0
    assert "hour 3" in baseline["blocker"]


@pytest.mark.parametrize(
    "day,count",
    [(date(2026, 3, 29), 23), (date(2026, 6, 15), 24), (date(2026, 10, 25), 25)],
)
def test_local_social_card_preserves_interval_labels_and_plot_bounds(
    tmp_path, monkeypatch, day, count
):
    from PIL import ImageDraw
    from scripts.social_bess_index import load_snapshot, draw_social_card

    latest = tmp_path / "latest.json"
    latest.write_text(
        json.dumps(build_public_bess_arbitrage_index_payload(day_rows(day, count))),
        encoding="utf-8",
    )
    snapshot = load_snapshot(latest)
    assert len(snapshot.timestamps) == count
    captured_lines = []
    original_line = ImageDraw.ImageDraw.line

    def capture(self, xy, *args, **kwargs):
        if kwargs.get("width") == 5 and isinstance(xy, list):
            captured_lines.append(xy)
        return original_line(self, xy, *args, **kwargs)

    monkeypatch.setattr(ImageDraw.ImageDraw, "line", capture)
    draw_social_card(
        snapshot, tmp_path / "card.png", "Local test", "No publication", "Test"
    )
    assert len(captured_lines[0]) == count
    assert captured_lines[0][0][0] == 816
    assert captured_lines[0][-1][0] == 1482
    assert (tmp_path / "card.png").exists()


@pytest.mark.parametrize(
    "day,count",
    [(date(2026, 3, 29), 23), (date(2026, 6, 15), 24), (date(2026, 10, 25), 25)],
)
def test_offline_index_materializer_roundtrips_all_intervals(tmp_path, day, count):
    from scripts.materialize_public_bess_arbitrage_index import main

    source = tmp_path / "input.json"
    rows = day_rows(day, count)
    source.write_text(
        json.dumps(rows, default=lambda value: value.isoformat()), encoding="utf-8"
    )
    output = tmp_path / "output"
    assert (
        main(
            [
                "--delivery-date",
                day.isoformat(),
                "--input-json",
                str(source),
                "--output-dir",
                str(output),
                "--fail-on-fetch-error",
            ]
        )
        == 0
    )
    latest = json.loads((output / "latest.json").read_text(encoding="utf-8"))
    status = json.loads(
        (output / "publication_status.json").read_text(encoding="utf-8")
    )
    assert latest["source"]["row_count"] == count
    assert status["realized"]["status"] == "published"
    assert len(latest["presets"][0]["hourly_schedule"]) == count
    assert latest["market_execution_enabled"] is False
