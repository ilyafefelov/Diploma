from __future__ import annotations

import argparse
import json
import os
import textwrap
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Any

PUBLIC_URL = "https://energy-index.full-iron.com/ukraine-bess-arbitrage-index"
PRESET_ID = "bess_100kw_215kwh"


@dataclass(frozen=True)
class IndexSnapshot:
    delivery_date: str
    generated_at: str
    row_count: int
    net_value_uah: float
    normalized_uah_per_mwh: float
    equivalent_cycles: float
    throughput_mwh: float
    prices: list[float]
    powers: list[float]
    soc_percent: list[float]


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def format_uah(value: float) -> str:
    return f"{value:,.0f}"


def load_snapshot(latest_path: Path) -> IndexSnapshot:
    latest = load_json(latest_path)
    preset = next(
        (item for item in latest["presets"] if item.get("preset_id") == PRESET_ID),
        latest["presets"][0],
    )
    battery = preset["battery"]
    schedule = preset["hourly_schedule"]
    capacity = float(battery["capacity_mwh"])
    return IndexSnapshot(
        delivery_date=latest["source"]["delivery_date"],
        generated_at=latest["generated_at"],
        row_count=int(latest["source"]["row_count"]),
        net_value_uah=float(preset["metrics"]["net_value_uah"]),
        normalized_uah_per_mwh=float(preset["metrics"]["normalized_uah_per_mwh_capacity"]),
        equivalent_cycles=float(preset["metrics"]["equivalent_full_cycles"]),
        throughput_mwh=float(preset["metrics"]["throughput_mwh"]),
        prices=[float(row["price_uah_mwh"]) for row in schedule],
        powers=[float(row["net_power_mw"]) for row in schedule],
        soc_percent=[float(row["soc_after_mwh"]) / capacity * 100 for row in schedule],
    )


def load_weekly_rows(history_path: Path) -> list[dict[str, Any]]:
    history = load_json(history_path)
    rows = [
        row
        for row in history.get("rows", [])
        if row.get("preset_id") == PRESET_ID and row.get("market_execution_enabled") is False
    ]
    rows.sort(key=lambda row: row["delivery_date"])
    return rows[-7:]


def weekly_key(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return datetime.now(UTC).strftime("%G-W%V")
    latest_date = datetime.fromisoformat(rows[-1]["delivery_date"])
    return latest_date.strftime("%G-W%V")


def account_env_prefix(account: str) -> str:
    if account == "daily":
        return "X_DAILY"
    if account == "personal":
        return "X_PERSONAL"
    raise ValueError(f"Unsupported account: {account}")


def account_handle(account: str) -> str:
    if account == "personal":
        return os.environ.get("X_PERSONAL_HANDLE", "fefelov")
    return os.environ.get("X_DAILY_HANDLE", "UkraineBESSIndex")


def load_x_credentials(account: str) -> dict[str, str] | None:
    prefix = account_env_prefix(account)
    values = {
        "api_key": os.environ.get(f"{prefix}_API_KEY", ""),
        "api_secret": os.environ.get(f"{prefix}_API_SECRET", ""),
        "access_token": os.environ.get(f"{prefix}_ACCESS_TOKEN", ""),
        "access_token_secret": os.environ.get(f"{prefix}_ACCESS_TOKEN_SECRET", "")
        or os.environ.get(f"{prefix}_ACCESS_SECRET", ""),
    }
    if all(values.values()):
        return values
    return None


def social_log_path(root: Path) -> Path:
    return root / "docs" / "marketing" / "social_log.json"


def load_social_log(root: Path) -> dict[str, Any]:
    path = social_log_path(root)
    if not path.exists():
        return {"schema_version": "bess_social_log.v1", "posts": []}
    return load_json(path)


def already_posted(log: dict[str, Any], key: str) -> bool:
    return any(post.get("key") == key and post.get("status") == "posted" for post in log["posts"])


def record_post(log: dict[str, Any], payload: dict[str, Any]) -> None:
    log["posts"] = [post for post in log["posts"] if post.get("key") != payload["key"]]
    log["posts"].append(payload)


def draw_social_card(
    snapshot: IndexSnapshot,
    output_path: Path,
    title: str,
    subtitle: str,
    footer: str,
) -> None:
    from PIL import Image, ImageDraw, ImageFont

    width, height = 1600, 900
    image = Image.new("RGB", (width, height), "#f6fcff")
    draw = ImageDraw.Draw(image)

    for x in range(0, width, 34):
        draw.line((x, 0, x, height), fill="#d9edf5", width=1)
    for y in range(0, height, 34):
        draw.line((0, y, width, y), fill="#e4f3f8", width=1)

    def font(size: int, bold: bool = False, serif: bool = False) -> Any:
        candidates = []
        if serif:
            candidates.extend(
                [
                    "C:/Windows/Fonts/georgiab.ttf" if bold else "C:/Windows/Fonts/georgia.ttf",
                    "/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf"
                    if bold
                    else "/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf",
                ]
            )
        candidates.extend(
            [
                "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
                if bold
                else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            ]
        )
        for candidate in candidates:
            if Path(candidate).exists():
                return ImageFont.truetype(candidate, size)
        return ImageFont.load_default()

    ink = "#082744"
    muted = "#466b83"
    blue = "#0a76c7"
    green = "#38ad8b"
    yellow = "#e5ba4f"
    line = "#b9d8e8"

    draw.rounded_rectangle((70, 50, 145, 125), radius=8, fill="#2a9ad8", outline="#0a76c7", width=2)
    for gx in range(70, 146, 19):
        draw.line((gx, 50, gx, 125), fill="#9bdcf3")
    for gy in range(50, 126, 19):
        draw.line((70, gy, 145, gy), fill="#9bdcf3")
    draw.text((164, 58), "FULLIRON", fill=ink, font=font(42, True))
    draw.text((164, 103), "PUBLIC PATH PROJECT", fill=muted, font=font(24, True))

    draw.line((70, 170, 144, 170), fill=blue, width=5)
    draw.text((156, 154), "UKRAINE BESS ARBITRAGE INDEX", fill=blue, font=font(32, True))
    draw.line((548, 170, 570, 170), fill=yellow, width=5)

    y = 220
    title_font = font(58, True, serif=True)
    for line_text in textwrap.wrap(title, width=20):
        draw.text((70, y), line_text, fill=ink, font=title_font)
        y += 64
    y += 18
    for line_text in textwrap.wrap(subtitle, width=42):
        draw.text((70, y), line_text, fill="#173f5c", font=font(29, True))
        y += 36

    badge_y = 640
    for i, (label, value) in enumerate((("SOURCE", "OREE DAM"), ("BOUNDARY", "NO EXECUTION"))):
        x0 = 70 + i * 318
        draw.rounded_rectangle((x0, badge_y, x0 + 300, badge_y + 98), radius=8, fill="#ffffff", outline=line)
        draw.text((x0 + 18, badge_y + 18), label, fill="#5d7b92", font=font(20, True))
        draw.text((x0 + 18, badge_y + 50), value, fill=ink, font=font(28, True))

    draw.line((70, 808, 690, 808), fill=line, width=1)
    draw.text((70, 835), "Public demo: ", fill=ink, font=font(22, True))
    draw.text((210, 835), footer, fill=muted, font=font(22, True))

    receipt_x = 744
    receipt_y = 50
    receipt_w = 786
    draw.rounded_rectangle((receipt_x, receipt_y, receipt_x + receipt_w, receipt_y + 176), radius=10, fill="#ffffff", outline=line)
    receipt_items = [
        ("DELIVERY DATE", datetime.fromisoformat(snapshot.delivery_date).strftime("%d %b %Y")),
        ("NET VALUE", f"{format_uah(snapshot.net_value_uah)} UAH"),
        ("NORMALIZED", f"{format_uah(snapshot.normalized_uah_per_mwh)} UAH/MWh"),
    ]
    col_w = receipt_w // 3
    for i, (label, value) in enumerate(receipt_items):
        x0 = receipt_x + i * col_w
        if i:
            draw.line((x0, receipt_y, x0, receipt_y + 176), fill=line)
        draw.text((x0 + 24, receipt_y + 22), label, fill="#5d7b92", font=font(19, True))
        for j, wrapped in enumerate(textwrap.wrap(value, width=16)):
            draw.text((x0 + 24, receipt_y + 58 + j * 36), wrapped, fill=ink, font=font(28, True, serif=True))
    draw.text((receipt_x + 24, receipt_y + 114), f"{snapshot.row_count} / 24 official hourly rows", fill=muted, font=font(18, True))

    chart_x, chart_y, chart_w, chart_h = 744, 246, 786, 468
    draw.rounded_rectangle((chart_x, chart_y, chart_x + chart_w, chart_y + chart_h), radius=10, fill="#ffffff", outline=line)
    draw.text((chart_x + 26, chart_y + 26), "DISPATCH AND PRICE RECEIPT", fill=ink, font=font(34, True))
    draw.text(
        (chart_x + 26, chart_y + 70),
        "Bars show BESS action. Blue line shows official DAM price.",
        fill=muted,
        font=font(18, True),
    )
    plot = (chart_x + 72, chart_y + 122, chart_x + chart_w - 48, chart_y + chart_h - 86)
    px0, py0, px1, py1 = plot
    for g in range(5):
        gy = py0 + (py1 - py0) * g / 4
        draw.line((px0, gy, px1, gy), fill="#e1eef5")
    for i in range(0, 24, 3):
        gx = px0 + (px1 - px0) * i / 23
        draw.line((gx, py0, gx, py1), fill="#e8f3f8")
        draw.text((gx - 9, py1 + 17), f"{i:02d}", fill=muted, font=font(14, True))

    max_price = max(max(snapshot.prices), 1.0)
    zero_y = py0 + (py1 - py0) * 0.58
    draw.line((px0, zero_y, px1, zero_y), fill="#c6ddeb")

    def x_at(i: int) -> float:
        return px0 + (px1 - px0) * i / 23

    def y_price(value: float) -> float:
        return py1 - (py1 - py0) * value / max_price

    def y_soc(value: float) -> float:
        return py1 - (py1 - py0) * value / 100

    power_scale = (py1 - py0) * 0.36 / 0.1
    for i, power in enumerate(snapshot.powers):
        if abs(power) < 0.008:
            continue
        x = x_at(i)
        bar_h = max(4, abs(power) * power_scale)
        color = blue if power > 0 else yellow
        y0 = zero_y - bar_h if power > 0 else zero_y
        draw.rounded_rectangle((x - 8, y0, x + 8, y0 + bar_h), radius=4, fill=color)

    price_points = [(x_at(i), y_price(value)) for i, value in enumerate(snapshot.prices)]
    soc_points = [(x_at(i), y_soc(value)) for i, value in enumerate(snapshot.soc_percent)]
    draw.line(soc_points, fill=green, width=3)
    draw.line(price_points, fill=blue, width=5, joint="curve")

    peak_i = snapshot.prices.index(max(snapshot.prices))
    low_i = snapshot.prices.index(min(snapshot.prices))
    for i, label, value in (
        (peak_i, f"PEAK {peak_i:02d}:00", f"{format_uah(snapshot.prices[peak_i])} UAH/MWh"),
        (low_i, f"LOW {low_i:02d}:00", f"{format_uah(snapshot.prices[low_i])} UAH/MWh"),
    ):
        cx, cy = price_points[i]
        draw.ellipse((cx - 7, cy - 7, cx + 7, cy + 7), fill="#ffffff", outline=blue, width=3)
        box_x = min(max(cx - 55, px0), px1 - 142)
        box_y = max(cy - 64, py0 + 2)
        draw.rounded_rectangle((box_x, box_y, box_x + 142, box_y + 52), radius=8, fill="#ffffff", outline=line)
        draw.text((box_x + 12, box_y + 9), label, fill=ink, font=font(13, True))
        draw.text((box_x + 12, box_y + 28), value, fill=ink, font=font(14, True))

    legend_y = chart_y + chart_h - 35
    for offset, color, label in ((28, blue, "Discharge"), (150, yellow, "Charge"), (250, green, "SOC")):
        draw.rounded_rectangle((chart_x + offset, legend_y, chart_x + offset + 20, legend_y + 8), radius=4, fill=color)
        draw.text((chart_x + offset + 28, legend_y - 5), label, fill="#365d78", font=font(16, True))

    strip_y = 734
    draw.rounded_rectangle((744, strip_y, 1530, 858), radius=10, fill="#ffffff", outline=line)
    strip_items = [
        ("CLAIM BOUNDARY", "Public index, not market execution"),
        ("LEAD PATH", "BESS analytics / forecasting / product design"),
        ("CALL TO ACTION", "Feedback, pilots, recruiting, investment conversations"),
    ]
    strip_w = 786 // 3
    for i, (label, value) in enumerate(strip_items):
        x0 = 744 + i * strip_w
        if i:
            draw.line((x0, strip_y, x0, 858), fill=line)
        draw.text((x0 + 20, strip_y + 20), label, fill="#5d7b92", font=font(18, True))
        yy = strip_y + 48
        for wrapped in textwrap.wrap(value, width=24):
            draw.text((x0 + 20, yy), wrapped, fill=muted, font=font(18, True))
            yy += 23

    output_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(output_path)


def daily_text(snapshot: IndexSnapshot, public_url: str) -> str:
    return "\n".join(
        [
            f"Ukraine BESS Arbitrage Index - {snapshot.delivery_date}",
            "",
            f"Official OREE DAM rows: {snapshot.row_count}/24",
            f"Standard 100 kW / 215 kWh BESS: {format_uah(snapshot.net_value_uah)} UAH",
            "Public demo only. No bids. No execution.",
            "",
            public_url,
        ]
    )


def weekly_text(rows: list[dict[str, Any]], public_url: str) -> str:
    if not rows:
        return "\n".join(
            [
                "Weekly Ukraine BESS Arbitrage Index update.",
                "",
                "Public demo only. No bids. No execution.",
                "",
                public_url,
            ]
        )
    values = [float(row["net_value_uah"]) for row in rows]
    best = max(rows, key=lambda row: float(row["net_value_uah"]))
    latest = rows[-1]
    return "\n".join(
        [
            "Weekly Ukraine BESS Arbitrage Index update.",
            "",
            f"Last {len(rows)} delivery days, 100 kW / 215 kWh:",
            f"avg {format_uah(mean(values))} UAH/day",
            f"best {best['delivery_date']}: {format_uah(float(best['net_value_uah']))} UAH",
            f"latest {latest['delivery_date']}: {format_uah(float(latest['net_value_uah']))} UAH",
            "",
            "Public demo only. No execution.",
            "",
            public_url,
        ]
    )


def post_to_x(text: str, image_path: Path, credentials: dict[str, str]) -> str:
    import tweepy

    auth = tweepy.OAuth1UserHandler(
        credentials["api_key"],
        credentials["api_secret"],
        credentials["access_token"],
        credentials["access_token_secret"],
    )
    api = tweepy.API(auth)
    media = api.media_upload(filename=str(image_path))
    client = tweepy.Client(
        consumer_key=credentials["api_key"],
        consumer_secret=credentials["api_secret"],
        access_token=credentials["access_token"],
        access_token_secret=credentials["access_token_secret"],
    )
    response = client.create_tweet(text=text, media_ids=[media.media_id_string])
    tweet_id = response.data["id"] if response.data else ""
    if not tweet_id:
        raise RuntimeError("X API did not return a tweet id.")
    return str(tweet_id)


def run(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.repo_root).resolve()
    latest_path = root / "dashboard" / "public" / "data" / "bess-arbitrage-index" / "latest.json"
    history_path = root / "dashboard" / "public" / "data" / "bess-arbitrage-index" / "history.json"
    output_dir = Path(args.output_dir).resolve()
    public_url = args.public_url
    snapshot = load_snapshot(latest_path)
    now = datetime.now(UTC).isoformat()

    if args.cadence == "daily":
        post_key = f"daily:{args.x_account}:{snapshot.delivery_date}"
        post_text = daily_text(snapshot, public_url)
        image_name = f"bess-index-daily-{snapshot.delivery_date}-{args.x_account}.png"
        card_title = "Battery storage value, made public and source-backed."
        card_subtitle = "A daily public receipt for what a standard BESS could capture on Ukrainian DAM prices."
    else:
        rows = load_weekly_rows(history_path)
        week = weekly_key(rows)
        post_key = f"weekly:{args.x_account}:{week}"
        post_text = weekly_text(rows, public_url)
        image_name = f"bess-index-weekly-{week}-{args.x_account}.png"
        card_title = "Weekly BESS value receipt, source-backed."
        card_subtitle = "A public weekly digest from official Ukrainian day-ahead market rows."

    image_path = output_dir / image_name
    text_path = output_dir / image_name.replace(".png", ".txt")
    draw_social_card(snapshot, image_path, card_title, card_subtitle, "energy-index.full-iron.com")
    output_dir.mkdir(parents=True, exist_ok=True)
    text_path.write_text(post_text + "\n", encoding="utf-8")

    log = load_social_log(root)
    if already_posted(log, post_key):
        return {
            "status": "skipped_already_posted",
            "key": post_key,
            "text_path": str(text_path),
            "image_path": str(image_path),
        }

    credentials = load_x_credentials(args.x_account)
    if not credentials:
        if args.require_post:
            raise RuntimeError(f"Missing X credentials for account {args.x_account}.")
        return {
            "status": "draft_only_missing_credentials",
            "key": post_key,
            "account": args.x_account,
            "handle": account_handle(args.x_account),
            "text_path": str(text_path),
            "image_path": str(image_path),
        }

    tweet_id = post_to_x(post_text, image_path, credentials)
    record_post(
        log,
        {
            "key": post_key,
            "status": "posted",
            "cadence": args.cadence,
            "x_account": args.x_account,
            "x_handle": account_handle(args.x_account),
            "tweet_id": tweet_id,
            "posted_at": now,
            "delivery_date": snapshot.delivery_date,
            "public_url": public_url,
        },
    )
    write_json(social_log_path(root), log)
    return {
        "status": "posted",
        "key": post_key,
        "tweet_id": tweet_id,
        "text_path": str(text_path),
        "image_path": str(image_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish or draft social posts for the public BESS index.")
    parser.add_argument("cadence", choices=["daily", "weekly"])
    parser.add_argument("--x-account", choices=["daily", "personal"], required=True)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output-dir", default=".social-outbox")
    parser.add_argument("--public-url", default=PUBLIC_URL)
    parser.add_argument("--require-post", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    result = run(parse_args())
    print(json.dumps(result, indent=2, sort_keys=True))
