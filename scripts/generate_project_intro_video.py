"""Generate the README project-intro video asset.

The HyperFrames source in docs/technical/final-demo-assets/project-intro-hyperframes
defines the creative storyboard. This script is a deterministic local render
fallback that produces the GitHub-facing MP4/poster without adding video
dependencies to the project lockfile.
"""

from __future__ import annotations

import math
import subprocess
from dataclasses import dataclass
from pathlib import Path

import imageio.v2 as imageio
import imageio_ffmpeg
import numpy as np
from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
ASSET_DIR = ROOT / "docs" / "technical" / "final-demo-assets"
HYPERFRAMES_DIR = ASSET_DIR / "project-intro-hyperframes"
OUTPUT_MP4 = ASSET_DIR / "project-intro.mp4"
OUTPUT_POSTER = ASSET_DIR / "project-intro-poster.png"
NARRATION_WAV = HYPERFRAMES_DIR / "narration.wav"

WIDTH = 1280
HEIGHT = 720
FPS = 24
DURATION_SECONDS = 43.0
TRANSITION_SECONDS = 0.55

BG = (8, 17, 31)
PANEL = (16, 42, 67)
PANEL_2 = (23, 63, 95)
LIME = (183, 255, 60)
CYAN = (125, 211, 252)
AMBER = (251, 191, 36)
TEXT = (247, 251, 255)
MUTED = (184, 199, 217)


@dataclass(frozen=True)
class Scene:
    start: float
    title: str
    eyebrow: str
    body: str
    caption: str
    kind: str


SCENES = [
    Scene(
        0.0,
        "Battery decisions,\nreviewed before action.",
        "Smart Energy Arbitrage 2026",
        "DAM/IDM hourly recommendation preview for a human BESS operator in the Ukrainian market context.",
        "Energy storage creates value when hourly prices move, but the operator needs evidence, not a black-box command.",
        "hero",
    ),
    Scene(
        5.4,
        "Markets move hourly.\nBatteries have physical limits.",
        "Concept",
        "The preview combines source-backed prices, SOC constraints, regret evidence, and safety gates before showing a recommendation.",
        "The project frames arbitrage as an operator preview problem, not as autonomous exchange submission.",
        "market",
    ),
    Scene(
        11.0,
        "Source rows become\nread-model evidence.",
        "System",
        "OREE rows, forecast context, tenant state, LP/V2+ evidence, FastAPI read models, and the Nuxt dashboard stay connected.",
        "Every strong claim is tied back to tracked repository evidence, tests, and source-readiness boundaries.",
        "pipeline",
    ),
    Scene(
        16.6,
        "Operator preview,\ndefense evidence,\nand API contracts.",
        "Product",
        "The UI is built for review: tenant, venue, date, schedule candidates, readiness, and no-execution controls.",
        "The dashboard shows recommendations as preview evidence, not dispatch commands.",
        "product",
    ),
    Scene(
        22.2,
        "Measured decision quality,\nguarded by deterministic contracts.",
        "Evidence",
        "V2+ remains the headline/default evidence, while DT/HF shadows stay bounded as manual research signals.",
        "The evidence is useful because it is source-backed, reproducible, and not promoted into market execution.",
        "evidence",
    ),
    Scene(
        28.0,
        "What it proves,\nand what it refuses to claim.",
        "Boundary",
        "This is a commission-facing product and evidence system: preview, read model, deterministic guardrails, and human review.",
        "market_execution_enabled=false; no ProposedBid; no market order payload.",
        "boundary",
    ),
    Scene(
        33.2,
        "Open the product,\nthen inspect the proof.",
        "Review path",
        "Start with the README, then open /operator, /defense, and FastAPI /docs.",
        "Smart Energy Arbitrage 2026: source-backed hourly operator recommendation preview, no market execution.",
        "cta",
    ),
]


def main() -> None:
    ASSET_DIR.mkdir(parents=True, exist_ok=True)
    scene_images = [render_scene(scene) for scene in SCENES]
    poster = scene_images[3]
    poster.save(OUTPUT_POSTER)

    silent_output = OUTPUT_MP4
    if NARRATION_WAV.exists():
        silent_output = OUTPUT_MP4.with_name("project-intro.silent.mp4")

    with imageio.get_writer(
        silent_output,
        fps=FPS,
        codec="libx264",
        quality=8,
        macro_block_size=16,
        ffmpeg_log_level="error",
    ) as writer:
        total_frames = int(DURATION_SECONDS * FPS)
        for frame_index in range(total_frames):
            t = frame_index / FPS
            frame = compose_frame(scene_images, t)
            writer.append_data(np.asarray(frame))

    if NARRATION_WAV.exists():
        mux_audio(silent_output, NARRATION_WAV, OUTPUT_MP4)
        silent_output.unlink(missing_ok=True)

    print(f"wrote {OUTPUT_MP4}")
    print(f"wrote {OUTPUT_POSTER}")


def compose_frame(scene_images: list[Image.Image], t: float) -> Image.Image:
    index = current_scene_index(t)
    current = scene_images[index]
    next_index = min(index + 1, len(scene_images) - 1)
    next_start = SCENES[next_index].start if next_index != index else DURATION_SECONDS + 10
    transition_start = next_start - TRANSITION_SECONDS

    if index < len(scene_images) - 1 and t >= transition_start:
        progress = clamp((t - transition_start) / TRANSITION_SECONDS)
        eased = ease_in_out(progress)
        slide = int(WIDTH * eased)
        frame = Image.new("RGB", (WIDTH, HEIGHT), BG)
        frame.paste(current.crop((slide, 0, WIDTH, HEIGHT)), (0, 0))
        frame.paste(next_index_image(scene_images, next_index).crop((0, 0, slide, HEIGHT)), (WIDTH - slide, 0))
        bar_x = max(0, WIDTH - slide - 18)
        draw = ImageDraw.Draw(frame)
        draw.rounded_rectangle((bar_x, 0, bar_x + 34, HEIGHT), radius=12, fill=LIME)
        return frame

    return current.copy()


def next_index_image(scene_images: list[Image.Image], index: int) -> Image.Image:
    return scene_images[index]


def current_scene_index(t: float) -> int:
    active = 0
    for idx, scene in enumerate(SCENES):
        if t >= scene.start:
            active = idx
    return active


def render_scene(scene: Scene) -> Image.Image:
    img = Image.new("RGB", (WIDTH, HEIGHT), BG)
    draw = ImageDraw.Draw(img)
    draw_background(draw)
    if scene.kind == "hero":
        draw_text_block(draw, scene, 82, 116, title_size=78, body_width=840)
    elif scene.kind == "market":
        draw_text_block(draw, scene, 72, 88, title_size=54, body_width=520)
        draw_market_card(draw, 720, 128)
    elif scene.kind == "pipeline":
        draw_text_block(draw, scene, 72, 70, title_size=54, body_width=780)
        draw_pipeline(draw, 86, 350)
    elif scene.kind == "product":
        draw_text_block(draw, scene, 72, 54, title_size=50, body_width=760)
        draw_dashboard(draw, 92, 324)
    elif scene.kind == "evidence":
        draw_text_block(draw, scene, 72, 62, title_size=52, body_width=840)
        draw_evidence(draw, 92, 340)
    elif scene.kind == "boundary":
        draw_text_block(draw, scene, 72, 78, title_size=56, body_width=900)
        draw_boundary(draw, 92, 360)
    elif scene.kind == "cta":
        draw_text_block(draw, scene, 72, 86, title_size=60, body_width=880)
        draw_cta(draw, 92, 392)
    draw_caption(draw, scene.caption)
    return img


def draw_background(draw: ImageDraw.ImageDraw) -> None:
    for x in range(0, WIDTH, 64):
        draw.line((x, 0, x, HEIGHT), fill=(18, 51, 73), width=1)
    for y in range(0, HEIGHT, 64):
        draw.line((0, y, WIDTH, y), fill=(18, 51, 73), width=1)
    draw.ellipse((-120, -90, 420, 310), fill=(15, 57, 82))
    draw.ellipse((930, 480, 1460, 900), fill=(28, 68, 37))


def draw_text_block(
    draw: ImageDraw.ImageDraw,
    scene: Scene,
    x: int,
    y: int,
    *,
    title_size: int,
    body_width: int,
) -> None:
    eyebrow_font = font("consola.ttf", 24)
    title_font = font("georgia.ttf", title_size)
    body_font = font("arial.ttf", 27)
    draw.text((x, y), scene.eyebrow.upper(), fill=LIME, font=eyebrow_font)
    draw.multiline_text((x, y + 40), scene.title, fill=TEXT, font=title_font, spacing=5)
    title_height = multiline_height(scene.title, title_font, spacing=5)
    body_lines = wrap_text(scene.body, body_font, body_width)
    draw.multiline_text((x, y + 58 + title_height), "\n".join(body_lines), fill=(216, 230, 244), font=body_font, spacing=7)


def draw_market_card(draw: ImageDraw.ImageDraw, x: int, y: int) -> None:
    rounded(draw, (x, y, x + 450, y + 365), PANEL)
    mono = font("consola.ttf", 24)
    small = font("consola.ttf", 20)
    rows = [("00:00", "low", 0.42, CYAN), ("07:00", "peak", 0.92, AMBER), ("12:00", "quiet", 0.25, CYAN), ("19:00", "peak", 0.86, AMBER), ("23:00", "close", 0.48, CYAN)]
    for idx, (hour, label, value, color) in enumerate(rows):
        yy = y + 40 + idx * 60
        draw.text((x + 28, yy), hour, fill=TEXT, font=mono)
        draw.rounded_rectangle((x + 135, yy + 11, x + 135 + int(210 * value), yy + 22), radius=6, fill=color)
        draw.text((x + 360, yy), label, fill=MUTED, font=small)


def draw_pipeline(draw: ImageDraw.ImageDraw, x: int, y: int) -> None:
    nodes = [
        ("SOURCE", "OREE\nDAM/IDM", x, y),
        ("FORECAST", "NBEATSx\nTFT", x + 235, y - 58),
        ("TENANT", "SOC\nlimits", x + 235, y + 90),
        ("EVIDENCE", "LP / V2+\nHFDT", x + 500, y + 15),
        ("API", "FastAPI\nread models", x + 765, y + 15),
        ("UI", "Nuxt\n/operator", x + 990, y + 15),
    ]
    edge_color = (88, 165, 197)
    draw.line((x + 154, y + 48, x + 235, y + 6), fill=edge_color, width=4)
    draw.line((x + 154, y + 76, x + 235, y + 138), fill=edge_color, width=4)
    draw.line((x + 389, y + 24, x + 500, y + 65), fill=edge_color, width=4)
    draw.line((x + 389, y + 172, x + 500, y + 95), fill=edge_color, width=4)
    draw.line((x + 654, y + 91, x + 765, y + 91), fill=edge_color, width=4)
    draw.line((x + 919, y + 91, x + 990, y + 91), fill=edge_color, width=4)
    for label, body, nx, ny in nodes:
        draw_node(draw, nx, ny, label, body)


def draw_node(draw: ImageDraw.ImageDraw, x: int, y: int, label: str, body: str) -> None:
    rounded(draw, (x, y, x + 154, y + 96), PANEL_2)
    draw.text((x + 14, y + 12), label, fill=LIME, font=font("consola.ttf", 16))
    draw.multiline_text((x + 14, y + 38), body, fill=TEXT, font=font("consola.ttf", 20), spacing=3)


def draw_dashboard(draw: ImageDraw.ImageDraw, x: int, y: int) -> None:
    rounded(draw, (x, y, x + 1096, y + 260), PANEL)
    metrics = [("NET VALUE", "review"), ("PRICE", "source"), ("CYCLE", "guarded"), ("BOUNDARY", "no bid")]
    for idx, (label, value) in enumerate(metrics):
        mx = x + 26 + idx * 262
        rounded(draw, (mx, y + 26, mx + 238, y + 92), (11, 30, 51))
        draw.text((mx + 18, y + 40), label, fill=MUTED, font=font("consola.ttf", 17))
        draw.text((mx + 18, y + 60), value, fill=LIME, font=font("consolab.ttf", 25))
    slots = [("03:00", "Charge", AMBER), ("04:00", "Charge", AMBER), ("07:00", "Discharge", LIME), ("11:00", "Hold", CYAN), ("20:00", "Discharge", LIME)]
    for idx, (hour, action, color) in enumerate(slots):
        sx = x + 26 + idx * 210
        fill = (116, 73, 20) if color == AMBER else ((30, 104, 37) if color == LIME else (17, 71, 101))
        rounded(draw, (sx, y + 118, sx + 185, y + 225), fill)
        draw.text((sx + 18, y + 136), hour, fill=TEXT if color != LIME else BG, font=font("consolab.ttf", 19))
        draw.text((sx + 18, y + 172), action, fill=TEXT if color != LIME else BG, font=font("arialbd.ttf", 25))


def draw_evidence(draw: ImageDraw.ImageDraw, x: int, y: int) -> None:
    cards = [
        ("174.77 UAH", "V2+ headline/default\nmean regret evidence"),
        ("168.16 UAH", "DT/V2+ safe-switch\nsecondary shadow signal"),
        ("158.71 UAH", "HF value-aligned\nmanual shadow signal"),
        ("252 tests", "dashboard unit tests\nin final repo audit"),
    ]
    for idx, (value, label) in enumerate(cards):
        cx = x + (idx % 2) * 540
        cy = y + (idx // 2) * 126
        rounded(draw, (cx, cy, cx + 500, cy + 104), PANEL)
        draw.text((cx + 24, cy + 18), value, fill=LIME, font=font("consolab.ttf", 34))
        draw.multiline_text((cx + 24, cy + 58), label, fill=(216, 230, 244), font=font("arial.ttf", 22), spacing=4)


def draw_boundary(draw: ImageDraw.ImageDraw, x: int, y: int) -> None:
    labels = ["market_execution_enabled=false", "no ProposedBid", "no market order payload"]
    for idx, label in enumerate(labels):
        bx = x + idx * 368
        rounded(draw, (bx, y, bx + 335, y + 118), (82, 59, 24), outline=AMBER)
        lines = wrap_text(label, font("consolab.ttf", 24), 286)
        draw.multiline_text((bx + 24, y + 34), "\n".join(lines), fill=(255, 247, 237), font=font("consolab.ttf", 24), spacing=4)


def draw_cta(draw: ImageDraw.ImageDraw, x: int, y: int) -> None:
    labels = [("/operator", "primary demo"), ("/defense", "evidence panels"), ("FastAPI /docs", "endpoint contracts")]
    for idx, (title, sub) in enumerate(labels):
        cx = x + idx * 368
        rounded(draw, (cx, y, cx + 335, y + 128), PANEL)
        draw.text((cx + 26, y + 24), str(idx + 1), fill=LIME, font=font("consolab.ttf", 22))
        draw.text((cx + 26, y + 54), title, fill=TEXT, font=font("consolab.ttf", 29))
        draw.text((cx + 26, y + 88), sub, fill=MUTED, font=font("arial.ttf", 20))


def draw_caption(draw: ImageDraw.ImageDraw, text: str) -> None:
    x0, y0, x1, y1 = 92, 612, 1188, 684
    rounded(draw, (x0, y0, x1, y1), (7, 19, 34), outline=(57, 107, 134))
    lines = wrap_text(text, font("arial.ttf", 22), 1040)
    draw.multiline_text((x0 + 20, y0 + 14), "\n".join(lines[:2]), fill=TEXT, font=font("arial.ttf", 22), spacing=4)


def rounded(
    draw: ImageDraw.ImageDraw,
    box: tuple[int, int, int, int],
    fill: tuple[int, int, int],
    *,
    outline: tuple[int, int, int] | None = None,
) -> None:
    draw.rounded_rectangle(box, radius=18, fill=fill, outline=outline, width=2 if outline else 1)


def wrap_text(text: str, fnt: ImageFont.FreeTypeFont, max_width: int) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current = ""
    scratch = Image.new("RGB", (1, 1))
    draw = ImageDraw.Draw(scratch)
    for word in words:
        trial = f"{current} {word}".strip()
        if draw.textbbox((0, 0), trial, font=fnt)[2] <= max_width:
            current = trial
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines


def multiline_height(text: str, fnt: ImageFont.FreeTypeFont, *, spacing: int) -> int:
    scratch = Image.new("RGB", (1, 1))
    draw = ImageDraw.Draw(scratch)
    bbox = draw.multiline_textbbox((0, 0), text, font=fnt, spacing=spacing)
    return bbox[3] - bbox[1]


def font(filename: str, size: int) -> ImageFont.FreeTypeFont:
    candidates = [
        Path("C:/Windows/Fonts") / filename,
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size=size)
    return ImageFont.load_default(size=size)


def ease_in_out(value: float) -> float:
    return 0.5 - 0.5 * math.cos(math.pi * clamp(value))


def clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def mux_audio(video_path: Path, audio_path: Path, output_path: Path) -> None:
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    tmp_path = output_path.with_suffix(".muxing.mp4")
    command = [
        ffmpeg,
        "-y",
        "-i",
        str(video_path),
        "-i",
        str(audio_path),
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-shortest",
        str(tmp_path),
    ]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    tmp_path.replace(output_path)


if __name__ == "__main__":
    main()
