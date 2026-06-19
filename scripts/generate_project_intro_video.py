"""Render the README project-intro video via the isolated Remotion project."""

from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = ROOT / "tools" / "project-intro-video"


def main() -> None:
    subprocess.run(["npm", "run", "build"], cwd=PROJECT_DIR, check=True)


if __name__ == "__main__":
    main()
