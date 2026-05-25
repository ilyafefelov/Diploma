"""Repo-local Python startup guards for Windows tooling.

On this workstation, ``platform.machine()`` can block in Windows WMI during
imports that should be cheap, including Polars' CPU capability check. Python
loads this module automatically from the repo root, so keep the patch narrow.
"""

from __future__ import annotations

import os
import platform

if os.name == "nt":
    _machine = os.environ.get("PROCESSOR_ARCHITEW6432") or os.environ.get(
        "PROCESSOR_ARCHITECTURE"
    )
    _processor = os.environ.get("PROCESSOR_IDENTIFIER")

    if _machine:
        platform.machine = lambda: _machine  # type: ignore[assignment]
    if _processor:
        platform.processor = lambda: _processor  # type: ignore[assignment]
