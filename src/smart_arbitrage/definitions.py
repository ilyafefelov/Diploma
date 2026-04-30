"""Backward-compatible Dagster definitions entrypoint.

Prefer ``smart_arbitrage.defs`` for modern ``dg`` workflows.
"""

from smart_arbitrage.defs import defs

__all__ = ["defs"]