"""Stderr logging for CLI / cron (Railway, Render, systemd timers)."""

from __future__ import annotations

import logging
import sys


def configure_cli_logging(level: str = "INFO") -> None:
    """Configure root logging once per process; ``force=True`` overrides prior handlers."""
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        force=True,
    )
    # httpx logs full URLs at INFO (includes api_key query param); keep library noise off INFO.
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)
