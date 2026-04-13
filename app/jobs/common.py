"""Shared job result type for CLI / cron."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class JobOutcome:
    """Returned by batch jobs; CLI maps ``ok`` to exit code (0 / 1)."""

    ok: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)
