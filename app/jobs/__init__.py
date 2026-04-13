"""Scheduled / CLI batch jobs (idempotent fetch + reporting).

Heavy imports (EIA / Comtrade / httpx) are lazy so ``run_quality_checks`` can load
without pulling optional HTTP clients.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = [
    "JobOutcome",
    "default_comtrade_period",
    "run_fetch_eia_data",
    "run_fetch_uncomtrade_data",
    "run_quality_checks",
]

if TYPE_CHECKING:
    from app.jobs.common import JobOutcome
    from app.jobs.uncomtrade_fetch import default_comtrade_period, run_fetch_uncomtrade_data
    from app.jobs.eia_fetch import run_fetch_eia_data
    from app.jobs.quality_checks import run_quality_checks


def __getattr__(name: str) -> Any:
    if name == "JobOutcome":
        from app.jobs.common import JobOutcome

        return JobOutcome
    if name == "run_fetch_eia_data":
        from app.jobs.eia_fetch import run_fetch_eia_data

        return run_fetch_eia_data
    if name == "run_fetch_uncomtrade_data":
        from app.jobs.uncomtrade_fetch import run_fetch_uncomtrade_data

        return run_fetch_uncomtrade_data
    if name == "default_comtrade_period":
        from app.jobs.uncomtrade_fetch import default_comtrade_period

        return default_comtrade_period
    if name == "run_quality_checks":
        from app.jobs.quality_checks import run_quality_checks

        return run_quality_checks
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
