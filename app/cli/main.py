"""
Command-line batch jobs (idempotent; structured logs).

Cron hosts (Railway, Render, GitHub Actions, etc.) typically run one command per schedule.
Use the **project virtualenv** (dependencies include ``httpx``), for example::

    .venv/bin/python -m app.cli fetch_eia_data
    .venv/bin/python -m app.cli fetch_uncomtrade_data --period 202401
    .venv/bin/python -m app.cli run_quality_checks --since-hours 48

On Debian/Ubuntu, ``python3`` without a venv often lacks these packages; prefer
``.venv/bin/python`` or ``pip install -r requirements.txt`` into your environment.

Environment: ``DATABASE_URL`` is required. ``EIA_API_KEY`` is required for EIA fetch.
``UNCOMTRADE_API_KEY`` is optional (job no-ops without it). Exit code ``0`` on success,
``1`` on failure (missing config, DB error, upstream HTTP error for EIA).

CLI options use a leading dash (e.g. ``--period 202412``), not square brackets.
"""

from __future__ import annotations

import json
import logging
import sys

import click

from app.cli.logging_config import configure_cli_logging

logger = logging.getLogger(__name__)


def _emit_json(outcome) -> None:
    """Single JSON line for log aggregators / scripts."""
    click.echo(
        json.dumps(
            {
                "ok": outcome.ok,
                "message": outcome.message,
                "details": outcome.details,
            },
            default=str,
        )
    )


@click.group()
def main() -> None:
    """Oil-flows scheduled jobs."""


@main.command("fetch_eia_data")
@click.option(
    "--log-level",
    default="INFO",
    show_default=True,
    help="Python logging level for stderr.",
)
@click.option("--start", default=None, help="EIA `start` query parameter (e.g. YYYY-MM).")
@click.option("--end", default=None, help="EIA `end` query parameter.")
@click.option(
    "--no-paginate",
    is_flag=True,
    default=False,
    help="Single request only (no offset/length paging).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Fetch + normalize + duplicate-key report only; do not write to the database.",
)
@click.option(
    "--include-eia-aggregates",
    is_flag=True,
    default=False,
    help="Include WORLD / REG_* / OPN_* (non-CTY) origin rows; default is country origins only.",
)
def fetch_eia_data_cmd(
    log_level: str,
    start: str | None,
    end: str | None,
    no_paginate: bool,
    dry_run: bool,
    include_eia_aggregates: bool,
) -> None:
    configure_cli_logging(log_level)
    from app.jobs.eia_fetch import run_fetch_eia_data

    outcome = run_fetch_eia_data(
        start=start,
        end=end,
        paginate=not no_paginate,
        dry_run=dry_run,
        country_origin_only=not include_eia_aggregates,
    )
    logger.info("job_result %s", json.dumps(outcome.details, default=str))
    _emit_json(outcome)
    sys.exit(0 if outcome.ok else 1)


@main.command("fetch_uncomtrade_data")
@click.option("--log-level", default="INFO", show_default=True)
@click.option(
    "--period",
    default=None,
    help="YYYYMM period for Comtrade query; default previous UTC month when key is set.",
)
def fetch_uncomtrade_data_cmd(log_level: str, period: str | None) -> None:
    configure_cli_logging(log_level)
    from app.jobs.uncomtrade_fetch import default_comtrade_period, run_fetch_uncomtrade_data

    resolved = period or default_comtrade_period()
    outcome = run_fetch_uncomtrade_data(period=resolved)
    logger.info("job_result %s", json.dumps(outcome.details, default=str))
    _emit_json(outcome)
    sys.exit(0 if outcome.ok else 1)


@main.command("run_quality_checks")
@click.option("--log-level", default="INFO", show_default=True)
@click.option(
    "--since-hours",
    default=24,
    type=int,
    show_default=True,
    help="Report window for DQ issues and ingestion runs.",
)
def run_quality_checks_cmd(log_level: str, since_hours: int) -> None:
    configure_cli_logging(log_level)
    from app.jobs.quality_checks import run_quality_checks

    outcome = run_quality_checks(since_hours=since_hours)
    logger.info("job_result %s", json.dumps(outcome.details, default=str))
    _emit_json(outcome)
    sys.exit(0 if outcome.ok else 1)
