"""CLI/cron job: optional UN Comtrade Plus fetch + ingest."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

from app.connectors.uncomtrade.fetch import (
    COMTRADE_EXPECTED_RAW_COLUMNS,
    fetch_comtrade_data_rows,
)
from app.connectors.uncomtrade.normalize import normalize_uncomtrade_rows
from app.core.database import SessionLocal
from app.jobs.common import JobOutcome
from app.services.ingestion_engine import ingest_trade_flow_records

logger = logging.getLogger(__name__)


def default_comtrade_period() -> str:
    """Previous calendar month as ``YYYYMM`` (UTC), a stable default for monthly series."""
    now = datetime.now(timezone.utc)
    first_this = date(now.year, now.month, 1)
    last_prev = first_this - timedelta(days=1)
    return f"{last_prev.year:04d}{last_prev.month:02d}"


def run_fetch_uncomtrade_data(*, period: str | None = None) -> JobOutcome:
    """
    Fetch Comtrade ``data`` rows when ``UNCOMTRADE_API_KEY`` is set; otherwise no-op.

    Re-running with the same upstream slice is idempotent at the trade_flow layer.
    """
    session = SessionLocal()
    try:
        raw_rows = fetch_comtrade_data_rows(period=period)
        if not raw_rows:
            details = {
                "connector": "uncomtrade",
                "raw_row_count": 0,
                "normalized_count": 0,
                "note": "skipped_or_empty",
            }
            logger.info(
                "fetch_uncomtrade_data: no raw rows (missing key or empty response)"
            )
            return JobOutcome(
                ok=True,
                message="UN Comtrade fetch skipped or returned no rows",
                details=details,
            )

        records = normalize_uncomtrade_rows(raw_rows)
        stats = ingest_trade_flow_records(
            session,
            records,
            source_hint="uncomtrade",
            raw_rows=raw_rows,
            expected_raw_columns=COMTRADE_EXPECTED_RAW_COLUMNS,
        )
        details = {
            "connector": "uncomtrade",
            "raw_row_count": len(raw_rows),
            "normalized_count": len(records),
            "ingestion_run_id": stats.run_id,
            "inserted": stats.inserted,
            "revised": stats.revised,
            "unchanged": stats.unchanged,
            "failed": stats.failed,
        }
        logger.info(
            "fetch_uncomtrade_data completed run_id=%s inserted=%s revised=%s unchanged=%s",
            stats.run_id,
            stats.inserted,
            stats.revised,
            stats.unchanged,
        )
        return JobOutcome(
            ok=True,
            message="UN Comtrade fetch and ingestion completed",
            details=details,
        )
    except Exception as exc:
        logger.exception("fetch_uncomtrade_data failed: %s", exc)
        session.rollback()
        return JobOutcome(
            ok=False,
            message=str(exc),
            details={"connector": "uncomtrade", "error": str(exc)},
        )
    finally:
        session.close()
